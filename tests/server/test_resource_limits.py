"""Transport resource limits leave library defaults unchanged."""

import pytest

from sidemantic import SemanticLayer
from sidemantic.server import common


def test_bounded_rows_and_legitimate_control(monkeypatch):
    monkeypatch.setattr(common, "_default_limits", common.ServerLimits(max_rows=3))
    layer = SemanticLayer()
    with pytest.raises(ValueError, match="row or byte"):
        common.execute_bounded(layer, "select * from range(100)")
    assert common.execute_bounded(layer, "select * from range(2)") == [{"range": 0}, {"range": 1}]
    assert layer.adapter.execute("select * from range(5)").fetchall() == [(0,), (1,), (2,), (3,), (4,)]


def test_bounded_result_bytes(monkeypatch):
    monkeypatch.setattr(common, "_default_limits", common.ServerLimits(max_bytes=100))
    with pytest.raises(ValueError, match="row or byte"):
        common.execute_bounded(SemanticLayer(), "select repeat('x', 200) as value")


def test_real_duckdb_cancellation(monkeypatch):
    monkeypatch.setattr(common, "_default_limits", common.ServerLimits(timeout_seconds=0.01))
    layer = SemanticLayer()
    with pytest.raises(ValueError, match="deadline"):
        common.execute_bounded(layer, "select sum(a.range * b.range) from range(1000000) a cross join range(1000000) b")
    monkeypatch.setattr(common, "_default_limits", common.ServerLimits())
    assert common.execute_bounded(layer, "select 1 as n") == [{"n": 1}]


def test_concurrency_limit(monkeypatch):
    import threading

    monkeypatch.setattr(common, "_default_limits", common.ServerLimits(queue_timeout_seconds=0.01))
    monkeypatch.setattr(common._default_limits, "slots", threading.BoundedSemaphore(0))
    with pytest.raises(ValueError, match="concurrency"):
        common.execute_bounded(SemanticLayer(), "select 1")


@pytest.mark.parametrize("width,height", [(0, 400), (600, -1), (2001, 400), (600, 10**9)])
def test_chart_dimensions_rejected_before_execution(width, height):
    from sidemantic.mcp_server import create_chart

    with pytest.raises(ValueError, match="pixels"):
        create_chart(width=width, height=height)


def test_http_configured_caps_cover_raw_json_and_arrow():
    from fastapi.testclient import TestClient

    from sidemantic.api_server import create_app

    client = TestClient(create_app(SemanticLayer(), server_limits=common.ServerLimits(max_rows=3)))
    for suffix in ("?format=json", "?format=arrow"):
        response = client.post("/raw" + suffix, json={"query": "select * from range(100)"})
        assert response.status_code == 400
        response = client.post("/raw" + suffix, json={"query": "select * from range(2)"})
        assert response.status_code == 200


def test_limit_and_sort_preserved():
    result = common.execute_bounded(SemanticLayer(), "select * from range(10) order by range desc limit 2")
    assert result == [{"range": 9}, {"range": 8}]
    assert common.execute_bounded(SemanticLayer(), "select * from range(10) limit 0") == []


def test_deadline_retries_interrupt_that_races_before_execute(monkeypatch):
    import threading
    import time
    from types import SimpleNamespace

    class Cursor:
        active = False
        cancelled = threading.Event()

        def interrupt(self):
            if self.active:
                self.cancelled.set()

        def execute(self, sql):
            time.sleep(0.02)  # Driver has not entered execution at the deadline.
            self.active = True
            assert self.cancelled.wait(0.5), "Deadline interrupt was lost before execute"
            raise RuntimeError("interrupted")

        def close(self):
            pass

    monkeypatch.setattr(common, "_default_limits", common.ServerLimits(timeout_seconds=0.001))
    layer = SimpleNamespace(adapter=SimpleNamespace(cursor=Cursor), dialect="duckdb")
    with pytest.raises(ValueError, match="deadline"):
        common.execute_bounded(layer, "select 1")
    assert Cursor.cancelled.is_set()


@pytest.mark.parametrize("timeout", [-1, float("inf"), float("nan")])
def test_invalid_queue_timeout(timeout):
    with pytest.raises(ValueError, match="queue timeout"):
        common.ServerLimits(queue_timeout_seconds=timeout)


def test_busy_event_loop_does_not_wait_for_query_slot():
    import asyncio
    from time import monotonic

    limits = common.ServerLimits(max_concurrency=1, queue_timeout_seconds=1)
    assert limits.slots.acquire(blocking=False)

    async def run():
        token = common.request_limits.set(limits)
        try:
            with SemanticLayer() as layer:
                started = monotonic()
                with pytest.raises(common.QueryConcurrencyError):
                    common.execute_bounded(layer, "select 1")
                assert monotonic() - started < 0.5
        finally:
            common.request_limits.reset(token)

    asyncio.run(run())
    # A rejected query must not release the active query's slot.
    assert not limits.slots.acquire(blocking=False)
    limits.slots.release()


@pytest.mark.parametrize("queue_timeout", [0, 0.01])
def test_http_queue_timeout_returns_retryable_status(queue_timeout):
    from fastapi.testclient import TestClient

    from sidemantic.api_server import create_app

    limits = common.ServerLimits(max_concurrency=1, queue_timeout_seconds=queue_timeout)
    with SemanticLayer() as layer, TestClient(create_app(layer, server_limits=limits)) as client:
        assert limits.slots.acquire(blocking=False)
        try:
            response = client.post("/raw", json={"query": "select 1"})
            assert response.status_code == 503
            assert response.headers["Retry-After"] == "1"
            assert not limits.slots.acquire(blocking=False)
        finally:
            limits.slots.release()
        assert client.post("/raw", json={"query": "select 1"}).status_code == 200


@pytest.mark.parametrize("queue_timeout", [0, 0.1])
def test_http_queue_deadline_includes_worker_pool_wait(monkeypatch, queue_timeout):
    import asyncio
    import threading

    import anyio
    import httpx

    from sidemantic import api_server

    entered = threading.Event()
    release = threading.Event()
    executions = []
    original_execute = api_server._execute_to_table

    def execute(layer, sql):
        executions.append(sql)
        return original_execute(layer, sql)

    monkeypatch.setattr(api_server, "_execute_to_table", execute)

    # The worker stays occupied until the burst finishes, so a request that
    # queued behind it would stall for the full hold. The burst guard below is
    # well under the hold but loose enough to absorb CI scheduler/GC pauses.
    worker_hold_seconds = 10
    burst_guard_seconds = 3

    def occupy_worker():
        entered.set()
        assert release.wait(worker_hold_seconds)

    async def run(layer):
        limiter = anyio.to_thread.current_default_thread_limiter()
        old_tokens = limiter.total_tokens
        limiter.total_tokens = 1
        worker = asyncio.create_task(anyio.to_thread.run_sync(occupy_worker))
        limits = common.ServerLimits(queue_timeout_seconds=queue_timeout)
        app = api_server.create_app(layer, server_limits=limits, auth_token="test-token")
        try:
            assert await asyncio.to_thread(entered.wait, 1)
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test",
                headers={"Authorization": "Bearer test-token"},
            ) as client:
                # Both auth and query dispatch must avoid waiting indefinitely
                # behind the occupied worker. Cover every execution endpoint.
                requests = [
                    client.post(path, json=payload)
                    for path, payload in [
                        ("/raw", {"query": "select 1"}),
                        ("/sql", {"query": "select 1"}),
                        ("/query", {"metrics": [], "dimensions": []}),
                    ]
                    for _ in range(4)
                ]
                responses = await asyncio.wait_for(asyncio.gather(*requests), timeout=burst_guard_seconds)
                assert all(response.status_code == 503 for response in responses)
                assert all(response.headers["Retry-After"] == "1" for response in responses)
                assert (await client.get("/readyz")).status_code == 200
                assert not executions
                release.set()
                await worker
                assert (await client.post("/raw", json={"query": "select 1"})).status_code == 200
                assert len(executions) == 1
                assert limiter.borrowed_tokens == 0
        finally:
            release.set()
            await worker
            limiter.total_tokens = old_tokens

    with SemanticLayer() as layer:
        asyncio.run(run(layer))


def test_http_execution_can_outlast_queue_deadline(monkeypatch):
    import time

    from fastapi.testclient import TestClient

    from sidemantic import api_server

    execute = api_server._execute_to_table

    def slow_response(layer, sql):
        result = execute(layer, sql)
        time.sleep(0.05)
        return result

    monkeypatch.setattr(api_server, "_execute_to_table", slow_response)
    limits = common.ServerLimits(queue_timeout_seconds=0.02)
    with SemanticLayer() as layer, TestClient(api_server.create_app(layer, server_limits=limits)) as client:
        assert client.post("/raw", json={"query": "select 1"}).status_code == 200


def test_http_worker_and_slot_wait_share_one_deadline():
    import asyncio
    import threading

    import anyio
    import httpx

    from sidemantic.api_server import create_app

    entered = threading.Event()
    release = threading.Event()

    def occupy_worker():
        entered.set()
        assert release.wait(3)

    async def run(layer):
        limiter = anyio.to_thread.current_default_thread_limiter()
        old_tokens = limiter.total_tokens
        limiter.total_tokens = 1
        worker = asyncio.create_task(anyio.to_thread.run_sync(occupy_worker))
        limits = common.ServerLimits(max_concurrency=1, queue_timeout_seconds=0.4)
        assert limits.slots.acquire(blocking=False)
        try:
            assert await asyncio.to_thread(entered.wait, 1)
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=create_app(layer, server_limits=limits)), base_url="http://test"
            ) as client:
                query = asyncio.create_task(client.post("/raw", json={"query": "select 1"}))
                await asyncio.sleep(0.3)
                release.set()
                await worker
                # Only ~0.1s remains for the query slot, not a fresh 0.4s.
                response = await asyncio.wait_for(query, timeout=0.25)
                assert response.status_code == 503
                assert not limits.slots.acquire(blocking=False)
        finally:
            release.set()
            await worker
            limits.slots.release()
            limiter.total_tokens = old_tokens

    with SemanticLayer() as layer:
        asyncio.run(run(layer))


def test_query_burst_queues_and_readiness_survives_worker_saturation(monkeypatch):
    import asyncio
    import threading

    import anyio
    import httpx

    from sidemantic.api_server import create_app

    limits = common.ServerLimits(max_concurrency=1, queue_timeout_seconds=2)
    waiting = threading.Event()
    original_acquire = common._acquire_query_slot
    arrivals = 0

    def acquire(limits):
        nonlocal arrivals
        arrivals += 1
        if arrivals == 2:
            waiting.set()
        return original_acquire(limits)

    monkeypatch.setattr(common, "_acquire_query_slot", acquire)

    async def run(layer):
        limiter = anyio.to_thread.current_default_thread_limiter()
        old_tokens = limiter.total_tokens
        limiter.total_tokens = 2
        app = create_app(layer, server_limits=limits)
        try:
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
                assert limits.slots.acquire(blocking=False)
                queries = [asyncio.create_task(client.post("/raw", json={"query": "select 1 as n"})) for _ in range(12)]
                try:
                    assert await asyncio.to_thread(waiting.wait, 1), "Queries did not enter both worker threads"
                    ready = await asyncio.wait_for(client.get("/readyz"), timeout=0.5)
                    assert ready.status_code == 200
                finally:
                    limits.slots.release()
                    responses = await asyncio.gather(*queries)
                assert all(response.status_code == 200 for response in responses)
        finally:
            limiter.total_tokens = old_tokens

    with SemanticLayer() as layer:
        asyncio.run(run(layer))
