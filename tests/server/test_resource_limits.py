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


def test_http_queue_timeout_returns_retryable_status():
    from fastapi.testclient import TestClient

    from sidemantic.api_server import create_app

    limits = common.ServerLimits(max_concurrency=1, queue_timeout_seconds=0.01)
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
