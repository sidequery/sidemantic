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
