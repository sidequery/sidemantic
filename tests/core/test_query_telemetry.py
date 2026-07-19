"""Tests for process-local sanitized query telemetry."""

from sidemantic.core.query_telemetry import QueryEvent, QueryTelemetry, sanitize_sql


def _event(query_id: str) -> QueryEvent:
    return QueryEvent(query_id=query_id, request_id=None, duration_ms=1.0, dialect="duckdb")


def test_history_is_bounded_and_newest_first():
    telemetry = QueryTelemetry(history_size=2)
    telemetry.record(_event("one"))
    telemetry.record(_event("two"))
    telemetry.record(_event("three"))

    assert [event.query_id for event in telemetry.history()] == ["three", "two"]


def test_listener_failure_does_not_block_other_listeners():
    telemetry = QueryTelemetry(history_size=1)
    received = []

    def broken(_event):
        raise RuntimeError("listener failed")

    telemetry.add_listener(broken)
    telemetry.add_listener(received.append)
    telemetry.record(_event("query"))

    assert [event.query_id for event in received] == ["query"]


def test_resize_preserves_newest_events_and_listeners():
    telemetry = QueryTelemetry(history_size=3)
    received = []
    telemetry.add_listener(received.append)
    for query_id in ("one", "two", "three"):
        telemetry.record(_event(query_id))

    telemetry.resize(1)
    telemetry.record(_event("four"))

    assert [event.query_id for event in telemetry.history()] == ["four"]
    assert received[-1].query_id == "four"


def test_sql_sanitization_removes_literals_and_comments():
    sanitized, fingerprint = sanitize_sql(
        "SELECT * FROM orders WHERE token = 'top-secret' AND account_id = 42 -- private",
        "duckdb",
    )

    assert sanitized is not None
    assert "top-secret" not in sanitized
    assert "42" not in sanitized
    assert "private" not in sanitized
    assert len(fingerprint) == 64


def test_unparseable_sql_retains_only_fingerprint():
    sanitized, fingerprint = sanitize_sql("SELECT 'secret' !!!", "duckdb")

    assert sanitized is None
    assert len(fingerprint) == 64
