"""Tests for server query admission and cancellation diagnostics."""

import threading
import time
from types import SimpleNamespace

import pytest

from sidemantic.db.base import BaseDatabaseAdapter
from sidemantic.server.query_execution import (
    QueryAdmission,
    QueryExecutionControl,
    QueryLimits,
    QueryResponseTooLargeError,
    execute_bounded,
    limit_query_sql,
)


class _ControlCommandAdapter(BaseDatabaseAdapter):
    def __init__(self):
        self.statements = []
        self.control_cursor_closed = False

    @property
    def dialect(self):
        return "postgres"

    @property
    def raw_connection(self):
        return self

    def execute(self, sql):
        self.statements.append(sql)
        adapter = self

        class Result:
            class Cursor:
                def close(self):
                    adapter.control_cursor_closed = True

            cursor = Cursor()

        return Result()

    def fetch_record_batch(self, result):
        raise AssertionError("session control commands must not be materialized")

    def executemany(self, sql, params):
        raise NotImplementedError

    def fetchone(self, result):
        raise NotImplementedError

    def get_tables(self):
        return []

    def get_columns(self, table_name, schema=None):
        return []

    def close(self):
        return None


class _StreamingLimitAdapter(_ControlCommandAdapter):
    @property
    def dialect(self):
        return "test"

    def execute(self, sql):
        self.statements.append(sql)
        return object()

    def fetch_record_batch(self, result):
        import pyarrow as pa

        batch = pa.record_batch([["x" * 1024]], names=["payload"])

        class Reader:
            schema = batch.schema

            def __iter__(self):
                yield batch

            def read_all(self):
                raise AssertionError("bounded serving must not call read_all()")

            def close(self):
                return None

        return Reader()


class _CancellationProbe:
    def __init__(self):
        self.calls = 0

    def cancel(self):
        self.calls += 1


class _BlockingFallbackAdapter(_StreamingLimitAdapter):
    def __init__(self):
        super().__init__()
        self.query_started = threading.Event()
        self.release_query = threading.Event()
        self.execute_calls = 0
        self.connection = _CancellationProbe()

    @property
    def raw_connection(self):
        return self.connection

    def execute(self, sql):
        self.execute_calls += 1
        if self.execute_calls == 1:
            self.query_started.set()
            assert self.release_query.wait(timeout=2)
        return object()

    def fetch_record_batch(self, result):
        import pyarrow as pa

        return pa.table({"value": [1]}).to_reader()


def test_query_limits_validate_conservative_values():
    limits = QueryLimits()

    assert limits.max_rows == 10_000
    assert limits.max_response_bytes == 16 * 1024 * 1024
    assert limits.execution_timeout_seconds == 30.0
    assert limits.max_concurrent_queries == 4
    assert limits.max_queued_queries == 16


def test_limit_query_sql_uses_target_dialect_syntax():
    bounded = limit_query_sql("SELECT value FROM facts", 10, "tsql")

    assert bounded == "SELECT TOP 11 value FROM facts"
    assert " LIMIT " not in bounded


def test_limit_query_sql_keeps_tsql_order_by_at_top_level():
    bounded = limit_query_sql("SELECT value FROM facts ORDER BY value", 10, "tsql")

    assert bounded == "SELECT TOP 11 value FROM facts ORDER BY value"


def test_limit_query_sql_preserves_smaller_tsql_limit():
    bounded = limit_query_sql("SELECT TOP 5 value FROM facts ORDER BY value", 10, "tsql")

    assert bounded == "SELECT TOP 5 value FROM facts ORDER BY value"


@pytest.mark.parametrize("operator", ["UNION", "EXCEPT", "INTERSECT"])
def test_limit_query_sql_hoists_order_from_tsql_set_queries(operator):
    bounded = limit_query_sql(f"SELECT 1 AS value {operator} SELECT 2 AS value ORDER BY value", 10, "tsql")

    assert bounded == (
        f"SELECT TOP 11 * FROM (SELECT 1 AS value {operator} SELECT 2 AS value) AS _sidemantic_bounded ORDER BY value"
    )


@pytest.mark.parametrize("sql", ["EXPLAIN SELECT 1", "DESCRIBE facts", "PRAGMA version"])
def test_limit_query_sql_passes_row_producing_commands_through(sql):
    assert limit_query_sql(sql, 10, "duckdb") == sql


def test_admission_rejects_when_bounded_queue_is_full():
    admission = QueryAdmission(max_concurrent=1, max_queued=1)
    assert admission.acquire(timeout=0.1) == "acquired"

    waiter_result = []

    def wait_for_slot():
        waiter_result.append(admission.acquire(timeout=1.0))

    waiter = threading.Thread(target=wait_for_slot)
    waiter.start()
    deadline = time.monotonic() + 1
    while admission.stats()["queued"] != 1 and time.monotonic() < deadline:
        time.sleep(0.001)

    assert admission.stats() == {"active": 1, "queued": 1}
    assert admission.acquire(timeout=0.1) == "full"
    admission.release()
    waiter.join(timeout=1)

    assert waiter_result == ["acquired"]
    admission.release()


def test_cancellation_without_execution_handle_is_explicit():
    control = QueryExecutionControl()

    outcome = control.cancel()

    assert outcome.supported is False
    assert outcome.cancelled is False
    assert "not available yet" in outcome.diagnostic


def test_statement_timeout_control_command_does_not_materialize_rows():
    adapter = _ControlCommandAdapter()

    diagnostic = adapter.configure_statement_timeout(adapter.cursor(), 2.5)

    assert diagnostic is None
    assert adapter.statements == ["SET statement_timeout = 2500"]
    assert adapter.control_cursor_closed is True


def test_statement_timeout_requires_positive_value():
    adapter = _ControlCommandAdapter()

    with pytest.raises(ValueError, match="must be positive"):
        adapter.configure_statement_timeout(adapter.cursor(), 0)


def test_fallback_cursor_enforces_byte_limit_before_full_materialization():
    adapter = _StreamingLimitAdapter()
    layer = SimpleNamespace(adapter=adapter)

    with pytest.raises(QueryResponseTooLargeError):
        execute_bounded(
            layer,
            "SELECT payload",
            limits=QueryLimits(max_response_bytes=32),
            control=QueryExecutionControl(),
        )

    assert adapter.statements == ["SELECT payload"]


def test_queued_fallback_cancellation_does_not_cancel_connection_owner():
    adapter = _BlockingFallbackAdapter()
    layer = SimpleNamespace(adapter=adapter)
    first_results = []
    first_errors = []
    second_errors = []
    second_control = QueryExecutionControl()

    def run_first():
        try:
            first_results.append(
                execute_bounded(layer, "SELECT 1", limits=QueryLimits(), control=QueryExecutionControl())
            )
        except Exception as exc:
            first_errors.append(exc)

    def run_second():
        try:
            execute_bounded(layer, "SELECT 2", limits=QueryLimits(), control=second_control)
        except Exception as exc:
            second_errors.append(exc)

    first = threading.Thread(target=run_first)
    first.start()
    assert adapter.query_started.wait(timeout=1)

    second = threading.Thread(target=run_second)
    second.start()
    time.sleep(0.05)
    outcome = second_control.cancel()

    assert outcome.cancelled is False
    assert adapter.connection.calls == 0

    adapter.release_query.set()
    first.join(timeout=1)
    second.join(timeout=1)

    assert not first.is_alive()
    assert not second.is_alive()
    assert first_errors == []
    assert first_results[0].row_count == 1
    assert len(second_errors) == 1
    assert "cancelled before acquiring" in str(second_errors[0])
    assert adapter.execute_calls == 1
    assert adapter.connection.calls == 0
