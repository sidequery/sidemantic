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


def test_query_limits_validate_conservative_values():
    limits = QueryLimits()

    assert limits.max_rows == 10_000
    assert limits.max_response_bytes == 16 * 1024 * 1024
    assert limits.execution_timeout_seconds == 30.0
    assert limits.max_concurrent_queries == 4
    assert limits.max_queued_queries == 16


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
