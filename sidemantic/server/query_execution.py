"""Bounded, cancellable execution primitives for server query paths."""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any, Literal

from sidemantic.db.base import CancellationOutcome
from sidemantic.server.common import result_to_record_batch_reader


class QueryExecutionError(RuntimeError):
    """Base class for stable server execution-policy errors."""


class QueryRowLimitExceededError(QueryExecutionError):
    """Raised when a result contains more than the configured row ceiling."""


class QueryResponseTooLargeError(QueryExecutionError):
    """Raised when a result cannot fit within the response byte ceiling."""


@dataclass(frozen=True, slots=True)
class QueryLimits:
    """Conservative per-process server query limits."""

    max_rows: int = 10_000
    max_response_bytes: int = 16 * 1024 * 1024
    execution_timeout_seconds: float = 30.0
    max_concurrent_queries: int = 4
    max_queued_queries: int = 16
    queue_timeout_seconds: float = 5.0

    def __post_init__(self) -> None:
        positive = {
            "max_rows": self.max_rows,
            "max_response_bytes": self.max_response_bytes,
            "execution_timeout_seconds": self.execution_timeout_seconds,
            "max_concurrent_queries": self.max_concurrent_queries,
            "queue_timeout_seconds": self.queue_timeout_seconds,
        }
        for name, value in positive.items():
            if value <= 0:
                raise ValueError(f"{name} must be positive")
        if self.max_queued_queries < 0:
            raise ValueError("max_queued_queries must be >= 0")


class QueryAdmission:
    """Bounded execution slots plus a bounded waiter count."""

    def __init__(self, max_concurrent: int, max_queued: int):
        if max_concurrent <= 0 or max_queued < 0:
            raise ValueError("invalid query admission limits")
        self._semaphore = threading.BoundedSemaphore(max_concurrent)
        self._max_queued = max_queued
        self._lock = threading.Lock()
        self._active = 0
        self._queued = 0

    def acquire(self, timeout: float) -> Literal["acquired", "full", "timeout"]:
        if self._semaphore.acquire(blocking=False):
            with self._lock:
                self._active += 1
            return "acquired"
        with self._lock:
            if self._queued >= self._max_queued:
                return "full"
            self._queued += 1
        try:
            acquired = self._semaphore.acquire(timeout=timeout)
        finally:
            with self._lock:
                self._queued -= 1
        if not acquired:
            return "timeout"
        with self._lock:
            self._active += 1
        return "acquired"

    def release(self) -> None:
        with self._lock:
            self._active -= 1
        self._semaphore.release()

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {"active": self._active, "queued": self._queued}


class QueryExecutionControl:
    """Thread-safe reference to the exact handle executing one query."""

    def __init__(self):
        self._lock = threading.Lock()
        self._adapter: Any | None = None
        self._handle: Any | None = None
        self._cancel_requested = False
        self._cancel_outcome: CancellationOutcome | None = None
        self.timeout_diagnostic: str | None = None

    @property
    def cancellation_outcome(self) -> CancellationOutcome | None:
        with self._lock:
            return self._cancel_outcome

    @property
    def cancel_requested(self) -> bool:
        """Whether the caller has requested that execution stop."""
        with self._lock:
            return self._cancel_requested

    def register(self, adapter: Any, handle: Any) -> None:
        with self._lock:
            self._adapter = adapter
            self._handle = handle
            cancel_requested = self._cancel_requested
        if cancel_requested:
            self.cancel()

    def register_if_active(self, adapter: Any, handle: Any) -> bool:
        """Register an acquired handle unless cancellation was already requested."""
        with self._lock:
            if self._cancel_requested:
                return False
            self._adapter = adapter
            self._handle = handle
            return True

    def unregister(self, handle: Any) -> None:
        with self._lock:
            if self._handle is handle:
                self._adapter = None
                self._handle = None

    def cancel(self) -> CancellationOutcome:
        with self._lock:
            self._cancel_requested = True
            if self._cancel_outcome is not None and not self._cancel_outcome.diagnostic.startswith(
                "query execution handle"
            ):
                return self._cancel_outcome
            adapter = self._adapter
            handle = self._handle
        if adapter is None or handle is None:
            outcome = CancellationOutcome(
                supported=False,
                cancelled=False,
                diagnostic="query execution handle is not available yet; cancellation remains pending",
            )
        else:
            outcome = adapter.cancel(handle)
        with self._lock:
            if self._cancel_outcome is None or self._cancel_outcome.diagnostic.startswith("query execution handle"):
                self._cancel_outcome = outcome
            return self._cancel_outcome


@dataclass(frozen=True, slots=True)
class BoundedQueryResult:
    table: Any
    row_count: int


def limit_query_sql(sql: str, max_rows: int, dialect: str) -> str:
    """Apply a dialect-aware outer row limit with one overflow sentinel row."""
    import sqlglot
    from sqlglot import exp

    statement = sqlglot.parse_one(sql, read=dialect)
    if not isinstance(statement, exp.Query):
        # Commands such as EXPLAIN, DESCRIBE, and PRAGMA may produce rows but
        # cannot be embedded in a derived table. Their streamed results still
        # pass through the row and byte checks in execute_bounded().
        return statement.sql(dialect=dialect)
    if dialect == "tsql" and isinstance(statement, exp.Select):
        # SQL Server rejects ORDER BY inside a derived table unless the inner
        # query also has TOP/OFFSET. Apply or tighten TOP/FETCH on the original
        # SELECT so ordered queries remain legal and smaller user limits survive.
        cap = max_rows + 1
        existing_limit = statement.args.get("limit")
        existing_count = None
        if existing_limit is not None:
            existing_count = existing_limit.args.get("expression") or existing_limit.args.get("count")
        if isinstance(existing_count, exp.Literal) and existing_count.is_int:
            if int(existing_count.this) <= cap:
                return statement.sql(dialect=dialect)
        return statement.limit(cap).sql(dialect=dialect)
    if dialect == "tsql" and isinstance(statement, (exp.Union, exp.Except, exp.Intersect)):
        # SQLGlot implements limit() on set operations by nesting the complete
        # expression. Hoist ORDER BY to the capped outer SELECT so SQL Server
        # does not see an illegal ordered derived table.
        cap = max_rows + 1
        existing_limit = statement.args.get("limit")
        existing_count = None
        if existing_limit is not None:
            existing_count = existing_limit.args.get("expression") or existing_limit.args.get("count")
        if isinstance(existing_count, exp.Literal) and existing_count.is_int and int(existing_count.this) <= cap:
            return statement.sql(dialect=dialect)
        inner = statement.copy()
        order = inner.args.get("order")
        if order is not None:
            inner.set("order", None)
        else:
            # SQLGlot represents a trailing TSQL ORDER BY on the rightmost
            # SELECT rather than on the set-operation node.
            rightmost = inner
            while isinstance(rightmost, (exp.Union, exp.Except, exp.Intersect)):
                rightmost = rightmost.expression
            order = rightmost.args.get("order")
            rightmost.set("order", None)
        inner.set("limit", None)
        bounded = exp.select("*").from_(inner.subquery("_sidemantic_bounded")).limit(cap)
        if order is not None:
            bounded.set("order", order.copy())
        return bounded.sql(dialect=dialect)
    bounded = exp.select("*").from_(statement.subquery("_sidemantic_bounded")).limit(max_rows + 1)
    return bounded.sql(dialect=dialect)


def execute_bounded(
    layer: Any,
    sql: str,
    *,
    limits: QueryLimits,
    control: QueryExecutionControl,
    cursor: Any | None = None,
) -> BoundedQueryResult:
    """Execute and consume Arrow batches without reading past ``max_rows + 1``."""
    import pyarrow as pa

    def consume(reader: Any) -> BoundedQueryResult:
        batches = []
        row_count = 0
        buffered_bytes = 0
        for batch in reader:
            remaining = limits.max_rows + 1 - row_count
            if remaining <= 0:
                break
            if batch.num_rows > remaining:
                batch = batch.slice(0, remaining)
            row_count += batch.num_rows
            buffered_bytes += int(batch.nbytes)
            if row_count > limits.max_rows:
                control.cancel()
                raise QueryRowLimitExceededError(
                    f"Query result exceeds the configured maximum of {limits.max_rows} rows; "
                    "add a narrower filter or LIMIT"
                )
            if buffered_bytes > limits.max_response_bytes:
                control.cancel()
                raise QueryResponseTooLargeError(
                    f"Query result exceeds the configured maximum response size of {limits.max_response_bytes} bytes"
                )
            batches.append(batch)
        table = pa.Table.from_batches(batches, schema=reader.schema)
        return BoundedQueryResult(table=table, row_count=row_count)

    if cursor is None:
        cursor = layer.adapter.cursor()
    execute_stream = getattr(cursor, "execute_stream", None)
    defer_registration = bool(getattr(cursor, "defer_execution_registration", False))
    if not defer_registration:
        control.register(layer.adapter, cursor)
    try:
        control.timeout_diagnostic = layer.adapter.configure_statement_timeout(cursor, limits.execution_timeout_seconds)
        if callable(execute_stream):
            stream_kwargs = {}
            if defer_registration:

                def register_after_lock_acquired() -> None:
                    if not control.register_if_active(layer.adapter, cursor):
                        raise QueryExecutionError(
                            "Query execution was cancelled before acquiring the shared database connection"
                        )

                stream_kwargs["on_acquired"] = register_after_lock_acquired
            with execute_stream(sql, **stream_kwargs) as reader:
                return consume(reader)
        result = cursor.execute(sql)
        return consume(result_to_record_batch_reader(result, layer.adapter))
    finally:
        control.unregister(cursor)
        close = getattr(cursor, "close", None)
        if callable(close):
            close()
