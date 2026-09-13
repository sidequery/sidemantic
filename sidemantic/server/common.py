"""Shared helpers for server-facing Sidemantic interfaces."""

from __future__ import annotations

import base64
import threading
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any

from sidemantic.db.base import BaseDatabaseAdapter

ARROW_STREAM_MEDIA_TYPE = "application/vnd.apache.arrow.stream"


def to_json_compatible(value: Any) -> Any:
    """Convert values to JSON-safe Python types."""
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, time):
        return value.isoformat()
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, tuple):
        return [to_json_compatible(item) for item in value]
    if isinstance(value, list):
        return [to_json_compatible(item) for item in value]
    if isinstance(value, dict):
        return {key: to_json_compatible(item) for key, item in value.items()}
    return value


def validate_filter_expression(filter_str: str, dialect: str | None = None) -> None:
    """Validate a filter string to prevent SQL injection."""
    import sqlglot
    from sqlglot.errors import SqlglotError

    try:
        parsed = sqlglot.parse_one(f"SELECT 1 WHERE {filter_str}", dialect=dialect)
    except SqlglotError as exc:
        raise ValueError(f"Invalid filter expression: {filter_str}") from exc

    # Check for multi-statement input after parsing succeeds. The raw ";"
    # check was removed because it rejected valid filters containing
    # semicolons inside string literals (e.g. status = ';').
    statements = sqlglot.parse(f"SELECT 1 WHERE {filter_str}", dialect=dialect)
    if len(statements) > 1:
        raise ValueError("Filter contains disallowed SQL: multi-statement input")

    disallowed_type_names = (
        "Drop",
        "Insert",
        "Delete",
        "Update",
        "Create",
        "Command",
        "AlterTable",
        "Alter",
    )
    disallowed_types = tuple(
        expr_type
        for type_name in disallowed_type_names
        if (expr_type := getattr(sqlglot.exp, type_name, None)) is not None
    )

    for node in parsed.walk():
        if disallowed_types and isinstance(node, disallowed_types):
            raise ValueError(f"Filter contains disallowed SQL: {type(node).__name__}")


def result_to_record_batch_reader(result: Any, adapter: BaseDatabaseAdapter) -> Any:
    """Return a RecordBatchReader for any adapter result."""
    if hasattr(result, "fetch_record_batch"):
        return result.fetch_record_batch()
    return adapter.fetch_record_batch(result)


def record_batch_reader_to_table(reader: Any) -> Any:
    """Materialize a RecordBatchReader into a PyArrow table."""
    return reader.read_all()


def table_to_json_rows(table: Any) -> list[dict[str, Any]]:
    """Convert a PyArrow table to JSON-safe rows."""
    return [to_json_compatible(row) for row in table.to_pylist()]


def reader_to_arrow_bytes(reader: Any) -> tuple[bytes, int]:
    """Stream a RecordBatchReader to Arrow IPC bytes without full table materialization.

    Returns ``(ipc_bytes, row_count)``.
    """
    import pyarrow as pa

    sink = pa.BufferOutputStream()
    row_count = 0
    with pa.ipc.new_stream(sink, reader.schema) as writer:
        for batch in reader:
            row_count += batch.num_rows
            writer.write_batch(batch)
    return sink.getvalue().to_pybytes(), row_count


def table_to_arrow_bytes(table: Any) -> bytes:
    """Serialize a PyArrow table to Arrow IPC stream bytes."""
    import pyarrow as pa

    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


# Transport limits do not change SemanticLayer's CLI/Python execution defaults.
MAX_RESULT_ROWS = 10_000
MAX_RESPONSE_BYTES = 16 * 1024 * 1024
QUERY_TIMEOUT_SECONDS = 30.0


@dataclass
class ServerLimits:
    max_rows: int = MAX_RESULT_ROWS
    max_bytes: int = MAX_RESPONSE_BYTES
    timeout_seconds: float = QUERY_TIMEOUT_SECONDS
    max_concurrency: int = 4
    slots: Any = field(init=False, repr=False)

    def __post_init__(self):
        import math

        if any(
            isinstance(value, bool) or not isinstance(value, int) or value <= 0
            for value in (self.max_rows, self.max_bytes, self.max_concurrency)
        ):
            raise ValueError("Server row, byte and concurrency limits must be positive integers")
        if not math.isfinite(self.timeout_seconds) or self.timeout_seconds <= 0:
            raise ValueError("Server timeout must be positive and finite")
        self.slots = threading.BoundedSemaphore(self.max_concurrency)


_default_limits = ServerLimits()
request_limits: ContextVar[ServerLimits | None] = ContextVar("server_limits", default=None)


def current_limits() -> ServerLimits:
    return request_limits.get() or _default_limits


def bounded_sql(sql: str, dialect: str) -> str:
    """Bound returned rows, including raw SQL; the extra row detects overflow."""
    import sqlglot

    query = sqlglot.parse_one(sql, dialect=dialect)
    maximum = current_limits().max_rows + 1
    limit = query.args.get("limit")
    expression = limit.expression if limit is not None else None
    if expression is None:
        return query.limit(maximum).sql(dialect=dialect)
    if isinstance(expression, sqlglot.exp.Literal) and expression.is_int:
        return query.limit(min(int(expression.this), maximum)).sql(dialect=dialect)
    return sqlglot.exp.select("*").from_(query.subquery("_sidemantic_result")).limit(maximum).sql(dialect=dialect)


def check_response_size(value: Any) -> None:
    import json

    size = len(value) if isinstance(value, bytes) else len(json.dumps(value, default=str).encode())
    if size > current_limits().max_bytes:
        raise ValueError(f"Response exceeds {current_limits().max_bytes} bytes")


def bounded_table(reader: Any) -> Any:
    """Drain Arrow batches with cumulative row and byte limits."""
    import pyarrow as pa

    batches = []
    rows = size = 0
    for batch in reader:
        rows += batch.num_rows
        size += batch.nbytes
        if rows > current_limits().max_rows or size > current_limits().max_bytes:
            raise ValueError("Query result exceeds server row or byte limit")
        batches.append(batch)
    return pa.Table.from_batches(batches, schema=reader.schema)


def execute_bounded(layer: Any, sql: str, *, arrow: bool = False) -> Any:
    """Bound transport execution and drain within the cursor lifetime.

    DuckDB's independent cursor supports interrupt. Other adapters still need
    backend statement timeouts configured by the operator.
    """
    import threading

    limits = current_limits()
    if not limits.slots.acquire(blocking=False):
        raise ValueError("Server query concurrency limit reached")
    cursor = None
    timer = None
    expired = threading.Event()
    finished = threading.Event()
    try:
        query = bounded_sql(sql, layer.dialect)
        cursor = layer.adapter.cursor()

        def interrupt():
            if finished.wait(limits.timeout_seconds):
                return
            expired.set()
            # A one-shot interrupt can race with execute starting (the driver
            # ignores interrupts while idle). Keep cancelling until draining ends.
            while not finished.is_set():
                cursor.interrupt()
                finished.wait(0.01)

        if callable(getattr(cursor, "interrupt", None)):
            timer = threading.Thread(target=interrupt, daemon=True)
            timer.start()
        execute = getattr(cursor, "execute_bounded", cursor.execute)
        result = execute(query)
        if arrow:
            output = bounded_table(result_to_record_batch_reader(result, layer.adapter))
        else:
            # Fetch incrementally; no pyarrow dependency for stdio MCP on DuckDB.
            columns = [desc[0] for desc in result.description]
            output = []
            size = 0
            import json

            while (row := result.fetchone()) is not None:
                item = {col: to_json_compatible(value) for col, value in zip(columns, row)}
                size += len(json.dumps(item).encode())
                if len(output) >= limits.max_rows or size > current_limits().max_bytes:
                    raise ValueError("Query result exceeds server row or byte limit")
                output.append(item)
        return output
    finally:
        if timer is not None:
            finished.set()
            timer.join()
        try:
            if cursor is not None:
                cursor.close()
        finally:
            limits.slots.release()
            if expired.is_set():
                raise ValueError("Query exceeded server execution deadline")
