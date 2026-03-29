"""Shared helpers for server-facing Sidemantic interfaces."""

from __future__ import annotations

import base64
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

    try:
        parsed = sqlglot.parse_one(f"SELECT 1 WHERE {filter_str}", dialect=dialect)
    except Exception as exc:
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
