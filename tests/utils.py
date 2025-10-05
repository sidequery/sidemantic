from __future__ import annotations

from decimal import Decimal
from typing import Any, Sequence


def _normalize(value: Any) -> Any:
    """Best-effort conversion of DuckDB result values into Python primitives."""

    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, memoryview):
        return value.tobytes()
    return value


def _column_names(result: Any) -> list[str]:
    if hasattr(result, "columns"):
        columns = getattr(result, "columns")
        if isinstance(columns, Sequence):
            return list(columns)
    if hasattr(result, "description") and result.description:
        return [col[0] for col in result.description]
    raise AttributeError("Result object does not expose column metadata")


def fetch_rows(result: Any) -> list[tuple[Any, ...]]:
    """Return query results as Python tuples with normalized values."""

    rows = result.fetchall()
    return [tuple(_normalize(value) for value in row) for row in rows]


def fetch_dicts(result: Any) -> list[dict[str, Any]]:
    """Return query results as dictionaries keyed by column name."""

    columns = _column_names(result)
    return [dict(zip(columns, row)) for row in fetch_rows(result)]


def df_rows(result: Any) -> list[tuple[Any, ...]]:
    """Backwards compatible alias for historical helper name."""

    return fetch_rows(result)


def fetch_columns(result: Any) -> list[str]:
    """Return the column names for a query result."""

    return _column_names(result)
