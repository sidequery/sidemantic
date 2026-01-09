"""Data registration utilities for accepting any dataframe-like object."""

from __future__ import annotations

from typing import Any


def to_arrow_table(data: Any):
    """Convert any dataframe-like object to PyArrow Table.

    Accepts:
    - PyArrow Table
    - Polars DataFrame (via __arrow_c_stream__)
    - Pandas DataFrame (via __dataframe__ interchange protocol)
    - DuckDB Relation
    - Arrow IPC bytes
    - Any object with __arrow_c_stream__ protocol

    Args:
        data: Input data in any supported format

    Returns:
        PyArrow Table
    """
    import pyarrow as pa

    # Already a PyArrow Table
    if isinstance(data, pa.Table):
        return data

    # Arrow IPC bytes
    if isinstance(data, (bytes, memoryview)):
        reader = pa.ipc.open_file(pa.BufferReader(data))
        return reader.read_all()

    # DuckDB Relation - call .arrow() method
    if hasattr(data, "arrow") and callable(data.arrow):
        return data.arrow()

    # Arrow PyCapsule interface (Polars, newer Arrow implementations)
    if hasattr(data, "__arrow_c_stream__"):
        reader = pa.RecordBatchReader.from_stream(data)
        return reader.read_all()

    # DataFrame interchange protocol (Pandas)
    if hasattr(data, "__dataframe__"):
        return pa.interchange.from_dataframe(data)

    # Try to convert via pa.table() as fallback
    return pa.table(data)


def register_data(data: Any, conn, table_name: str = "widget_data") -> str:
    """Register data into a DuckDB connection.

    Creates a permanent table (not just a view) to avoid Arrow type compatibility issues.

    Args:
        data: Input data in any supported format
        conn: DuckDB connection
        table_name: Name for the registered table

    Returns:
        The table name
    """
    arrow_table = to_arrow_table(data)
    # Register as temp view, then create table from it to materialize data
    # This avoids string_view and other Arrow type compatibility issues
    conn.register("_temp_arrow_data", arrow_table)
    conn.execute(f'CREATE OR REPLACE TABLE "{table_name}" AS SELECT * FROM _temp_arrow_data')
    conn.unregister("_temp_arrow_data")
    return table_name


def get_schema(data: Any):
    """Get the Arrow schema from any dataframe-like object.

    Args:
        data: Input data in any supported format

    Returns:
        PyArrow Schema
    """
    arrow_table = to_arrow_table(data)
    return arrow_table.schema
