"""Tests for DuckDB adapter."""

import pytest

from sidemantic.db.duckdb import DuckDBAdapter


def test_duckdb_adapter_memory():
    """Test DuckDB adapter with in-memory database."""
    adapter = DuckDBAdapter(":memory:")
    assert adapter.dialect == "duckdb"
    assert adapter.raw_connection is not None


def test_duckdb_adapter_execute():
    """Test executing queries."""
    adapter = DuckDBAdapter()
    result = adapter.execute("SELECT 1 as x, 2 as y")
    row = result.fetchone()
    assert row == (1, 2)


def test_duckdb_adapter_executemany():
    """Test executemany."""
    adapter = DuckDBAdapter()
    adapter.execute("CREATE TABLE test (x INT, y INT)")
    adapter.executemany("INSERT INTO test VALUES (?, ?)", [(1, 2), (3, 4)])
    result = adapter.execute("SELECT COUNT(*) FROM test")
    assert result.fetchone()[0] == 2


def test_duckdb_adapter_from_url_memory():
    """Test creating adapter from URL (memory)."""
    adapter = DuckDBAdapter.from_url("duckdb:///:memory:")
    assert adapter.dialect == "duckdb"
    result = adapter.execute("SELECT 42")
    assert result.fetchone()[0] == 42


def test_duckdb_adapter_from_url_variations():
    """Test various memory URL formats."""
    urls = [
        "duckdb:///:memory:",
        "duckdb:///",
    ]
    for url in urls:
        adapter = DuckDBAdapter.from_url(url)
        result = adapter.execute("SELECT 1")
        assert result.fetchone()[0] == 1


def test_duckdb_adapter_get_tables():
    """Test getting table list."""
    adapter = DuckDBAdapter()
    adapter.execute("CREATE TABLE test1 (x INT)")
    adapter.execute("CREATE TABLE test2 (x INT)")

    tables = adapter.get_tables()
    table_names = {t["table_name"] for t in tables}
    assert "test1" in table_names
    assert "test2" in table_names


def test_duckdb_adapter_get_columns():
    """Test getting column list."""
    adapter = DuckDBAdapter()
    adapter.execute("CREATE TABLE test (x INT, y VARCHAR)")

    columns = adapter.get_columns("test")
    assert len(columns) == 2
    col_names = {c["column_name"] for c in columns}
    assert "x" in col_names
    assert "y" in col_names


def test_duckdb_adapter_close():
    """Test closing connection."""
    adapter = DuckDBAdapter()
    adapter.execute("SELECT 1")
    adapter.close()
    # After close, new queries should fail
    with pytest.raises(Exception):
        adapter.execute("SELECT 1")


@pytest.mark.skipif(True, reason="Requires pyarrow (optional dependency)")
def test_duckdb_adapter_fetch_record_batch():
    """Test fetching Arrow RecordBatch.

    Skipped by default since pyarrow is optional.
    """
    pytest.importorskip("pyarrow")
    adapter = DuckDBAdapter()
    result = adapter.execute("SELECT 1 as x, 2 as y")
    batch = adapter.fetch_record_batch(result)
    # Should return Arrow RecordBatchReader
    assert batch is not None
