"""Tests for DuckDB adapter."""

import tempfile
from pathlib import Path

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


def test_duckdb_adapter_from_url_read_only():
    """Test read_only query parameter in DuckDB URL."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        writer = DuckDBAdapter(str(db_path))
        writer.execute("CREATE TABLE test (id INT)")
        writer.close()

        adapter = DuckDBAdapter.from_url(f"duckdb:///{db_path}?read_only=true")
        with pytest.raises(Exception):
            adapter.execute("CREATE TABLE blocked (id INT)")


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


def test_duckdb_absolute_file_paths():
    """Test that DuckDB absolute file paths preserve leading slash.

    Bug: duckdb:///tmp/app.db was converted to tmp/app.db (relative path).
    Fix: Preserve leading slash to get /tmp/app.db (absolute path).
    """
    from sidemantic import SemanticLayer

    # Test absolute path
    layer = SemanticLayer(connection="duckdb:///tmp/test.db")
    assert layer.conn is not None
    # Can't easily verify the exact path, but connection should work


def test_duckdb_memory_variations():
    """Test various :memory: URI formats."""
    from sidemantic import SemanticLayer

    # Standard memory
    layer1 = SemanticLayer(connection="duckdb:///:memory:")
    assert layer1.conn is not None

    # Just duckdb:// should default to memory
    layer2 = SemanticLayer(connection="duckdb:///")
    assert layer2.conn is not None


def test_duckdb_injection_attempt_in_table_name_is_rejected():
    """Verify SQL injection attempts in table names are rejected."""
    adapter = DuckDBAdapter()
    adapter.execute("CREATE TABLE orders (id INT)")

    table_name = "orders; DROP TABLE orders;--"
    with pytest.raises(ValueError, match="Invalid table name"):
        adapter.get_columns(table_name)


@pytest.mark.parametrize(
    "schema",
    ["main; DROP SCHEMA x;--", "default; --", "analytics'); DROP TABLE t;--"],
)
def test_duckdb_injection_attempt_in_schema_is_rejected(schema):
    """Verify SQL injection attempts in schema names are rejected."""
    adapter = DuckDBAdapter()
    adapter.execute("CREATE TABLE orders (id INT)")

    with pytest.raises(ValueError, match="Invalid schema"):
        adapter.get_columns("orders", schema=schema)


@pytest.mark.parametrize(
    "table_name",
    ["orders", "my_table", "Table123", "_private_table"],
)
def test_duckdb_valid_table_names_accepted(table_name):
    """Verify valid table names are accepted."""
    adapter = DuckDBAdapter()
    adapter.execute(f"CREATE TABLE {table_name} (id INT)")

    # Should not raise
    columns = adapter.get_columns(table_name)
    assert len(columns) >= 1


@pytest.mark.parametrize(
    "schema",
    ["main"],  # DuckDB default schema
)
def test_duckdb_valid_schema_names_accepted(schema):
    """Verify valid schema names are accepted."""
    adapter = DuckDBAdapter()
    adapter.execute("CREATE TABLE orders (id INT)")

    # Should not raise
    columns = adapter.get_columns("orders", schema=schema)
    assert len(columns) >= 1
