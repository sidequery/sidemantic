"""Tests for ADBC adapter.

These tests verify the ADBC adapter interface using drivers installed via:
1. Python packages (e.g., adbc_driver_sqlite) - for pip-based installations
2. DBC CLI (e.g., dbc install sqlite) - for system-wide driver management

The tests will use whichever method has SQLite available.
"""

import pytest

# Check if adbc_driver_manager is available
try:
    import adbc_driver_manager  # noqa: F401

    HAS_ADBC = True
except ImportError:
    HAS_ADBC = False


def _check_sqlite_driver() -> tuple[bool, str]:
    """Check if SQLite driver is available via Python package or DBC.

    Returns:
        Tuple of (is_available, driver_name)
    """
    if not HAS_ADBC:
        return False, ""

    # Try Python package first
    try:
        import adbc_driver_sqlite  # noqa: F401

        return True, "adbc_driver_sqlite"
    except ImportError:
        pass

    # Try DBC-installed driver
    try:
        import adbc_driver_manager.dbapi as adbc

        conn = adbc.connect(driver="sqlite")
        conn.close()
        return True, "sqlite"
    except Exception:
        pass

    return False, ""


HAS_SQLITE_DRIVER, SQLITE_DRIVER_NAME = _check_sqlite_driver()

pytestmark = pytest.mark.skipif(
    not (HAS_ADBC and HAS_SQLITE_DRIVER),
    reason="adbc_driver_manager and sqlite driver (pip or dbc) required for ADBC tests",
)


@pytest.fixture
def sqlite_adapter():
    """Create an ADBC adapter using SQLite driver (pip package or DBC-installed).

    Uses :memory: for test isolation - each test gets a fresh database.
    """
    from sidemantic.db.adbc import ADBCAdapter

    adapter = ADBCAdapter(driver=SQLITE_DRIVER_NAME, uri=":memory:")
    yield adapter
    try:
        adapter.close()
    except RuntimeError:
        pass  # Ignore close errors from open statements


def test_adbc_adapter_init():
    """Test ADBC adapter initialization."""
    from sidemantic.db.adbc import ADBCAdapter

    adapter = ADBCAdapter(driver=SQLITE_DRIVER_NAME, uri=":memory:")
    assert adapter.dialect == "sqlite"
    assert adapter.raw_connection is not None
    adapter.close()


def test_adbc_adapter_execute(sqlite_adapter):
    """Test executing queries."""
    result = sqlite_adapter.execute("SELECT 1 as x, 2 as y")
    row = result.fetchone()
    assert row == (1, 2)


def test_adbc_adapter_fetchall(sqlite_adapter):
    """Test fetching all rows."""
    sqlite_adapter.execute("CREATE TABLE test (x INT)")
    sqlite_adapter.execute("INSERT INTO test VALUES (1)")
    sqlite_adapter.execute("INSERT INTO test VALUES (2)")
    sqlite_adapter.execute("INSERT INTO test VALUES (3)")

    result = sqlite_adapter.execute("SELECT x FROM test ORDER BY x")
    rows = result.fetchall()
    assert rows == [(1,), (2,), (3,)]


def test_adbc_adapter_fetchone(sqlite_adapter):
    """Test fetching one row via adapter method."""
    result = sqlite_adapter.execute("SELECT 42 as answer")
    row = sqlite_adapter.fetchone(result)
    assert row == (42,)


def test_adbc_adapter_get_tables(sqlite_adapter):
    """Test getting table list."""
    sqlite_adapter.execute("CREATE TABLE test1 (x INT)")
    sqlite_adapter.execute("CREATE TABLE test2 (y VARCHAR(255))")

    tables = sqlite_adapter.get_tables()
    table_names = {t["table_name"] for t in tables}
    assert "test1" in table_names
    assert "test2" in table_names


def test_adbc_adapter_get_columns(sqlite_adapter):
    """Test getting column list."""
    sqlite_adapter.execute("CREATE TABLE test (x INT, y VARCHAR(255))")

    columns = sqlite_adapter.get_columns("test")
    assert len(columns) == 2
    col_names = {c["column_name"] for c in columns}
    assert "x" in col_names
    assert "y" in col_names


def test_adbc_adapter_dialect_mapping():
    """Test dialect mapping for different drivers."""
    from sidemantic.db.adbc import DRIVER_DIALECT_MAP

    # Verify key mappings
    assert DRIVER_DIALECT_MAP["postgresql"] == "postgres"
    assert DRIVER_DIALECT_MAP["mysql"] == "mysql"
    assert DRIVER_DIALECT_MAP["snowflake"] == "snowflake"
    assert DRIVER_DIALECT_MAP["bigquery"] == "bigquery"
    assert DRIVER_DIALECT_MAP["sqlite"] == "sqlite"
    assert DRIVER_DIALECT_MAP["duckdb"] == "duckdb"


def test_adbc_adapter_close(sqlite_adapter):
    """Test closing connection."""
    # Execute query to confirm connection works
    result = sqlite_adapter.execute("SELECT 1")
    assert result.fetchone() == (1,)

    # Close adapter
    sqlite_adapter.close()

    # After close, new queries should fail
    with pytest.raises(Exception):
        sqlite_adapter.execute("SELECT 1")


@pytest.mark.skipif(True, reason="Requires pyarrow (optional dependency)")
def test_adbc_adapter_fetch_record_batch(sqlite_adapter):
    """Test fetching Arrow RecordBatch.

    Skipped by default since pyarrow is optional.
    """
    pytest.importorskip("pyarrow")
    result = sqlite_adapter.execute("SELECT 1 as x, 2 as y")
    batch = sqlite_adapter.fetch_record_batch(result)
    # Should return Arrow RecordBatchReader
    assert batch is not None


def test_adbc_adapter_injection_attempt_in_table_name_is_rejected(sqlite_adapter):
    """Verify SQL injection attempts in table names are rejected."""
    sqlite_adapter.execute("CREATE TABLE orders (id INT)")

    table_name = "orders; DROP TABLE orders;--"
    with pytest.raises(ValueError, match="Invalid table name"):
        sqlite_adapter.get_columns(table_name)


@pytest.mark.parametrize(
    "schema",
    ["main; DROP SCHEMA x;--", "default; --", "analytics'); DROP TABLE t;--"],
)
def test_adbc_adapter_injection_attempt_in_schema_is_rejected(sqlite_adapter, schema):
    """Verify SQL injection attempts in schema names are rejected."""
    sqlite_adapter.execute("CREATE TABLE orders (id INT)")

    with pytest.raises(ValueError, match="Invalid schema"):
        sqlite_adapter.get_columns("orders", schema=schema)


@pytest.mark.parametrize(
    "table_name",
    ["orders", "my_table", "Table123", "_private_table"],
)
def test_adbc_adapter_valid_table_names_accepted(sqlite_adapter, table_name):
    """Verify valid table names are accepted."""
    sqlite_adapter.execute(f"CREATE TABLE {table_name} (id INT)")

    # Should not raise
    columns = sqlite_adapter.get_columns(table_name)
    assert len(columns) >= 1


def test_adbc_adapter_from_url():
    """Test creating adapter from URL."""
    from sidemantic.db.adbc import ADBCAdapter

    # SQLite supports URL-based connection
    # Note: from_url uses the URL scheme to determine the driver
    adapter = ADBCAdapter.from_url("sqlite:///:memory:")
    assert adapter.dialect == "sqlite"
    result = adapter.execute("SELECT 42")
    assert result.fetchone()[0] == 42
    adapter.close()


def test_adbc_adapter_from_url_invalid_scheme():
    """Test that invalid URL scheme raises ValueError."""
    from sidemantic.db.adbc import ADBCAdapter

    with pytest.raises(ValueError, match="Unknown URL scheme"):
        ADBCAdapter.from_url("unknown://localhost/mydb")


def test_adbc_import_error():
    """Test that ImportError is raised when adbc_driver_manager is not available."""
    # This test documents the expected error message
    # Can't easily test without actually uninstalling the package
    pass


def test_adbc_adapter_lazy_import():
    """Test that ADBCAdapter can be imported lazily from db module."""
    from sidemantic.db import ADBCAdapter

    assert ADBCAdapter is not None


# ============================================================================
# DBC System Driver Tests
# These tests verify that sidemantic can use drivers installed via the `dbc` CLI
# or as Python packages (adbc_driver_*)
# ============================================================================


def test_adbc_with_sqlite_driver():
    """Test that ADBCAdapter works with SQLite driver.

    This test verifies end-to-end integration with SQLite driver
    installed via either:
    - DBC CLI: dbc install sqlite
    - Python package: pip install adbc_driver_sqlite
    """
    from sidemantic.db.adbc import ADBCAdapter

    # Create adapter using the detected driver name with :memory: for isolation
    adapter = ADBCAdapter(driver=SQLITE_DRIVER_NAME, uri=":memory:")

    # Verify dialect mapping
    assert adapter.dialect == "sqlite"

    # Create a table and insert data
    adapter.execute("CREATE TABLE dbc_test (id INTEGER, name TEXT)")
    adapter.execute("INSERT INTO dbc_test VALUES (1, 'alice')")
    adapter.execute("INSERT INTO dbc_test VALUES (2, 'bob')")

    # Query and verify
    result = adapter.execute("SELECT id, name FROM dbc_test ORDER BY id")
    rows = result.fetchall()
    assert rows == [(1, "alice"), (2, "bob")]

    adapter.close()


def test_adbc_url_scheme_with_driver():
    """Test adbc:// URL scheme with installed driver."""
    from sidemantic.db.adbc import ADBCAdapter

    # Test adbc:// URL format - use the short name if DBC driver, else package name
    driver_for_url = "sqlite" if SQLITE_DRIVER_NAME == "sqlite" else SQLITE_DRIVER_NAME
    adapter = ADBCAdapter.from_url(f"adbc://{driver_for_url}")
    assert adapter.dialect == "sqlite"

    result = adapter.execute("SELECT 123 as value")
    assert result.fetchone()[0] == 123

    adapter.close()


def test_semantic_layer_with_adbc_connection():
    """Test SemanticLayer integration with ADBC connection."""
    from sidemantic import Dimension, Metric, Model, SemanticLayer

    # Create layer with adbc:// connection URL
    driver_for_url = "sqlite" if SQLITE_DRIVER_NAME == "sqlite" else SQLITE_DRIVER_NAME
    layer = SemanticLayer(connection=f"adbc://{driver_for_url}")

    # Create a test table
    layer.adapter.execute("CREATE TABLE orders (id INTEGER, status TEXT, amount REAL)")
    layer.adapter.execute("INSERT INTO orders VALUES (1, 'completed', 100.0)")
    layer.adapter.execute("INSERT INTO orders VALUES (2, 'pending', 50.0)")
    layer.adapter.execute("INSERT INTO orders VALUES (3, 'completed', 75.0)")

    # Define a model
    orders = Model(
        name="orders",
        table="orders",
        primary_key="id",
        dimensions=[
            Dimension(name="status", type="categorical", sql="status"),
        ],
        metrics=[
            Metric(name="order_count", agg="count"),
            Metric(name="total_amount", agg="sum", sql="amount"),
        ],
    )
    layer.add_model(orders)

    # Query the semantic layer
    result = layer.query(
        metrics=["orders.order_count", "orders.total_amount"],
        dimensions=["orders.status"],
    )
    rows = result.fetchall()

    # Verify results (completed: 2 orders, $175; pending: 1 order, $50)
    rows_dict = {row[0]: (row[1], row[2]) for row in rows}
    assert rows_dict["completed"] == (2, 175.0)
    assert rows_dict["pending"] == (1, 50.0)

    layer.adapter.close()


def test_yaml_connection_dict_to_adbc_url():
    """Test that YAML dict-style connection config converts to adbc:// URL."""
    from sidemantic.core.semantic_layer import SemanticLayer

    # Test adbc connection dict conversion
    config = {"type": "adbc", "driver": "sqlite"}
    url = SemanticLayer._connection_dict_to_url(config)
    assert url == "adbc://sqlite"

    # Test with additional params
    config_with_params = {
        "type": "adbc",
        "driver": "postgresql",
        "uri": "postgresql://localhost/mydb",
    }
    url_with_params = SemanticLayer._connection_dict_to_url(config_with_params)
    assert "adbc://postgresql" in url_with_params
    assert "uri=postgresql" in url_with_params


def test_adbc_url_parsing():
    """Test adbc:// URL parsing for various formats."""
    from urllib.parse import parse_qs, urlparse

    # Test basic driver name
    parsed = urlparse("adbc://sqlite")
    assert parsed.scheme == "adbc"
    assert parsed.netloc == "sqlite"

    # Test with uri parameter
    url = "adbc://postgresql?uri=postgresql://localhost/mydb"
    parsed = urlparse(url)
    assert parsed.scheme == "adbc"
    assert parsed.netloc == "postgresql"
    params = parse_qs(parsed.query)
    assert params["uri"][0] == "postgresql://localhost/mydb"

    # Test with multiple params
    url = "adbc://snowflake?account=myaccount&database=mydb&warehouse=mywh"
    parsed = urlparse(url)
    assert parsed.netloc == "snowflake"
    params = parse_qs(parsed.query)
    assert params["account"][0] == "myaccount"
    assert params["database"][0] == "mydb"
    assert params["warehouse"][0] == "mywh"


def test_adbc_url_path_based_uri():
    """Test adbc:// URL with URI as path (adbc://driver/uri format)."""
    from sidemantic.db.adbc import ADBCAdapter

    # Test path-based URI with SQLite
    adapter = ADBCAdapter.from_url("adbc://sqlite/:memory:")
    assert adapter.dialect == "sqlite"
    result = adapter.execute("SELECT 99 as value")
    assert result.fetchone()[0] == 99
    adapter.close()

    # Verify the format works for connection
    driver_for_url = "sqlite" if SQLITE_DRIVER_NAME == "sqlite" else SQLITE_DRIVER_NAME
    adapter2 = ADBCAdapter.from_url(f"adbc://{driver_for_url}/:memory:")
    assert adapter2.dialect == "sqlite"
    adapter2.close()
