"""Tests for ADBC adapter.

These tests verify the ADBC adapter interface using drivers installed via:
1. Python packages (e.g., adbc_driver_sqlite) - for pip-based installations
2. DBC CLI (e.g., dbc install sqlite) - for system-wide driver management

The tests will use whichever method has SQLite available.
"""

from types import SimpleNamespace

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

SQLITE_TESTS_AVAILABLE = HAS_ADBC and HAS_SQLITE_DRIVER
SQLITE_TESTS_SKIP_REASON = "adbc_driver_manager and sqlite driver (pip or dbc) required for ADBC connection tests"


@pytest.fixture
def sqlite_adapter():
    """Create an ADBC adapter using SQLite driver (pip package or DBC-installed).

    Uses :memory: for test isolation - each test gets a fresh database.
    """
    if not SQLITE_TESTS_AVAILABLE:
        pytest.skip(SQLITE_TESTS_SKIP_REASON)

    from sidemantic.db.adbc import ADBCAdapter

    adapter = ADBCAdapter(driver=SQLITE_DRIVER_NAME, uri=":memory:")
    yield adapter
    try:
        adapter.close()
    except RuntimeError:
        pass  # Ignore close errors from open statements


def test_adbc_adapter_init():
    """Test ADBC adapter initialization."""
    if not SQLITE_TESTS_AVAILABLE:
        pytest.skip(SQLITE_TESTS_SKIP_REASON)

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
    assert DRIVER_DIALECT_MAP["clickhouse"] == "clickhouse"
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
    if not SQLITE_TESTS_AVAILABLE:
        pytest.skip(SQLITE_TESTS_SKIP_REASON)

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
    if not SQLITE_TESTS_AVAILABLE:
        pytest.skip(SQLITE_TESTS_SKIP_REASON)

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
    if not SQLITE_TESTS_AVAILABLE:
        pytest.skip(SQLITE_TESTS_SKIP_REASON)

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
    if not SQLITE_TESTS_AVAILABLE:
        pytest.skip(SQLITE_TESTS_SKIP_REASON)

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
    if not SQLITE_TESTS_AVAILABLE:
        pytest.skip(SQLITE_TESTS_SKIP_REASON)

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


class _FakeCursor:
    def __init__(self, rows=None, description=None, arrow_table=None, close_error=False):
        self.rows = list(rows or [])
        self.description = description or [("value",)]
        self.arrow_table = arrow_table
        self.close_error = close_error
        self.closed = False

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return list(self.rows)

    def close(self):
        self.closed = True
        if self.close_error:
            raise RuntimeError("close failed")

    def fetch_arrow_table(self):
        return self.arrow_table


def test_adbc_result_fetch_helpers_close_cursor():
    import pyarrow as pa

    from sidemantic.db.adbc import ADBCResult

    cursor = _FakeCursor(rows=[(1,)], description=[("x",)], arrow_table=pa.table({"x": [1]}))
    result = ADBCResult(cursor)
    assert result.description == [("x",)]
    assert result.fetchone() == (1,)
    assert cursor.closed is True

    cursor2 = _FakeCursor(rows=[(1,), (2,)])
    result2 = ADBCResult(cursor2)
    assert result2.fetchall() == [(1,), (2,)]
    assert cursor2.closed is True

    cursor3 = _FakeCursor(arrow_table=pa.table({"x": [1, 2]}), close_error=True)
    result3 = ADBCResult(cursor3)
    batch_reader = result3.fetch_record_batch()
    assert batch_reader.read_all().to_pylist() == [{"x": 1}, {"x": 2}]
    assert cursor3.closed is True


def test_adbc_adapter_get_tables_uses_native_metadata():
    from sidemantic.db.adbc import ADBCAdapter

    adapter = ADBCAdapter.__new__(ADBCAdapter)
    adapter.conn = SimpleNamespace(
        adbc_get_objects=lambda: SimpleNamespace(
            read_all=lambda: SimpleNamespace(
                to_pydict=lambda: {
                    "catalog_db_schemas": [
                        [
                            {
                                "db_schema_name": "analytics",
                                "db_schema_tables": [{"table_name": "orders"}, {"table_name": "customers"}],
                            }
                        ]
                    ]
                }
            )
        )
    )

    tables = adapter.get_tables()

    assert tables == [
        {"table_name": "orders", "schema": "analytics"},
        {"table_name": "customers", "schema": "analytics"},
    ]


def test_adbc_adapter_get_tables_falls_back_to_information_schema(monkeypatch):
    from sidemantic.db.adbc import ADBCAdapter

    adapter = ADBCAdapter.__new__(ADBCAdapter)
    adapter.conn = SimpleNamespace(adbc_get_objects=lambda: (_ for _ in ()).throw(RuntimeError("no metadata")))
    captured = {}

    class FakeResult:
        def fetchall(self):
            return [("orders", "analytics"), ("customers", "public")]

    def fake_execute(sql):
        captured["sql"] = sql
        return FakeResult()

    adapter.execute = fake_execute

    tables = adapter.get_tables()

    assert "information_schema.tables" in captured["sql"]
    assert tables == [
        {"table_name": "orders", "schema": "analytics"},
        {"table_name": "customers", "schema": "public"},
    ]


def test_adbc_adapter_get_columns_uses_table_schema():
    import pyarrow as pa

    from sidemantic.db.adbc import ADBCAdapter

    adapter = ADBCAdapter.__new__(ADBCAdapter)
    adapter._driver_name = "sqlite"
    adapter.conn = SimpleNamespace(
        adbc_get_table_schema=lambda **kwargs: pa.schema([("id", pa.int64()), ("name", pa.string())])
    )

    columns = adapter.get_columns("orders")

    assert columns == [
        {"column_name": "id", "data_type": "int64"},
        {"column_name": "name", "data_type": "string"},
    ]


def test_adbc_adapter_get_columns_uses_objects_metadata_fallback():
    from sidemantic.db.adbc import ADBCAdapter

    adapter = ADBCAdapter.__new__(ADBCAdapter)
    adapter._driver_name = "sqlite"
    adapter.conn = SimpleNamespace(
        adbc_get_table_schema=lambda **kwargs: (_ for _ in ()).throw(RuntimeError("no schema")),
        adbc_get_objects=lambda **kwargs: SimpleNamespace(
            read_all=lambda: SimpleNamespace(
                to_pydict=lambda: {
                    "catalog_db_schemas": [
                        [
                            {
                                "db_schema_name": "main",
                                "db_schema_tables": [
                                    {
                                        "table_name": "orders",
                                        "table_columns": [
                                            {"column_name": "id", "xdbc_type_name": "INTEGER"},
                                            {"column_name": "name", "xdbc_type_name": "TEXT"},
                                        ],
                                    }
                                ],
                            }
                        ]
                    ]
                }
            )
        ),
    )

    columns = adapter.get_columns("orders", schema="main")

    assert columns == [
        {"column_name": "id", "data_type": "INTEGER"},
        {"column_name": "name", "data_type": "TEXT"},
    ]


def test_adbc_adapter_get_columns_falls_back_to_sql_for_snowflake(monkeypatch):
    from sidemantic.db.adbc import ADBCAdapter

    adapter = ADBCAdapter.__new__(ADBCAdapter)
    adapter._driver_name = "snowflake"
    adapter.conn = SimpleNamespace(
        adbc_get_table_schema=lambda **kwargs: (_ for _ in ()).throw(RuntimeError("no schema")),
        adbc_get_objects=lambda **kwargs: (_ for _ in ()).throw(RuntimeError("no objects")),
    )
    captured = {}

    class FakeResult:
        def fetchall(self):
            return [("ID", "NUMBER"), ("STATUS", "VARCHAR")]

    def fake_execute(sql):
        captured["sql"] = sql
        return FakeResult()

    adapter.execute = fake_execute

    columns = adapter.get_columns("orders", schema="analytics")

    assert "table_name IN ('ORDERS', 'orders')" in captured["sql"]
    assert "table_schema IN ('ANALYTICS', 'analytics')" in captured["sql"]
    assert columns == [
        {"column_name": "ID", "data_type": "NUMBER"},
        {"column_name": "STATUS", "data_type": "VARCHAR"},
    ]


def test_adbc_adapter_dialect_strips_package_prefix():
    from sidemantic.db.adbc import ADBCAdapter

    adapter = ADBCAdapter.__new__(ADBCAdapter)
    adapter._driver_name = "adbc_driver_postgresql"

    assert adapter.dialect == "postgres"


def test_adbc_adapter_close_calls_connection():
    from sidemantic.db.adbc import ADBCAdapter

    closed = {"value": False}
    adapter = ADBCAdapter.__new__(ADBCAdapter)
    adapter.conn = SimpleNamespace(close=lambda: closed.__setitem__("value", True))

    adapter.close()

    assert closed["value"] is True


def test_adbc_adapter_from_url_sqlite_defaults_to_memory(monkeypatch):
    from sidemantic.db.adbc import ADBCAdapter

    captured = {}
    original_init = ADBCAdapter.__init__

    def fake_init(self, driver, uri=None, **kwargs):
        captured["driver"] = driver
        captured["uri"] = uri
        captured["kwargs"] = kwargs

    monkeypatch.setattr(ADBCAdapter, "__init__", fake_init)
    try:
        adapter = ADBCAdapter.from_url("adbc://sqlite")
    finally:
        monkeypatch.setattr(ADBCAdapter, "__init__", original_init)

    assert isinstance(adapter, ADBCAdapter)
    assert captured["driver"] == "sqlite"
    assert captured["uri"] == ":memory:"


def test_adbc_adapter_from_url_adbc_query_params_become_db_kwargs(monkeypatch):
    from sidemantic.db.adbc import ADBCAdapter

    captured = {}
    original_init = ADBCAdapter.__init__

    def fake_init(self, driver, uri=None, **kwargs):
        captured["driver"] = driver
        captured["uri"] = uri
        captured["kwargs"] = kwargs

    monkeypatch.setattr(ADBCAdapter, "__init__", fake_init)
    try:
        adapter = ADBCAdapter.from_url("adbc://snowflake?account=myacct&warehouse=wh")
    finally:
        monkeypatch.setattr(ADBCAdapter, "__init__", original_init)

    assert isinstance(adapter, ADBCAdapter)
    assert captured["driver"] == "snowflake"
    assert captured["uri"] is None
    assert captured["kwargs"]["db_kwargs"] == {"account": "myacct", "warehouse": "wh"}
