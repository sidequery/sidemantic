"""Tests for Databricks adapter."""

import builtins
import sys

import pytest

from sidemantic.db.databricks import DatabricksAdapter, DatabricksResult


def _block_import(monkeypatch, module_prefix: str) -> None:
    for name in list(sys.modules):
        if name == module_prefix or name.startswith(f"{module_prefix}."):
            monkeypatch.delitem(sys.modules, name, raising=False)

    real_import = builtins.__import__

    def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == module_prefix or name.startswith(f"{module_prefix}."):
            raise ImportError(f"Blocked import: {module_prefix}")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", fake_import)


class _FakeCursor:
    def __init__(self, rows=None, description=None):
        self._rows = rows or []
        self._idx = 0
        self.description = description or []
        self.executed = []
        self.executemany_args = None

    def execute(self, sql):
        self.executed.append(sql)

    def executemany(self, sql, params):
        self.executemany_args = (sql, params)

    def fetchone(self):
        if self._idx >= len(self._rows):
            return None
        row = self._rows[self._idx]
        self._idx += 1
        return row

    def fetchall(self):
        return self._rows[self._idx :]


class _FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def test_databricks_adapter_import_error_message(monkeypatch):
    _block_import(monkeypatch, "databricks")

    with pytest.raises(ImportError) as exc:
        DatabricksAdapter(server_hostname="host", http_path="/sql")

    assert "databricks" in str(exc.value).lower()


def test_databricks_from_url_matrix(monkeypatch):
    cases = [
        (
            "databricks://token@host/sql/1.0/warehouses/abc",
            {"server_hostname": "host", "http_path": "/sql/1.0/warehouses/abc", "access_token": "token"},
        ),
        ("databricks://token@host/sql", {"http_path": "/sql"}),
        ("databricks://token@host", {"http_path": ""}),
        ("databricks://@host/sql/1.0/warehouses/abc", {"access_token": None}),
        ("databricks://token@host/sql/1.0/warehouses/abc?catalog=main", {"catalog": "main"}),
        ("databricks://token@host/sql/1.0/warehouses/abc?schema=default", {"schema": "default"}),
        (
            "databricks://token@host/sql/1.0/warehouses/abc?catalog=main&schema=default",
            {"catalog": "main", "schema": "default"},
        ),
        ("databricks://token@host/sql/1.0/warehouses/abc?http=1", {"http": "1"}),
        ("databricks://token@host/sql/1.0/warehouses/abc?param=1", {"param": "1"}),
        ("databricks://token@host/sql/1.0/warehouses/abc?param=1&param=2", {"param": ["1", "2"]}),
        ("databricks://token@host/sql/1.0/warehouses/abc?catalog=main&http=1", {"catalog": "main", "http": "1"}),
        ("databricks://token@host/sql/1.0/warehouses/abc?user_agent=test", {"user_agent": "test"}),
        ("databricks://token@host/sql/1.0/warehouses/abc?param=a&param=b", {"param": ["a", "b"]}),
        ("databricks://token@host/sql/1.0/warehouses/abc?schema=analytics", {"schema": "analytics"}),
    ]

    for url, expected in cases:
        captured = {}

        def fake_init(self, **kwargs):
            captured.update(kwargs)

        monkeypatch.setattr(DatabricksAdapter, "__init__", fake_init)
        adapter = DatabricksAdapter.from_url(url)
        assert isinstance(adapter, DatabricksAdapter)

        for key, value in expected.items():
            assert captured[key] == value


def test_databricks_from_url_invalid_scheme():
    with pytest.raises(ValueError, match="Invalid Databricks URL"):
        DatabricksAdapter.from_url("http://host/db")


def test_databricks_from_url_missing_hostname():
    with pytest.raises(ValueError, match="server hostname"):
        DatabricksAdapter.from_url("databricks://token@/sql")


def test_databricks_from_url_http_path_param_raises():
    with pytest.raises(TypeError, match="http_path"):
        DatabricksAdapter.from_url("databricks://token@host/sql/1.0/warehouses/abc?http_path=/sql")


def test_databricks_executemany_and_get_tables_sql():
    cursor = _FakeCursor(rows=[("default", "orders")])

    adapter = DatabricksAdapter.__new__(DatabricksAdapter)
    adapter.conn = _FakeConn(cursor)
    adapter.schema = None
    adapter.catalog = None

    adapter.executemany("INSERT INTO t VALUES (?)", [(1,), (2,)])
    assert cursor.executemany_args == ("INSERT INTO t VALUES (?)", [(1,), (2,)])

    tables = adapter.get_tables()
    assert tables == [{"table_name": "orders", "schema": "default"}]
    assert cursor.executed[0].startswith("SHOW TABLES")


def test_databricks_get_columns_sql_and_schema_override():
    cursor = _FakeCursor(rows=[("id", "int")])

    adapter = DatabricksAdapter.__new__(DatabricksAdapter)
    adapter.conn = _FakeConn(cursor)
    adapter.schema = "default"

    cols = adapter.get_columns("orders", schema="analytics")
    assert cols == [{"column_name": "id", "data_type": "int"}]
    assert cursor.executed[0] == "DESCRIBE analytics.orders"


def test_databricks_query_history_sql():
    adapter = DatabricksAdapter.__new__(DatabricksAdapter)
    captured = {}

    class FakeResult:
        def fetchall(self):
            return [["select 1 -- sidemantic: z"], [None]]

    def fake_execute(sql):
        captured["sql"] = sql
        return FakeResult()

    adapter.execute = fake_execute

    results = adapter.get_query_history(days_back=2, limit=25)

    assert "system.query.history" in captured["sql"]
    assert "INTERVAL 2 DAYS" in captured["sql"]
    assert "LIMIT 25" in captured["sql"]
    assert results == ["select 1 -- sidemantic: z"]


def test_databricks_result_ordering_and_exhaustion():
    cursor = _FakeCursor(rows=[(1,), (2,)], description=[("a", None)])
    wrapper = DatabricksResult(cursor)
    assert wrapper.fetchone() == (1,)
    assert wrapper.fetchone() == (2,)
    assert wrapper.fetchone() is None


def test_databricks_result_description_order():
    desc = [("b", "text"), ("a", "int")]
    cursor = _FakeCursor(rows=[], description=desc)
    assert DatabricksResult(cursor).description == desc


def test_databricks_fetch_record_batch_mixed_types():
    pytest.importorskip("pyarrow")

    cursor = _FakeCursor(rows=[(1, "x")], description=[("a", None), ("b", None)])
    reader = DatabricksResult(cursor).fetch_record_batch()
    table = reader.read_all()
    assert table.column(0).to_pylist() == [1]
    assert table.column(1).to_pylist() == ["x"]


def test_databricks_injection_attempt_in_table_name_is_rejected():
    """Verify SQL injection attempts in table names are rejected."""
    cursor = _FakeCursor()
    adapter = DatabricksAdapter.__new__(DatabricksAdapter)
    adapter.conn = _FakeConn(cursor)
    adapter.schema = "default"

    table_name = "orders; DROP TABLE users;--"
    with pytest.raises(ValueError, match="Invalid table name"):
        adapter.get_columns(table_name)

    # Verify no SQL was executed
    assert len(cursor.executed) == 0


@pytest.mark.parametrize(
    "schema",
    ["default; DROP SCHEMA x;--", "analytics; --", "schema'); DROP TABLE t;--"],
)
def test_databricks_injection_attempt_in_schema_is_rejected(schema):
    """Verify SQL injection attempts in schema names are rejected."""
    cursor = _FakeCursor()
    adapter = DatabricksAdapter.__new__(DatabricksAdapter)
    adapter.conn = _FakeConn(cursor)
    adapter.schema = None  # Force use of provided schema parameter

    with pytest.raises(ValueError, match="Invalid schema"):
        adapter.get_columns("orders", schema=schema)

    # Verify no SQL was executed
    assert len(cursor.executed) == 0


@pytest.mark.parametrize(
    "table_name",
    ["orders", "MY_TABLE", "Table123", "_private_table"],
)
def test_databricks_valid_table_names_accepted(table_name):
    """Verify valid table names are accepted."""
    cursor = _FakeCursor(rows=[("id", "int")])
    adapter = DatabricksAdapter.__new__(DatabricksAdapter)
    adapter.conn = _FakeConn(cursor)
    adapter.schema = None

    # Should not raise
    adapter.get_columns(table_name)
    assert len(cursor.executed) == 1
    assert f"DESCRIBE {table_name}" == cursor.executed[0]


@pytest.mark.parametrize(
    "schema",
    ["default", "my_schema", "Schema123", "_private", "analytics"],
)
def test_databricks_valid_schema_names_accepted(schema):
    """Verify valid schema names are accepted."""
    cursor = _FakeCursor(rows=[("id", "int")])
    adapter = DatabricksAdapter.__new__(DatabricksAdapter)
    adapter.conn = _FakeConn(cursor)
    adapter.schema = None

    # Should not raise
    adapter.get_columns("orders", schema=schema)
    assert len(cursor.executed) == 1
    assert f"DESCRIBE {schema}.orders" == cursor.executed[0]
