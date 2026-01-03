"""Tests for Spark adapter."""

import builtins
import sys

import pytest

from sidemantic.db.spark import SparkAdapter, SparkResult


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


def test_spark_adapter_import_error_message(monkeypatch):
    _block_import(monkeypatch, "pyhive")

    with pytest.raises(ImportError) as exc:
        SparkAdapter()

    message = str(exc.value).lower()
    assert "pyhive" in message or "spark" in message


def test_spark_from_url_matrix(monkeypatch):
    cases = [
        ("spark://host:10000/db", {"host": "host", "port": 10000, "database": "db"}),
        ("spark://host/db", {"port": 10000, "database": "db"}),
        ("spark://user:pass@host/db", {"username": "user", "password": "pass"}),
        ("spark://host", {"database": "default"}),
        ("spark://host:1234/analytics", {"port": 1234, "database": "analytics"}),
        ("spark://host:10000/db?auth=nosasl", {"auth": "nosasl"}),
        ("spark://host:10000/db?transportMode=http", {"transportMode": "http"}),
        ("spark://host:10000/db?ssl=true", {"ssl": "true"}),
        ("spark://host:10000/db?param=1", {"param": "1"}),
        ("spark://host:10000/db?param=1&param=2", {"param": ["1", "2"]}),
        ("spark://host:10000/db?http_path=/cliservice", {"http_path": "/cliservice"}),
        ("spark://host:10000/db?kerberos_service_name=hive", {"kerberos_service_name": "hive"}),
        ("spark://host:10000/db?auth=ldap", {"auth": "ldap"}),
        ("spark://host:10000/db?use_ssl=1", {"use_ssl": "1"}),
        ("spark://host:10000/db?session_conf=key%3Dvalue", {"session_conf": "key=value"}),
    ]

    for url, expected in cases:
        captured = {}

        def fake_init(self, **kwargs):
            captured.update(kwargs)

        monkeypatch.setattr(SparkAdapter, "__init__", fake_init)
        adapter = SparkAdapter.from_url(url)
        assert isinstance(adapter, SparkAdapter)

        for key, value in expected.items():
            assert captured[key] == value


def test_spark_from_url_invalid():
    with pytest.raises(ValueError, match="Invalid Spark URL"):
        SparkAdapter.from_url("http://host/db")


def test_spark_executemany_and_get_tables_sql():
    cursor = _FakeCursor(rows=[("default", "orders")])

    adapter = SparkAdapter.__new__(SparkAdapter)
    adapter.conn = _FakeConn(cursor)
    adapter.database = "default"

    adapter.executemany("INSERT INTO t VALUES (?)", [(1,), (2,)])
    assert cursor.executemany_args == ("INSERT INTO t VALUES (?)", [(1,), (2,)])

    tables = adapter.get_tables()
    assert tables == [{"table_name": "orders", "schema": "default"}]
    assert cursor.executed[0] == "SHOW TABLES IN default"


def test_spark_get_columns_sql_and_schema_override():
    cursor = _FakeCursor(rows=[("id", "int")])

    adapter = SparkAdapter.__new__(SparkAdapter)
    adapter.conn = _FakeConn(cursor)
    adapter.database = "default"

    cols = adapter.get_columns("orders", schema="analytics")
    assert cols == [{"column_name": "id", "data_type": "int"}]
    assert cursor.executed[0] == "DESCRIBE analytics.orders"


def test_spark_result_ordering_and_exhaustion():
    cursor = _FakeCursor(rows=[(1,), (2,)], description=[("a", None)])
    wrapper = SparkResult(cursor)
    assert wrapper.fetchone() == (1,)
    assert wrapper.fetchone() == (2,)
    assert wrapper.fetchone() is None


def test_spark_result_description_order():
    desc = [("b", "text"), ("a", "int")]
    cursor = _FakeCursor(rows=[], description=desc)
    assert SparkResult(cursor).description == desc


def test_spark_fetch_record_batch_mixed_types():
    pytest.importorskip("pyarrow")

    cursor = _FakeCursor(rows=[(1.5,)], description=[("a", None)])
    reader = SparkResult(cursor).fetch_record_batch()
    table = reader.read_all()
    assert table.column(0).to_pylist() == [1.5]


def test_spark_injection_surface_in_get_columns():
    cursor = _FakeCursor()
    adapter = SparkAdapter.__new__(SparkAdapter)
    adapter.conn = _FakeConn(cursor)
    adapter.database = "default"

    table_name = "orders; DROP TABLE users;--"
    adapter.get_columns(table_name)
    assert table_name in cursor.executed[0]
