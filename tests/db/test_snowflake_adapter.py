"""Tests for Snowflake adapter."""

import pytest

from sidemantic.db.snowflake import SnowflakeAdapter, SnowflakeResult


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


def test_snowflake_adapter_import_error_message():
    import importlib.util

    if importlib.util.find_spec("snowflake.connector") is not None:
        pytest.skip("snowflake connector installed")

    try:
        SnowflakeAdapter()
    except ImportError as exc:
        assert "snowflake" in str(exc).lower()


def test_snowflake_from_url_matrix(monkeypatch):
    cases = [
        (
            "snowflake://u:p@acct/db/schema",
            {"account": "acct", "user": "u", "password": "p", "database": "db", "schema": "schema"},
        ),
        ("snowflake://u@acct/db", {"account": "acct", "user": "u", "database": "db", "schema": None}),
        ("snowflake://acct", {"account": "acct", "user": None, "password": None, "database": None, "schema": None}),
        ("snowflake://u:p@acct/db/schema?warehouse=WH", {"warehouse": "WH"}),
        ("snowflake://u:p@acct/db/schema?role=R", {"role": "R"}),
        ("snowflake://u:p@acct/db/schema?client_session_keep_alive=true", {"client_session_keep_alive": "true"}),
        ("snowflake://u:p@acct/db/schema?param=1", {"param": "1"}),
        ("snowflake://u:p@acct/db", {"schema": None}),
        ("snowflake://u:p@acct", {"database": None, "schema": None}),
        ("snowflake://u:p@acct/db/schema?warehouse=WH&role=R", {"warehouse": "WH", "role": "R"}),
        ("snowflake://u:p@acct/db/schema?region=us-east-1", {"region": "us-east-1"}),
        ("snowflake://u:p@acct/db/schema?authenticator=externalbrowser", {"authenticator": "externalbrowser"}),
        ("snowflake://u:p@acct/db/schema?client_session_keep_alive=1", {"client_session_keep_alive": "1"}),
    ]

    for url, expected in cases:
        captured = {}

        def fake_init(self, **kwargs):
            captured.update(kwargs)

        monkeypatch.setattr(SnowflakeAdapter, "__init__", fake_init)
        adapter = SnowflakeAdapter.from_url(url)
        assert isinstance(adapter, SnowflakeAdapter)

        for key, value in expected.items():
            assert captured[key] == value


def test_snowflake_from_url_invalid():
    with pytest.raises(ValueError, match="Invalid Snowflake URL"):
        SnowflakeAdapter.from_url("postgres://host/db")


def test_snowflake_executemany_and_get_tables_sql():
    cursor = _FakeCursor(rows=[("orders", "PUBLIC")])

    adapter = SnowflakeAdapter.__new__(SnowflakeAdapter)
    adapter.conn = _FakeConn(cursor)
    adapter.schema = None
    adapter.database = None

    adapter.executemany("INSERT INTO t VALUES (?)", [(1,), (2,)])
    assert cursor.executemany_args == ("INSERT INTO t VALUES (?)", [(1,), (2,)])

    tables = adapter.get_tables()
    assert tables == [{"table_name": "orders", "schema": "PUBLIC"}]
    assert "information_schema.tables" in cursor.executed[0]


def test_snowflake_get_columns_sql_and_schema_filter():
    cursor = _FakeCursor(rows=[("id", "NUMBER")])

    adapter = SnowflakeAdapter.__new__(SnowflakeAdapter)
    adapter.conn = _FakeConn(cursor)
    adapter.schema = "PUBLIC"

    cols = adapter.get_columns("orders")
    assert cols == [{"column_name": "id", "data_type": "NUMBER"}]
    assert "information_schema.columns" in cursor.executed[0]
    assert "table_schema = 'PUBLIC'" in cursor.executed[0]


def test_snowflake_query_history_sql():
    adapter = SnowflakeAdapter.__new__(SnowflakeAdapter)
    captured = {}

    class FakeResult:
        def fetchall(self):
            return [["select 1 -- sidemantic: y"], [None]]

    def fake_execute(sql):
        captured["sql"] = sql
        return FakeResult()

    adapter.execute = fake_execute

    results = adapter.get_query_history(days_back=5, limit=10)

    assert "INFORMATION_SCHEMA.QUERY_HISTORY" in captured["sql"]
    assert "-5" in captured["sql"]
    assert "LIMIT 10" in captured["sql"]
    assert results == ["select 1 -- sidemantic: y"]


def test_snowflake_result_ordering_and_exhaustion():
    cursor = _FakeCursor(rows=[(1,), (2,)], description=[("a", None)])
    wrapper = SnowflakeResult(cursor)
    assert wrapper.fetchone() == (1,)
    assert wrapper.fetchone() == (2,)
    assert wrapper.fetchone() is None


def test_snowflake_result_description_order():
    desc = [("b", "text"), ("a", "int")]
    cursor = _FakeCursor(rows=[], description=desc)
    assert SnowflakeResult(cursor).description == desc


def test_snowflake_fetch_record_batch_mixed_types():
    pa = pytest.importorskip("pyarrow")

    cursor = _FakeCursor(rows=[(1, None)], description=[("a", None), ("b", None)])
    reader = SnowflakeResult(cursor).fetch_record_batch()
    table = reader.read_all()
    assert table.column(1).to_pylist() == [None]
    assert isinstance(table, pa.Table)


def test_snowflake_injection_surface_in_get_columns():
    cursor = _FakeCursor()
    adapter = SnowflakeAdapter.__new__(SnowflakeAdapter)
    adapter.conn = _FakeConn(cursor)
    adapter.schema = "PUBLIC"

    table_name = "orders; DROP TABLE users;--"
    adapter.get_columns(table_name)
    assert table_name in cursor.executed[0]
