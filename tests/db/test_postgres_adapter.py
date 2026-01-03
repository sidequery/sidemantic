"""Tests for PostgreSQL adapter."""

import pytest

from sidemantic.db.postgres import PostgreSQLAdapter, PostgresResult


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
        self.autocommit = False

    def cursor(self):
        return self._cursor


def test_postgres_adapter_import_error_message():
    try:
        PostgreSQLAdapter()
    except ImportError as exc:
        assert "psycopg" in str(exc).lower()


def test_postgres_from_url_matrix(monkeypatch):
    cases = [
        ("postgres://u:p@host:5432/db", {"host": "host", "port": 5432, "database": "db", "user": "u", "password": "p"}),
        ("postgresql://u@host/db", {"host": "host", "port": 5432, "database": "db", "user": "u", "password": None}),
        ("postgres://host/db", {"host": "host", "port": 5432, "database": "db", "user": None, "password": None}),
        ("postgres://host", {"host": "host", "port": 5432, "database": "postgres", "user": None, "password": None}),
        ("postgres://u:p@host:6543/db?sslmode=require", {"port": 6543, "sslmode": "require"}),
        ("postgresql://host:6543/db?application_name=app", {"port": 6543, "application_name": "app"}),
        ("postgres://u@host:5432/analytics", {"database": "analytics", "user": "u"}),
        ("postgresql://host:5432/", {"database": ""}),
        ("postgres://u:p@host", {"database": "postgres"}),
        ("postgresql://u:p@host:7777/db", {"port": 7777}),
        ("postgresql://u:p@host:7777/db?connect_timeout=5", {"connect_timeout": "5"}),
        ("postgres://u:p@host:5432/db?options=-c%20search_path%3Dpublic", {"options": "-c search_path=public"}),
        ("postgres://u@host:5432/db?sslmode=disable", {"sslmode": "disable"}),
        ("postgresql://host:5432/db?target_session_attrs=read-write", {"target_session_attrs": "read-write"}),
        ("postgresql://host:5432/db?keepalives=1", {"keepalives": "1"}),
        ("postgres://u:p@host:5432/db?keepalives_idle=10", {"keepalives_idle": "10"}),
        ("postgres://u:p@host:5432/db?keepalives_interval=5", {"keepalives_interval": "5"}),
        ("postgres://u:p@host:5432/db?keepalives_count=3", {"keepalives_count": "3"}),
        ("postgres://u:p@host:5432/db?sslmode=verify-full", {"sslmode": "verify-full"}),
        ("postgres://u:p@host:5432/db?options=-c%20statement_timeout%3D5000", {"options": "-c statement_timeout=5000"}),
    ]

    for url, expected in cases:
        captured = {}

        def fake_init(self, **kwargs):
            captured.update(kwargs)

        monkeypatch.setattr(PostgreSQLAdapter, "__init__", fake_init)
        adapter = PostgreSQLAdapter.from_url(url)
        assert isinstance(adapter, PostgreSQLAdapter)

        for key, value in expected.items():
            assert captured[key] == value


def test_postgres_from_url_invalid():
    with pytest.raises(ValueError, match="Invalid PostgreSQL URL"):
        PostgreSQLAdapter.from_url("mysql://host/db")


def test_postgres_executemany_and_get_tables_sql():
    cursor = _FakeCursor(rows=[("orders", "public")])
    adapter = PostgreSQLAdapter.__new__(PostgreSQLAdapter)
    adapter.conn = _FakeConn(cursor)

    adapter.executemany("INSERT INTO t VALUES (%s)", [(1,), (2,)])
    assert cursor.executemany_args == ("INSERT INTO t VALUES (%s)", [(1,), (2,)])

    tables = adapter.get_tables()
    assert tables == [{"table_name": "orders", "schema": "public"}]
    assert "information_schema.tables" in cursor.executed[0]


def test_postgres_get_columns_sql_and_schema_filter():
    cursor = _FakeCursor(rows=[("id", "integer")])
    adapter = PostgreSQLAdapter.__new__(PostgreSQLAdapter)
    adapter.conn = _FakeConn(cursor)

    cols = adapter.get_columns("orders", schema="public")
    assert cols == [{"column_name": "id", "data_type": "integer"}]
    assert "information_schema.columns" in cursor.executed[0]
    assert "table_schema = 'public'" in cursor.executed[0]


def test_postgres_get_columns_schema_optional():
    cursor = _FakeCursor(rows=[("id", "integer")])
    adapter = PostgreSQLAdapter.__new__(PostgreSQLAdapter)
    adapter.conn = _FakeConn(cursor)

    adapter.get_columns("orders")
    assert "table_schema" not in cursor.executed[0]


def test_postgres_get_tables_filters_catalog():
    cursor = _FakeCursor(rows=[("orders", "public")])
    adapter = PostgreSQLAdapter.__new__(PostgreSQLAdapter)
    adapter.conn = _FakeConn(cursor)

    adapter.get_tables()
    sql = cursor.executed[0]
    assert "information_schema" in sql
    assert "pg_catalog" in sql


def test_postgres_result_ordering_and_exhaustion():
    cursor = _FakeCursor(rows=[(1,), (2,)], description=[("a", None)])
    wrapper = PostgresResult(cursor)
    assert wrapper.fetchone() == (1,)
    assert wrapper.fetchone() == (2,)
    assert wrapper.fetchone() is None


def test_postgres_injection_surface_in_get_columns():
    cursor = _FakeCursor()
    adapter = PostgreSQLAdapter.__new__(PostgreSQLAdapter)
    adapter.conn = _FakeConn(cursor)

    table_name = "orders; DROP TABLE users;--"
    adapter.get_columns(table_name)
    assert table_name in cursor.executed[0]


@pytest.mark.parametrize(
    "schema",
    ["public; DROP SCHEMA x;--", "default; --", "analytics'); DROP TABLE t;--"],
)
def test_schema_inputs_are_unsanitized(schema):
    cursor = _FakeCursor()
    adapter = PostgreSQLAdapter.__new__(PostgreSQLAdapter)
    adapter.conn = _FakeConn(cursor)

    adapter.get_columns("orders", schema=schema)
    assert schema in cursor.executed[0]
