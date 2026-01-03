"""Tests for PostgreSQL adapter."""

import builtins
import sys

import pytest

from sidemantic.db.postgres import PostgreSQLAdapter, PostgresResult


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
        self.autocommit = False

    def cursor(self):
        return self._cursor


def test_postgres_adapter_import_error_message(monkeypatch):
    _block_import(monkeypatch, "psycopg")

    with pytest.raises(ImportError) as exc:
        PostgreSQLAdapter()

    assert "psycopg" in str(exc.value).lower()


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


def test_postgres_injection_attempt_in_table_name_is_rejected():
    """Verify SQL injection attempts in table names are rejected."""
    cursor = _FakeCursor()
    adapter = PostgreSQLAdapter.__new__(PostgreSQLAdapter)
    adapter.conn = _FakeConn(cursor)

    table_name = "orders; DROP TABLE users;--"
    with pytest.raises(ValueError, match="Invalid table name"):
        adapter.get_columns(table_name)

    # Verify no SQL was executed
    assert len(cursor.executed) == 0


@pytest.mark.parametrize(
    "schema",
    ["public; DROP SCHEMA x;--", "default; --", "analytics'); DROP TABLE t;--"],
)
def test_postgres_injection_attempt_in_schema_is_rejected(schema):
    """Verify SQL injection attempts in schema names are rejected."""
    cursor = _FakeCursor()
    adapter = PostgreSQLAdapter.__new__(PostgreSQLAdapter)
    adapter.conn = _FakeConn(cursor)

    with pytest.raises(ValueError, match="Invalid schema"):
        adapter.get_columns("orders", schema=schema)

    # Verify no SQL was executed
    assert len(cursor.executed) == 0


@pytest.mark.parametrize(
    "table_name",
    [
        "orders",
        "my_table",
        "Table123",
        "_private_table",
        "schema.table",
        "my_schema.my_table",
    ],
)
def test_postgres_valid_table_names_accepted(table_name):
    """Verify valid table names are accepted."""
    cursor = _FakeCursor(rows=[("id", "integer")])
    adapter = PostgreSQLAdapter.__new__(PostgreSQLAdapter)
    adapter.conn = _FakeConn(cursor)

    # Should not raise
    adapter.get_columns(table_name)
    assert len(cursor.executed) == 1
    assert table_name in cursor.executed[0]


@pytest.mark.parametrize(
    "schema",
    ["public", "my_schema", "Schema123", "_private", "analytics"],
)
def test_postgres_valid_schema_names_accepted(schema):
    """Verify valid schema names are accepted."""
    cursor = _FakeCursor(rows=[("id", "integer")])
    adapter = PostgreSQLAdapter.__new__(PostgreSQLAdapter)
    adapter.conn = _FakeConn(cursor)

    # Should not raise
    adapter.get_columns("orders", schema=schema)
    assert len(cursor.executed) == 1
    assert f"table_schema = '{schema}'" in cursor.executed[0]


def test_postgres_dialect():
    """Test dialect property returns postgres."""
    adapter = PostgreSQLAdapter.__new__(PostgreSQLAdapter)
    assert adapter.dialect == "postgres"


def test_postgres_raw_connection():
    """Test raw_connection property returns the underlying connection."""
    cursor = _FakeCursor()
    fake_conn = _FakeConn(cursor)
    adapter = PostgreSQLAdapter.__new__(PostgreSQLAdapter)
    adapter.conn = fake_conn

    assert adapter.raw_connection is fake_conn


def test_postgres_close():
    """Test close method calls close on connection."""

    class _FakeConnWithClose:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    adapter = PostgreSQLAdapter.__new__(PostgreSQLAdapter)
    fake_conn = _FakeConnWithClose()
    adapter.conn = fake_conn

    adapter.close()
    assert fake_conn.closed is True


def test_postgres_fetchone():
    """Test fetchone method on adapter."""
    cursor = _FakeCursor(rows=[(42, "test")])
    adapter = PostgreSQLAdapter.__new__(PostgreSQLAdapter)
    adapter.conn = _FakeConn(cursor)

    result = PostgresResult(cursor)
    row = adapter.fetchone(result)
    assert row == (42, "test")


def test_postgres_result_fetchall():
    """Test fetchall on PostgresResult."""
    cursor = _FakeCursor(rows=[(1,), (2,), (3,)])
    wrapper = PostgresResult(cursor)
    rows = wrapper.fetchall()
    assert rows == [(1,), (2,), (3,)]


def test_postgres_result_description():
    """Test description property on PostgresResult."""
    description = [("id", "integer"), ("name", "varchar")]
    cursor = _FakeCursor(description=description)
    wrapper = PostgresResult(cursor)
    assert wrapper.description == description


def test_postgres_execute_returns_result():
    """Test execute returns a PostgresResult wrapper."""
    cursor = _FakeCursor(rows=[(1,)])
    adapter = PostgreSQLAdapter.__new__(PostgreSQLAdapter)
    adapter.conn = _FakeConn(cursor)

    result = adapter.execute("SELECT 1")
    assert isinstance(result, PostgresResult)
    assert "SELECT 1" in cursor.executed


def test_postgres_fetch_record_batch():
    """Test fetch_record_batch on adapter."""
    pytest.importorskip("pyarrow")

    description = [type("Desc", (), {"name": "id"})(), type("Desc", (), {"name": "name"})()]
    cursor = _FakeCursor(rows=[(1, "a"), (2, "b")], description=description)
    adapter = PostgreSQLAdapter.__new__(PostgreSQLAdapter)
    adapter.conn = _FakeConn(cursor)

    result = PostgresResult(cursor)
    reader = adapter.fetch_record_batch(result)
    assert reader is not None


def test_postgres_result_fetch_record_batch_empty():
    """Test fetch_record_batch with empty result."""
    pytest.importorskip("pyarrow")

    description = [type("Desc", (), {"name": "id"})(), type("Desc", (), {"name": "name"})()]
    cursor = _FakeCursor(rows=[], description=description)
    wrapper = PostgresResult(cursor)
    reader = wrapper.fetch_record_batch()
    assert reader is not None


def test_postgres_get_tables_multiple_schemas():
    """Test get_tables returns tables from multiple schemas."""
    rows = [
        ("users", "public"),
        ("orders", "public"),
        ("logs", "analytics"),
    ]
    cursor = _FakeCursor(rows=rows)
    adapter = PostgreSQLAdapter.__new__(PostgreSQLAdapter)
    adapter.conn = _FakeConn(cursor)

    tables = adapter.get_tables()
    assert len(tables) == 3
    schemas = {t["schema"] for t in tables}
    assert "public" in schemas
    assert "analytics" in schemas
