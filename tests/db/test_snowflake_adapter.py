"""Tests for Snowflake adapter."""

import builtins
import sys

import pytest

from sidemantic.db.snowflake import SnowflakeAdapter, SnowflakeResult


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


def test_snowflake_adapter_import_error_message(monkeypatch):
    _block_import(monkeypatch, "snowflake")

    with pytest.raises(ImportError) as exc:
        SnowflakeAdapter()

    assert "snowflake" in str(exc.value).lower()


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


def test_snowflake_injection_attempt_in_table_name_is_rejected():
    """Verify SQL injection attempts in table names are rejected."""
    cursor = _FakeCursor()
    adapter = SnowflakeAdapter.__new__(SnowflakeAdapter)
    adapter.conn = _FakeConn(cursor)
    adapter.schema = "PUBLIC"

    table_name = "orders; DROP TABLE users;--"
    with pytest.raises(ValueError, match="Invalid table name"):
        adapter.get_columns(table_name)

    # Verify no SQL was executed
    assert len(cursor.executed) == 0


@pytest.mark.parametrize(
    "schema",
    ["PUBLIC; DROP SCHEMA x;--", "default; --", "ANALYTICS'); DROP TABLE t;--"],
)
def test_snowflake_injection_attempt_in_schema_is_rejected(schema):
    """Verify SQL injection attempts in schema names are rejected."""
    cursor = _FakeCursor()
    adapter = SnowflakeAdapter.__new__(SnowflakeAdapter)
    adapter.conn = _FakeConn(cursor)
    adapter.schema = None  # Force use of provided schema parameter

    with pytest.raises(ValueError, match="Invalid schema"):
        adapter.get_columns("orders", schema=schema)

    # Verify no SQL was executed
    assert len(cursor.executed) == 0


@pytest.mark.parametrize(
    "table_name",
    [
        "orders",
        "MY_TABLE",
        "Table123",
        "_private_table",
        "schema.table",
        "MY_SCHEMA.MY_TABLE",
    ],
)
def test_snowflake_valid_table_names_accepted(table_name):
    """Verify valid table names are accepted."""
    cursor = _FakeCursor(rows=[("id", "NUMBER")])
    adapter = SnowflakeAdapter.__new__(SnowflakeAdapter)
    adapter.conn = _FakeConn(cursor)
    adapter.schema = None

    # Should not raise
    adapter.get_columns(table_name)
    assert len(cursor.executed) == 1
    assert table_name in cursor.executed[0]


@pytest.mark.parametrize(
    "schema",
    ["PUBLIC", "my_schema", "Schema123", "_PRIVATE", "ANALYTICS"],
)
def test_snowflake_valid_schema_names_accepted(schema):
    """Verify valid schema names are accepted."""
    cursor = _FakeCursor(rows=[("id", "NUMBER")])
    adapter = SnowflakeAdapter.__new__(SnowflakeAdapter)
    adapter.conn = _FakeConn(cursor)
    adapter.schema = None

    # Should not raise
    adapter.get_columns("orders", schema=schema)
    assert len(cursor.executed) == 1
    assert f"table_schema = '{schema}'" in cursor.executed[0]


def test_snowflake_dialect():
    """Test dialect property returns snowflake."""
    adapter = SnowflakeAdapter.__new__(SnowflakeAdapter)
    assert adapter.dialect == "snowflake"


def test_snowflake_raw_connection():
    """Test raw_connection property returns the underlying connection."""
    cursor = _FakeCursor()
    fake_conn = _FakeConn(cursor)
    adapter = SnowflakeAdapter.__new__(SnowflakeAdapter)
    adapter.conn = fake_conn

    assert adapter.raw_connection is fake_conn


def test_snowflake_close():
    """Test close method calls close on connection."""

    class _FakeConnWithClose:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    adapter = SnowflakeAdapter.__new__(SnowflakeAdapter)
    fake_conn = _FakeConnWithClose()
    adapter.conn = fake_conn

    adapter.close()
    assert fake_conn.closed is True


def test_snowflake_fetchone():
    """Test fetchone method on adapter."""
    cursor = _FakeCursor(rows=[(42, "test")])
    adapter = SnowflakeAdapter.__new__(SnowflakeAdapter)
    adapter.conn = _FakeConn(cursor)

    result = SnowflakeResult(cursor)
    row = adapter.fetchone(result)
    assert row == (42, "test")


def test_snowflake_result_fetchall():
    """Test fetchall on SnowflakeResult."""
    cursor = _FakeCursor(rows=[(1,), (2,), (3,)])
    wrapper = SnowflakeResult(cursor)
    rows = wrapper.fetchall()
    assert rows == [(1,), (2,), (3,)]


def test_snowflake_execute_returns_result():
    """Test execute returns a SnowflakeResult wrapper."""
    cursor = _FakeCursor(rows=[(1,)])
    adapter = SnowflakeAdapter.__new__(SnowflakeAdapter)
    adapter.conn = _FakeConn(cursor)

    result = adapter.execute("SELECT 1")
    assert isinstance(result, SnowflakeResult)
    assert "SELECT 1" in cursor.executed


def test_snowflake_get_tables_with_schema_filter():
    """Test get_tables when schema is set filters to that schema."""
    cursor = _FakeCursor(rows=[("orders", "PUBLIC")])

    adapter = SnowflakeAdapter.__new__(SnowflakeAdapter)
    adapter.conn = _FakeConn(cursor)
    adapter.schema = "PUBLIC"
    adapter.database = "MYDB"

    tables = adapter.get_tables()
    assert tables == [{"table_name": "orders", "schema": "PUBLIC"}]
    sql = cursor.executed[0]
    assert "table_schema = 'PUBLIC'" in sql


def test_snowflake_get_columns_with_explicit_schema():
    """Test get_columns with explicitly provided schema overrides adapter schema."""
    cursor = _FakeCursor(rows=[("id", "NUMBER")])

    adapter = SnowflakeAdapter.__new__(SnowflakeAdapter)
    adapter.conn = _FakeConn(cursor)
    adapter.schema = "DEFAULT_SCHEMA"

    cols = adapter.get_columns("orders", schema="OVERRIDE_SCHEMA")
    assert cols == [{"column_name": "id", "data_type": "NUMBER"}]
    assert "table_schema = 'OVERRIDE_SCHEMA'" in cursor.executed[0]


def test_snowflake_get_columns_no_schema():
    """Test get_columns when no schema is set."""
    cursor = _FakeCursor(rows=[("id", "NUMBER")])

    adapter = SnowflakeAdapter.__new__(SnowflakeAdapter)
    adapter.conn = _FakeConn(cursor)
    adapter.schema = None

    adapter.get_columns("orders")
    # Should not have schema filter when schema is None
    assert "table_schema" not in cursor.executed[0]


def test_snowflake_fetch_record_batch_empty():
    """Test fetch_record_batch with empty result."""
    pytest.importorskip("pyarrow")

    cursor = _FakeCursor(rows=[], description=[("a", None), ("b", None)])
    reader = SnowflakeResult(cursor).fetch_record_batch()
    table = reader.read_all()
    assert len(table) == 0
    assert table.num_columns == 2


def test_snowflake_get_tables_multiple():
    """Test get_tables returns multiple tables."""
    rows = [
        ("users", "PUBLIC"),
        ("orders", "PUBLIC"),
        ("logs", "ANALYTICS"),
    ]
    cursor = _FakeCursor(rows=rows)

    adapter = SnowflakeAdapter.__new__(SnowflakeAdapter)
    adapter.conn = _FakeConn(cursor)
    adapter.schema = None
    adapter.database = "MYDB"

    tables = adapter.get_tables()
    assert len(tables) == 3
    table_names = {t["table_name"] for t in tables}
    assert "users" in table_names
    assert "orders" in table_names
    assert "logs" in table_names
