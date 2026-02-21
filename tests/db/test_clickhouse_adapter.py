"""Tests for ClickHouse adapter."""

import builtins
import sys

import pytest

from sidemantic.db.clickhouse import ClickHouseAdapter, ClickHouseResult


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


def test_clickhouse_adapter_import_error_message(monkeypatch):
    _block_import(monkeypatch, "clickhouse_connect")

    with pytest.raises(ImportError) as exc:
        ClickHouseAdapter()

    assert "clickhouse" in str(exc.value).lower()


def test_clickhouse_from_url_matrix(monkeypatch):
    cases = [
        (
            "clickhouse://u:p@host:8123/db",
            {"host": "host", "port": 8123, "database": "db", "user": "u", "password": "p"},
        ),
        ("clickhouse://host/db", {"host": "host", "port": 8123, "database": "db", "user": "default", "password": ""}),
        ("clickhouse://host", {"database": "default"}),
        ("clickhouse://u@host/db", {"user": "u"}),
        ("clickhouse://u:p@host:9000/db", {"port": 9000}),
        ("clickhouse://host/db?secure=true", {"secure": True}),
        ("clickhouse://host/db?secure=false", {"secure": False}),
        ("clickhouse://host/db?secure=1", {"secure": True}),
        ("clickhouse://host/db?secure=yes", {"secure": True}),
        ("clickhouse://host/db?secure=no", {"secure": False}),
        ("clickhouse://host/db?compression=lz4", {"compression": "lz4"}),
        ("clickhouse://host/db?query_id=abc", {"query_id": "abc"}),
        ("clickhouse://host/db?readonly=1", {"readonly": "1"}),
        ("clickhouse://host/db?max_execution_time=30", {"max_execution_time": "30"}),
        ("clickhouse://host/db?send_timeout=10", {"send_timeout": "10"}),
        ("clickhouse://host/db?receive_timeout=20", {"receive_timeout": "20"}),
        ("clickhouse://host/db?client_name=test", {"client_name": "test"}),
        ("clickhouse://host/db?compress=0", {"compress": "0"}),
        ("clickhouse://host/db?settings=max_threads%3D4", {"settings": "max_threads=4"}),
    ]

    for url, expected in cases:
        captured = {}

        def fake_init(self, **kwargs):
            captured.update(kwargs)

        monkeypatch.setattr(ClickHouseAdapter, "__init__", fake_init)
        adapter = ClickHouseAdapter.from_url(url)
        assert isinstance(adapter, ClickHouseAdapter)

        for key, value in expected.items():
            assert captured[key] == value


def test_clickhouse_from_url_invalid():
    with pytest.raises(ValueError, match="Invalid ClickHouse URL"):
        ClickHouseAdapter.from_url("http://host/db")


def test_clickhouse_executemany_and_get_tables_sql():
    calls = []

    class FakeQueryResult:
        def __init__(self):
            self.result_rows = [("orders", "default")]
            self.column_names = ["name", "database"]
            self.row_count = len(self.result_rows)

    class FakeClient:
        def query(self, sql, parameters=None):
            calls.append((sql, parameters))
            return FakeQueryResult()

    adapter = ClickHouseAdapter.__new__(ClickHouseAdapter)
    adapter.client = FakeClient()
    adapter.database = "default"

    adapter.executemany("SELECT %(id)s", [{"id": 1}, {"id": 2}])
    assert calls[0][0].startswith("SELECT")

    tables = adapter.get_tables()
    assert tables == [{"table_name": "orders", "schema": "default"}]
    assert "system.tables" in calls[-1][0]


def test_clickhouse_executemany_empty_params_uses_select_1():
    calls = []

    class FakeQueryResult:
        def __init__(self):
            self.result_rows = [(1,)]
            self.column_names = ["ok"]
            self.row_count = 1

    class FakeClient:
        def query(self, sql, parameters=None):
            calls.append((sql, parameters))
            return FakeQueryResult()

    adapter = ClickHouseAdapter.__new__(ClickHouseAdapter)
    adapter.client = FakeClient()
    adapter.database = "default"

    adapter.executemany("SELECT 1", [])
    assert calls[-1][0] == "SELECT 1"


def test_clickhouse_executemany_returns_last_result():
    class FakeQueryResult:
        def __init__(self, label):
            self.result_rows = [(label,)]
            self.column_names = ["label"]
            self.row_count = 1

    class FakeClient:
        def __init__(self):
            self.count = 0

        def query(self, sql, parameters=None):
            self.count += 1
            return FakeQueryResult(f"r{self.count}")

    adapter = ClickHouseAdapter.__new__(ClickHouseAdapter)
    adapter.client = FakeClient()
    adapter.database = "default"

    result = adapter.executemany("SELECT %(id)s", [{"id": 1}, {"id": 2}, {"id": 3}])
    assert result.fetchall() == [("r3",)]


def test_clickhouse_get_columns_sql_and_schema_override():
    calls = []

    class FakeQueryResult:
        def __init__(self):
            self.result_rows = [("id", "Int64")]
            self.column_names = ["name", "type"]
            self.row_count = 1

    class FakeClient:
        def query(self, sql, parameters=None):
            calls.append((sql, parameters))
            return FakeQueryResult()

    adapter = ClickHouseAdapter.__new__(ClickHouseAdapter)
    adapter.client = FakeClient()
    adapter.database = "default"

    cols = adapter.get_columns("orders", schema="analytics")
    assert cols == [{"column_name": "id", "data_type": "Int64"}]
    assert "system.columns" in calls[-1][0]
    assert calls[-1][1]["schema"] == "analytics"


def test_clickhouse_query_history_sql():
    adapter = ClickHouseAdapter.__new__(ClickHouseAdapter)
    captured = {}

    class FakeResult:
        result_rows = [["select 1 -- sidemantic: x"], [None]]

    class FakeClient:
        def query(self, sql):
            captured["sql"] = sql
            return FakeResult()

    adapter.client = FakeClient()

    results = adapter.get_query_history(days_back=3, limit=50)

    assert "system.query_log" in captured["sql"]
    assert "INTERVAL 3 DAY" in captured["sql"]
    assert "LIMIT 50" in captured["sql"]
    assert results == ["select 1 -- sidemantic: x"]


def test_clickhouse_result_ordering_and_exhaustion():
    result = ClickHouseResult(
        type("R", (), {"result_rows": [(1,), (2,), (3,)], "row_count": 3, "column_names": ["a"]})()
    )

    assert result.fetchone() == (1,)
    assert result.fetchone() == (2,)
    assert result.fetchall() == [(3,)]
    assert result.fetchone() is None


def test_clickhouse_result_description_order():
    wrapper = ClickHouseResult(type("R", (), {"result_rows": [], "row_count": 0, "column_names": ["b", "a"]})())
    assert wrapper.description == [("b", None), ("a", None)]


def test_clickhouse_get_columns_uses_parameterized_queries():
    """Verify ClickHouse uses parameterized queries for get_columns (safe from injection)."""
    calls = []

    class FakeQueryResult:
        def __init__(self):
            self.result_rows = [("id", "Int64")]
            self.column_names = ["name", "type"]
            self.row_count = 1

    class FakeClient:
        def query(self, sql, parameters=None):
            calls.append((sql, parameters))
            return FakeQueryResult()

    adapter = ClickHouseAdapter.__new__(ClickHouseAdapter)
    adapter.client = FakeClient()
    adapter.database = "default"

    # Even with malicious input, it's passed as a parameter, not interpolated
    table_name = "orders; DROP TABLE users;--"
    adapter.get_columns(table_name)

    # Verify parameterized query is used (safe from SQL injection)
    assert calls[-1][1]["table"] == table_name
    # The SQL itself should use placeholders, not string interpolation
    assert "%(table)s" in calls[-1][0] or ":table" in calls[-1][0] or "table = " in calls[-1][0]


def test_clickhouse_execute_native_arrow():
    """Test that execute() uses native Arrow fetching when available."""
    pa = pytest.importorskip("pyarrow")

    arrow_table = pa.table({"a": [1, 2], "b": ["x", "y"]})
    calls = []

    class FakeClient:
        def query_arrow(self, sql):
            calls.append(("arrow", sql))
            return arrow_table

        def query(self, sql):
            calls.append(("row", sql))
            raise AssertionError("Should not be called when query_arrow succeeds")

    adapter = ClickHouseAdapter.__new__(ClickHouseAdapter)
    adapter.client = FakeClient()
    adapter.database = "default"

    result = adapter.execute("SELECT a, b FROM test")

    # Should have used query_arrow
    assert len(calls) == 1
    assert calls[0][0] == "arrow"

    # Result should contain Arrow data
    reader = result.fetch_record_batch()
    result_table = reader.read_all()
    assert result_table.num_rows == 2
    assert result_table.column("a").to_pylist() == [1, 2]


def test_clickhouse_execute_fallback_to_query():
    """Test that execute() falls back to query() when query_arrow fails."""
    calls = []

    class FakeQueryResult:
        def __init__(self):
            self.result_rows = [(1, "x")]
            self.column_names = ["a", "b"]
            self.row_count = 1

    class FakeClient:
        def query_arrow(self, sql):
            calls.append(("arrow", sql))
            raise Exception("Arrow not supported")

        def query(self, sql):
            calls.append(("row", sql))
            return FakeQueryResult()

    adapter = ClickHouseAdapter.__new__(ClickHouseAdapter)
    adapter.client = FakeClient()
    adapter.database = "default"

    result = adapter.execute("SELECT a, b FROM test")

    # Should have tried arrow first, then fallen back to row
    assert len(calls) == 2
    assert calls[0][0] == "arrow"
    assert calls[1][0] == "row"

    # Result should still work via fallback
    assert result.fetchone() == (1, "x")


def test_clickhouse_result_native_arrow_fetch_record_batch():
    """Test ClickHouseResult.fetch_record_batch() with native Arrow data."""
    pa = pytest.importorskip("pyarrow")

    arrow_table = pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    result = ClickHouseResult(result=None, arrow_table=arrow_table)

    reader = result.fetch_record_batch()
    result_table = reader.read_all()

    assert result_table.num_rows == 3
    assert result_table.column("a").to_pylist() == [1, 2, 3]
    assert result_table.column("b").to_pylist() == ["x", "y", "z"]


def test_clickhouse_result_native_arrow_fetchone():
    """Test ClickHouseResult.fetchone() with native Arrow data."""
    pa = pytest.importorskip("pyarrow")

    arrow_table = pa.table({"a": [1, 2], "b": ["x", "y"]})
    result = ClickHouseResult(result=None, arrow_table=arrow_table)

    assert result.fetchone() == (1, "x")
    assert result.fetchone() == (2, "y")
    assert result.fetchone() is None


def test_clickhouse_result_native_arrow_fetchall():
    """Test ClickHouseResult.fetchall() with native Arrow data."""
    pa = pytest.importorskip("pyarrow")

    arrow_table = pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    result = ClickHouseResult(result=None, arrow_table=arrow_table)

    # Fetch one first, then fetchall should get remaining
    result.fetchone()
    remaining = result.fetchall()

    assert remaining == [(2, "y"), (3, "z")]


def test_clickhouse_result_native_arrow_empty():
    """Test ClickHouseResult with empty native Arrow data."""
    pa = pytest.importorskip("pyarrow")

    arrow_table = pa.table({"a": pa.array([], type=pa.int64()), "b": pa.array([], type=pa.string())})
    result = ClickHouseResult(result=None, arrow_table=arrow_table)

    assert result.fetchone() is None
    assert result.fetchall() == []

    reader = result.fetch_record_batch()
    result_table = reader.read_all()
    assert result_table.num_rows == 0


def test_clickhouse_executemany_native_arrow():
    """Test that executemany() uses native Arrow fetching when available."""
    pa = pytest.importorskip("pyarrow")

    arrow_tables = [
        pa.table({"a": [1], "b": ["x"]}),
        pa.table({"a": [2], "b": ["y"]}),
    ]
    call_idx = [0]
    calls = []

    class FakeClient:
        def query_arrow(self, sql, parameters=None):
            calls.append(("arrow", sql, parameters))
            result = arrow_tables[call_idx[0]]
            call_idx[0] += 1
            return result

        def query(self, sql, parameters=None):
            calls.append(("row", sql, parameters))
            raise AssertionError("Should not be called when query_arrow succeeds")

    adapter = ClickHouseAdapter.__new__(ClickHouseAdapter)
    adapter.client = FakeClient()
    adapter.database = "default"

    result = adapter.executemany("SELECT %(id)s", [{"id": 1}, {"id": 2}])

    # Should have used query_arrow for both calls
    assert len(calls) == 2
    assert all(c[0] == "arrow" for c in calls)
    assert calls[0][2] == {"id": 1}
    assert calls[1][2] == {"id": 2}

    # Result should be the last one (Arrow)
    reader = result.fetch_record_batch()
    result_table = reader.read_all()
    assert result_table.num_rows == 1
    assert result_table.column("a").to_pylist() == [2]
