"""Tests for ClickHouse adapter."""

import pytest

from sidemantic.db.clickhouse import ClickHouseAdapter, ClickHouseResult


def test_clickhouse_adapter_import_error_message():
    try:
        ClickHouseAdapter()
    except ImportError as exc:
        assert "clickhouse" in str(exc).lower()


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


def test_clickhouse_injection_surface_in_get_columns():
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

    table_name = "orders; DROP TABLE users;--"
    adapter.get_columns(table_name)
    assert table_name in calls[-1][1]["table"]
