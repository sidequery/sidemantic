"""Tests for ClickHouse adapter."""

import builtins
import sys
from datetime import UTC, date, datetime
from decimal import Decimal
from io import BytesIO
from pathlib import Path

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
    assert "lower(query) NOT LIKE '%system.query_log%'" in captured["sql"]
    assert results == ["select 1 -- sidemantic: x"]

    adapter.get_query_history(days_back=3, limit=50, instrumented_only=False)
    assert "query LIKE '%-- sidemantic:%'" not in captured["sql"]


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


def test_clickhouse_execute_parquet():
    """Test that execute() uses Parquet fetching when available."""
    pa = pytest.importorskip("pyarrow")

    arrow_table = pa.table({"a": [1, 2], "b": ["x", "y"]})
    calls = []

    class FakeClient:
        def raw_query(self, sql, parameters=None, fmt=None):
            assert fmt == "Parquet"
            calls.append(("parquet", sql))
            return _parquet_bytes(arrow_table)

        def query(self, sql):
            calls.append(("row", sql))
            raise AssertionError("Should not be called when Parquet fetching succeeds")

    adapter = ClickHouseAdapter.__new__(ClickHouseAdapter)
    adapter.client = FakeClient()
    adapter.database = "default"

    result = adapter.execute("SELECT a, b FROM test")

    # Should have used Parquet
    assert len(calls) == 1
    assert calls[0][0] == "parquet"

    # Result should contain Arrow data
    reader = result.fetch_record_batch()
    result_table = reader.read_all()
    assert result_table.num_rows == 2
    assert result_table.column("a").to_pylist() == [1, 2]


def test_clickhouse_execute_fallback_to_query():
    """Clients without raw_query still support row-based results."""
    calls = []

    class FakeClient:
        def query(self, sql):
            calls.append(sql)
            return type("R", (), {"result_rows": [(1,)], "column_names": ["a"], "row_count": 1})()

    adapter = ClickHouseAdapter.__new__(ClickHouseAdapter)
    adapter.client = FakeClient()
    assert adapter.execute("SELECT 1").fetchone() == (1,)
    assert calls == ["SELECT 1"]


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


def test_clickhouse_executemany_parquet():
    """Test that executemany() uses Parquet fetching when available."""
    pa = pytest.importorskip("pyarrow")

    arrow_tables = [
        pa.table({"a": [1], "b": ["x"]}),
        pa.table({"a": [2], "b": ["y"]}),
    ]
    call_idx = [0]
    calls = []

    class FakeClient:
        def raw_query(self, sql, parameters=None, fmt=None):
            assert fmt == "Parquet"
            calls.append(("parquet", sql, parameters))
            result = arrow_tables[call_idx[0]]
            call_idx[0] += 1
            return _parquet_bytes(result)

        def query(self, sql, parameters=None):
            calls.append(("row", sql, parameters))
            raise AssertionError("Should not be called when Parquet fetching succeeds")

    adapter = ClickHouseAdapter.__new__(ClickHouseAdapter)
    adapter.client = FakeClient()
    adapter.database = "default"

    result = adapter.executemany("SELECT %(id)s", [{"id": 1}, {"id": 2}])

    # Should have used Parquet for both calls
    assert len(calls) == 2
    assert all(c[0] == "parquet" for c in calls)
    assert calls[0][2] == {"id": 1}
    assert calls[1][2] == {"id": 2}

    # Result should be the last one (Arrow)
    reader = result.fetch_record_batch()
    result_table = reader.read_all()
    assert result_table.num_rows == 1
    assert result_table.column("a").to_pylist() == [2]


def _parquet_bytes(table):
    pq = pytest.importorskip("pyarrow.parquet")
    buffer = BytesIO()
    pq.write_table(table, buffer)
    return buffer.getvalue()


@pytest.mark.parametrize("method", ["execute", "executemany"])
def test_clickhouse_temporal_results(method):
    """Decode actual ClickHouse output, including integer values resembling epochs."""
    pa = pytest.importorskip("pyarrow")
    payload = (Path(__file__).parents[1] / "fixtures/clickhouse/temporal_results.parquet").read_bytes()
    calls = []

    class FakeClient:
        def raw_query(self, sql, parameters=None, fmt=None):
            calls.append((sql, parameters, fmt))
            return payload

        def query(self, *args, **kwargs):
            pytest.fail("Typed Parquet results must not use the row fallback")

    adapter = ClickHouseAdapter.__new__(ClickHouseAdapter)
    adapter.client = FakeClient()
    sql = "SELECT temporal_columns"
    result = adapter.execute(sql) if method == "execute" else adapter.executemany(sql, [{"id": 1}, {"id": 2}])
    expected_parameters = [None] if method == "execute" else [{"id": 1}, {"id": 2}]
    assert calls == [(sql, parameters, "Parquet") for parameters in expected_parameters]

    table = result.fetch_record_batch().read_all()
    assert table.schema.field("day").type == pa.timestamp("ms", tz="UTC")
    assert table.schema.field("hour").type == pa.timestamp("ms", tz="UTC")
    assert table.schema.field("precise").type == pa.timestamp("us", tz="UTC")
    assert table.schema.field("week").type == pa.date32()
    assert table.schema.field("integer32").type == pa.uint32()
    assert table.schema.field("integer64").type == pa.uint64()
    assert pa.types.is_decimal(table.schema.field("amount").type)
    row = result.fetchone()
    assert row == (
        datetime(2026, 10, 12, tzinfo=UTC),
        datetime(2026, 10, 12, 17, tzinfo=UTC),
        datetime(2026, 10, 12, 13, 45, 56, 123456, tzinfo=UTC),
        date(2026, 10, 12),
        1791763200,
        18446744073709551615,
        Decimal("1234.56"),
        None,
        None,
    )
    assert row[0].isoformat() == "2026-10-12T00:00:00+00:00"
    assert row[1].isoformat() == "2026-10-12T17:00:00+00:00"


@pytest.mark.parametrize("method", ["execute", "executemany"])
def test_clickhouse_parquet_fallback(method, monkeypatch):
    calls = []
    timestamp = datetime(2026, 10, 12, tzinfo=UTC)

    class FakeClient:
        def raw_query(self, sql, parameters=None, fmt=None):
            pytest.fail("Missing PyArrow must be detected before execution")

        def query(self, sql, parameters=None):
            calls.append((sql, parameters))
            return type("R", (), {"result_rows": [(timestamp,)], "column_names": ["day"], "row_count": 1})()

    _block_import(monkeypatch, "pyarrow")
    adapter = ClickHouseAdapter.__new__(ClickHouseAdapter)
    adapter.client = FakeClient()
    sql = "SELECT day"
    result = adapter.execute(sql) if method == "execute" else adapter.executemany(sql, [{"id": 1}])
    assert result.fetchall() == [(timestamp,)]
    assert calls == [(sql, None if method == "execute" else {"id": 1})]


def test_clickhouse_temporal_results_json():
    pytest.importorskip("pyarrow")
    pytest.importorskip("mcp")
    from sidemantic.mcp_server import _convert_to_json_compatible

    payload = (Path(__file__).parents[1] / "fixtures/clickhouse/temporal_results.parquet").read_bytes()

    class FakeClient:
        def raw_query(self, sql, parameters=None, fmt=None):
            return payload

    adapter = ClickHouseAdapter.__new__(ClickHouseAdapter)
    adapter.client = FakeClient()
    result = adapter.execute("SELECT temporal_columns")
    values = [_convert_to_json_compatible(value) for value in result.fetchone()]
    assert values == [
        "2026-10-12T00:00:00+00:00",
        "2026-10-12T17:00:00+00:00",
        "2026-10-12T13:45:56.123456+00:00",
        "2026-10-12",
        1791763200,
        18446744073709551615,
        1234.56,
        None,
        None,
    ]


@pytest.mark.parametrize("method", ["execute", "executemany"])
@pytest.mark.parametrize("sql", ["CREATE TABLE rollup AS SELECT 1", "INSERT INTO rollup SELECT 1"])
def test_clickhouse_resultless_mutations_execute_once(method, sql):
    pytest.importorskip("pyarrow")
    mutations = []

    class FakeClient:
        def raw_query(self, statement, parameters=None, fmt=None):
            mutations.append((statement, parameters))
            return b""

        def query(self, statement, parameters=None):
            mutations.append((statement, parameters))
            pytest.fail("A completed mutation must not be replayed")

    adapter = ClickHouseAdapter.__new__(ClickHouseAdapter)
    adapter.client = FakeClient()
    params = [{"id": 1}, {"id": 2}]
    result = adapter.execute(sql) if method == "execute" else adapter.executemany(sql, params)
    assert mutations == [(sql, p) for p in ([None] if method == "execute" else params)]
    assert result.fetchone() is None
    assert result.fetchall() == []
    assert result.description == []
    assert result.fetch_record_batch().read_all().num_rows == 0


@pytest.mark.parametrize("method", ["execute", "executemany"])
@pytest.mark.parametrize("sql", ["INSERT INTO rollup SELECT 1", "SELECT day FROM rollup"])
@pytest.mark.parametrize("failure", ["decode", "transport"])
def test_clickhouse_post_execution_errors_are_not_retried(method, sql, failure):
    pa = pytest.importorskip("pyarrow")
    executions = []

    class FakeClient:
        def raw_query(self, statement, parameters=None, fmt=None):
            executions.append((statement, parameters))
            if failure == "transport":
                raise ConnectionError("Response lost after execution")
            return b"not parquet"

        def query(self, statement, parameters=None):
            executions.append((statement, parameters))
            pytest.fail("Execution outcome is uncertain; do not retry")

    adapter = ClickHouseAdapter.__new__(ClickHouseAdapter)
    adapter.client = FakeClient()
    with pytest.raises(ConnectionError if failure == "transport" else pa.ArrowInvalid):
        if method == "execute":
            adapter.execute(sql)
        else:
            adapter.executemany(sql, [{"id": 1}, {"id": 2}])
    assert executions == [(sql, None if method == "execute" else {"id": 1})]


@pytest.mark.parametrize(
    "options, expected_autogenerate",
    [
        ({}, False),
        ({"autogenerate_session_id": False}, False),
        ({"autogenerate_session_id": True}, True),
        ({"autogenerate_session_id": None}, None),
        ({"session_id": "explicit"}, False),
        ({"settings": {"session_id": "configured"}}, False),
    ],
)
def test_clickhouse_session_configuration(monkeypatch, options, expected_autogenerate):
    from types import SimpleNamespace

    captured = {}

    def get_client(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(params={})

    monkeypatch.setitem(sys.modules, "clickhouse_connect", SimpleNamespace(get_client=get_client))
    ClickHouseAdapter(**options)
    assert captured["autogenerate_session_id"] is expected_autogenerate
    for name, value in options.items():
        assert captured[name] == value


@pytest.mark.parametrize("arrow", [False, True])
def test_clickhouse_server_requests_overlap_without_closing_client(monkeypatch, arrow):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event
    from types import SimpleNamespace

    import pyarrow as pa

    from sidemantic.server.common import execute_bounded

    slow_started = Event()
    release_slow = Event()

    class Client:
        params = {}
        close_calls = 0

        def query_arrow(self, sql):
            if "slow" in sql:
                slow_started.set()
                assert release_slow.wait(5)
                return pa.table({"value": ["slow"]})
            return pa.table({"value": ["fast"]})

        def raw_query(self, sql, fmt, parameters=None):
            import pyarrow.parquet as pq

            assert parameters is None
            assert fmt == "Parquet"
            output = pa.BufferOutputStream()
            pq.write_table(self.query_arrow(sql), output)
            return output.getvalue().to_pybytes()

        def close(self):
            self.close_calls += 1

    client = Client()
    monkeypatch.setitem(sys.modules, "clickhouse_connect", SimpleNamespace(get_client=lambda **kwargs: client))
    adapter = ClickHouseAdapter()
    layer = SimpleNamespace(adapter=adapter, dialect=adapter.dialect)
    with ThreadPoolExecutor(max_workers=2) as pool:
        slow = pool.submit(execute_bounded, layer, "select 'slow' as value", arrow=arrow)
        try:
            assert slow_started.wait(5)
            fast = pool.submit(execute_bounded, layer, "select 'fast' as value", arrow=arrow)
            fast_result = fast.result(timeout=2)
            assert not slow.done()
            assert (fast_result.to_pylist() if arrow else fast_result) == [{"value": "fast"}]
            assert client.close_calls == 0
        finally:
            release_slow.set()
        slow_result = slow.result(timeout=5)
        assert (slow_result.to_pylist() if arrow else slow_result) == [{"value": "slow"}]
    assert client.close_calls == 0
    adapter.close()
    assert client.close_calls == 1


def test_clickhouse_session_requests_remain_serialized(monkeypatch):
    from concurrent.futures import ThreadPoolExecutor, TimeoutError
    from threading import Event
    from types import SimpleNamespace

    import pyarrow as pa

    from sidemantic.server.common import execute_bounded

    slow_started = Event()
    fast_submitted = Event()
    release_slow = Event()

    class Client:
        params = {"session_id": "explicit"}

        def query_arrow(self, sql):
            if "slow" in sql:
                slow_started.set()
                assert release_slow.wait(5)
            else:
                assert release_slow.is_set(), "Session queries must not overlap"
            return pa.table({"value": [sql]})

        def raw_query(self, sql, fmt, parameters=None):
            import pyarrow.parquet as pq

            assert parameters is None
            assert fmt == "Parquet"
            output = pa.BufferOutputStream()
            pq.write_table(self.query_arrow(sql), output)
            return output.getvalue().to_pybytes()

        def close(self):
            pytest.fail("Request must not close the shared client")

    monkeypatch.setitem(sys.modules, "clickhouse_connect", SimpleNamespace(get_client=lambda **kwargs: Client()))
    adapter = ClickHouseAdapter(session_id="explicit")
    layer = SimpleNamespace(adapter=adapter, dialect=adapter.dialect)

    def fast_query():
        fast_submitted.set()
        return execute_bounded(layer, "select 'fast' as value")

    with ThreadPoolExecutor(max_workers=2) as pool:
        slow = pool.submit(execute_bounded, layer, "select 'slow' as value")
        try:
            assert slow_started.wait(5)
            fast = pool.submit(fast_query)
            assert fast_submitted.wait(5)
            with pytest.raises(TimeoutError):
                fast.result(timeout=0.1)
        finally:
            release_slow.set()
        assert "slow" in slow.result(timeout=5)[0]["value"]
        assert "fast" in fast.result(timeout=5)[0]["value"]
