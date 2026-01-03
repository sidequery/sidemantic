"""Tests for BigQuery adapter."""

import builtins
import sys
from types import SimpleNamespace

import pytest

from sidemantic.db.bigquery import BigQueryAdapter, BigQueryResult


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


def test_bigquery_adapter_import_error_message(monkeypatch):
    _block_import(monkeypatch, "google.cloud")

    with pytest.raises(ImportError) as exc:
        BigQueryAdapter()

    assert "bigquery" in str(exc.value).lower()


def test_bigquery_from_url_matrix(monkeypatch):
    cases = [
        ("bigquery://proj/dataset", {"project_id": "proj", "dataset_id": "dataset"}),
        ("bigquery://proj", {"project_id": "proj", "dataset_id": None}),
        ("bigquery://proj/", {"project_id": "proj", "dataset_id": ""}),
        ("bigquery://proj/ds1", {"dataset_id": "ds1"}),
        ("bigquery://proj/analytics", {"dataset_id": "analytics"}),
        ("bigquery://proj/ds-1", {"dataset_id": "ds-1"}),
        ("bigquery://proj/ds_1", {"dataset_id": "ds_1"}),
        ("bigquery://proj/123", {"dataset_id": "123"}),
        ("bigquery://proj/ds", {"dataset_id": "ds"}),
        ("bigquery://proj/dataset/ignored", {"project_id": "proj", "dataset_id": "dataset"}),
        ("bigquery://project-x/dataset-y", {"project_id": "project-x", "dataset_id": "dataset-y"}),
        ("bigquery://project123/dataset123", {"project_id": "project123", "dataset_id": "dataset123"}),
        ("bigquery://p/d", {"project_id": "p", "dataset_id": "d"}),
        ("bigquery://project/ds__1", {"project_id": "project", "dataset_id": "ds__1"}),
        ("bigquery://proj/ds.with.dot", {"project_id": "proj", "dataset_id": "ds.with.dot"}),
    ]

    for url, expected in cases:
        captured = {}

        def fake_init(self, **kwargs):
            captured.update(kwargs)

        monkeypatch.setattr(BigQueryAdapter, "__init__", fake_init)
        adapter = BigQueryAdapter.from_url(url)
        assert isinstance(adapter, BigQueryAdapter)

        for key, value in expected.items():
            assert captured[key] == value


def test_bigquery_from_url_invalid():
    with pytest.raises(ValueError, match="Invalid BigQuery URL"):
        BigQueryAdapter.from_url("postgres://invalid")

    with pytest.raises(ValueError, match="project_id"):
        BigQueryAdapter.from_url("bigquery://")


def test_bigquery_get_columns_requires_schema():
    adapter = BigQueryAdapter.__new__(BigQueryAdapter)
    adapter.dataset_id = None

    with pytest.raises(ValueError, match="schema .* required"):
        adapter.get_columns("orders")


def test_bigquery_injection_attempt_in_table_name_is_rejected():
    """Verify SQL injection attempts in table names are rejected."""
    adapter = BigQueryAdapter.__new__(BigQueryAdapter)
    adapter.dataset_id = "my_dataset"

    table_name = "orders; DROP TABLE users;--"
    with pytest.raises(ValueError, match="Invalid table name"):
        adapter.get_columns(table_name)


@pytest.mark.parametrize(
    "schema",
    ["my_dataset; DROP SCHEMA x;--", "default; --", "analytics'); DROP TABLE t;--"],
)
def test_bigquery_injection_attempt_in_schema_is_rejected(schema):
    """Verify SQL injection attempts in schema names are rejected."""
    adapter = BigQueryAdapter.__new__(BigQueryAdapter)
    adapter.dataset_id = None  # Force use of provided schema parameter

    with pytest.raises(ValueError, match="Invalid schema"):
        adapter.get_columns("orders", schema=schema)


@pytest.mark.parametrize(
    "table_name",
    ["orders", "my_table", "Table123", "_private_table"],
)
def test_bigquery_valid_table_names_accepted(table_name):
    """Verify valid table names are accepted."""
    adapter = BigQueryAdapter.__new__(BigQueryAdapter)

    class FakeField:
        def __init__(self, name, field_type, mode="NULLABLE"):
            self.name = name
            self.field_type = field_type
            self.mode = mode

    class FakeClient:
        def dataset(self, dataset_id):
            return SimpleNamespace(table=lambda name: (dataset_id, name))

        def get_table(self, table_ref):
            return SimpleNamespace(schema=[FakeField("id", "INT64")])

    adapter.client = FakeClient()
    adapter.dataset_id = "my_dataset"

    # Should not raise
    cols = adapter.get_columns(table_name)
    assert len(cols) == 1


@pytest.mark.parametrize(
    "schema",
    ["my_dataset", "analytics", "Schema123", "_private"],
)
def test_bigquery_valid_schema_names_accepted(schema):
    """Verify valid schema names are accepted."""
    adapter = BigQueryAdapter.__new__(BigQueryAdapter)

    class FakeField:
        def __init__(self, name, field_type, mode="NULLABLE"):
            self.name = name
            self.field_type = field_type
            self.mode = mode

    class FakeClient:
        def dataset(self, dataset_id):
            return SimpleNamespace(table=lambda name: (dataset_id, name))

        def get_table(self, table_ref):
            return SimpleNamespace(schema=[FakeField("id", "INT64")])

    adapter.client = FakeClient()
    adapter.dataset_id = None

    # Should not raise
    cols = adapter.get_columns("orders", schema=schema)
    assert len(cols) == 1


def test_bigquery_get_tables_and_columns():
    adapter = BigQueryAdapter.__new__(BigQueryAdapter)

    class FakeTable:
        def __init__(self, table_id):
            self.table_id = table_id

    class FakeDataset:
        def __init__(self, dataset_id):
            self.dataset_id = dataset_id

    class FakeField:
        def __init__(self, name, field_type, mode="NULLABLE"):
            self.name = name
            self.field_type = field_type
            self.mode = mode

    class FakeClient:
        def __init__(self):
            self.project = "proj"
            self.location = "US"

        def list_datasets(self):
            return [FakeDataset("a"), FakeDataset("b")]

        def dataset(self, dataset_id):
            return SimpleNamespace(dataset_id=dataset_id, table=lambda name: (dataset_id, name))

        def list_tables(self, dataset_ref):
            return [FakeTable("t1"), FakeTable("t2")]

        def get_table(self, table_ref):
            assert table_ref == ("a", "orders")
            return SimpleNamespace(schema=[FakeField("id", "INT64", mode="REQUIRED")])

    adapter.client = FakeClient()
    adapter.project_id = "proj"
    adapter.dataset_id = None

    tables = adapter.get_tables()
    assert {t["table_name"] for t in tables} == {"t1", "t2"}
    assert {t["schema"] for t in tables} == {"a", "b"}

    adapter.dataset_id = "a"
    cols = adapter.get_columns("orders")
    assert cols == [{"column_name": "id", "data_type": "INT64", "is_nullable": False}]


def test_bigquery_query_history_sql():
    adapter = BigQueryAdapter.__new__(BigQueryAdapter)
    adapter.project_id = "proj"

    class FakeClient:
        location = "US"

    adapter.client = FakeClient()

    captured = {}

    class FakeResult:
        def fetchall(self):
            return [["select 1 -- sidemantic: ok"], [None]]

    def fake_execute(sql):
        captured["sql"] = sql
        return FakeResult()

    adapter.execute = fake_execute

    results = adapter.get_query_history(days_back=4, limit=9)
    assert "INFORMATION_SCHEMA.JOBS_BY_PROJECT" in captured["sql"]
    assert "INTERVAL 4 DAY" in captured["sql"]
    assert "LIMIT 9" in captured["sql"]
    assert results == ["select 1 -- sidemantic: ok"]


def test_bigquery_result_fetchone_fetchall():
    class FakeRow:
        def __init__(self, **kwargs):
            self._data = kwargs

        def values(self):
            return self._data.values()

    class FakeResult:
        schema = [SimpleNamespace(name="a", field_type="INT64")]

        def __iter__(self):
            return iter([FakeRow(a=1), FakeRow(a=2), FakeRow(a=3)])

        def to_arrow(self):
            return None

    class FakeJob:
        def result(self):
            return FakeResult()

    result = BigQueryResult(FakeJob())
    assert result.fetchone() == (1,)
    assert result.fetchall() == [(2,), (3,)]


def test_bigquery_fetch_record_batch():
    pa = pytest.importorskip("pyarrow")

    class FakeResult:
        schema = [SimpleNamespace(name="id", field_type="INT64")]

        def __iter__(self):
            return iter([])

        def to_arrow(self):
            return pa.table({"id": [1]})

    class FakeJob:
        def result(self):
            return FakeResult()

    reader = BigQueryResult(FakeJob()).fetch_record_batch()
    assert isinstance(reader, pa.RecordBatchReader)


def test_bigquery_executemany_passes_parameter_sets():
    adapter = BigQueryAdapter.__new__(BigQueryAdapter)
    calls = []

    class FakeResult:
        schema = []

        def __iter__(self):
            return iter([])

        def to_arrow(self):
            return None

    class FakeJob:
        def result(self):
            return FakeResult()

    class FakeClient:
        def query(self, sql, job_config=None):
            calls.append(job_config)
            return FakeJob()

    adapter.client = FakeClient()

    params = [[{"name": "a", "parameterType": "INT64", "parameterValue": 1}], [{"name": "a", "parameterValue": 2}]]
    adapter.executemany("SELECT @a", params)

    assert calls[0]["query_parameters"] == params[0]
    assert calls[1]["query_parameters"] == params[1]


def test_bigquery_dialect():
    """Test dialect property returns bigquery."""
    adapter = BigQueryAdapter.__new__(BigQueryAdapter)
    assert adapter.dialect == "bigquery"


def test_bigquery_raw_connection():
    """Test raw_connection property returns the underlying client."""

    class FakeClient:
        pass

    adapter = BigQueryAdapter.__new__(BigQueryAdapter)
    fake_client = FakeClient()
    adapter.client = fake_client

    assert adapter.raw_connection is fake_client


def test_bigquery_close():
    """Test close method calls close on client."""

    class FakeClient:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    adapter = BigQueryAdapter.__new__(BigQueryAdapter)
    fake_client = FakeClient()
    adapter.client = fake_client

    adapter.close()
    assert fake_client.closed is True


def test_bigquery_fetchone():
    """Test fetchone method on adapter."""

    class FakeRow:
        def __init__(self, val):
            self.val = val

        def values(self):
            return [self.val]

    class FakeResult:
        schema = [SimpleNamespace(name="x", field_type="INT64")]

        def __iter__(self):
            return iter([FakeRow(42)])

        def to_arrow(self):
            return None

    class FakeJob:
        def result(self):
            return FakeResult()

    adapter = BigQueryAdapter.__new__(BigQueryAdapter)
    result = BigQueryResult(FakeJob())
    row = adapter.fetchone(result)
    assert row == (42,)


def test_bigquery_execute_returns_result():
    """Test execute returns a BigQueryResult wrapper."""

    class FakeRow:
        def values(self):
            return [1]

    class FakeQueryResult:
        schema = [SimpleNamespace(name="x", field_type="INT64")]

        def __iter__(self):
            return iter([FakeRow()])

        def to_arrow(self):
            return None

    class FakeJob:
        def result(self):
            return FakeQueryResult()

    class FakeClient:
        def query(self, sql):
            return FakeJob()

    adapter = BigQueryAdapter.__new__(BigQueryAdapter)
    adapter.client = FakeClient()

    result = adapter.execute("SELECT 1")
    assert isinstance(result, BigQueryResult)


def test_bigquery_result_description():
    """Test description property on BigQueryResult."""

    class FakeResult:
        schema = [
            SimpleNamespace(name="id", field_type="INT64"),
            SimpleNamespace(name="name", field_type="STRING"),
        ]

        def __iter__(self):
            return iter([])

        def to_arrow(self):
            return None

    class FakeJob:
        def result(self):
            return FakeResult()

    result = BigQueryResult(FakeJob())
    assert result.description == [("id", "INT64"), ("name", "STRING")]


def test_bigquery_result_fetchone_exhausted():
    """Test fetchone returns None when rows exhausted."""

    class FakeResult:
        schema = []

        def __iter__(self):
            return iter([])

        def to_arrow(self):
            return None

    class FakeJob:
        def result(self):
            return FakeResult()

    result = BigQueryResult(FakeJob())
    assert result.fetchone() is None


def test_bigquery_get_tables_with_dataset():
    """Test get_tables when dataset_id is set."""
    adapter = BigQueryAdapter.__new__(BigQueryAdapter)

    class FakeTable:
        def __init__(self, table_id):
            self.table_id = table_id

    class FakeClient:
        def dataset(self, dataset_id):
            return SimpleNamespace(dataset_id=dataset_id)

        def list_tables(self, dataset_ref):
            return [FakeTable("orders"), FakeTable("users")]

    adapter.client = FakeClient()
    adapter.project_id = "proj"
    adapter.dataset_id = "my_dataset"

    tables = adapter.get_tables()
    assert len(tables) == 2
    assert {"table_name": "orders", "schema": "my_dataset"} in tables
    assert {"table_name": "users", "schema": "my_dataset"} in tables


def test_bigquery_get_columns_with_explicit_schema():
    """Test get_columns with explicitly provided schema overrides adapter dataset_id."""
    adapter = BigQueryAdapter.__new__(BigQueryAdapter)

    class FakeField:
        def __init__(self, name, field_type, mode="NULLABLE"):
            self.name = name
            self.field_type = field_type
            self.mode = mode

    class FakeClient:
        def dataset(self, dataset_id):
            return SimpleNamespace(table=lambda name: (dataset_id, name))

        def get_table(self, table_ref):
            return SimpleNamespace(schema=[FakeField("id", "INT64")])

    adapter.client = FakeClient()
    adapter.dataset_id = "default_dataset"

    cols = adapter.get_columns("orders", schema="override_dataset")
    assert cols == [{"column_name": "id", "data_type": "INT64", "is_nullable": True}]


def test_bigquery_get_columns_nullable_modes():
    """Test get_columns correctly identifies nullable vs required fields."""
    adapter = BigQueryAdapter.__new__(BigQueryAdapter)

    class FakeField:
        def __init__(self, name, field_type, mode):
            self.name = name
            self.field_type = field_type
            self.mode = mode

    class FakeClient:
        def dataset(self, dataset_id):
            return SimpleNamespace(table=lambda name: (dataset_id, name))

        def get_table(self, table_ref):
            return SimpleNamespace(
                schema=[
                    FakeField("id", "INT64", "REQUIRED"),
                    FakeField("name", "STRING", "NULLABLE"),
                    FakeField("tags", "STRING", "REPEATED"),
                ]
            )

    adapter.client = FakeClient()
    adapter.dataset_id = "ds"

    cols = adapter.get_columns("orders")
    assert cols[0] == {"column_name": "id", "data_type": "INT64", "is_nullable": False}
    assert cols[1] == {"column_name": "name", "data_type": "STRING", "is_nullable": True}
    assert cols[2] == {"column_name": "tags", "data_type": "STRING", "is_nullable": True}


def test_bigquery_fetch_record_batch_via_adapter():
    """Test fetch_record_batch method on adapter."""
    pa = pytest.importorskip("pyarrow")

    class FakeResult:
        schema = [SimpleNamespace(name="id", field_type="INT64")]

        def __iter__(self):
            return iter([])

        def to_arrow(self):
            return pa.table({"id": [1, 2, 3]})

    class FakeJob:
        def result(self):
            return FakeResult()

    adapter = BigQueryAdapter.__new__(BigQueryAdapter)
    result = BigQueryResult(FakeJob())
    reader = adapter.fetch_record_batch(result)

    assert isinstance(reader, pa.RecordBatchReader)
    table = reader.read_all()
    assert table.column("id").to_pylist() == [1, 2, 3]
