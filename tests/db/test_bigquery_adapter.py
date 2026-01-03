"""Tests for BigQuery adapter."""

from types import SimpleNamespace

import pytest

from sidemantic.db.bigquery import BigQueryAdapter, BigQueryResult


def test_bigquery_adapter_import_error_message():
    try:
        BigQueryAdapter()
    except ImportError as exc:
        assert "bigquery" in str(exc).lower()


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
