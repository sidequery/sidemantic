"""Tests for Superset adapter - extended dataset/column/metric metadata.

Covers Apache Superset dataset import/export fields that have no first-class
Sidemantic equivalent and are preserved under ``meta['superset']``:

- Dataset: ``catalog`` (multi-catalog qualifier), ``currency_code_column``,
  ``folders`` (column/metric folder organization).
- Column: ``advanced_data_type``, ``python_date_format``, ``datetime_format``.
- Metric: ``currency`` (``{symbol, symbolPosition}``), ``d3format``, ``warning_text``.
"""

import tempfile
from pathlib import Path

import pytest
import yaml

from sidemantic.adapters.superset import SupersetAdapter

# =============================================================================
# MULTI-CATALOG FIXTURE PARSING
# =============================================================================


class TestMultiCatalogParsing:
    """Tests for the multi_catalog_revenue.yaml fixture."""

    @pytest.fixture
    def graph(self):
        adapter = SupersetAdapter()
        return adapter.parse("tests/fixtures/superset/multi_catalog_revenue.yaml")

    @pytest.fixture
    def model(self, graph):
        return graph.models["revenue_by_region"]

    def test_model_loads(self, graph):
        assert "revenue_by_region" in graph.models

    def test_catalog_in_table_reference(self, model):
        """catalog.schema.table is preserved as a 3-part qualified table."""
        assert model.table == "analytics_catalog.finance.revenue_by_region"

    def test_catalog_in_meta(self, model):
        assert model.meta["superset"]["catalog"] == "analytics_catalog"

    def test_currency_code_column_in_meta(self, model):
        assert model.meta["superset"]["currency_code_column"] == "iso_currency"

    def test_folders_in_meta(self, model):
        folders = model.meta["superset"]["folders"]
        assert isinstance(folders, list)
        assert folders[0]["name"] == "Money"
        assert folders[0]["type"] == "folder"
        child_names = [c["name"] for c in folders[0]["children"]]
        assert "total_revenue" in child_names
        assert "avg_revenue" in child_names

    def test_column_advanced_data_type(self, model):
        region = model.get_dimension("region")
        assert region.meta["superset"]["advanced_data_type"] == "country"

    def test_column_date_formats(self, model):
        report_date = model.get_dimension("report_date")
        assert report_date.meta["superset"]["python_date_format"] == "%Y-%m-%d"
        assert report_date.meta["superset"]["datetime_format"] == "%Y-%m-%d"

    def test_metric_d3format_maps_to_format(self, model):
        total_revenue = model.get_metric("total_revenue")
        assert total_revenue.format == "$,.2f"
        assert total_revenue.meta["superset"]["d3format"] == "$,.2f"

    def test_metric_currency(self, model):
        total_revenue = model.get_metric("total_revenue")
        currency = total_revenue.meta["superset"]["currency"]
        assert currency["symbol"] == "EUR"
        assert currency["symbolPosition"] == "suffix"

    def test_metric_warning_text(self, model):
        total_revenue = model.get_metric("total_revenue")
        assert total_revenue.meta["superset"]["warning_text"] == "Preliminary figures, subject to revision"

    def test_metric_without_metadata_has_no_superset_meta(self, model):
        count = model.get_metric("count")
        # No d3format/currency/warning_text -> no superset meta payload.
        assert count.meta is None
        assert count.format is None


# =============================================================================
# DATASET-LEVEL METADATA ON THE ORDERS FIXTURE
# =============================================================================


class TestOrdersMetadata:
    """The orders.yaml fixture carries currency_code_column, folders, and
    column/metric metadata (no catalog, to keep its table reference 2-part)."""

    @pytest.fixture
    def model(self):
        adapter = SupersetAdapter()
        graph = adapter.parse("tests/fixtures/superset/orders.yaml")
        return graph.models["orders"]

    def test_table_reference_unchanged(self, model):
        assert model.table == "public.orders"

    def test_currency_code_column(self, model):
        assert model.meta["superset"]["currency_code_column"] == "currency_code"

    def test_folders(self, model):
        folders = model.meta["superset"]["folders"]
        names = [f["name"] for f in folders]
        assert "Revenue" in names
        assert "Attributes" in names

    def test_column_advanced_data_type(self, model):
        customer_id = model.get_dimension("customer_id")
        assert customer_id.meta["superset"]["advanced_data_type"] == "internet_address"

    def test_column_date_formats(self, model):
        created_at = model.get_dimension("created_at")
        assert created_at.meta["superset"]["python_date_format"] == "%Y-%m-%d %H:%M:%S"
        assert created_at.meta["superset"]["datetime_format"] == "%Y-%m-%dT%H:%M:%S"

    def test_metric_currency_and_warning(self, model):
        revenue = model.get_metric("total_revenue")
        assert revenue.meta["superset"]["currency"]["symbol"] == "USD"
        assert revenue.meta["superset"]["warning_text"] == "Excludes refunded orders"


# =============================================================================
# EXPORT TESTS
# =============================================================================


class TestMetadataExport:
    """New metadata fields are emitted on export."""

    @pytest.fixture
    def exported(self):
        adapter = SupersetAdapter()
        graph = adapter.parse("tests/fixtures/superset/multi_catalog_revenue.yaml")
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter.export(graph, tmpdir)
            with open(Path(tmpdir) / "revenue_by_region.yaml") as f:
                yield yaml.safe_load(f)

    def test_catalog_exported(self, exported):
        assert exported["catalog"] == "analytics_catalog"
        assert exported["schema"] == "finance"
        assert exported["table_name"] == "revenue_by_region"

    def test_currency_code_column_exported(self, exported):
        assert exported["currency_code_column"] == "iso_currency"

    def test_folders_exported(self, exported):
        assert isinstance(exported["folders"], list)
        assert exported["folders"][0]["name"] == "Money"

    def test_column_metadata_exported(self, exported):
        region = next(c for c in exported["columns"] if c["column_name"] == "region")
        assert region["advanced_data_type"] == "country"

        report_date = next(c for c in exported["columns"] if c["column_name"] == "report_date")
        assert report_date["python_date_format"] == "%Y-%m-%d"
        assert report_date["datetime_format"] == "%Y-%m-%d"

    def test_metric_metadata_exported(self, exported):
        total_revenue = next(m for m in exported["metrics"] if m["metric_name"] == "total_revenue")
        assert total_revenue["d3format"] == "$,.2f"
        assert total_revenue["currency"]["symbol"] == "EUR"
        assert total_revenue["currency"]["symbolPosition"] == "suffix"
        assert total_revenue["warning_text"] == "Preliminary figures, subject to revision"


# =============================================================================
# ROUNDTRIP TESTS
# =============================================================================


class TestMetadataRoundtrip:
    """All new metadata survives Superset -> Sidemantic -> Superset."""

    @pytest.fixture
    def reparsed(self):
        adapter = SupersetAdapter()
        graph = adapter.parse("tests/fixtures/superset/multi_catalog_revenue.yaml")
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter.export(graph, tmpdir)
            graph2 = adapter.parse(Path(tmpdir) / "revenue_by_region.yaml")
            yield graph2.models["revenue_by_region"]

    def test_catalog_roundtrip(self, reparsed):
        assert reparsed.table == "analytics_catalog.finance.revenue_by_region"
        assert reparsed.meta["superset"]["catalog"] == "analytics_catalog"

    def test_currency_code_column_roundtrip(self, reparsed):
        assert reparsed.meta["superset"]["currency_code_column"] == "iso_currency"

    def test_folders_roundtrip(self, reparsed):
        folders = reparsed.meta["superset"]["folders"]
        assert folders[0]["name"] == "Money"
        assert {c["name"] for c in folders[0]["children"]} == {"total_revenue", "avg_revenue"}

    def test_column_metadata_roundtrip(self, reparsed):
        region = reparsed.get_dimension("region")
        assert region.meta["superset"]["advanced_data_type"] == "country"
        report_date = reparsed.get_dimension("report_date")
        assert report_date.meta["superset"]["python_date_format"] == "%Y-%m-%d"
        assert report_date.meta["superset"]["datetime_format"] == "%Y-%m-%d"

    def test_metric_metadata_roundtrip(self, reparsed):
        total_revenue = reparsed.get_metric("total_revenue")
        assert total_revenue.format == "$,.2f"
        assert total_revenue.meta["superset"]["d3format"] == "$,.2f"
        assert total_revenue.meta["superset"]["currency"] == {"symbol": "EUR", "symbolPosition": "suffix"}
        assert total_revenue.meta["superset"]["warning_text"] == "Preliminary figures, subject to revision"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
