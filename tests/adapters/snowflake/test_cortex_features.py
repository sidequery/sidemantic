"""Tests for newer Snowflake Cortex Analyst spec features.

Covers the keys added to the adapter on top of the legacy facts-based format:
- table-level `measures` (legacy alias of `facts`)
- `synonyms` on dimensions/measures/metrics
- `sample_values`, nested `cortex_search_service`, `is_enum`, `unique`,
  `access_modifier`, `labels`, `tags` on dimensions
- `non_additive_dimensions` / `using_relationships` preserved in metadata
- top-level `verified_queries`, `custom_instructions`, `module_custom_instructions`
- export round-trip preservation of all of the above
"""

from pathlib import Path

import pytest
import yaml

from sidemantic.adapters.snowflake import SnowflakeAdapter


@pytest.fixture
def adapter():
    return SnowflakeAdapter()


@pytest.fixture
def fixture_path():
    return Path(__file__).parent.parent.parent / "fixtures" / "snowflake" / "cortex_features.yaml"


@pytest.fixture
def graph(adapter, fixture_path):
    return adapter.parse(fixture_path)


class TestMeasuresAlias:
    def test_measures_parsed_as_metrics(self, graph):
        model = graph.models["orders"]
        names = {m.name for m in model.metrics}
        assert "order_total" in names
        assert "distinct_orders" in names

    def test_measure_default_aggregation(self, graph):
        model = graph.models["orders"]
        order_total = model.get_metric("order_total")
        assert order_total.agg == "sum"
        assert order_total.sql == "total"


class TestSynonyms:
    def test_dimension_synonyms(self, graph):
        model = graph.models["orders"]
        status = model.get_dimension("status")
        assert status.synonyms == ["state"]

    def test_measure_synonyms(self, graph):
        model = graph.models["orders"]
        order_total = model.get_metric("order_total")
        assert order_total.synonyms == ["revenue"]

    def test_metric_synonyms(self, graph):
        model = graph.models["orders"]
        distinct_orders = model.get_metric("distinct_orders")
        assert distinct_orders.synonyms == ["order count"]


class TestDimensionEnrichment:
    def test_sample_values(self, graph):
        model = graph.models["orders"]
        status = model.get_dimension("status")
        assert "delivered" in status.sample_values

    def test_nested_cortex_search_service(self, graph):
        model = graph.models["orders"]
        cust = model.get_dimension("customer_name")
        assert cust.cortex_search_service_name == "customer_name_search"

    def test_is_enum_and_modifier_in_metadata(self, graph):
        model = graph.models["orders"]
        status = model.get_dimension("status")
        sf = status.metadata["snowflake"]
        assert sf["is_enum"] is True
        assert sf["access_modifier"] == "public_access"
        assert sf["labels"] == ["Order Status"]
        assert sf["tags"] == ["core"]


class TestMeasureMetricMetadata:
    def test_non_additive_dimensions_preserved(self, graph):
        model = graph.models["orders"]
        order_total = model.get_metric("order_total")
        sf = order_total.metadata["snowflake"]
        assert sf["non_additive_dimensions"][0]["dimension"] == "order_date"
        assert sf["access_modifier"] == "public_access"

    def test_using_relationships_preserved(self, graph):
        model = graph.models["orders"]
        distinct_orders = model.get_metric("distinct_orders")
        sf = distinct_orders.metadata["snowflake"]
        assert sf["using_relationships"] == ["orders_to_customers"]


class TestTopLevelSections:
    def test_verified_queries(self, graph):
        assert len(graph.verified_queries) == 1
        assert graph.verified_queries[0]["name"] == "total revenue"

    def test_custom_instructions(self, graph):
        assert graph.custom_instructions == "Always prefer revenue over total when answering."

    def test_module_custom_instructions(self, graph):
        mci = graph.module_custom_instructions
        assert mci["sql_generation"] == "Prefer explicit column references."
        assert mci["question_categorization"] == "Treat revenue questions as financial."


class TestRoundtrip:
    def test_roundtrip_preserves_cortex_features(self, adapter, graph, tmp_path):
        output = tmp_path / "out.yaml"
        adapter.export(graph, output)

        data = yaml.safe_load(output.read_text())

        # Top-level sections survive export.
        assert "verified_queries" in data
        assert data["custom_instructions"] == "Always prefer revenue over total when answering."
        assert "module_custom_instructions" in data

        # Re-parse and confirm key fields persist.
        graph2 = adapter.parse(output)
        model = graph2.models["orders"]

        status = model.get_dimension("status")
        assert status.synonyms == ["state"]
        assert "delivered" in status.sample_values

        cust = model.get_dimension("customer_name")
        assert cust.cortex_search_service_name == "customer_name_search"

        order_total = model.get_metric("order_total")
        assert order_total.synonyms == ["revenue"]

        assert len(graph2.verified_queries) == 1
