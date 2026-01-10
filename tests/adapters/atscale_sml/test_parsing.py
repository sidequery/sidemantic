"""Tests for AtScale SML adapter parsing."""

from pathlib import Path

import pytest

from sidemantic.adapters.atscale_sml import AtScaleSMLAdapter


@pytest.fixture
def adapter():
    return AtScaleSMLAdapter()


@pytest.fixture
def fixtures_dir():
    return Path(__file__).parent.parent.parent / "fixtures" / "atscale_sml"


class TestAtScaleSMLParsing:
    def test_parse_repository(self, adapter, fixtures_dir):
        graph = adapter.parse(fixtures_dir)

        assert "sales_model" in graph.models
        assert "dim_customers" in graph.models
        assert "dim_regions" in graph.models

        fact_sales = graph.models["sales_model"]
        dim_customers = graph.models["dim_customers"]
        dim_regions = graph.models["dim_regions"]

        assert fact_sales.table == "analytics.public.sales"
        assert dim_customers.table == "analytics.public.customers"
        assert dim_regions.table == "analytics.public.regions"

    def test_parse_dimensions(self, adapter, fixtures_dir):
        graph = adapter.parse(fixtures_dir)

        fact_sales = graph.models["sales_model"]
        dim_customers = graph.models["dim_customers"]
        dim_regions = graph.models["dim_regions"]

        order_date = fact_sales.get_dimension("order_date")
        assert order_date is not None
        assert order_date.type == "time"
        assert order_date.granularity == "day"

        customer_id = dim_customers.get_dimension("customer_id")
        assert customer_id is not None
        assert customer_id.type == "numeric"

        customer_name = dim_customers.get_dimension("customer_name")
        assert customer_name is not None
        assert customer_name.type == "categorical"
        assert customer_name.parent == "customer_id"

        region_name = dim_regions.get_dimension("region_name")
        assert region_name is not None
        assert region_name.type == "categorical"

    def test_parse_metrics(self, adapter, fixtures_dir):
        graph = adapter.parse(fixtures_dir)
        fact_sales = graph.models["sales_model"]
        dim_customers = graph.models["dim_customers"]

        total_sales = fact_sales.get_metric("total_sales")
        assert total_sales is not None
        assert total_sales.agg == "sum"
        assert total_sales.sql == "amount"
        assert total_sales.format == "standard"

        order_count = fact_sales.get_metric("order_count")
        assert order_count is not None
        assert order_count.agg == "count"
        assert order_count.sql == "sale_id"

        sales_stddev = fact_sales.get_metric("sales_stddev")
        assert sales_stddev is not None
        assert sales_stddev.type == "derived"
        assert sales_stddev.sql == "STDDEV_POP(amount)"

        sales_median = fact_sales.get_metric("sales_median")
        assert sales_median is not None
        assert sales_median.agg == "median"
        assert sales_median.sql == "amount"

        avg_order_value = fact_sales.get_metric("avg_order_value")
        assert avg_order_value is not None
        assert avg_order_value.type == "derived"
        assert "total_sales" in avg_order_value.sql

        customer_count = dim_customers.get_metric("customer_count")
        assert customer_count is not None
        assert customer_count.agg == "count_distinct"

    def test_parse_relationships(self, adapter, fixtures_dir):
        graph = adapter.parse(fixtures_dir)
        fact_sales = graph.models["sales_model"]
        dim_customers = graph.models["dim_customers"]

        rel_names = [rel.name for rel in fact_sales.relationships]
        assert "dim_customers" in rel_names

        customer_rel = next(rel for rel in fact_sales.relationships if rel.name == "dim_customers")
        assert customer_rel.type == "many_to_one"
        assert customer_rel.foreign_key == "customer_id"
        assert customer_rel.primary_key == "customer_id"

        customer_rel_names = [rel.name for rel in dim_customers.relationships]
        assert "dim_regions" in customer_rel_names

    def test_parse_aggregates_and_drillthroughs(self, adapter, fixtures_dir):
        graph = adapter.parse(fixtures_dir)
        fact_sales = graph.models["sales_model"]

        assert len(fact_sales.pre_aggregations) == 1
        preagg = fact_sales.pre_aggregations[0]
        assert preagg.name == "sales_rollup"
        assert "total_sales" in preagg.measures
        assert "order_count" in preagg.measures

        total_sales = fact_sales.get_metric("total_sales")
        assert total_sales.drill_fields is not None
        assert "customer_name" in total_sales.drill_fields
        assert "region_name" in total_sales.drill_fields


class TestAtScaleSMLEdgeCases:
    def test_parse_empty_file(self, adapter, tmp_path):
        empty_file = tmp_path / "empty.yml"
        empty_file.write_text("")

        graph = adapter.parse(empty_file)
        assert len(graph.models) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
