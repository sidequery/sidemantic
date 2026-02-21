"""Kitchen sink tests for AtScale SML adapter."""

from pathlib import Path

import pytest

from sidemantic.adapters.atscale_sml import AtScaleSMLAdapter


@pytest.fixture
def adapter():
    return AtScaleSMLAdapter()


@pytest.fixture
def fixtures_dir():
    return Path(__file__).parent.parent.parent / "fixtures" / "atscale_sml_kitchen_sink"


class TestAtScaleSMLKitchenSink:
    def test_parse_models_and_tables(self, adapter, fixtures_dir):
        graph = adapter.parse(fixtures_dir)

        assert "order_model" in graph.models
        assert "return_model" in graph.models
        assert "dim_customers" in graph.models
        assert "dim_dates" in graph.models
        assert "dim_promos" in graph.models

        order_model = graph.models["order_model"]
        return_model = graph.models["return_model"]
        dim_customers = graph.models["dim_customers"]
        dim_dates = graph.models["dim_dates"]
        dim_promos = graph.models["dim_promos"]

        assert order_model.table == "warehouse.analytics.orders"
        assert return_model.table == "warehouse.analytics.returns"
        assert dim_customers.table == "warehouse.analytics.customers"
        assert dim_promos.table == "warehouse.analytics.promos"
        assert dim_dates.sql == "select * from analytics.dates"

    def test_parse_dimensions(self, adapter, fixtures_dir):
        graph = adapter.parse(fixtures_dir)

        order_model = graph.models["order_model"]
        return_model = graph.models["return_model"]
        dim_customers = graph.models["dim_customers"]
        dim_dates = graph.models["dim_dates"]

        order_year = dim_dates.get_dimension("order_year")
        assert order_year is not None
        assert order_year.type == "time"
        assert order_year.granularity == "year"

        order_month = dim_dates.get_dimension("order_month")
        assert order_month is not None
        assert order_month.parent == "order_year"

        order_date = dim_dates.get_dimension("order_date")
        assert order_date is not None
        assert order_date.parent == "order_month"

        email = dim_customers.get_dimension("email")
        assert email is not None
        assert email.parent == "customer_id"

        customer_alias = dim_customers.get_dimension("customer_alias")
        assert customer_alias is not None
        assert customer_alias.parent == "customer_name"

        order_status_orders = order_model.get_dimension("order_status")
        order_status_returns = return_model.get_dimension("order_status")
        assert order_status_orders is not None
        assert order_status_returns is not None
        assert order_status_orders.type == "categorical"

    def test_parse_metrics(self, adapter, fixtures_dir):
        graph = adapter.parse(fixtures_dir)

        order_model = graph.models["order_model"]
        return_model = graph.models["return_model"]
        dim_customers = graph.models["dim_customers"]

        total_amount = order_model.get_metric("total_amount")
        assert total_amount is not None
        assert total_amount.agg == "sum"
        assert total_amount.sql == "total_amount"
        assert total_amount.format == "currency"

        order_count = order_model.get_metric("order_count")
        assert order_count is not None
        assert order_count.agg == "count"
        assert order_count.sql == "order_id"

        unique_customers = order_model.get_metric("unique_customers")
        assert unique_customers is not None
        assert unique_customers.agg == "count_distinct"

        total_amount_distinct = order_model.get_metric("total_amount_distinct")
        assert total_amount_distinct is not None
        assert total_amount_distinct.type == "derived"
        assert total_amount_distinct.sql == "SUM(DISTINCT total_amount)"

        amount_stddev = order_model.get_metric("amount_stddev")
        assert amount_stddev is not None
        assert amount_stddev.type == "derived"
        assert amount_stddev.sql == "STDDEV_SAMP(total_amount)"

        amount_median = order_model.get_metric("amount_median")
        assert amount_median is not None
        assert amount_median.agg == "median"
        assert amount_median.sql == "total_amount"

        amount_p90 = order_model.get_metric("amount_p90")
        assert amount_p90 is not None
        assert amount_p90.type == "derived"
        assert amount_p90.sql == "PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY total_amount)"

        avg_order_value = order_model.get_metric("avg_order_value")
        assert avg_order_value is not None
        assert avg_order_value.type == "derived"
        assert "total_amount" in avg_order_value.sql

        customer_count = dim_customers.get_metric("customer_count")
        assert customer_count is not None
        assert customer_count.agg == "count_distinct"

        return_count = return_model.get_metric("return_count")
        assert return_count is not None
        assert return_count.agg == "count"

    def test_parse_relationships_and_rollups(self, adapter, fixtures_dir):
        graph = adapter.parse(fixtures_dir)
        order_model = graph.models["order_model"]

        rel_names = {rel.name: rel for rel in order_model.relationships}
        assert "dim_customers" in rel_names
        assert "dim_dates" in rel_names
        assert "dim_promos" in rel_names

        customer_rel = rel_names["dim_customers"]
        assert customer_rel.type == "many_to_one"
        assert customer_rel.foreign_key == "customer_id"
        assert customer_rel.primary_key == "customer_id"

        promo_rel = rel_names["dim_promos"]
        assert promo_rel.type == "many_to_many"
        assert promo_rel.foreign_key == "promo_id"

        assert len(order_model.pre_aggregations) == 1
        preagg = order_model.pre_aggregations[0]
        assert preagg.name == "orders_rollup"
        assert "total_amount" in preagg.measures
        assert "order_count" in preagg.measures
        assert "order_year" in preagg.dimensions
        assert "promo_name" in preagg.dimensions

        total_amount = order_model.get_metric("total_amount")
        assert total_amount.drill_fields is not None
        assert "customer_name" in total_amount.drill_fields
        assert "promo_name" in total_amount.drill_fields


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
