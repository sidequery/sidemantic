"""Kitchen sink tests for Holistics AML adapter using doc patterns."""

import pytest

from sidemantic import SemanticLayer
from sidemantic.adapters.holistics import HolisticsAdapter


@pytest.fixture
def kitchen_sink_layer():
    adapter = HolisticsAdapter()
    graph = adapter.parse("tests/fixtures/holistics_kitchen_sink")
    layer = SemanticLayer()
    layer.graph = graph
    return layer


class TestKitchenSinkParsing:
    def test_models_load(self, kitchen_sink_layer):
        graph = kitchen_sink_layer.graph
        assert "kitchen_orders" in graph.models
        assert "kitchen_customers" in graph.models
        assert "kitchen_products" in graph.models
        assert "kitchen_order_summary" in graph.models
        assert "kitchen_orders_extended" in graph.models
        assert "kitchen_orders_inline" in graph.models
        assert "finance.refunds" in graph.models

    def test_query_model_parsed(self, kitchen_sink_layer):
        model = kitchen_sink_layer.graph.models["kitchen_order_summary"]
        assert model.sql is not None
        assert model.table is None

    def test_dimension_types_and_formats(self, kitchen_sink_layer):
        model = kitchen_sink_layer.graph.models["kitchen_orders"]

        order_id = model.get_dimension("order_id")
        assert order_id.type == "numeric"
        assert order_id.format == "#,##0"
        assert order_id.label == "Order ID"

        order_date = model.get_dimension("order_date")
        assert order_date.type == "time"
        assert order_date.granularity == "day"

        created_at = model.get_dimension("created_at")
        assert created_at.type == "time"
        assert created_at.granularity == "hour"

        is_priority = model.get_dimension("is_priority")
        assert is_priority.type == "boolean"

        net_amount = model.get_dimension("net_amount")
        assert net_amount.sql == "amount - discount"

        aql_count = model.get_metric("order_count_aql")
        assert aql_count.sql == "COUNT(order_id)"

        revenue_per_order = model.get_metric("revenue_per_order_aql")
        assert "SUM(amount)" in revenue_per_order.sql
        assert "COUNT(order_id)" in revenue_per_order.sql

        today = model.get_dimension("today")
        assert today.sql == "CURRENT_DATE"

    def test_measure_aggregation_types(self, kitchen_sink_layer):
        model = kitchen_sink_layer.graph.models["kitchen_orders"]

        assert model.get_metric("order_count").agg == "count"
        assert model.get_metric("distinct_customers").agg == "count_distinct"
        assert model.get_metric("revenue_sum").agg == "sum"
        assert model.get_metric("revenue_avg").agg == "avg"
        assert model.get_metric("revenue_min").agg == "min"
        assert model.get_metric("revenue_max").agg == "max"
        assert model.get_metric("revenue_median").agg == "median"

        stdev = model.get_metric("revenue_stdev")
        assert stdev.type == "derived"
        assert "STDDEV_SAMP" in stdev.sql

        stdevp = model.get_metric("revenue_stdevp")
        assert stdevp.type == "derived"
        assert "STDDEV_POP" in stdevp.sql

        variance = model.get_metric("revenue_var")
        assert variance.type == "derived"
        assert "VAR_SAMP" in variance.sql

        variancep = model.get_metric("revenue_varp")
        assert variancep.type == "derived"
        assert "VAR_POP" in variancep.sql

        aov = model.get_metric("aov")
        assert aov.type == "ratio"
        assert aov.numerator == "revenue_sum"
        assert aov.denominator == "order_count"
        assert aov.format == "$#,##0.00"

    def test_relationships(self, kitchen_sink_layer):
        orders = kitchen_sink_layer.graph.models["kitchen_orders"]
        customers = kitchen_sink_layer.graph.models["kitchen_customers"]
        refunds = kitchen_sink_layer.graph.models["finance.refunds"]
        summary = kitchen_sink_layer.graph.models["kitchen_order_summary"]

        rel_to_customers = next(r for r in orders.relationships if r.name == "kitchen_customers")
        assert rel_to_customers.type == "many_to_one"
        assert rel_to_customers.foreign_key == "customer_id"

        rel_to_products = next(r for r in orders.relationships if r.name == "kitchen_products")
        assert rel_to_products.type == "many_to_one"
        assert rel_to_products.foreign_key == "product_id"

        rel_to_summary = next(r for r in customers.relationships if r.name == "kitchen_order_summary")
        assert rel_to_summary.type == "one_to_one"
        assert rel_to_summary.foreign_key == "customer_id"
        assert rel_to_summary.primary_key == "customer_id"

        rel_to_orders = next(r for r in refunds.relationships if r.name == "kitchen_orders")
        assert rel_to_orders.type == "many_to_one"
        assert rel_to_orders.foreign_key == "order_id"

        assert summary is not None

    def test_dataset_level_metrics_and_dimensions(self, kitchen_sink_layer):
        """Dataset-level metric/dimension blocks (AQL, cross-model) are surfaced."""
        graph = kitchen_sink_layer.graph

        # Datasets surface as models named after the dataset.
        assert "kitchen_transactions" in graph.models
        assert "kitchen_sink" in graph.models

        transactions = graph.models["kitchen_transactions"]

        # Dataset dimension authored with an aggregate AQL (count(...)) is
        # surfaced as a derived metric, since an aggregate cannot be a
        # groupable dimension.
        total_buyer_orders = transactions.get_metric("total_buyer_orders")
        assert total_buyer_orders is not None
        assert total_buyer_orders.sql == "COUNT(kitchen_orders.order_id)"

        # Dataset metric with no aggregation_type, defined purely via @aql.
        avg_order_amount = transactions.get_metric("avg_order_amount")
        assert avg_order_amount is not None
        assert "SUM(kitchen_orders.amount)" in avg_order_amount.sql
        assert "COUNT(kitchen_orders.order_id)" in avg_order_amount.sql

        buyer_event_ratio = transactions.get_metric("buyer_event_ratio")
        assert buyer_event_ratio is not None
        assert "COUNT(kitchen_events.event_id)" in buyer_event_ratio.sql
        assert "COUNT(DISTINCT kitchen_buyers.person_id)" in buyer_event_ratio.sql

        sink = graph.models["kitchen_sink"]
        revenue_per_customer = sink.get_metric("revenue_per_customer")
        assert revenue_per_customer is not None
        assert "SUM(kitchen_orders.amount)" in revenue_per_customer.sql

    def test_dataset_metrics_registered_at_graph_scope(self, kitchen_sink_layer):
        """Dataset metrics are first-class graph metrics, reachable without a
        model prefix via graph.metrics, list_metrics(), and get_metric()."""
        layer = kitchen_sink_layer
        graph = layer.graph

        for name in ("avg_order_amount", "buyer_event_ratio", "total_buyer_orders"):
            assert name in graph.metrics, f"{name} should be registered at graph scope"
            assert name in layer.list_metrics()
            assert layer.get_metric(name).name == name

        # Dataset metric from a different dataset is also surfaced.
        assert "revenue_per_customer" in graph.metrics

    def test_standalone_metric_and_partial_dataset(self, kitchen_sink_layer):
        """Standalone Metric blocks register as graph metrics; PartialDataset
        metrics compose into a Dataset via .extend()."""
        graph = kitchen_sink_layer.graph

        # Standalone top-level Metric -> graph-level metric.
        assert "kitchen_global_revenue" in graph.metrics
        global_revenue = graph.metrics["kitchen_global_revenue"]
        assert global_revenue.label == "Global Revenue"
        assert global_revenue.sql == "SUM(kitchen_orders.amount)"

        # Dataset = base.extend(partial_dataset) surfaces the partial's metrics.
        assert "kitchen_metric_store" in graph.models
        store = graph.models["kitchen_metric_store"]

        order_count = store.get_metric("reusable_order_count")
        assert order_count is not None
        assert order_count.sql == "COUNT(kitchen_orders.order_id)"

        # where() table function preserved through the pipe; count still applies.
        high_value = store.get_metric("high_value_orders")
        assert high_value is not None
        assert high_value.sql == "COUNT(kitchen_orders.order_id)"

        # of_all() metric modifier preserves the inner aggregation.
        revenue_share = store.get_metric("revenue_share")
        assert revenue_share is not None
        assert revenue_share.sql == "SUM(kitchen_orders.amount)"

        # relative_period() period-over-period preserves the inner aggregation.
        last_month = store.get_metric("revenue_last_month")
        assert last_month is not None
        assert last_month.sql == "SUM(kitchen_orders.amount)"

    def test_extends_merge(self, kitchen_sink_layer):
        extended = kitchen_sink_layer.graph.models["kitchen_orders_extended"]
        inline = kitchen_sink_layer.graph.models["kitchen_orders_inline"]

        status = extended.get_dimension("status")
        assert status.label == "Order Status"
        assert status.type == "categorical"

        shipping_method = extended.get_dimension("shipping_method")
        assert shipping_method is not None
        assert shipping_method.type == "categorical"

        inline_status = inline.get_dimension("status")
        assert inline_status.label == "Inline Status"

        promised_at = inline.get_dimension("promised_at")
        assert promised_at.type == "time"
        assert promised_at.granularity == "hour"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
