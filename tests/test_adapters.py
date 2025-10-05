"""Tests for semantic model adapters."""

from pathlib import Path

import pytest

from sidemantic.adapters.cube import CubeAdapter
from sidemantic.adapters.lookml import LookMLAdapter
from sidemantic.adapters.metricflow import MetricFlowAdapter


def test_cube_adapter():
    """Test Cube adapter with example YAML."""
    adapter = CubeAdapter()
    graph = adapter.parse(Path("examples/cube/orders.yml"))

    # Check models were imported
    assert "orders" in graph.models

    # Check orders model
    orders = graph.get_model("orders")
    assert orders.table == "public.orders"
    assert len(orders.dimensions) > 0
    assert len(orders.metrics) > 0

    # Check dimensions
    status_dim = orders.get_dimension("status")
    assert status_dim is not None
    assert status_dim.type == "categorical"

    created_dim = orders.get_dimension("created_at")
    assert created_dim is not None
    assert created_dim.type == "time"

    # Check measures
    count_measure = orders.get_metric("count")
    assert count_measure is not None
    assert count_measure.agg == "count"

    revenue_measure = orders.get_metric("revenue")
    assert revenue_measure is not None
    assert revenue_measure.agg == "sum"

    # Check segments imported
    assert len(orders.segments) > 0
    completed_segment = next((s for s in orders.segments if s.name == "completed"), None)
    assert completed_segment is not None


def test_metricflow_adapter():
    """Test MetricFlow adapter with example YAML."""
    adapter = MetricFlowAdapter()
    graph = adapter.parse(Path("examples/metricflow/semantic_models.yml"))

    # Check models were imported
    assert "orders" in graph.models
    assert "customers" in graph.models

    # Check orders model
    orders = graph.get_model("orders")
    assert orders.table == "public.orders"

    # Check primary key
    assert orders.primary_key is not None

    # Check relationships
    assert len(orders.relationships) > 0
    # Should have a many_to_one relationship to customer
    customer_rel = next((r for r in orders.relationships if r.name == "customer"), None)
    assert customer_rel is not None
    assert customer_rel.type == "many_to_one"

    # Check dimensions
    order_date_dim = orders.get_dimension("order_date")
    assert order_date_dim is not None
    assert order_date_dim.type == "time"
    assert order_date_dim.granularity == "day"

    status_dim = orders.get_dimension("status")
    assert status_dim is not None
    assert status_dim.type == "categorical"

    # Check measures
    revenue_measure = orders.get_metric("revenue")
    assert revenue_measure is not None
    assert revenue_measure.agg == "sum"

    # Check metrics
    assert "total_revenue" in graph.metrics
    total_revenue = graph.get_metric("total_revenue")
    assert total_revenue.type is None  # Untyped (was simple)
    assert total_revenue.sql == "revenue"

    assert "average_order_value" in graph.metrics
    avg_order = graph.get_metric("average_order_value")
    assert avg_order.type == "ratio"
    assert avg_order.numerator == "revenue"
    assert avg_order.denominator == "order_count"


def test_cube_adapter_join_discovery():
    """Test that Cube adapter enables join discovery."""
    adapter = CubeAdapter()
    graph = adapter.parse(Path("examples/cube/orders.yml"))

    # Check that relationships were imported
    orders = graph.get_model("orders")
    assert len(orders.relationships) > 0
    # Note: The Cube example only has one model, so no actual join path can be tested
    # but we verify that the relationship structure was imported correctly


def test_metricflow_adapter_join_discovery():
    """Test that MetricFlow adapter enables join discovery."""
    adapter = MetricFlowAdapter()
    graph = adapter.parse(Path("examples/metricflow/semantic_models.yml"))

    # MetricFlow uses entity names for relationships
    # The orders model has a relationship to "customer" (entity name)
    # which corresponds to the customers model
    # For now, the adapter uses entity names, not model names
    # TODO: Update adapter to resolve entity names to model names

    # Check that orders has a relationship defined
    orders = graph.get_model("orders")
    assert len(orders.relationships) > 0
    customer_rel = next((r for r in orders.relationships if r.name == "customer"), None)
    assert customer_rel is not None
    assert customer_rel.type == "many_to_one"


def test_cube_adapter_pre_aggregations():
    """Test Cube adapter with pre-aggregations."""
    adapter = CubeAdapter()
    graph = adapter.parse(Path("examples/cube/orders_with_preagg.yml"))

    orders = graph.get_model("orders")
    assert orders is not None

    # Check pre-aggregations were parsed
    # Note: Pre-aggregations are not stored as first-class objects in SemanticGraph
    # but the adapter should handle them gracefully during parsing
    assert len(orders.dimensions) > 0
    assert len(orders.metrics) > 0
    assert len(orders.segments) == 2

    # Verify time dimensions
    completed_at = orders.get_dimension("completed_at")
    assert completed_at is not None
    assert completed_at.type == "time"

    created_at = orders.get_dimension("created_at")
    assert created_at is not None
    assert created_at.type == "time"


def test_cube_adapter_multi_cube():
    """Test Cube adapter with multiple related cubes."""
    adapter = CubeAdapter()
    graph = adapter.parse(Path("examples/cube/ecommerce_multi_cube.yml"))

    # Check all models were imported
    assert "orders" in graph.models
    assert "customers" in graph.models
    assert "line_items" in graph.models
    assert "products" in graph.models

    # Check relationships
    orders = graph.get_model("orders")
    assert len(orders.relationships) == 2

    # Check many_to_one relationship to customers
    customer_rel = next((r for r in orders.relationships if r.name == "customers"), None)
    assert customer_rel is not None
    assert customer_rel.type == "many_to_one"

    # Check one_to_many relationship to line_items
    line_items_rel = next((r for r in orders.relationships if r.name == "line_items"), None)
    assert line_items_rel is not None
    assert line_items_rel.type == "one_to_many"

    # Check drill members (if supported)
    count_metric = orders.get_metric("count")
    assert count_metric is not None
    if hasattr(count_metric, "drill_fields") and count_metric.drill_fields:
        # Verify drill fields include cross-cube references
        assert any("customers" in str(field) for field in count_metric.drill_fields)


def test_cube_adapter_segments():
    """Test Cube adapter segment parsing with SQL transformations."""
    adapter = CubeAdapter()
    graph = adapter.parse(Path("examples/cube/ecommerce_multi_cube.yml"))

    # Test orders segments
    orders = graph.get_model("orders")
    completed_segment = next((s for s in orders.segments if s.name == "completed"), None)
    assert completed_segment is not None
    # Check that ${CUBE} was replaced with {model}
    assert "{model}" in completed_segment.sql or "orders" in completed_segment.sql.lower()

    # Test customers segments
    customers = graph.get_model("customers")
    assert len(customers.segments) == 2

    sf_segment = next((s for s in customers.segments if s.name == "sf_customers"), None)
    assert sf_segment is not None

    ca_segment = next((s for s in customers.segments if s.name == "ca_customers"), None)
    assert ca_segment is not None


def test_cube_adapter_drill_members():
    """Test Cube adapter drill member parsing."""
    adapter = CubeAdapter()
    graph = adapter.parse(Path("examples/cube/ecommerce_multi_cube.yml"))

    orders = graph.get_model("orders")
    count_metric = orders.get_metric("count")
    assert count_metric is not None

    # Note: drill_members parsing is not yet implemented in Cube adapter
    # This test will validate when the feature is added
    # For now, we just verify the metric exists and has correct basic properties
    assert count_metric.name == "count"
    assert count_metric.agg == "count"


def test_metricflow_advanced_metrics():
    """Test MetricFlow adapter with comprehensive metric types."""
    adapter = MetricFlowAdapter()
    graph = adapter.parse(Path("examples/metricflow/advanced_metrics.yml"))

    # Check models were imported
    assert "orders" in graph.models
    assert "subscriptions" in graph.models

    # Check orders model
    orders = graph.get_model("orders")
    assert orders.table == "public.orders"
    assert orders.primary_key == "order_id"

    # Check entities converted to relationships
    assert len(orders.relationships) == 2
    customer_rel = next((r for r in orders.relationships if r.name == "customer"), None)
    assert customer_rel is not None
    assert customer_rel.type == "many_to_one"
    assert customer_rel.foreign_key == "customer_id"

    product_rel = next((r for r in orders.relationships if r.name == "product"), None)
    assert product_rel is not None
    assert product_rel.type == "many_to_one"
    assert product_rel.foreign_key == "product_id"

    # Check dimensions
    order_date_dim = orders.get_dimension("order_date")
    assert order_date_dim is not None
    assert order_date_dim.type == "time"
    assert order_date_dim.granularity == "day"
    assert order_date_dim.sql == "created_at"

    status_dim = orders.get_dimension("status")
    assert status_dim is not None
    assert status_dim.type == "categorical"

    is_food_dim = orders.get_dimension("is_food_order")
    assert is_food_dim is not None
    assert is_food_dim.type == "categorical"

    # Check measures (model-level metrics)
    order_count = orders.get_metric("order_count")
    assert order_count is not None
    assert order_count.agg == "count"

    order_total = orders.get_metric("order_total")
    assert order_total is not None
    assert order_total.agg == "sum"
    assert order_total.sql == "amount"

    avg_value = orders.get_metric("avg_order_value")
    assert avg_value is not None
    assert avg_value.agg == "avg"

    distinct_customers = orders.get_metric("distinct_customers")
    assert distinct_customers is not None
    assert distinct_customers.agg == "count_distinct"

    # Check graph-level metrics - Simple metric
    assert "total_order_revenue" in graph.metrics
    total_order_revenue = graph.get_metric("total_order_revenue")
    assert total_order_revenue.type is None  # Simple metrics are untyped
    assert total_order_revenue.sql == "order_total"

    # Check ratio metrics
    assert "average_order_value" in graph.metrics
    avg_order = graph.get_metric("average_order_value")
    assert avg_order.type == "ratio"
    assert avg_order.numerator == "order_total"
    assert avg_order.denominator == "order_count"

    assert "profit_margin" in graph.metrics
    profit_margin = graph.get_metric("profit_margin")
    assert profit_margin.type == "ratio"
    assert profit_margin.numerator == "order_total"
    assert profit_margin.denominator == "order_cost"

    # Check derived metrics
    assert "order_gross_profit" in graph.metrics
    gross_profit = graph.get_metric("order_gross_profit")
    assert gross_profit.type == "derived"
    assert gross_profit.sql == "revenue - cost"

    # Check derived metric with filters
    assert "food_order_profit" in graph.metrics
    food_profit = graph.get_metric("food_order_profit")
    assert food_profit.type == "derived"
    assert food_profit.sql == "revenue - cost"

    # Check cumulative metrics
    assert "cumulative_revenue" in graph.metrics
    cum_revenue = graph.get_metric("cumulative_revenue")
    assert cum_revenue.type == "cumulative"
    assert cum_revenue.window is None  # No window = all time

    assert "weekly_revenue" in graph.metrics
    weekly_revenue = graph.get_metric("weekly_revenue")
    assert weekly_revenue.type == "cumulative"
    assert weekly_revenue.window == "7d"

    assert "monthly_revenue" in graph.metrics
    monthly_revenue = graph.get_metric("monthly_revenue")
    assert monthly_revenue.type == "cumulative"

    # Check subscriptions model with non-additive dimension
    subscriptions = graph.get_model("subscriptions")
    assert subscriptions is not None
    assert subscriptions.table == "public.subscriptions"

    subscription_count = subscriptions.get_metric("subscription_count")
    assert subscription_count is not None
    assert subscription_count.non_additive_dimension == "subscription_date"


def test_metricflow_multi_model():
    """Test MetricFlow adapter with multiple related models."""
    adapter = MetricFlowAdapter()
    graph = adapter.parse(Path("examples/metricflow/ecommerce_multi_model.yml"))

    # Check all models were imported
    assert "customers" in graph.models
    assert "products" in graph.models
    assert "orders" in graph.models
    assert "line_items" in graph.models

    # Check customers model
    customers = graph.get_model("customers")
    assert customers.table == "public.customers"
    assert customers.primary_key == "customer_id"

    # Check customer dimensions
    region_dim = customers.get_dimension("region")
    assert region_dim is not None
    assert region_dim.type == "categorical"

    tier_dim = customers.get_dimension("tier")
    assert tier_dim is not None

    signup_dim = customers.get_dimension("signup_date")
    assert signup_dim is not None
    assert signup_dim.type == "time"
    assert signup_dim.granularity == "day"

    # Check products model
    products = graph.get_model("products")
    assert products.table == "public.products"
    assert products.primary_key == "product_id"

    category_dim = products.get_dimension("category")
    assert category_dim is not None

    # Check orders model with multiple relationships
    orders = graph.get_model("orders")
    assert orders.table == "public.orders"
    assert orders.primary_key == "order_id"
    assert len(orders.relationships) == 2

    # Verify relationships
    customer_rel = next((r for r in orders.relationships if r.name == "customer"), None)
    assert customer_rel is not None
    assert customer_rel.type == "many_to_one"

    product_rel = next((r for r in orders.relationships if r.name == "product"), None)
    assert product_rel is not None
    assert product_rel.type == "many_to_one"

    # Check time dimensions with different expressions
    order_date = orders.get_dimension("order_date")
    assert order_date is not None
    assert order_date.type == "time"
    assert order_date.sql == "created_at"

    shipped_date = orders.get_dimension("shipped_date")
    assert shipped_date is not None
    assert shipped_date.type == "time"
    assert shipped_date.sql == "shipped_at"

    # Check line_items model
    line_items = graph.get_model("line_items")
    assert line_items.table == "public.line_items"
    assert line_items.primary_key == "line_item_id"
    assert len(line_items.relationships) == 2

    # Check line_items relationships
    order_rel = next((r for r in line_items.relationships if r.name == "order"), None)
    assert order_rel is not None
    assert order_rel.type == "many_to_one"

    # Check graph-level metrics
    assert "ecommerce_total_orders" in graph.metrics
    assert "ecommerce_total_revenue" in graph.metrics
    assert "ecommerce_average_order_value" in graph.metrics
    assert "ecommerce_revenue_per_customer" in graph.metrics
    assert "ecommerce_items_per_order" in graph.metrics
    assert "ecommerce_gross_profit" in graph.metrics


def test_metricflow_advanced_dimensions():
    """Test MetricFlow adapter with advanced dimension features."""
    adapter = MetricFlowAdapter()
    graph = adapter.parse(Path("examples/metricflow/advanced_dimensions.yml"))

    # Check transactions model
    assert "transactions" in graph.models
    transactions = graph.get_model("transactions")
    assert transactions.table == "public.transactions"
    assert transactions.primary_key == "transaction_id"

    # Check multiple time dimensions with different granularities
    tx_date = transactions.get_dimension("transaction_date")
    assert tx_date is not None
    assert tx_date.type == "time"
    assert tx_date.granularity == "day"

    tx_week = transactions.get_dimension("transaction_week")
    assert tx_week is not None
    assert tx_week.type == "time"
    assert tx_week.granularity == "week"

    tx_month = transactions.get_dimension("transaction_month")
    assert tx_month is not None
    assert tx_month.granularity == "month"

    tx_quarter = transactions.get_dimension("transaction_quarter")
    assert tx_quarter is not None
    assert tx_quarter.granularity == "quarter"

    tx_year = transactions.get_dimension("transaction_year")
    assert tx_year is not None
    assert tx_year.granularity == "year"

    # Check categorical dimensions
    tx_type = transactions.get_dimension("transaction_type")
    assert tx_type is not None
    assert tx_type.type == "categorical"

    payment = transactions.get_dimension("payment_method")
    assert payment is not None
    assert payment.type == "categorical"

    # Check boolean-style dimensions
    is_online = transactions.get_dimension("is_online")
    assert is_online is not None
    assert is_online.type == "categorical"
    assert "=" in is_online.sql

    # Check dimensions with complex expressions
    location = transactions.get_dimension("transaction_location")
    assert location is not None
    assert "CONCAT" in location.sql

    hour_of_day = transactions.get_dimension("hour_of_day")
    assert hour_of_day is not None
    assert "EXTRACT" in hour_of_day.sql

    # Check various aggregation types in measures
    tx_count = transactions.get_metric("transaction_count")
    assert tx_count is not None
    assert tx_count.agg == "count"

    tx_total = transactions.get_metric("transaction_total")
    assert tx_total is not None
    assert tx_total.agg == "sum"

    avg_tx = transactions.get_metric("avg_transaction_value")
    assert avg_tx is not None
    assert avg_tx.agg == "avg"

    min_tx = transactions.get_metric("min_transaction_value")
    assert min_tx is not None
    assert min_tx.agg == "min"

    max_tx = transactions.get_metric("max_transaction_value")
    assert max_tx is not None
    assert max_tx.agg == "max"

    median_tx = transactions.get_metric("median_transaction_value")
    assert median_tx is not None
    assert median_tx.agg == "median"

    distinct_customers = transactions.get_metric("distinct_customers")
    assert distinct_customers is not None
    assert distinct_customers.agg == "count_distinct"

    # Check measures with complex expressions
    refund_amount = transactions.get_metric("refund_amount")
    assert refund_amount is not None
    assert "CASE" in refund_amount.sql

    # Check inventory model with non-additive dimension
    assert "inventory" in graph.models
    inventory = graph.get_model("inventory")
    assert inventory.table == "public.inventory"

    inv_qty = inventory.get_metric("inventory_quantity")
    assert inv_qty is not None
    assert inv_qty.non_additive_dimension == "snapshot_date"

    inv_value = inventory.get_metric("inventory_value")
    assert inv_value is not None
    assert inv_value.non_additive_dimension == "snapshot_date"

    # Check graph-level metrics with filters
    assert "online_revenue" in graph.metrics
    online_revenue = graph.get_metric("online_revenue")
    assert online_revenue.filters is not None
    assert len(online_revenue.filters) > 0


def test_metricflow_aggregation_types():
    """Test that MetricFlow adapter correctly maps all aggregation types."""
    adapter = MetricFlowAdapter()
    graph = adapter.parse(Path("examples/metricflow/advanced_metrics.yml"))

    orders = graph.get_model("orders")

    # Test aggregation type mappings
    sum_measure = orders.get_metric("order_total")
    assert sum_measure.agg == "sum"

    count_measure = orders.get_metric("order_count")
    assert count_measure.agg == "count"

    avg_measure = orders.get_metric("avg_order_value")
    assert avg_measure.agg == "avg"

    min_measure = orders.get_metric("min_order_value")
    assert min_measure.agg == "min"

    max_measure = orders.get_metric("max_order_value")
    assert max_measure.agg == "max"

    count_distinct_measure = orders.get_metric("distinct_customers")
    assert count_distinct_measure.agg == "count_distinct"


def test_metricflow_dimension_types():
    """Test that MetricFlow adapter correctly parses dimension types."""
    adapter = MetricFlowAdapter()
    graph = adapter.parse(Path("examples/metricflow/advanced_dimensions.yml"))

    transactions = graph.get_model("transactions")

    # Time dimensions should preserve granularity
    time_dims = [
        ("transaction_date", "day"),
        ("transaction_week", "week"),
        ("transaction_month", "month"),
        ("transaction_quarter", "quarter"),
        ("transaction_year", "year"),
    ]

    for dim_name, expected_granularity in time_dims:
        dim = transactions.get_dimension(dim_name)
        assert dim is not None
        assert dim.type == "time"
        assert dim.granularity == expected_granularity

    # Categorical dimensions
    categorical_dims = [
        "transaction_type",
        "payment_method",
        "channel",
        "is_online",
        "is_refund",
    ]

    for dim_name in categorical_dims:
        dim = transactions.get_dimension(dim_name)
        assert dim is not None
        assert dim.type == "categorical"


def test_metricflow_metric_dependencies():
    """Test that MetricFlow adapter correctly parses metric dependencies."""
    adapter = MetricFlowAdapter()
    graph = adapter.parse(Path("examples/metricflow/advanced_metrics.yml"))

    # Derived metrics should have expressions referencing other metrics
    gross_profit = graph.get_metric("order_gross_profit")
    assert gross_profit.type == "derived"
    assert "revenue" in gross_profit.sql.lower()
    assert "cost" in gross_profit.sql.lower()

    # Ratio metrics should have numerator and denominator
    avg_order_value = graph.get_metric("average_order_value")
    assert avg_order_value.type == "ratio"
    assert avg_order_value.numerator is not None
    assert avg_order_value.denominator is not None


def test_metricflow_coalesce_2023_orders():
    """Test MetricFlow adapter with Coalesce 2023 orders example."""
    adapter = MetricFlowAdapter()
    graph = adapter.parse(Path("examples/metricflow/coalesce_2023_orders.yml"))

    # Check semantic model
    assert "orders" in graph.models
    orders = graph.get_model("orders")
    assert orders.primary_key == "order_id"
    assert orders.table == "fct_orders"

    # Check entities/relationships
    assert len(orders.relationships) == 2
    location_rel = next((r for r in orders.relationships if r.name == "location"), None)
    assert location_rel is not None
    assert location_rel.type == "many_to_one"
    assert location_rel.foreign_key == "location_id"

    customer_rel = next((r for r in orders.relationships if r.name == "customer"), None)
    assert customer_rel is not None
    assert customer_rel.foreign_key == "customer_id"

    # Check dimensions
    ordered_at = orders.get_dimension("ordered_at")
    assert ordered_at is not None
    assert ordered_at.type == "time"
    assert ordered_at.granularity == "day"

    is_food_order = orders.get_dimension("is_food_order")
    assert is_food_order is not None
    assert is_food_order.type == "categorical"

    # Check measures with various aggregations
    order_total = orders.get_metric("order_total")
    assert order_total is not None
    assert order_total.agg == "sum"

    order_count = orders.get_metric("order_count")
    assert order_count is not None
    assert order_count.agg == "sum"
    assert order_count.sql == "1"

    customers_with_orders = orders.get_metric("customers_with_orders")
    assert customers_with_orders is not None
    assert customers_with_orders.agg == "count_distinct"
    assert customers_with_orders.sql == "customer_id"

    # Check percentile measure - NOT YET SUPPORTED, skip for now
    # order_value_p99 = orders.get_metric("order_value_p99")
    # assert order_value_p99 is not None
    # assert order_value_p99.agg == "percentile"


def test_metricflow_coalesce_2023_customers():
    """Test MetricFlow adapter with Coalesce 2023 customers example."""
    adapter = MetricFlowAdapter()
    graph = adapter.parse(Path("examples/metricflow/coalesce_2023_customers.yml"))

    # Check semantic model
    assert "customers" in graph.models
    customers = graph.get_model("customers")
    # Primary key uses the expr from the entity definition
    assert customers.primary_key == "customer_id"
    assert customers.table == "dim_customers"

    # Check time dimensions
    first_ordered = customers.get_dimension("first_ordered_at")
    assert first_ordered is not None
    assert first_ordered.type == "time"
    assert first_ordered.granularity == "day"

    last_ordered = customers.get_dimension("last_ordered_at")
    assert last_ordered is not None
    assert last_ordered.type == "time"

    # Check categorical dimensions
    customer_type = customers.get_dimension("customer_type")
    assert customer_type is not None
    assert customer_type.type == "categorical"

    # Check measures
    lifetime_spend = customers.get_metric("lifetime_spend")
    assert lifetime_spend is not None
    assert lifetime_spend.agg == "sum"

    customer_count = customers.get_metric("customers")
    assert customer_count is not None
    assert customer_count.agg == "count_distinct"

    # Check graph-level metrics
    assert "customers" in graph.metrics
    customers_metric = graph.get_metric("customers")
    assert customers_metric.type is None  # Simple metric
    assert customers_metric.sql == "customers"

    assert "customers_with_orders" in graph.metrics


def test_metricflow_coalesce_2023_order_items():
    """Test MetricFlow adapter with Coalesce 2023 order items example."""
    adapter = MetricFlowAdapter()
    graph = adapter.parse(Path("examples/metricflow/coalesce_2023_order_items.yml"))

    # Check semantic model
    assert "order_item" in graph.models
    order_items = graph.get_model("order_item")
    assert order_items.primary_key == "order_item_id"

    # Check foreign entities
    assert len(order_items.relationships) == 2
    order_rel = next((r for r in order_items.relationships if r.name == "order_id"), None)
    assert order_rel is not None
    assert order_rel.foreign_key == "order_id"

    product_rel = next((r for r in order_items.relationships if r.name == "product"), None)
    assert product_rel is not None
    assert product_rel.foreign_key == "product_id"

    # Check dimensions
    ordered_at = order_items.get_dimension("ordered_at")
    assert ordered_at is not None
    assert ordered_at.type == "time"

    is_food = order_items.get_dimension("is_food_item")
    assert is_food is not None

    is_drink = order_items.get_dimension("is_drink_item")
    assert is_drink is not None

    # Check conditional revenue measures
    revenue = order_items.get_metric("revenue")
    assert revenue is not None
    assert revenue.agg == "sum"
    assert revenue.sql == "product_price"

    food_revenue = order_items.get_metric("food_revenue")
    assert food_revenue is not None
    assert food_revenue.agg == "sum"
    assert "case when is_food_item" in food_revenue.sql.lower()

    drink_revenue = order_items.get_metric("drink_revenue")
    assert drink_revenue is not None
    assert "case when is_drink_item" in drink_revenue.sql.lower()


def test_metricflow_conversion_metrics():
    """Test MetricFlow adapter with conversion metrics."""
    adapter = MetricFlowAdapter()
    graph = adapter.parse(Path("examples/metricflow/conversion_metrics.yml"))

    # Check semantic model
    assert "user_events" in graph.models
    events = graph.get_model("user_events")
    assert events.primary_key == "user_id"

    # Check measures for conversion
    visits = events.get_metric("visits")
    assert visits is not None
    assert visits.agg == "sum"

    buys = events.get_metric("buys")
    assert buys is not None

    # Check conversion metrics - NOTE: conversion type not yet supported in adapter
    # Conversion metrics are skipped during parsing (return None) for unsupported types
    # Verify they're not in the graph
    assert "visit_to_buy_conversion_rate" not in graph.metrics
    assert "visit_to_buy_conversions_1_week" not in graph.metrics
    assert "view_to_purchase_same_product" not in graph.metrics

    # Test that parsing doesn't fail even with unsupported conversion type
    assert graph is not None
    assert len(graph.models) > 0


def test_metricflow_cumulative_grain_to_date():
    """Test MetricFlow adapter with cumulative metrics and grain_to_date."""
    adapter = MetricFlowAdapter()
    graph = adapter.parse(Path("examples/metricflow/cumulative_metrics_grain_to_date.yml"))

    # Check semantic model
    assert "revenue_transactions" in graph.models
    transactions = graph.get_model("revenue_transactions")
    assert transactions.primary_key == "transaction_id"

    # Check measures
    revenue = transactions.get_metric("revenue")
    assert revenue is not None
    assert revenue.agg == "sum"

    order_total = transactions.get_metric("order_total")
    assert order_total is not None

    # Check cumulative metrics with window
    assert "cumulative_revenue_all_time" in graph.metrics
    all_time = graph.get_metric("cumulative_revenue_all_time")
    assert all_time.type == "cumulative"
    assert all_time.window is None  # No window = all time

    assert "cumulative_revenue_7_days" in graph.metrics
    seven_days = graph.get_metric("cumulative_revenue_7_days")
    assert seven_days.type == "cumulative"
    assert seven_days.window == "7 days"

    # Check grain_to_date cumulative metrics
    assert "cumulative_order_total_mtd" in graph.metrics
    mtd = graph.get_metric("cumulative_order_total_mtd")
    assert mtd.type == "cumulative"
    assert mtd.grain_to_date == "month"

    assert "cumulative_revenue_ytd" in graph.metrics
    ytd = graph.get_metric("cumulative_revenue_ytd")
    assert ytd.type == "cumulative"
    assert ytd.grain_to_date == "year"

    assert "cumulative_revenue_wtd" in graph.metrics
    wtd = graph.get_metric("cumulative_revenue_wtd")
    assert wtd.grain_to_date == "week"


def test_metricflow_jaffle_sl_testing():
    """Test MetricFlow adapter with jaffle-sl-testing order items example."""
    adapter = MetricFlowAdapter()
    graph = adapter.parse(Path("examples/metricflow/jaffle_sl_testing_order_items.yml"))

    # Check semantic model
    assert "order_items" in graph.models
    order_items = graph.get_model("order_items")
    assert order_items.primary_key == "order_item_id"

    # Check relationships
    assert len(order_items.relationships) == 2

    # Check dimensions
    ordered_at = order_items.get_dimension("ordered_at")
    assert ordered_at is not None
    assert ordered_at.type == "time"
    assert ordered_at.granularity == "day"

    # Check measures including median aggregation
    revenue = order_items.get_metric("revenue")
    assert revenue is not None
    assert revenue.agg == "sum"
    assert revenue.sql == "subtotal"

    median_revenue = order_items.get_metric("median_revenue")
    assert median_revenue is not None
    assert median_revenue.agg == "median"
    assert median_revenue.sql == "subtotal"

    # Check conditional measures
    food_revenue = order_items.get_metric("food_revenue")
    assert food_revenue is not None
    assert "case when" in food_revenue.sql.lower()

    # Check graph-level metric
    assert "revenue" in graph.metrics
    revenue_metric = graph.get_metric("revenue")
    assert revenue_metric.type is None  # Simple metric
    assert revenue_metric.sql == "revenue"


def test_metricflow_saved_queries():
    """Test MetricFlow adapter with saved queries example."""
    adapter = MetricFlowAdapter()
    graph = adapter.parse(Path("examples/metricflow/saved_queries_example.yml"))

    # Check semantic model
    assert "sales_data" in graph.models
    sales = graph.get_model("sales_data")
    assert sales.primary_key == "sale_id"

    # Check dimensions
    sale_date = sales.get_dimension("sale_date")
    assert sale_date is not None
    assert sale_date.type == "time"

    region = sales.get_dimension("region")
    assert region is not None
    assert region.type == "categorical"

    # Check measures
    sales_amount = sales.get_metric("sales_amount")
    assert sales_amount is not None
    assert sales_amount.agg == "sum"

    sales_count = sales.get_metric("sales_count")
    assert sales_count is not None
    assert sales_count.agg == "count"

    # Check graph-level metrics
    assert "total_sales" in graph.metrics
    total_sales = graph.get_metric("total_sales")
    assert total_sales.type is None  # Simple metric
    assert total_sales.sql == "sales_amount"

    assert "sales_transactions" in graph.metrics
    transactions = graph.get_metric("sales_transactions")
    assert transactions.sql == "sales_count"


def test_lookml_adapter_basic():
    """Test LookML adapter with basic orders example."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("examples/lookml/orders.lkml"))

    # Check models were imported
    assert "orders" in graph.models
    assert "customers" in graph.models

    # Check orders model
    orders = graph.get_model("orders")
    assert orders.table == "public.orders"
    assert orders.primary_key == "id"
    assert len(orders.dimensions) > 0
    assert len(orders.metrics) > 0

    # Check dimensions
    status_dim = orders.get_dimension("status")
    assert status_dim is not None
    assert status_dim.type == "categorical"

    customer_id_dim = orders.get_dimension("customer_id")
    assert customer_id_dim is not None
    assert customer_id_dim.type == "numeric"

    # Check time dimension groups were parsed
    created_date = orders.get_dimension("created_date")
    assert created_date is not None
    assert created_date.type == "time"
    assert created_date.granularity == "day"

    created_week = orders.get_dimension("created_week")
    assert created_week is not None
    assert created_week.type == "time"
    assert created_week.granularity == "week"

    created_month = orders.get_dimension("created_month")
    assert created_month is not None
    assert created_month.type == "time"
    assert created_month.granularity == "month"

    # Check basic measures
    count_measure = orders.get_metric("count")
    assert count_measure is not None
    assert count_measure.agg == "count"

    revenue_measure = orders.get_metric("revenue")
    assert revenue_measure is not None
    assert revenue_measure.agg == "sum"

    avg_measure = orders.get_metric("avg_order_value")
    assert avg_measure is not None
    assert avg_measure.agg == "avg"

    # Check filtered measure
    completed_revenue = orders.get_metric("completed_revenue")
    assert completed_revenue is not None
    assert completed_revenue.agg == "sum"
    assert completed_revenue.filters is not None
    assert len(completed_revenue.filters) > 0

    # Check derived measure (ratio metric)
    conversion_rate = orders.get_metric("conversion_rate")
    assert conversion_rate is not None
    assert conversion_rate.type == "derived"
    assert conversion_rate.sql is not None

    # Check segments
    assert len(orders.segments) > 0
    high_value_segment = next((s for s in orders.segments if s.name == "high_value"), None)
    assert high_value_segment is not None

    # Check customers model
    customers = graph.get_model("customers")
    assert customers.table == "public.customers"
    assert customers.primary_key == "id"


def test_lookml_adapter_ecommerce():
    """Test LookML adapter with comprehensive ecommerce example."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("examples/lookml/ecommerce.lkml"))

    # Check all models were imported
    assert "orders" in graph.models
    assert "customers" in graph.models
    assert "products" in graph.models
    assert "order_items" in graph.models

    # Check orders model
    orders = graph.get_model("orders")
    assert orders.table == "ecommerce.orders"

    # Test various dimension types
    amount_dim = orders.get_dimension("amount")
    assert amount_dim is not None
    assert amount_dim.type == "numeric"

    status_dim = orders.get_dimension("status")
    assert status_dim is not None
    assert status_dim.type == "categorical"

    # Test dimension group with multiple timeframes
    created_time = orders.get_dimension("created_time")
    assert created_time is not None
    assert created_time.granularity == "hour"

    created_date = orders.get_dimension("created_date")
    assert created_date is not None
    assert created_date.granularity == "day"

    created_week = orders.get_dimension("created_week")
    assert created_week is not None
    assert created_week.granularity == "week"

    created_quarter = orders.get_dimension("created_quarter")
    assert created_quarter is not None
    assert created_quarter.granularity == "quarter"

    # Test various measure types
    count_measure = orders.get_metric("count")
    assert count_measure.agg == "count"

    sum_measure = orders.get_metric("total_revenue")
    assert sum_measure.agg == "sum"

    avg_measure = orders.get_metric("avg_order_value")
    assert avg_measure.agg == "avg"

    min_measure = orders.get_metric("min_order_value")
    assert min_measure.agg == "min"

    max_measure = orders.get_metric("max_order_value")
    assert max_measure.agg == "max"

    # Test filtered measures
    delivered_orders = orders.get_metric("delivered_orders")
    assert delivered_orders is not None
    assert delivered_orders.agg == "count"
    assert delivered_orders.filters is not None

    high_value_orders = orders.get_metric("high_value_orders")
    assert high_value_orders is not None
    assert high_value_orders.filters is not None

    # Test derived measure
    avg_discount = orders.get_metric("avg_discount_percentage")
    assert avg_discount is not None
    assert avg_discount.type == "derived"

    # Test segments
    assert len(orders.segments) == 3
    segment_names = [s.name for s in orders.segments]
    assert "delivered" in segment_names
    assert "high_value" in segment_names
    assert "international" in segment_names

    # Check customers model
    customers = graph.get_model("customers")
    assert customers.table == "ecommerce.customers"

    # Test count_distinct measure
    count_active = customers.get_metric("count_active")
    assert count_active is not None
    assert count_active.agg == "count_distinct"
    assert count_active.filters is not None

    # Check products model
    products = graph.get_model("products")
    assert products.table == "ecommerce.products"

    # Check order_items model
    order_items = graph.get_model("order_items")
    assert order_items.table == "ecommerce.order_items"

    # Test count_distinct on order_items
    distinct_products = order_items.get_metric("distinct_products_sold")
    assert distinct_products is not None
    assert distinct_products.agg == "count_distinct"


def test_lookml_adapter_advanced_measures():
    """Test LookML adapter with advanced measures example."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("examples/lookml/advanced_measures.lkml"))

    # Check models
    assert "sales_analytics" in graph.models
    assert "marketing_campaigns" in graph.models

    # Check sales_analytics model
    sales = graph.get_model("sales_analytics")
    assert sales.table == "analytics.sales"

    # Test count_distinct measures
    unique_customers = sales.get_metric("unique_customers")
    assert unique_customers is not None
    assert unique_customers.agg == "count_distinct"

    unique_products = sales.get_metric("unique_products")
    assert unique_products is not None
    assert unique_products.agg == "count_distinct"

    # Test multiple filtered measures
    online_sales = sales.get_metric("online_sales")
    assert online_sales is not None
    assert online_sales.agg == "sum"
    assert online_sales.filters is not None

    online_large_sales = sales.get_metric("online_large_sales")
    assert online_large_sales is not None
    assert online_large_sales.filters is not None
    # Should have 2 filters (channel and sale_amount)
    assert len(online_large_sales.filters) == 2

    # Test derived/ratio measures
    gross_profit = sales.get_metric("gross_profit")
    assert gross_profit is not None
    assert gross_profit.type == "derived"

    profit_margin = sales.get_metric("profit_margin")
    assert profit_margin is not None
    assert profit_margin.type == "derived"

    # Check marketing_campaigns model
    campaigns = graph.get_model("marketing_campaigns")
    assert campaigns.table == "analytics.campaigns"

    # Test ratio metrics in campaigns
    ctr = campaigns.get_metric("click_through_rate")
    assert ctr is not None
    assert ctr.type == "derived"

    conversion_rate = campaigns.get_metric("conversion_rate")
    assert conversion_rate is not None
    assert conversion_rate.type == "derived"


def test_lookml_adapter_explores():
    """Test LookML adapter with explores and joins."""
    adapter = LookMLAdapter()

    # Create a temporary directory with just the files we need
    import tempfile
    import shutil

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Copy the files we need for this test
        shutil.copy("examples/lookml/orders.lkml", tmpdir_path / "orders.lkml")
        shutil.copy("examples/lookml/orders.explore.lkml", tmpdir_path / "orders.explore.lkml")

        # Parse the directory
        graph = adapter.parse(tmpdir_path)

        # Check that all models exist
        assert "orders" in graph.models
        assert "customers" in graph.models

        # Check orders model has relationships
        orders = graph.get_model("orders")

        # The explores are in a separate file, so relationships should be parsed
        assert len(orders.relationships) > 0

        # Find customer relationship
        customer_rel = next((r for r in orders.relationships if r.name == "customers"), None)
        assert customer_rel is not None
        assert customer_rel.type == "many_to_one"
        assert customer_rel.foreign_key == "customer_id"


def test_lookml_adapter_explores_multi_join():
    """Test LookML adapter with multiple joins in explores."""
    adapter = LookMLAdapter()

    # Create a temporary directory with just the ecommerce files
    import tempfile
    import shutil

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Copy the ecommerce files
        shutil.copy("examples/lookml/ecommerce.lkml", tmpdir_path / "ecommerce.lkml")
        shutil.copy("examples/lookml/ecommerce_explores.lkml", tmpdir_path / "ecommerce_explores.lkml")

        # Parse the directory
        graph = adapter.parse(tmpdir_path)

        # Check that ecommerce models were parsed
        assert "orders" in graph.models
        assert "customers" in graph.models
        assert "products" in graph.models
        assert "order_items" in graph.models

        # Check orders relationships
        orders = graph.get_model("orders")
        assert len(orders.relationships) > 0

        # Check for various join types
        relationships_by_name = {r.name: r for r in orders.relationships}

        # Verify customers relationship is many_to_one
        assert "customers" in relationships_by_name
        assert relationships_by_name["customers"].type == "many_to_one"
        assert relationships_by_name["customers"].foreign_key == "customer_id"

        # Verify order_items relationship is one_to_many
        assert "order_items" in relationships_by_name
        assert relationships_by_name["order_items"].type == "one_to_many"
        assert relationships_by_name["order_items"].foreign_key == "id"

        # Verify products relationship exists (through order_items)
        assert "products" in relationships_by_name
        assert relationships_by_name["products"].type == "many_to_one"

        # Check customers explore
        customers = graph.get_model("customers")
        if len(customers.relationships) > 0:
            customer_rels = {r.name: r for r in customers.relationships}
            # Customers should have one_to_many to orders
            if "orders" in customer_rels:
                assert customer_rels["orders"].type == "one_to_many"


def test_lookml_adapter_derived_tables():
    """Test LookML adapter with derived tables."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("examples/lookml/derived_tables.lkml"))

    # Check model was created from derived table
    assert "customer_summary" in graph.models

    customer_summary = graph.get_model("customer_summary")

    # Should not have a table name, but should have SQL
    assert customer_summary.table is None
    assert customer_summary.sql is not None
    assert "SELECT" in customer_summary.sql.upper()

    # Check primary key
    assert customer_summary.primary_key == "customer_id"

    # Check dimensions
    customer_id_dim = customer_summary.get_dimension("customer_id")
    assert customer_id_dim is not None

    order_count_dim = customer_summary.get_dimension("order_count")
    assert order_count_dim is not None
    assert order_count_dim.type == "numeric"

    # Check time dimension group
    last_order_date = customer_summary.get_dimension("last_order_date")
    assert last_order_date is not None
    assert last_order_date.type == "time"

    # Check measures
    total_customers = customer_summary.get_metric("total_customers")
    assert total_customers is not None
    assert total_customers.agg == "count"

    avg_orders = customer_summary.get_metric("avg_orders_per_customer")
    assert avg_orders is not None
    assert avg_orders.agg == "avg"


def test_lookml_adapter_export():
    """Test LookML adapter export functionality."""
    adapter = LookMLAdapter()

    # Parse a simple example
    graph = adapter.parse(Path("examples/lookml/orders.lkml"))

    # Export to a temporary file
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.lkml', delete=False) as f:
        output_path = f.name

    try:
        adapter.export(graph, output_path)

        # Read back and verify structure
        adapter2 = LookMLAdapter()
        graph2 = adapter2.parse(output_path)

        # Check that models were preserved
        assert "orders" in graph2.models
        assert "customers" in graph2.models

        # Check basic structure is preserved
        orders = graph2.get_model("orders")
        assert orders.table == "public.orders"
        assert len(orders.dimensions) > 0
        assert len(orders.metrics) > 0
    finally:
        # Clean up
        import os
        if os.path.exists(output_path):
            os.remove(output_path)


def test_lookml_thelook_orders():
    """Test LookML adapter with real thelook orders example from looker-open-source."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("examples/lookml/thelook_orders.view.lkml"))

    # Check model was imported
    assert "orders" in graph.models
    orders = graph.get_model("orders")

    # Check primary key
    assert orders.primary_key == "id"

    # Check dimensions
    id_dim = orders.get_dimension("id")
    assert id_dim is not None
    assert id_dim.type == "numeric"

    status_dim = orders.get_dimension("status")
    assert status_dim is not None
    assert status_dim.type == "categorical"

    traffic_source_dim = orders.get_dimension("traffic_source")
    assert traffic_source_dim is not None

    user_id_dim = orders.get_dimension("user_id")
    assert user_id_dim is not None
    assert user_id_dim.type == "numeric"

    # Check complex dimensions with SQL
    total_amount_dim = orders.get_dimension("total_amount_of_order_usd")
    assert total_amount_dim is not None
    assert total_amount_dim.type == "numeric"
    assert "SELECT SUM" in total_amount_dim.sql

    order_profit_dim = orders.get_dimension("order_profit")
    assert order_profit_dim is not None
    # Should reference other dimensions
    assert "total_amount_of_order_usd" in order_profit_dim.sql or "total_cost_of_order" in order_profit_dim.sql

    # Check yesno dimension
    is_first_purchase_dim = orders.get_dimension("is_first_purchase")
    assert is_first_purchase_dim is not None
    assert is_first_purchase_dim.type == "categorical"  # yesno maps to categorical

    # Check time dimension group
    created_date = orders.get_dimension("created_date")
    assert created_date is not None
    assert created_date.type == "time"
    assert created_date.granularity == "day"

    created_week = orders.get_dimension("created_week")
    assert created_week is not None
    assert created_week.granularity == "week"

    created_month = orders.get_dimension("created_month")
    assert created_month is not None
    assert created_month.granularity == "month"

    # Check measures
    count_measure = orders.get_metric("count")
    assert count_measure is not None
    assert count_measure.agg == "count"

    revenue_measure = orders.get_metric("total_revenue")
    assert revenue_measure is not None
    assert revenue_measure.agg == "sum"


def test_lookml_thelook_users():
    """Test LookML adapter with real thelook users example."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("examples/lookml/thelook_users.view.lkml"))

    assert "users" in graph.models
    users = graph.get_model("users")

    # Check primary key
    assert users.primary_key == "id"

    # Check various dimension types
    age_dim = users.get_dimension("age")
    assert age_dim is not None
    assert age_dim.type == "numeric"

    city_dim = users.get_dimension("city")
    assert city_dim is not None
    assert city_dim.type == "categorical"

    email_dim = users.get_dimension("email")
    assert email_dim is not None

    # Check geographic dimensions
    lat_dim = users.get_dimension("lat")
    assert lat_dim is not None
    assert lat_dim.type == "numeric"

    lng_dim = users.get_dimension("lng")
    assert lng_dim is not None
    assert lng_dim.type == "numeric"

    # Check zipcode dimension (special type)
    zipcode_dim = users.get_dimension("zipcode")
    assert zipcode_dim is not None

    # Check time dimension group
    created_date = users.get_dimension("created_date")
    assert created_date is not None
    assert created_date.type == "time"

    # Check measure
    count_measure = users.get_metric("count")
    assert count_measure is not None
    assert count_measure.agg == "count"


def test_lookml_thelook_products():
    """Test LookML adapter with real thelook products example."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("examples/lookml/thelook_products.view.lkml"))

    assert "products" in graph.models
    products = graph.get_model("products")

    # Check dimensions
    brand_dim = products.get_dimension("brand")
    assert brand_dim is not None

    category_dim = products.get_dimension("category")
    assert category_dim is not None

    department_dim = products.get_dimension("department")
    assert department_dim is not None

    retail_price_dim = products.get_dimension("retail_price")
    assert retail_price_dim is not None
    assert retail_price_dim.type == "numeric"

    sku_dim = products.get_dimension("sku")
    assert sku_dim is not None

    # Check measure
    count_measure = products.get_metric("count")
    assert count_measure is not None
    assert count_measure.agg == "count"


def test_lookml_thelook_order_items():
    """Test LookML adapter with real thelook order_items example."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("examples/lookml/thelook_order_items.view.lkml"))

    assert "order_items" in graph.models
    order_items = graph.get_model("order_items")

    # Check primary key
    assert order_items.primary_key == "id"

    # Check foreign key dimensions
    inventory_item_id_dim = order_items.get_dimension("inventory_item_id")
    assert inventory_item_id_dim is not None
    assert inventory_item_id_dim.type == "numeric"

    order_id_dim = order_items.get_dimension("order_id")
    assert order_id_dim is not None
    assert order_id_dim.type == "numeric"

    # Check time dimensions with multiple granularities
    created_date = order_items.get_dimension("created_date")
    assert created_date is not None
    assert created_date.type == "time"

    created_week = order_items.get_dimension("created_week")
    assert created_week is not None

    # Check returned time dimension group
    returned_date = order_items.get_dimension("returned_date")
    assert returned_date is not None
    assert returned_date.type == "time"

    # Check numeric dimension
    sale_price_dim = order_items.get_dimension("sale_price")
    assert sale_price_dim is not None
    assert sale_price_dim.type == "numeric"

    # Check measures
    count_measure = order_items.get_metric("count")
    assert count_measure is not None
    assert count_measure.agg == "count"

    total_sale_price = order_items.get_metric("total_sale_price")
    assert total_sale_price is not None
    assert total_sale_price.agg == "sum"

    avg_sale_price = order_items.get_metric("average_sale_price")
    assert avg_sale_price is not None
    assert avg_sale_price.agg == "avg"


def test_lookml_thelook_inventory_items():
    """Test LookML adapter with real thelook inventory_items example."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("examples/lookml/thelook_inventory_items.view.lkml"))

    assert "inventory_items" in graph.models
    inventory = graph.get_model("inventory_items")

    # Check dimensions
    cost_dim = inventory.get_dimension("cost")
    assert cost_dim is not None
    assert cost_dim.type == "numeric"

    product_id_dim = inventory.get_dimension("product_id")
    assert product_id_dim is not None
    assert product_id_dim.type == "numeric"

    # Check time dimensions with convert_tz: no
    created_date = inventory.get_dimension("created_date")
    assert created_date is not None
    assert created_date.type == "time"

    sold_date = inventory.get_dimension("sold_date")
    assert sold_date is not None
    assert sold_date.type == "time"

    # Check measure
    count_measure = inventory.get_metric("count")
    assert count_measure is not None


def test_lookml_bq_thelook_events():
    """Test LookML adapter with BigQuery thelook events example (compact syntax)."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("examples/lookml/bq_thelook_events.view.lkml"))

    assert "events" in graph.models
    events = graph.get_model("events")

    # Check sql_table_name was parsed
    assert events.table == "thelook_web_analytics.events"

    # Check primary key with compact syntax
    assert events.primary_key == "id"

    # Check compact dimension definitions (no sql: ${TABLE}.field)
    browser_dim = events.get_dimension("browser")
    assert browser_dim is not None

    city_dim = events.get_dimension("city")
    assert city_dim is not None

    event_type_dim = events.get_dimension("event_type")
    assert event_type_dim is not None

    # Check numeric dimensions
    latitude_dim = events.get_dimension("latitude")
    assert latitude_dim is not None
    assert latitude_dim.type == "numeric"

    sequence_num_dim = events.get_dimension("sequence_number")
    assert sequence_num_dim is not None
    assert sequence_num_dim.type == "numeric"

    # Check dimension with complex SQL (REGEXP_EXTRACT)
    user_id_dim = events.get_dimension("user_id")
    assert user_id_dim is not None
    assert user_id_dim.type == "numeric"
    assert "REGEXP_EXTRACT" in user_id_dim.sql or "CAST" in user_id_dim.sql

    # Check time dimension (defaults to "date" when no timeframes specified)
    created_date = events.get_dimension("created_date")
    assert created_date is not None
    assert created_date.type == "time"
    assert created_date.granularity == "day"

    # Check basic measure
    count_measure = events.get_metric("count")
    assert count_measure is not None
    assert count_measure.agg == "count"

    # Check derived/custom measures with SQL only (no type:)
    min_time = events.get_metric("minimum_time")
    assert min_time is not None
    # These are "derived" because they have custom SQL but no agg type
    assert min_time.type == "derived"

    max_time = events.get_metric("max_time")
    assert max_time is not None
    assert max_time.type == "derived"

    # Check measures with array aggregations
    ip_addresses = events.get_metric("ip_addresses")
    assert ip_addresses is not None
    assert "ARRAY_TO_STRING" in ip_addresses.sql or "ARRAY_AGG" in ip_addresses.sql

    event_types = events.get_metric("event_types")
    assert event_types is not None

    # Check min aggregation type
    first_user_id = events.get_metric("first_user_id")
    assert first_user_id is not None
    assert first_user_id.agg == "min"


def test_lookml_bq_thelook_inventory_compact():
    """Test LookML adapter with compact syntax (no ${TABLE} references)."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("examples/lookml/bq_thelook_inventory.view.lkml"))

    assert "inventory_items" in graph.models
    inventory = graph.get_model("inventory_items")

    # Check table name
    assert inventory.table == "thelook_web_analytics.inventory_items"

    # Check compact dimensions
    product_brand_dim = inventory.get_dimension("product_brand")
    assert product_brand_dim is not None

    product_category_dim = inventory.get_dimension("product_category")
    assert product_category_dim is not None

    product_name_dim = inventory.get_dimension("product_name")
    assert product_name_dim is not None

    # Check numeric dimensions
    cost_dim = inventory.get_dimension("cost")
    assert cost_dim is not None
    assert cost_dim.type == "numeric"

    retail_price_dim = inventory.get_dimension("product_retail_price")
    assert retail_price_dim is not None
    assert retail_price_dim.type == "numeric"

    # Check measure
    count_measure = inventory.get_metric("count")
    assert count_measure is not None


def test_lookml_bq_thelook_distribution_centers():
    """Test LookML adapter with geographic dimensions."""
    adapter = LookMLAdapter()
    graph = adapter.parse(Path("examples/lookml/bq_thelook_distribution_centers.view.lkml"))

    assert "distribution_centers" in graph.models
    dc = graph.get_model("distribution_centers")

    # Check table
    assert dc.table == "thelook_web_analytics.distribution_centers"

    # Check geographic dimensions
    latitude_dim = dc.get_dimension("latitude")
    assert latitude_dim is not None
    assert latitude_dim.type == "numeric"

    longitude_dim = dc.get_dimension("longitude")
    assert longitude_dim is not None
    assert longitude_dim.type == "numeric"

    name_dim = dc.get_dimension("name")
    assert name_dim is not None

    # Check measure
    count_measure = dc.get_metric("count")
    assert count_measure is not None


def test_cube_saas_analytics():
    """Test Cube adapter with SaaS analytics example."""
    adapter = CubeAdapter()
    graph = adapter.parse(Path("examples/cube/saas_analytics.yml"))

    # Check all models were imported
    assert "users" in graph.models
    assert "subscriptions" in graph.models
    assert "events" in graph.models

    # Check users model
    users = graph.get_model("users")
    assert users.table == "public.users"
    assert len(users.dimensions) >= 5
    assert len(users.metrics) >= 2
    assert len(users.segments) == 2

    # Check user dimensions
    email_dim = users.get_dimension("email")
    assert email_dim is not None
    assert email_dim.type == "categorical"

    city_dim = users.get_dimension("city")
    assert city_dim is not None

    signup_date_dim = users.get_dimension("signup_date")
    assert signup_date_dim is not None
    assert signup_date_dim.type == "time"

    # Check user measures
    count_measure = users.get_metric("count")
    assert count_measure is not None
    assert count_measure.agg == "count"

    # Check user segments
    sf_segment = next((s for s in users.segments if s.name == "sf_users"), None)
    assert sf_segment is not None
    assert "San Francisco" in sf_segment.sql or "CA" in sf_segment.sql

    # Check subscriptions model
    subscriptions = graph.get_model("subscriptions")
    assert subscriptions.table == "public.subscriptions"
    assert len(subscriptions.relationships) == 1

    # Check subscription relationship
    user_rel = next((r for r in subscriptions.relationships if r.name == "users"), None)
    assert user_rel is not None
    assert user_rel.type == "many_to_one"

    # Check subscription dimensions
    plan_dim = subscriptions.get_dimension("plan")
    assert plan_dim is not None

    status_dim = subscriptions.get_dimension("status")
    assert status_dim is not None

    # Check subscription measures
    active_subs = subscriptions.get_metric("active_subscriptions")
    assert active_subs is not None
    assert active_subs.filters is not None

    # Check subscription pre-aggregations
    assert len(subscriptions.pre_aggregations) == 1
    preagg = subscriptions.pre_aggregations[0]
    assert preagg.name == "subscriptions_by_plan"
    assert preagg.granularity == "day"
    assert preagg.partition_granularity == "month"

    # Check events model
    events = graph.get_model("events")
    assert events.table == "public.events"
    assert len(events.segments) == 2

    # Check event measures
    unique_users = events.get_metric("unique_users")
    assert unique_users is not None
    assert unique_users.agg == "count_distinct"


def test_cube_retail_analytics():
    """Test Cube adapter with retail analytics example."""
    adapter = CubeAdapter()
    graph = adapter.parse(Path("examples/cube/retail_analytics.yml"))

    # Check all models were imported
    assert "products" in graph.models
    assert "line_items" in graph.models
    assert "orders" in graph.models
    assert "customers" in graph.models

    # Check products model
    products = graph.get_model("products")
    assert products.table == "public.products"

    # Check product dimensions
    category_dim = products.get_dimension("category")
    assert category_dim is not None

    price_dim = products.get_dimension("price")
    assert price_dim is not None
    assert price_dim.type == "numeric"

    # Check product measures
    avg_price = products.get_metric("avg_price")
    assert avg_price is not None
    assert avg_price.agg == "avg"

    min_price = products.get_metric("min_price")
    assert min_price is not None
    assert min_price.agg == "min"

    # Check product segments
    electronics_segment = next((s for s in products.segments if s.name == "electronics"), None)
    assert electronics_segment is not None

    # Check line_items model
    line_items = graph.get_model("line_items")
    assert line_items.table == "public.line_items"
    assert len(line_items.relationships) == 2

    # Check line_items joins
    product_rel = next((r for r in line_items.relationships if r.name == "products"), None)
    assert product_rel is not None
    assert product_rel.type == "many_to_one"

    orders_rel = next((r for r in line_items.relationships if r.name == "orders"), None)
    assert orders_rel is not None
    assert orders_rel.type == "many_to_one"

    # Check line_items pre-aggregations
    assert len(line_items.pre_aggregations) == 1
    preagg = line_items.pre_aggregations[0]
    assert preagg.refresh_key is not None
    assert preagg.refresh_key.every == "1 day"

    # Check orders model
    orders = graph.get_model("orders")
    assert len(orders.relationships) == 2

    # Check orders pre-aggregations with indexes
    assert len(orders.pre_aggregations) == 1
    orders_preagg = orders.pre_aggregations[0]
    assert orders_preagg.refresh_key is not None
    assert orders_preagg.refresh_key.incremental is True
    assert orders_preagg.refresh_key.update_window == "7 day"
    assert len(orders_preagg.indexes) == 1
    assert orders_preagg.indexes[0].name == "status_idx"


def test_cube_web_analytics():
    """Test Cube adapter with web analytics example."""
    adapter = CubeAdapter()
    graph = adapter.parse(Path("examples/cube/web_analytics.yml"))

    # Check all models were imported
    assert "page_views" in graph.models
    assert "sessions" in graph.models
    assert "conversions" in graph.models

    # Check page_views model
    page_views = graph.get_model("page_views")
    assert page_views.table == "analytics.page_views"

    # Check page_views dimensions
    url_dim = page_views.get_dimension("url")
    assert url_dim is not None

    duration_dim = page_views.get_dimension("duration")
    assert duration_dim is not None
    assert duration_dim.type == "numeric"

    # Check page_views measures
    unique_sessions = page_views.get_metric("unique_sessions")
    assert unique_sessions is not None
    assert unique_sessions.agg == "count_distinct"

    avg_duration = page_views.get_metric("avg_duration")
    assert avg_duration is not None
    assert avg_duration.agg == "avg"

    # Check page_views segments
    assert len(page_views.segments) >= 3
    homepage_segment = next((s for s in page_views.segments if s.name == "homepage"), None)
    assert homepage_segment is not None

    # Check page_views pre-aggregation with incremental refresh
    assert len(page_views.pre_aggregations) == 1
    preagg = page_views.pre_aggregations[0]
    assert preagg.granularity == "hour"
    assert preagg.partition_granularity == "day"
    assert preagg.refresh_key.incremental is True

    # Check sessions model
    sessions = graph.get_model("sessions")
    assert sessions.table == "analytics.sessions"
    assert len(sessions.relationships) == 2

    # Check session dimensions
    device_dim = sessions.get_dimension("device_type")
    assert device_dim is not None

    utm_source_dim = sessions.get_dimension("utm_source")
    assert utm_source_dim is not None

    # Check session segments
    assert len(sessions.segments) >= 4
    mobile_segment = next((s for s in sessions.segments if s.name == "mobile"), None)
    assert mobile_segment is not None

    # Check conversions model
    conversions = graph.get_model("conversions")
    assert conversions.table == "analytics.conversions"

    # Check conversion measures
    purchases = conversions.get_metric("purchases")
    assert purchases is not None
    assert purchases.filters is not None

    # Check conversion pre-aggregation with indexes
    assert len(conversions.pre_aggregations) == 1
    conv_preagg = conversions.pre_aggregations[0]
    assert len(conv_preagg.indexes) == 1


def test_cube_financial_analytics():
    """Test Cube adapter with financial analytics example."""
    adapter = CubeAdapter()
    graph = adapter.parse(Path("examples/cube/financial_analytics.yml"))

    # Check all models were imported
    assert "transactions" in graph.models
    assert "accounts" in graph.models
    assert "categories" in graph.models
    assert "customers" in graph.models

    # Check transactions model
    transactions = graph.get_model("transactions")
    assert transactions.table == "finance.transactions"
    assert len(transactions.relationships) == 2

    # Check transaction dimensions
    transaction_type_dim = transactions.get_dimension("transaction_type")
    assert transaction_type_dim is not None

    is_recurring_dim = transactions.get_dimension("is_recurring")
    assert is_recurring_dim is not None
    assert is_recurring_dim.type == "categorical"  # boolean maps to categorical

    # Check transaction measures with filters
    credit_amount = transactions.get_metric("credit_amount")
    assert credit_amount is not None
    assert credit_amount.filters is not None
    assert credit_amount.agg == "sum"

    debit_amount = transactions.get_metric("debit_amount")
    assert debit_amount is not None
    assert debit_amount.filters is not None

    # Check transaction segments
    assert len(transactions.segments) >= 4
    large_tx_segment = next((s for s in transactions.segments if s.name == "large_transactions"), None)
    assert large_tx_segment is not None

    # Check transaction pre-aggregation
    assert len(transactions.pre_aggregations) == 1
    preagg = transactions.pre_aggregations[0]
    assert preagg.refresh_key.incremental is True
    assert preagg.refresh_key.update_window == "3 day"

    # Check accounts model
    accounts = graph.get_model("accounts")
    assert accounts.table == "finance.accounts"
    assert len(accounts.relationships) == 2

    # Check account measures
    active_accounts = accounts.get_metric("active_accounts")
    assert active_accounts is not None
    assert active_accounts.filters is not None


def test_cube_logistics_shipping():
    """Test Cube adapter with logistics/shipping example."""
    adapter = CubeAdapter()
    graph = adapter.parse(Path("examples/cube/logistics_shipping.yml"))

    # Check all models were imported
    assert "shipments" in graph.models
    assert "warehouses" in graph.models
    assert "carriers" in graph.models
    assert "orders" in graph.models

    # Check shipments model
    shipments = graph.get_model("shipments")
    assert shipments.table == "logistics.shipments"
    assert len(shipments.relationships) == 3

    # Check shipment dimensions
    status_dim = shipments.get_dimension("status")
    assert status_dim is not None

    weight_dim = shipments.get_dimension("weight")
    assert weight_dim is not None
    assert weight_dim.type == "numeric"

    # Check shipment measures
    total_weight = shipments.get_metric("total_weight")
    assert total_weight is not None
    assert total_weight.agg == "sum"

    delayed = shipments.get_metric("delayed")
    assert delayed is not None
    assert delayed.filters is not None

    # Check shipment segments
    assert len(shipments.segments) >= 5
    heavy_segment = next((s for s in shipments.segments if s.name == "heavy_shipments"), None)
    assert heavy_segment is not None

    # Check shipment pre-aggregation with indexes
    assert len(shipments.pre_aggregations) == 1
    preagg = shipments.pre_aggregations[0]
    assert len(preagg.indexes) == 1
    assert preagg.indexes[0].name == "status_carrier_idx"
    assert len(preagg.indexes[0].columns) == 2

    # Check warehouses model
    warehouses = graph.get_model("warehouses")
    assert warehouses.table == "logistics.warehouses"

    # Check warehouse dimensions
    capacity_dim = warehouses.get_dimension("capacity")
    assert capacity_dim is not None
    assert capacity_dim.type == "numeric"

    # Check carriers model
    carriers = graph.get_model("carriers")
    assert carriers.table == "logistics.carriers"

    # Check carrier segments
    assert len(carriers.segments) >= 3


def test_cube_iot_sensors():
    """Test Cube adapter with IoT/sensor data example."""
    adapter = CubeAdapter()
    graph = adapter.parse(Path("examples/cube/iot_sensors.yml"))

    # Check all models were imported
    assert "sensor_readings" in graph.models
    assert "devices" in graph.models
    assert "locations" in graph.models
    assert "alerts" in graph.models

    # Check sensor_readings model
    sensor_readings = graph.get_model("sensor_readings")
    assert sensor_readings.table == "iot.sensor_readings"
    assert len(sensor_readings.relationships) == 2

    # Check sensor dimensions
    sensor_type_dim = sensor_readings.get_dimension("sensor_type")
    assert sensor_type_dim is not None

    value_dim = sensor_readings.get_dimension("value")
    assert value_dim is not None
    assert value_dim.type == "numeric"

    # Check sensor measures
    avg_value = sensor_readings.get_metric("avg_value")
    assert avg_value is not None
    assert avg_value.agg == "avg"

    avg_temp = sensor_readings.get_metric("avg_temperature")
    assert avg_temp is not None
    assert avg_temp.filters is not None

    # Check sensor segments
    assert len(sensor_readings.segments) >= 5

    # Check sensor pre-aggregations
    assert len(sensor_readings.pre_aggregations) == 1

    hourly_preagg = next((p for p in sensor_readings.pre_aggregations if p.name == "readings_hourly"), None)
    assert hourly_preagg is not None
    assert hourly_preagg.granularity == "hour"
    assert hourly_preagg.refresh_key.every == "1 hour"
    assert len(hourly_preagg.indexes) == 1

    # Check devices model
    devices = graph.get_model("devices")
    assert devices.table == "iot.devices"

    # Check device measures
    maintenance_needed = devices.get_metric("maintenance_needed")
    assert maintenance_needed is not None
    assert maintenance_needed.filters is not None

    # Check alerts model
    alerts = graph.get_model("alerts")
    assert alerts.table == "iot.alerts"

    # Check alert measures
    critical_alerts = alerts.get_metric("critical_alerts")
    assert critical_alerts is not None
    assert critical_alerts.filters is not None


def test_cube_healthcare_patients():
    """Test Cube adapter with healthcare analytics example."""
    adapter = CubeAdapter()
    graph = adapter.parse(Path("examples/cube/healthcare_patients.yml"))

    # Check all models were imported
    assert "patients" in graph.models
    assert "appointments" in graph.models
    assert "treatments" in graph.models
    assert "physicians" in graph.models
    assert "departments" in graph.models

    # Check patients model
    patients = graph.get_model("patients")
    assert patients.table == "healthcare.patients"
    assert len(patients.relationships) == 2

    # Check patient dimensions
    age_dim = patients.get_dimension("age")
    assert age_dim is not None
    assert age_dim.type == "numeric"

    blood_type_dim = patients.get_dimension("blood_type")
    assert blood_type_dim is not None

    # Check patient measures
    avg_age = patients.get_metric("avg_age")
    assert avg_age is not None
    assert avg_age.agg == "avg"

    pediatric = patients.get_metric("pediatric_patients")
    assert pediatric is not None
    assert pediatric.filters is not None

    # Check patient segments
    assert len(patients.segments) >= 4

    # Check appointments model
    appointments = graph.get_model("appointments")
    assert appointments.table == "healthcare.appointments"
    assert len(appointments.relationships) == 3

    # Check appointment dimensions
    appointment_type_dim = appointments.get_dimension("appointment_type")
    assert appointment_type_dim is not None

    duration_dim = appointments.get_dimension("duration_minutes")
    assert duration_dim is not None
    assert duration_dim.type == "numeric"

    # Check appointment measures
    completed = appointments.get_metric("completed_appointments")
    assert completed is not None
    assert completed.filters is not None

    no_show = appointments.get_metric("no_show_appointments")
    assert no_show is not None
    assert no_show.filters is not None

    # Check appointment segments
    assert len(appointments.segments) >= 5

    # Check appointment pre-aggregation
    assert len(appointments.pre_aggregations) == 1
    preagg = appointments.pre_aggregations[0]
    assert preagg.name == "appointments_daily"
    assert len(preagg.measures) >= 3

    # Check treatments model
    treatments = graph.get_model("treatments")
    assert treatments.table == "healthcare.treatments"

    # Check treatment measures
    total_cost = treatments.get_metric("total_cost")
    assert total_cost is not None
    assert total_cost.agg == "sum"

    # Check physicians model
    physicians = graph.get_model("physicians")
    assert physicians.table == "healthcare.physicians"

    # Check departments model
    departments = graph.get_model("departments")
    assert departments.table == "healthcare.departments"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
