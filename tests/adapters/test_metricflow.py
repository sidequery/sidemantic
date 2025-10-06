"""Tests for MetricFlow adapter."""

from pathlib import Path

from sidemantic.adapters.metricflow import MetricFlowAdapter


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
    # Should have a many_to_one relationship to customers (resolved from entity name "customer")
    customer_rel = next((r for r in orders.relationships if r.name == "customers"), None)
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


def test_metricflow_adapter_join_discovery():
    """Test that MetricFlow adapter resolves entity names to model names."""
    adapter = MetricFlowAdapter()
    graph = adapter.parse(Path("examples/metricflow/semantic_models.yml"))

    # MetricFlow uses entity names for relationships
    # The orders model has a relationship to "customer" (entity name)
    # which should be resolved to the actual model name "customers"

    # Check that orders has a relationship defined
    orders = graph.get_model("orders")
    assert len(orders.relationships) > 0

    # After resolution, the relationship should point to "customers" (not "customer")
    customer_rel = next((r for r in orders.relationships if r.name == "customers"), None)
    assert customer_rel is not None, "Relationship should be resolved from 'customer' to 'customers'"
    assert customer_rel.type == "many_to_one"

    # Verify that queries can now build join paths
    from sidemantic.sql.generator import SQLGenerator

    generator = SQLGenerator(graph)

    # This query should work now that relationships are resolved
    sql = generator.generate(
        metrics=["orders.revenue"],
        dimensions=["customers.region"],  # Cross-model dimension
    )

    # Should contain a join to customers
    assert "customers" in sql.lower()


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
    # Note: "customer" and "product" entities cannot be resolved since there are no matching models
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

    # Verify relationships (resolved from entity names "customer" and "product")
    customer_rel = next((r for r in orders.relationships if r.name == "customers"), None)
    assert customer_rel is not None
    assert customer_rel.type == "many_to_one"

    product_rel = next((r for r in orders.relationships if r.name == "products"), None)
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

    # Check line_items relationships (should be resolved from entity names)
    order_rel = next((r for r in line_items.relationships if r.name == "orders"), None)
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
