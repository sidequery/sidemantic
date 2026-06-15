"""Tests for MetricFlow adapter - parsing."""

from pathlib import Path

import pytest

from sidemantic.adapters.metricflow import MetricFlowAdapter
from sidemantic.sql.generator import SQLGenerator

# =============================================================================
# PARSING TESTS
# =============================================================================


def test_metricflow_adapter():
    """Test MetricFlow adapter with example YAML."""
    adapter = MetricFlowAdapter()
    graph = adapter.parse(Path("tests/fixtures/metricflow/semantic_models.yml"))

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
    graph = adapter.parse(Path("tests/fixtures/metricflow/semantic_models.yml"))

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
    graph = adapter.parse(Path("tests/fixtures/metricflow/advanced_metrics.yml"))

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

    # Check derived metrics - plain (non-offset, unfiltered) aliases are
    # rewritten back to their real input metrics so the metric is queryable.
    assert "order_gross_profit" in graph.metrics
    gross_profit = graph.get_metric("order_gross_profit")
    assert gross_profit.type == "derived"
    assert gross_profit.sql == "total_order_revenue - total_order_cost"

    # Check derived metric with filters - filtered-input aliases are preserved
    # because the filtered value differs from the underlying metric.
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
    graph = adapter.parse(Path("tests/fixtures/metricflow/ecommerce_multi_model.yml"))

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
    graph = adapter.parse(Path("tests/fixtures/metricflow/advanced_dimensions.yml"))

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
    graph = adapter.parse(Path("tests/fixtures/metricflow/advanced_metrics.yml"))

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
    graph = adapter.parse(Path("tests/fixtures/metricflow/advanced_dimensions.yml"))

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
    graph = adapter.parse(Path("tests/fixtures/metricflow/advanced_metrics.yml"))

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
    graph = adapter.parse(Path("tests/fixtures/metricflow/coalesce_2023_orders.yml"))

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
    graph = adapter.parse(Path("tests/fixtures/metricflow/coalesce_2023_customers.yml"))

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
    graph = adapter.parse(Path("tests/fixtures/metricflow/coalesce_2023_order_items.yml"))

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
    graph = adapter.parse(Path("tests/fixtures/metricflow/conversion_metrics.yml"))

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

    # MetricFlow conversion metrics reference base/conversion *measures*, which
    # have no faithful mapping to Sidemantic's event-filter conversion funnel.
    # They are retained as non-queryable metadata rather than registered as
    # broken queryable metrics, so they never appear in graph.metrics.
    assert "visit_to_buy_conversion_rate" not in graph.metrics
    assert "visit_to_buy_conversions_1_week" not in graph.metrics
    assert "view_to_purchase_same_product" not in graph.metrics

    conv_specs = graph.metadata["metricflow_conversion_metrics"]

    conv = conv_specs["visit_to_buy_conversion_rate"]
    assert conv["entity"] == "user"
    assert conv["base_measure"] == "visits"
    assert conv["conversion_measure"] == "buys"
    assert conv["window"] == "7 days"
    assert conv["calculation"] == "conversion_rate"

    # Conversion with calculation: conversions (count flavor)
    conversions = conv_specs["visit_to_buy_conversions_1_week"]
    assert conversions["window"] == "1 week"
    assert conversions["calculation"] == "conversions"

    # Conversion with constant_properties
    same_product = conv_specs["view_to_purchase_same_product"]
    assert same_product["base_measure"] == "view_item_detail"
    assert same_product["constant_properties"] == [{"base_property": "product", "conversion_property": "product"}]

    # Test that parsing doesn't fail
    assert graph is not None
    assert len(graph.models) > 0


def test_metricflow_cumulative_grain_to_date():
    """Test MetricFlow adapter with cumulative metrics and grain_to_date."""
    adapter = MetricFlowAdapter()
    graph = adapter.parse(Path("tests/fixtures/metricflow/cumulative_metrics_grain_to_date.yml"))

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
    graph = adapter.parse(Path("tests/fixtures/metricflow/jaffle_sl_testing_order_items.yml"))

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
    graph = adapter.parse(Path("tests/fixtures/metricflow/saved_queries_example.yml"))

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

    # Saved queries are parsed into graph metadata (not into graph.metrics).
    saved = graph.metadata.get("saved_queries")
    assert saved is not None
    assert "monthly_sales_by_region" in saved
    sq = saved["monthly_sales_by_region"]
    assert sq["metrics"] == ["total_sales", "sales_transactions"]
    assert sq["group_by"] == ["TimeDimension('sale_date', 'month')", "Dimension('region')"]
    assert sq["exports"][0]["name"] == "monthly_sales_export"
    assert sq["exports"][0]["config"]["export_as"] == "table"


def test_import_real_metricflow_example():
    """Test importing a real dbt MetricFlow schema file."""
    adapter = MetricFlowAdapter()
    graph = adapter.parse("tests/fixtures/metricflow/semantic_models.yml")

    # Verify models loaded
    assert "orders" in graph.models
    assert "customers" in graph.models

    orders = graph.models["orders"]
    customers = graph.models["customers"]

    # Verify dimensions
    order_dims = [d.name for d in orders.dimensions]
    assert "order_date" in order_dims
    assert "status" in order_dims

    customer_dims = [d.name for d in customers.dimensions]
    assert "region" in customer_dims
    assert "tier" in customer_dims

    # Verify measures
    measure_names = [m.name for m in orders.metrics]
    assert "order_count" in measure_names
    assert "revenue" in measure_names
    assert "avg_order_value" in measure_names

    # Verify relationships were created from entities (resolved to model names)
    rel_names = [r.name for r in orders.relationships]
    assert "customers" in rel_names
    customer_rel = next(r for r in orders.relationships if r.name == "customers")
    assert customer_rel.type == "many_to_one"
    assert customer_rel.foreign_key == "customer_id"

    # Verify graph-level metrics
    assert "total_revenue" in graph.metrics
    assert "average_order_value" in graph.metrics

    total_revenue = graph.metrics["total_revenue"]
    assert total_revenue.type is None  # Simple metric maps to untyped

    avg_order = graph.metrics["average_order_value"]
    assert avg_order.type == "ratio"
    assert avg_order.numerator == "revenue"
    assert avg_order.denominator == "order_count"


def test_metricflow_latest_spec_models():
    """Test the latest spec (dbt Core 1.12 / Fusion): models: with nested semantic_model:."""
    adapter = MetricFlowAdapter()
    graph = adapter.parse(Path("tests/fixtures/metricflow/latest_spec_models.yml"))

    # Semantic models embedded under models: are imported
    assert "orders" in graph.models
    assert "customers" in graph.models

    orders = graph.get_model("orders")
    # Primary entity column becomes the primary key
    assert orders.primary_key == "order_id"
    # The underlying table is the dbt model name
    assert orders.table == "orders"
    # agg_time_dimension promoted to top level
    assert orders.default_time_dimension == "ordered_at"

    # Column-based foreign entity becomes a relationship (resolved to model name)
    rel_names = {r.name for r in orders.relationships}
    assert "customers" in rel_names
    customer_rel = next(r for r in orders.relationships if r.name == "customers")
    assert customer_rel.type == "many_to_one"
    assert customer_rel.foreign_key == "customer_id"

    # Column-based dimensions (time + categorical)
    ordered_at = orders.get_dimension("ordered_at")
    assert ordered_at is not None
    assert ordered_at.type == "time"
    assert ordered_at.granularity == "day"

    status = orders.get_dimension("status")
    assert status is not None
    assert status.type == "categorical"
    # expr falls back to the column name
    assert status.sql == "order_status"

    # Measures folded into inline simple metrics. The expression is qualified
    # with the owning model so a query selecting only the metric can infer its
    # model.
    order_total = graph.get_metric("order_total")
    assert order_total is not None
    assert order_total.type is None
    assert order_total.agg == "sum"
    assert order_total.sql == "orders.amount"

    order_count = graph.get_metric("order_count")
    assert order_count is not None
    assert order_count.agg == "count"
    # A bare ``count`` (no expr) is anchored to the model via its primary key.
    assert order_count.sql == "orders.order_id"

    # Ratio with promoted numerator/denominator (no type_params)
    rpo = graph.get_metric("revenue_per_order")
    assert rpo.type == "ratio"
    assert rpo.numerator == "order_total"
    assert rpo.denominator == "order_count"

    # Derived with promoted expr + input_metrics offset modifiers. The plain
    # alias (current_total) is rewritten to its input metric (order_total); the
    # offset alias (total_7_days_ago) is preserved because it is time-shifted.
    growth = graph.get_metric("order_total_growth")
    assert growth.type == "derived"
    assert growth.sql == "order_total - total_7_days_ago"
    inputs = growth.metadata["input_metrics"]
    offset_input = next(i for i in inputs if i.get("alias") == "total_7_days_ago")
    assert offset_input["offset_window"] == "7 days"

    # Cumulative with promoted input_metric + window + period_agg
    rolling = graph.get_metric("rolling_30d_revenue")
    assert rolling.type == "cumulative"
    assert rolling.sql == "order_total"
    assert rolling.window == "30 days"
    assert rolling.metadata["period_agg"] == "sum"

    # Cumulative with promoted grain_to_date
    mtd = graph.get_metric("revenue_mtd")
    assert mtd.type == "cumulative"
    assert mtd.grain_to_date == "month"

    # Conversion with promoted base_metric/conversion_metric/entity is retained
    # as non-queryable metadata (no faithful mapping to Sidemantic's funnel).
    assert "order_to_repeat_conversion" not in graph.metrics
    conv = graph.metadata["metricflow_conversion_metrics"]["order_to_repeat_conversion"]
    assert conv["entity"] == "customer"
    assert conv["base_measure"] == "order_count"
    assert conv["conversion_measure"] == "order_count"
    assert conv["calculation"] == "conversion_rate"
    assert conv["constant_properties"] == [{"base_property": "region", "conversion_property": "region"}]


def test_metricflow_latest_spec_inline_metric_queryable_alone():
    """Inline simple metrics carry model context so they can be queried alone.

    A latest-spec ``type: simple`` metric folds a model measure. Its SQL must be
    qualified with the owning model, otherwise selecting only that metric raises
    ``No models found for query``.
    """
    adapter = MetricFlowAdapter()
    graph = adapter.parse(Path("tests/fixtures/metricflow/latest_spec_models.yml"))
    generator = SQLGenerator(graph)

    # A measure-backed simple metric (sum with expr)
    sql = generator.generate(metrics=["order_total"])
    assert "orders" in sql.lower()
    assert "sum" in sql.lower()

    # A bare count measure (no expr) is anchored via the primary key
    sql = generator.generate(metrics=["order_count"])
    assert "orders" in sql.lower()
    assert "count" in sql.lower()

    # A ratio referencing the inline measures by bare name still resolves
    sql = generator.generate(metrics=["revenue_per_order"])
    assert "orders" in sql.lower()


def test_metricflow_inline_constant_count_anchored_to_model():
    """A constant inline count (``agg: count`` with ``expr: 1``/``'*'``) is queryable.

    Such a measure has no column to qualify, so without anchoring it carries no
    model reference and selecting it raises ``No models found for query``. It
    should take the same primary-key anchoring path as a bare ``count``.
    """
    import tempfile
    import textwrap

    yml = textwrap.dedent("""
        models:
          - name: events
            semantic_model:
              enabled: true
              name: events
            columns:
              - name: event_id
                entity:
                  type: primary
                  name: event
            metrics:
              - name: row_count
                type: simple
                agg: count
                expr: 1
              - name: star_count
                type: simple
                agg: count
                expr: '*'
    """)
    with tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False) as f:
        f.write(yml)
        path = Path(f.name)

    try:
        adapter = MetricFlowAdapter()
        graph = adapter.parse(path)
        # Constant counts are anchored to the model primary key.
        assert graph.get_metric("row_count").sql == "events.event_id"
        assert graph.get_metric("star_count").sql == "events.event_id"

        generator = SQLGenerator(graph)
        for metric_name in ("row_count", "star_count"):
            sql = generator.generate(metrics=[metric_name])
            assert "events" in sql.lower()
            assert "count" in sql.lower()
    finally:
        path.unlink(missing_ok=True)


def test_metricflow_inline_exprless_non_count_uses_metric_column():
    """An expr-less non-count inline measure aggregates its own column, not the PK.

    In MetricFlow an expr-less measure aggregates the column named after the
    measure. Only count-style measures may be anchored to the primary key;
    anchoring a ``sum``/``max``/etc. there would silently aggregate the wrong
    column (e.g. ``SUM(customers.customer_id)``).
    """
    import tempfile
    import textwrap

    yml = textwrap.dedent("""
        models:
          - name: customers
            semantic_model:
              enabled: true
              name: customers
            columns:
              - name: customer_id
                entity:
                  type: primary
                  name: customer
            metrics:
              - name: lifetime_spend_pretax
                type: simple
                agg: sum
              - name: max_spend
                type: simple
                agg: max
              - name: customer_count
                type: simple
                agg: count
    """)
    with tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False) as f:
        f.write(yml)
        path = Path(f.name)

    try:
        adapter = MetricFlowAdapter()
        graph = adapter.parse(path)
        # Expr-less non-count aggregates use the metric's own column.
        assert graph.get_metric("lifetime_spend_pretax").sql == "customers.lifetime_spend_pretax"
        assert graph.get_metric("max_spend").sql == "customers.max_spend"
        # Expr-less count is still anchored to the primary key.
        assert graph.get_metric("customer_count").sql == "customers.customer_id"

        sql = SQLGenerator(graph).generate(metrics=["lifetime_spend_pretax"])
        assert "sum(customers_cte.lifetime_spend_pretax)" in sql.lower()
        assert "customer_id" not in sql.lower()
    finally:
        path.unlink(missing_ok=True)


def test_metricflow_derived_non_offset_aliases_queryable():
    """Derived metrics whose inputs are all non-offset aliases are queryable.

    MetricFlow lets a derived expression reference an input metric by ``alias``.
    Aliases without an offset modifier are rewritten back to their real input
    metric so dependency resolution sees real metrics; otherwise the planner
    cannot infer a model and the metric cannot be queried.
    """
    adapter = MetricFlowAdapter()
    graph = adapter.parse(Path("tests/fixtures/metricflow/latest_spec_models.yml"))

    net = graph.get_metric("net_order_total")
    assert net.type == "derived"
    # Both aliases (gross_total, half_total) rewritten back to order_total.
    assert net.sql == "order_total - order_total / 2"
    assert net.get_dependencies(graph) == {"order_total"}

    sql = SQLGenerator(graph).generate(metrics=["net_order_total"])
    assert "orders" in sql.lower()


def test_metricflow_derived_offset_alias_preserved():
    """An offset-carrying derived input alias is left intact (time-shifted value).

    Rewriting an offset alias to its base metric would conflate a time-shifted
    value with the current one, so those aliases are preserved as-is and the
    metric remains round-trip metadata rather than a misleading queryable metric.
    """
    adapter = MetricFlowAdapter()
    graph = adapter.parse(Path("tests/fixtures/metricflow/latest_spec_models.yml"))

    growth = graph.get_metric("order_total_growth")
    # Non-offset alias (current_total) rewritten; offset alias (total_7_days_ago) kept.
    assert growth.sql == "order_total - total_7_days_ago"


def test_metricflow_conversion_metrics_not_queryable_metadata_only():
    """Conversion metrics are retained as metadata, never registered as metrics.

    MetricFlow conversion metrics reference base/conversion *measures*, which do
    not map to Sidemantic's event-filter conversion funnel. Registering one
    would generate wrong SQL (filtering an event_type dimension by a measure
    name), so the spec is captured in graph metadata instead.
    """
    adapter = MetricFlowAdapter()
    graph = adapter.parse(Path("tests/fixtures/metricflow/latest_spec_models.yml"))

    assert "order_to_repeat_conversion" not in graph.metrics
    specs = graph.metadata["metricflow_conversion_metrics"]
    spec = specs["order_to_repeat_conversion"]
    assert spec["entity"] == "customer"
    assert spec["base_measure"] == "order_count"
    assert spec["conversion_measure"] == "order_count"
    assert spec["calculation"] == "conversion_rate"


def test_metricflow_conversion_metrics_not_leaked_on_reuse():
    """Reusing an adapter must not leak conversion metrics into a later graph."""
    adapter = MetricFlowAdapter()

    with_conv = adapter.parse(Path("tests/fixtures/metricflow/latest_spec_models.yml"))
    assert "metricflow_conversion_metrics" in with_conv.metadata

    # Parsing a source without conversion metrics on the same adapter must not
    # retain the previous file's entries.
    without_conv = adapter.parse(Path("tests/fixtures/metricflow/semantic_models.yml"))
    assert "metricflow_conversion_metrics" not in without_conv.metadata


def test_metricflow_saved_queries_not_leaked_on_reuse():
    """Reusing an adapter must not leak saved queries into a later graph."""
    adapter = MetricFlowAdapter()

    with_sq = adapter.parse(Path("tests/fixtures/metricflow/saved_queries_example.yml"))
    assert "saved_queries" in with_sq.metadata

    # Parsing a source without saved queries on the same adapter must not retain
    # the previous file's entries.
    without_sq = adapter.parse(Path("tests/fixtures/metricflow/latest_spec_models.yml"))
    assert "saved_queries" not in without_sq.metadata


def test_metricflow_period_agg_and_offset_metadata():
    """period_agg and per-input offset modifiers are retained in metric metadata."""
    adapter = MetricFlowAdapter()
    graph = adapter.parse(Path("tests/fixtures/metricflow/simple_manifest_metrics.yaml"))

    # Cumulative period_agg (legacy cumulative_type_params shape)
    t2mr = graph.get_metric("trailing_2_months_revenue")
    assert t2mr.metadata is not None
    assert t2mr.metadata["period_agg"] == "average"

    all_time = graph.get_metric("revenue_all_time")
    assert all_time.metadata["period_agg"] == "last"

    # Derived per-input offset_window (legacy type_params.metrics shape)
    offset_metric = graph.get_metric("booking_fees_last_week_per_booker_this_week")
    assert offset_metric.type == "derived"
    inputs = offset_metric.metadata["input_metrics"]
    assert any(i.get("offset_window") == "1 week" for i in inputs)


def test_metricflow_disabled_model_skips_inline_metrics():
    """Inline metrics on a disabled semantic model are not registered.

    ``semantic_model.enabled: false`` produces no model, so its inline
    model-local metrics have no SQL/model context. Registering them anyway would
    leave queryable-looking metrics in the graph that raise ``No models found for
    query`` when selected. The disabled model's metrics must be skipped while a
    sibling enabled model's metrics remain queryable.
    """
    import tempfile
    import textwrap

    yml = textwrap.dedent("""
        models:
          - name: events
            semantic_model:
              enabled: false
              name: events
            columns:
              - name: event_id
                entity:
                  type: primary
                  name: event
            metrics:
              - name: event_count
                type: simple
                agg: count
          - name: orders
            semantic_model:
              enabled: true
              name: orders
            columns:
              - name: order_id
                entity:
                  type: primary
                  name: order
            metrics:
              - name: order_count
                type: simple
                agg: count
    """)
    with tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False) as f:
        f.write(yml)
        path = Path(f.name)

    try:
        graph = MetricFlowAdapter().parse(path)

        # Disabled model and its inline metric are absent.
        assert "events" not in graph.models
        assert "event_count" not in graph.metrics

        # Sibling enabled model and its inline metric remain queryable.
        assert "orders" in graph.models
        assert "order_count" in graph.metrics
        sql = SQLGenerator(graph).generate(metrics=["order_count"])
        assert "orders" in sql.lower()
    finally:
        path.unlink(missing_ok=True)


def test_metricflow_unrepresentable_agg_is_skipped():
    """An ``agg`` Sidemantic cannot represent is skipped, not coerced to sum.

    MetricFlow allows ``agg: percentile`` (with ``agg_params``). Sidemantic has no
    percentile aggregation, and the agg mapper previously defaulted unknown aggs
    to ``sum``, so ``revenue_p95`` parsed as ``SUM(amount)`` and silently returned
    the wrong value. Such metrics/measures must be dropped while representable
    siblings remain.
    """
    import tempfile
    import textwrap

    # Latest-spec inline metric.
    inline_yml = textwrap.dedent("""
        models:
          - name: payments
            semantic_model:
              enabled: true
              name: payments
            columns:
              - name: payment_id
                entity:
                  type: primary
                  name: payment
            metrics:
              - name: revenue_p95
                type: simple
                agg: percentile
                expr: amount
              - name: total_revenue
                type: simple
                agg: sum
                expr: amount
    """)
    with tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False) as f:
        f.write(inline_yml)
        inline_path = Path(f.name)

    # Legacy-spec measure.
    legacy_yml = textwrap.dedent("""
        semantic_models:
          - name: payments_legacy
            model: ref('payments')
            entities:
              - name: payment
                type: primary
                expr: payment_id
            measures:
              - name: p95_amount
                agg: percentile
                expr: amount
              - name: total_amount
                agg: sum
                expr: amount
    """)
    with tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False) as f:
        f.write(legacy_yml)
        legacy_path = Path(f.name)

    try:
        inline_graph = MetricFlowAdapter().parse(inline_path)
        legacy_graph = MetricFlowAdapter().parse(legacy_path)
    finally:
        inline_path.unlink(missing_ok=True)
        legacy_path.unlink(missing_ok=True)

    # Inline percentile metric is skipped; sibling sum metric is kept.
    assert "revenue_p95" not in inline_graph.metrics
    assert "total_revenue" in inline_graph.metrics

    # Legacy percentile measure is skipped; sibling sum measure is kept.
    legacy_measures = {m.name for m in legacy_graph.get_model("payments_legacy").metrics}
    assert "p95_amount" not in legacy_measures
    assert "total_amount" in legacy_measures


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
