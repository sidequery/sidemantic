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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
