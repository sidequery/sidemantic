"""Basic tests for Sidemantic core functionality."""

import pytest

from sidemantic import Dimension, Entity, Measure, Metric, Model, SemanticLayer


def test_create_model():
    """Test creating a basic model."""
    model = Model(
        name="orders",
        table="public.orders",
        entities=[Entity(name="order", type="primary", expr="order_id")],
        dimensions=[Dimension(name="status", type="categorical")],
        measures=[Measure(name="count", agg="count")],
    )

    assert model.name == "orders"
    assert model.table == "public.orders"
    assert len(model.entities) == 1
    assert len(model.dimensions) == 1
    assert len(model.measures) == 1


def test_semantic_layer():
    """Test semantic layer basic operations."""
    sl = SemanticLayer()

    orders = Model(
        name="orders",
        table="public.orders",
        entities=[Entity(name="order", type="primary", expr="order_id")],
        dimensions=[Dimension(name="status", type="categorical")],
        measures=[Measure(name="revenue", agg="sum", expr="order_amount")],
    )

    sl.add_model(orders)

    assert "orders" in sl.list_models()
    assert sl.get_model("orders").name == "orders"


def test_join_path_discovery():
    """Test automatic join path discovery."""
    sl = SemanticLayer()

    orders = Model(
        name="orders",
        table="public.orders",
        entities=[
            Entity(name="order", type="primary", expr="order_id"),
            Entity(name="customer", type="foreign", expr="customer_id"),
        ],
    )

    customers = Model(
        name="customers",
        table="public.customers",
        entities=[Entity(name="customer", type="primary", expr="customer_id")],
    )

    sl.add_model(orders)
    sl.add_model(customers)

    # Test join path finding
    join_path = sl.graph.find_join_path("orders", "customers")
    assert len(join_path) == 1
    assert join_path[0].from_model == "orders"
    assert join_path[0].to_model == "customers"
    assert join_path[0].from_entity == "customer"


def test_time_dimension_granularity():
    """Test time dimension with granularity."""
    dim = Dimension(name="created_at", type="time", granularity="day", expr="created_at")

    # Test granularity SQL generation
    sql = dim.with_granularity("month")
    assert "DATE_TRUNC('month', created_at)" in sql


def test_measure_aggregation():
    """Test measure SQL generation."""
    measure = Measure(name="revenue", agg="sum", expr="order_amount")

    sql = measure.to_sql()
    assert sql == "SUM(order_amount)"


def test_simple_metric():
    """Test creating a simple metric."""
    metric = Metric(name="total_revenue", type="simple", measure="orders.revenue")

    assert metric.name == "total_revenue"
    assert metric.type == "simple"
    assert metric.measure == "orders.revenue"


def test_ratio_metric():
    """Test creating a ratio metric."""
    metric = Metric(
        name="conversion_rate",
        type="ratio",
        numerator="orders.completed_revenue",
        denominator="orders.revenue",
    )

    assert metric.name == "conversion_rate"
    assert metric.type == "ratio"
    assert metric.numerator == "orders.completed_revenue"
    assert metric.denominator == "orders.revenue"


def test_sql_compilation():
    """Test SQL query compilation."""
    sl = SemanticLayer()

    orders = Model(
        name="orders",
        table="public.orders",
        entities=[Entity(name="order", type="primary", expr="order_id")],
        dimensions=[Dimension(name="status", type="categorical")],
        measures=[Measure(name="revenue", agg="sum", expr="order_amount")],
    )

    sl.add_model(orders)

    sql = sl.compile(metrics=["orders.revenue"], dimensions=["orders.status"])

    # Check that SQL contains expected components
    assert "WITH" in sql
    assert "orders_cte" in sql
    assert "SUM" in sql
    assert "GROUP BY" in sql


def test_multi_model_query():
    """Test query across multiple models."""
    sl = SemanticLayer()

    orders = Model(
        name="orders",
        table="public.orders",
        entities=[
            Entity(name="order", type="primary", expr="order_id"),
            Entity(name="customer", type="foreign", expr="customer_id"),
        ],
        dimensions=[Dimension(name="status", type="categorical")],
        measures=[Measure(name="revenue", agg="sum", expr="order_amount")],
    )

    customers = Model(
        name="customers",
        table="public.customers",
        entities=[Entity(name="customer", type="primary", expr="customer_id")],
        dimensions=[Dimension(name="region", type="categorical")],
    )

    sl.add_model(orders)
    sl.add_model(customers)

    sql = sl.compile(
        metrics=["orders.revenue"],
        dimensions=["customers.region"],
    )

    # Check for join
    assert "LEFT JOIN" in sql
    assert "customers_cte" in sql


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
