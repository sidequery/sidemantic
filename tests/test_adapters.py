"""Tests for semantic model adapters."""

from pathlib import Path

import pytest

from sidemantic.adapters.cube import CubeAdapter
from sidemantic.adapters.metricflow import MetricFlowAdapter


def test_cube_adapter():
    """Test Cube adapter with example YAML."""
    adapter = CubeAdapter()
    graph = adapter.parse(Path("examples/cube"))

    # Check models were imported
    assert "orders" in graph.models
    assert "customer" in graph.models

    # Check orders model
    orders = graph.get_model("orders")
    assert orders.table == "public.orders"
    assert len(orders.entities) > 0
    assert len(orders.dimensions) > 0
    assert len(orders.measures) > 0

    # Check dimensions
    status_dim = orders.get_dimension("status")
    assert status_dim is not None
    assert status_dim.type == "categorical"

    created_dim = orders.get_dimension("created_at")
    assert created_dim is not None
    assert created_dim.type == "time"

    # Check measures
    count_measure = orders.get_measure("count")
    assert count_measure is not None
    assert count_measure.agg == "count"

    revenue_measure = orders.get_measure("revenue")
    assert revenue_measure is not None
    assert revenue_measure.agg == "sum"

    # Check customer model
    customer = graph.get_model("customer")
    assert customer.table == "public.customers"

    region_dim = customer.get_dimension("region")
    assert region_dim is not None


def test_metricflow_adapter():
    """Test MetricFlow adapter with example YAML."""
    adapter = MetricFlowAdapter()
    graph = adapter.parse(Path("examples/metricflow"))

    # Check models were imported
    assert "orders" in graph.models
    assert "customers" in graph.models

    # Check orders model
    orders = graph.get_model("orders")
    assert orders.table == "public.orders"

    # Check entities
    order_entity = orders.get_entity("order")
    assert order_entity is not None
    assert order_entity.type == "primary"

    customer_entity = orders.get_entity("customer")
    assert customer_entity is not None
    assert customer_entity.type == "foreign"

    # Check dimensions
    order_date_dim = orders.get_dimension("order_date")
    assert order_date_dim is not None
    assert order_date_dim.type == "time"
    assert order_date_dim.granularity == "day"

    status_dim = orders.get_dimension("status")
    assert status_dim is not None
    assert status_dim.type == "categorical"

    # Check measures
    revenue_measure = orders.get_measure("revenue")
    assert revenue_measure is not None
    assert revenue_measure.agg == "sum"

    # Check metrics
    assert "total_revenue" in graph.metrics
    total_revenue = graph.get_metric("total_revenue")
    assert total_revenue.type == "simple"
    assert total_revenue.measure == "revenue"

    assert "average_order_value" in graph.metrics
    avg_order = graph.get_metric("average_order_value")
    assert avg_order.type == "ratio"
    assert avg_order.numerator == "revenue"
    assert avg_order.denominator == "order_count"


def test_cube_adapter_join_discovery():
    """Test that Cube adapter enables join discovery."""
    adapter = CubeAdapter()
    graph = adapter.parse(Path("examples/cube"))

    # Should be able to find join path
    join_path = graph.find_join_path("orders", "customer")
    assert len(join_path) == 1
    assert join_path[0].from_model == "orders"
    assert join_path[0].to_model == "customer"


def test_metricflow_adapter_join_discovery():
    """Test that MetricFlow adapter enables join discovery."""
    adapter = MetricFlowAdapter()
    graph = adapter.parse(Path("examples/metricflow"))

    # Should be able to find join path
    join_path = graph.find_join_path("orders", "customers")
    assert len(join_path) == 1
    assert join_path[0].from_model == "orders"
    assert join_path[0].to_model == "customers"
    assert join_path[0].from_entity == "customer"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
