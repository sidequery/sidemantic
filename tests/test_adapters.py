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
    graph = adapter.parse(Path("examples/metricflow"))

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
    graph = adapter.parse(Path("examples/cube"))

    # Check that relationships were imported
    orders = graph.get_model("orders")
    assert len(orders.relationships) > 0
    # Note: The Cube example only has one model, so no actual join path can be tested
    # but we verify that the relationship structure was imported correctly


def test_metricflow_adapter_join_discovery():
    """Test that MetricFlow adapter enables join discovery."""
    adapter = MetricFlowAdapter()
    graph = adapter.parse(Path("examples/metricflow"))

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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
