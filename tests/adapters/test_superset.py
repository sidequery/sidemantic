"""Tests for Superset adapter parsing."""

import pytest

from sidemantic.adapters.superset import SupersetAdapter


def test_import_real_superset_example():
    """Test importing real Superset dataset files."""
    adapter = SupersetAdapter()
    graph = adapter.parse("examples/superset/")

    # Verify models loaded
    assert "orders" in graph.models
    assert "sales_summary" in graph.models

    # Verify orders dataset
    orders = graph.models["orders"]
    assert orders.table == "public.orders"
    assert orders.description == "Customer orders dataset"

    # Verify dimensions
    dim_names = [d.name for d in orders.dimensions]
    assert "id" in dim_names
    assert "customer_id" in dim_names
    assert "created_at" in dim_names
    assert "status" in dim_names
    assert "amount" in dim_names

    # Verify main datetime column
    created_at = next(d for d in orders.dimensions if d.name == "created_at")
    assert created_at.type == "time"
    assert created_at.label == "Order Date"

    # Verify metrics
    metric_names = [m.name for m in orders.metrics]
    assert "count" in metric_names
    assert "total_revenue" in metric_names
    assert "avg_order_value" in metric_names

    # Verify metric types
    count_metric = next(m for m in orders.metrics if m.name == "count")
    assert count_metric.agg == "count"

    revenue_metric = next(m for m in orders.metrics if m.name == "total_revenue")
    assert revenue_metric.agg == "sum"
    assert revenue_metric.label == "Total Revenue"


def test_import_superset_virtual_dataset():
    """Test that Superset virtual datasets (SQL-based) are imported."""
    adapter = SupersetAdapter()
    graph = adapter.parse("examples/superset/sales_summary.yaml")

    sales = graph.models["sales_summary"]

    # Verify it has SQL (virtual dataset)
    assert sales.sql is not None
    assert "SELECT" in sales.sql
    assert sales.table is None  # Virtual datasets don't have table

    # Verify derived metric without aggregation type
    revenue_per_order = next(m for m in sales.metrics if m.name == "revenue_per_order")
    assert revenue_per_order.type == "derived"
    assert revenue_per_order.agg is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
