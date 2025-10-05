"""Tests for Omni adapter parsing."""

import pytest

from sidemantic.adapters.omni import OmniAdapter


def test_import_real_omni_example():
    """Test importing real Omni view files."""
    adapter = OmniAdapter()
    graph = adapter.parse("examples/omni/")

    # Verify models loaded
    assert "orders" in graph.models
    assert "customers" in graph.models

    # Verify orders view
    orders = graph.models["orders"]
    assert orders.table == "public.orders"
    assert orders.description == "Customer order transactions"

    # Verify dimensions
    dim_names = [d.name for d in orders.dimensions]
    assert "id" in dim_names
    assert "customer_id" in dim_names
    assert "created_at" in dim_names
    assert "status" in dim_names
    assert "amount" in dim_names

    # Verify time dimension
    created_at = next(d for d in orders.dimensions if d.name == "created_at")
    assert created_at.type == "time"
    assert created_at.label == "Order Date"

    # Verify measures
    metric_names = [m.name for m in orders.metrics]
    assert "count" in metric_names
    assert "total_revenue" in metric_names
    assert "avg_order_value" in metric_names
    assert "completed_revenue" in metric_names

    # Verify measure with filter
    completed_revenue = next(m for m in orders.metrics if m.name == "completed_revenue")
    assert completed_revenue.filters is not None
    assert len(completed_revenue.filters) > 0

    # Verify relationships
    rel_names = [r.name for r in orders.relationships]
    assert "customers" in rel_names
    customers_rel = next(r for r in orders.relationships if r.name == "customers")
    assert customers_rel.type == "many_to_one"


def test_import_omni_with_timeframes():
    """Test that Omni timeframes are properly imported."""
    adapter = OmniAdapter()
    graph = adapter.parse("examples/omni/views/orders.yaml")

    orders = graph.models["orders"]

    # Verify time dimension has granularity from timeframes
    created_at = next(d for d in orders.dimensions if d.name == "created_at")
    assert created_at.type == "time"
    assert created_at.granularity is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
