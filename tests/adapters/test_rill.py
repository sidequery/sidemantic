"""Tests for Rill adapter parsing."""

import pytest

from sidemantic.adapters.rill import RillAdapter


def test_import_real_rill_example():
    """Test importing a real Rill metrics view YAML file."""
    adapter = RillAdapter()
    graph = adapter.parse("examples/rill/orders.yaml")

    # Verify models loaded
    assert "orders" in graph.models
    orders = graph.models["orders"]

    # Verify dimensions
    dim_names = [d.name for d in orders.dimensions]
    assert "status" in dim_names
    assert "customer_id" in dim_names
    assert "country" in dim_names
    assert "product_category" in dim_names

    # Verify measures
    measure_names = [m.name for m in orders.metrics]
    assert "total_orders" in measure_names
    assert "total_revenue" in measure_names
    assert "avg_order_value" in measure_names
    assert "completed_orders" in measure_names

    # Verify timeseries dimension was created
    # Should have timeseries as a time dimension
    time_dims = [d for d in orders.dimensions if d.type == "time"]
    assert len(time_dims) > 0


def test_import_rill_with_derived_measures():
    """Test importing Rill metrics view with derived measures."""
    adapter = RillAdapter()
    graph = adapter.parse("examples/rill/users.yaml")

    assert "users" in graph.models
    users = graph.models["users"]

    # Verify derived measures were detected
    derived_metrics = [m for m in users.metrics if m.type == "derived"]
    assert len(derived_metrics) == 2

    derived_names = [m.name for m in derived_metrics]
    assert "avg_revenue_per_user" in derived_names
    assert "activation_rate" in derived_names


def test_import_rill_with_table_reference():
    """Test importing Rill metrics view that references a table."""
    adapter = RillAdapter()
    graph = adapter.parse("examples/rill/sales.yaml")

    assert "sales" in graph.models
    sales = graph.models["sales"]

    # Verify table reference was captured
    assert sales.table == "public.sales"

    # Verify dimensions
    dim_names = [d.name for d in sales.dimensions]
    assert "store_id" in dim_names
    assert "product_id" in dim_names
    assert "sales_rep" in dim_names
    assert "region" in dim_names


def test_query_imported_rill_example():
    """Test that we can compile queries from imported Rill schema."""
    from sidemantic import SemanticLayer

    adapter = RillAdapter()
    graph = adapter.parse("examples/rill/orders.yaml")

    layer = SemanticLayer()
    layer.graph = graph

    # Simple metric query
    sql = layer.compile(metrics=["orders.total_orders"])
    assert "COUNT" in sql.upper()

    # Query with dimension
    sql = layer.compile(metrics=["orders.total_revenue"], dimensions=["orders.status"])
    assert "GROUP BY" in sql.upper()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
