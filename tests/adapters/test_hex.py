"""Tests for Hex adapter parsing."""

import pytest

from sidemantic.adapters.hex import HexAdapter


def test_import_real_hex_example():
    """Test importing real Hex semantic model files."""
    adapter = HexAdapter()
    graph = adapter.parse("tests/fixtures/hex/")

    # Verify models loaded
    assert "orders" in graph.models
    assert "users" in graph.models
    assert "organizations" in graph.models

    orders = graph.models["orders"]

    # Verify dimensions
    dim_names = [d.name for d in orders.dimensions]
    assert "id" in dim_names
    assert "customer_id" in dim_names
    assert "amount" in dim_names
    assert "status" in dim_names
    assert "is_completed" in dim_names

    # Verify primary key from unique dimension
    assert orders.primary_key == "id"

    # Verify measures
    measure_names = [m.name for m in orders.metrics]
    assert "order_count" in measure_names
    assert "revenue" in measure_names
    assert "avg_order_value" in measure_names
    assert "completed_revenue" in measure_names

    # Verify measure with filter
    completed_revenue = next(m for m in orders.metrics if m.name == "completed_revenue")
    assert completed_revenue.filters is not None
    assert len(completed_revenue.filters) > 0

    # Verify custom func_sql measure
    conversion_rate = next(m for m in orders.metrics if m.name == "conversion_rate")
    assert conversion_rate.type == "derived"

    # Verify relationships
    rel_names = [r.name for r in orders.relationships]
    assert "customers" in rel_names
    customers_rel = next(r for r in orders.relationships if r.name == "customers")
    assert customers_rel.type == "many_to_one"


def test_import_hex_with_relations():
    """Test that Hex relations are properly imported."""
    adapter = HexAdapter()
    graph = adapter.parse("tests/fixtures/hex/")

    users = graph.models["users"]
    orgs = graph.models["organizations"]

    # Verify many_to_one from users to organizations
    user_rels = [r.name for r in users.relationships]
    assert "organizations" in user_rels

    # Verify one_to_many from organizations to users
    org_rels = [r.name for r in orgs.relationships]
    assert "users" in org_rels
    users_rel = next(r for r in orgs.relationships if r.name == "users")
    assert users_rel.type == "one_to_many"


def test_import_hex_calculated_dimensions():
    """Test that Hex calculated dimensions (expr_sql) are imported."""
    adapter = HexAdapter()
    graph = adapter.parse("tests/fixtures/hex/users.yml")

    users = graph.models["users"]

    # Find the calculated dimension
    annual_price = next(d for d in users.dimensions if d.name == "annual_seat_price")
    assert annual_price.sql is not None
    assert "IF" in annual_price.sql


def test_query_imported_hex_example():
    """Test that we can compile queries from imported Hex schema."""
    from sidemantic import SemanticLayer

    adapter = HexAdapter()
    graph = adapter.parse("tests/fixtures/hex/orders.yml")

    layer = SemanticLayer()
    layer.graph = graph

    # Test basic metric query
    sql = layer.compile(metrics=["orders.revenue"])
    assert "SUM" in sql.upper()

    # Test with dimension
    sql = layer.compile(metrics=["orders.revenue", "orders.order_count"], dimensions=["orders.status"])
    assert "GROUP BY" in sql.upper()
    assert "status" in sql.lower()

    # Test with filter
    sql = layer.compile(metrics=["orders.revenue"], filters=["orders.status = 'completed'"])
    assert "WHERE" in sql.upper()
    assert "completed" in sql.lower()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
