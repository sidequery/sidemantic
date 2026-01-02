"""Tests for querying Rill models."""

from sidemantic import SemanticLayer
from sidemantic.adapters.rill import RillAdapter


def test_query_imported_rill_example():
    """Test that we can compile queries from imported Rill schema."""
    adapter = RillAdapter()
    graph = adapter.parse("tests/fixtures/rill/orders.yaml")

    layer = SemanticLayer()
    layer.graph = graph

    # Simple metric query
    sql = layer.compile(metrics=["orders.total_orders"])
    assert "COUNT" in sql.upper()

    # Query with dimension
    sql = layer.compile(metrics=["orders.total_revenue"], dimensions=["orders.status"])
    assert "GROUP BY" in sql.upper()
