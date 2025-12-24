"""Tests for LookML adapter - query compilation."""

import pytest

from sidemantic.adapters.lookml import LookMLAdapter

# =============================================================================
# QUERY TESTS
# =============================================================================


def test_query_imported_lookml_example():
    """Test that we can compile queries from imported LookML schema."""
    from sidemantic import SemanticLayer

    adapter = LookMLAdapter()
    graph = adapter.parse("tests/fixtures/lookml/orders.lkml")

    layer = SemanticLayer()
    layer.graph = graph

    # Test basic metric query
    sql = layer.compile(metrics=["orders.revenue"])
    assert "SUM" in sql.upper()

    # Test with dimension
    sql = layer.compile(metrics=["orders.revenue", "orders.count"], dimensions=["orders.status"])
    assert "GROUP BY" in sql.upper()
    assert "status" in sql.lower()

    # Test with segment
    sql = layer.compile(metrics=["orders.revenue"], segments=["orders.completed"])
    assert "WHERE" in sql.upper()
    assert "status" in sql.lower()


def test_query_with_time_dimension_lookml():
    """Test querying time dimensions from LookML import."""
    from sidemantic import SemanticLayer

    adapter = LookMLAdapter()
    graph = adapter.parse("tests/fixtures/lookml/orders.lkml")

    layer = SemanticLayer()
    layer.graph = graph

    # Query with time dimension
    sql = layer.compile(metrics=["orders.revenue"], dimensions=["orders.created_date"])
    assert "created_at" in sql.lower()
    assert "GROUP BY" in sql.upper()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
