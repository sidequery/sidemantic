"""Tests for Cube adapter - query compilation."""

from sidemantic import SemanticLayer
from sidemantic.adapters.cube import CubeAdapter


def test_query_imported_cube_example():
    """Test that we can compile queries from imported Cube schema."""
    adapter = CubeAdapter()
    graph = adapter.parse("tests/fixtures/cube/orders.yml")

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


def test_query_with_time_dimension_cube():
    """Test querying time dimensions from Cube import."""
    adapter = CubeAdapter()
    graph = adapter.parse("tests/fixtures/cube/orders.yml")

    layer = SemanticLayer()
    layer.graph = graph

    sql = layer.compile(metrics=["orders.revenue"], dimensions=["orders.created_at"])
    assert "created_at" in sql.lower()
    assert "GROUP BY" in sql.upper()
