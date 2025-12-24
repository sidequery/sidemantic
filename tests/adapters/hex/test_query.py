"""Tests for Hex adapter - query compilation."""

import pytest

from sidemantic.adapters.hex import HexAdapter


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
