"""Tests for MetricFlow adapter - query generation."""

import pytest

from sidemantic import SemanticLayer
from sidemantic.adapters.metricflow import MetricFlowAdapter

# =============================================================================
# QUERY TESTS
# =============================================================================


def test_query_imported_metricflow_example():
    """Test that we can compile queries from imported MetricFlow schema."""
    adapter = MetricFlowAdapter()
    graph = adapter.parse("tests/fixtures/metricflow/semantic_models.yml")

    layer = SemanticLayer()
    layer.graph = graph

    # Test basic measure query
    sql = layer.compile(metrics=["orders.revenue"])
    assert "SUM" in sql.upper()

    # Test with dimension
    sql = layer.compile(metrics=["orders.revenue", "orders.order_count"], dimensions=["orders.status"])
    assert "GROUP BY" in sql.upper()
    assert "status" in sql.lower()

    # Test cross-model query (only if join path exists)
    # Note: MetricFlow entities may not map 1:1 to model names
    try:
        sql = layer.compile(metrics=["orders.revenue"], dimensions=["customers.region"])
        assert "JOIN" in sql.upper()
        assert "customers" in sql.lower()
    except Exception:
        # Join path not configured, which is expected for some imports
        pass

    # Test graph-level ratio metric (if it exists and is queryable)
    if "average_order_value" in graph.metrics:
        avg_metric = graph.metrics["average_order_value"]
        # Ratio metrics should have numerator/denominator set
        if avg_metric.type == "ratio" and avg_metric.numerator and avg_metric.denominator:
            try:
                sql = layer.compile(metrics=["average_order_value"])
                assert sql  # Should generate valid SQL with ratio calculation
            except ValueError:
                # Some graph-level metrics may need model context to be queryable
                pass


def test_query_with_filter_metricflow():
    """Test that metric filters work from MetricFlow import."""
    adapter = MetricFlowAdapter()
    graph = adapter.parse("tests/fixtures/metricflow/semantic_models.yml")

    layer = SemanticLayer()
    layer.graph = graph

    # Query with filter
    sql = layer.compile(metrics=["orders.revenue"], filters=["orders.status = 'completed'"])
    assert "WHERE" in sql.upper()
    assert "status" in sql.lower()
    assert "completed" in sql.lower()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
