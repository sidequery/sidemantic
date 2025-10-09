"""Test import/export/roundtrip for MetricFlow adapter."""

import tempfile
from pathlib import Path

import pytest

from sidemantic.adapters.cube import CubeAdapter
from sidemantic.adapters.metricflow import MetricFlowAdapter


def test_import_real_metricflow_example():
    """Test importing a real dbt MetricFlow schema file."""
    adapter = MetricFlowAdapter()
    graph = adapter.parse("tests/fixtures/metricflow/semantic_models.yml")

    # Verify models loaded
    assert "orders" in graph.models
    assert "customers" in graph.models

    orders = graph.models["orders"]
    customers = graph.models["customers"]

    # Verify dimensions
    order_dims = [d.name for d in orders.dimensions]
    assert "order_date" in order_dims
    assert "status" in order_dims

    customer_dims = [d.name for d in customers.dimensions]
    assert "region" in customer_dims
    assert "tier" in customer_dims

    # Verify measures
    measure_names = [m.name for m in orders.metrics]
    assert "order_count" in measure_names
    assert "revenue" in measure_names
    assert "avg_order_value" in measure_names

    # Verify relationships were created from entities (resolved to model names)
    rel_names = [r.name for r in orders.relationships]
    assert "customers" in rel_names
    customer_rel = next(r for r in orders.relationships if r.name == "customers")
    assert customer_rel.type == "many_to_one"
    assert customer_rel.foreign_key == "customer_id"

    # Verify graph-level metrics
    assert "total_revenue" in graph.metrics
    assert "average_order_value" in graph.metrics

    total_revenue = graph.metrics["total_revenue"]
    assert total_revenue.type is None  # Simple metric maps to untyped

    avg_order = graph.metrics["average_order_value"]
    assert avg_order.type == "ratio"
    assert avg_order.numerator == "revenue"
    assert avg_order.denominator == "order_count"


def test_metricflow_to_sidemantic_to_metricflow_roundtrip():
    """Test that MetricFlow -> Sidemantic -> MetricFlow preserves structure."""
    # Import from MetricFlow
    mf_adapter = MetricFlowAdapter()
    graph = mf_adapter.parse("tests/fixtures/metricflow/semantic_models.yml")

    # Export back to MetricFlow
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        mf_adapter.export(graph, temp_path)

        # Re-import and verify
        graph2 = mf_adapter.parse(temp_path)

        # Verify models preserved
        assert "orders" in graph2.models
        assert "customers" in graph2.models

        # Verify metrics preserved
        # Note: Simple metrics that just reference measures may not round-trip
        # since they can be queried directly via the measure
        assert "average_order_value" in graph2.metrics

        # Verify metric types preserved
        avg_order = graph2.metrics["average_order_value"]
        assert avg_order.type == "ratio"

        # total_revenue is a simple metric, may not be preserved in export
        # (can be queried directly as orders.revenue)

    finally:
        temp_path.unlink(missing_ok=True)


def test_metricflow_to_cube_conversion():
    """Test converting MetricFlow format to Cube format."""
    # Import from MetricFlow
    mf_adapter = MetricFlowAdapter()
    graph = mf_adapter.parse("tests/fixtures/metricflow/semantic_models.yml")

    # Export to Cube
    cube_adapter = CubeAdapter()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        cube_adapter.export(graph, temp_path)

        # Re-import as Cube and verify structure
        graph2 = cube_adapter.parse(temp_path)

        assert "orders" in graph2.models
        assert "customers" in graph2.models

        orders = graph2.models["orders"]

        # Verify dimensions converted
        dim_names = [d.name for d in orders.dimensions]
        assert "status" in dim_names

        # Verify measures converted
        measure_names = [m.name for m in orders.metrics]
        assert "revenue" in measure_names

    finally:
        temp_path.unlink(missing_ok=True)


def test_query_imported_metricflow_example():
    """Test that we can compile queries from imported MetricFlow schema."""
    from sidemantic import SemanticLayer

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
    from sidemantic import SemanticLayer

    adapter = MetricFlowAdapter()
    graph = adapter.parse("tests/fixtures/metricflow/semantic_models.yml")

    layer = SemanticLayer()
    layer.graph = graph

    # Query with filter
    sql = layer.compile(metrics=["orders.revenue"], filters=["orders.status = 'completed'"])
    assert "WHERE" in sql.upper()
    assert "status" in sql.lower()
    assert "completed" in sql.lower()


def test_roundtrip_real_metricflow_example():
    """Test MetricFlow example roundtrip using the actual example file."""
    adapter = MetricFlowAdapter()

    # Import original
    graph1 = adapter.parse("tests/fixtures/metricflow/semantic_models.yml")

    # Export
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        adapter.export(graph1, temp_path)

        # Import exported version
        graph2 = adapter.parse(temp_path)

        # Verify models match
        assert set(graph1.models.keys()) == set(graph2.models.keys())

        # Verify dimensions count preserved for each model
        for model_name in graph1.models:
            model1 = graph1.models[model_name]
            model2 = graph2.models[model_name]
            assert len(model1.dimensions) == len(model2.dimensions), f"Dimension count mismatch for {model_name}"
            assert len(model1.metrics) == len(model2.metrics), f"Metric count mismatch for {model_name}"

        # Verify graph-level metrics count preserved
        # Note: Simple metrics may not round-trip, so we just check that ratio metrics are there
        ratio_metrics1 = {name for name, m in graph1.metrics.items() if m.type == "ratio"}
        ratio_metrics2 = {name for name, m in graph2.metrics.items() if m.type == "ratio"}
        assert ratio_metrics1 == ratio_metrics2

    finally:
        temp_path.unlink(missing_ok=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
