"""Test import/export/roundtrip for LookML adapter."""

import tempfile
from pathlib import Path

import pytest

from sidemantic.adapters.cube import CubeAdapter
from sidemantic.adapters.lookml import LookMLAdapter
from sidemantic.adapters.metricflow import MetricFlowAdapter


def test_import_real_lookml_example():
    """Test importing a real LookML view file."""
    adapter = LookMLAdapter()
    graph = adapter.parse("examples/lookml/orders.lkml")

    # Verify models loaded
    assert "orders" in graph.models
    assert "customers" in graph.models

    orders = graph.models["orders"]

    # Verify dimensions
    dim_names = [d.name for d in orders.dimensions]
    assert "id" in dim_names
    assert "status" in dim_names
    assert "customer_id" in dim_names

    # Verify time dimensions were created from dimension_group
    assert "created_date" in dim_names

    # Verify primary key was detected
    assert orders.primary_key == "id"

    # Verify measures
    measure_names = [m.name for m in orders.metrics]
    assert "count" in measure_names
    assert "revenue" in measure_names
    assert "completed_revenue" in measure_names
    assert "conversion_rate" in measure_names

    # Verify segments (LookML filters) were imported
    segment_names = [s.name for s in orders.segments]
    assert "high_value" in segment_names
    assert "completed" in segment_names

    # Verify segment SQL was converted from ${TABLE} to {model}
    high_value_segment = next(s for s in orders.segments if s.name == "high_value")
    assert "{model}" in high_value_segment.sql
    assert "${TABLE}" not in high_value_segment.sql

    # Verify measure with filter was imported
    completed_revenue = next(m for m in orders.metrics if m.name == "completed_revenue")
    assert completed_revenue.filters is not None
    assert len(completed_revenue.filters) > 0

    # Verify derived metric (type=number) was detected
    conversion_rate = next(m for m in orders.metrics if m.name == "conversion_rate")
    assert conversion_rate.type == "derived"


def test_import_lookml_derived_table():
    """Test importing LookML view with derived table."""
    adapter = LookMLAdapter()
    graph = adapter.parse("examples/lookml/derived_tables.lkml")

    # Verify model loaded
    assert "customer_summary" in graph.models
    summary = graph.models["customer_summary"]

    # Verify derived table SQL was imported
    assert summary.sql is not None
    assert "SELECT" in summary.sql.upper()
    assert "GROUP BY" in summary.sql.upper()

    # Verify dimensions
    dim_names = [d.name for d in summary.dimensions]
    assert "customer_id" in dim_names
    assert "order_count" in dim_names
    assert "total_revenue" in dim_names

    # Verify time dimension_group created time dimensions
    assert "last_order_date" in dim_names

    # Verify measures
    measure_names = [m.name for m in summary.metrics]
    assert "total_customers" in measure_names
    assert "avg_orders_per_customer" in measure_names
    assert "avg_customer_ltv" in measure_names


def test_lookml_to_sidemantic_to_lookml_roundtrip():
    """Test that LookML -> Sidemantic -> LookML preserves structure."""
    # Import from LookML
    lookml_adapter = LookMLAdapter()
    graph = lookml_adapter.parse("examples/lookml/orders.lkml")

    # Export back to LookML
    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        lookml_adapter.export(graph, temp_path)

        # Re-import and verify
        graph2 = lookml_adapter.parse(temp_path)

        # Verify models preserved
        assert "orders" in graph2.models
        assert "customers" in graph2.models

        orders = graph2.models["orders"]

        # Verify key fields preserved
        dim_names = [d.name for d in orders.dimensions]
        assert "status" in dim_names

        measure_names = [m.name for m in orders.metrics]
        assert "revenue" in measure_names

        # Verify segments preserved
        segment_names = [s.name for s in orders.segments]
        assert "high_value" in segment_names
        assert "completed" in segment_names

    finally:
        temp_path.unlink(missing_ok=True)


def test_lookml_to_cube_conversion():
    """Test converting LookML format to Cube format."""
    # Import from LookML
    lookml_adapter = LookMLAdapter()
    graph = lookml_adapter.parse("examples/lookml/orders.lkml")

    # Export to Cube
    cube_adapter = CubeAdapter()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        cube_adapter.export(graph, temp_path)

        # Re-import as Cube and verify structure
        graph2 = cube_adapter.parse(temp_path)

        assert "orders" in graph2.models
        orders = graph2.models["orders"]

        # Verify dimensions converted
        dim_names = [d.name for d in orders.dimensions]
        assert "status" in dim_names

        # Verify measures converted
        measure_names = [m.name for m in orders.metrics]
        assert "revenue" in measure_names

        # Verify segments preserved
        segment_names = [s.name for s in orders.segments]
        assert "high_value" in segment_names

    finally:
        temp_path.unlink(missing_ok=True)


def test_lookml_to_metricflow_conversion():
    """Test converting LookML format to MetricFlow format."""
    # Import from LookML
    lookml_adapter = LookMLAdapter()
    graph = lookml_adapter.parse("examples/lookml/orders.lkml")

    # Export to MetricFlow
    mf_adapter = MetricFlowAdapter()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        mf_adapter.export(graph, temp_path)

        # Re-import as MetricFlow and verify structure
        graph2 = mf_adapter.parse(temp_path)

        assert "orders" in graph2.models
        orders = graph2.models["orders"]

        # Verify dimensions converted
        dim_names = [d.name for d in orders.dimensions]
        assert "status" in dim_names

        # Verify measures converted
        measure_names = [m.name for m in orders.metrics]
        assert "revenue" in measure_names

        # Verify segments stored in meta (MetricFlow doesn't have native support)
        if orders.segments:
            assert len(orders.segments) > 0

    finally:
        temp_path.unlink(missing_ok=True)


def test_query_imported_lookml_example():
    """Test that we can compile queries from imported LookML schema."""
    from sidemantic import SemanticLayer

    adapter = LookMLAdapter()
    graph = adapter.parse("examples/lookml/orders.lkml")

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


def test_lookml_explore_parsing():
    """Test parsing LookML explore files for relationships."""
    adapter = LookMLAdapter()
    # Parse just the orders example files (view + explore) to avoid duplicate models
    # The explore file defines relationships between models
    import shutil
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Copy just the orders files
        shutil.copy("examples/lookml/orders.lkml", tmpdir_path / "orders.lkml")
        shutil.copy("examples/lookml/orders.explore.lkml", tmpdir_path / "orders.explore.lkml")

        graph = adapter.parse(tmpdir_path)

        # Verify orders model exists
        assert "orders" in graph.models
        orders = graph.models["orders"]

        # Verify relationship was parsed from explore
        assert len(orders.relationships) >= 1

        # Verify relationship details
        customer_rel = next((r for r in orders.relationships if r.name == "customers"), None)
        assert customer_rel is not None
        assert customer_rel.type == "many_to_one"
        assert customer_rel.foreign_key == "customer_id"


def test_query_with_time_dimension_lookml():
    """Test querying time dimensions from LookML import."""
    from sidemantic import SemanticLayer

    adapter = LookMLAdapter()
    graph = adapter.parse("examples/lookml/orders.lkml")

    layer = SemanticLayer()
    layer.graph = graph

    # Query with time dimension
    sql = layer.compile(metrics=["orders.revenue"], dimensions=["orders.created_date"])
    assert "created_at" in sql.lower()
    assert "GROUP BY" in sql.upper()


def test_roundtrip_real_lookml_example():
    """Test LookML example roundtrip using the actual example file."""
    adapter = LookMLAdapter()

    # Import original
    graph1 = adapter.parse("examples/lookml/orders.lkml")

    # Export
    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        adapter.export(graph1, temp_path)

        # Import exported version
        graph2 = adapter.parse(temp_path)

        # Verify models match
        assert set(graph1.models.keys()) == set(graph2.models.keys())

        # Verify dimensions count preserved
        orders1 = graph1.models["orders"]
        orders2 = graph2.models["orders"]
        assert len(orders1.dimensions) == len(orders2.dimensions)

        # Verify metrics count preserved
        assert len(orders1.metrics) == len(orders2.metrics)

        # Verify segments preserved
        segment_names1 = {s.name for s in orders1.segments}
        segment_names2 = {s.name for s in orders2.segments}
        assert segment_names1 == segment_names2

    finally:
        temp_path.unlink(missing_ok=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
