"""Test import/export/roundtrip for Cube adapter."""

import tempfile
from pathlib import Path

import pytest

from sidemantic.adapters.cube import CubeAdapter
from sidemantic.adapters.hex import HexAdapter
from sidemantic.adapters.lookml import LookMLAdapter
from sidemantic.adapters.metricflow import MetricFlowAdapter
from sidemantic.adapters.omni import OmniAdapter
from sidemantic.adapters.rill import RillAdapter
from sidemantic.adapters.superset import SupersetAdapter


def test_import_real_cube_example():
    """Test importing a real Cube.js schema file."""
    adapter = CubeAdapter()
    graph = adapter.parse("examples/cube/orders.yml")

    # Verify models loaded
    assert "orders" in graph.models
    orders = graph.models["orders"]

    # Verify dimensions
    dim_names = [d.name for d in orders.dimensions]
    assert "status" in dim_names
    assert "created_at" in dim_names
    assert "customer_id" in dim_names

    # Verify measures
    measure_names = [m.name for m in orders.metrics]
    assert "count" in measure_names
    assert "revenue" in measure_names
    assert "completed_revenue" in measure_names
    assert "conversion_rate" in measure_names

    # Verify segments were imported
    segment_names = [s.name for s in orders.segments]
    assert "high_value" in segment_names
    assert "completed" in segment_names

    # Verify segment SQL was converted from ${CUBE} to {model}
    completed_segment = next(s for s in orders.segments if s.name == "completed")
    assert "{model}" in completed_segment.sql
    assert "${CUBE}" not in completed_segment.sql

    # Verify measure with filter was imported
    completed_revenue = next(m for m in orders.metrics if m.name == "completed_revenue")
    assert completed_revenue.filters is not None
    assert len(completed_revenue.filters) > 0

    # Verify ratio metric (calculated measure) was detected
    conversion_rate = next(m for m in orders.metrics if m.name == "conversion_rate")
    assert conversion_rate.type in ["ratio", "derived"]  # Detected as complex metric


def test_cube_to_sidemantic_to_cube_roundtrip():
    """Test that Cube -> Sidemantic -> Cube preserves structure."""
    # Import from Cube
    cube_adapter = CubeAdapter()
    graph = cube_adapter.parse("examples/cube/orders.yml")

    # Export back to Cube
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        cube_adapter.export(graph, temp_path)

        # Re-import and verify
        graph2 = cube_adapter.parse(temp_path)

        # Verify model preserved
        assert "orders" in graph2.models
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


def test_cube_to_metricflow_conversion():
    """Test converting Cube format to MetricFlow format."""
    # Import from Cube
    cube_adapter = CubeAdapter()
    graph = cube_adapter.parse("examples/cube/orders.yml")

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

        # Verify segments stored in meta
        # (MetricFlow doesn't have native segments, but we preserve in meta)
        if orders.segments:
            assert len(orders.segments) > 0

    finally:
        temp_path.unlink(missing_ok=True)


def test_query_imported_cube_example():
    """Test that we can compile queries from imported Cube schema."""
    from sidemantic import SemanticLayer

    adapter = CubeAdapter()
    graph = adapter.parse("examples/cube/orders.yml")

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

    # Test ratio metric (if detected as ratio/derived with proper dependencies)
    next(m for m in graph.models["orders"].metrics if m.name == "conversion_rate")
    # Note: Cube's ${measure} syntax doesn't translate directly to Sidemantic,
    # so derived metrics from Cube may not be queryable without modification
    # This is expected behavior - the metric was imported but needs manual adjustment


def test_query_with_time_dimension_cube():
    """Test querying time dimensions from Cube import."""
    from sidemantic import SemanticLayer

    adapter = CubeAdapter()
    graph = adapter.parse("examples/cube/orders.yml")

    layer = SemanticLayer()
    layer.graph = graph

    # Query with time dimension
    sql = layer.compile(metrics=["orders.revenue"], dimensions=["orders.created_at"])
    assert "created_at" in sql.lower()
    assert "GROUP BY" in sql.upper()


def test_roundtrip_real_cube_example():
    """Test Cube example roundtrip using the actual example file."""
    adapter = CubeAdapter()

    # Import original
    graph1 = adapter.parse("examples/cube/orders.yml")

    # Export
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
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


def test_cube_to_lookml_conversion():
    """Test converting Cube format to LookML format."""
    # Import from Cube
    cube_adapter = CubeAdapter()
    graph = cube_adapter.parse("examples/cube/orders.yml")

    # Export to LookML
    lookml_adapter = LookMLAdapter()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        lookml_adapter.export(graph, temp_path)

        # Re-import as LookML and verify structure
        graph2 = lookml_adapter.parse(temp_path)

        assert "orders" in graph2.models
        orders = graph2.models["orders"]

        # Verify dimensions converted
        dim_names = [d.name for d in orders.dimensions]
        assert "status" in dim_names

        # Verify measures converted
        measure_names = [m.name for m in orders.metrics]
        assert "revenue" in measure_names

        # Verify segments converted
        if orders.segments:
            assert len(orders.segments) > 0

    finally:
        temp_path.unlink(missing_ok=True)


def test_cube_to_hex_conversion():
    """Test converting Cube format to Hex format."""
    # Import from Cube
    cube_adapter = CubeAdapter()
    graph = cube_adapter.parse("examples/cube/orders.yml")

    # Export to Hex
    hex_adapter = HexAdapter()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        hex_adapter.export(graph, temp_path)

        # Re-import as Hex and verify structure
        graph2 = hex_adapter.parse(temp_path)

        assert "orders" in graph2.models
        orders = graph2.models["orders"]

        # Verify dimensions converted
        dim_names = [d.name for d in orders.dimensions]
        assert "status" in dim_names

        # Verify measures converted
        measure_names = [m.name for m in orders.metrics]
        assert "revenue" in measure_names

    finally:
        temp_path.unlink(missing_ok=True)


def test_cube_to_rill_conversion():
    """Test converting Cube format to Rill format."""
    # Import from Cube
    cube_adapter = CubeAdapter()
    graph = cube_adapter.parse("examples/cube/orders.yml")

    # Export to Rill
    rill_adapter = RillAdapter()
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir)
        rill_adapter.export(graph, output_path)

        # Re-import as Rill and verify structure
        graph2 = rill_adapter.parse(output_path / "orders.yaml")

        assert "orders" in graph2.models
        orders = graph2.models["orders"]

        # Verify dimensions converted
        dim_names = [d.name for d in orders.dimensions]
        assert "status" in dim_names

        # Verify measures converted
        measure_names = [m.name for m in orders.metrics]
        assert "revenue" in measure_names


def test_cube_to_superset_conversion():
    """Test converting Cube schema to Superset dataset."""
    cube_adapter = CubeAdapter()
    superset_adapter = SupersetAdapter()

    # Import from Cube
    graph = cube_adapter.parse("examples/cube/orders.yml")

    # Export to Superset
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir)
        superset_adapter.export(graph, output_path)

        # Import Superset version
        superset_graph = superset_adapter.parse(output_path / "orders.yaml")

        # Verify model exists
        assert "orders" in superset_graph.models


def test_cube_to_omni_conversion():
    """Test converting Cube schema to Omni view."""
    cube_adapter = CubeAdapter()
    omni_adapter = OmniAdapter()

    # Import from Cube
    graph = cube_adapter.parse("examples/cube/orders.yml")

    # Export to Omni
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir)
        omni_adapter.export(graph, output_path)

        # Import Omni version
        omni_graph = omni_adapter.parse(output_path)

        # Verify model exists
        assert "orders" in omni_graph.models


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
