"""Tests for LookML adapter - cross-format conversion."""

import tempfile
from pathlib import Path

import pytest

from sidemantic.adapters.cube import CubeAdapter
from sidemantic.adapters.lookml import LookMLAdapter
from sidemantic.adapters.metricflow import MetricFlowAdapter

# =============================================================================
# CROSS-FORMAT CONVERSION TESTS
# =============================================================================


def test_lookml_to_cube_conversion():
    """Test converting LookML format to Cube format."""
    # Import from LookML
    lookml_adapter = LookMLAdapter()
    graph = lookml_adapter.parse("tests/fixtures/lookml/orders.lkml")

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
    graph = lookml_adapter.parse("tests/fixtures/lookml/orders.lkml")

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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
