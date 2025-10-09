"""Tests for Superset adapter parsing."""

import tempfile
from pathlib import Path

import pytest

from sidemantic.adapters.cube import CubeAdapter
from sidemantic.adapters.lookml import LookMLAdapter
from sidemantic.adapters.metricflow import MetricFlowAdapter
from sidemantic.adapters.omni import OmniAdapter
from sidemantic.adapters.rill import RillAdapter
from sidemantic.adapters.superset import SupersetAdapter


def test_superset_to_sidemantic_to_superset_roundtrip():
    """Test roundtrip: Superset → Sidemantic → Superset."""
    adapter = SupersetAdapter()

    # Import original
    graph1 = adapter.parse("tests/fixtures/superset/orders.yaml")

    # Export
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir)
        adapter.export(graph1, output_path)

        # Import exported version
        graph2 = adapter.parse(output_path / "orders.yaml")

        # Verify models match
        assert set(graph1.models.keys()) == set(graph2.models.keys())

        # Verify dimensions preserved
        orders1 = graph1.models["orders"]
        orders2 = graph2.models["orders"]
        assert len(orders1.dimensions) == len(orders2.dimensions)

        # Verify metrics preserved
        assert len(orders1.metrics) == len(orders2.metrics)


def test_superset_to_cube_conversion():
    """Test converting Superset dataset to Cube format."""
    superset_adapter = SupersetAdapter()
    cube_adapter = CubeAdapter()

    # Import from Superset
    graph = superset_adapter.parse("tests/fixtures/superset/orders.yaml")

    # Export to Cube
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "orders.yml"
        cube_adapter.export(graph, output_path)

        # Verify Cube file was created
        assert output_path.exists()

        # Import Cube version
        cube_graph = cube_adapter.parse(output_path)

        # Verify model exists
        assert "orders" in cube_graph.models


def test_cube_to_superset_conversion():
    """Test converting Cube schema to Superset dataset."""
    cube_adapter = CubeAdapter()
    superset_adapter = SupersetAdapter()

    # Import from Cube
    graph = cube_adapter.parse("tests/fixtures/cube/orders.yml")

    # Export to Superset
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir)
        superset_adapter.export(graph, output_path)

        # Import Superset version
        superset_graph = superset_adapter.parse(output_path / "orders.yaml")

        # Verify model exists
        assert "orders" in superset_graph.models


def test_superset_to_metricflow_conversion():
    """Test converting Superset dataset to MetricFlow."""
    superset_adapter = SupersetAdapter()
    mf_adapter = MetricFlowAdapter()

    # Import from Superset
    graph = superset_adapter.parse("tests/fixtures/superset/orders.yaml")

    # Export to MetricFlow
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "orders.yml"
        mf_adapter.export(graph, output_path)

        # Verify file created
        assert output_path.exists()


def test_superset_to_lookml_conversion():
    """Test converting Superset dataset to LookML."""
    superset_adapter = SupersetAdapter()
    lookml_adapter = LookMLAdapter()

    # Import from Superset
    graph = superset_adapter.parse("tests/fixtures/superset/orders.yaml")

    # Export to LookML
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "orders.lkml"
        lookml_adapter.export(graph, output_path)

        # Verify file created
        assert output_path.exists()


def test_superset_to_rill_conversion():
    """Test converting Superset dataset to Rill."""
    superset_adapter = SupersetAdapter()
    rill_adapter = RillAdapter()

    # Import from Superset
    graph = superset_adapter.parse("tests/fixtures/superset/orders.yaml")

    # Export to Rill
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir)
        rill_adapter.export(graph, output_path)

        # Verify file created
        assert (output_path / "orders.yaml").exists()


def test_omni_to_superset_conversion():
    """Test converting Omni view to Superset."""
    omni_adapter = OmniAdapter()
    superset_adapter = SupersetAdapter()

    # Import from Omni
    graph = omni_adapter.parse("tests/fixtures/omni/")

    # Export to Superset
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir)
        superset_adapter.export(graph, output_path)

        # Verify file created
        assert (output_path / "orders.yaml").exists()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
