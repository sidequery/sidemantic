"""Tests for Omni adapter - cross-format conversion."""

import tempfile
from pathlib import Path

import pytest

from sidemantic.adapters.cube import CubeAdapter
from sidemantic.adapters.lookml import LookMLAdapter
from sidemantic.adapters.metricflow import MetricFlowAdapter
from sidemantic.adapters.omni import OmniAdapter
from sidemantic.adapters.rill import RillAdapter
from sidemantic.adapters.superset import SupersetAdapter


def test_omni_to_cube_conversion():
    """Test converting Omni view to Cube format."""
    omni_adapter = OmniAdapter()
    cube_adapter = CubeAdapter()

    # Import from Omni
    graph = omni_adapter.parse("tests/fixtures/omni/")

    # Export to Cube
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "orders.yml"
        cube_adapter.export(graph, output_path)

        # Verify Cube file was created
        assert output_path.exists()


def test_cube_to_omni_conversion():
    """Test converting Cube schema to Omni view."""
    cube_adapter = CubeAdapter()
    omni_adapter = OmniAdapter()

    # Import from Cube
    graph = cube_adapter.parse("tests/fixtures/cube/orders.yml")

    # Export to Omni
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir)
        omni_adapter.export(graph, output_path)

        # Import Omni version
        omni_graph = omni_adapter.parse(output_path)

        # Verify model exists
        assert "orders" in omni_graph.models


def test_omni_to_metricflow_conversion():
    """Test converting Omni view to MetricFlow."""
    omni_adapter = OmniAdapter()
    mf_adapter = MetricFlowAdapter()

    # Import from Omni
    graph = omni_adapter.parse("tests/fixtures/omni/")

    # Export to MetricFlow
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "orders.yml"
        mf_adapter.export(graph, output_path)

        # Verify file created
        assert output_path.exists()


def test_omni_to_lookml_conversion():
    """Test converting Omni view to LookML."""
    omni_adapter = OmniAdapter()
    lookml_adapter = LookMLAdapter()

    # Import from Omni
    graph = omni_adapter.parse("tests/fixtures/omni/")

    # Export to LookML
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "orders.lkml"
        lookml_adapter.export(graph, output_path)

        # Verify file created
        assert output_path.exists()


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


def test_omni_to_rill_conversion():
    """Test converting Omni view to Rill."""
    omni_adapter = OmniAdapter()
    rill_adapter = RillAdapter()

    # Import from Omni
    graph = omni_adapter.parse("tests/fixtures/omni/")

    # Export to Rill
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir)
        rill_adapter.export(graph, output_path)

        # Verify file created
        assert (output_path / "orders.yaml").exists()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
