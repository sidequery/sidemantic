"""Tests for Omni adapter parsing."""

import tempfile
from pathlib import Path

import pytest

from sidemantic.adapters.cube import CubeAdapter
from sidemantic.adapters.lookml import LookMLAdapter
from sidemantic.adapters.metricflow import MetricFlowAdapter
from sidemantic.adapters.omni import OmniAdapter
from sidemantic.adapters.rill import RillAdapter
from sidemantic.adapters.superset import SupersetAdapter
from tests.adapters.helpers import (
    assert_dimension_equivalent,
    assert_graph_equivalent,
    assert_metric_equivalent,
)


def test_omni_to_sidemantic_to_omni_roundtrip():
    """Test roundtrip: Omni → Sidemantic → Omni."""
    adapter = OmniAdapter()

    # Import original
    graph1 = adapter.parse("tests/fixtures/omni/")

    # Export
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir)
        adapter.export(graph1, output_path)

        # Import exported version
        graph2 = adapter.parse(output_path)

        # Verify semantic equivalence
        # NOTE: Omni doesn't have native segments
        assert_graph_equivalent(graph1, graph2, check_segments=False)


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


def test_omni_roundtrip_dimension_properties():
    """Test that dimension properties survive Omni roundtrip."""
    adapter = OmniAdapter()
    graph1 = adapter.parse("tests/fixtures/omni/")
    orders1 = graph1.models["orders"]

    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir)
        adapter.export(graph1, output_path)
        graph2 = adapter.parse(output_path)
        orders2 = graph2.models["orders"]

        for dim1 in orders1.dimensions:
            dim2 = orders2.get_dimension(dim1.name)
            assert dim2 is not None, f"Dimension {dim1.name} missing after roundtrip"
            assert_dimension_equivalent(dim1, dim2)


def test_omni_roundtrip_metric_properties():
    """Test that metric properties survive Omni roundtrip."""
    adapter = OmniAdapter()
    graph1 = adapter.parse("tests/fixtures/omni/")
    orders1 = graph1.models["orders"]

    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir)
        adapter.export(graph1, output_path)
        graph2 = adapter.parse(output_path)
        orders2 = graph2.models["orders"]

        for m1 in orders1.metrics:
            m2 = orders2.get_metric(m1.name)
            assert m2 is not None, f"Metric {m1.name} missing after roundtrip"
            assert_metric_equivalent(m1, m2)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
