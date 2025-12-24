"""Tests for cross-format conversion with Rill adapter."""

import tempfile
from pathlib import Path

from sidemantic.adapters.cube import CubeAdapter
from sidemantic.adapters.lookml import LookMLAdapter
from sidemantic.adapters.metricflow import MetricFlowAdapter
from sidemantic.adapters.omni import OmniAdapter
from sidemantic.adapters.rill import RillAdapter
from sidemantic.adapters.superset import SupersetAdapter


def test_rill_to_cube_conversion():
    """Test converting Rill format to Cube format."""
    # Import from Rill
    rill_adapter = RillAdapter()
    graph = rill_adapter.parse("tests/fixtures/rill/orders.yaml")

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
        assert "total_revenue" in measure_names

    finally:
        temp_path.unlink(missing_ok=True)


def test_cube_to_rill_conversion():
    """Test converting Cube format to Rill format."""
    # Import from Cube
    cube_adapter = CubeAdapter()
    graph = cube_adapter.parse("tests/fixtures/cube/orders.yml")

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


def test_rill_to_metricflow_conversion():
    """Test converting Rill format to MetricFlow format."""
    # Import from Rill
    rill_adapter = RillAdapter()
    graph = rill_adapter.parse("tests/fixtures/rill/orders.yaml")

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
        assert "total_revenue" in measure_names

    finally:
        temp_path.unlink(missing_ok=True)


def test_rill_to_lookml_conversion():
    """Test converting Rill format to LookML format."""
    # Import from Rill
    rill_adapter = RillAdapter()
    graph = rill_adapter.parse("tests/fixtures/rill/orders.yaml")

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
        assert "total_revenue" in measure_names

    finally:
        temp_path.unlink(missing_ok=True)


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
