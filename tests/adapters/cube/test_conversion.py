"""Tests for Cube adapter - cross-format conversion."""

import tempfile
from pathlib import Path

import yaml

from sidemantic.adapters.cube import CubeAdapter
from sidemantic.adapters.hex import HexAdapter
from sidemantic.adapters.lookml import LookMLAdapter
from sidemantic.adapters.metricflow import MetricFlowAdapter
from sidemantic.adapters.omni import OmniAdapter
from sidemantic.adapters.rill import RillAdapter
from sidemantic.adapters.sidemantic import SidemanticAdapter
from sidemantic.adapters.superset import SupersetAdapter


def test_cube_to_metricflow_conversion():
    """Test converting Cube format to MetricFlow format."""
    cube_adapter = CubeAdapter()
    graph = cube_adapter.parse("tests/fixtures/cube/orders.yml")

    mf_adapter = MetricFlowAdapter()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        mf_adapter.export(graph, temp_path)
        graph2 = mf_adapter.parse(temp_path)

        assert "orders" in graph2.models
        orders = graph2.models["orders"]

        dim_names = [d.name for d in orders.dimensions]
        assert "status" in dim_names

        measure_names = [m.name for m in orders.metrics]
        assert "revenue" in measure_names

        if orders.segments:
            assert len(orders.segments) > 0

    finally:
        temp_path.unlink(missing_ok=True)


def test_cube_to_lookml_conversion():
    """Test converting Cube format to LookML format."""
    cube_adapter = CubeAdapter()
    graph = cube_adapter.parse("tests/fixtures/cube/orders.yml")

    lookml_adapter = LookMLAdapter()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        lookml_adapter.export(graph, temp_path)
        graph2 = lookml_adapter.parse(temp_path)

        assert "orders" in graph2.models
        orders = graph2.models["orders"]

        dim_names = [d.name for d in orders.dimensions]
        assert "status" in dim_names

        measure_names = [m.name for m in orders.metrics]
        assert "revenue" in measure_names

        if orders.segments:
            assert len(orders.segments) > 0

    finally:
        temp_path.unlink(missing_ok=True)


def test_cube_to_hex_conversion():
    """Test converting Cube format to Hex format."""
    cube_adapter = CubeAdapter()
    graph = cube_adapter.parse("tests/fixtures/cube/orders.yml")

    hex_adapter = HexAdapter()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        hex_adapter.export(graph, temp_path)
        graph2 = hex_adapter.parse(temp_path)

        assert "orders" in graph2.models
        orders = graph2.models["orders"]

        dim_names = [d.name for d in orders.dimensions]
        assert "status" in dim_names

        measure_names = [m.name for m in orders.metrics]
        assert "revenue" in measure_names

    finally:
        temp_path.unlink(missing_ok=True)


def test_cube_to_rill_conversion():
    """Test converting Cube format to Rill format."""
    cube_adapter = CubeAdapter()
    graph = cube_adapter.parse("tests/fixtures/cube/orders.yml")

    rill_adapter = RillAdapter()
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir)
        rill_adapter.export(graph, output_path)

        graph2 = rill_adapter.parse(output_path / "orders.yaml")

        assert "orders" in graph2.models
        orders = graph2.models["orders"]

        dim_names = [d.name for d in orders.dimensions]
        assert "status" in dim_names

        measure_names = [m.name for m in orders.metrics]
        assert "revenue" in measure_names


def test_cube_to_superset_conversion():
    """Test converting Cube schema to Superset dataset."""
    cube_adapter = CubeAdapter()
    superset_adapter = SupersetAdapter()

    graph = cube_adapter.parse("tests/fixtures/cube/orders.yml")

    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir)
        superset_adapter.export(graph, output_path)

        superset_graph = superset_adapter.parse(output_path / "orders.yaml")

        assert "orders" in superset_graph.models


def test_cube_to_omni_conversion():
    """Test converting Cube schema to Omni view."""
    cube_adapter = CubeAdapter()
    omni_adapter = OmniAdapter()

    graph = cube_adapter.parse("tests/fixtures/cube/orders.yml")

    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir)
        omni_adapter.export(graph, output_path)

        omni_graph = omni_adapter.parse(output_path)

        assert "orders" in omni_graph.models


def test_sidemantic_to_cube_export():
    """Test export from Sidemantic to Cube format."""
    # Load native format
    native_adapter = SidemanticAdapter()
    graph = native_adapter.parse("tests/fixtures/sidemantic/orders.yml")

    # Export to Cube
    cube_adapter = CubeAdapter()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        cube_adapter.export(graph, temp_path)

        # Verify file structure
        with open(temp_path) as f:
            data = yaml.safe_load(f)

        assert "cubes" in data
        assert len(data["cubes"]) == 2

        # Verify orders cube
        orders_cube = next(c for c in data["cubes"] if c["name"] == "orders")
        assert orders_cube["sql_table"] == "public.orders"
        assert "dimensions" in orders_cube
        assert "measures" in orders_cube
        # Note: joins only exported when foreign entity name matches target model name

        # Verify round-trip (parse exported file)
        graph2 = cube_adapter.parse(temp_path)
        assert len(graph2.models) == 2

    finally:
        temp_path.unlink(missing_ok=True)


def test_sidemantic_to_cube_roundtrip():
    """Test Sidemantic -> Cube -> Sidemantic round-trip."""
    # Load native
    native_adapter = SidemanticAdapter()
    graph = native_adapter.parse("tests/fixtures/sidemantic/orders.yml")

    # Export to Cube
    cube_adapter = CubeAdapter()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        cube_path = Path(f.name)

    try:
        cube_adapter.export(graph, cube_path)

        # Import from Cube
        graph2 = cube_adapter.parse(cube_path)

        # Verify structure preserved
        assert set(graph2.models.keys()) == set(graph.models.keys())

        # Verify measures preserved
        orders1 = graph.models["orders"]
        orders2 = graph2.models["orders"]
        assert len(orders1.metrics) == len(orders2.metrics)

    finally:
        cube_path.unlink(missing_ok=True)
