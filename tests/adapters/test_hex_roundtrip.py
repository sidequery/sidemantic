"""Tests for Hex adapter parsing."""

import tempfile
from pathlib import Path

import pytest

from sidemantic.adapters.cube import CubeAdapter
from sidemantic.adapters.hex import HexAdapter
from sidemantic.adapters.lookml import LookMLAdapter
from sidemantic.adapters.metricflow import MetricFlowAdapter


def test_hex_to_sidemantic_to_hex_roundtrip():
    """Test that Hex -> Sidemantic -> Hex preserves structure."""
    # Import from Hex
    hex_adapter = HexAdapter()
    graph = hex_adapter.parse("tests/fixtures/hex/orders.yml")

    # Export back to Hex
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        hex_adapter.export(graph, temp_path)

        # Re-import and verify
        graph2 = hex_adapter.parse(temp_path)

        # Verify model preserved
        assert "orders" in graph2.models
        orders = graph2.models["orders"]

        # Verify key fields preserved
        dim_names = [d.name for d in orders.dimensions]
        assert "status" in dim_names
        assert "amount" in dim_names

        measure_names = [m.name for m in orders.metrics]
        assert "revenue" in measure_names
        assert "order_count" in measure_names

    finally:
        temp_path.unlink(missing_ok=True)


def test_hex_to_cube_conversion():
    """Test converting Hex format to Cube format."""
    # Import from Hex
    hex_adapter = HexAdapter()
    graph = hex_adapter.parse("tests/fixtures/hex/orders.yml")

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

    finally:
        temp_path.unlink(missing_ok=True)


def test_cube_to_hex_conversion():
    """Test converting Cube format to Hex format."""
    # Import from Cube
    cube_adapter = CubeAdapter()
    graph = cube_adapter.parse("tests/fixtures/cube/orders.yml")

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


def test_hex_to_metricflow_conversion():
    """Test converting Hex format to MetricFlow format."""
    # Import from Hex
    hex_adapter = HexAdapter()
    graph = hex_adapter.parse("tests/fixtures/hex/orders.yml")

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

    finally:
        temp_path.unlink(missing_ok=True)


def test_hex_to_lookml_conversion():
    """Test converting Hex format to LookML format."""
    # Import from Hex
    hex_adapter = HexAdapter()
    graph = hex_adapter.parse("tests/fixtures/hex/orders.yml")

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

    finally:
        temp_path.unlink(missing_ok=True)


def test_roundtrip_real_hex_example():
    """Test Hex example roundtrip using actual example files."""
    adapter = HexAdapter()

    # Import original
    graph1 = adapter.parse("tests/fixtures/hex/orders.yml")

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

    finally:
        temp_path.unlink(missing_ok=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
