"""Tests for Holistics AML adapter - cross-format conversion."""

import tempfile
from pathlib import Path

import pytest

from sidemantic.adapters.cube import CubeAdapter
from sidemantic.adapters.holistics import HolisticsAdapter
from sidemantic.adapters.metricflow import MetricFlowAdapter


def test_holistics_to_cube_conversion():
    """Test converting Holistics AML to Cube format."""
    holistics_adapter = HolisticsAdapter()
    graph = holistics_adapter.parse("tests/fixtures/holistics/orders.model.aml")

    cube_adapter = CubeAdapter()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        cube_adapter.export(graph, temp_path)
        graph2 = cube_adapter.parse(temp_path)

        assert "orders" in graph2.models
        orders = graph2.models["orders"]
        assert "status" in [d.name for d in orders.dimensions]
        assert "revenue" in [m.name for m in orders.metrics]
    finally:
        temp_path.unlink(missing_ok=True)


def test_cube_to_holistics_conversion():
    """Test converting Cube format to Holistics AML."""
    cube_adapter = CubeAdapter()
    graph = cube_adapter.parse("tests/fixtures/cube/orders.yml")

    holistics_adapter = HolisticsAdapter()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".aml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        holistics_adapter.export(graph, temp_path)
        graph2 = holistics_adapter.parse(temp_path)

        assert "orders" in graph2.models
        orders = graph2.models["orders"]
        assert "status" in [d.name for d in orders.dimensions]
        assert "revenue" in [m.name for m in orders.metrics]
    finally:
        temp_path.unlink(missing_ok=True)


def test_holistics_to_metricflow_conversion():
    """Test converting Holistics AML to MetricFlow format."""
    holistics_adapter = HolisticsAdapter()
    graph = holistics_adapter.parse("tests/fixtures/holistics/orders.model.aml")

    mf_adapter = MetricFlowAdapter()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        mf_adapter.export(graph, temp_path)
        graph2 = mf_adapter.parse(temp_path)

        assert "orders" in graph2.models
        orders = graph2.models["orders"]
        assert "status" in [d.name for d in orders.dimensions]
        assert "revenue" in [m.name for m in orders.metrics]
    finally:
        temp_path.unlink(missing_ok=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
