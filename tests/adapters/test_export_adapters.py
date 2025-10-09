"""Test export adapters for Cube and MetricFlow."""

import tempfile
from pathlib import Path

import pytest
import yaml

from sidemantic.adapters.cube import CubeAdapter
from sidemantic.adapters.metricflow import MetricFlowAdapter
from sidemantic.adapters.sidemantic import SidemanticAdapter


def test_cube_export():
    """Test export to Cube format."""
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


def test_metricflow_export():
    """Test export to MetricFlow format."""
    # Load native format
    native_adapter = SidemanticAdapter()
    graph = native_adapter.parse("tests/fixtures/sidemantic/orders.yml")

    # Export to MetricFlow
    mf_adapter = MetricFlowAdapter()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        mf_adapter.export(graph, temp_path)

        # Verify file structure
        with open(temp_path) as f:
            data = yaml.safe_load(f)

        assert "semantic_models" in data
        assert "metrics" in data
        assert len(data["semantic_models"]) == 2
        assert len(data["metrics"]) == 3

        # Verify orders semantic model
        orders_model = next(m for m in data["semantic_models"] if m["name"] == "orders")
        assert "entities" in orders_model
        assert "dimensions" in orders_model
        assert "measures" in orders_model

        # Verify metrics
        metric_names = [m["name"] for m in data["metrics"]]
        assert "total_revenue" in metric_names
        assert "conversion_rate" in metric_names
        assert "revenue_per_order" in metric_names

        # Verify metric types
        total_revenue = next(m for m in data["metrics"] if m["name"] == "total_revenue")
        assert total_revenue["type"] == "simple"
        assert "type_params" in total_revenue
        assert total_revenue["type_params"]["measure"]["name"] == "orders.revenue"

        conversion_rate = next(m for m in data["metrics"] if m["name"] == "conversion_rate")
        assert conversion_rate["type"] == "ratio"
        assert conversion_rate["type_params"]["numerator"]["name"] == "orders.completed_revenue"

        # Verify round-trip (parse exported file)
        graph2 = mf_adapter.parse(temp_path)
        assert len(graph2.models) == 2
        assert len(graph2.metrics) == 3

    finally:
        temp_path.unlink(missing_ok=True)


def test_cube_round_trip():
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


def test_metricflow_round_trip():
    """Test Sidemantic -> MetricFlow -> Sidemantic round-trip."""
    # Load native
    native_adapter = SidemanticAdapter()
    graph = native_adapter.parse("tests/fixtures/sidemantic/orders.yml")

    # Export to MetricFlow
    mf_adapter = MetricFlowAdapter()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        mf_path = Path(f.name)

    try:
        mf_adapter.export(graph, mf_path)

        # Import from MetricFlow
        graph2 = mf_adapter.parse(mf_path)

        # Verify structure preserved
        assert set(graph2.models.keys()) == set(graph.models.keys())
        assert set(graph2.metrics.keys()) == set(graph.metrics.keys())

        # Verify metric types preserved (simple -> None since we removed simple type)
        assert graph2.metrics["total_revenue"].type is None  # Was simple, now untyped
        assert graph2.metrics["conversion_rate"].type == "ratio"
        assert graph2.metrics["revenue_per_order"].type == "derived"

    finally:
        mf_path.unlink(missing_ok=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
