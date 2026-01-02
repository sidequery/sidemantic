"""Tests for MetricFlow adapter - conversion between formats."""

import tempfile
from pathlib import Path

import pytest
import yaml

from sidemantic.adapters.cube import CubeAdapter
from sidemantic.adapters.metricflow import MetricFlowAdapter
from sidemantic.adapters.sidemantic import SidemanticAdapter

# =============================================================================
# CROSS-FORMAT CONVERSION TESTS
# =============================================================================


def test_metricflow_to_cube_conversion():
    """Test converting MetricFlow format to Cube format."""
    # Import from MetricFlow
    mf_adapter = MetricFlowAdapter()
    graph = mf_adapter.parse("tests/fixtures/metricflow/semantic_models.yml")

    # Export to Cube
    cube_adapter = CubeAdapter()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        cube_adapter.export(graph, temp_path)

        # Re-import as Cube and verify structure
        graph2 = cube_adapter.parse(temp_path)

        assert "orders" in graph2.models
        assert "customers" in graph2.models

        orders = graph2.models["orders"]

        # Verify dimensions converted
        dim_names = [d.name for d in orders.dimensions]
        assert "status" in dim_names

        # Verify measures converted
        measure_names = [m.name for m in orders.metrics]
        assert "revenue" in measure_names

    finally:
        temp_path.unlink(missing_ok=True)


# =============================================================================
# SIDEMANTIC CONVERSION TESTS
# =============================================================================


def test_sidemantic_to_metricflow_export():
    """Test export from Sidemantic to MetricFlow format."""
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


def test_sidemantic_to_metricflow_roundtrip():
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
