"""Tests for Superset adapter - parsing, export, and roundtrip."""

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

# =============================================================================
# PARSING TESTS
# =============================================================================


def test_import_real_superset_example():
    """Test importing real Superset dataset files."""
    adapter = SupersetAdapter()
    graph = adapter.parse("tests/fixtures/superset/")

    # Verify models loaded
    assert "orders" in graph.models
    assert "sales_summary" in graph.models

    # Verify orders dataset
    orders = graph.models["orders"]
    assert orders.table == "public.orders"
    assert orders.description == "Customer orders dataset"

    # Verify dimensions
    dim_names = [d.name for d in orders.dimensions]
    assert "id" in dim_names
    assert "customer_id" in dim_names
    assert "created_at" in dim_names
    assert "status" in dim_names
    assert "amount" in dim_names

    # Verify main datetime column
    created_at = next(d for d in orders.dimensions if d.name == "created_at")
    assert created_at.type == "time"
    assert created_at.label == "Order Date"

    # Verify metrics
    metric_names = [m.name for m in orders.metrics]
    assert "count" in metric_names
    assert "total_revenue" in metric_names
    assert "avg_order_value" in metric_names

    # Verify metric types
    count_metric = next(m for m in orders.metrics if m.name == "count")
    assert count_metric.agg == "count"

    revenue_metric = next(m for m in orders.metrics if m.name == "total_revenue")
    assert revenue_metric.agg == "sum"
    assert revenue_metric.label == "Total Revenue"


def test_import_superset_virtual_dataset():
    """Test that Superset virtual datasets (SQL-based) are imported."""
    adapter = SupersetAdapter()
    graph = adapter.parse("tests/fixtures/superset/sales_summary.yaml")

    sales = graph.models["sales_summary"]

    # Verify it has SQL (virtual dataset)
    assert sales.sql is not None
    assert "SELECT" in sales.sql
    assert sales.table is None  # Virtual datasets don't have table

    # Verify derived metric without aggregation type
    revenue_per_order = next(m for m in sales.metrics if m.name == "revenue_per_order")
    assert revenue_per_order.type == "derived"
    assert revenue_per_order.agg is None


# =============================================================================
# ROUNDTRIP TESTS
# =============================================================================


def test_superset_to_sidemantic_to_superset_roundtrip():
    """Test roundtrip: Superset -> Sidemantic -> Superset."""
    adapter = SupersetAdapter()

    # Import original
    graph1 = adapter.parse("tests/fixtures/superset/orders.yaml")

    # Export
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir)
        adapter.export(graph1, output_path)

        # Import exported version
        graph2 = adapter.parse(output_path / "orders.yaml")

        # Verify semantic equivalence
        # NOTE: Superset doesn't have native relationships or segments
        assert_graph_equivalent(graph1, graph2, check_relationships=False, check_segments=False)


def test_superset_roundtrip_dimension_properties():
    """Test that dimension properties survive Superset roundtrip."""
    adapter = SupersetAdapter()
    graph1 = adapter.parse("tests/fixtures/superset/orders.yaml")
    orders1 = graph1.models["orders"]

    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir)
        adapter.export(graph1, output_path)
        graph2 = adapter.parse(output_path / "orders.yaml")
        orders2 = graph2.models["orders"]

        for dim1 in orders1.dimensions:
            dim2 = orders2.get_dimension(dim1.name)
            assert dim2 is not None, f"Dimension {dim1.name} missing after roundtrip"
            assert_dimension_equivalent(dim1, dim2)


def test_superset_roundtrip_metric_properties():
    """Test that metric properties survive Superset roundtrip."""
    adapter = SupersetAdapter()
    graph1 = adapter.parse("tests/fixtures/superset/orders.yaml")
    orders1 = graph1.models["orders"]

    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir)
        adapter.export(graph1, output_path)
        graph2 = adapter.parse(output_path / "orders.yaml")
        orders2 = graph2.models["orders"]

        for m1 in orders1.metrics:
            m2 = orders2.get_metric(m1.name)
            assert m2 is not None, f"Metric {m1.name} missing after roundtrip"
            assert_metric_equivalent(m1, m2)


# =============================================================================
# CROSS-FORMAT CONVERSION TESTS
# =============================================================================


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
