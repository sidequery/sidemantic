"""Tests for Cube adapter - roundtrip."""

import tempfile
from pathlib import Path

import yaml

from sidemantic.adapters.cube import CubeAdapter
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph

from ..helpers import (
    assert_dimension_equivalent,
    assert_graph_equivalent,
    assert_metric_equivalent,
    assert_segment_equivalent,
)


def test_cube_to_sidemantic_to_cube_roundtrip():
    """Test that Cube -> Sidemantic -> Cube preserves structure."""
    cube_adapter = CubeAdapter()
    graph1 = cube_adapter.parse("tests/fixtures/cube/orders.yml")

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        cube_adapter.export(graph1, temp_path)
        graph2 = cube_adapter.parse(temp_path)

        # NOTE: check_relationships=False because Cube exporter doesn't export joins yet
        # TODO: Fix CubeAdapter.export() to include joins section
        assert_graph_equivalent(graph1, graph2, check_relationships=False)

    finally:
        temp_path.unlink(missing_ok=True)


def test_cube_roundtrip_dimension_properties():
    """Test that dimension properties survive Cube roundtrip."""
    adapter = CubeAdapter()
    graph1 = adapter.parse("tests/fixtures/cube/orders.yml")
    orders1 = graph1.models["orders"]

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        adapter.export(graph1, temp_path)
        graph2 = adapter.parse(temp_path)
        orders2 = graph2.models["orders"]

        for dim1 in orders1.dimensions:
            dim2 = orders2.get_dimension(dim1.name)
            assert dim2 is not None, f"Dimension {dim1.name} missing after roundtrip"
            assert_dimension_equivalent(dim1, dim2)

    finally:
        temp_path.unlink(missing_ok=True)


def test_cube_roundtrip_metric_properties():
    """Test that metric properties survive Cube roundtrip."""
    adapter = CubeAdapter()
    graph1 = adapter.parse("tests/fixtures/cube/orders.yml")
    orders1 = graph1.models["orders"]

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        adapter.export(graph1, temp_path)
        graph2 = adapter.parse(temp_path)
        orders2 = graph2.models["orders"]

        for m1 in orders1.metrics:
            m2 = orders2.get_metric(m1.name)
            assert m2 is not None, f"Metric {m1.name} missing after roundtrip"
            assert_metric_equivalent(m1, m2)

    finally:
        temp_path.unlink(missing_ok=True)


def test_cube_roundtrip_segment_properties():
    """Test that segment properties survive Cube roundtrip."""
    adapter = CubeAdapter()
    graph1 = adapter.parse("tests/fixtures/cube/orders.yml")
    orders1 = graph1.models["orders"]

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        adapter.export(graph1, temp_path)
        graph2 = adapter.parse(temp_path)
        orders2 = graph2.models["orders"]

        for seg1 in orders1.segments:
            seg2 = orders2.get_segment(seg1.name)
            assert seg2 is not None, f"Segment {seg1.name} missing after roundtrip"
            assert_segment_equivalent(seg1, seg2)

    finally:
        temp_path.unlink(missing_ok=True)


def test_cube_export_primary_key_on_existing_dimension():
    """Test that primary key is marked on existing dimensions when exported.

    Cube requires primary_key: true on dimensions when joins are defined.
    This test ensures that when a model has a primary_key that matches
    an existing dimension, that dimension gets primary_key: true in export.
    """
    # Create a graph with models that have joins
    graph = SemanticGraph()

    customers = Model(
        name="customers",
        table="customers",
        primary_key="customer_id",
        dimensions=[
            Dimension(name="customer_id", type="numeric", sql="customer_id"),
            Dimension(name="name", type="categorical", sql="name"),
        ],
        metrics=[Metric(name="customer_count", agg="count", sql="customer_id")],
    )

    orders = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        relationships=[
            Relationship(name="customers", type="many_to_one", foreign_key="customer_id"),
        ],
        dimensions=[
            Dimension(name="order_id", type="numeric", sql="order_id"),
            Dimension(name="customer_id", type="numeric", sql="customer_id"),
        ],
        metrics=[Metric(name="total_orders", agg="count", sql="order_id")],
    )

    graph.add_model(customers)
    graph.add_model(orders)

    # Export to Cube format
    adapter = CubeAdapter()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        adapter.export(graph, temp_path)

        # Read the exported YAML and verify primary keys
        with open(temp_path) as f:
            exported = yaml.safe_load(f)

        cubes_by_name = {c["name"]: c for c in exported["cubes"]}

        # Check customers cube has primary_key on customer_id dimension
        customers_cube = cubes_by_name["customers"]
        customer_id_dim = next(d for d in customers_cube["dimensions"] if d["name"] == "customer_id")
        assert customer_id_dim.get("primary_key") is True, "customer_id should have primary_key: true"

        # Check orders cube has primary_key on order_id dimension
        orders_cube = cubes_by_name["orders"]
        order_id_dim = next(d for d in orders_cube["dimensions"] if d["name"] == "order_id")
        assert order_id_dim.get("primary_key") is True, "order_id should have primary_key: true"

        # Verify joins are exported
        assert "joins" in orders_cube, "orders cube should have joins"
        assert len(orders_cube["joins"]) == 1
        assert orders_cube["joins"][0]["name"] == "customers"

    finally:
        temp_path.unlink(missing_ok=True)
