"""Tests for Rill adapter roundtrip conversion."""

import tempfile
from pathlib import Path

from sidemantic.adapters.rill import RillAdapter
from tests.adapters.helpers import (
    assert_dimension_equivalent,
    assert_graph_equivalent,
    assert_metric_equivalent,
)


def test_rill_to_sidemantic_to_rill_roundtrip():
    """Test Rill roundtrip conversion."""
    adapter = RillAdapter()

    # Import original
    graph1 = adapter.parse("tests/fixtures/rill/orders.yaml")

    # Export
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir)
        adapter.export(graph1, output_path)

        # Import exported version
        graph2 = adapter.parse(output_path / "orders.yaml")

        # Verify semantic equivalence
        # NOTE: Rill doesn't have native relationships or segments
        assert_graph_equivalent(graph1, graph2, check_relationships=False, check_segments=False)


def test_roundtrip_real_rill_example():
    """Test Rill example roundtrip using actual example files."""
    adapter = RillAdapter()

    # Import original
    graph1 = adapter.parse("tests/fixtures/rill/orders.yaml")

    # Export
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir)
        adapter.export(graph1, output_path)

        # Import exported version
        graph2 = adapter.parse(output_path / "orders.yaml")

        # Verify semantic equivalence
        assert_graph_equivalent(graph1, graph2, check_relationships=False, check_segments=False)


def test_rill_roundtrip_dimension_properties():
    """Test that dimension properties survive Rill roundtrip."""
    adapter = RillAdapter()
    graph1 = adapter.parse("tests/fixtures/rill/orders.yaml")
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


def test_rill_roundtrip_metric_properties():
    """Test that metric properties survive Rill roundtrip."""
    adapter = RillAdapter()
    graph1 = adapter.parse("tests/fixtures/rill/orders.yaml")
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
