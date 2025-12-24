"""Tests for Hex adapter - roundtrip."""

import tempfile
from pathlib import Path

import pytest

from sidemantic.adapters.hex import HexAdapter

from ..helpers import (
    assert_dimension_equivalent,
    assert_graph_equivalent,
    assert_metric_equivalent,
)


def test_hex_to_sidemantic_to_hex_roundtrip():
    """Test that Hex -> Sidemantic -> Hex preserves structure."""
    # Import from Hex
    hex_adapter = HexAdapter()
    graph1 = hex_adapter.parse("tests/fixtures/hex/orders.yml")

    # Export back to Hex
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        hex_adapter.export(graph1, temp_path)

        # Re-import and verify
        graph2 = hex_adapter.parse(temp_path)

        # Verify semantic equivalence
        # NOTE: Hex doesn't have native relationships or segments
        assert_graph_equivalent(graph1, graph2, check_relationships=False, check_segments=False)

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

        # Verify semantic equivalence
        assert_graph_equivalent(graph1, graph2, check_relationships=False, check_segments=False)

    finally:
        temp_path.unlink(missing_ok=True)


def test_hex_roundtrip_dimension_properties():
    """Test that dimension properties survive Hex roundtrip."""
    adapter = HexAdapter()
    graph1 = adapter.parse("tests/fixtures/hex/orders.yml")
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


def test_hex_roundtrip_metric_properties():
    """Test that metric properties survive Hex roundtrip."""
    adapter = HexAdapter()
    graph1 = adapter.parse("tests/fixtures/hex/orders.yml")
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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
