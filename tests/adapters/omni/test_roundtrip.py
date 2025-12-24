"""Tests for Omni adapter - roundtrip."""

import tempfile
from pathlib import Path

import pytest

from sidemantic.adapters.omni import OmniAdapter

from ..helpers import (
    assert_dimension_equivalent,
    assert_graph_equivalent,
    assert_metric_equivalent,
)


def test_omni_to_sidemantic_to_omni_roundtrip():
    """Test roundtrip: Omni -> Sidemantic -> Omni."""
    adapter = OmniAdapter()

    # Import original
    graph1 = adapter.parse("tests/fixtures/omni/")

    # Export
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir)
        adapter.export(graph1, output_path)

        # Import exported version
        graph2 = adapter.parse(output_path)

        # Verify semantic equivalence
        # NOTE: Omni doesn't have native segments
        assert_graph_equivalent(graph1, graph2, check_segments=False)


def test_omni_roundtrip_dimension_properties():
    """Test that dimension properties survive Omni roundtrip."""
    adapter = OmniAdapter()
    graph1 = adapter.parse("tests/fixtures/omni/")
    orders1 = graph1.models["orders"]

    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir)
        adapter.export(graph1, output_path)
        graph2 = adapter.parse(output_path)
        orders2 = graph2.models["orders"]

        for dim1 in orders1.dimensions:
            dim2 = orders2.get_dimension(dim1.name)
            assert dim2 is not None, f"Dimension {dim1.name} missing after roundtrip"
            assert_dimension_equivalent(dim1, dim2)


def test_omni_roundtrip_metric_properties():
    """Test that metric properties survive Omni roundtrip."""
    adapter = OmniAdapter()
    graph1 = adapter.parse("tests/fixtures/omni/")
    orders1 = graph1.models["orders"]

    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir)
        adapter.export(graph1, output_path)
        graph2 = adapter.parse(output_path)
        orders2 = graph2.models["orders"]

        for m1 in orders1.metrics:
            m2 = orders2.get_metric(m1.name)
            assert m2 is not None, f"Metric {m1.name} missing after roundtrip"
            assert_metric_equivalent(m1, m2)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
