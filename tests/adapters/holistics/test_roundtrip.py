"""Tests for Holistics AML adapter - roundtrip."""

import tempfile

import pytest

from sidemantic.adapters.holistics import HolisticsAdapter

from ..helpers import (
    assert_dimension_equivalent,
    assert_graph_equivalent,
    assert_metric_equivalent,
)


def test_holistics_roundtrip_directory():
    """Test Holistics AML roundtrip preserves structure."""
    adapter = HolisticsAdapter()
    graph1 = adapter.parse("tests/fixtures/holistics")

    with tempfile.TemporaryDirectory() as tmpdir:
        adapter.export(graph1, tmpdir)
        graph2 = adapter.parse(tmpdir)

        assert_graph_equivalent(graph1, graph2, check_segments=False)


def test_holistics_roundtrip_dimension_properties():
    """Test that dimension properties survive Holistics roundtrip."""
    adapter = HolisticsAdapter()
    graph1 = adapter.parse("tests/fixtures/holistics")
    users1 = graph1.models["users"]

    with tempfile.TemporaryDirectory() as tmpdir:
        adapter.export(graph1, tmpdir)
        graph2 = adapter.parse(tmpdir)
        users2 = graph2.models["users"]

        for dim1 in users1.dimensions:
            dim2 = users2.get_dimension(dim1.name)
            assert dim2 is not None, f"Dimension {dim1.name} missing after roundtrip"
            assert_dimension_equivalent(dim1, dim2)


def test_holistics_roundtrip_metric_properties():
    """Test that metric properties survive Holistics roundtrip."""
    adapter = HolisticsAdapter()
    graph1 = adapter.parse("tests/fixtures/holistics")
    orders1 = graph1.models["orders"]

    with tempfile.TemporaryDirectory() as tmpdir:
        adapter.export(graph1, tmpdir)
        graph2 = adapter.parse(tmpdir)
        orders2 = graph2.models["orders"]

        for metric1 in orders1.metrics:
            metric2 = orders2.get_metric(metric1.name)
            assert metric2 is not None, f"Metric {metric1.name} missing after roundtrip"
            assert_metric_equivalent(metric1, metric2)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
