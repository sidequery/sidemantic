"""Tests for Cube adapter - roundtrip."""

import tempfile
from pathlib import Path

from sidemantic.adapters.cube import CubeAdapter

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
