"""Tests for LookML adapter - roundtrip."""

import tempfile
from pathlib import Path

import pytest

from sidemantic.adapters.lookml import LookMLAdapter

from ..helpers import (
    assert_dimension_equivalent,
    assert_graph_equivalent,
    assert_metric_equivalent,
    assert_segment_equivalent,
)

# =============================================================================
# ROUNDTRIP TESTS
# =============================================================================


def test_lookml_to_sidemantic_to_lookml_roundtrip():
    """Test that LookML -> Sidemantic -> LookML preserves structure."""
    # Import from LookML
    lookml_adapter = LookMLAdapter()
    graph1 = lookml_adapter.parse("tests/fixtures/lookml/orders.lkml")

    # Export back to LookML
    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        lookml_adapter.export(graph1, temp_path)

        # Re-import and verify
        graph2 = lookml_adapter.parse(temp_path)

        # Verify semantic equivalence
        # NOTE: LookML relationships come from explore files, not view files
        assert_graph_equivalent(graph1, graph2, check_relationships=False)

    finally:
        temp_path.unlink(missing_ok=True)


def test_roundtrip_real_lookml_example():
    """Test LookML example roundtrip using the actual example file."""
    adapter = LookMLAdapter()

    # Import original
    graph1 = adapter.parse("tests/fixtures/lookml/orders.lkml")

    # Export
    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        adapter.export(graph1, temp_path)

        # Import exported version
        graph2 = adapter.parse(temp_path)

        # Verify semantic equivalence
        assert_graph_equivalent(graph1, graph2, check_relationships=False)

    finally:
        temp_path.unlink(missing_ok=True)


def test_lookml_roundtrip_dimension_properties():
    """Test that dimension properties survive LookML roundtrip."""
    adapter = LookMLAdapter()
    graph1 = adapter.parse("tests/fixtures/lookml/orders.lkml")

    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        adapter.export(graph1, temp_path)
        graph2 = adapter.parse(temp_path)

        for model_name, model1 in graph1.models.items():
            model2 = graph2.models[model_name]
            for dim1 in model1.dimensions:
                dim2 = model2.get_dimension(dim1.name)
                assert dim2 is not None, f"Dimension {model_name}.{dim1.name} missing after roundtrip"
                assert_dimension_equivalent(dim1, dim2)

    finally:
        temp_path.unlink(missing_ok=True)


def test_lookml_roundtrip_metric_properties():
    """Test that metric properties survive LookML roundtrip."""
    adapter = LookMLAdapter()
    graph1 = adapter.parse("tests/fixtures/lookml/orders.lkml")

    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        adapter.export(graph1, temp_path)
        graph2 = adapter.parse(temp_path)

        for model_name, model1 in graph1.models.items():
            model2 = graph2.models[model_name]
            for m1 in model1.metrics:
                m2 = model2.get_metric(m1.name)
                assert m2 is not None, f"Metric {model_name}.{m1.name} missing after roundtrip"
                assert_metric_equivalent(m1, m2)

    finally:
        temp_path.unlink(missing_ok=True)


def test_lookml_roundtrip_segment_properties():
    """Test that segment properties survive LookML roundtrip."""
    adapter = LookMLAdapter()
    graph1 = adapter.parse("tests/fixtures/lookml/orders.lkml")

    with tempfile.NamedTemporaryFile(mode="w", suffix=".lkml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        adapter.export(graph1, temp_path)
        graph2 = adapter.parse(temp_path)

        for model_name, model1 in graph1.models.items():
            model2 = graph2.models[model_name]
            for seg1 in model1.segments:
                seg2 = model2.get_segment(seg1.name)
                assert seg2 is not None, f"Segment {model_name}.{seg1.name} missing after roundtrip"
                assert_segment_equivalent(seg1, seg2)

    finally:
        temp_path.unlink(missing_ok=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
