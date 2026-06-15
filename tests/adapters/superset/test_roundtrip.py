"""Tests for Superset adapter - roundtrip."""

import tempfile
from pathlib import Path

import pytest
import yaml

from sidemantic.adapters.superset import SupersetAdapter

from ..helpers import (
    assert_dimension_equivalent,
    assert_graph_equivalent,
    assert_metric_equivalent,
)

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


@pytest.mark.parametrize(
    "dataset_extra, expected_table",
    [
        ({"catalog": "warehouse", "schema": None}, "warehouse.events"),
        ({"catalog": "warehouse", "schema": "public"}, "warehouse.public.events"),
        ({"schema": "public"}, "public.events"),
        ({}, "events"),
    ],
)
def test_superset_catalog_schema_roundtrip(dataset_extra, expected_table):
    """Test catalog/schema qualifiers survive a Superset roundtrip.

    A catalog-only (schema null) reference must not be re-emitted with the
    catalog duplicated into ``schema`` (regression: ``cat.table`` -> ``cat.cat.table``).
    """
    dataset = {
        "table_name": "events",
        "columns": [{"column_name": "id", "type": "INT"}],
        **dataset_extra,
    }

    adapter = SupersetAdapter()

    with tempfile.TemporaryDirectory() as tmpdir:
        input_path = Path(tmpdir) / "events.yaml"
        input_path.write_text(yaml.dump(dataset))

        graph1 = adapter.parse(input_path)
        assert graph1.models["events"].table == expected_table

        out_dir = Path(tmpdir) / "out"
        adapter.export(graph1, out_dir)
        graph2 = adapter.parse(out_dir / "events.yaml")

        assert graph2.models["events"].table == expected_table


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
