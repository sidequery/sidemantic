"""Tests for MetricFlow adapter - roundtrip."""

import tempfile
from pathlib import Path

import pytest

from sidemantic.adapters.metricflow import MetricFlowAdapter

from ..helpers import (
    assert_dimension_equivalent,
    assert_graph_equivalent,
    assert_metric_equivalent,
)

# =============================================================================
# ROUNDTRIP TESTS
# =============================================================================


def test_metricflow_to_sidemantic_to_metricflow_roundtrip():
    """Test that MetricFlow -> Sidemantic -> MetricFlow preserves structure."""
    # Import from MetricFlow
    mf_adapter = MetricFlowAdapter()
    graph = mf_adapter.parse("tests/fixtures/metricflow/semantic_models.yml")

    # Export back to MetricFlow
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        mf_adapter.export(graph, temp_path)

        # Re-import and verify
        graph2 = mf_adapter.parse(temp_path)

        # Verify models preserved
        assert "orders" in graph2.models
        assert "customers" in graph2.models

        # Verify metrics preserved
        # Note: Simple metrics that just reference measures may not round-trip
        # since they can be queried directly via the measure
        assert "average_order_value" in graph2.metrics

        # Verify metric types preserved
        avg_order = graph2.metrics["average_order_value"]
        assert avg_order.type == "ratio"

        # total_revenue is a simple metric, may not be preserved in export
        # (can be queried directly as orders.revenue)

    finally:
        temp_path.unlink(missing_ok=True)


def test_roundtrip_real_metricflow_example():
    """Test MetricFlow example roundtrip using the actual example file."""
    adapter = MetricFlowAdapter()

    # Import original
    graph1 = adapter.parse("tests/fixtures/metricflow/semantic_models.yml")

    # Export
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        adapter.export(graph1, temp_path)

        # Import exported version
        graph2 = adapter.parse(temp_path)

        # Verify semantic equivalence
        # NOTE: MetricFlow entities create relationships that may not fully round-trip
        # NOTE: MetricFlow uses ref() syntax which doesn't preserve schema prefixes
        assert_graph_equivalent(graph1, graph2, check_relationships=False, check_table_schema=False)

        # Verify graph-level metrics preserved
        # Note: Simple metrics may not round-trip, so we just check that ratio metrics are there
        ratio_metrics1 = {name for name, m in graph1.metrics.items() if m.type == "ratio"}
        ratio_metrics2 = {name for name, m in graph2.metrics.items() if m.type == "ratio"}
        assert ratio_metrics1 == ratio_metrics2

        # Verify ratio metric properties preserved
        for name in ratio_metrics1:
            m1 = graph1.metrics[name]
            m2 = graph2.metrics[name]
            assert m1.numerator == m2.numerator, f"Metric {name}: numerator mismatch"
            assert m1.denominator == m2.denominator, f"Metric {name}: denominator mismatch"

    finally:
        temp_path.unlink(missing_ok=True)


def test_metricflow_roundtrip_dimension_properties():
    """Test that dimension properties survive MetricFlow roundtrip."""
    adapter = MetricFlowAdapter()
    graph1 = adapter.parse("tests/fixtures/metricflow/semantic_models.yml")

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
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


def test_metricflow_roundtrip_metric_properties():
    """Test that metric properties survive MetricFlow roundtrip."""
    adapter = MetricFlowAdapter()
    graph1 = adapter.parse("tests/fixtures/metricflow/semantic_models.yml")

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
