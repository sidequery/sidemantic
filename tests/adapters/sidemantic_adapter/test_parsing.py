"""Test Sidemantic native YAML adapter parsing and export."""

import tempfile
from pathlib import Path

import pytest

from sidemantic.adapters.sidemantic import SidemanticAdapter
from sidemantic.core.semantic_layer import SemanticLayer


def test_parse_native_yaml():
    """Test parsing native Sidemantic YAML."""
    adapter = SidemanticAdapter()
    graph = adapter.parse("tests/fixtures/sidemantic/orders.yml")

    # Verify models
    assert len(graph.models) == 2
    assert "orders" in graph.models
    assert "customers" in graph.models

    # Verify orders model
    orders = graph.models["orders"]
    assert orders.table == "public.orders"
    # Note: Example YAML still uses old format, will be updated in task #10
    assert len(orders.dimensions) == 3
    assert len(orders.metrics) == 3

    # Verify metrics
    assert len(graph.metrics) == 3
    assert "total_revenue" in graph.metrics
    assert "conversion_rate" in graph.metrics
    assert "revenue_per_order" in graph.metrics

    # Verify metric types
    assert graph.metrics["total_revenue"].type is None  # Untyped (was simple)
    assert graph.metrics["conversion_rate"].type == "ratio"
    assert graph.metrics["revenue_per_order"].type == "derived"


def test_export_native_yaml():
    """Test exporting to native Sidemantic YAML."""
    # Load example
    adapter = SidemanticAdapter()
    graph = adapter.parse("tests/fixtures/sidemantic/orders.yml")

    # Export to temp file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        adapter.export(graph, temp_path)

        # Verify file exists and is readable
        assert temp_path.exists()

        # Re-parse exported file
        graph2 = adapter.parse(temp_path)

        # Verify round-trip preserves structure
        assert len(graph2.models) == len(graph.models)
        assert len(graph2.metrics) == len(graph.metrics)
        assert set(graph2.models.keys()) == set(graph.models.keys())
        assert set(graph2.metrics.keys()) == set(graph.metrics.keys())

    finally:
        temp_path.unlink(missing_ok=True)


def test_semantic_layer_from_yaml():
    """Test SemanticLayer.from_yaml() convenience method."""
    sl = SemanticLayer.from_yaml("tests/fixtures/sidemantic/orders.yml")

    assert sl.list_models() == ["orders", "customers"]
    assert set(sl.list_metrics()) == {"total_revenue", "conversion_rate", "revenue_per_order"}


def test_semantic_layer_to_yaml():
    """Test SemanticLayer.to_yaml() convenience method."""
    sl = SemanticLayer.from_yaml("tests/fixtures/sidemantic/orders.yml")

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        sl.to_yaml(temp_path)
        assert temp_path.exists()

        # Verify round-trip
        sl2 = SemanticLayer.from_yaml(temp_path)
        assert sl2.list_models() == sl.list_models()
        assert set(sl2.list_metrics()) == set(sl.list_metrics())

    finally:
        temp_path.unlink(missing_ok=True)


def test_adapter_validation():
    """Test adapter validation catches errors."""
    adapter = SidemanticAdapter()
    graph = adapter.parse("tests/fixtures/sidemantic/orders.yml")

    # Should have no errors
    errors = adapter.validate(graph)
    assert len(errors) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
