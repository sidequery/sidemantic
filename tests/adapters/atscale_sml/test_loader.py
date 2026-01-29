"""Tests for AtScale SML loader auto-detection."""

from pathlib import Path

import pytest

from sidemantic import SemanticLayer
from sidemantic.loaders import load_from_directory


@pytest.fixture
def fixtures_dir():
    return Path(__file__).parent.parent.parent / "fixtures" / "atscale_sml"


@pytest.fixture
def kitchen_sink_dir():
    return Path(__file__).parent.parent.parent / "fixtures" / "atscale_sml_kitchen_sink"


def test_load_from_directory_detects_sml(fixtures_dir):
    layer = SemanticLayer()
    load_from_directory(layer, fixtures_dir)

    assert "sales_model" in layer.graph.models
    assert "dim_customers" in layer.graph.models

    fact_sales = layer.graph.models["sales_model"]
    assert fact_sales.get_metric("total_sales") is not None


def test_load_from_directory_detects_sml_with_atscale_catalog(kitchen_sink_dir):
    layer = SemanticLayer()
    load_from_directory(layer, kitchen_sink_dir)

    assert "order_model" in layer.graph.models
    assert "return_model" in layer.graph.models


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
