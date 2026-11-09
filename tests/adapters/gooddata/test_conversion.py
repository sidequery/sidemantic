"""Tests for GoodData adapter - cross-format conversion."""

import tempfile
from pathlib import Path

import pytest

from sidemantic.adapters.cube import CubeAdapter
from sidemantic.adapters.gooddata import GoodDataAdapter


def test_gooddata_to_cube_conversion():
    """Test converting GoodData LDM to Cube format."""
    gooddata_adapter = GoodDataAdapter()
    cube_adapter = CubeAdapter()

    graph = gooddata_adapter.parse("tests/fixtures/gooddata/cloud_ldm.json")

    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "models.yml"
        cube_adapter.export(graph, output_path)

        assert output_path.exists()

        cube_graph = cube_adapter.parse(output_path)
        assert "orders" in cube_graph.models
        assert "customers" in cube_graph.models


def test_cube_to_gooddata_conversion():
    """Test converting Cube schema to GoodData LDM JSON."""
    cube_adapter = CubeAdapter()
    gooddata_adapter = GoodDataAdapter()

    graph = cube_adapter.parse("tests/fixtures/cube/orders.yml")

    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir)
        # An LDM fact is a physical column that imports as SUM. Counts, averages,
        # filtered measures and ratios need MAQL metrics, so the export names
        # them instead of writing facts that would import as different metrics.
        with pytest.raises(ValueError) as rejected:
            gooddata_adapter.export(graph, output_path)
        for metric in ("orders.count", "orders.completed_revenue", "orders.avg_order_value", "orders.conversion_rate"):
            assert metric in str(rejected.value)
        assert "orders.revenue" not in str(rejected.value).replace("orders.completed_revenue", "")
        assert not (output_path / "ldm.json").exists()

        orders = graph.models["orders"]
        orders.metrics = [metric for metric in orders.metrics if metric.name == "revenue"]
        gooddata_adapter.export(graph, output_path)

        ldm_path = output_path / "ldm.json"
        assert ldm_path.exists()

        gd_graph = gooddata_adapter.parse(ldm_path)
        revenue = gd_graph.models["orders"].get_metric("revenue")
        assert (revenue.agg, revenue.sql) == ("sum", "amount")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
