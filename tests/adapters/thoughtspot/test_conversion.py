"""Tests for ThoughtSpot adapter - cross-format conversion."""

import tempfile
from pathlib import Path

from sidemantic.adapters.sidemantic import SidemanticAdapter
from sidemantic.adapters.thoughtspot import ThoughtSpotAdapter


def test_thoughtspot_to_sidemantic_conversion():
    """Test converting ThoughtSpot TML to Sidemantic YAML."""
    ts_adapter = ThoughtSpotAdapter()
    sidemantic_adapter = SidemanticAdapter()

    graph = ts_adapter.parse("tests/fixtures/thoughtspot/orders.table.tml")

    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "orders.yml"
        sidemantic_adapter.export(graph, output_path)

        assert output_path.exists()
        converted = sidemantic_adapter.parse(output_path)
        assert "orders" in converted.models
