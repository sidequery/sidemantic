"""Tests for ThoughtSpot adapter roundtrip (parse -> export -> parse)."""

from pathlib import Path

from sidemantic.adapters.thoughtspot import ThoughtSpotAdapter


def test_thoughtspot_roundtrip_table(tmp_path: Path):
    """Test roundtrip of a ThoughtSpot table TML file."""
    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/orders.table.tml")

    output_file = tmp_path / "roundtrip.tml"
    adapter.export(graph, output_file)

    graph2 = adapter.parse(output_file)
    assert "orders" in graph2.models

    model = graph2.models["orders"]
    amount = model.get_metric("amount")
    assert amount is not None
    assert amount.agg == "sum"

    order_date = model.get_dimension("order_date")
    assert order_date is not None
    assert order_date.type == "time"


def test_thoughtspot_export_worksheet(tmp_path: Path):
    """Test exporting a ThoughtSpot worksheet TML file."""
    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/sales.worksheet.tml")

    output_file = tmp_path / "worksheet.tml"
    adapter.export(graph, output_file)

    data = output_file.read_text()
    assert "worksheet:" in data
    assert "joins:" in data


def test_thoughtspot_roundtrip_worksheet(tmp_path: Path):
    """Test roundtrip of worksheet export preserves joins and formulas."""
    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/worksheet_multi_join.worksheet.tml")

    output_file = tmp_path / "worksheet_roundtrip.tml"
    adapter.export(graph, output_file)

    graph2 = adapter.parse(output_file)
    assert "sales_multi_join" in graph2.models
    model = graph2.models["sales_multi_join"]
    assert model.sql is not None

    net_revenue = model.get_metric("net_revenue")
    assert net_revenue is not None
    assert "gross_revenue" in (net_revenue.sql or "")
