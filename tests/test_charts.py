"""Behavior tests for the optional Altair chart surface."""

import base64

import pytest

from sidemantic import charts


def test_chart_generation_available():
    assert charts.alt is not None
    assert charts.vl_convert is not None


def test_format_label():
    assert charts._format_label("order_count") == "Order Count"
    assert charts._format_label("total_revenue") == "Total Revenue"
    assert charts._format_label("created_at__month") == "Created At (Month)"
    assert charts._format_label("orders.revenue") == "Revenue"


def test_color_palette():
    assert "primary" in charts.COLORS
    assert "categorical" in charts.COLORS
    assert len(charts.COLORS["categorical"]) >= 8
    assert all(color.startswith("#") for color in charts.COLORS["categorical"])


def test_create_chart_auto_selects_temporal_area_with_readable_axes():
    chart = charts.create_chart(
        [
            {"created_at__month": "2025-01-01", "total_revenue": 120.0},
            {"created_at__month": "2025-02-01", "total_revenue": 180.0},
        ],
        title="Monthly revenue",
        width=480,
        height=240,
    )

    spec = charts.chart_to_vega(chart)

    assert spec["mark"]["type"] == "area"
    assert spec["encoding"]["x"]["type"] == "temporal"
    assert spec["encoding"]["x"]["title"] == "Created At (Month)"
    assert spec["encoding"]["y"]["title"] == "Total Revenue"
    assert spec["title"] == "Monthly revenue"
    assert spec["width"] == 480
    assert spec["height"] == 240


def test_create_chart_folds_multiple_metrics_into_series():
    chart = charts.create_chart(
        [
            {"month": "2025-01", "revenue": 120.0, "orders": 3},
            {"month": "2025-02", "revenue": 180.0, "orders": 5},
        ],
        x="month",
        y=["revenue", "orders"],
        chart_type="line",
    )

    spec = chart.to_dict()

    assert spec["mark"]["type"] == "line"
    assert spec["transform"] == [{"fold": ["revenue", "orders"], "as": ["metric", "value"]}]
    assert spec["encoding"]["color"]["field"] == "metric"
    assert spec["encoding"]["y"]["field"] == "value"


def test_create_chart_auto_selects_scatter_for_numeric_x():
    chart = charts.create_chart([{"spend": 10.0, "revenue": 25.0}], x="spend", y="revenue")

    spec = chart.to_dict()

    assert spec["mark"]["type"] == "circle"
    assert spec["encoding"]["x"]["type"] == "quantitative"
    assert spec["encoding"]["y"]["type"] == "quantitative"


def test_chart_png_exports_are_real_png_payloads():
    chart = charts.create_chart([{"region": "west", "revenue": 42.0}], x="region", y="revenue")

    png = charts.chart_to_png(chart)
    data_url = charts.chart_to_base64_png(chart)

    assert png.startswith(b"\x89PNG\r\n\x1a\n")
    assert data_url.startswith("data:image/png;base64,")
    assert base64.b64decode(data_url.split(",", 1)[1]) == png


def test_create_chart_rejects_empty_data():
    with pytest.raises(ValueError, match="No data provided"):
        charts.create_chart([])


def test_missing_chart_dependencies_hint_names_charts_extra(monkeypatch):
    monkeypatch.setattr(charts, "alt", None)
    monkeypatch.setattr(charts, "vl_convert", None)

    with pytest.raises(ImportError) as exc_info:
        charts.check_altair_available()

    assert "sidemantic[charts]" in str(exc_info.value)
    assert "optional serve" not in str(exc_info.value)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
