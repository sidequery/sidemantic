"""Tests for chart generation (skipped if altair not installed)."""

import pytest


def test_chart_generation_available():
    """Test that chart generation module can be imported."""
    try:
        from sidemantic import charts

        assert charts is not None
    except ImportError:
        pytest.skip("altair not installed - chart generation unavailable")


def test_format_label():
    """Test label formatting logic."""
    try:
        from sidemantic.charts import _format_label

        assert _format_label("order_count") == "Order Count"
        assert _format_label("total_revenue") == "Total Revenue"
        assert _format_label("created_at__month") == "Created At (Month)"
        assert _format_label("orders.revenue") == "Revenue"
    except ImportError:
        pytest.skip("altair not installed")


def test_color_palette():
    """Test that color palette is well-defined."""
    try:
        from sidemantic.charts import COLORS

        assert "primary" in COLORS
        assert "categorical" in COLORS
        assert len(COLORS["categorical"]) >= 8  # Need enough colors for variety
        assert all(c.startswith("#") for c in COLORS["categorical"])  # Valid hex colors
    except ImportError:
        pytest.skip("altair not installed")


def test_missing_chart_dependencies_hint_names_charts_extra(monkeypatch):
    from sidemantic import charts

    monkeypatch.setattr(charts, "alt", None)
    monkeypatch.setattr(charts, "vl_convert", None)

    with pytest.raises(ImportError) as exc_info:
        charts.check_altair_available()

    assert "sidemantic[charts]" in str(exc_info.value)
    assert "optional serve" not in str(exc_info.value)


@pytest.mark.parametrize("metric", ["total_revenue", "index"])
def test_single_metric_chart_has_real_axis_without_mutating_records(metric):
    pytest.importorskip("altair")
    pytest.importorskip("vl_convert")
    from sidemantic.charts import create_chart

    data = [{metric: 450}, {metric: 600}]
    spec = create_chart(data).to_dict()
    x = spec["encoding"]["x"]["field"]
    assert x != metric
    assert [row[x] for row in spec["data"]["values"]] == [0, 1]
    assert [row[metric] for row in spec["data"]["values"]] == [450, 600]
    assert data == [{metric: 450}, {metric: 600}]


def test_y_only_chart_keeps_requested_metric():
    pytest.importorskip("altair")
    pytest.importorskip("vl_convert")
    from sidemantic.charts import create_chart

    spec = create_chart([{"category": "a", "revenue": 10, "cost": 3}], y="cost").to_dict()
    assert spec["encoding"]["y"]["field"] == "cost"
    assert spec["encoding"]["x"]["field"] == "category"
    assert "transform" not in spec


def test_x_only_chart_keeps_requested_dimension():
    pytest.importorskip("altair")
    pytest.importorskip("vl_convert")
    from sidemantic.charts import create_chart

    spec = create_chart([{"region": "a", "month": "Jan", "revenue": 10}], x="month").to_dict()
    assert spec["encoding"]["x"]["field"] == "month"
    assert spec["encoding"]["y"]["field"] == "revenue"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
