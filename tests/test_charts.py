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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
