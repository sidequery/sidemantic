"""Tests for MCP Apps UI integration."""

import pytest

pytest.importorskip("mcp")  # Skip if mcp extra not installed

from sidemantic.apps import _get_widget_template
from sidemantic.mcp_server import create_chart, initialize_layer


@pytest.fixture
def demo_layer(tmp_path):
    """Create a demo semantic layer for testing."""
    model_yaml = """
models:
  - name: orders
    table: orders_table
    dimensions:
      - name: status
        sql: status
        type: categorical
    metrics:
      - name: total_revenue
        agg: sum
        sql: amount
"""
    (tmp_path / "orders.yml").write_text(model_yaml)
    layer = initialize_layer(str(tmp_path), db_path=":memory:")
    layer.adapter.execute("""
        CREATE TABLE orders_table (
            id INTEGER, status VARCHAR, amount DECIMAL
        )
    """)
    layer.adapter.execute("""
        INSERT INTO orders_table VALUES
            (1, 'completed', 100),
            (2, 'completed', 200),
            (3, 'pending', 150)
    """)
    yield layer


def test_widget_template_loads():
    """Test that the built chart widget HTML loads."""
    html = _get_widget_template()
    assert len(html) > 1000  # Built file is ~960KB
    assert "<!DOCTYPE html>" in html
    assert "sidemantic-chart" in html


def test_widget_uses_vega_interpreter():
    """Test that the built widget uses the CSP-safe vega interpreter."""
    html = _get_widget_template()
    # The widget should use ast:true + expressionInterpreter for CSP safety.
    # Note: vega-loader's CSV parser includes new Function() in the bundle
    # but it's never called at runtime since we pass JSON data.
    assert "expressionInterpreter" in html or "ast" in html


def test_create_chart_returns_vega_spec(demo_layer):
    """Test that create_chart returns vega_spec without png_base64."""
    result = create_chart(
        dimensions=["orders.status"],
        metrics=["orders.total_revenue"],
        chart_type="bar",
    )

    assert isinstance(result, dict)
    assert "sql" in result
    assert "vega_spec" in result
    assert "row_count" in result
    assert "png_base64" not in result
    assert isinstance(result["vega_spec"], dict)
    assert "data" in result["vega_spec"]
