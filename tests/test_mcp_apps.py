"""Tests for MCP Apps (mcp-ui-server) integration."""

import pytest

pytest.importorskip("mcp_ui_server")  # Skip if apps extra not installed
pytest.importorskip("mcp")

from sidemantic.apps import build_chart_html, create_chart_resource
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


def test_build_chart_html():
    """Test that build_chart_html embeds the Vega spec."""
    spec = {"$schema": "https://vega.github.io/schema/vega-lite/v5.json", "mark": "bar"}
    html = build_chart_html(spec)

    assert "{{VEGA_SPEC}}" not in html
    assert '"$schema"' in html
    assert '"mark"' in html
    assert "vega-embed" in html


def test_build_chart_html_escapes_json():
    """Test that JSON with special chars is properly embedded."""
    spec = {"title": "Revenue <&> Costs", "description": 'Test\'s "spec"'}
    html = build_chart_html(spec)

    # < in the JSON data should be escaped to \u003c
    assert "\\u003c" in html
    # The raw < from user input should not appear in the JSON block
    assert "Revenue <&>" not in html
    assert "Revenue \\u003c&>" in html


def test_build_chart_html_prevents_script_injection():
    """Test that </script> in user input cannot break out of the JSON block."""
    spec = {"title": '</script><script>alert("xss")</script>'}
    html = build_chart_html(spec)

    assert "</script><script>" not in html
    assert "\\u003c/script>" in html


def test_create_chart_resource():
    """Test that create_chart_resource returns a valid UIResource."""
    spec = {"mark": "bar", "data": {"values": [{"x": 1, "y": 2}]}}
    resource = create_chart_resource(spec)

    dumped = resource.model_dump()
    assert dumped["type"] == "resource"
    assert str(dumped["resource"]["uri"]) == "ui://sidemantic/chart"
    assert dumped["resource"]["mimeType"] == "text/html;profile=mcp-app"
    assert "text" in dumped["resource"]
    assert "vega-embed" in dumped["resource"]["text"]


def test_create_chart_with_apps_enabled(demo_layer):
    """Test that create_chart includes UIResource when apps mode is on."""
    import sidemantic.mcp_server as _mcp_mod

    original = _mcp_mod._apps_enabled
    try:
        _mcp_mod._apps_enabled = True
        result = create_chart(
            dimensions=["orders.status"],
            metrics=["orders.total_revenue"],
            chart_type="bar",
        )

        # Should return a list with [dict, UIResource]
        assert isinstance(result, list)
        assert len(result) == 2

        data = result[0]
        assert "sql" in data
        assert "vega_spec" in data
        assert "png_base64" in data

        ui_resource = result[1]
        dumped = ui_resource.model_dump()
        assert dumped["type"] == "resource"
        assert str(dumped["resource"]["uri"]) == "ui://sidemantic/chart"
    finally:
        _mcp_mod._apps_enabled = original


def test_create_chart_without_apps_returns_dict(demo_layer):
    """Test that create_chart returns a plain dict when apps mode is off."""
    import sidemantic.mcp_server as _mcp_mod

    original = _mcp_mod._apps_enabled
    try:
        _mcp_mod._apps_enabled = False
        result = create_chart(
            dimensions=["orders.status"],
            metrics=["orders.total_revenue"],
            chart_type="bar",
        )

        assert isinstance(result, dict)
        assert "sql" in result
        assert "vega_spec" in result
    finally:
        _mcp_mod._apps_enabled = original
