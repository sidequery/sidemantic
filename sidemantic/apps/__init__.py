"""MCP Apps integration for sidemantic.

Uses mcp-ui-server to create vendor-neutral UI resources that render
interactive charts in any MCP Apps-compatible host.
"""

import json
from pathlib import Path
from typing import Any

_WIDGET_TEMPLATE: str | None = None


def _get_widget_template() -> str:
    """Load the chart widget HTML template."""
    global _WIDGET_TEMPLATE
    if _WIDGET_TEMPLATE is None:
        path = Path(__file__).parent / "chart_widget.html"
        _WIDGET_TEMPLATE = path.read_text()
    return _WIDGET_TEMPLATE


def build_chart_html(vega_spec: dict[str, Any]) -> str:
    """Build a self-contained chart widget HTML with embedded Vega spec.

    Args:
        vega_spec: Vega-Lite specification dict.

    Returns:
        Complete HTML string with the spec injected.
    """
    template = _get_widget_template()
    return template.replace("{{VEGA_SPEC}}", json.dumps(vega_spec))


def create_chart_resource(vega_spec: dict[str, Any]):
    """Create a UIResource for a chart visualization.

    Args:
        vega_spec: Vega-Lite specification dict.

    Returns:
        UIResource (EmbeddedResource) for MCP Apps-compatible hosts.
    """
    from mcp_ui_server import create_ui_resource

    html = build_chart_html(vega_spec)
    return create_ui_resource(
        {
            "uri": "ui://sidemantic/chart",
            "content": {
                "type": "rawHtml",
                "htmlString": html,
            },
            "encoding": "text",
        }
    )
