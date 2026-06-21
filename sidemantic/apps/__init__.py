"""MCP Apps integration for sidemantic.

Provides interactive chart widgets for MCP Apps-compatible hosts.
The widget is built with Vite (sidemantic/apps/web/) and bundled into
a single HTML file (sidemantic/apps/chart.html) that includes the
ext-apps SDK and Vega-Lite with CSP-safe interpreter.
"""

import json
from pathlib import Path
from typing import Any

from sidemantic.vendor_assets import inline_vendor_scripts

_CHART_HTML: str | None = None
_EXPLORER_HTML: str | None = None


def _get_widget_template() -> str:
    """Load the built chart widget HTML for the MCP Apps resource handler."""
    global _CHART_HTML
    if _CHART_HTML is None:
        built = Path(__file__).parent / "chart.html"
        if built.exists():
            _CHART_HTML = built.read_text()
        else:
            raise FileNotFoundError(
                f"Chart widget not built at {built}. Run: cd sidemantic/apps/web && bun install && bun run build"
            )
    return _CHART_HTML


def _get_explorer_template() -> str:
    """Load the built explorer widget HTML for the MCP Apps resource handler."""
    global _EXPLORER_HTML
    if _EXPLORER_HTML is None:
        built = Path(__file__).parent / "explorer.html"
        if built.exists():
            _EXPLORER_HTML = built.read_text()
        else:
            raise FileNotFoundError(
                f"Explorer widget not built at {built}. Run: cd sidemantic/apps/web && bun install && bun run build"
            )
    return _EXPLORER_HTML


_CHART_WIDGET_TEMPLATE: str | None = None


def _get_chart_widget_template() -> str:
    """Load the templated chart widget HTML with vendor-script placeholders."""
    global _CHART_WIDGET_TEMPLATE
    if _CHART_WIDGET_TEMPLATE is None:
        path = Path(__file__).parent / "chart_widget.html"
        _CHART_WIDGET_TEMPLATE = path.read_text()
    return _CHART_WIDGET_TEMPLATE


def build_chart_html(vega_spec: dict[str, Any]) -> str:
    """Build a self-contained chart widget HTML with embedded Vega spec.

    Args:
        vega_spec: Vega-Lite specification dict.

    Returns:
        Complete HTML string with the spec injected.
    """
    template = _get_chart_widget_template()
    # Escape </script> sequences to prevent XSS when user-provided strings
    # (e.g., chart titles) flow into the Vega spec.
    safe_json = json.dumps(vega_spec).replace("<", "\\u003c")
    return template.replace("{{VEGA_SPEC}}", safe_json).replace(
        "{{VEGA_VENDOR_JS}}",
        inline_vendor_scripts("vega", "vega_lite", "vega_embed"),
    )


def create_chart_resource(vega_spec: dict[str, Any]):
    """Create a UIResource for a chart visualization.

    Args:
        vega_spec: Vega-Lite specification dict.

    Returns:
        UIResource (EmbeddedResource) for MCP Apps-compatible hosts.
    """
    from sidemantic.apps._mcp_ui import create_ui_resource

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
