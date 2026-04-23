"""MCP Apps integration for sidemantic.

Provides interactive chart widgets for MCP Apps-compatible hosts.
The widget is built with Vite (sidemantic/apps/web/) and bundled into
a single HTML file (sidemantic/apps/chart.html) that includes the
ext-apps SDK and Vega-Lite with CSP-safe interpreter.
"""

from pathlib import Path

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
