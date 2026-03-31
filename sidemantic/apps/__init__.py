"""MCP Apps integration for sidemantic.

Provides interactive chart widgets for MCP Apps-compatible hosts.
The widget is built with Vite (sidemantic/apps/web/) and bundled into
a single HTML file (sidemantic/apps/chart.html) that includes the
ext-apps SDK and Vega-Lite with CSP-safe interpreter.
"""

from pathlib import Path

_WIDGET_HTML: str | None = None


def _get_widget_template() -> str:
    """Load the built chart widget HTML for the MCP Apps resource handler."""
    global _WIDGET_HTML
    if _WIDGET_HTML is None:
        built = Path(__file__).parent / "chart.html"
        if built.exists():
            _WIDGET_HTML = built.read_text()
        else:
            raise FileNotFoundError(
                f"Chart widget not built at {built}. Run: cd sidemantic/apps/web && bun install && bun run build"
            )
    return _WIDGET_HTML
