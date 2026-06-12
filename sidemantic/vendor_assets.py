"""Vendored browser renderer assets for standalone chart HTML."""

from __future__ import annotations

from functools import cache
from importlib.resources import files

_VENDOR_ASSETS = {
    "d3": "d3-7.9.0.min.js",
    "observable_plot": "observable-plot-0.6.17.umd.min.js",
    "plotly": "plotly-2.35.2.min.js",
    "vega": "vega-5.33.1.min.js",
    "vega_embed": "vega-embed-6.29.0.min.js",
    "vega_lite": "vega-lite-5.23.0.min.js",
}


@cache
def vendor_asset_text(name: str) -> str:
    """Return the vendored JavaScript bundle text for a renderer dependency."""
    try:
        filename = _VENDOR_ASSETS[name]
    except KeyError as exc:
        raise ValueError(f"Unknown vendored renderer asset {name!r}") from exc
    return files("sidemantic").joinpath("assets", "vendor", filename).read_text(encoding="utf-8")


def inline_vendor_script(name: str) -> str:
    """Return a script tag containing a vendored renderer dependency."""
    return f'<script data-sidemantic-vendor="{name}">\n{_escape_script_body(vendor_asset_text(name))}\n</script>'


def inline_vendor_scripts(*names: str) -> str:
    """Return script tags for vendored renderer dependencies in load order."""
    return "\n".join(inline_vendor_script(name) for name in names)


def _escape_script_body(script: str) -> str:
    return script.replace("</script", "<\\/script").replace("</SCRIPT", "<\\/SCRIPT")
