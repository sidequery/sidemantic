#!/usr/bin/env python
"""Modal deployment for the headless crossfilter charting demo.

Run from the repository root:
  uvx modal serve examples/integrations/modal_headless_charting.py
  uvx modal deploy examples/integrations/modal_headless_charting.py

The app builds the demo semantic layer on container startup using a persisted
DuckDB file on a Modal Volume. Larger demo sources are view-backed on Modal.
Run warm_crossfilter_cache after deployment to precompute and persist the
interaction pre-aggregations used by the dashboard.
"""

from __future__ import annotations

import json
from contextlib import suppress

import modal

APP_NAME = "sidemantic-headless-charting-demo"
DEFAULT_RENDERER = "vega-lite"
CACHE_DIR = "/cache"
DUCKDB_PATH = f"{CACHE_DIR}/crossfilter.duckdb"
CACHE_VOLUME_NAME = "sidemantic-headless-charting-cache"

image = (
    modal.Image.debian_slim(python_version="3.12")
    .uv_pip_install(
        "antlr4-python3-runtime>=4.13.2",
        "duckdb>=1.0.0",
        "fastapi[standard]>=0.115.0",
        "jinja2>=3.1.0",
        "pydantic>=2.0.0",
        "pyyaml>=6.0",
        "sqlglot==27.12.0",
        "typer>=0.9.0",
    )
    .add_local_python_source("sidemantic")
    .add_local_dir(
        "examples/integrations",
        remote_path="/root/examples/integrations",
        ignore=["**/__pycache__/**", "**/*.pyc"],
    )
)

app = modal.App(APP_NAME)
cache_volume = modal.Volume.from_name(CACHE_VOLUME_NAME, create_if_missing=True)


def _build_dashboard():
    from examples.integrations.headless_charting import build_crossfilter_dashboard

    return build_crossfilter_dashboard(
        connection=f"duckdb:///{DUCKDB_PATH}",
        dashboard_renderer=DEFAULT_RENDERER,
        large_records=200_000,
        huge_records=2_000_000,
        massive_records=20_000_000,
        extreme_records=100_000_000,
        large_materialized=False,
        huge_materialized=False,
    )


@app.function(image=image, volumes={CACHE_DIR: cache_volume}, timeout=900)
def warm_crossfilter_cache():
    cache_volume.reload()
    dashboard = _build_dashboard()
    diagnostics = dashboard.warm_interaction_preaggregations()
    _checkpoint_and_close(dashboard)
    cache_volume.commit()
    return json.dumps(_warm_summary(diagnostics), indent=2, sort_keys=True)


@app.function(
    image=image,
    volumes={CACHE_DIR: cache_volume},
    max_containers=1,
    scaledown_window=60,
    timeout=300,
)
@modal.asgi_app(label="crossfilter")
def crossfilter():
    cache_volume.reload()
    dashboard = _build_dashboard()
    return dashboard.to_asgi_app()


def _checkpoint_and_close(dashboard) -> None:
    adapters = {}
    for tab in dashboard.tabs:
        adapter = tab.session.chart.layer.adapter
        adapters[id(adapter)] = adapter
    for adapter in adapters.values():
        with suppress(Exception):
            adapter.execute("CHECKPOINT")
        with suppress(Exception):
            adapter.close()


def _warm_summary(diagnostics: dict) -> dict:
    summary = {}
    for tab_id, info in diagnostics.items():
        table = info.get("table") or {}
        summary[tab_id] = {
            "used": info.get("used"),
            "reused": info.get("reused"),
            "table_name": table.get("table_name"),
            "row_count": table.get("row_count"),
            "build_ms": round(float(table.get("build_ms") or 0), 2),
        }
    return summary
