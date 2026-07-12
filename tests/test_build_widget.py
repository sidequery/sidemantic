"""Tests for deterministic widget artifact synchronization."""

from __future__ import annotations

import importlib.util
from pathlib import Path


def _module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "build_widget.py"
    spec = importlib.util.spec_from_file_location("sidemantic_build_widget", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_files_match_compares_presence_and_bytes(tmp_path: Path) -> None:
    module = _module()
    left = tmp_path / "left.js"
    right = tmp_path / "right.js"

    assert module.files_match(left, right) is False
    left.write_text("same", encoding="utf-8")
    right.write_text("same", encoding="utf-8")
    assert module.files_match(left, right) is True
    right.write_text("different", encoding="utf-8")
    assert module.files_match(left, right) is False


def test_widget_loading_and_brush_paths_use_canonical_mounts() -> None:
    root = Path(__file__).resolve().parents[1]
    source = (root / "js/widget.js").read_text()

    assert "renderMetricSkeleton" not in source
    assert 'mountState(metricsColEl, { kind: "loading"' in source
    assert 'key: "active-date-range"' in source
    assert "onRemove: clearBrush" in source


def test_widget_css_contains_canonical_utilities_and_host_layout() -> None:
    root = Path(__file__).resolve().parents[1]
    css = (root / "sidemantic/widget/static/widget.css").read_text()

    assert ".flex{display:flex}" in css
    assert ".bg-surface{background-color:var(--surface)}" in css
    assert ".sidemantic-widget" in css
    assert ".widget-layout" in css
