"""Tests for deterministic web UI artifact synchronization."""

from __future__ import annotations

import importlib.util
from pathlib import Path


def _module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "build_webapp.py"
    spec = importlib.util.spec_from_file_location("sidemantic_build_webapp", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_directories_match_compares_paths_and_bytes(tmp_path: Path) -> None:
    module = _module()
    source = tmp_path / "source"
    target = tmp_path / "target"
    (source / "assets").mkdir(parents=True)
    (target / "assets").mkdir(parents=True)
    (source / "index.html").write_text("same", encoding="utf-8")
    (target / "index.html").write_text("same", encoding="utf-8")
    (source / "assets" / "app.js").write_text("one", encoding="utf-8")
    (target / "assets" / "app.js").write_text("one", encoding="utf-8")

    assert module.directories_match(source, target) is True

    (target / "assets" / "app.js").write_text("two", encoding="utf-8")
    assert module.directories_match(source, target) is False

    (target / "assets" / "app.js").write_text("one", encoding="utf-8")
    (target / "extra.css").write_text("extra", encoding="utf-8")
    assert module.directories_match(source, target) is False


def test_webapp_never_reads_or_persists_bearer_from_url() -> None:
    root = Path(__file__).resolve().parents[1]
    app_source = root.joinpath("webapp/src/App.tsx").read_text()
    adapter_source = root.joinpath("webapp/src/data/httpAdapter.ts").read_text()

    assert 'get("token")' not in app_source
    assert "sidemantic-token" not in app_source
    assert "localStorage" not in app_source
    assert "createBrowserSession" in app_source
    assert 'fetch(this.url("/auth/session")' in adapter_source
