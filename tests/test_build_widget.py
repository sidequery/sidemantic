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
