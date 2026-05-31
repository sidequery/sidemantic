"""Tests for directory loaders."""

import builtins
import sys
from pathlib import Path

from sidemantic import SemanticLayer
from sidemantic.loaders import load_from_directory


def test_load_from_directory_does_not_require_antlr4_without_antlr_formats(tmp_path, monkeypatch):
    """Cube loading should work even when ANTLR runtime is unavailable."""
    fixture_file = Path(__file__).parent / "fixtures" / "cube" / "orders.yml"
    (tmp_path / "orders.yml").write_text(fixture_file.read_text())

    monkeypatch.delitem(sys.modules, "sidemantic.adapters.holistics", raising=False)
    for module_name in list(sys.modules):
        if module_name == "antlr4" or module_name.startswith("antlr4."):
            monkeypatch.delitem(sys.modules, module_name, raising=False)

    real_import = builtins.__import__

    def blocked_antlr4_import(name, *args, **kwargs):
        if name == "antlr4" or name.startswith("antlr4."):
            raise ImportError("simulated missing antlr4")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", blocked_antlr4_import)

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path)

    assert "orders" in layer.graph.models


def test_native_inheritance_does_not_register_model_metrics_globally(tmp_path):
    (tmp_path / "models.yml").write_text(
        """
models:
  - name: base_orders
    table: orders
    primary_key: order_id
    metrics:
      - name: margin_label
        type: derived
        sql: "'margin'"

  - name: orders
    extends: base_orders
    table: orders
    primary_key: order_id
"""
    )

    layer = SemanticLayer(auto_register=True)
    load_from_directory(layer, tmp_path)

    assert "orders" in layer.graph.models
    assert layer.graph.models["orders"].get_metric("margin_label") is not None
    assert "margin_label" not in layer.graph.metrics
