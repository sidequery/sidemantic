"""Tests for directory loaders."""

import builtins
import sys
from pathlib import Path

import pytest

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


def test_load_from_directory_strict_raises_on_detected_parse_error(tmp_path):
    """Strict loading fails instead of returning a partial graph."""
    (tmp_path / "good.yml").write_text(
        """
models:
  - name: orders
    table: orders
    primary_key: id
"""
    )
    (tmp_path / "bad.yml").write_text(
        """
models:
  - name: broken
    table: [
"""
    )

    layer = SemanticLayer()
    with pytest.raises(ValueError, match="Could not parse .*bad.yml"):
        load_from_directory(layer, tmp_path)

    assert not layer.graph.models


def test_load_from_directory_lenient_mode_skips_detected_parse_error(tmp_path):
    """Lenient loading remains available as an explicit opt-in."""
    (tmp_path / "good.yml").write_text(
        """
models:
  - name: orders
    table: orders
    primary_key: id
"""
    )
    (tmp_path / "bad.yml").write_text(
        """
models:
  - name: broken
    table: [
"""
    )

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path, strict=False)

    assert set(layer.graph.models) == {"orders"}


def test_load_from_directory_resolves_native_inheritance_across_files(tmp_path):
    (tmp_path / "base.yml").write_text(
        """
version: 1
models:
  - name: base_orders
    table: orders
    primary_key: id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: amount
"""
    )
    (tmp_path / "child.yml").write_text(
        """
version: 1
models:
  - name: paid_orders
    extends: base_orders
    dimensions:
      - name: paid_at
        type: time
        sql: paid_at
        granularity: day
"""
    )

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path)

    paid_orders = layer.graph.models["paid_orders"]
    assert paid_orders.table == "orders"
    assert paid_orders.primary_key == "id"
    assert paid_orders.extends is None
    assert paid_orders.get_dimension("status") is not None
    assert paid_orders.get_dimension("paid_at") is not None
    assert paid_orders.get_metric("revenue") is not None


def test_load_from_directory_resolves_native_metric_inheritance_after_model_merge(tmp_path):
    (tmp_path / "base.yml").write_text(
        """
version: 1
models:
  - name: base_orders
    table: orders
    primary_key: id
    metrics:
      - name: revenue
        agg: sum
        sql: amount
"""
    )
    (tmp_path / "child.yml").write_text(
        """
version: 1
models:
  - name: paid_orders
    extends: base_orders
    metrics:
      - name: paid_revenue
        extends: revenue
        filters:
          - status = 'paid'
"""
    )

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path)

    paid_revenue = layer.graph.models["paid_orders"].get_metric("paid_revenue")
    assert paid_revenue is not None
    assert paid_revenue.extends is None
    assert paid_revenue.agg == "sum"
    assert paid_revenue.sql == "amount"
    assert paid_revenue.filters == ["status = 'paid'"]


def test_load_from_directory_detects_native_metrics_only_file(tmp_path):
    (tmp_path / "models.yml").write_text(
        """
version: 1
models:
  - name: orders
    table: orders
    primary_key: id
    metrics:
      - name: revenue
        agg: sum
        sql: amount
      - name: order_count
        agg: count
"""
    )
    (tmp_path / "metrics.yml").write_text(
        """
version: 1
metrics:
  - name: finance.revenue_per_order
    type: ratio
    numerator: orders.revenue
    denominator: orders.order_count
"""
    )

    layer = SemanticLayer()
    load_from_directory(layer, tmp_path)

    metric = layer.graph.metrics["finance.revenue_per_order"]
    assert metric.numerator == "orders.revenue"
    assert metric.denominator == "orders.order_count"


def test_load_from_directory_strict_raises_on_missing_native_parent(tmp_path):
    (tmp_path / "child.yml").write_text(
        """
version: 1
models:
  - name: paid_orders
    extends: missing_base
    table: orders
"""
    )

    layer = SemanticLayer()
    with pytest.raises(ValueError, match="Native model 'paid_orders' extends unknown model 'missing_base'"):
        load_from_directory(layer, tmp_path)

    assert not layer.graph.models


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
