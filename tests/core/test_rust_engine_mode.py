"""Explicit runtime engine selection tests."""

import pytest

import sidemantic.core.semantic_layer as semantic_layer_module
import sidemantic.rust_bridge as rust_bridge
from sidemantic import Dimension, Metric, Model, SemanticLayer


class FakeRustModule:
    def validate_query_references(self, _models_yaml, _metrics, _dimensions):
        return []

    def compile_with_yaml(self, _models_yaml, _query_yaml):
        return "SELECT 1 AS from_rust"


def test_semantic_layer_rust_engine_uses_rust_compile_without_sql_string_verification(monkeypatch):
    fake = FakeRustModule()
    monkeypatch.setattr(semantic_layer_module, "get_rust_module", lambda: fake)
    monkeypatch.setattr(rust_bridge, "get_rust_module", lambda: fake)

    layer = SemanticLayer(engine="rust")
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="order_id",
            dimensions=[Dimension(name="status", type="categorical")],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        )
    )

    sql = layer.compile(metrics=["orders.revenue"], dimensions=["orders.status"])

    assert sql.startswith("SELECT 1 AS from_rust")
    assert layer._rust_no_fallback is True
    assert layer._rust_sql_verify is False


def test_semantic_layer_python_engine_ignores_rust_env_flags(monkeypatch):
    monkeypatch.setenv("SIDEMANTIC_RS_SQL_GENERATOR", "1")
    monkeypatch.setenv("SIDEMANTIC_RS_QUERY_VALIDATION", "1")

    layer = SemanticLayer(engine="python")

    assert layer._use_rust_sql_generator is False
    assert layer._use_rust_query_validation is False


def test_semantic_layer_invalid_engine_rejected():
    with pytest.raises(ValueError, match="engine must be one of"):
        SemanticLayer(engine="sideways")
