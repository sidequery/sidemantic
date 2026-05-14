"""Strict-mode behavior tests for Rust SQL generator entrypoint."""

import pytest
import yaml

import sidemantic.core.semantic_layer as semantic_layer_module
import sidemantic.rust_parity as rust_parity
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.semantic_layer import SemanticLayer


@pytest.fixture(autouse=True)
def _reset_strict_targets_cache():
    rust_parity.strict_targets.cache_clear()
    yield
    rust_parity.strict_targets.cache_clear()


def _configure_strict_sql_entrypoint(monkeypatch) -> None:
    monkeypatch.setenv("SIDEMANTIC_RS_STRICT_SUBSYSTEMS", "sql_generator_entrypoint")
    monkeypatch.delenv("SIDEMANTIC_RS_SQL_GENERATOR", raising=False)
    monkeypatch.delenv("SIDEMANTIC_RS_SQL_GENERATOR_VERIFY", raising=False)
    rust_parity.strict_targets.cache_clear()


def _build_layer(monkeypatch) -> SemanticLayer:
    monkeypatch.setattr(semantic_layer_module, "get_rust_module", lambda: object())
    layer = SemanticLayer(auto_register=False)
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="order_id",
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        )
    )
    return layer


def test_strict_sql_entrypoint_forces_rust_compile(monkeypatch):
    _configure_strict_sql_entrypoint(monkeypatch)
    layer = _build_layer(monkeypatch)

    calls = {"rust": 0, "python": 0}

    def fake_rust_compile(**_kwargs):
        calls["rust"] += 1
        return "SELECT 1"

    def fake_python_compile(**_kwargs):
        calls["python"] += 1
        return "SELECT 2"

    monkeypatch.setattr(layer, "_compile_with_rust", fake_rust_compile)
    monkeypatch.setattr(layer, "_compile_with_python", fake_python_compile)

    sql = layer.compile(metrics=["orders.revenue"])
    assert sql.startswith("SELECT 1")
    assert calls["rust"] == 1
    assert calls["python"] == 0
    assert layer._use_rust_sql_generator is True


def test_strict_sql_entrypoint_rejects_python_fallback(monkeypatch):
    _configure_strict_sql_entrypoint(monkeypatch)
    layer = _build_layer(monkeypatch)

    monkeypatch.setattr(layer, "_compile_with_rust", lambda **_kwargs: None)
    monkeypatch.setattr(
        layer,
        "_compile_with_python",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("python fallback should not run")),
    )

    with pytest.raises(ValueError, match="returned no SQL in strict mode"):
        layer.compile(metrics=["orders.revenue"])


def test_strict_sql_entrypoint_disables_python_verify(monkeypatch):
    _configure_strict_sql_entrypoint(monkeypatch)
    layer = _build_layer(monkeypatch)
    assert layer._rust_sql_verify is False


def test_rust_compile_payload_includes_preaggregation_flags(monkeypatch):
    _configure_strict_sql_entrypoint(monkeypatch)
    layer = _build_layer(monkeypatch)
    captured = {}

    class FakeRustModule:
        def compile_with_yaml(self, _models_yaml, query_yaml):
            captured.update(yaml.safe_load(query_yaml))
            return "SELECT 1"

    layer._rust_module = FakeRustModule()
    sql = layer.compile(metrics=["orders.revenue"], use_preaggregations=True)

    assert sql.startswith("SELECT 1")
    assert captured["use_preaggregations"] is True
    assert "preagg_database" in captured
    assert "preagg_schema" in captured
