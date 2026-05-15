"""Strict and env-gated tests for Rust-backed query validation."""

import pytest

import sidemantic.rust_parity as rust_parity
import sidemantic.validation as validation_module
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.semantic_layer import SemanticLayer


@pytest.fixture(autouse=True)
def _reset_strict_targets_cache():
    rust_parity.strict_targets.cache_clear()
    yield
    rust_parity.strict_targets.cache_clear()


def _clear_strict_cache() -> None:
    rust_parity.strict_targets.cache_clear()


def _build_layer() -> SemanticLayer:
    layer = SemanticLayer(auto_register=False)
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="order_id",
            dimensions=[Dimension(name="status", type="categorical")],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        )
    )
    return layer


def test_query_validation_routes_to_rust(monkeypatch):
    import sidemantic.rust_bridge as rust_bridge

    monkeypatch.setenv("SIDEMANTIC_RS_QUERY_VALIDATION", "1")
    monkeypatch.delenv("SIDEMANTIC_RS_STRICT_SUBSYSTEMS", raising=False)
    monkeypatch.delenv("SIDEMANTIC_RS_NO_FALLBACK", raising=False)
    _clear_strict_cache()

    layer = _build_layer()
    monkeypatch.setattr(rust_bridge, "validate_query_with_rust", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        validation_module,
        "validate_query",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("python validation should not run")),
    )

    sql = layer.compile(metrics=["orders.revenue"], dimensions=["orders.status"])
    assert "SELECT" in sql


def test_query_validation_strict_raises_without_fallback(monkeypatch):
    import sidemantic.rust_bridge as rust_bridge

    monkeypatch.setenv("SIDEMANTIC_RS_STRICT_SUBSYSTEMS", "semantic_core_query_validation")
    monkeypatch.setenv("SIDEMANTIC_RS_NO_FALLBACK", "1")
    monkeypatch.delenv("SIDEMANTIC_RS_QUERY_VALIDATION", raising=False)
    _clear_strict_cache()

    layer = _build_layer()
    monkeypatch.setattr(
        rust_bridge,
        "validate_query_with_rust",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("rust validation failure")),
    )

    with pytest.raises(
        validation_module.QueryValidationError, match="Rust query validation failed: rust validation failure"
    ):
        layer.compile(metrics=["orders.revenue"], dimensions=["orders.status"])


def test_query_validation_with_rust_matches_python_error_text(monkeypatch):
    monkeypatch.setenv("SIDEMANTIC_RS_QUERY_VALIDATION", "1")
    monkeypatch.delenv("SIDEMANTIC_RS_STRICT_SUBSYSTEMS", raising=False)
    monkeypatch.delenv("SIDEMANTIC_RS_NO_FALLBACK", raising=False)
    _clear_strict_cache()

    layer = _build_layer()

    with pytest.raises(validation_module.QueryValidationError) as exc_info:
        layer.compile(metrics=["missing_metric"], dimensions=["orders.status"])

    assert "Metric 'missing_metric' not found" in str(exc_info.value)


def test_validate_query_with_rust_prefers_reference_entrypoint(monkeypatch):
    import sidemantic.rust_bridge as rust_bridge

    calls = {"references": 0}

    class _FakeRustModule:
        def validate_query_references(self, models_yaml, metrics, dimensions):
            calls["references"] += 1
            assert models_yaml == "models: []"
            assert metrics == ["orders.revenue"]
            assert dimensions == ["orders.status"]
            return []

        def validate_query_with_yaml(self, _models_yaml, _query_yaml):
            raise AssertionError("legacy validation entrypoint should not be used")

    monkeypatch.setattr(rust_bridge, "get_rust_module", lambda: _FakeRustModule())
    monkeypatch.setattr(rust_bridge, "graph_to_rust_yaml", lambda _graph: "models: []")

    layer = _build_layer()
    errors = rust_bridge.validate_query_with_rust(layer.graph, ["orders.revenue"], ["orders.status"])
    assert errors == []
    assert calls["references"] == 1


def test_validate_query_with_rust_falls_back_to_legacy_payload_entrypoint(monkeypatch):
    import sidemantic.rust_bridge as rust_bridge

    calls = {"legacy": 0}

    class _LegacyRustModule:
        def validate_query_with_yaml(self, models_yaml, query_yaml):
            calls["legacy"] += 1
            assert models_yaml == "models: []"
            assert "metrics:" in query_yaml
            assert "dimensions:" in query_yaml
            return ["legacy error"]

    monkeypatch.setattr(rust_bridge, "get_rust_module", lambda: _LegacyRustModule())
    monkeypatch.setattr(rust_bridge, "graph_to_rust_yaml", lambda _graph: "models: []")

    layer = _build_layer()
    errors = rust_bridge.validate_query_with_rust(layer.graph, ["orders.revenue"], ["orders.status"])
    assert errors == ["legacy error"]
    assert calls["legacy"] == 1


def test_validate_query_with_rust_falls_back_on_reference_signature_incompatibility(monkeypatch):
    import sidemantic.rust_bridge as rust_bridge

    calls = {"legacy": 0}

    class _IncompatibleReferenceRustModule:
        def validate_query_references(self, _models_yaml, _query_yaml):
            return []

        def validate_query_with_yaml(self, models_yaml, query_yaml):
            calls["legacy"] += 1
            assert models_yaml == "models: []"
            assert "metrics:" in query_yaml
            assert "dimensions:" in query_yaml
            return ["legacy signature fallback"]

    monkeypatch.setattr(rust_bridge, "get_rust_module", lambda: _IncompatibleReferenceRustModule())
    monkeypatch.setattr(rust_bridge, "graph_to_rust_yaml", lambda _graph: "models: []")

    layer = _build_layer()
    errors = rust_bridge.validate_query_with_rust(layer.graph, ["orders.revenue"], ["orders.status"])
    assert errors == ["legacy signature fallback"]
    assert calls["legacy"] == 1


def test_validate_query_with_rust_propagates_reference_validation_error(monkeypatch):
    import sidemantic.rust_bridge as rust_bridge

    class _ReferenceValidationErrorRustModule:
        def validate_query_references(self, models_yaml, metrics, dimensions):
            assert models_yaml == "models: []"
            assert metrics == ["orders.revenue"]
            assert dimensions == ["orders.status"]
            raise ValueError("reference validation failure")

        def validate_query_with_yaml(self, _models_yaml, _query_yaml):
            raise AssertionError("legacy entrypoint should not be used for validation errors")

    monkeypatch.setattr(rust_bridge, "get_rust_module", lambda: _ReferenceValidationErrorRustModule())
    monkeypatch.setattr(rust_bridge, "graph_to_rust_yaml", lambda _graph: "models: []")

    layer = _build_layer()
    with pytest.raises(ValueError, match="reference validation failure"):
        rust_bridge.validate_query_with_rust(layer.graph, ["orders.revenue"], ["orders.status"])
