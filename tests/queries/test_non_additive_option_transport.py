"""Explicit snapshot escape-hatch preferences reach every compilation entrypoint."""

import json

import pytest

from sidemantic import Dimension, Metric, Model, SemanticLayer
from sidemantic.rust_bridge import rewrite_semantic_input, validate_semantic_input


@pytest.fixture
def layer(monkeypatch):
    monkeypatch.setattr("sidemantic.core.semantic_layer.get_rust_module", lambda: object())
    result = SemanticLayer(engine="rust", fallback=False, allow_non_additive_unsafe=True, auto_register=False)
    result.add_model(
        Model(
            name="accounts",
            table="accounts",
            dimensions=[Dimension(name="day", type="time", granularity="day")],
            metrics=[Metric(name="balance", agg="sum", sql="amount", non_additive_dimension="day")],
        )
    )
    yield result
    result.adapter.close()


@pytest.mark.parametrize("method", ["compile", "query", "sql", "explain_sql"])
def test_layer_passes_live_snapshot_preference(monkeypatch, layer, method):
    calls = []

    def compile(graph, query, **kwargs):
        calls.append(query.get("allow_non_additive_unsafe", False))
        return "SELECT 42 AS balance"

    def rewrite(graph, query, **kwargs):
        calls.append(kwargs.get("allow_non_additive_unsafe", False))
        return "SELECT 42 AS balance"

    monkeypatch.setattr("sidemantic.rust_bridge.compile_semantic_input", compile)
    monkeypatch.setattr("sidemantic.rust_bridge.validate_semantic_input", lambda *args, **kwargs: [])
    monkeypatch.setattr("sidemantic.sql.query_rewriter.rewrite_semantic_input", rewrite)
    for enabled in [True, False]:
        layer.allow_non_additive_unsafe = enabled
        if method in {"compile", "query"}:
            result = getattr(layer, method)(metrics=["accounts.balance"])
        else:
            result = getattr(layer, method)("SELECT balance FROM accounts")
        if method in {"query", "sql"}:
            assert result.fetchall() == [(42,)]
        assert calls[-1] is enabled
    assert calls == [True, False]


@pytest.mark.parametrize("method", ["validate", "rewrite"])
def test_bridge_serializes_snapshot_preference(layer, method):
    calls = []

    class Extension:
        def validate_with_semantic_input(self, source, query):
            calls.append(json.loads(query))
            return []

        def rewrite_with_semantic_input_context_diagnostics(self, source, sql, context):
            calls.append(json.loads(context))
            return json.dumps({"sql": "SELECT 42", "warnings": []})

    if method == "validate":
        validate_semantic_input(
            layer.graph, ["accounts.balance"], [], allow_non_additive_unsafe=True, rust_module=Extension()
        )
    else:
        rewrite_semantic_input(
            layer.graph,
            "SELECT balance FROM accounts",
            allow_non_additive_unsafe=True,
            rust_module=Extension(),
        )
    assert calls[0]["allow_non_additive_unsafe"] is True
