"""Strict wide-table transport must preserve policy and binding failures."""

import json
from types import SimpleNamespace

import pytest

from sidemantic import SemanticLayer
from sidemantic.core.model import Model
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.rust_bridge import compile_ossie_wide_semantic_input
from sidemantic.sql.ossie_wide import OssieWideQueryError


def test_native_wide_transport_retains_caller_filter_obligations():
    graph = SemanticGraph()
    graph.add_model(Model(name="orders", table="orders", required_filters=["tenant_id"]))
    captured = {}

    def native(input_json, query_json, dialect):
        captured.update(input=json.loads(input_json), query=json.loads(query_json), dialect=dialect)
        raise ValueError("Validation error: E_INVALID_QUERY: Policy context is unavailable")

    module = SimpleNamespace(compile_ossie_wide_with_semantic_input=native)
    with pytest.raises(OssieWideQueryError) as result:
        compile_ossie_wide_semantic_input(graph, {"metrics": ["orders.count"]}, rust_module=module)
    assert result.value.code == "E_INVALID_QUERY"
    assert captured["input"]["models"][0]["required_filters"] == ["tenant_id"]


def test_native_binding_error_does_not_fall_back_to_python():
    def native(*_):
        raise ValueError("E_AMBIGUOUS_PATH: Multiple directed paths")

    layer = SemanticLayer(engine="python", auto_register=False)
    try:
        layer._use_rust_sql_generator = True
        layer._rust_no_fallback = False
        layer._rust_module = SimpleNamespace(compile_ossie_wide_with_semantic_input=native)
        with pytest.raises(OssieWideQueryError) as result:
            layer.compile_ossie_wide({"metrics": ["orders.count"]})
        assert result.value.code == "E_AMBIGUOUS_PATH"
    finally:
        layer.adapter.close()


def test_strict_wide_rejects_unhandled_security_context():
    layer = SemanticLayer(engine="python", auto_register=False)
    try:
        with pytest.raises(OssieWideQueryError) as result:
            layer.query_ossie_wide({"metrics": ["orders.count"]}, user_attributes={"tenant_id": 1})
        assert result.value.code == "E_INVALID_QUERY"
    finally:
        layer.adapter.close()


def test_strict_wide_rejects_visibility_enforcement_even_without_policy_models():
    layer = SemanticLayer(engine="python", auto_register=False, enforce_visibility=True)
    try:
        with pytest.raises(OssieWideQueryError) as result:
            layer.compile_ossie_wide({"metrics": ["orders.count"]})
        assert result.value.code == "E_INVALID_QUERY"
    finally:
        layer.adapter.close()


def test_wide_compile_preserves_graph_input_dialect_when_output_changes():
    captured = {}

    def native(input_json, query_json, dialect):
        captured.update(input=json.loads(input_json), output=dialect)
        return "SELECT 1"

    layer = SemanticLayer(engine="python", auto_register=False, freeze_current_time=False)
    try:
        layer.dialect = "postgres"
        layer._use_rust_sql_generator = True
        layer._rust_module = SimpleNamespace(compile_ossie_wide_with_semantic_input=native)
        assert layer.compile_ossie_wide({"metrics": ["orders.count"]}, dialect="snowflake") == "SELECT 1"
        assert captured["input"]["input_dialect"] == "postgres"
        assert captured["output"] == "snowflake"
    finally:
        layer.adapter.close()
