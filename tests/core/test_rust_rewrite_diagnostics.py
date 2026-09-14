"""Native diagnostics preserve Python warning and binding-error behavior."""

import json
import warnings
from types import SimpleNamespace

import pytest

from sidemantic import Dimension, Metric, Model
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.rust_bridge import rewrite_semantic_input
from sidemantic.sql.query_rewriter import YardstickBindingError, YardstickWarning


@pytest.fixture
def graph():
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="sales_v",
            table="sales",
            dimensions=[Dimension(name="year", type="numeric")],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
            metadata={"yardstick": {}},
        )
    )
    return graph


def test_rewrite_diagnostics_emit_warning_on_every_call(graph):
    calls = []

    def compile(graph_json, sql, context_json):
        calls.append((sql, json.loads(context_json)))
        return json.dumps({"sql": "select 375", "warnings": ["AT ALL drops year"]})

    module = SimpleNamespace(rewrite_with_semantic_input_context_diagnostics=compile)
    with pytest.warns(YardstickWarning, match="year"):
        assert (
            rewrite_semantic_input(graph, "select aggregate(revenue) from sales_v", rust_module=module) == "select 375"
        )
    with warnings.catch_warnings():
        warnings.simplefilter("error", YardstickWarning)
        with pytest.raises(YardstickWarning, match="year"):
            rewrite_semantic_input(graph, "select aggregate(revenue) from sales_v", rust_module=module)
    assert len(calls) == 2
    assert calls[0][1] == {"user_attributes": None, "enforce_visibility": False}


@pytest.mark.parametrize("report", ["not json", "{}", '{"sql":1,"warnings":[]}', '{"sql":"select 1","warnings":[1]}'])
def test_invalid_diagnostics_never_become_query_passthrough(graph, report):
    module = SimpleNamespace(rewrite_with_semantic_input_context_diagnostics=lambda *args: report)
    with pytest.raises(TypeError, match="diagnostics"):
        rewrite_semantic_input(graph, "select aggregate(revenue) from sales_v", rust_module=module)


def test_native_yardstick_binding_error_preserves_public_type(graph):
    class NativeBindingError(ValueError):
        pass

    def fail(*args):
        raise NativeBindingError("Unknown measure missing")

    module = SimpleNamespace(YardstickBindingError=NativeBindingError, rewrite_with_semantic_input=fail)
    with pytest.raises(YardstickBindingError, match="missing"):
        rewrite_semantic_input(graph, "select aggregate(missing) from sales_v", rust_module=module)
