"""Preaggregation preferences cross the SQL rewrite boundary without fallback."""

import json

import pytest

from sidemantic import Dimension, Metric, Model
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.rust_bridge import rewrite_semantic_input
from sidemantic.sql.query_rewriter import QueryRewriter


def graph():
    result = SemanticGraph()
    result.add_model(
        Model(
            name="orders",
            table="raw_orders",
            dimensions=[Dimension(name="status", type="categorical")],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        )
    )
    return result


@pytest.mark.parametrize("method", ["rewrite", "explain"])
@pytest.mark.parametrize(
    "sql",
    [
        "SELECT revenue FROM orders",
        "WITH orders_cte AS (SELECT revenue FROM orders) SELECT * FROM orders_cte",
    ],
)
def test_rollup_preference_reaches_native_compiler(monkeypatch, method, sql):
    calls = []

    def rewrite(graph, sql, **options):
        calls.append(options)
        return "SELECT 42 AS revenue"

    monkeypatch.setattr("sidemantic.sql.query_rewriter.rewrite_semantic_input", rewrite)
    rewriter = QueryRewriter(graph(), use_rust_rewriter=True, rust_no_fallback=True, use_preaggregations=True)
    result = getattr(rewriter, method)(sql)
    assert (result if method == "rewrite" else result.rewritten_sql) == "SELECT 42 AS revenue"
    assert calls[0]["use_preaggregations"] is True
    assert rewriter.last_engine_selection == {"engine": "rust", "reason": None}


@pytest.mark.parametrize("enabled", [False, True])
def test_bridge_encodes_rollup_preference_in_context(enabled):
    calls = []

    class Extension:
        def rewrite_with_semantic_input_context_diagnostics(self, source, sql, context):
            calls.append(json.loads(context))
            return json.dumps({"sql": "SELECT 42 AS revenue", "warnings": []})

    result = rewrite_semantic_input(
        graph(), "SELECT revenue FROM orders", use_preaggregations=enabled, rust_module=Extension()
    )
    assert result == "SELECT 42 AS revenue"
    assert calls[0].get("use_preaggregations", False) is enabled
