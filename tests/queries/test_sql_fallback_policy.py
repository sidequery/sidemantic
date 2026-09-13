"""Characterization tests for explicit SQL and runtime fallback boundaries."""

import pytest
from sqlglot import exp

import sidemantic.sql.parsing as sql_parsing
import sidemantic.sql.query_rewriter as query_rewriter_module
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.sql.aggregation_detection import sql_has_aggregate
from sidemantic.sql.parsing import parse_fragment, try_parse_fragment
from sidemantic.sql.query_rewriter import QueryRewriter


def test_cached_fragment_parser_returns_independent_mutable_trees():
    first = parse_fragment("orders.amount", "duckdb")
    second = parse_fragment("orders.amount", "duckdb")

    first_column = next(first.find_all(exp.Column))
    first_column.set("table", None)

    assert first.sql() == "amount"
    assert second.sql() == "orders.amount"


def test_best_effort_fragment_parse_only_swallows_invalid_sql():
    assert try_parse_fragment("sum(", "duckdb") is None

    with pytest.raises(ValueError, match="Unknown dialect"):
        try_parse_fragment("sum(amount)", "not_a_dialect")


def test_best_effort_fragment_parse_does_not_hide_runtime_bug(monkeypatch):
    def fail_unexpectedly(_sql, _dialect):
        raise RuntimeError("parser integration bug")

    monkeypatch.setattr(sql_parsing, "parse_fragment", fail_unexpectedly)

    with pytest.raises(RuntimeError, match="parser integration bug"):
        try_parse_fragment("amount", "duckdb")


def test_aggregate_regex_fallback_is_limited_to_parse_errors():
    assert sql_has_aggregate("SUM(", "duckdb") is True

    with pytest.raises(ValueError, match="Unknown dialect"):
        sql_has_aggregate("SUM(amount)", "not_a_dialect")


def test_non_strict_rewrite_does_not_hide_runtime_bug(monkeypatch):
    rewriter = QueryRewriter(SemanticGraph())

    def fail_unexpectedly(*_args, **_kwargs):
        raise RuntimeError("rewriter integration bug")

    monkeypatch.setattr(query_rewriter_module, "parse_fragment", fail_unexpectedly)

    with pytest.raises(RuntimeError, match="rewriter integration bug"):
        rewriter.rewrite("SELECT 1", strict=False)


def test_rust_backend_fallback_is_observable(monkeypatch):
    from sidemantic import Metric, Model
    from sidemantic.semantic_handoff import RustBackendUnavailableError

    def unavailable(*args, **kwargs):
        raise RustBackendUnavailableError("bindings are not installed")

    monkeypatch.setattr(query_rewriter_module, "rewrite_semantic_input", unavailable)
    graph = SemanticGraph()
    graph.add_model(Model(name="orders", table="orders", metrics=[Metric(name="count", agg="count")]))
    rewriter = QueryRewriter(graph, use_rust_rewriter=True, rust_no_fallback=False)
    assert "COUNT(" in rewriter.rewrite("SELECT count FROM orders")
    assert rewriter.rust_fallback_reason == "RustBackendUnavailableError: bindings are not installed"
    assert rewriter.last_engine_selection["engine"] == "python"


@pytest.mark.parametrize("error", [AttributeError("binding contract bug"), TypeError("rewrite binding contract bug")])
def test_rust_rewrite_does_not_hide_api_contract_defect(monkeypatch, error):
    from sidemantic import Metric, Model

    def fail_unexpectedly(*args, **kwargs):
        raise error

    monkeypatch.setattr(query_rewriter_module, "rewrite_semantic_input", fail_unexpectedly)
    graph = SemanticGraph()
    graph.add_model(Model(name="orders", table="orders", metrics=[Metric(name="count", agg="count")]))
    rewriter = QueryRewriter(graph, use_rust_rewriter=True, rust_no_fallback=False)
    with pytest.raises(type(error), match=str(error)):
        rewriter.rewrite("SELECT count FROM orders")
