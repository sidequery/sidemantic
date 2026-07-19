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


def test_rust_initialization_fallback_is_observable(monkeypatch):
    monkeypatch.setenv("SIDEMANTIC_RS_REWRITER", "1")
    monkeypatch.delenv("SIDEMANTIC_RS_NO_FALLBACK", raising=False)

    def unavailable():
        raise ImportError("bindings are not installed")

    monkeypatch.setattr(query_rewriter_module, "get_rust_module", unavailable)

    rewriter = QueryRewriter(SemanticGraph())

    assert rewriter.rust_fallback_reason == "ImportError: bindings are not installed"
    assert rewriter._rust_module is None


def test_rust_initialization_does_not_hide_unexpected_failure(monkeypatch):
    monkeypatch.setenv("SIDEMANTIC_RS_REWRITER", "1")

    def fail_unexpectedly():
        raise AttributeError("binding contract bug")

    monkeypatch.setattr(query_rewriter_module, "get_rust_module", fail_unexpectedly)

    with pytest.raises(AttributeError, match="binding contract bug"):
        QueryRewriter(SemanticGraph())
