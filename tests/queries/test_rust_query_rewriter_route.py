import pytest

from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.sql.query_rewriter import QueryRewriter


def _graph() -> SemanticGraph:
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[Dimension(name="status", type="categorical", sql="status")],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        )
    )
    return graph


def test_query_rewriter_routes_to_rust_when_enabled(monkeypatch):
    class FakeRustModule:
        def __init__(self):
            self.calls = []

        def rewrite_with_yaml(self, yaml_text: str, sql_text: str) -> str:
            self.calls.append((yaml_text, sql_text))
            return "SELECT 1 AS from_rust"

    fake = FakeRustModule()
    monkeypatch.setenv("SIDEMANTIC_RS_REWRITER", "1")
    monkeypatch.delenv("SIDEMANTIC_RS_NO_FALLBACK", raising=False)
    monkeypatch.setattr("sidemantic.sql.query_rewriter.get_rust_module", lambda: fake)
    monkeypatch.setattr("sidemantic.sql.query_rewriter.graph_to_rust_yaml", lambda _graph: "models: []")

    rewritten = QueryRewriter(_graph()).rewrite("SELECT orders.revenue FROM orders")

    assert rewritten == "SELECT 1 AS from_rust"
    assert fake.calls == [("models: []", "SELECT orders.revenue FROM orders")]


def test_query_rewriter_no_fallback_raises_when_rust_fails(monkeypatch):
    class FailingRustModule:
        def rewrite_with_yaml(self, _yaml_text: str, _sql_text: str) -> str:
            raise RuntimeError("boom")

    monkeypatch.setenv("SIDEMANTIC_RS_REWRITER", "1")
    monkeypatch.setenv("SIDEMANTIC_RS_NO_FALLBACK", "1")
    monkeypatch.setattr("sidemantic.sql.query_rewriter.get_rust_module", lambda: FailingRustModule())
    monkeypatch.setattr("sidemantic.sql.query_rewriter.graph_to_rust_yaml", lambda _graph: "models: []")

    with pytest.raises(ValueError, match="Rust rewriter failed: boom"):
        QueryRewriter(_graph()).rewrite("SELECT orders.revenue FROM orders")


def test_rust_rewriter_falls_back_when_graph_metric_sql_cannot_be_prepared(monkeypatch):
    class FailingRustModule:
        def __init__(self):
            self.calls = []

        def rewrite_with_yaml(self, yaml_text: str, sql_text: str) -> str:
            self.calls.append((yaml_text, sql_text))
            raise RuntimeError("boom")

    graph = _graph()
    graph.add_metric(
        Metric(
            name="placeholder_metric",
            type="derived",
            sql="${TABLE}.amount",
        )
    )

    fake = FailingRustModule()
    monkeypatch.setenv("SIDEMANTIC_RS_REWRITER", "1")
    monkeypatch.delenv("SIDEMANTIC_RS_NO_FALLBACK", raising=False)
    monkeypatch.setattr("sidemantic.sql.query_rewriter.get_rust_module", lambda: fake)
    monkeypatch.setattr("sidemantic.sql.query_rewriter.graph_to_rust_yaml", lambda _graph: "models: []")

    rewriter = QueryRewriter(graph)
    monkeypatch.setattr(rewriter, "_rewrite_simple_query", lambda _parsed: "SELECT 42 AS fallback")

    rewritten = rewriter.rewrite("SELECT placeholder_metric FROM metrics")

    assert rewritten == "SELECT 42 AS fallback"
    assert fake.calls == [("models: []", "SELECT placeholder_metric FROM metrics")]
