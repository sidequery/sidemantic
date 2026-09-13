import pytest

from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.semantic_handoff import RustBackendUnavailableError, UnsupportedSemanticFeaturesError
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


def test_query_rewriter_passes_unexpanded_graph_metric_to_rust(monkeypatch):
    graph = _graph()
    graph.add_metric(Metric(name="total_revenue", type="derived", sql="orders.revenue * 2"))
    calls = []

    def rewrite(graph_input, sql, **kwargs):
        calls.append((graph_input, sql, kwargs))
        return "SELECT 1 AS from_rust"

    monkeypatch.setattr("sidemantic.sql.query_rewriter.rewrite_semantic_input", rewrite)
    rewriter = QueryRewriter(graph, use_rust_rewriter=True, rust_no_fallback=True)
    sql = "SELECT total_revenue FROM metrics"
    assert rewriter.rewrite(sql) == "SELECT 1 AS from_rust"
    assert calls == [(graph, sql, {"input_dialect": "duckdb"})]
    assert rewriter.last_engine_selection["engine"] == "rust"


@pytest.mark.parametrize("error", [RuntimeError("boom"), ValueError("invalid model")])
@pytest.mark.parametrize("no_fallback", [True, False])
@pytest.mark.parametrize("method", ["rewrite", "explain"])
def test_unexpected_compiler_errors_propagate(monkeypatch, error, no_fallback, method):
    def fail(*args, **kwargs):
        raise error

    monkeypatch.setattr("sidemantic.sql.query_rewriter.rewrite_semantic_input", fail)
    rewriter = QueryRewriter(_graph(), use_rust_rewriter=True, rust_no_fallback=no_fallback)
    with pytest.raises(type(error), match=str(error)):
        getattr(rewriter, method)("SELECT revenue FROM orders")


@pytest.mark.parametrize(
    "error", [RustBackendUnavailableError("missing backend"), UnsupportedSemanticFeaturesError(["query.shape"])]
)
@pytest.mark.parametrize("no_fallback", [True, False])
def test_only_typed_failures_allow_fallback(monkeypatch, error, no_fallback):
    def fail(*args, **kwargs):
        raise error

    monkeypatch.setattr("sidemantic.sql.query_rewriter.rewrite_semantic_input", fail)
    rewriter = QueryRewriter(_graph(), use_rust_rewriter=True, rust_no_fallback=no_fallback)
    if no_fallback:
        with pytest.raises(type(error)):
            rewriter.rewrite("SELECT revenue FROM orders", strict=False)
    else:
        assert "SUM(" in rewriter.rewrite("SELECT revenue FROM orders")
        assert rewriter.last_engine_selection["engine"] == "python"
        assert str(error) in rewriter.last_engine_selection["reason"]


@pytest.mark.parametrize("method", ["rewrite", "explain"])
def test_strict_rust_cannot_enter_python_yardstick_path(method):
    rewriter = QueryRewriter(_graph(), use_rust_rewriter=True, rust_no_fallback=True)
    with pytest.raises(UnsupportedSemanticFeaturesError, match="yardstick"):
        getattr(rewriter, method)("SEMANTIC SELECT revenue FROM orders")


def test_security_fallback_does_not_disable_future_rust_queries(monkeypatch):
    monkeypatch.setattr("sidemantic.sql.query_rewriter.rewrite_semantic_input", lambda *a, **kw: "SELECT 7")
    rewriter = QueryRewriter(_graph(), use_rust_rewriter=True, rust_no_fallback=False)
    rewriter.rewrite("SELECT revenue FROM orders", user_attributes={})
    assert rewriter.last_engine_selection["engine"] == "python"
    assert rewriter.rewrite("SELECT revenue FROM orders") == "SELECT 7"
    assert rewriter.last_engine_selection["engine"] == "rust"


def test_explicit_python_overrides_legacy_environment(monkeypatch):
    monkeypatch.setenv("SIDEMANTIC_RS_REWRITER", "1")
    rewriter = QueryRewriter(_graph(), use_rust_rewriter=False)
    assert "SUM(" in rewriter.rewrite("SELECT revenue FROM orders")
    assert rewriter.last_engine_selection["engine"] == "python"


def test_strict_rust_preserves_query_only_validation():
    rewriter = QueryRewriter(_graph(), use_rust_rewriter=True, rust_no_fallback=True)
    with pytest.raises(ValueError, match="Only SELECT"):
        rewriter.rewrite("DELETE FROM orders")


@pytest.mark.parametrize("method", ["rewrite", "explain"])
def test_set_operations_reach_rust_before_python_rewrite(monkeypatch, method):
    sql = "SELECT revenue FROM orders UNION ALL SELECT revenue FROM orders"
    calls = []

    def rewrite(graph, query, **kwargs):
        calls.append(query)
        return "SELECT 42"

    monkeypatch.setattr("sidemantic.sql.query_rewriter.rewrite_semantic_input", rewrite)
    rewriter = QueryRewriter(_graph(), use_rust_rewriter=True, rust_no_fallback=True)
    result = getattr(rewriter, method)(sql)
    assert (result if method == "rewrite" else result.rewritten_sql) == "SELECT 42"
    assert calls == [sql]


def test_plain_query_explanation_does_not_claim_rust_compilation():
    rewriter = QueryRewriter(_graph(), use_rust_rewriter=True, rust_no_fallback=True)
    explanation = rewriter.explain("SELECT 1")
    assert explanation.chosen_plan == "passthrough_plain_sql"
    assert rewriter.last_engine_selection["engine"] == "passthrough"
