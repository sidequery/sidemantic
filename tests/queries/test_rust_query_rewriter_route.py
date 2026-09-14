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
    assert calls == [(graph, sql, {"input_dialect": "duckdb", "user_attributes": None, "enforce_visibility": False})]
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
def test_strict_rust_passes_yardstick_sql_to_rust(monkeypatch, method):
    calls = []

    def rewrite(graph, sql, **kwargs):
        calls.append(sql)
        return "SELECT 1 AS revenue"

    monkeypatch.setattr("sidemantic.sql.query_rewriter.rewrite_semantic_input", rewrite)
    rewriter = QueryRewriter(_graph(), use_rust_rewriter=True, rust_no_fallback=True)
    getattr(rewriter, method)("SEMANTIC SELECT revenue FROM orders")
    assert calls == ["SEMANTIC SELECT revenue FROM orders"]
    assert rewriter.last_engine_selection["engine"] == "rust"


@pytest.mark.parametrize(
    "sql,message",
    [("SELECT *", "requires a FROM clause"), ("SELECT FROM orders", "select at least one")],
)
def test_rust_route_validates_empty_projection_and_source(monkeypatch, sql, message):
    def reject(*args, **kwargs):
        raise AssertionError("Invalid query framing must be rejected before native compilation")

    monkeypatch.setattr("sidemantic.sql.query_rewriter.rewrite_semantic_input", reject)
    rewriter = QueryRewriter(_graph(), use_rust_rewriter=True, rust_no_fallback=True)
    with pytest.raises(ValueError, match=message):
        rewriter.rewrite(sql)
    assert rewriter.rewrite(sql, strict=False) == sql


def test_nonstrict_yardstick_validation_returns_original_sql(monkeypatch):
    def invalid(*args, **kwargs):
        raise ValueError("Unknown Yardstick measure revenue")

    monkeypatch.setattr("sidemantic.sql.query_rewriter.rewrite_semantic_input", invalid)
    rewriter = QueryRewriter(_graph(), use_rust_rewriter=True, rust_no_fallback=True)
    sql = "SELECT AGGREGATE(revenue) FROM orders"
    assert rewriter.rewrite(sql, strict=False) == sql
    assert rewriter.last_engine_selection["engine"] == "passthrough"


@pytest.mark.parametrize("query", ["SELECT AGGREGATE(revenue) FROM orders", "SELECT revenue FROM orders"])
@pytest.mark.parametrize("engine", ["python", "rust"])
def test_secured_transport_rejects_yardstick_before_native_compilation(monkeypatch, query, engine):
    from sidemantic import SecurityPolicy, SemanticLayer
    from sidemantic.core.semantic_layer import SecurityError

    def reject(*args, **kwargs):
        raise AssertionError("Secured Yardstick queries must not reach the compiler")

    monkeypatch.setattr("sidemantic.sql.query_rewriter.rewrite_semantic_input", reject)
    monkeypatch.setattr("sidemantic.core.semantic_layer.get_rust_module", lambda: object())
    layer = SemanticLayer(engine=engine, fallback=False, auto_register=False)
    model = _graph().models["orders"]
    model.metadata = {"yardstick": {}}
    model.security = SecurityPolicy(row_filters=["tenant = 1"])
    layer.add_model(model)
    try:
        with pytest.raises(SecurityError, match="Yardstick"):
            layer.sql(query)
    finally:
        layer.adapter.close()


def test_caller_context_reaches_rust_without_fallback(monkeypatch):
    calls = []

    def rewrite(*args, **kwargs):
        calls.append(kwargs)
        return "SELECT 7"

    monkeypatch.setattr("sidemantic.sql.query_rewriter.rewrite_semantic_input", rewrite)
    rewriter = QueryRewriter(_graph(), use_rust_rewriter=True, rust_no_fallback=False, enforce_visibility=True)
    attributes = {"subject": "O'Reilly"}
    assert rewriter.rewrite("SELECT orders.revenue FROM metrics", user_attributes=attributes) == "SELECT 7"
    assert calls == [{"input_dialect": "duckdb", "user_attributes": attributes, "enforce_visibility": True}]
    assert rewriter.last_engine_selection == {"engine": "rust", "reason": None}


@pytest.mark.parametrize("no_fallback", [True, False])
def test_security_errors_never_fall_back(monkeypatch, no_fallback):
    from sidemantic.core.semantic_layer import SecurityError

    def deny(*args, **kwargs):
        raise SecurityError("Access denied")

    monkeypatch.setattr("sidemantic.sql.query_rewriter.rewrite_semantic_input", deny)
    rewriter = QueryRewriter(_graph(), use_rust_rewriter=True, rust_no_fallback=no_fallback)
    with pytest.raises(SecurityError, match="Access denied"):
        rewriter.rewrite("SELECT orders.revenue FROM metrics", user_attributes={})
    assert rewriter.rust_fallback_reason is None


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


def test_old_extension_cannot_silently_drop_caller_context():
    from sidemantic.rust_bridge import rewrite_semantic_input

    class OldExtension:
        def rewrite_with_semantic_input(self, *args):
            pytest.fail("Legacy entrypoint must not receive a contextual request")

    with pytest.raises(RustBackendUnavailableError, match="rewrite_with_semantic_input_context"):
        rewrite_semantic_input(
            _graph(), "SELECT orders.revenue FROM metrics", user_attributes={}, rust_module=OldExtension()
        )


@pytest.mark.parametrize("context", ["attributes", "visibility", "security", "invariant", "none"])
def test_cte_collision_binding_belongs_to_contextual_rust(monkeypatch, context):
    from sidemantic.core.security import SecurityPolicy

    graph = _graph()
    if context == "security":
        graph.models["orders"].security = SecurityPolicy(access=True)
    if context == "invariant":
        graph.models["orders"].invariant_filters = ["status != 'deleted'"]
    calls = []

    def rewrite(graph, query, **kwargs):
        calls.append(query)
        return "SELECT 42"

    monkeypatch.setattr("sidemantic.sql.query_rewriter.rewrite_semantic_input", rewrite)
    rewriter = QueryRewriter(
        graph, use_rust_rewriter=True, rust_no_fallback=True, enforce_visibility=context == "visibility"
    )
    sql = "WITH orders_cte AS (SELECT orders.revenue FROM metrics) SELECT * FROM orders_cte"
    if context == "none":
        with pytest.raises(ValueError, match="conflicts with an internally generated name"):
            rewriter.rewrite(sql)
        assert calls == []
    else:
        assert rewriter.rewrite(sql, user_attributes={} if context == "attributes" else None) == "SELECT 42"
        assert calls == [sql]
