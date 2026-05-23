"""Tests for semantic SQL rewrite planning and explanations."""

import pytest

from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.pre_aggregation import PreAggregation
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_layer import SemanticLayer
from sidemantic.sql.query_rewriter import QueryRewriter
from tests.utils import fetch_dicts


@pytest.fixture
def semantic_layer():
    layer = SemanticLayer(auto_register=False)

    orders = Model(
        name="orders",
        table="orders",
        primary_key="id",
        dimensions=[
            Dimension(name="status", type="categorical", sql="status"),
            Dimension(name="order_date", type="time", sql="order_date", granularity="day"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
            Metric(name="count", agg="count"),
        ],
        relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
    )
    customers = Model(
        name="customers",
        table="customers",
        primary_key="id",
        dimensions=[
            Dimension(name="region", type="categorical", sql="region"),
            Dimension(name="tier", type="categorical", sql="tier"),
        ],
        metrics=[Metric(name="count", agg="count")],
        relationships=[Relationship(name="orders", type="one_to_many", foreign_key="customer_id")],
    )

    layer.add_model(orders)
    layer.add_model(customers)
    layer.conn.execute("""
        CREATE TABLE orders (
            id INTEGER,
            customer_id INTEGER,
            status VARCHAR,
            order_date DATE,
            amount DECIMAL(10, 2)
        )
    """)
    layer.conn.execute("""
        INSERT INTO orders VALUES
            (1, 1, 'completed', '2024-01-01', 100.00),
            (2, 1, 'completed', '2024-01-02', 150.00),
            (3, 2, 'pending', '2024-01-03', 200.00)
    """)
    layer.conn.execute("""
        CREATE TABLE customers (
            id INTEGER,
            region VARCHAR,
            tier VARCHAR
        )
    """)
    layer.conn.execute("""
        INSERT INTO customers VALUES
            (1, 'US', 'premium'),
            (2, 'EU', 'standard')
    """)
    return layer


def _candidate_by_name(explanation):
    return {candidate.name: candidate for candidate in explanation.candidate_plans}


def _rows(result):
    return fetch_dicts(result)


def test_explain_direct_semantic_query_is_machine_testable(semantic_layer):
    rewriter = QueryRewriter(semantic_layer.graph, dialect="duckdb")

    explanation = rewriter.explain("SELECT orders.revenue, orders.status FROM orders WHERE orders.status = 'completed'")

    assert explanation.chosen_plan == "direct_semantic"
    assert explanation.source_kind == "model"
    assert explanation.metrics == ["orders.revenue"]
    assert explanation.dimensions == ["orders.status"]
    assert explanation.filters == ["orders.status = 'completed'"]
    assert explanation.rewritten_sql == rewriter.rewrite(
        "SELECT orders.revenue, orders.status FROM orders WHERE orders.status = 'completed'"
    )


def test_explain_from_metrics_query_records_metrics_source(semantic_layer):
    rewriter = QueryRewriter(semantic_layer.graph, dialect="duckdb")

    explanation = rewriter.explain("SELECT orders.revenue, customers.region FROM metrics")

    assert explanation.chosen_plan == "direct_semantic"
    assert explanation.source_kind == "metrics"
    assert explanation.metrics == ["orders.revenue"]
    assert explanation.dimensions == ["customers.region"]


def test_explain_lists_deterministic_candidate_plans(semantic_layer):
    rewriter = QueryRewriter(semantic_layer.graph, dialect="duckdb")

    explanation = rewriter.explain("SELECT orders.revenue, customers.region FROM orders")
    candidates = _candidate_by_name(explanation)

    assert set(candidates) == {
        "direct_semantic",
        "semantic_plus_postprocess",
        "single_model_preaggregation",
        "fanout_preaggregation",
        "window_metric",
        "passthrough_plain_sql",
    }
    assert candidates["direct_semantic"].valid is True
    assert candidates["passthrough_plain_sql"].valid is False
    assert candidates["single_model_preaggregation"].details["reason"] in {
        "model_has_no_preaggregations",
        "not_single_model_query",
        "no_matching_preaggregation",
        "matching_preaggregation",
    }


def test_safe_outer_filter_pushdown_rewrites_to_direct_semantic(semantic_layer):
    rewriter = QueryRewriter(semantic_layer.graph, dialect="duckdb")
    wrapped_sql = "SELECT * FROM (SELECT orders.revenue, customers.region FROM orders) sq WHERE region = 'US'"
    direct_sql = "SELECT orders.revenue, customers.region FROM orders WHERE customers.region = 'US'"

    explanation = rewriter.explain(wrapped_sql)
    candidates = _candidate_by_name(explanation)

    assert explanation.chosen_plan == "direct_semantic"
    assert explanation.post_process is None
    assert explanation.metrics == ["orders.revenue"]
    assert explanation.dimensions == ["customers.region"]
    assert explanation.filters == ["customers.region = 'US'"]
    assert explanation.pushed_filters == ["customers.region = 'US'"]
    assert "safe_filter_pushdown" in explanation.applied_rules
    assert candidates["direct_semantic"].valid is True
    assert " AS sq WHERE" not in explanation.rewritten_sql
    assert explanation.rewritten_sql == rewriter.rewrite(wrapped_sql)
    assert _rows(semantic_layer.sql(wrapped_sql)) == _rows(semantic_layer.sql(direct_sql))


def test_safe_outer_filter_pushdown_from_cte_wrapper(semantic_layer):
    rewriter = QueryRewriter(semantic_layer.graph, dialect="duckdb")
    wrapped_sql = """
        WITH orders_agg AS (
            SELECT orders.revenue, orders.status FROM orders
        )
        SELECT * FROM orders_agg WHERE status = 'completed'
    """
    direct_sql = "SELECT orders.revenue, orders.status FROM orders WHERE orders.status = 'completed'"

    explanation = rewriter.explain(wrapped_sql)
    candidates = _candidate_by_name(explanation)

    assert explanation.chosen_plan == "direct_semantic"
    assert explanation.post_process is None
    assert explanation.metrics == ["orders.revenue"]
    assert explanation.dimensions == ["orders.status"]
    assert explanation.filters == ["orders.status = 'completed'"]
    assert explanation.pushed_filters == ["orders.status = 'completed'"]
    assert "safe_filter_pushdown" in explanation.applied_rules
    assert candidates["direct_semantic"].valid is True
    assert "orders_agg" not in explanation.rewritten_sql
    assert explanation.rewritten_sql == rewriter.rewrite(wrapped_sql)
    assert _rows(semantic_layer.sql(wrapped_sql)) == _rows(semantic_layer.sql(direct_sql))


def test_explain_cte_semantic_query_rejects_metric_filter_pushdown(semantic_layer):
    rewriter = QueryRewriter(semantic_layer.graph, dialect="duckdb")
    sql = """
        WITH orders_agg AS (
            SELECT orders.revenue, orders.status FROM orders
        )
        SELECT * FROM orders_agg WHERE revenue > 100
    """

    explanation = rewriter.explain(sql)
    candidates = _candidate_by_name(explanation)

    assert explanation.chosen_plan == "semantic_plus_postprocess"
    assert explanation.metrics == ["orders.revenue"]
    assert explanation.dimensions == ["orders.status"]
    assert len(explanation.semantic_scopes) == 1
    assert "metric or computed field" in explanation.rejected_rules["safe_filter_pushdown"]
    assert candidates["semantic_plus_postprocess"].valid is True
    assert explanation.rewritten_sql == rewriter.rewrite(sql)


def test_explain_multiple_semantic_ctes_reports_all_scopes(semantic_layer):
    rewriter = QueryRewriter(semantic_layer.graph, dialect="duckdb")
    sql = """
        WITH
        orders_agg AS (
            SELECT orders.revenue, orders.status FROM orders
        ),
        customers_agg AS (
            SELECT customers.count, customers.region FROM customers
        )
        SELECT *
        FROM orders_agg
        JOIN customers_agg ON orders_agg.status IS NOT NULL
    """

    explanation = rewriter.explain(sql)

    assert explanation.chosen_plan == "semantic_plus_postprocess"
    assert explanation.metrics == ["orders.revenue", "customers.count"]
    assert explanation.dimensions == ["orders.status", "customers.region"]
    assert len(explanation.semantic_scopes) == 2
    assert [scope.source_kind for scope in explanation.semantic_scopes] == ["model", "model"]
    assert explanation.rewritten_sql == rewriter.rewrite(sql)


def test_explain_root_semantic_query_preserves_user_cte(semantic_layer):
    rewriter = QueryRewriter(semantic_layer.graph, dialect="duckdb")
    sql = """
        WITH statuses AS (
            SELECT 'completed' AS status
        )
        SELECT orders.revenue
        FROM orders
        WHERE orders.status IN (SELECT status FROM statuses)
    """

    explanation = rewriter.explain(sql)

    assert explanation.chosen_plan == "direct_semantic"
    assert explanation.metrics == ["orders.revenue"]
    assert explanation.filters == ["orders.status IN (SELECT status FROM statuses)"]
    assert "WITH statuses AS" in explanation.rewritten_sql
    assert explanation.rewritten_sql == rewriter.rewrite(sql)


def test_explain_fanout_preaggregation_candidate(semantic_layer):
    rewriter = QueryRewriter(semantic_layer.graph, dialect="duckdb")

    explanation = rewriter.explain("SELECT orders.revenue, customers.count FROM orders")
    candidates = _candidate_by_name(explanation)

    assert explanation.chosen_plan == "fanout_preaggregation"
    assert explanation.fanout["eligible"] is True
    assert explanation.fanout["reason"] == "fanout_protection_required"
    assert candidates["fanout_preaggregation"].valid is True


def test_explain_window_metric_candidate(semantic_layer):
    semantic_layer.add_metric(
        Metric(
            name="running_total_revenue",
            type="cumulative",
            sql="orders.revenue",
        )
    )
    rewriter = QueryRewriter(semantic_layer.graph, dialect="duckdb")

    explanation = rewriter.explain("SELECT running_total_revenue, orders.order_date FROM metrics")
    candidates = _candidate_by_name(explanation)

    assert explanation.chosen_plan == "window_metric"
    assert candidates["window_metric"].valid is True
    assert candidates["window_metric"].details["metrics"] == ["running_total_revenue"]


def test_explain_single_model_preaggregation_candidate(semantic_layer):
    orders = semantic_layer.get_model("orders")
    orders.pre_aggregations = [
        PreAggregation(
            name="by_status",
            measures=["revenue"],
            dimensions=["status"],
        )
    ]
    rewriter = QueryRewriter(semantic_layer.graph, dialect="duckdb")

    explanation = rewriter.explain("SELECT orders.revenue, orders.status FROM orders")
    candidates = _candidate_by_name(explanation)

    assert explanation.chosen_plan == "direct_semantic"
    assert explanation.preaggregation["eligible"] is True
    assert explanation.preaggregation["reason"] == "matching_preaggregation"
    assert explanation.preaggregation["requires_enablement"] is True
    assert candidates["single_model_preaggregation"].valid is True


def test_explain_yardstick_route(monkeypatch, semantic_layer):
    rewriter = QueryRewriter(semantic_layer.graph, dialect="duckdb")
    monkeypatch.setattr(
        rewriter,
        "_rewrite_yardstick_query",
        lambda _sql, strict=True, allow_plain_measures=False: "SELECT 1 AS yardstick",
    )

    explanation = rewriter.explain("SEMANTIC SELECT AGGREGATE(revenue) AS revenue FROM sales_v")
    candidates = _candidate_by_name(explanation)

    assert explanation.chosen_plan == "yardstick_semantic_sql"
    assert explanation.source_kind == "yardstick"
    assert explanation.rewritten_sql == "SELECT 1 AS yardstick"
    assert candidates["yardstick_semantic_sql"].valid is True
    assert explanation.warnings == ["Yardstick semantic SQL uses a separate rewrite path."]


def test_explain_rust_rewriter_route(monkeypatch, semantic_layer):
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

    explanation = QueryRewriter(semantic_layer.graph, dialect="duckdb").explain("SELECT orders.revenue FROM orders")
    candidates = _candidate_by_name(explanation)

    assert explanation.chosen_plan == "rust_semantic_rewriter"
    assert explanation.source_kind == "rust"
    assert explanation.rewritten_sql == "SELECT 1 AS from_rust"
    assert candidates["rust_semantic_rewriter"].valid is True
    assert fake.calls == [("models: []", "SELECT orders.revenue FROM orders")]


def test_trivial_wrapper_uses_direct_semantic_plan(semantic_layer):
    rewriter = QueryRewriter(semantic_layer.graph, dialect="duckdb")
    wrapped_sql = "SELECT * FROM (SELECT orders.revenue, orders.status FROM orders) sq"
    direct_sql = "SELECT orders.revenue, orders.status FROM orders"

    explanation = rewriter.explain(wrapped_sql)

    assert explanation.chosen_plan == "direct_semantic"
    assert explanation.post_process is None
    assert explanation.rewritten_sql == rewriter.rewrite(direct_sql)
    assert rewriter.rewrite(wrapped_sql) == rewriter.rewrite(direct_sql)
    assert "trivial_wrapper_flattening" in explanation.applied_rules
    assert "wrapper_flattening" in explanation.applied_rules


def test_safe_outer_order_limit_offset_pushdown(semantic_layer):
    rewriter = QueryRewriter(semantic_layer.graph, dialect="duckdb")
    wrapped_sql = """
        SELECT *
        FROM (SELECT orders.revenue, orders.status FROM orders) sq
        ORDER BY status DESC
        LIMIT 1
        OFFSET 1
    """
    direct_sql = """
        SELECT orders.revenue, orders.status
        FROM orders
        ORDER BY orders.status DESC
        LIMIT 1
        OFFSET 1
    """

    explanation = rewriter.explain(wrapped_sql)

    assert explanation.chosen_plan == "direct_semantic"
    assert explanation.post_process is None
    assert explanation.order_by == ["status DESC"]
    assert explanation.limit == 1
    assert explanation.offset == 1
    assert "safe_order_pushdown" in explanation.applied_rules
    assert "safe_limit_pushdown" in explanation.applied_rules
    assert " AS sq" not in explanation.rewritten_sql
    assert explanation.rewritten_sql == rewriter.rewrite(wrapped_sql)
    assert _rows(semantic_layer.sql(wrapped_sql)) == _rows(semantic_layer.sql(direct_sql))


def test_wrapper_projection_flattening_aliases_without_changing_grouping(semantic_layer):
    rewriter = QueryRewriter(semantic_layer.graph, dialect="duckdb")
    wrapped_sql = """
        SELECT status, revenue AS total_revenue
        FROM (SELECT orders.revenue, orders.count, orders.status FROM orders) sq
        ORDER BY status
    """
    direct_sql = """
        SELECT orders.status, orders.revenue AS total_revenue
        FROM orders
        ORDER BY orders.status
    """

    explanation = rewriter.explain(wrapped_sql)

    assert explanation.chosen_plan == "direct_semantic"
    assert explanation.post_process is None
    assert explanation.metrics == ["orders.revenue"]
    assert explanation.dimensions == ["orders.status"]
    assert explanation.aliases == {"orders.revenue": "total_revenue"}
    assert "wrapper_projection_flattening" in explanation.applied_rules
    assert "safe_order_pushdown" in explanation.applied_rules
    assert "total_revenue" in explanation.rewritten_sql
    assert "count" not in explanation.rewritten_sql
    assert " AS sq" not in explanation.rewritten_sql
    assert explanation.rewritten_sql == rewriter.rewrite(wrapped_sql)
    assert _rows(semantic_layer.sql(wrapped_sql)) == _rows(semantic_layer.sql(direct_sql))


def test_wrapped_preaggregation_route_selection(semantic_layer):
    orders = semantic_layer.get_model("orders")
    orders.pre_aggregations = [
        PreAggregation(
            name="by_status",
            measures=["revenue"],
            dimensions=["status"],
        )
    ]
    rewriter = QueryRewriter(semantic_layer.graph, dialect="duckdb", use_preaggregations=True)
    wrapped_sql = "SELECT * FROM (SELECT orders.revenue, orders.status FROM orders) sq"

    explanation = rewriter.explain(wrapped_sql)
    candidates = _candidate_by_name(explanation)

    assert explanation.chosen_plan == "single_model_preaggregation"
    assert explanation.preaggregation["eligible"] is True
    assert explanation.preaggregation["enabled"] is True
    assert "preaggregation_route_selection" in explanation.applied_rules
    assert candidates["single_model_preaggregation"].valid is True
    assert "orders_preagg_by_status" in explanation.rewritten_sql
    assert " AS sq" not in explanation.rewritten_sql


def test_wrapped_preaggregation_executes_against_materialized_table(semantic_layer):
    orders = semantic_layer.get_model("orders")
    orders.pre_aggregations = [
        PreAggregation(
            name="by_status",
            measures=["revenue"],
            dimensions=["status"],
        )
    ]
    semantic_layer.use_preaggregations = True
    semantic_layer.conn.execute("""
        CREATE TABLE orders_preagg_by_status AS
        SELECT
            status,
            SUM(amount) AS revenue_raw
        FROM orders
        GROUP BY status
    """)
    wrapped_sql = """
        SELECT *
        FROM (SELECT orders.revenue, orders.status FROM orders) sq
        ORDER BY status
    """

    explanation = semantic_layer.explain_sql(wrapped_sql)
    preagg_rows = _rows(semantic_layer.sql(wrapped_sql))
    base_rows = _rows(
        semantic_layer.query(
            metrics=["orders.revenue"],
            dimensions=["orders.status"],
            order_by=["status"],
            use_preaggregations=False,
        )
    )

    assert explanation.chosen_plan == "single_model_preaggregation"
    assert "orders_preagg_by_status" in explanation.rewritten_sql
    assert " AS sq" not in explanation.rewritten_sql
    assert preagg_rows == base_rows


def test_wrapped_fanout_strategy_selection(semantic_layer):
    rewriter = QueryRewriter(semantic_layer.graph, dialect="duckdb")
    wrapped_sql = "SELECT * FROM (SELECT orders.revenue, customers.count FROM orders) sq"

    explanation = rewriter.explain(wrapped_sql)
    candidates = _candidate_by_name(explanation)

    assert explanation.chosen_plan == "fanout_preaggregation"
    assert explanation.post_process is None
    assert explanation.fanout["eligible"] is True
    assert explanation.fanout["reason"] == "fanout_protection_required"
    assert "fanout_strategy_selection" in explanation.applied_rules
    assert candidates["fanout_preaggregation"].valid is True
    assert " AS sq" not in explanation.rewritten_sql


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM (SELECT orders.revenue, orders.status FROM orders) sq WHERE revenue > 100",
        "SELECT revenue * 2 AS doubled FROM (SELECT orders.revenue, orders.status FROM orders) sq",
        "SELECT revenue, ROW_NUMBER() OVER () AS rn FROM (SELECT orders.revenue, orders.status FROM orders) sq",
        "SELECT SUM(revenue) AS total FROM (SELECT orders.revenue, orders.status FROM orders) sq",
        "SELECT DISTINCT status FROM (SELECT orders.revenue, orders.status FROM orders) sq",
        "SELECT revenue FROM (SELECT orders.revenue, orders.status FROM orders) sq",
        "SELECT * FROM (SELECT orders.revenue, orders.status FROM orders LIMIT 1) sq WHERE status = 'completed'",
        "SELECT * FROM (SELECT orders.revenue, orders.status FROM orders) sq JOIN other_table ot ON TRUE",
        """
        WITH passthrough AS (
            SELECT 1 AS marker
        )
        SELECT * FROM (SELECT orders.revenue, orders.status FROM orders) sq
        """,
    ],
)
def test_wrapped_optimizer_negative_cases(semantic_layer, sql):
    rewriter = QueryRewriter(semantic_layer.graph, dialect="duckdb")

    explanation = rewriter.explain(sql)

    assert explanation.chosen_plan == "semantic_plus_postprocess"
    assert explanation.post_process is not None
    assert explanation.rejected_rules
    assert explanation.rewritten_sql == rewriter.rewrite(sql)


def test_explain_passthrough_plain_sql_non_strict(semantic_layer):
    rewriter = QueryRewriter(semantic_layer.graph, dialect="duckdb")

    explanation = rewriter.explain("SHOW TABLES", strict=False)

    assert explanation.chosen_plan == "passthrough_plain_sql"
    assert explanation.source_kind == "plain_sql"
    assert explanation.rewritten_sql == "SHOW TABLES"


def test_semantic_layer_explain_sql(semantic_layer):
    explanation = semantic_layer.explain_sql("SELECT orders.revenue FROM orders")

    assert explanation.chosen_plan == "direct_semantic"
    assert explanation.metrics == ["orders.revenue"]
    assert "orders_cte" in explanation.rewritten_sql


def test_rewrite_simple_query_still_generates_same_sql(semantic_layer):
    rewriter = QueryRewriter(semantic_layer.graph, dialect="duckdb")
    sql = "SELECT orders.revenue, orders.status FROM orders ORDER BY orders.status DESC LIMIT 10"

    assert rewriter.explain(sql).rewritten_sql == rewriter.rewrite(sql)


def test_explanation_can_be_serialized_to_dict(semantic_layer):
    rewriter = QueryRewriter(semantic_layer.graph, dialect="duckdb")

    explanation_dict = rewriter.explain("SELECT orders.revenue FROM orders").to_dict()

    assert explanation_dict["chosen_plan"] == "direct_semantic"
    assert explanation_dict["semantic_scopes"][0]["metrics"] == ["orders.revenue"]
    assert explanation_dict["candidate_plans"][0]["name"] == "direct_semantic"
