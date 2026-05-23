"""Tests for semantic SQL rewrite planning and explanations."""

import pytest

from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.pre_aggregation import PreAggregation
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_layer import SemanticLayer
from sidemantic.sql.query_rewriter import QueryRewriter
from tests.utils import fetch_columns, fetch_dicts, fetch_rows


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


def _subquery(sql: str) -> str:
    return "(\n" + sql.rstrip() + "\n)"


def _compiled_semantic_sql(layer: SemanticLayer, sql: str, use_preaggregations: bool = False) -> str:
    return QueryRewriter(
        layer.graph,
        dialect="duckdb",
        use_preaggregations=use_preaggregations,
    ).rewrite(sql)


def _sorted_rows(rows):
    return sorted(rows, key=repr)


def _assert_query_matches_baseline(
    layer: SemanticLayer,
    sql: str,
    baseline_sql: str,
    ordered: bool = False,
):
    explanation = layer.explain_sql(sql)
    optimized_result = layer.sql(sql)
    optimized_columns = fetch_columns(optimized_result)
    optimized_rows = fetch_rows(optimized_result)
    baseline_result = layer.conn.execute(baseline_sql)
    baseline_columns = fetch_columns(baseline_result)
    baseline_rows = fetch_rows(baseline_result)

    assert optimized_columns == baseline_columns
    if ordered:
        assert optimized_rows == baseline_rows
    else:
        assert _sorted_rows(optimized_rows) == _sorted_rows(baseline_rows)

    return explanation


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
        "join_key_preaggregation",
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


def test_positive_wrapper_rewrites_match_unoptimized_baselines(semantic_layer):
    orders_revenue_status = _compiled_semantic_sql(
        semantic_layer,
        "SELECT orders.revenue, orders.status FROM orders",
    )
    orders_revenue_region = _compiled_semantic_sql(
        semantic_layer,
        "SELECT orders.revenue, customers.region FROM orders",
    )

    cases = [
        {
            "sql": "SELECT * FROM (SELECT orders.revenue, customers.region FROM orders) sq WHERE region = 'US'",
            "baseline": "SELECT * FROM " + _subquery(orders_revenue_region) + " sq WHERE region = 'US'",
            "ordered": False,
            "rule": "safe_filter_pushdown",
        },
        {
            "sql": """
                WITH orders_agg AS (
                    SELECT orders.revenue, orders.status FROM orders
                )
                SELECT * FROM orders_agg WHERE status = 'completed'
            """,
            "baseline": "WITH orders_agg AS "
            + _subquery(orders_revenue_status)
            + " SELECT * FROM orders_agg WHERE status = 'completed'",
            "ordered": False,
            "rule": "safe_filter_pushdown",
        },
        {
            "sql": """
                SELECT *
                FROM (SELECT orders.revenue, orders.status FROM orders) sq
                ORDER BY status DESC
                LIMIT 1
                OFFSET 1
            """,
            "baseline": "SELECT * FROM "
            + _subquery(orders_revenue_status)
            + " sq ORDER BY status DESC LIMIT 1 OFFSET 1",
            "ordered": True,
            "rule": "safe_order_pushdown",
        },
        {
            "sql": """
                SELECT status, revenue AS total_revenue
                FROM (SELECT orders.revenue, orders.status FROM orders) sq
                ORDER BY status
            """,
            "baseline": "SELECT status, revenue AS total_revenue FROM "
            + _subquery(orders_revenue_status)
            + " sq ORDER BY status",
            "ordered": True,
            "rule": "wrapper_projection_flattening",
        },
    ]

    for case in cases:
        explanation = _assert_query_matches_baseline(
            semantic_layer,
            case["sql"],
            case["baseline"],
            ordered=case["ordered"],
        )
        assert case["rule"] in explanation.applied_rules
        assert explanation.post_process is None


def test_external_bi_wrapper_corpus_positive_cases(semantic_layer):
    orders_revenue_status_aliases = _compiled_semantic_sql(
        semantic_layer,
        'SELECT orders.revenue AS "Total Revenue", orders.status AS "Order Status" FROM orders',
    )
    orders_revenue_status = _compiled_semantic_sql(
        semantic_layer,
        "SELECT orders.revenue, orders.status FROM orders",
    )
    from_metrics = _compiled_semantic_sql(
        semantic_layer,
        "SELECT orders.revenue, customers.region FROM metrics",
    )

    cases = [
        {
            "name": "tableau_custom_sql",
            "sql": """
                SELECT "Order Status" AS status, "Total Revenue" AS total_revenue
                FROM (
                    SELECT orders.revenue AS "Total Revenue", orders.status AS "Order Status" FROM orders
                ) "Custom SQL Query"
                WHERE "Order Status" = 'completed'
                ORDER BY "Total Revenue" DESC
            """,
            "baseline": 'SELECT "Order Status" AS status, "Total Revenue" AS total_revenue FROM '
            + _subquery(orders_revenue_status_aliases)
            + ' "Custom SQL Query" WHERE "Order Status" = \'completed\' ORDER BY "Total Revenue" DESC',
            "ordered": True,
        },
        {
            "name": "power_query_native_query",
            "sql": """
                SELECT "_"."status", "_"."revenue"
                FROM (SELECT orders.revenue, orders.status FROM orders) AS "_"
                WHERE "_"."status" = 'completed'
            """,
            "baseline": 'SELECT "_"."status", "_"."revenue" FROM '
            + _subquery(orders_revenue_status)
            + ' AS "_" WHERE "_"."status" = \'completed\'',
            "ordered": False,
        },
        {
            "name": "superset_virtual_dataset",
            "sql": """
                SELECT status, revenue
                FROM (SELECT orders.revenue, orders.status FROM orders) AS virtual_table
                WHERE status = 'completed'
            """,
            "baseline": "SELECT status, revenue FROM "
            + _subquery(orders_revenue_status)
            + " AS virtual_table WHERE status = 'completed'",
            "ordered": False,
        },
        {
            "name": "metabase_saved_question_cte",
            "sql": """
                WITH question_42 AS (
                    SELECT orders.revenue, orders.status FROM orders
                )
                SELECT status, revenue FROM question_42 WHERE status = 'completed'
            """,
            "baseline": "WITH question_42 AS "
            + _subquery(orders_revenue_status)
            + " SELECT status, revenue FROM question_42 WHERE status = 'completed'",
            "ordered": False,
        },
        {
            "name": "from_metrics_wrapper",
            "sql": """
                SELECT region AS market, revenue AS total_revenue
                FROM (SELECT orders.revenue, customers.region FROM metrics) metric_source
                WHERE region = 'US'
                ORDER BY total_revenue DESC
            """,
            "baseline": "SELECT region AS market, revenue AS total_revenue FROM "
            + _subquery(from_metrics)
            + " metric_source WHERE region = 'US' ORDER BY total_revenue DESC",
            "ordered": True,
        },
    ]

    for case in cases:
        explanation = _assert_query_matches_baseline(
            semantic_layer,
            case["sql"],
            case["baseline"],
            ordered=case["ordered"],
        )
        assert explanation.chosen_plan in {"direct_semantic", "fanout_preaggregation"}
        assert explanation.post_process is None
        assert "wrapper_flattening" in explanation.applied_rules


def test_bi_corpus_acceptance_matrix_records_expected_routes(semantic_layer):
    cases = [
        {
            "name": "tableau_joined_custom_sql",
            "sql": """
                SELECT custom_sql.status, labels.label
                FROM (SELECT orders.revenue, orders.status FROM orders) custom_sql
                JOIN (SELECT 'completed' AS status, 'Closed' AS label) labels
                  ON labels.status = custom_sql.status
            """,
            "baseline": "SELECT custom_sql.status, labels.label FROM "
            + _subquery(_compiled_semantic_sql(semantic_layer, "SELECT orders.revenue, orders.status FROM orders"))
            + " custom_sql JOIN (SELECT 'completed' AS status, 'Closed' AS label) labels"
            + " ON labels.status = custom_sql.status",
            "route": "semantic_plus_postprocess",
            "rule": "semantic_island_optimization",
        },
        {
            "name": "power_query_projection_pruning",
            "sql": """
                SELECT "_"."status"
                FROM (SELECT orders.revenue, orders.status FROM orders) AS "_"
                WHERE "_"."status" = 'completed'
            """,
            "baseline": 'SELECT "_"."status" FROM '
            + _subquery(_compiled_semantic_sql(semantic_layer, "SELECT orders.revenue, orders.status FROM orders"))
            + ' AS "_" WHERE "_"."status" = \'completed\'',
            "route": "direct_semantic",
            "rule": "wrapper_projection_flattening",
        },
        {
            "name": "metabase_field_filter_in",
            "sql": """
                WITH question_42 AS (
                    SELECT orders.revenue, orders.status FROM orders
                )
                SELECT status, revenue
                FROM question_42
                WHERE status IN ('completed', 'pending')
            """,
            "baseline": "WITH question_42 AS "
            + _subquery(_compiled_semantic_sql(semantic_layer, "SELECT orders.revenue, orders.status FROM orders"))
            + " SELECT status, revenue FROM question_42 WHERE status IN ('completed', 'pending')",
            "route": "direct_semantic",
            "rule": "safe_filter_pushdown",
        },
        {
            "name": "hex_chained_semantic_and_raw_branch",
            "sql": """
                SELECT status, revenue
                FROM (SELECT orders.revenue, orders.status FROM orders) semantic_branch
                UNION ALL
                SELECT 'raw' AS status, 0 AS revenue
            """,
            "baseline": "SELECT status, revenue FROM "
            + _subquery(_compiled_semantic_sql(semantic_layer, "SELECT orders.revenue, orders.status FROM orders"))
            + " semantic_branch UNION ALL SELECT 'raw' AS status, 0 AS revenue",
            "route": "semantic_plus_postprocess",
            "rule": "set_operation_branch_optimization",
        },
        {
            "name": "sigma_custom_sql_workbook_filter",
            "sql": """
                SELECT status, revenue
                FROM (SELECT orders.revenue, orders.status FROM orders) workbook_sql
                WHERE status = 'completed'
                ORDER BY revenue DESC
            """,
            "baseline": "SELECT status, revenue FROM "
            + _subquery(_compiled_semantic_sql(semantic_layer, "SELECT orders.revenue, orders.status FROM orders"))
            + " workbook_sql WHERE status = 'completed' ORDER BY revenue DESC",
            "route": "direct_semantic",
            "rule": "safe_filter_pushdown",
            "ordered": True,
        },
        {
            "name": "superset_time_and_rls_filter",
            "sql": """
                SELECT order_date__day, status, revenue
                FROM (
                    SELECT orders.revenue, orders.status, orders.order_date__day FROM orders
                ) virtual_table
                WHERE order_date__day >= DATE '2024-01-01'
                  AND order_date__day < DATE '2024-02-01'
                  AND status = 'completed'
            """,
            "baseline": "SELECT order_date__day, status, revenue FROM "
            + _subquery(
                _compiled_semantic_sql(
                    semantic_layer,
                    "SELECT orders.revenue, orders.status, orders.order_date__day FROM orders",
                )
            )
            + " virtual_table WHERE order_date__day >= DATE '2024-01-01'"
            + " AND order_date__day < DATE '2024-02-01'"
            + " AND status = 'completed'",
            "route": "direct_semantic",
            "rule": "safe_filter_pushdown",
        },
    ]

    for case in cases:
        explanation = _assert_query_matches_baseline(
            semantic_layer,
            case["sql"],
            case["baseline"],
            ordered=case.get("ordered", False),
        )

        assert explanation.chosen_plan == case["route"], case["name"]
        assert case["rule"] in explanation.applied_rules, case["name"]


@pytest.mark.parametrize(
    ("name", "sql", "rejected_rule"),
    [
        (
            "tableau_computed_projection",
            "SELECT status || 'x' AS status_x FROM (SELECT orders.status FROM orders) sq",
            "wrapper_flattening",
        ),
        (
            "power_query_non_foldable_transform",
            """
                SELECT COALESCE(status, 'unknown') AS status_bucket, SUM(revenue) AS revenue
                FROM (SELECT orders.status, orders.revenue FROM orders) sq
                GROUP BY 1
            """,
            "aggregate_boundary_rollup",
        ),
        (
            "power_bi_distinct_count_subtotal",
            """
                SELECT status, SUM(unique_customers) AS unique_customers
                FROM (
                    SELECT orders.unique_customers, orders.status, orders.order_date FROM orders
                ) sq
                GROUP BY status
            """,
            "aggregate_boundary_rollup",
        ),
        (
            "superset_mixed_or_filter",
            """
                SELECT *
                FROM (SELECT orders.revenue, orders.status FROM orders) sq
                WHERE status = 'completed' OR revenue > 100
            """,
            "safe_filter_pushdown",
        ),
    ],
)
def test_bi_corpus_rejection_matrix_records_expected_reasons(semantic_layer, name, sql, rejected_rule):
    if name == "power_bi_distinct_count_subtotal":
        orders = semantic_layer.get_model("orders")
        orders.metrics.append(Metric(name="unique_customers", agg="count_distinct", sql="customer_id"))

    explanation = semantic_layer.explain_sql(sql)

    assert explanation.chosen_plan == "semantic_plus_postprocess"
    assert rejected_rule in explanation.rejected_rules
    assert explanation.semantic_islands or explanation.post_process is not None


def test_external_cte_chain_corpus_flattens_linear_steps(semantic_layer):
    sql = """
        WITH base AS (
            SELECT orders.revenue, orders.status FROM orders
        ),
        filtered AS (
            SELECT * FROM base WHERE status = 'completed'
        ),
        projected AS (
            SELECT status, revenue FROM filtered
        )
        SELECT status, revenue FROM projected ORDER BY revenue DESC LIMIT 1
    """
    baseline_sql = (
        "WITH base AS "
        + _subquery(_compiled_semantic_sql(semantic_layer, "SELECT orders.revenue, orders.status FROM orders"))
        + ", filtered AS (SELECT * FROM base WHERE status = 'completed'), "
        + "projected AS (SELECT status, revenue FROM filtered) "
        + "SELECT status, revenue FROM projected ORDER BY revenue DESC LIMIT 1"
    )

    explanation = _assert_query_matches_baseline(semantic_layer, sql, baseline_sql, ordered=True)
    candidates = _candidate_by_name(explanation)

    assert explanation.chosen_plan == "direct_semantic"
    assert explanation.post_process is None
    assert explanation.filters == ["orders.status = 'completed'"]
    assert explanation.order_by == ["revenue DESC"]
    assert explanation.limit == 1
    assert "linear_cte_chain_flattening" in explanation.applied_rules
    assert "safe_filter_pushdown" in explanation.applied_rules
    assert "safe_order_pushdown" in explanation.applied_rules
    assert "safe_limit_pushdown" in explanation.applied_rules
    assert candidates["linear_cte_chain_flattening"].valid is True
    assert "WITH base" not in explanation.rewritten_sql


@pytest.mark.parametrize(
    "sql",
    [
        """
        WITH base AS (
            SELECT orders.revenue, orders.status FROM orders
        ),
        filtered AS (
            SELECT * FROM base
        )
        SELECT a.status FROM filtered a JOIN filtered b ON a.status = b.status
        """,
        """
        WITH base AS (
            SELECT orders.revenue, orders.status FROM orders
        ),
        projected AS (
            SELECT status, revenue * 2 AS doubled FROM base
        )
        SELECT * FROM projected
        """,
        """
        WITH base AS (
            SELECT orders.revenue, orders.status FROM orders
        ),
        ranked AS (
            SELECT status, revenue, ROW_NUMBER() OVER (ORDER BY revenue DESC) AS rn FROM base
        )
        SELECT status, revenue FROM ranked
        """,
        """
        WITH base AS (
            SELECT orders.revenue, orders.status FROM orders
        ),
        distinct_statuses AS (
            SELECT DISTINCT status FROM base
        )
        SELECT * FROM distinct_statuses
        """,
        """
        WITH base AS (
            SELECT orders.revenue, orders.status FROM orders
        ),
        rolled AS (
            SELECT status, SUM(revenue) AS revenue FROM base GROUP BY status
        )
        SELECT * FROM rolled
        """,
    ],
)
def test_linear_cte_chain_rejects_unsafe_steps(semantic_layer, sql):
    explanation = semantic_layer.explain_sql(sql)

    assert explanation.chosen_plan == "semantic_plus_postprocess"
    assert explanation.post_process is not None
    assert "linear_cte_chain_flattening" in explanation.rejected_rules


@pytest.mark.parametrize(
    "predicate",
    [
        "status IN ('completed', 'pending')",
        "status NOT IN ('pending')",
        "order_date BETWEEN DATE '2024-01-01' AND DATE '2024-01-02'",
        "status IS NOT NULL",
        "status LIKE 'comp%'",
        "NOT (status = 'pending')",
        "status = 'completed' OR status = 'pending'",
        "sq.status = 'completed'",
    ],
)
def test_outer_dimension_filter_pushdown_predicate_matrix(semantic_layer, predicate):
    inner_sql = "SELECT orders.revenue, orders.status, orders.order_date FROM orders"
    wrapped_sql = f"SELECT * FROM ({inner_sql}) sq WHERE {predicate}"
    baseline_sql = (
        "SELECT * FROM " + _subquery(_compiled_semantic_sql(semantic_layer, inner_sql)) + f" sq WHERE {predicate}"
    )

    explanation = _assert_query_matches_baseline(semantic_layer, wrapped_sql, baseline_sql)

    assert explanation.chosen_plan == "direct_semantic"
    assert "safe_filter_pushdown" in explanation.applied_rules


@pytest.mark.parametrize(
    ("outer_order", "ordered"),
    [
        ("status ASC", True),
        ("total_revenue DESC", True),
        ("sq.status DESC", True),
        ("sq.total_revenue DESC", True),
    ],
)
def test_outer_order_limit_pushdown_matrix(semantic_layer, outer_order, ordered):
    inner_sql = "SELECT orders.revenue AS total_revenue, orders.status FROM orders"
    wrapped_sql = f"""
        SELECT status, total_revenue
        FROM ({inner_sql}) sq
        ORDER BY {outer_order}
        LIMIT 2
    """
    baseline_sql = (
        "SELECT status, total_revenue FROM "
        + _subquery(_compiled_semantic_sql(semantic_layer, inner_sql))
        + f" sq ORDER BY {outer_order} LIMIT 2"
    )

    explanation = _assert_query_matches_baseline(semantic_layer, wrapped_sql, baseline_sql, ordered=ordered)

    assert explanation.chosen_plan == "direct_semantic"
    assert "safe_order_pushdown" in explanation.applied_rules
    assert "safe_limit_pushdown" in explanation.applied_rules


def test_explain_cte_semantic_query_pushes_metric_filter_to_having(semantic_layer):
    rewriter = QueryRewriter(semantic_layer.graph, dialect="duckdb")
    sql = """
        WITH orders_agg AS (
            SELECT orders.revenue, orders.status FROM orders
        )
        SELECT * FROM orders_agg WHERE revenue > 225
    """
    baseline_sql = (
        "SELECT * FROM "
        + _subquery(_compiled_semantic_sql(semantic_layer, "SELECT orders.revenue, orders.status FROM orders"))
        + " orders_agg WHERE revenue > 225"
    )

    explanation = _assert_query_matches_baseline(semantic_layer, sql, baseline_sql)
    candidates = _candidate_by_name(explanation)

    assert explanation.chosen_plan == "direct_semantic"
    assert explanation.metrics == ["orders.revenue"]
    assert explanation.dimensions == ["orders.status"]
    assert explanation.filters == ["orders.revenue > 225"]
    assert explanation.row_filters == []
    assert explanation.aggregate_filters == ["orders.revenue > 225"]
    assert len(explanation.semantic_scopes) == 1
    assert "safe_metric_filter_having_pushdown" in explanation.applied_rules
    assert candidates["direct_semantic"].valid is True
    assert "HAVING" in explanation.rewritten_sql
    assert "orders_agg" not in explanation.rewritten_sql
    assert explanation.rewritten_sql == rewriter.rewrite(sql)


def test_wrapper_mixed_and_filter_splits_row_and_metric_stages(semantic_layer):
    sql = """
        SELECT *
        FROM (SELECT orders.revenue, orders.status FROM orders) sq
        WHERE status = 'completed' AND revenue > 225
    """
    baseline_sql = (
        "SELECT * FROM "
        + _subquery(_compiled_semantic_sql(semantic_layer, "SELECT orders.revenue, orders.status FROM orders"))
        + " sq WHERE status = 'completed' AND revenue > 225"
    )

    explanation = _assert_query_matches_baseline(semantic_layer, sql, baseline_sql)

    assert explanation.chosen_plan == "direct_semantic"
    assert explanation.row_filters == ["orders.status = 'completed'"]
    assert explanation.aggregate_filters == ["orders.revenue > 225"]
    assert "safe_filter_pushdown" in explanation.applied_rules
    assert "safe_metric_filter_having_pushdown" in explanation.applied_rules
    assert "WHERE status = 'completed'" in explanation.rewritten_sql
    assert "HAVING" in explanation.rewritten_sql


def test_wrapper_mixed_or_filter_stays_postprocess(semantic_layer):
    rewriter = QueryRewriter(semantic_layer.graph, dialect="duckdb")
    sql = """
        SELECT *
        FROM (SELECT orders.revenue, orders.status FROM orders) sq
        WHERE status = 'completed' OR revenue > 225
    """

    explanation = rewriter.explain(sql)

    assert explanation.chosen_plan == "semantic_plus_postprocess"
    assert explanation.post_process is not None
    assert explanation.rejected_rules["safe_filter_pushdown"] == (
        "safe_filter_pushdown cannot split mixed metric/dimension OR predicate"
    )


def test_wrapper_metric_filter_over_hidden_metric_stays_postprocess(semantic_layer):
    rewriter = QueryRewriter(semantic_layer.graph, dialect="duckdb")
    sql = """
        SELECT status
        FROM (SELECT orders.revenue, orders.status FROM orders) sq
        WHERE revenue > 225
    """

    explanation = rewriter.explain(sql)

    assert explanation.chosen_plan == "semantic_plus_postprocess"
    assert explanation.post_process is not None
    assert explanation.rejected_rules["safe_filter_pushdown"] == (
        "safe_filter_pushdown cannot move unprojected or computed field 'revenue'"
    )


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


def test_multiple_semantic_cte_islands_use_preaggregations(semantic_layer):
    orders = semantic_layer.get_model("orders")
    orders.pre_aggregations = [
        PreAggregation(
            name="by_status",
            measures=["revenue"],
            dimensions=["status"],
        )
    ]
    customers = semantic_layer.get_model("customers")
    customers.pre_aggregations = [
        PreAggregation(
            name="by_region",
            measures=["count"],
            dimensions=["region"],
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
    semantic_layer.conn.execute("""
        CREATE TABLE customers_preagg_by_region AS
        SELECT
            region,
            COUNT(*) AS count_raw
        FROM customers
        GROUP BY region
    """)
    sql = """
        WITH
        orders_agg AS (
            SELECT orders.revenue, orders.status FROM orders
        ),
        customers_agg AS (
            SELECT customers.count, customers.region FROM customers
        )
        SELECT o.status, c.region, o.revenue, c.count
        FROM orders_agg o
        JOIN customers_agg c ON o.status IS NOT NULL
        ORDER BY o.status, c.region
    """
    baseline_sql = (
        "WITH orders_agg AS "
        + _subquery(
            _compiled_semantic_sql(
                semantic_layer,
                "SELECT orders.revenue, orders.status FROM orders",
                use_preaggregations=False,
            )
        )
        + ", customers_agg AS "
        + _subquery(
            _compiled_semantic_sql(
                semantic_layer,
                "SELECT customers.count, customers.region FROM customers",
                use_preaggregations=False,
            )
        )
        + " SELECT o.status, c.region, o.revenue, c.count"
        + " FROM orders_agg o JOIN customers_agg c ON o.status IS NOT NULL"
        + " ORDER BY o.status, c.region"
    )

    explanation = _assert_query_matches_baseline(semantic_layer, sql, baseline_sql, ordered=True)

    assert explanation.chosen_plan == "semantic_plus_postprocess"
    assert explanation.post_process is not None
    assert len(explanation.semantic_islands) == 2
    assert {island["name"] for island in explanation.semantic_islands} == {"orders_agg", "customers_agg"}
    assert {island["chosen_plan"] for island in explanation.semantic_islands} == {"single_model_preaggregation"}
    assert "semantic_island_optimization" in explanation.applied_rules
    assert explanation.rewritten_sql.count("orders_preagg_by_status") == 1
    assert explanation.rewritten_sql.count("customers_preagg_by_region") == 1
    assert "JOIN customers_agg AS c" in explanation.rewritten_sql


def test_one_semantic_cte_island_preserves_plain_cte(semantic_layer):
    sql = """
        WITH
        orders_agg AS (
            SELECT orders.revenue, orders.status FROM orders
        ),
        labels AS (
            SELECT 'completed' AS status, 'Closed' AS label
        )
        SELECT labels.label, orders_agg.revenue
        FROM orders_agg
        JOIN labels ON labels.status = orders_agg.status
        ORDER BY labels.label
    """
    baseline_sql = (
        "WITH orders_agg AS "
        + _subquery(_compiled_semantic_sql(semantic_layer, "SELECT orders.revenue, orders.status FROM orders"))
        + ", labels AS (SELECT 'completed' AS status, 'Closed' AS label)"
        + " SELECT labels.label, orders_agg.revenue"
        + " FROM orders_agg JOIN labels ON labels.status = orders_agg.status"
        + " ORDER BY labels.label"
    )

    explanation = _assert_query_matches_baseline(semantic_layer, sql, baseline_sql, ordered=True)

    assert explanation.chosen_plan == "semantic_plus_postprocess"
    assert len(explanation.semantic_islands) == 1
    assert explanation.semantic_islands[0]["name"] == "orders_agg"
    assert "labels AS (SELECT 'completed' AS status, 'Closed' AS label)" in explanation.rewritten_sql


def test_derived_table_semantic_island_under_outer_join_uses_preaggregation(semantic_layer):
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
    sql = """
        SELECT orders_agg.status, labels.label, orders_agg.revenue
        FROM (
            SELECT orders.revenue, orders.status FROM orders
        ) orders_agg
        JOIN (
            SELECT 'completed' AS status, 'Closed' AS label
            UNION ALL
            SELECT 'pending' AS status, 'Open' AS label
        ) labels
          ON labels.status = orders_agg.status
        ORDER BY orders_agg.status
    """
    baseline_sql = (
        "SELECT orders_agg.status, labels.label, orders_agg.revenue FROM "
        + _subquery(
            _compiled_semantic_sql(
                semantic_layer,
                "SELECT orders.revenue, orders.status FROM orders",
                use_preaggregations=False,
            )
        )
        + " orders_agg JOIN "
        + _subquery(
            """
            SELECT 'completed' AS status, 'Closed' AS label
            UNION ALL
            SELECT 'pending' AS status, 'Open' AS label
            """
        )
        + " labels ON labels.status = orders_agg.status ORDER BY orders_agg.status"
    )

    explanation = _assert_query_matches_baseline(semantic_layer, sql, baseline_sql, ordered=True)

    assert explanation.chosen_plan == "semantic_plus_postprocess"
    assert len(explanation.semantic_islands) == 1
    assert explanation.semantic_islands[0]["source_kind"] == "subquery"
    assert explanation.semantic_islands[0]["chosen_plan"] == "single_model_preaggregation"
    assert "semantic_island_optimization" in explanation.applied_rules
    assert "preaggregation_route_selection" in explanation.applied_rules
    assert "orders_preagg_by_status" in explanation.rewritten_sql
    assert "JOIN (SELECT 'completed' AS status, 'Closed' AS label" in explanation.rewritten_sql


def test_semantic_island_with_outer_dependency_is_rejected(semantic_layer):
    sql = """
        SELECT *
        FROM (SELECT 1 AS id) c
        WHERE EXISTS (
            SELECT 1
            FROM orders
            WHERE orders.customer_id = c.id
        )
    """

    explanation = semantic_layer.explain_sql(sql)

    assert explanation.chosen_plan == "semantic_plus_postprocess"
    assert explanation.semantic_islands == []
    assert explanation.rejected_rules["semantic_island_optimization"] == "semantic island references outer query scope"


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


@pytest.mark.parametrize(
    ("sql", "reason"),
    [
        ("SELECT orders.revenue FROM orders GROUP BY orders.status", "GROUP BY"),
    ],
)
def test_root_semantic_query_rejects_unsupported_aggregate_clauses(semantic_layer, sql, reason):
    rewriter = QueryRewriter(semantic_layer.graph, dialect="duckdb")

    with pytest.raises(ValueError, match=reason):
        rewriter.explain(sql)

    with pytest.raises(ValueError, match=reason):
        rewriter.rewrite(sql)


def test_root_semantic_query_allows_redundant_group_by_dimensions(semantic_layer):
    sql = """
        SELECT orders.revenue, orders.status
        FROM orders
        GROUP BY orders.status
    """
    direct_sql = "SELECT orders.revenue, orders.status FROM orders"

    explanation = semantic_layer.explain_sql(sql)

    assert explanation.chosen_plan == "direct_semantic"
    assert explanation.metrics == ["orders.revenue"]
    assert explanation.dimensions == ["orders.status"]
    assert _sorted_rows(_rows(semantic_layer.sql(sql))) == _sorted_rows(_rows(semantic_layer.sql(direct_sql)))


def test_root_semantic_query_preserves_having_metric_filter(semantic_layer):
    sql = """
        SELECT orders.revenue, orders.status
        FROM orders
        HAVING orders.revenue > 225
    """

    explanation = semantic_layer.explain_sql(sql)
    rows = _rows(semantic_layer.sql(sql))

    assert explanation.chosen_plan == "direct_semantic"
    assert explanation.filters == ["orders.revenue > 225"]
    assert "HAVING" in explanation.rewritten_sql
    assert "revenue > 225" in explanation.rewritten_sql
    assert rows == [{"status": "completed", "revenue": 250.0}]


def test_root_unqualified_dimension_filter_is_qualified_for_pushdown(semantic_layer):
    qualified_sql = """
        SELECT orders.revenue, orders.status
        FROM orders
        WHERE orders.status = 'completed'
    """
    unqualified_sql = """
        SELECT revenue, status
        FROM orders
        WHERE status = 'completed'
    """

    explanation = semantic_layer.explain_sql(unqualified_sql)

    assert explanation.chosen_plan == "direct_semantic"
    assert explanation.filters == ["orders.status = 'completed'"]
    assert explanation.row_filters == ["orders.status = 'completed'"]
    assert explanation.aggregate_filters == []
    assert "WHERE status = 'completed'" in explanation.rewritten_sql
    assert _rows(semantic_layer.sql(unqualified_sql)) == _rows(semantic_layer.sql(qualified_sql))


def test_aggregate_boundary_sum_rollup_drops_finer_dimension(semantic_layer):
    wrapped_sql = """
        SELECT status, SUM(revenue) AS revenue
        FROM (
            SELECT orders.revenue, orders.status, orders.order_date FROM orders
        ) sq
        GROUP BY status
        ORDER BY status
    """
    baseline_sql = (
        "SELECT status, SUM(revenue) AS revenue FROM "
        + _subquery(
            _compiled_semantic_sql(
                semantic_layer,
                "SELECT orders.revenue, orders.status, orders.order_date FROM orders",
            )
        )
        + " sq GROUP BY status ORDER BY status"
    )

    explanation = _assert_query_matches_baseline(semantic_layer, wrapped_sql, baseline_sql, ordered=True)

    assert explanation.chosen_plan == "direct_semantic"
    assert explanation.metrics == ["orders.revenue"]
    assert explanation.dimensions == ["orders.status"]
    assert explanation.aliases == {"orders.revenue": "revenue"}
    assert "aggregate_boundary_rollup" in explanation.applied_rules
    assert "additive_metric_rollup" in explanation.applied_rules
    assert " AS sq" not in explanation.rewritten_sql


def test_same_grain_aggregate_wrapper_flattens_without_changing_grouping(semantic_layer):
    wrapped_sql = """
        SELECT status, revenue
        FROM (
            SELECT orders.revenue, orders.status FROM orders
        ) sq
        GROUP BY status, revenue
        ORDER BY status
    """
    baseline_sql = (
        "SELECT status, revenue FROM "
        + _subquery(_compiled_semantic_sql(semantic_layer, "SELECT orders.revenue, orders.status FROM orders"))
        + " sq GROUP BY status, revenue ORDER BY status"
    )

    explanation = _assert_query_matches_baseline(semantic_layer, wrapped_sql, baseline_sql, ordered=True)
    candidates = _candidate_by_name(explanation)

    assert explanation.chosen_plan == "direct_semantic"
    assert explanation.metrics == ["orders.revenue"]
    assert explanation.dimensions == ["orders.status"]
    assert "same_grain_metric_wrapper" in explanation.applied_rules
    assert candidates["same_grain_metric_wrapper"].valid is True
    assert " AS sq" not in explanation.rewritten_sql


def test_same_grain_aggregate_wrapper_rejects_computed_projection(semantic_layer):
    rewriter = QueryRewriter(semantic_layer.graph, dialect="duckdb")
    sql = """
        SELECT status, revenue * 2 AS doubled_revenue
        FROM (
            SELECT orders.revenue, orders.status FROM orders
        ) sq
        GROUP BY status, revenue
    """

    explanation = rewriter.explain(sql)

    assert explanation.chosen_plan == "semantic_plus_postprocess"
    assert explanation.rejected_rules["same_grain_metric_wrapper"] == "outer_projection_computes_expression"


def test_aggregate_boundary_sum_rollup_uses_preaggregation(semantic_layer):
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
        SELECT status, SUM(revenue) AS revenue
        FROM (
            SELECT orders.revenue, orders.status, orders.order_date__day FROM orders
        ) sq
        GROUP BY status
        ORDER BY status
    """
    baseline_sql = (
        "SELECT status, SUM(revenue) AS revenue FROM "
        + _subquery(
            _compiled_semantic_sql(
                semantic_layer,
                "SELECT orders.revenue, orders.status, orders.order_date__day FROM orders",
                use_preaggregations=False,
            )
        )
        + " sq GROUP BY status ORDER BY status"
    )

    explanation = _assert_query_matches_baseline(semantic_layer, wrapped_sql, baseline_sql, ordered=True)
    candidates = _candidate_by_name(explanation)

    assert explanation.chosen_plan == "single_model_preaggregation"
    assert candidates["aggregate_boundary_rollup"].valid is True
    assert "aggregate_boundary_rollup" in explanation.applied_rules
    assert "preaggregation_route_selection" in explanation.applied_rules
    assert "orders_preagg_by_status" in explanation.rewritten_sql


def test_aggregate_boundary_count_metric_rollup(semantic_layer):
    wrapped_sql = """
        SELECT status, SUM(count) AS count
        FROM (
            SELECT orders.count, orders.status, orders.order_date FROM orders
        ) sq
        GROUP BY status
        ORDER BY status
    """
    baseline_sql = (
        "SELECT status, SUM(count) AS count FROM "
        + _subquery(
            _compiled_semantic_sql(
                semantic_layer,
                "SELECT orders.count, orders.status, orders.order_date FROM orders",
            )
        )
        + " sq GROUP BY status ORDER BY status"
    )

    explanation = _assert_query_matches_baseline(semantic_layer, wrapped_sql, baseline_sql, ordered=True)

    assert explanation.chosen_plan == "direct_semantic"
    assert explanation.metrics == ["orders.count"]
    assert explanation.dimensions == ["orders.status"]
    assert "aggregate_boundary_rollup" in explanation.applied_rules
    assert "count_metric_rollup" in explanation.applied_rules


def test_aggregate_boundary_min_max_metric_rollup(semantic_layer):
    orders = semantic_layer.get_model("orders")
    orders.metrics.extend(
        [
            Metric(name="min_amount", agg="min", sql="amount"),
            Metric(name="max_amount", agg="max", sql="amount"),
        ]
    )
    min_sql = """
        SELECT status, MIN(min_amount) AS min_amount
        FROM (
            SELECT orders.min_amount, orders.status, orders.order_date FROM orders
        ) sq
        GROUP BY status
        ORDER BY status
    """
    max_sql = """
        SELECT status, MAX(max_amount) AS max_amount
        FROM (
            SELECT orders.max_amount, orders.status, orders.order_date FROM orders
        ) sq
        GROUP BY status
        ORDER BY status
    """
    min_baseline = (
        "SELECT status, MIN(min_amount) AS min_amount FROM "
        + _subquery(
            _compiled_semantic_sql(
                semantic_layer,
                "SELECT orders.min_amount, orders.status, orders.order_date FROM orders",
            )
        )
        + " sq GROUP BY status ORDER BY status"
    )
    max_baseline = (
        "SELECT status, MAX(max_amount) AS max_amount FROM "
        + _subquery(
            _compiled_semantic_sql(
                semantic_layer,
                "SELECT orders.max_amount, orders.status, orders.order_date FROM orders",
            )
        )
        + " sq GROUP BY status ORDER BY status"
    )

    min_explanation = _assert_query_matches_baseline(semantic_layer, min_sql, min_baseline, ordered=True)
    max_explanation = _assert_query_matches_baseline(semantic_layer, max_sql, max_baseline, ordered=True)

    assert "aggregate_boundary_rollup" in min_explanation.applied_rules
    assert "min_metric_rollup" in min_explanation.applied_rules
    assert "aggregate_boundary_rollup" in max_explanation.applied_rules
    assert "max_metric_rollup" in max_explanation.applied_rules


def test_aggregate_boundary_rejects_mismatched_min_rollup(semantic_layer):
    orders = semantic_layer.get_model("orders")
    orders.metrics.append(Metric(name="min_amount", agg="min", sql="amount"))
    rewriter = QueryRewriter(semantic_layer.graph, dialect="duckdb")
    sql = """
        SELECT status, SUM(min_amount) AS min_amount
        FROM (
            SELECT orders.min_amount, orders.status, orders.order_date FROM orders
        ) sq
        GROUP BY status
    """

    explanation = rewriter.explain(sql)

    assert explanation.chosen_plan == "semantic_plus_postprocess"
    assert explanation.rejected_rules["aggregate_boundary_rollup"] == "outer_aggregate_not_rollup_safe"


def test_aggregate_boundary_scalar_sum_rollup(semantic_layer):
    wrapped_sql = """
        SELECT SUM(revenue) AS total_revenue
        FROM (
            SELECT orders.revenue, orders.status FROM orders
        ) sq
    """
    baseline_sql = (
        "SELECT SUM(revenue) AS total_revenue FROM "
        + _subquery(_compiled_semantic_sql(semantic_layer, "SELECT orders.revenue, orders.status FROM orders"))
        + " sq"
    )

    explanation = _assert_query_matches_baseline(semantic_layer, wrapped_sql, baseline_sql)

    assert explanation.chosen_plan == "direct_semantic"
    assert explanation.metrics == ["orders.revenue"]
    assert explanation.dimensions == []
    assert explanation.aliases == {"orders.revenue": "total_revenue"}
    assert "aggregate_boundary_rollup" in explanation.applied_rules


def test_additive_total_union_uses_branch_preaggregations(semantic_layer):
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
    sql = """
        SELECT status, revenue
        FROM (SELECT orders.revenue, orders.status FROM orders) detail
        UNION ALL
        SELECT NULL AS status, SUM(revenue) AS revenue
        FROM (SELECT orders.revenue, orders.status FROM orders) detail_total
        ORDER BY status
    """
    baseline_sql = (
        "SELECT status, revenue FROM "
        + _subquery(
            _compiled_semantic_sql(
                semantic_layer,
                "SELECT orders.revenue, orders.status FROM orders",
                use_preaggregations=False,
            )
        )
        + " detail UNION ALL SELECT NULL AS status, SUM(revenue) AS revenue FROM "
        + _subquery(
            _compiled_semantic_sql(
                semantic_layer,
                "SELECT orders.revenue, orders.status FROM orders",
                use_preaggregations=False,
            )
        )
        + " detail_total ORDER BY status"
    )

    explanation = _assert_query_matches_baseline(semantic_layer, sql, baseline_sql, ordered=True)

    assert explanation.chosen_plan == "semantic_plus_postprocess"
    assert "set_operation_branch_optimization" in explanation.applied_rules
    assert "semantic_island_optimization" in explanation.applied_rules
    assert "preaggregation_route_selection" in explanation.applied_rules
    assert explanation.rewritten_sql.count("orders_preagg_by_status") == 2
    assert "UNION ALL SELECT NULL AS status, SUM(revenue) AS revenue" in explanation.rewritten_sql


def test_grouping_sets_subtotal_preserves_outer_shape_and_uses_preaggregation(semantic_layer):
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
    sql = """
        SELECT status, SUM(revenue) AS revenue
        FROM (SELECT orders.revenue, orders.status FROM orders) sq
        GROUP BY GROUPING SETS ((status), ())
        ORDER BY status
    """
    baseline_sql = (
        "SELECT status, SUM(revenue) AS revenue FROM "
        + _subquery(
            _compiled_semantic_sql(
                semantic_layer,
                "SELECT orders.revenue, orders.status FROM orders",
                use_preaggregations=False,
            )
        )
        + " sq GROUP BY GROUPING SETS ((status), ()) ORDER BY status"
    )

    explanation = _assert_query_matches_baseline(semantic_layer, sql, baseline_sql, ordered=True)

    assert explanation.chosen_plan == "semantic_plus_postprocess"
    assert "semantic_island_optimization" in explanation.applied_rules
    assert "preaggregation_route_selection" in explanation.applied_rules
    assert "GROUP BY GROUPING SETS ((status), ())" in explanation.rewritten_sql
    assert "orders_preagg_by_status" in explanation.rewritten_sql


def test_aggregate_boundary_rejects_non_additive_subtotals(semantic_layer):
    orders = semantic_layer.get_model("orders")
    orders.metrics.append(Metric(name="median_amount", agg="median", sql="amount"))
    ratio_sql = """
        SELECT status, SUM(revenue) / SUM(count) AS revenue_per_order
        FROM (
            SELECT orders.revenue, orders.count, orders.status, orders.order_date FROM orders
        ) sq
        GROUP BY status
    """
    median_sql = """
        SELECT status, MEDIAN(median_amount) AS median_amount
        FROM (
            SELECT orders.median_amount, orders.status, orders.order_date FROM orders
        ) sq
        GROUP BY status
    """

    ratio_explanation = semantic_layer.explain_sql(ratio_sql)
    median_explanation = semantic_layer.explain_sql(median_sql)

    assert ratio_explanation.chosen_plan == "semantic_plus_postprocess"
    assert ratio_explanation.rejected_rules["aggregate_boundary_rollup"] == "outer_projection_computes_expression"
    assert median_explanation.chosen_plan == "semantic_plus_postprocess"
    assert median_explanation.rejected_rules["aggregate_boundary_rollup"] == "outer_projection_computes_expression"


def test_aggregate_boundary_time_grain_rollup_day_to_month(semantic_layer):
    wrapped_sql = """
        SELECT DATE_TRUNC('month', order_date__day) AS order_month, SUM(revenue) AS revenue
        FROM (
            SELECT orders.order_date__day, orders.revenue FROM orders
        ) sq
        GROUP BY 1
        ORDER BY order_month
    """
    baseline_sql = (
        "SELECT DATE_TRUNC('month', order_date__day) AS order_month, SUM(revenue) AS revenue FROM "
        + _subquery(_compiled_semantic_sql(semantic_layer, "SELECT orders.order_date__day, orders.revenue FROM orders"))
        + " sq GROUP BY 1 ORDER BY order_month"
    )

    explanation = _assert_query_matches_baseline(semantic_layer, wrapped_sql, baseline_sql, ordered=True)

    assert explanation.chosen_plan == "direct_semantic"
    assert explanation.metrics == ["orders.revenue"]
    assert explanation.dimensions == ["orders.order_date__month"]
    assert explanation.aliases == {
        "orders.order_date__month": "order_month",
        "orders.revenue": "revenue",
    }
    assert "aggregate_boundary_rollup" in explanation.applied_rules
    assert "time_grain_rollup" in explanation.applied_rules
    assert "DATE_TRUNC('MONTH', order_date) AS order_date__month" in explanation.rewritten_sql


def test_aggregate_boundary_time_grain_rollup_uses_daily_preaggregation(semantic_layer):
    orders = semantic_layer.get_model("orders")
    orders.pre_aggregations = [
        PreAggregation(
            name="daily_revenue",
            measures=["revenue"],
            time_dimension="order_date",
            granularity="day",
        )
    ]
    semantic_layer.use_preaggregations = True
    semantic_layer.conn.execute("""
        CREATE TABLE orders_preagg_daily_revenue AS
        SELECT
            DATE_TRUNC('day', order_date) AS order_date_day,
            SUM(amount) AS revenue_raw
        FROM orders
        GROUP BY 1
    """)
    wrapped_sql = """
        SELECT DATE_TRUNC('month', order_date__day) AS order_month, SUM(revenue) AS revenue
        FROM (
            SELECT orders.order_date__day, orders.revenue FROM orders
        ) sq
        GROUP BY 1
        ORDER BY order_month
    """
    baseline_sql = (
        "SELECT DATE_TRUNC('month', order_date__day) AS order_month, SUM(revenue) AS revenue FROM "
        + _subquery(
            _compiled_semantic_sql(
                semantic_layer,
                "SELECT orders.order_date__day, orders.revenue FROM orders",
                use_preaggregations=False,
            )
        )
        + " sq GROUP BY 1 ORDER BY order_month"
    )

    explanation = _assert_query_matches_baseline(semantic_layer, wrapped_sql, baseline_sql, ordered=True)

    assert explanation.chosen_plan == "single_model_preaggregation"
    assert "aggregate_boundary_rollup" in explanation.applied_rules
    assert "time_grain_rollup" in explanation.applied_rules
    assert "preaggregation_route_selection" in explanation.applied_rules
    assert "orders_preagg_daily_revenue" in explanation.rewritten_sql
    assert "DATE_TRUNC('MONTH', order_date_day)" in explanation.rewritten_sql


def test_aggregate_boundary_time_grain_rollup_rejects_week_to_month(semantic_layer):
    rewriter = QueryRewriter(semantic_layer.graph, dialect="duckdb")
    sql = """
        SELECT DATE_TRUNC('month', order_date__week) AS order_month, SUM(revenue) AS revenue
        FROM (
            SELECT orders.order_date__week, orders.revenue FROM orders
        ) sq
        GROUP BY 1
    """

    explanation = rewriter.explain(sql)

    assert explanation.chosen_plan == "semantic_plus_postprocess"
    assert explanation.rejected_rules["aggregate_boundary_rollup"] == "time_grain_rollup_not_safe"


@pytest.mark.parametrize(
    ("sql", "reason"),
    [
        (
            """
            SELECT COALESCE(status, 'unknown') AS status_bucket, SUM(revenue) AS revenue
            FROM (
                SELECT orders.status, orders.revenue FROM orders
            ) sq
            GROUP BY 1
            """,
            "outer_group_expression_not_supported",
        ),
        (
            """
            SELECT CURRENT_DATE AS today, SUM(revenue) AS revenue
            FROM (
                SELECT orders.status, orders.revenue FROM orders
            ) sq
            GROUP BY 1
            """,
            "outer_group_expression_not_supported",
        ),
    ],
)
def test_aggregate_boundary_time_grain_rollup_rejects_unsafe_dimension_expressions(semantic_layer, sql, reason):
    explanation = semantic_layer.explain_sql(sql)

    assert explanation.chosen_plan == "semantic_plus_postprocess"
    assert explanation.rejected_rules["aggregate_boundary_rollup"] == reason
    assert "semantic_island_optimization" in explanation.applied_rules


def test_conditional_aggregate_pivot_uses_inner_preaggregation(semantic_layer):
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
    sql = """
        SELECT
            SUM(CASE WHEN status = 'completed' THEN revenue ELSE 0 END) AS completed_revenue,
            SUM(CASE WHEN status = 'pending' THEN revenue ELSE 0 END) AS pending_revenue
        FROM (
            SELECT orders.revenue, orders.status FROM orders
        ) sq
    """
    baseline_sql = (
        "SELECT "
        + "SUM(CASE WHEN status = 'completed' THEN revenue ELSE 0 END) AS completed_revenue, "
        + "SUM(CASE WHEN status = 'pending' THEN revenue ELSE 0 END) AS pending_revenue FROM "
        + _subquery(
            _compiled_semantic_sql(
                semantic_layer,
                "SELECT orders.revenue, orders.status FROM orders",
                use_preaggregations=False,
            )
        )
        + " sq"
    )

    explanation = _assert_query_matches_baseline(semantic_layer, sql, baseline_sql)

    assert explanation.chosen_plan == "semantic_plus_postprocess"
    assert len(explanation.semantic_islands) == 1
    assert explanation.semantic_islands[0]["chosen_plan"] == "single_model_preaggregation"
    assert "conditional_aggregate_wrapper" in explanation.applied_rules
    assert "orders_preagg_by_status" in explanation.rewritten_sql
    assert "completed_revenue" in explanation.rewritten_sql


def test_conditional_aggregate_pivot_rejects_count_distinct_metric(semantic_layer):
    orders = semantic_layer.get_model("orders")
    orders.metrics.append(Metric(name="unique_customers", agg="count_distinct", sql="customer_id"))
    rewriter = QueryRewriter(semantic_layer.graph, dialect="duckdb")
    sql = """
        SELECT
            SUM(CASE WHEN status = 'completed' THEN unique_customers ELSE 0 END) AS completed_customers
        FROM (
            SELECT orders.unique_customers, orders.status FROM orders
        ) sq
    """

    explanation = rewriter.explain(sql)

    assert explanation.chosen_plan == "semantic_plus_postprocess"
    assert "conditional_aggregate_wrapper" not in explanation.applied_rules
    assert explanation.rejected_rules["conditional_aggregate_wrapper"] == (
        "conditional aggregate metric is not additive"
    )


def test_conditional_aggregate_pivot_with_outer_row_filter_uses_preaggregation(semantic_layer):
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
    sql = """
        SELECT
            SUM(CASE WHEN status = 'completed' THEN revenue ELSE 0 END) AS completed_revenue
        FROM (
            SELECT orders.revenue, orders.status FROM orders
        ) sq
        WHERE status IS NOT NULL
    """
    baseline_sql = (
        "SELECT SUM(CASE WHEN status = 'completed' THEN revenue ELSE 0 END) AS completed_revenue FROM "
        + _subquery(
            _compiled_semantic_sql(
                semantic_layer,
                "SELECT orders.revenue, orders.status FROM orders",
                use_preaggregations=False,
            )
        )
        + " sq WHERE status IS NOT NULL"
    )

    explanation = _assert_query_matches_baseline(semantic_layer, sql, baseline_sql)

    assert explanation.chosen_plan == "semantic_plus_postprocess"
    assert "conditional_aggregate_wrapper" in explanation.applied_rules
    assert "preaggregation_route_selection" in explanation.applied_rules
    assert "orders_preagg_by_status" in explanation.rewritten_sql
    assert "WHERE NOT status IS NULL" in explanation.rewritten_sql


@pytest.mark.parametrize(
    ("sql", "reason"),
    [
        (
            """
            SELECT
                SUM(CASE WHEN tier = 'premium' THEN revenue ELSE 0 END) AS premium_revenue
            FROM (
                SELECT orders.revenue, orders.status FROM orders
            ) sq
            """,
            "conditional aggregate predicate references a non-dimension",
        ),
        (
            """
            SELECT
                SUM(CASE WHEN status = 'completed' THEN revenue * 2 ELSE 0 END) AS completed_revenue
            FROM (
                SELECT orders.revenue, orders.status FROM orders
            ) sq
            """,
            "conditional aggregate result is not a metric column",
        ),
    ],
)
def test_conditional_aggregate_pivot_rejects_unsafe_shapes(semantic_layer, sql, reason):
    explanation = semantic_layer.explain_sql(sql)

    assert explanation.chosen_plan == "semantic_plus_postprocess"
    assert "conditional_aggregate_wrapper" not in explanation.applied_rules
    assert explanation.rejected_rules["conditional_aggregate_wrapper"] == reason


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


@pytest.mark.parametrize(
    ("preaggregation", "sql", "reason", "failed_check"),
    [
        (
            PreAggregation(
                name="by_status",
                measures=["revenue"],
                dimensions=["status"],
            ),
            "SELECT orders.revenue, orders.order_date FROM orders",
            "dimension_not_in_rollup",
            "dimensions",
        ),
        (
            PreAggregation(
                name="total_revenue",
                measures=["revenue"],
                dimensions=[],
            ),
            "SELECT orders.revenue FROM orders WHERE orders.status = 'completed'",
            "filter_not_compatible",
            "filters",
        ),
        (
            PreAggregation(
                name="by_status_count",
                measures=["count"],
                dimensions=["status"],
            ),
            "SELECT orders.revenue, orders.status FROM orders",
            "metric_not_in_rollup",
            "measures",
        ),
        (
            PreAggregation(
                name="monthly_revenue",
                measures=["revenue"],
                dimensions=[],
                time_dimension="order_date",
                granularity="month",
            ),
            "SELECT orders.revenue, orders.order_date__day FROM orders",
            "time_grain_mismatch",
            "granularity",
        ),
    ],
)
def test_explain_preaggregation_negative_eligibility_reasons(
    semantic_layer,
    preaggregation,
    sql,
    reason,
    failed_check,
):
    orders = semantic_layer.get_model("orders")
    orders.pre_aggregations = [preaggregation]
    rewriter = QueryRewriter(semantic_layer.graph, dialect="duckdb")

    explanation = rewriter.explain(sql)
    candidate = explanation.preaggregation["candidates"][0]
    failed_checks = {check["name"] for check in candidate["checks"] if not check["passed"]}

    assert explanation.preaggregation["eligible"] is False
    assert explanation.preaggregation["reason"] == reason
    assert failed_check in failed_checks


def test_count_distinct_exact_grain_preaggregation_candidate(semantic_layer):
    orders = semantic_layer.get_model("orders")
    orders.metrics.append(Metric(name="unique_customers", agg="count_distinct", sql="customer_id"))
    orders.pre_aggregations = [
        PreAggregation(
            name="unique_customers_by_status",
            measures=["unique_customers"],
            dimensions=["status"],
        )
    ]
    rewriter = QueryRewriter(semantic_layer.graph, dialect="duckdb")

    explanation = rewriter.explain("SELECT orders.unique_customers, orders.status FROM orders")

    assert explanation.preaggregation["eligible"] is True
    assert explanation.preaggregation["reason"] == "matching_preaggregation"


def test_count_distinct_exact_grain_preaggregation_executes(semantic_layer):
    orders = semantic_layer.get_model("orders")
    orders.metrics.append(Metric(name="unique_customers", agg="count_distinct", sql="customer_id"))
    orders.pre_aggregations = [
        PreAggregation(
            name="unique_customers_by_status",
            measures=["unique_customers"],
            dimensions=["status"],
        )
    ]
    semantic_layer.use_preaggregations = True
    semantic_layer.conn.execute("""
        CREATE TABLE orders_preagg_unique_customers_by_status AS
        SELECT
            status,
            COUNT(DISTINCT customer_id) AS unique_customers_raw
        FROM orders
        GROUP BY status
    """)
    sql = "SELECT orders.unique_customers, orders.status FROM orders ORDER BY orders.status"
    baseline_sql = _compiled_semantic_sql(semantic_layer, sql, use_preaggregations=False)

    explanation = _assert_query_matches_baseline(semantic_layer, sql, baseline_sql, ordered=True)

    assert explanation.chosen_plan == "single_model_preaggregation"
    assert "orders_preagg_unique_customers_by_status" in explanation.rewritten_sql
    assert "SUM(unique_customers_raw) AS unique_customers" in explanation.rewritten_sql


def test_count_distinct_rollup_preaggregation_rejected(semantic_layer):
    orders = semantic_layer.get_model("orders")
    orders.metrics.append(Metric(name="unique_customers", agg="count_distinct", sql="customer_id"))
    orders.pre_aggregations = [
        PreAggregation(
            name="unique_customers_by_status_day",
            measures=["unique_customers"],
            dimensions=["status", "order_date"],
        )
    ]
    rewriter = QueryRewriter(semantic_layer.graph, dialect="duckdb")

    explanation = rewriter.explain("SELECT orders.unique_customers, orders.status FROM orders")
    measure_check = next(
        check for check in explanation.preaggregation["candidates"][0]["checks"] if check["name"] == "measures"
    )

    assert explanation.preaggregation["eligible"] is False
    assert explanation.preaggregation["reason"] == "count_distinct_not_rollup_safe"
    assert "count_distinct_not_rollup_safe" in measure_check["detail"]


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


def test_root_having_metric_filter_uses_preaggregation(semantic_layer):
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
    sql = """
        SELECT orders.revenue, orders.status
        FROM orders
        HAVING orders.revenue > 225
    """
    baseline_sql = _compiled_semantic_sql(semantic_layer, sql, use_preaggregations=False)

    explanation = _assert_query_matches_baseline(semantic_layer, sql, baseline_sql)

    assert explanation.chosen_plan == "single_model_preaggregation"
    assert explanation.row_filters == []
    assert explanation.aggregate_filters == ["orders.revenue > 225"]
    assert explanation.preaggregation["aggregate_filters"] == ["orders.revenue > 225"]
    assert "orders_preagg_by_status" in explanation.rewritten_sql
    assert "HAVING SUM(revenue_raw) > 225" in explanation.rewritten_sql


def test_wrapped_metric_filter_uses_preaggregation_having(semantic_layer):
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
        WHERE revenue > 225
    """
    baseline_sql = (
        "SELECT * FROM "
        + _subquery(
            _compiled_semantic_sql(
                semantic_layer,
                "SELECT orders.revenue, orders.status FROM orders",
                use_preaggregations=False,
            )
        )
        + " sq WHERE revenue > 225"
    )

    explanation = _assert_query_matches_baseline(semantic_layer, wrapped_sql, baseline_sql)

    assert explanation.chosen_plan == "single_model_preaggregation"
    assert "safe_metric_filter_having_pushdown" in explanation.applied_rules
    assert "preaggregation_route_selection" in explanation.applied_rules
    assert explanation.aggregate_filters == ["orders.revenue > 225"]
    assert explanation.preaggregation["aggregate_filters"] == ["orders.revenue > 225"]
    assert "orders_preagg_by_status" in explanation.rewritten_sql
    assert "HAVING SUM(revenue_raw) > 225" in explanation.rewritten_sql
    assert " AS sq" not in explanation.rewritten_sql


def test_wrapped_preaggregation_preserves_projection_alias_and_order(semantic_layer):
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
        SELECT status, revenue AS total_revenue
        FROM (SELECT orders.revenue, orders.status FROM orders) sq
        ORDER BY total_revenue DESC
    """
    baseline_sql = (
        "SELECT status, revenue AS total_revenue FROM "
        + _subquery(_compiled_semantic_sql(semantic_layer, "SELECT orders.revenue, orders.status FROM orders"))
        + " sq ORDER BY total_revenue DESC"
    )

    explanation = _assert_query_matches_baseline(semantic_layer, wrapped_sql, baseline_sql, ordered=True)

    assert explanation.chosen_plan == "single_model_preaggregation"
    assert explanation.aliases == {"orders.revenue": "total_revenue"}
    assert "SUM(revenue_raw) AS total_revenue" in explanation.rewritten_sql
    assert "ORDER BY total_revenue DESC" in explanation.rewritten_sql


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


def test_wrapped_fanout_preserves_aliases_and_executes(semantic_layer):
    wrapped_sql = """
        SELECT *
        FROM (
            SELECT orders.revenue AS total_revenue, customers.count AS customer_count FROM orders
        ) sq
        ORDER BY total_revenue DESC
    """
    baseline_sql = (
        "SELECT * FROM "
        + _subquery(
            _compiled_semantic_sql(
                semantic_layer,
                "SELECT orders.revenue AS total_revenue, customers.count AS customer_count FROM orders",
            )
        )
        + " sq ORDER BY total_revenue DESC"
    )

    explanation = _assert_query_matches_baseline(semantic_layer, wrapped_sql, baseline_sql, ordered=True)

    assert explanation.chosen_plan == "fanout_preaggregation"
    assert explanation.aliases == {
        "orders.revenue": "total_revenue",
        "customers.count": "customer_count",
    }
    assert "orders_preagg.total_revenue AS total_revenue" in explanation.rewritten_sql
    assert "customers_preagg.customer_count AS customer_count" in explanation.rewritten_sql


def test_wrapped_fanout_uses_child_preaggregations(semantic_layer):
    orders = semantic_layer.get_model("orders")
    orders.pre_aggregations = [
        PreAggregation(
            name="total_revenue",
            measures=["revenue"],
            dimensions=[],
        )
    ]
    customers = semantic_layer.get_model("customers")
    customers.pre_aggregations = [
        PreAggregation(
            name="total_count",
            measures=["count"],
            dimensions=[],
        )
    ]
    semantic_layer.use_preaggregations = True
    semantic_layer.conn.execute("""
        CREATE TABLE orders_preagg_total_revenue AS
        SELECT SUM(amount) AS revenue_raw
        FROM orders
    """)
    semantic_layer.conn.execute("""
        CREATE TABLE customers_preagg_total_count AS
        SELECT COUNT(*) AS count_raw
        FROM customers
    """)
    wrapped_sql = """
        SELECT *
        FROM (
            SELECT orders.revenue AS total_revenue, customers.count AS customer_count FROM orders
        ) sq
    """
    baseline_sql = (
        "SELECT * FROM "
        + _subquery(
            _compiled_semantic_sql(
                semantic_layer,
                "SELECT orders.revenue AS total_revenue, customers.count AS customer_count FROM orders",
                use_preaggregations=False,
            )
        )
        + " sq"
    )

    explanation = _assert_query_matches_baseline(semantic_layer, wrapped_sql, baseline_sql)

    assert explanation.chosen_plan == "fanout_preaggregation"
    assert "orders_preagg_total_revenue" in explanation.rewritten_sql
    assert "customers_preagg_total_count" in explanation.rewritten_sql
    assert "fanout_strategy_selection" in explanation.applied_rules


def test_fanout_join_key_preaggregation_rolls_orders_to_customer_region(semantic_layer):
    orders = semantic_layer.get_model("orders")
    orders.pre_aggregations = [
        PreAggregation(
            name="by_customer",
            measures=["revenue"],
            dimensions=["customer_id"],
        )
    ]
    semantic_layer.use_preaggregations = True
    semantic_layer.conn.execute("""
        CREATE TABLE orders_preagg_by_customer AS
        SELECT
            customer_id,
            SUM(amount) AS revenue_raw
        FROM orders
        GROUP BY customer_id
    """)
    sql = """
        SELECT orders.revenue, customers.region
        FROM orders
        ORDER BY customers.region
    """
    baseline_sql = _compiled_semantic_sql(semantic_layer, sql, use_preaggregations=False)

    explanation = _assert_query_matches_baseline(semantic_layer, sql, baseline_sql, ordered=True)
    candidates = _candidate_by_name(explanation)

    assert explanation.chosen_plan == "join_key_preaggregation"
    assert "join_key_preaggregation_route_selection" in explanation.applied_rules
    assert candidates["join_key_preaggregation"].valid is True
    assert candidates["join_key_preaggregation"].details["preaggregation"] == "by_customer"
    assert candidates["join_key_preaggregation"].details["preaggregation_dimensions"] == ["customer_id"]
    assert candidates["join_key_preaggregation"].details["join_keys"][0]["relationship"] == "many_to_one"
    assert "orders_preagg_by_customer" in explanation.rewritten_sql
    assert "LEFT JOIN customers AS customers" in explanation.rewritten_sql
    assert "FROM orders\n" not in explanation.rewritten_sql


def test_join_key_preaggregation_reads_local_time_dimension_from_grain_column(semantic_layer):
    orders = semantic_layer.get_model("orders")
    orders.pre_aggregations = [
        PreAggregation(
            name="by_customer_day",
            measures=["revenue"],
            dimensions=["customer_id"],
            time_dimension="order_date",
            granularity="day",
        )
    ]
    semantic_layer.use_preaggregations = True
    semantic_layer.conn.execute("""
        CREATE TABLE orders_preagg_by_customer_day AS
        SELECT
            DATE_TRUNC('day', order_date) AS order_date_day,
            customer_id,
            SUM(amount) AS revenue_raw
        FROM orders
        GROUP BY 1, 2
    """)
    sql = """
        SELECT orders.revenue, orders.order_date__month, customers.region
        FROM orders
        ORDER BY orders.order_date__month, customers.region
    """
    baseline_sql = _compiled_semantic_sql(semantic_layer, sql, use_preaggregations=False)

    explanation = _assert_query_matches_baseline(semantic_layer, sql, baseline_sql, ordered=True)

    assert explanation.chosen_plan == "join_key_preaggregation"
    assert "orders_preagg_by_customer_day" in explanation.rewritten_sql
    assert "DATE_TRUNC('MONTH', orders_rollup.order_date_day)" in explanation.rewritten_sql
    assert "orders_rollup.order_date)" not in explanation.rewritten_sql


def test_fanout_join_key_preaggregation_rejects_missing_join_key_rollup(semantic_layer):
    orders = semantic_layer.get_model("orders")
    orders.pre_aggregations = [
        PreAggregation(
            name="by_status",
            measures=["revenue"],
            dimensions=["status"],
        )
    ]
    semantic_layer.use_preaggregations = True
    sql = "SELECT orders.revenue, customers.region FROM orders"

    explanation = semantic_layer.explain_sql(sql)
    candidates = _candidate_by_name(explanation)

    assert explanation.chosen_plan == "direct_semantic"
    assert candidates["join_key_preaggregation"].valid is False
    assert candidates["join_key_preaggregation"].reason == "missing_join_key_preaggregation"


def test_fanout_join_key_preaggregation_rejects_one_to_many_remote_dimension(semantic_layer):
    customers = semantic_layer.get_model("customers")
    customers.pre_aggregations = [
        PreAggregation(
            name="by_customer",
            measures=["count"],
            dimensions=["id"],
        )
    ]
    semantic_layer.use_preaggregations = True
    sql = "SELECT customers.count, orders.status FROM customers"

    explanation = semantic_layer.explain_sql(sql)
    candidates = _candidate_by_name(explanation)

    assert explanation.chosen_plan == "direct_semantic"
    assert candidates["join_key_preaggregation"].valid is False
    assert candidates["join_key_preaggregation"].reason == "remote_dimension_not_many_to_one"


def test_wrapped_window_metric_executes_against_baseline(semantic_layer):
    semantic_layer.add_metric(
        Metric(
            name="running_total_revenue",
            type="cumulative",
            sql="orders.revenue",
        )
    )
    wrapped_sql = """
        SELECT *
        FROM (SELECT running_total_revenue, orders.order_date FROM metrics) sq
        ORDER BY order_date
    """
    baseline_sql = (
        "SELECT * FROM "
        + _subquery(
            _compiled_semantic_sql(
                semantic_layer,
                "SELECT running_total_revenue, orders.order_date FROM metrics",
            )
        )
        + " sq ORDER BY order_date"
    )

    explanation = _assert_query_matches_baseline(semantic_layer, wrapped_sql, baseline_sql, ordered=True)

    assert explanation.chosen_plan == "window_metric"
    assert "safe_order_pushdown" in explanation.applied_rules


def test_wrapped_window_metric_uses_inner_preaggregation(semantic_layer):
    semantic_layer.add_metric(
        Metric(
            name="running_total_revenue",
            type="cumulative",
            sql="orders.revenue",
        )
    )
    orders = semantic_layer.get_model("orders")
    orders.pre_aggregations = [
        PreAggregation(
            name="daily_revenue",
            measures=["revenue"],
            time_dimension="order_date",
            granularity="day",
        )
    ]
    semantic_layer.use_preaggregations = True
    semantic_layer.conn.execute("""
        CREATE TABLE orders_preagg_daily_revenue AS
        SELECT
            DATE_TRUNC('day', order_date) AS order_date_day,
            SUM(amount) AS revenue_raw
        FROM orders
        GROUP BY 1
    """)
    wrapped_sql = """
        SELECT *
        FROM (SELECT running_total_revenue, orders.order_date__day FROM metrics) sq
        ORDER BY order_date__day
    """
    baseline_sql = (
        "SELECT * FROM "
        + _subquery(
            _compiled_semantic_sql(
                semantic_layer,
                "SELECT running_total_revenue, orders.order_date__day FROM metrics",
                use_preaggregations=False,
            )
        )
        + " sq ORDER BY order_date__day"
    )

    explanation = _assert_query_matches_baseline(semantic_layer, wrapped_sql, baseline_sql, ordered=True)
    window_candidate = _candidate_by_name(explanation)["window_metric"]

    assert explanation.chosen_plan == "window_metric"
    assert window_candidate.details["inner_preaggregation"]["eligible"] is True
    assert window_candidate.details["inner_preaggregation"]["reason"] == "matching_preaggregation"
    assert window_candidate.details["inner_preaggregation"]["metrics"] == ["orders.revenue"]
    assert "orders_preagg_daily_revenue" in explanation.rewritten_sql
    assert "used_preagg=true" in explanation.rewritten_sql
    assert "safe_order_pushdown" in explanation.applied_rules


def test_wrapper_window_metric_filter_stays_postprocess(semantic_layer):
    semantic_layer.add_metric(
        Metric(
            name="running_total_revenue",
            type="cumulative",
            sql="orders.revenue",
        )
    )
    rewriter = QueryRewriter(semantic_layer.graph, dialect="duckdb")
    sql = """
        SELECT *
        FROM (SELECT running_total_revenue, orders.order_date FROM metrics) sq
        WHERE running_total_revenue > 100
    """

    explanation = rewriter.explain(sql)

    assert explanation.chosen_plan == "semantic_plus_postprocess"
    assert explanation.post_process is not None
    assert explanation.rejected_rules["safe_filter_pushdown"] == (
        "safe_filter_pushdown cannot move window metric filter 'running_total_revenue'"
    )


def test_dimension_only_distinct_wrapper_flattens(semantic_layer):
    sql = """
        SELECT DISTINCT status
        FROM (SELECT orders.status FROM orders) sq
        ORDER BY status
    """
    baseline_sql = (
        "SELECT DISTINCT status FROM "
        + _subquery(_compiled_semantic_sql(semantic_layer, "SELECT orders.status FROM orders"))
        + " sq ORDER BY status"
    )

    explanation = _assert_query_matches_baseline(semantic_layer, sql, baseline_sql, ordered=True)
    candidates = _candidate_by_name(explanation)

    assert explanation.chosen_plan == "direct_semantic"
    assert explanation.metrics == []
    assert explanation.dimensions == ["orders.status"]
    assert "dimension_distinct_wrapper" in explanation.applied_rules
    assert candidates["dimension_distinct_wrapper"].valid is True
    assert "DISTINCT" not in explanation.rewritten_sql
    assert " AS sq" not in explanation.rewritten_sql


def test_dimension_distinct_slicer_preserves_null_filter_order_and_limit(semantic_layer):
    sql = """
        SELECT DISTINCT status
        FROM (SELECT orders.status FROM orders) sq
        WHERE status IS NOT NULL
        ORDER BY status
        LIMIT 1000
    """
    baseline_sql = (
        "SELECT DISTINCT status FROM "
        + _subquery(_compiled_semantic_sql(semantic_layer, "SELECT orders.status FROM orders"))
        + " sq WHERE status IS NOT NULL ORDER BY status LIMIT 1000"
    )

    explanation = _assert_query_matches_baseline(semantic_layer, sql, baseline_sql, ordered=True)

    assert explanation.chosen_plan == "direct_semantic"
    assert explanation.filters == ["NOT orders.status IS NULL"]
    assert explanation.limit == 1000
    assert "dimension_distinct_wrapper" in explanation.applied_rules
    assert "safe_filter_pushdown" in explanation.applied_rules
    assert "safe_order_pushdown" in explanation.applied_rules
    assert "safe_limit_pushdown" in explanation.applied_rules
    assert "DISTINCT" not in explanation.rewritten_sql
    assert "FROM (" not in explanation.rewritten_sql


def test_dimension_distinct_slicer_supports_lower_like_search(semantic_layer):
    sql = """
        SELECT DISTINCT status
        FROM (SELECT orders.status FROM orders) sq
        WHERE LOWER(status) LIKE 'comp%'
        ORDER BY status
    """
    baseline_sql = (
        "SELECT DISTINCT status FROM "
        + _subquery(_compiled_semantic_sql(semantic_layer, "SELECT orders.status FROM orders"))
        + " sq WHERE LOWER(status) LIKE 'comp%' ORDER BY status"
    )

    explanation = _assert_query_matches_baseline(semantic_layer, sql, baseline_sql, ordered=True)

    assert explanation.chosen_plan == "direct_semantic"
    assert explanation.filters == ["LOWER(orders.status) LIKE 'comp%'"]
    assert "dimension_distinct_wrapper" in explanation.applied_rules
    assert "safe_filter_pushdown" in explanation.applied_rules
    assert "LOWER(status) LIKE 'comp%'" in explanation.rewritten_sql


def test_dimension_distinct_remote_dimension_probe_flattens(semantic_layer):
    sql = """
        SELECT DISTINCT region
        FROM (SELECT customers.region FROM orders) sq
        WHERE region IN ('US', 'EU')
        ORDER BY region
        LIMIT 10
    """
    baseline_sql = (
        "SELECT DISTINCT region FROM "
        + _subquery(_compiled_semantic_sql(semantic_layer, "SELECT customers.region FROM orders"))
        + " sq WHERE region IN ('US', 'EU') ORDER BY region LIMIT 10"
    )

    explanation = _assert_query_matches_baseline(semantic_layer, sql, baseline_sql, ordered=True)

    assert explanation.chosen_plan == "direct_semantic"
    assert explanation.dimensions == ["customers.region"]
    assert "dimension_distinct_wrapper" in explanation.applied_rules
    assert "FROM customers" in explanation.rewritten_sql
    assert "DISTINCT" not in explanation.rewritten_sql


def test_dimension_distinct_wrapper_rejects_metrics(semantic_layer):
    rewriter = QueryRewriter(semantic_layer.graph, dialect="duckdb")
    sql = """
        SELECT DISTINCT status
        FROM (SELECT orders.revenue, orders.status FROM orders) sq
    """

    explanation = rewriter.explain(sql)

    assert explanation.chosen_plan == "semantic_plus_postprocess"
    assert explanation.rejected_rules["dimension_distinct_wrapper"] == "inner semantic query projects metrics"


def test_dimension_distinct_wrapper_rejects_computed_projection(semantic_layer):
    rewriter = QueryRewriter(semantic_layer.graph, dialect="duckdb")
    sql = """
        SELECT DISTINCT SUBSTR(status, 1, 1) AS status_prefix
        FROM (SELECT orders.status FROM orders) sq
    """

    explanation = rewriter.explain(sql)

    assert explanation.chosen_plan == "semantic_plus_postprocess"
    assert explanation.rejected_rules["dimension_distinct_wrapper"] == "outer projection computes a new expression"


def test_dimension_distinct_wrapper_rejects_hidden_filter(semantic_layer):
    rewriter = QueryRewriter(semantic_layer.graph, dialect="duckdb")
    sql = """
        SELECT DISTINCT status
        FROM (SELECT orders.status FROM orders) sq
        WHERE order_date__day >= DATE '2024-01-01'
    """

    explanation = rewriter.explain(sql)

    assert explanation.chosen_plan == "semantic_plus_postprocess"
    assert explanation.rejected_rules["dimension_distinct_wrapper"] == (
        "safe_filter_pushdown references unknown field 'order_date__day'"
    )
    assert explanation.rejected_rules["safe_filter_pushdown"] == (
        "safe_filter_pushdown references unknown field 'order_date__day'"
    )


def test_global_row_number_topn_rewrites_to_order_limit(semantic_layer):
    sql = """
        SELECT status, revenue
        FROM (
            SELECT
                status,
                revenue,
                ROW_NUMBER() OVER (ORDER BY revenue DESC) AS rn
            FROM (SELECT orders.revenue, orders.status FROM orders) semantic_result
        ) ranked
        WHERE rn <= 1
        ORDER BY revenue DESC
    """
    baseline_sql = (
        "SELECT status, revenue FROM ("
        "SELECT status, revenue, ROW_NUMBER() OVER (ORDER BY revenue DESC) AS rn FROM "
        + _subquery(_compiled_semantic_sql(semantic_layer, "SELECT orders.revenue, orders.status FROM orders"))
        + " semantic_result) ranked WHERE rn <= 1 ORDER BY revenue DESC"
    )

    explanation = _assert_query_matches_baseline(semantic_layer, sql, baseline_sql, ordered=True)
    candidates = _candidate_by_name(explanation)

    assert explanation.chosen_plan == "direct_semantic"
    assert explanation.order_by == ["revenue DESC"]
    assert explanation.limit == 1
    assert "global_row_number_topn" in explanation.applied_rules
    assert "safe_order_pushdown" in explanation.applied_rules
    assert "safe_limit_pushdown" in explanation.applied_rules
    assert candidates["global_row_number_topn"].valid is True
    assert "ROW_NUMBER" not in explanation.rewritten_sql
    assert "LIMIT 1" in explanation.rewritten_sql


def test_global_row_number_between_rewrites_to_limit_offset(semantic_layer):
    sql = """
        SELECT status, revenue
        FROM (
            SELECT
                status,
                revenue,
                ROW_NUMBER() OVER (ORDER BY revenue DESC) AS rn
            FROM (SELECT orders.revenue, orders.status FROM orders) semantic_result
        ) ranked
        WHERE rn BETWEEN 2 AND 2
        ORDER BY revenue DESC
    """
    baseline_sql = (
        "SELECT status, revenue FROM ("
        "SELECT status, revenue, ROW_NUMBER() OVER (ORDER BY revenue DESC) AS rn FROM "
        + _subquery(_compiled_semantic_sql(semantic_layer, "SELECT orders.revenue, orders.status FROM orders"))
        + " semantic_result) ranked WHERE rn BETWEEN 2 AND 2 ORDER BY revenue DESC"
    )

    explanation = _assert_query_matches_baseline(semantic_layer, sql, baseline_sql, ordered=True)

    assert explanation.chosen_plan == "direct_semantic"
    assert explanation.limit == 1
    assert explanation.offset == 1
    assert "global_row_number_topn" in explanation.applied_rules
    assert "ROW_NUMBER" not in explanation.rewritten_sql
    assert "LIMIT 1" in explanation.rewritten_sql
    assert "OFFSET 1" in explanation.rewritten_sql


def test_qualify_row_number_topn_rewrites_to_order_limit(semantic_layer):
    sql = """
        SELECT status, revenue
        FROM (SELECT orders.revenue, orders.status FROM orders) sq
        QUALIFY ROW_NUMBER() OVER (ORDER BY revenue DESC) <= 1
    """
    baseline_sql = (
        "SELECT status, revenue FROM "
        + _subquery(_compiled_semantic_sql(semantic_layer, "SELECT orders.revenue, orders.status FROM orders"))
        + " sq QUALIFY ROW_NUMBER() OVER (ORDER BY revenue DESC) <= 1"
    )

    explanation = _assert_query_matches_baseline(semantic_layer, sql, baseline_sql, ordered=True)
    candidates = _candidate_by_name(explanation)

    assert explanation.chosen_plan == "direct_semantic"
    assert explanation.limit == 1
    assert explanation.order_by == ["revenue DESC"]
    assert "qualify_row_number_topn" in explanation.applied_rules
    assert candidates["qualify_row_number_topn"].valid is True
    assert "QUALIFY" not in explanation.rewritten_sql
    assert "ROW_NUMBER" not in explanation.rewritten_sql
    assert "LIMIT 1" in explanation.rewritten_sql


def test_fetch_first_topn_rewrites_through_limit_pushdown(semantic_layer):
    sql = """
        SELECT status, revenue
        FROM (SELECT orders.revenue, orders.status FROM orders) sq
        ORDER BY revenue DESC
        FETCH FIRST 1 ROWS ONLY
    """
    baseline_sql = (
        "SELECT status, revenue FROM "
        + _subquery(_compiled_semantic_sql(semantic_layer, "SELECT orders.revenue, orders.status FROM orders"))
        + " sq ORDER BY revenue DESC FETCH FIRST 1 ROWS ONLY"
    )

    explanation = _assert_query_matches_baseline(semantic_layer, sql, baseline_sql, ordered=True)

    assert explanation.chosen_plan == "direct_semantic"
    assert explanation.limit == 1
    assert "safe_order_pushdown" in explanation.applied_rules
    assert "safe_limit_pushdown" in explanation.applied_rules
    assert "FETCH" not in explanation.rewritten_sql
    assert "LIMIT 1" in explanation.rewritten_sql


def test_sql_server_top_n_rewrites_through_limit_pushdown(semantic_layer):
    rewriter = QueryRewriter(semantic_layer.graph, dialect="tsql")
    sql = """
        SELECT TOP 1 status, revenue
        FROM (SELECT orders.revenue, orders.status FROM orders) sq
        ORDER BY revenue DESC
    """

    explanation = rewriter.explain(sql)

    assert explanation.chosen_plan == "direct_semantic"
    assert "safe_limit_pushdown" in explanation.applied_rules
    assert "safe_order_pushdown" in explanation.applied_rules
    assert "TOP 1" in explanation.rewritten_sql
    assert " AS sq" not in explanation.rewritten_sql


@pytest.mark.parametrize(
    ("window_fn", "expected_reason"),
    [
        ("RANK", "only ROW_NUMBER is supported"),
        ("DENSE_RANK", "only ROW_NUMBER is supported"),
    ],
)
def test_global_topn_rejects_rank_tie_semantics(semantic_layer, window_fn, expected_reason):
    sql = f"""
        SELECT status, revenue
        FROM (
            SELECT status, revenue, {window_fn}() OVER (ORDER BY revenue DESC) AS rank_value
            FROM (SELECT orders.revenue, orders.status FROM orders) semantic_result
        ) ranked
        WHERE rank_value <= 1
        ORDER BY revenue DESC
    """

    explanation = semantic_layer.explain_sql(sql)

    assert explanation.chosen_plan == "semantic_plus_postprocess"
    assert explanation.rejected_rules["global_row_number_topn"] == expected_reason
    assert "semantic_island_optimization" in explanation.applied_rules


def test_global_topn_rejects_outer_projection_of_rank_column(semantic_layer):
    sql = """
        SELECT status, revenue, rn
        FROM (
            SELECT status, revenue, ROW_NUMBER() OVER (ORDER BY revenue DESC) AS rn
            FROM (SELECT orders.revenue, orders.status FROM orders) semantic_result
        ) ranked
        WHERE rn <= 1
        ORDER BY revenue DESC
    """

    explanation = semantic_layer.explain_sql(sql)

    assert explanation.chosen_plan == "semantic_plus_postprocess"
    assert (
        explanation.rejected_rules["global_row_number_topn"] == "outer projection 'rn' is not an inner semantic field"
    )
    assert "semantic_island_optimization" in explanation.applied_rules


def test_union_all_semantic_branches_use_preaggregations(semantic_layer):
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
    sql = """
        SELECT status, revenue
        FROM (SELECT orders.revenue, orders.status FROM orders WHERE orders.status = 'completed') completed
        UNION ALL
        SELECT status, revenue
        FROM (SELECT orders.revenue, orders.status FROM orders WHERE orders.status = 'pending') pending
    """
    baseline_sql = (
        "SELECT status, revenue FROM "
        + _subquery(
            _compiled_semantic_sql(
                semantic_layer,
                "SELECT orders.revenue, orders.status FROM orders WHERE orders.status = 'completed'",
                use_preaggregations=False,
            )
        )
        + " completed UNION ALL SELECT status, revenue FROM "
        + _subquery(
            _compiled_semantic_sql(
                semantic_layer,
                "SELECT orders.revenue, orders.status FROM orders WHERE orders.status = 'pending'",
                use_preaggregations=False,
            )
        )
        + " pending"
    )

    explanation = _assert_query_matches_baseline(semantic_layer, sql, baseline_sql)
    candidates = _candidate_by_name(explanation)

    assert explanation.chosen_plan == "semantic_plus_postprocess"
    assert candidates["set_operation_branch_optimization"].valid is True
    assert len(explanation.semantic_islands) == 2
    assert {island["chosen_plan"] for island in explanation.semantic_islands} == {"single_model_preaggregation"}
    assert "set_operation_branch_optimization" in explanation.applied_rules
    assert explanation.rewritten_sql.count("orders_preagg_by_status") == 2
    assert "UNION ALL" in explanation.rewritten_sql


def test_hex_style_union_preview_cte_preserves_outer_limit(semantic_layer):
    sql = """
        WITH query AS (
            SELECT status
            FROM (SELECT orders.status FROM orders WHERE orders.status = 'completed') completed
            UNION ALL
            SELECT status
            FROM (SELECT orders.status FROM orders WHERE orders.status = 'pending') pending
        )
        SELECT * FROM query LIMIT 2
    """
    completed_status = _compiled_semantic_sql(
        semantic_layer,
        "SELECT orders.status FROM orders WHERE orders.status = 'completed'",
    )
    pending_status = _compiled_semantic_sql(
        semantic_layer,
        "SELECT orders.status FROM orders WHERE orders.status = 'pending'",
    )
    baseline_sql = (
        "WITH query AS (SELECT status FROM "
        + _subquery(completed_status)
        + " completed UNION ALL SELECT status FROM "
        + _subquery(pending_status)
        + " pending) SELECT * FROM query LIMIT 2"
    )

    explanation = _assert_query_matches_baseline(semantic_layer, sql, baseline_sql)

    assert explanation.chosen_plan == "semantic_plus_postprocess"
    assert len(explanation.semantic_islands) == 2
    assert "set_operation_branch_optimization" in explanation.applied_rules
    assert "SELECT * FROM query LIMIT 2" in explanation.rewritten_sql


def test_set_operation_preserves_raw_branch(semantic_layer):
    sql = """
        SELECT status, revenue
        FROM (SELECT orders.revenue, orders.status FROM orders WHERE orders.status = 'completed') completed
        UNION ALL
        SELECT 'manual' AS status, 0 AS revenue
    """
    baseline_sql = (
        "SELECT status, revenue FROM "
        + _subquery(
            _compiled_semantic_sql(
                semantic_layer,
                "SELECT orders.revenue, orders.status FROM orders WHERE orders.status = 'completed'",
            )
        )
        + " completed UNION ALL SELECT 'manual' AS status, 0 AS revenue"
    )

    explanation = _assert_query_matches_baseline(semantic_layer, sql, baseline_sql)

    assert explanation.chosen_plan == "semantic_plus_postprocess"
    assert len(explanation.semantic_islands) == 1
    assert "SELECT 'manual' AS status, 0 AS revenue" in explanation.rewritten_sql


def test_set_operation_mismatched_branch_projection_is_rejected(semantic_layer):
    rewriter = QueryRewriter(semantic_layer.graph, dialect="duckdb")
    sql = """
        SELECT status
        FROM (SELECT orders.status FROM orders) left_branch
        UNION ALL
        SELECT status, revenue
        FROM (SELECT orders.revenue, orders.status FROM orders) right_branch
    """

    explanation = rewriter.explain(sql)

    assert explanation.chosen_plan == "passthrough_plain_sql"
    assert explanation.semantic_islands == []
    assert explanation.rejected_rules["set_operation_branch_optimization"] == (
        "set operation branch projections do not align"
    )


def test_set_operation_branch_order_by_is_rejected(semantic_layer):
    sql = """
        SELECT orders.revenue, orders.status FROM orders ORDER BY orders.status
        UNION ALL
        SELECT orders.revenue, orders.status FROM orders
    """

    explanation = semantic_layer.explain_sql(sql)

    assert explanation.chosen_plan == "passthrough_plain_sql"
    assert explanation.semantic_islands == []
    assert explanation.rejected_rules["set_operation_branch_optimization"] == (
        "branch-level ORDER BY is not safe to optimize inside a set operation"
    )


def test_global_row_number_topn_rejects_partitioned_rank(semantic_layer):
    rewriter = QueryRewriter(semantic_layer.graph, dialect="duckdb")
    sql = """
        SELECT status, revenue
        FROM (
            SELECT
                status,
                revenue,
                ROW_NUMBER() OVER (PARTITION BY status ORDER BY revenue DESC) AS rn
            FROM (SELECT orders.revenue, orders.status FROM orders) semantic_result
        ) ranked
        WHERE rn <= 1
    """

    explanation = rewriter.explain(sql)

    assert explanation.chosen_plan == "semantic_plus_postprocess"
    assert explanation.rejected_rules["global_row_number_topn"] == "partitioned ROW_NUMBER is not global"


def test_projection_width_reduction_omits_unused_primary_key(semantic_layer):
    sql = "SELECT orders.revenue, orders.status FROM orders"

    explanation = semantic_layer.explain_sql(sql)

    assert explanation.chosen_plan == "direct_semantic"
    assert "id AS id" not in explanation.rewritten_sql
    assert "customer_id AS customer_id" not in explanation.rewritten_sql
    assert "status AS status" in explanation.rewritten_sql
    assert "amount AS revenue_raw" in explanation.rewritten_sql


def test_projection_width_reduction_keeps_join_keys_for_joined_dimensions(semantic_layer):
    sql = "SELECT orders.revenue, customers.region FROM orders"

    explanation = semantic_layer.explain_sql(sql)

    assert explanation.chosen_plan == "direct_semantic"
    assert "customer_id AS customer_id" in explanation.rewritten_sql
    assert "id AS id" in explanation.rewritten_sql
    assert _sorted_rows(_rows(semantic_layer.sql(sql))) == _sorted_rows(
        _rows(semantic_layer.query(metrics=["orders.revenue"], dimensions=["customers.region"]))
    )


def test_projection_width_reduction_keeps_count_distinct_key_state(semantic_layer):
    orders = semantic_layer.get_model("orders")
    orders.metrics.append(Metric(name="unique_orders", agg="count_distinct"))
    sql = "SELECT orders.unique_orders, orders.status FROM orders ORDER BY orders.status"

    explanation = semantic_layer.explain_sql(sql)
    rows = _rows(semantic_layer.sql(sql))

    assert explanation.chosen_plan == "direct_semantic"
    assert "id AS unique_orders_raw" in explanation.rewritten_sql
    assert "id AS id" not in explanation.rewritten_sql
    assert rows == [
        {"status": "completed", "unique_orders": 2},
        {"status": "pending", "unique_orders": 1},
    ]


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT revenue * 2 AS doubled FROM (SELECT orders.revenue, orders.status FROM orders) sq",
        "SELECT revenue, ROW_NUMBER() OVER () AS rn FROM (SELECT orders.revenue, orders.status FROM orders) sq",
        "SELECT DISTINCT status FROM (SELECT orders.revenue, orders.status FROM orders) sq",
        "SELECT revenue FROM (SELECT orders.revenue, orders.status FROM orders) sq",
        "SELECT * FROM (SELECT orders.revenue, orders.status FROM orders LIMIT 1) sq WHERE status = 'completed'",
        "SELECT * FROM (SELECT orders.revenue, orders.status FROM orders) sq ORDER BY 1",
        "SELECT * FROM (SELECT orders.revenue, orders.status FROM orders) sq ORDER BY status || ''",
        "SELECT * FROM (SELECT orders.revenue, orders.status FROM orders) sq ORDER BY status DESC NULLS FIRST",
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
