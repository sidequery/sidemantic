"""Benchmark semantic SQL wrapper rewrites against legacy wrapper-shaped SQL.

Run with:
    uv run scripts/benchmark_semantic_sql_planner.py --rows 200000 --iterations 5

The baseline SQL here approximates the old post-process shape: compile the
inner semantic query, then apply the user wrapper outside it. The optimized SQL
is produced by QueryRewriter.explain() on the wrapped semantic SQL.
"""

from __future__ import annotations

import argparse
import json
import statistics
import time
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Literal

from sidemantic import Dimension, Metric, Model, PreAggregation, Relationship, SemanticLayer
from sidemantic.sql.query_rewriter import QueryRewriter


@dataclass(frozen=True)
class BenchmarkCase:
    name: str
    wrapped_sql: str
    baseline_sql: Callable[[SemanticLayer], str]
    use_preaggregations: bool = False
    case_type: Literal["smoke", "performance"] = "smoke"
    expected_plan: str | None = None
    expected_rules: tuple[str, ...] = ()
    expected_sql_contains: str | tuple[str, ...] | None = None
    forbidden_sql_contains: str | tuple[str, ...] | None = None
    min_speedup: float | None = None


def _normalize(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    return value


def _fetch_dicts(layer: SemanticLayer, sql: str) -> list[dict[str, Any]]:
    result = layer.conn.execute(sql)
    columns = [col[0] for col in result.description]
    return [dict(zip(columns, [_normalize(value) for value in row])) for row in result.fetchall()]


def _median_ms(layer: SemanticLayer, sql: str, iterations: int) -> float:
    layer.conn.execute(sql).fetchall()
    timings: list[float] = []
    for _ in range(iterations):
        start = time.perf_counter()
        layer.conn.execute(sql).fetchall()
        timings.append((time.perf_counter() - start) * 1000)
    return statistics.median(timings)


def _inner_sql(layer: SemanticLayer, sql: str, use_preaggregations: bool = False) -> str:
    return QueryRewriter(
        layer.graph,
        dialect=layer.dialect,
        use_preaggregations=use_preaggregations,
    ).rewrite(sql)


def _subquery(sql: str) -> str:
    return "(\n" + sql.rstrip() + "\n)"


def _fragments(value: str | tuple[str, ...] | None) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    return value


def _build_layer(row_count: int) -> SemanticLayer:
    layer = SemanticLayer(auto_register=False, use_preaggregations=True)
    orders = Model(
        name="orders",
        table="orders",
        primary_key="wide_id",
        dimensions=[
            Dimension(name="customer_id", type="numeric", sql="customer_id"),
            Dimension(name="status", type="categorical", sql="status"),
            Dimension(name="order_date", type="time", sql="order_date", granularity="day"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
            Metric(name="count", agg="count"),
        ],
        relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
        pre_aggregations=[
            PreAggregation(
                name="by_status",
                measures=["revenue"],
                dimensions=["status"],
            ),
            PreAggregation(
                name="by_status_day",
                measures=["revenue"],
                dimensions=["status"],
                time_dimension="order_date",
                granularity="day",
            ),
            PreAggregation(
                name="daily_revenue",
                measures=["revenue"],
                time_dimension="order_date",
                granularity="day",
            ),
            PreAggregation(
                name="total_revenue",
                measures=["revenue"],
                dimensions=[],
            ),
            PreAggregation(
                name="by_customer",
                measures=["revenue"],
                dimensions=["customer_id"],
            ),
        ],
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
        pre_aggregations=[
            PreAggregation(
                name="by_region",
                measures=["count"],
                dimensions=["region"],
            ),
            PreAggregation(
                name="total_count",
                measures=["count"],
                dimensions=[],
            ),
        ],
    )
    layer.add_model(orders)
    layer.add_model(customers)
    layer.add_metric(Metric(name="running_total_revenue", type="cumulative", sql="orders.revenue"))

    layer.conn.execute("""
        CREATE TABLE customers AS
        SELECT
            i::INTEGER AS id,
            CASE WHEN i % 2 = 0 THEN 'US' ELSE 'EU' END AS region,
            CASE WHEN i % 5 = 0 THEN 'premium' ELSE 'standard' END AS tier
        FROM range(1, 1001) AS t(i)
    """)
    layer.conn.execute(f"""
        CREATE TABLE orders AS
        SELECT
            i::INTEGER AS id,
            LPAD(CAST(i AS VARCHAR), 512, '0') AS wide_id,
            (((i - 1) % 1000) + 1)::INTEGER AS customer_id,
            CASE
                WHEN i % 3 = 0 THEN 'completed'
                WHEN i % 3 = 1 THEN 'pending'
                ELSE 'returned'
            END AS status,
            DATE '2024-01-01' + CAST(i % 365 AS INTEGER) AS order_date,
            CAST((i % 1000) + 1 AS DECIMAL(10, 2)) AS amount
        FROM range(1, {row_count + 1}) AS t(i)
    """)
    layer.conn.execute("""
        CREATE TABLE orders_preagg_by_status AS
        SELECT
            status,
            SUM(amount) AS revenue_raw
        FROM orders
        GROUP BY status
    """)
    layer.conn.execute("""
        CREATE TABLE orders_preagg_by_status_day AS
        SELECT
            status,
            DATE_TRUNC('day', order_date) AS order_date_day,
            SUM(amount) AS revenue_raw
        FROM orders
        GROUP BY 1, 2
    """)
    layer.conn.execute("""
        CREATE TABLE orders_preagg_daily_revenue AS
        SELECT
            DATE_TRUNC('day', order_date) AS order_date_day,
            SUM(amount) AS revenue_raw
        FROM orders
        GROUP BY 1
    """)
    layer.conn.execute("""
        CREATE TABLE orders_preagg_total_revenue AS
        SELECT
            SUM(amount) AS revenue_raw
        FROM orders
    """)
    layer.conn.execute("""
        CREATE TABLE orders_preagg_by_customer AS
        SELECT
            customer_id,
            SUM(amount) AS revenue_raw
        FROM orders
        GROUP BY customer_id
    """)
    layer.conn.execute("""
        CREATE TABLE customers_preagg_by_region AS
        SELECT
            region,
            COUNT(*) AS count_raw
        FROM customers
        GROUP BY region
    """)
    layer.conn.execute("""
        CREATE TABLE customers_preagg_total_count AS
        SELECT
            COUNT(*) AS count_raw
        FROM customers
    """)
    return layer


def _cases(layer: SemanticLayer) -> list[BenchmarkCase]:
    return [
        BenchmarkCase(
            name="safe_filter_pushdown",
            wrapped_sql="""
                SELECT *
                FROM (SELECT orders.revenue, orders.status FROM orders) sq
                WHERE status = 'completed'
                ORDER BY status
            """,
            baseline_sql=lambda layer: "SELECT * FROM "
            + _subquery(
                _inner_sql(layer, "SELECT orders.revenue, orders.status FROM orders", use_preaggregations=False)
            )
            + " sq WHERE status = 'completed' ORDER BY status",
            expected_plan="direct_semantic",
            expected_rules=("safe_filter_pushdown",),
            forbidden_sql_contains="FROM (",
        ),
        BenchmarkCase(
            name="wrapper_metric_filter_having_pushdown",
            wrapped_sql="""
                SELECT *
                FROM (SELECT orders.revenue, orders.status FROM orders) sq
                WHERE revenue > 33000000
                ORDER BY status
            """,
            baseline_sql=lambda layer: "SELECT * FROM "
            + _subquery(
                _inner_sql(layer, "SELECT orders.revenue, orders.status FROM orders", use_preaggregations=False)
            )
            + " sq WHERE revenue > 33000000 ORDER BY status",
            expected_plan="direct_semantic",
            expected_rules=("safe_metric_filter_having_pushdown",),
            expected_sql_contains="HAVING",
            forbidden_sql_contains="FROM (",
        ),
        BenchmarkCase(
            name="safe_order_limit_pushdown",
            wrapped_sql="""
                SELECT *
                FROM (SELECT orders.revenue, orders.status FROM orders) sq
                ORDER BY status DESC
                LIMIT 2
            """,
            baseline_sql=lambda layer: "SELECT * FROM "
            + _subquery(
                _inner_sql(layer, "SELECT orders.revenue, orders.status FROM orders", use_preaggregations=False)
            )
            + " sq ORDER BY status DESC LIMIT 2",
            expected_plan="direct_semantic",
            expected_rules=("safe_order_pushdown", "safe_limit_pushdown"),
            forbidden_sql_contains="FROM (",
        ),
        BenchmarkCase(
            name="linear_cte_chain_preaggregation",
            wrapped_sql="""
                WITH base AS (
                    SELECT orders.revenue, orders.status FROM orders
                ),
                filtered AS (
                    SELECT * FROM base WHERE status = 'completed'
                )
                SELECT status, revenue FROM filtered ORDER BY revenue DESC
            """,
            baseline_sql=lambda layer: "WITH base AS "
            + _subquery(
                _inner_sql(layer, "SELECT orders.revenue, orders.status FROM orders", use_preaggregations=False)
            )
            + ", filtered AS (SELECT * FROM base WHERE status = 'completed') "
            + "SELECT status, revenue FROM filtered ORDER BY revenue DESC",
            use_preaggregations=True,
            case_type="performance",
            expected_plan="single_model_preaggregation",
            expected_rules=("linear_cte_chain_flattening", "safe_filter_pushdown", "preaggregation_route_selection"),
            expected_sql_contains=("orders_preagg_by_status", "WHERE status = 'completed'"),
            forbidden_sql_contains=("WITH base", "FROM orders\n"),
            min_speedup=1.2,
        ),
        BenchmarkCase(
            name="multi_semantic_cte_island_preaggregation",
            wrapped_sql="""
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
            """,
            baseline_sql=lambda layer: "WITH orders_agg AS "
            + _subquery(
                _inner_sql(layer, "SELECT orders.revenue, orders.status FROM orders", use_preaggregations=False)
            )
            + ", customers_agg AS "
            + _subquery(
                _inner_sql(layer, "SELECT customers.count, customers.region FROM customers", use_preaggregations=False)
            )
            + " SELECT o.status, c.region, o.revenue, c.count"
            + " FROM orders_agg o JOIN customers_agg c ON o.status IS NOT NULL"
            + " ORDER BY o.status, c.region",
            use_preaggregations=True,
            case_type="performance",
            expected_plan="semantic_plus_postprocess",
            expected_rules=("semantic_island_optimization", "preaggregation_route_selection"),
            expected_sql_contains=("orders_preagg_by_status", "customers_preagg_by_region"),
            forbidden_sql_contains=("FROM orders\n", "FROM customers\n"),
            min_speedup=1.05,
        ),
        BenchmarkCase(
            name="dimension_distinct_wrapper",
            wrapped_sql="""
                SELECT DISTINCT status
                FROM (SELECT orders.status FROM orders) sq
                ORDER BY status
            """,
            baseline_sql=lambda layer: "SELECT DISTINCT status FROM "
            + _subquery(_inner_sql(layer, "SELECT orders.status FROM orders", use_preaggregations=False))
            + " sq ORDER BY status",
            expected_plan="direct_semantic",
            expected_rules=("dimension_distinct_wrapper",),
            forbidden_sql_contains=("DISTINCT", "FROM ("),
        ),
        BenchmarkCase(
            name="dimension_slicer_null_search_limit",
            wrapped_sql="""
                SELECT DISTINCT status
                FROM (SELECT orders.status FROM orders) sq
                WHERE status IS NOT NULL AND LOWER(status) LIKE 'comp%'
                ORDER BY status
                LIMIT 1000
            """,
            baseline_sql=lambda layer: "SELECT DISTINCT status FROM "
            + _subquery(_inner_sql(layer, "SELECT orders.status FROM orders", use_preaggregations=False))
            + " sq WHERE status IS NOT NULL AND LOWER(status) LIKE 'comp%' ORDER BY status LIMIT 1000",
            case_type="performance",
            expected_plan="direct_semantic",
            expected_rules=("dimension_distinct_wrapper", "safe_filter_pushdown", "safe_limit_pushdown"),
            expected_sql_contains=("LOWER(status) LIKE 'comp%'", "LIMIT 1000"),
            forbidden_sql_contains=("DISTINCT", "FROM ("),
            min_speedup=1.05,
        ),
        BenchmarkCase(
            name="global_row_number_topn",
            wrapped_sql="""
                SELECT status, revenue
                FROM (
                    SELECT
                        status,
                        revenue,
                        ROW_NUMBER() OVER (ORDER BY revenue DESC) AS rn
                    FROM (SELECT orders.revenue, orders.status FROM orders) semantic_result
                ) ranked
                WHERE rn <= 2
                ORDER BY revenue DESC
            """,
            baseline_sql=lambda layer: (
                "SELECT status, revenue FROM ("
                "SELECT status, revenue, ROW_NUMBER() OVER (ORDER BY revenue DESC) AS rn FROM "
                + _subquery(
                    _inner_sql(layer, "SELECT orders.revenue, orders.status FROM orders", use_preaggregations=False)
                )
                + " semantic_result) ranked WHERE rn <= 2 ORDER BY revenue DESC"
            ),
            expected_plan="direct_semantic",
            expected_rules=("global_row_number_topn", "safe_order_pushdown", "safe_limit_pushdown"),
            expected_sql_contains="LIMIT 2",
            forbidden_sql_contains=("ROW_NUMBER", "FROM ("),
        ),
        BenchmarkCase(
            name="topn_pagination_preaggregation",
            wrapped_sql="""
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
            """,
            baseline_sql=lambda layer: (
                "SELECT status, revenue FROM ("
                "SELECT status, revenue, ROW_NUMBER() OVER (ORDER BY revenue DESC) AS rn FROM "
                + _subquery(
                    _inner_sql(layer, "SELECT orders.revenue, orders.status FROM orders", use_preaggregations=False)
                )
                + " semantic_result) ranked WHERE rn BETWEEN 2 AND 2 ORDER BY revenue DESC"
            ),
            use_preaggregations=True,
            case_type="performance",
            expected_plan="single_model_preaggregation",
            expected_rules=("global_row_number_topn", "safe_limit_pushdown", "preaggregation_route_selection"),
            expected_sql_contains=("orders_preagg_by_status", "LIMIT 1", "OFFSET 1"),
            forbidden_sql_contains=("ROW_NUMBER", "FROM orders\n"),
            min_speedup=1.05,
        ),
        BenchmarkCase(
            name="projection_metric_pruning",
            wrapped_sql="""
                SELECT status, revenue
                FROM (SELECT orders.revenue, orders.count, orders.status FROM orders) sq
                ORDER BY status
            """,
            baseline_sql=lambda layer: "SELECT status, revenue FROM "
            + _subquery(
                _inner_sql(
                    layer,
                    "SELECT orders.revenue, orders.count, orders.status FROM orders",
                    use_preaggregations=False,
                )
            )
            + " sq ORDER BY status",
            expected_plan="direct_semantic",
            expected_rules=("wrapper_projection_flattening",),
            forbidden_sql_contains="COUNT(*)",
        ),
        BenchmarkCase(
            name="projection_width_reduction_wide_key",
            wrapped_sql="""
                SELECT *
                FROM (SELECT orders.revenue, orders.status FROM orders) sq
                ORDER BY status
            """,
            baseline_sql=lambda _layer: """
                WITH orders_cte AS MATERIALIZED (
                  SELECT
                    wide_id AS wide_id,
                    status AS status,
                    amount AS revenue_raw
                  FROM orders
                )
                SELECT
                  orders_cte.status AS status,
                  SUM(orders_cte.revenue_raw) AS revenue
                FROM orders_cte
                GROUP BY orders_cte.status
                ORDER BY status
            """,
            case_type="performance",
            expected_plan="direct_semantic",
            expected_rules=("trivial_wrapper_flattening",),
            expected_sql_contains=("status AS status", "amount AS revenue_raw"),
            forbidden_sql_contains=("wide_id AS wide_id", "FROM ("),
            min_speedup=1.2,
        ),
        BenchmarkCase(
            name="wrapped_preaggregation",
            wrapped_sql="""
                SELECT *
                FROM (SELECT orders.revenue, orders.status FROM orders) sq
                ORDER BY status
            """,
            baseline_sql=lambda layer: "SELECT * FROM "
            + _subquery(
                _inner_sql(layer, "SELECT orders.revenue, orders.status FROM orders ORDER BY orders.status", False)
            )
            + " sq",
            use_preaggregations=True,
            case_type="performance",
            expected_plan="single_model_preaggregation",
            expected_rules=("preaggregation_route_selection",),
            expected_sql_contains="orders_preagg_by_status",
            forbidden_sql_contains="FROM orders\n",
            min_speedup=1.2,
        ),
        BenchmarkCase(
            name="root_having_metric_preagg",
            wrapped_sql="""
                SELECT orders.revenue, orders.status
                FROM orders
                HAVING orders.revenue > 33000000
                ORDER BY status
            """,
            baseline_sql=lambda layer: _inner_sql(
                layer,
                """
                    SELECT orders.revenue, orders.status
                    FROM orders
                    HAVING orders.revenue > 33000000
                    ORDER BY orders.status
                """,
                use_preaggregations=False,
            ),
            use_preaggregations=True,
            case_type="performance",
            expected_plan="single_model_preaggregation",
            expected_rules=("preaggregation_route_selection",),
            expected_sql_contains=("orders_preagg_by_status", "HAVING SUM(revenue_raw) > 33000000"),
            forbidden_sql_contains="FROM orders\n",
            min_speedup=1.2,
        ),
        BenchmarkCase(
            name="wrapped_fanout_strategy",
            wrapped_sql="SELECT * FROM (SELECT orders.revenue, customers.count FROM orders) sq",
            baseline_sql=lambda layer: "SELECT * FROM "
            + _subquery(
                _inner_sql(layer, "SELECT orders.revenue, customers.count FROM orders", use_preaggregations=False)
            )
            + " sq",
            expected_plan="fanout_preaggregation",
            expected_rules=("fanout_strategy_selection",),
        ),
        BenchmarkCase(
            name="aggregate_boundary_sum_rollup_raw",
            wrapped_sql="""
                SELECT status, SUM(revenue) AS revenue
                FROM (
                    SELECT orders.revenue, orders.status, orders.order_date__day FROM orders
                ) sq
                GROUP BY status
                ORDER BY status
            """,
            baseline_sql=lambda layer: "SELECT status, SUM(revenue) AS revenue FROM "
            + _subquery(
                _inner_sql(
                    layer,
                    "SELECT orders.revenue, orders.status, orders.order_date__day FROM orders",
                    use_preaggregations=False,
                )
            )
            + " sq GROUP BY status ORDER BY status",
            case_type="performance",
            expected_plan="direct_semantic",
            expected_rules=("aggregate_boundary_rollup", "additive_metric_rollup"),
            forbidden_sql_contains="orders.order_date__day",
            min_speedup=1.05,
        ),
        BenchmarkCase(
            name="aggregate_boundary_sum_rollup_preagg",
            wrapped_sql="""
                SELECT status, SUM(revenue) AS revenue
                FROM (
                    SELECT orders.revenue, orders.status, orders.order_date__day FROM orders
                ) sq
                GROUP BY status
                ORDER BY status
            """,
            baseline_sql=lambda layer: "SELECT status, SUM(revenue) AS revenue FROM "
            + _subquery(
                _inner_sql(
                    layer,
                    "SELECT orders.revenue, orders.status, orders.order_date__day FROM orders",
                    use_preaggregations=False,
                )
            )
            + " sq GROUP BY status ORDER BY status",
            use_preaggregations=True,
            case_type="performance",
            expected_plan="single_model_preaggregation",
            expected_rules=("aggregate_boundary_rollup", "additive_metric_rollup", "preaggregation_route_selection"),
            expected_sql_contains="orders_preagg_by_status",
            forbidden_sql_contains="FROM orders\n",
            min_speedup=1.2,
        ),
        BenchmarkCase(
            name="additive_total_union_preaggregation",
            wrapped_sql="""
                SELECT status, revenue
                FROM (SELECT orders.revenue, orders.status FROM orders) detail
                UNION ALL
                SELECT NULL AS status, SUM(revenue) AS revenue
                FROM (SELECT orders.revenue, orders.status FROM orders) detail_total
                ORDER BY status
            """,
            baseline_sql=lambda layer: "SELECT status, revenue FROM "
            + _subquery(
                _inner_sql(layer, "SELECT orders.revenue, orders.status FROM orders", use_preaggregations=False)
            )
            + " detail UNION ALL SELECT NULL AS status, SUM(revenue) AS revenue FROM "
            + _subquery(
                _inner_sql(layer, "SELECT orders.revenue, orders.status FROM orders", use_preaggregations=False)
            )
            + " detail_total ORDER BY status",
            use_preaggregations=True,
            case_type="performance",
            expected_plan="semantic_plus_postprocess",
            expected_rules=("semantic_island_optimization", "preaggregation_route_selection"),
            expected_sql_contains=("orders_preagg_by_status", "UNION ALL"),
            forbidden_sql_contains="FROM orders\n",
            min_speedup=1.05,
        ),
        BenchmarkCase(
            name="virtual_dataset_time_rls_preaggregation",
            wrapped_sql="""
                SELECT *
                FROM (
                    SELECT orders.revenue, orders.order_date__day, orders.status FROM orders
                ) virtual_table
                WHERE order_date__day >= DATE '2024-01-01'
                  AND order_date__day < DATE '2024-02-01'
                  AND status = 'completed'
                ORDER BY order_date__day
            """,
            baseline_sql=lambda layer: "SELECT * FROM "
            + _subquery(
                _inner_sql(
                    layer,
                    "SELECT orders.revenue, orders.order_date__day, orders.status FROM orders",
                    use_preaggregations=False,
                )
            )
            + " virtual_table WHERE order_date__day >= DATE '2024-01-01'"
            + " AND order_date__day < DATE '2024-02-01'"
            + " AND status = 'completed' ORDER BY order_date__day",
            use_preaggregations=True,
            case_type="performance",
            expected_plan="single_model_preaggregation",
            expected_rules=("safe_filter_pushdown", "preaggregation_route_selection"),
            expected_sql_contains=("orders_preagg_by_status_day", "WHERE"),
            forbidden_sql_contains="FROM orders\n",
            min_speedup=1.05,
        ),
        BenchmarkCase(
            name="conditional_pivot_preaggregation",
            wrapped_sql="""
                SELECT
                    SUM(CASE WHEN status = 'completed' THEN revenue ELSE 0 END) AS completed_revenue,
                    SUM(CASE WHEN status = 'pending' THEN revenue ELSE 0 END) AS pending_revenue
                FROM (
                    SELECT orders.revenue, orders.status FROM orders
                ) sq
            """,
            baseline_sql=lambda layer: "SELECT "
            + "SUM(CASE WHEN status = 'completed' THEN revenue ELSE 0 END) AS completed_revenue, "
            + "SUM(CASE WHEN status = 'pending' THEN revenue ELSE 0 END) AS pending_revenue FROM "
            + _subquery(
                _inner_sql(layer, "SELECT orders.revenue, orders.status FROM orders", use_preaggregations=False)
            )
            + " sq",
            use_preaggregations=True,
            case_type="performance",
            expected_plan="semantic_plus_postprocess",
            expected_rules=("conditional_aggregate_wrapper", "preaggregation_route_selection"),
            expected_sql_contains=("orders_preagg_by_status", "completed_revenue"),
            forbidden_sql_contains="FROM orders\n",
            min_speedup=1.05,
        ),
        BenchmarkCase(
            name="time_expression_grain_rollup_preaggregation",
            wrapped_sql="""
                SELECT DATE_TRUNC('month', order_date__day) AS order_month, SUM(revenue) AS revenue
                FROM (
                    SELECT orders.order_date__day, orders.revenue FROM orders
                ) sq
                GROUP BY 1
                ORDER BY order_month
            """,
            baseline_sql=lambda layer: "SELECT DATE_TRUNC('month', order_date__day) AS order_month, "
            + "SUM(revenue) AS revenue FROM "
            + _subquery(
                _inner_sql(
                    layer,
                    "SELECT orders.order_date__day, orders.revenue FROM orders",
                    use_preaggregations=False,
                )
            )
            + " sq GROUP BY 1 ORDER BY order_month",
            use_preaggregations=True,
            case_type="performance",
            expected_plan="single_model_preaggregation",
            expected_rules=("aggregate_boundary_rollup", "time_grain_rollup", "preaggregation_route_selection"),
            expected_sql_contains=("orders_preagg_daily_revenue", "DATE_TRUNC('MONTH', order_date_day)"),
            forbidden_sql_contains="FROM orders\n",
            min_speedup=1.05,
        ),
        BenchmarkCase(
            name="union_branch_semantic_islands_preaggregation",
            wrapped_sql="""
                SELECT status, revenue
                FROM (SELECT orders.revenue, orders.status FROM orders WHERE orders.status = 'completed') completed
                UNION ALL
                SELECT status, revenue
                FROM (SELECT orders.revenue, orders.status FROM orders WHERE orders.status = 'pending') pending
            """,
            baseline_sql=lambda layer: "SELECT status, revenue FROM "
            + _subquery(
                _inner_sql(
                    layer,
                    "SELECT orders.revenue, orders.status FROM orders WHERE orders.status = 'completed'",
                    use_preaggregations=False,
                )
            )
            + " completed UNION ALL SELECT status, revenue FROM "
            + _subquery(
                _inner_sql(
                    layer,
                    "SELECT orders.revenue, orders.status FROM orders WHERE orders.status = 'pending'",
                    use_preaggregations=False,
                )
            )
            + " pending",
            use_preaggregations=True,
            case_type="performance",
            expected_plan="semantic_plus_postprocess",
            expected_rules=("set_operation_branch_optimization", "preaggregation_route_selection"),
            expected_sql_contains=("orders_preagg_by_status", "UNION ALL"),
            forbidden_sql_contains="FROM orders\n",
            min_speedup=1.05,
        ),
        BenchmarkCase(
            name="window_inner_preaggregation",
            wrapped_sql="""
                SELECT *
                FROM (SELECT running_total_revenue, orders.order_date__day FROM metrics) sq
                ORDER BY order_date__day
            """,
            baseline_sql=lambda layer: "SELECT * FROM "
            + _subquery(
                _inner_sql(
                    layer,
                    "SELECT running_total_revenue, orders.order_date__day FROM metrics",
                    use_preaggregations=False,
                )
            )
            + " sq ORDER BY order_date__day",
            use_preaggregations=True,
            case_type="performance",
            expected_plan="window_metric",
            expected_rules=("safe_order_pushdown",),
            expected_sql_contains="orders_preagg_daily_revenue",
            forbidden_sql_contains="FROM orders\n",
            min_speedup=1.05,
        ),
        BenchmarkCase(
            name="fanout_child_preaggregation",
            wrapped_sql="""
                SELECT *
                FROM (
                    SELECT orders.revenue AS total_revenue, customers.count AS customer_count FROM orders
                ) sq
            """,
            baseline_sql=lambda layer: "SELECT * FROM "
            + _subquery(
                _inner_sql(
                    layer,
                    "SELECT orders.revenue AS total_revenue, customers.count AS customer_count FROM orders",
                    use_preaggregations=False,
                )
            )
            + " sq",
            use_preaggregations=True,
            case_type="performance",
            expected_plan="fanout_preaggregation",
            expected_rules=("fanout_strategy_selection",),
            expected_sql_contains=("orders_preagg_total_revenue", "customers_preagg_total_count"),
            forbidden_sql_contains=("FROM orders\n", "FROM customers\n"),
            min_speedup=1.05,
        ),
        BenchmarkCase(
            name="fanout_join_key_preagg_region",
            wrapped_sql="""
                SELECT orders.revenue, customers.region
                FROM orders
                ORDER BY customers.region
            """,
            baseline_sql=lambda layer: _inner_sql(
                layer,
                "SELECT orders.revenue, customers.region FROM orders ORDER BY customers.region",
                use_preaggregations=False,
            ),
            use_preaggregations=True,
            case_type="performance",
            expected_plan="join_key_preaggregation",
            expected_rules=("join_key_preaggregation_route_selection",),
            expected_sql_contains=(
                "FROM customers AS customers",
                "LEFT JOIN orders_preagg_by_customer AS orders_rollup",
            ),
            forbidden_sql_contains="FROM orders\n",
            min_speedup=1.2,
        ),
    ]


def run_benchmarks(row_count: int, iterations: int) -> list[dict[str, Any]]:
    layer = _build_layer(row_count)
    results = []
    for case in _cases(layer):
        rewriter = QueryRewriter(
            layer.graph,
            dialect=layer.dialect,
            use_preaggregations=case.use_preaggregations,
        )
        explanation = rewriter.explain(case.wrapped_sql)
        optimized_sql = explanation.rewritten_sql
        baseline_sql = case.baseline_sql(layer)

        optimized_rows = _fetch_dicts(layer, optimized_sql)
        baseline_rows = _fetch_dicts(layer, baseline_sql)
        rows_equal = optimized_rows == baseline_rows
        baseline_ms = _median_ms(layer, baseline_sql, iterations)
        optimized_ms = _median_ms(layer, optimized_sql, iterations)
        speedup = baseline_ms / optimized_ms if optimized_ms else None
        expected_fragments = _fragments(case.expected_sql_contains)
        forbidden_fragments = _fragments(case.forbidden_sql_contains)
        expected_fragments_present = (
            all(fragment in optimized_sql for fragment in expected_fragments) if expected_fragments else None
        )
        forbidden_fragments_absent = (
            all(fragment not in optimized_sql for fragment in forbidden_fragments) if forbidden_fragments else None
        )
        expected_rules_present = all(rule in explanation.applied_rules for rule in case.expected_rules)
        chosen_plan_matches = explanation.chosen_plan == case.expected_plan if case.expected_plan else None
        speedup_floor_met = speedup >= case.min_speedup if speedup is not None and case.min_speedup else None

        results.append(
            {
                "name": case.name,
                "case_type": case.case_type,
                "chosen_plan": explanation.chosen_plan,
                "expected_plan": case.expected_plan,
                "chosen_plan_matches": chosen_plan_matches,
                "expected_rules": list(case.expected_rules),
                "expected_rules_present": expected_rules_present,
                "expected_fragments": list(expected_fragments),
                "expected_fragments_present": expected_fragments_present,
                "forbidden_fragments": list(forbidden_fragments),
                "forbidden_fragments_absent": forbidden_fragments_absent,
                "applied_rules": explanation.applied_rules,
                "rows_equal": rows_equal,
                "baseline_ms": round(baseline_ms, 3),
                "optimized_ms": round(optimized_ms, 3),
                "speedup": round(speedup, 3) if speedup else None,
                "min_speedup": case.min_speedup,
                "speedup_floor_met": speedup_floor_met,
                "sql_changed": baseline_sql.strip() != optimized_sql.strip(),
            }
        )
    return results


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=200_000, help="Number of synthetic orders to generate")
    parser.add_argument("--iterations", type=int, default=5, help="Timing iterations per query")
    parser.add_argument(
        "--enforce-speedups",
        action="store_true",
        help="Fail when a performance case with min_speedup misses its floor",
    )
    args = parser.parse_args()

    results = run_benchmarks(row_count=args.rows, iterations=args.iterations)
    failed = [
        result
        for result in results
        if not result["rows_equal"]
        or result["chosen_plan_matches"] is False
        or result["expected_rules_present"] is False
        or result["expected_fragments_present"] is False
        or result["forbidden_fragments_absent"] is False
        or (args.enforce_speedups and result["speedup_floor_met"] is False)
    ]
    print(
        json.dumps(
            {
                "rows": args.rows,
                "iterations": args.iterations,
                "enforce_speedups": args.enforce_speedups,
                "results": results,
            },
            indent=2,
        )
    )
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
