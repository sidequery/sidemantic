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
from typing import Any

from sidemantic import Dimension, Metric, Model, PreAggregation, Relationship, SemanticLayer
from sidemantic.sql.query_rewriter import QueryRewriter


@dataclass(frozen=True)
class BenchmarkCase:
    name: str
    wrapped_sql: str
    baseline_sql: Callable[[SemanticLayer], str]
    use_preaggregations: bool = False
    expected_rule: str | None = None


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


def _build_layer(row_count: int) -> SemanticLayer:
    layer = SemanticLayer(auto_register=False, use_preaggregations=True)
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
        pre_aggregations=[
            PreAggregation(
                name="by_status",
                measures=["revenue"],
                dimensions=["status"],
            )
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
    )
    layer.add_model(orders)
    layer.add_model(customers)

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
            expected_rule="safe_filter_pushdown",
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
            expected_rule="safe_order_pushdown",
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
            expected_rule="wrapper_projection_flattening",
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
            expected_rule="preaggregation_route_selection",
        ),
        BenchmarkCase(
            name="wrapped_fanout_strategy",
            wrapped_sql="SELECT * FROM (SELECT orders.revenue, customers.count FROM orders) sq",
            baseline_sql=lambda layer: "SELECT * FROM "
            + _subquery(
                _inner_sql(layer, "SELECT orders.revenue, customers.count FROM orders", use_preaggregations=False)
            )
            + " sq",
            expected_rule="fanout_strategy_selection",
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

        results.append(
            {
                "name": case.name,
                "chosen_plan": explanation.chosen_plan,
                "expected_rule_present": case.expected_rule in explanation.applied_rules
                if case.expected_rule
                else None,
                "applied_rules": explanation.applied_rules,
                "rows_equal": rows_equal,
                "baseline_ms": round(baseline_ms, 3),
                "optimized_ms": round(optimized_ms, 3),
                "speedup": round(speedup, 3) if speedup else None,
                "sql_changed": baseline_sql.strip() != optimized_sql.strip(),
            }
        )
    return results


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=200_000, help="Number of synthetic orders to generate")
    parser.add_argument("--iterations", type=int, default=5, help="Timing iterations per query")
    args = parser.parse_args()

    results = run_benchmarks(row_count=args.rows, iterations=args.iterations)
    failed = [result for result in results if not result["rows_equal"] or result["expected_rule_present"] is False]
    print(json.dumps({"rows": args.rows, "iterations": args.iterations, "results": results}, indent=2))
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
