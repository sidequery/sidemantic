"""Compile-latency benchmark for the Python SQL generator.

Guards against regressions in the sqlglot-heavy compile path. The generator builds
SQL fragments and re-parses them via sqlglot many times per compile, so this test
times a representative 2-model join compile and asserts a generous, non-flaky ceiling.
The actual median/p95 are printed so regressions are visible in test output.
"""

import statistics
import time

from sidemantic import Dimension, Metric, Model, Relationship, SemanticLayer


def _build_layer() -> SemanticLayer:
    """Build a 2-model layer joined via a relationship (orders -> customers)."""
    layer = SemanticLayer()
    orders = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical", sql="status"),
            Dimension(name="order_date", type="time", granularity="day", sql="order_date"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
            Metric(name="order_count", agg="count"),
        ],
        relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
    )
    customers = Model(
        name="customers",
        table="customers",
        primary_key="customer_id",
        dimensions=[
            Dimension(name="region", type="categorical", sql="region"),
        ],
    )
    layer.add_model(orders)
    layer.add_model(customers)
    return layer


def _compile_once(layer: SemanticLayer) -> str:
    return layer.compile(
        metrics=["orders.revenue", "orders.order_count"],
        dimensions=["orders.status", "orders.order_date__month", "customers.region"],
        filters=["orders.status = 'completed'"],
        order_by=["orders.revenue"],
        limit=100,
    )


def test_compile_latency_benchmark(capsys):
    layer = _build_layer()

    # Sanity: the query actually compiles and joins the two models.
    sql = _compile_once(layer)
    assert "orders_cte" in sql
    assert "customers_cte" in sql

    # Warm up (module-level caches, dialect init, etc.).
    for _ in range(5):
        _compile_once(layer)

    timings = []
    for _ in range(50):
        start = time.perf_counter()
        _compile_once(layer)
        timings.append((time.perf_counter() - start) * 1000.0)

    timings.sort()
    median = statistics.median(timings)
    p95 = timings[int(len(timings) * 0.95) - 1]

    with capsys.disabled():
        print(f"\ncompile latency: median={median:.3f}ms p95={p95:.3f}ms (n={len(timings)})")

    # Generous, non-flaky ceiling. Measured post-optimization median is ~10ms without
    # coverage instrumentation and ~17ms under pytest-cov (the repo's default config
    # inflates per-line timing 2-3x). Pre-optimization was ~26ms bare / ~45ms+ under cov,
    # so this ceiling still catches a real regression while never flaking in CI.
    assert median < 40.0, f"compile median {median:.3f}ms exceeded 40ms ceiling"
