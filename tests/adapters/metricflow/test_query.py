"""Tests for MetricFlow adapter - query generation."""

import pytest

from sidemantic import SemanticLayer
from sidemantic.adapters.metricflow import MetricFlowAdapter

# =============================================================================
# QUERY TESTS
# =============================================================================


def test_query_imported_metricflow_example():
    """Test that we can compile queries from imported MetricFlow schema."""
    adapter = MetricFlowAdapter()
    graph = adapter.parse("tests/fixtures/metricflow/semantic_models.yml")

    layer = SemanticLayer()
    layer.graph = graph

    # Test basic measure query
    sql = layer.compile(metrics=["orders.revenue"])
    assert "SUM" in sql.upper()

    # Test with dimension
    sql = layer.compile(metrics=["orders.revenue", "orders.order_count"], dimensions=["orders.status"])
    assert "GROUP BY" in sql.upper()
    assert "status" in sql.lower()

    # Test cross-model query (only if join path exists)
    # Note: MetricFlow entities may not map 1:1 to model names
    try:
        sql = layer.compile(metrics=["orders.revenue"], dimensions=["customers.region"])
        assert "JOIN" in sql.upper()
        assert "customers" in sql.lower()
    except Exception:
        # Join path not configured, which is expected for some imports
        pass

    # Test graph-level ratio metric (if it exists and is queryable)
    if "average_order_value" in graph.metrics:
        avg_metric = graph.metrics["average_order_value"]
        # Ratio metrics should have numerator/denominator set
        if avg_metric.type == "ratio" and avg_metric.numerator and avg_metric.denominator:
            try:
                sql = layer.compile(metrics=["average_order_value"])
                assert sql  # Should generate valid SQL with ratio calculation
            except ValueError:
                # Some graph-level metrics may need model context to be queryable
                pass


def test_query_with_filter_metricflow():
    """Test that metric filters work from MetricFlow import."""
    adapter = MetricFlowAdapter()
    graph = adapter.parse("tests/fixtures/metricflow/semantic_models.yml")

    layer = SemanticLayer()
    layer.graph = graph

    # Query with filter
    sql = layer.compile(metrics=["orders.revenue"], filters=["orders.status = 'completed'"])
    assert "WHERE" in sql.upper()
    assert "status" in sql.lower()
    assert "completed" in sql.lower()


def test_inline_simple_metric_filter_is_applied():
    """A latest-spec inline simple metric with a ``filter`` scopes its aggregation.

    Regression: the filter was registered on the graph-level metric but the
    aggregation path ignored ``Metric.filters``, so a filtered metric like
    ``completed_revenue`` aggregated every row (``SUM(amount)``) and silently
    dropped the MetricFlow filter. The filter must be rendered via CASE WHEN so
    only matching rows contribute, and only for that metric.
    """
    import tempfile
    import textwrap
    from pathlib import Path

    import duckdb

    from tests.utils import fetch_dicts

    yml = textwrap.dedent("""
        models:
          - name: orders
            semantic_model:
              enabled: true
              name: orders
            columns:
              - name: order_id
                entity:
                  type: primary
                  name: order
              - name: order_status
                dimension:
                  type: categorical
              - name: amount
                dimension:
                  type: categorical
            metrics:
              - name: total_revenue
                type: simple
                agg: sum
                expr: amount
              - name: completed_revenue
                type: simple
                agg: sum
                expr: amount
                filter: "order_status = 'completed'"
              - name: completed_count
                type: simple
                agg: count
                expr: amount
                filter: "order_status = 'completed'"
    """)
    with tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False) as f:
        f.write(yml)
        path = Path(f.name)

    try:
        graph = MetricFlowAdapter().parse(path)
    finally:
        path.unlink(missing_ok=True)

    conn = duckdb.connect(":memory:")
    conn.execute(
        "CREATE TABLE orders AS SELECT * FROM (VALUES "
        "(1, 'completed', 100.0), (2, 'completed', 50.0), (3, 'pending', 999.0)) "
        "AS t(order_id, order_status, amount)"
    )

    layer = SemanticLayer(auto_register=False)
    layer.conn = conn
    layer.graph = graph

    # The filter renders as CASE WHEN, not a bare aggregate.
    sql = layer.compile(metrics=["completed_revenue"])
    assert "CASE WHEN" in sql.upper()

    # Filtered totals only include matching rows; the unfiltered metric includes all.
    row = fetch_dicts(layer.query(metrics=["total_revenue", "completed_revenue", "completed_count"]))[0]
    assert row["total_revenue"] == 1149.0
    assert row["completed_revenue"] == 150.0
    assert row["completed_count"] == 2

    # The per-metric filter does not leak into a sibling metric grouped by dimension.
    grouped = {
        r["order_status"]: r
        for r in fetch_dicts(
            layer.query(
                metrics=["total_revenue", "completed_revenue"],
                dimensions=["orders.order_status"],
            )
        )
    }
    assert grouped["pending"]["total_revenue"] == 999.0
    assert grouped["pending"]["completed_revenue"] in (None, 0)


def test_inline_metric_filter_qualified_in_join():
    """A filtered inline metric's columns are qualified to its owning model CTE.

    Regression: the metric filter was rendered with unqualified columns. When the
    metric is queried with a joined dimension whose CTE also exposes a same-named
    column, the unqualified filter column was ambiguous and the query failed to
    bind. The filter columns must be qualified with the owning model's CTE.
    """
    import tempfile
    import textwrap
    from pathlib import Path

    import duckdb

    from tests.utils import fetch_dicts

    yml = textwrap.dedent("""
        models:
          - name: orders
            semantic_model:
              enabled: true
              name: orders
            columns:
              - name: order_id
                entity:
                  type: primary
                  name: order
              - name: customer_id
                entity:
                  type: foreign
                  name: customer
              - name: status
                dimension:
                  type: categorical
              - name: amount
                dimension:
                  type: categorical
            metrics:
              - name: completed_revenue
                type: simple
                agg: sum
                expr: amount
                filter: "status = 'completed'"
          - name: customers
            semantic_model:
              enabled: true
              name: customers
            columns:
              - name: customer_id
                entity:
                  type: primary
                  name: customer
              - name: status
                dimension:
                  type: categorical
    """)
    with tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False) as f:
        f.write(yml)
        path = Path(f.name)

    try:
        graph = MetricFlowAdapter().parse(path)
    finally:
        path.unlink(missing_ok=True)

    conn = duckdb.connect(":memory:")
    conn.execute(
        "CREATE TABLE orders AS SELECT * FROM (VALUES "
        "(1, 10, 'completed', 100.0), (2, 11, 'pending', 50.0), (3, 10, 'completed', 25.0)) "
        "AS t(order_id, customer_id, status, amount)"
    )
    conn.execute(
        "CREATE TABLE customers AS SELECT * FROM (VALUES (10, 'active'), (11, 'active')) AS t(customer_id, status)"
    )

    layer = SemanticLayer(auto_register=False)
    layer.conn = conn
    layer.graph = graph

    # The filter column is qualified to the orders CTE (not the ambiguous bare name).
    sql = layer.compile(metrics=["completed_revenue"], dimensions=["customers.status"])
    assert "orders_cte.status" in sql

    # The join query binds and returns the filtered total.
    rows = fetch_dicts(layer.query(metrics=["completed_revenue"], dimensions=["customers.status"]))
    assert {r["status"]: r["completed_revenue"] for r in rows} == {"active": 125.0}


def test_inline_filtered_count_skips_null_expression():
    """A filtered ``count`` over an expression skips NULL values like the unfiltered count.

    Regression: the filtered count counted a constant 1 for every matching row,
    so rows with a NULL counted expression were included even though the
    unfiltered ``COUNT(expr)`` path skips them. A filtered count over an
    expression must count the expression (skipping NULLs); only a bare row count
    counts every matching row.
    """
    import tempfile
    import textwrap
    from pathlib import Path

    import duckdb

    from tests.utils import fetch_dicts

    yml = textwrap.dedent("""
        models:
          - name: orders
            semantic_model:
              enabled: true
              name: orders
            columns:
              - name: order_id
                entity:
                  type: primary
                  name: order
              - name: status
                dimension:
                  type: categorical
              - name: amount
                dimension:
                  type: categorical
            metrics:
              - name: amount_count
                type: simple
                agg: count
                expr: amount
              - name: completed_amount_count
                type: simple
                agg: count
                expr: amount
                filter: "status = 'completed'"
              - name: completed_row_count
                type: simple
                agg: count
                filter: "status = 'completed'"
    """)
    with tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False) as f:
        f.write(yml)
        path = Path(f.name)

    try:
        graph = MetricFlowAdapter().parse(path)
    finally:
        path.unlink(missing_ok=True)

    conn = duckdb.connect(":memory:")
    # Four completed rows, one with a NULL amount; one pending row.
    conn.execute(
        "CREATE TABLE orders AS SELECT * FROM (VALUES "
        "(1, 'completed', 100.0), (2, 'completed', NULL), (3, 'completed', 50.0), "
        "(4, 'completed', 25.0), (5, 'pending', 999.0)) "
        "AS t(order_id, status, amount)"
    )

    layer = SemanticLayer(auto_register=False)
    layer.conn = conn
    layer.graph = graph

    row = fetch_dicts(layer.query(metrics=["amount_count", "completed_amount_count", "completed_row_count"]))[0]
    # Unfiltered count(amount) skips the single NULL across all five rows.
    assert row["amount_count"] == 4
    # Filtered count(amount) keeps completed rows with a non-NULL amount.
    assert row["completed_amount_count"] == 3
    # A bare filtered count is a row count: every completed row.
    assert row["completed_row_count"] == 4


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
