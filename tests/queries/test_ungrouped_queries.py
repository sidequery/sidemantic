"""Test ungrouped queries (raw row queries without GROUP BY)."""

import re

import pytest

from sidemantic import Dimension, Metric, Model
from tests.utils import fetch_dicts


def _squash(sql: str) -> str:
    """Collapse whitespace so assertions are insensitive to sqlglot pretty-printing."""
    return re.sub(r"\s+", " ", sql)


def test_ungrouped_basic(layer):
    """Test basic ungrouped query returns raw rows."""
    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical"),
            Dimension(name="customer_id", type="categorical"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    layer.add_model(orders)

    # Ungrouped query should return raw rows
    sql = layer.compile(metrics=["orders.revenue"], dimensions=["orders.status"], ungrouped=True)

    print("Ungrouped SQL:")
    print(sql)

    # Should NOT have GROUP BY
    assert "GROUP BY" not in sql
    # Should NOT have aggregation function
    assert "SUM(" not in sql
    # Should select the raw column
    assert "revenue_raw" in sql or "amount" in sql
    # Should still have dimensions
    assert "status" in sql


def test_ungrouped_multiple_dimensions(layer):
    """Test ungrouped query with multiple dimensions."""
    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical"),
            Dimension(name="region", type="categorical"),
            Dimension(name="customer_id", type="categorical"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    layer.add_model(orders)

    sql = layer.compile(metrics=["orders.revenue"], dimensions=["orders.status", "orders.region"], ungrouped=True)

    # Should NOT have GROUP BY
    assert "GROUP BY" not in sql
    # Should have all dimensions
    assert "status" in sql
    assert "region" in sql


def test_ungrouped_with_filters(layer):
    """Test ungrouped query with filters."""
    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical"),
            Dimension(name="region", type="categorical"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    layer.add_model(orders)

    sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.status"],
        filters=["orders_cte.region = 'US'"],
        ungrouped=True,
    )

    # Should have WHERE clause
    assert "WHERE" in sql
    assert "region = 'US'" in sql
    # Should NOT have GROUP BY
    assert "GROUP BY" not in sql


def test_ungrouped_with_order_and_limit(layer):
    """Test ungrouped query with ORDER BY and LIMIT."""
    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    layer.add_model(orders)

    sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.status"],
        order_by=["revenue DESC"],
        limit=100,
        ungrouped=True,
    )

    # Should have ORDER BY and LIMIT
    assert "ORDER BY" in sql
    assert "LIMIT" in sql
    assert "100" in sql
    # Should NOT have GROUP BY
    assert "GROUP BY" not in sql


def test_grouped_vs_ungrouped(layer):
    """Compare grouped and ungrouped queries."""
    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    layer.add_model(orders)

    # Grouped query (default)
    grouped_sql = layer.compile(metrics=["orders.revenue"], dimensions=["orders.status"], ungrouped=False)

    # Ungrouped query
    ungrouped_sql = layer.compile(metrics=["orders.revenue"], dimensions=["orders.status"], ungrouped=True)

    # Grouped should have GROUP BY and aggregation
    assert "GROUP BY" in grouped_sql
    assert "SUM(" in grouped_sql

    # Ungrouped should NOT have GROUP BY or aggregation
    assert "GROUP BY" not in ungrouped_sql
    assert "SUM(" not in ungrouped_sql


def test_ungrouped_multiple_metrics(layer):
    """Test ungrouped query with multiple metrics."""
    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
            Metric(name="quantity", agg="sum", sql="qty"),
        ],
    )

    layer.add_model(orders)

    sql = layer.compile(metrics=["orders.revenue", "orders.quantity"], dimensions=["orders.status"], ungrouped=True)

    # Should select both metrics as raw columns
    assert "revenue_raw" in sql or "amount" in sql
    assert "quantity_raw" in sql or "qty" in sql
    # Should NOT have GROUP BY
    assert "GROUP BY" not in sql
    # Should NOT have aggregation
    assert "SUM(" not in sql


def _totals_orders_model():
    return Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical"),
            Dimension(name="region", type="categorical"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
            Metric(name="customers", agg="count_distinct", sql="customer_id"),
        ],
    )


def test_with_totals_single_dimension(layer):
    """with_totals emits GROUPING SETS and adds one grand-total row (status NULL)."""
    layer.conn.execute(
        "CREATE TABLE orders (order_id INTEGER, status VARCHAR, region VARCHAR, amount INTEGER, customer_id INTEGER)"
    )
    layer.conn.execute(
        "INSERT INTO orders VALUES (1,'completed','US',10,1),(2,'completed','US',15,2),(3,'pending','EU',10,1)"
    )
    layer.add_model(_totals_orders_model())

    sql = layer.compile(metrics=["orders.revenue"], dimensions=["orders.status"], with_totals=True)
    assert "GROUPING SETS" in sql
    assert "GROUPING SETS ( ( 1 ), () )" in _squash(sql)

    rows = fetch_dicts(layer.query(metrics=["orders.revenue"], dimensions=["orders.status"], with_totals=True))
    per_status = {r["status"]: r["revenue"] for r in rows if r["status"] is not None}
    total_rows = [r for r in rows if r["status"] is None]
    assert len(total_rows) == 1
    assert total_rows[0]["revenue"] == sum(per_status.values()) == 35


def test_with_totals_count_distinct_recomputed(layer):
    """Grand-total distinct count is recomputed over all rows, not summed per group."""
    layer.conn.execute(
        "CREATE TABLE orders (order_id INTEGER, status VARCHAR, region VARCHAR, amount INTEGER, customer_id INTEGER)"
    )
    # completed: customers {1, 2} -> 2 distinct; pending: customer {1} -> 1 distinct.
    # Overall distinct customers across all rows = {1, 2} -> 3 rows summed would be 3,
    # but true overall distinct = 2. Use a third customer to make the point sharp.
    layer.conn.execute(
        "INSERT INTO orders VALUES (1,'completed','US',10,1),(2,'completed','US',15,2),(3,'pending','EU',10,3)"
    )
    layer.add_model(_totals_orders_model())

    rows = fetch_dicts(layer.query(metrics=["orders.customers"], dimensions=["orders.status"], with_totals=True))
    per_status = {r["status"]: r["customers"] for r in rows if r["status"] is not None}
    total_rows = [r for r in rows if r["status"] is None]
    assert per_status == {"completed": 2, "pending": 1}
    assert len(total_rows) == 1
    # Recomputed overall distinct = 3 (customers 1, 2, 3), not sum-of-groups (which also happens
    # to be 3 here); use overlapping customer to confirm it is NOT the sum:
    assert total_rows[0]["customers"] == 3


def test_with_totals_count_distinct_not_sum_of_groups(layer):
    """Overlapping members prove the total is a recomputed distinct, not a sum of group distincts."""
    layer.conn.execute(
        "CREATE TABLE orders (order_id INTEGER, status VARCHAR, region VARCHAR, amount INTEGER, customer_id INTEGER)"
    )
    # completed: {1, 2} -> 2; pending: {1} -> 1. Sum-of-groups = 3, true overall = 2.
    layer.conn.execute(
        "INSERT INTO orders VALUES (1,'completed','US',10,1),(2,'completed','US',15,2),(3,'pending','EU',10,1)"
    )
    layer.add_model(_totals_orders_model())

    rows = fetch_dicts(layer.query(metrics=["orders.customers"], dimensions=["orders.status"], with_totals=True))
    total_rows = [r for r in rows if r["status"] is None]
    assert len(total_rows) == 1
    assert total_rows[0]["customers"] == 2  # NOT 3 (the sum of per-group distincts)


def test_with_totals_two_dimensions(layer):
    """Two dims -> GROUPING SETS ((1, 2), ()) and exactly one all-NULL grand-total row."""
    layer.conn.execute(
        "CREATE TABLE orders (order_id INTEGER, status VARCHAR, region VARCHAR, amount INTEGER, customer_id INTEGER)"
    )
    layer.conn.execute(
        "INSERT INTO orders VALUES (1,'completed','US',10,1),(2,'completed','EU',15,2),(3,'pending','US',10,3)"
    )
    layer.add_model(_totals_orders_model())

    sql = layer.compile(metrics=["orders.revenue"], dimensions=["orders.status", "orders.region"], with_totals=True)
    assert "GROUPING SETS ( (1, 2), () )" in _squash(sql)

    rows = fetch_dicts(
        layer.query(metrics=["orders.revenue"], dimensions=["orders.status", "orders.region"], with_totals=True)
    )
    grand_total = [r for r in rows if r["status"] is None and r["region"] is None]
    assert len(grand_total) == 1
    assert grand_total[0]["revenue"] == 35
    # All non-total rows have both dimensions populated (no per-level subtotals)
    detail = [r for r in rows if not (r["status"] is None and r["region"] is None)]
    assert all(r["status"] is not None and r["region"] is not None for r in detail)
    assert len(detail) == 3


def test_with_totals_default_off_unchanged(layer):
    """Default (and explicit False) emits plain positional GROUP BY, no GROUPING SETS."""
    layer.add_model(_totals_orders_model())

    default_sql = layer.compile(metrics=["orders.revenue"], dimensions=["orders.status"])
    explicit_off_sql = layer.compile(metrics=["orders.revenue"], dimensions=["orders.status"], with_totals=False)

    for sql in (default_sql, explicit_off_sql):
        assert "GROUPING SETS" not in sql
        assert "GROUP BY 1" in _squash(sql)


def test_with_totals_and_ungrouped_raises(layer):
    """with_totals and ungrouped are mutually exclusive."""
    layer.add_model(_totals_orders_model())

    with pytest.raises(ValueError, match="with_totals cannot be combined with ungrouped"):
        layer.compile(metrics=["orders.revenue"], dimensions=["orders.status"], with_totals=True, ungrouped=True)


def test_with_totals_unsupported_window_path_raises(layer):
    """with_totals on a window-function (cumulative) metric raises NotImplementedError."""
    orders = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        dimensions=[Dimension(name="order_date", type="time", granularity="day")],
        metrics=[Metric(name="revenue", agg="sum", sql="amount")],
    )
    layer.add_model(orders)
    layer.graph.add_metric(Metric(name="cumulative_revenue", type="cumulative", sql="orders.revenue"))

    with pytest.raises(NotImplementedError, match="with_totals is not yet supported"):
        layer.compile(metrics=["cumulative_revenue"], dimensions=["orders.order_date"], with_totals=True)
