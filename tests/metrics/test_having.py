"""Test metric-level HAVING clause propagation and ungrouped mode interaction."""

from sidemantic import Dimension, Metric, Model


def test_metric_having_basic(layer):
    """Test that a metric with having produces a HAVING clause."""
    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="region", type="categorical"),
        ],
        metrics=[
            Metric(
                name="revenue",
                agg="sum",
                sql="amount",
                having=["SUM(amount) > 100"],
            ),
        ],
    )

    layer.add_model(orders)

    sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.region"],
    )

    assert "HAVING" in sql
    assert "SUM(amount) > 100" in sql


def test_derived_metric_propagates_base_having(layer):
    """Test that a derived metric referencing a base metric with having still applies it."""
    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="region", type="categorical"),
        ],
        metrics=[
            Metric(
                name="revenue",
                agg="sum",
                sql="amount",
                having=["SUM(amount) > 100"],
            ),
            Metric(name="order_count", agg="count"),
        ],
    )

    layer.add_model(orders)

    layer.add_metric(
        Metric(
            name="revenue_per_order",
            type="ratio",
            numerator="orders.revenue",
            denominator="orders.order_count",
        )
    )

    sql = layer.compile(
        metrics=["revenue_per_order"],
        dimensions=["orders.region"],
    )

    # The base metric "orders.revenue" has having, it should propagate
    assert "HAVING" in sql
    assert "SUM(amount) > 100" in sql


def test_ungrouped_skips_having(layer):
    """Test that ungrouped queries with metrics that have having do NOT produce HAVING."""
    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="region", type="categorical"),
        ],
        metrics=[
            Metric(
                name="revenue",
                agg="sum",
                sql="amount",
                having=["SUM(amount) > 100"],
            ),
        ],
    )

    layer.add_model(orders)

    sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.region"],
        ungrouped=True,
    )

    # Ungrouped mode: no GROUP BY, no HAVING
    assert "GROUP BY" not in sql
    assert "HAVING" not in sql


def test_ungrouped_skips_query_level_having(layer):
    """Test that ungrouped queries also skip query-level metric filters that would be HAVING."""
    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="region", type="categorical"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    layer.add_model(orders)

    sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.region"],
        filters=["orders.revenue > 100"],
        ungrouped=True,
    )

    # Ungrouped mode: no GROUP BY, no HAVING
    assert "GROUP BY" not in sql
    assert "HAVING" not in sql


def test_ratio_metric_propagates_having_from_both_parts(layer):
    """Test that a ratio metric collects having from both numerator and denominator."""
    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="region", type="categorical"),
        ],
        metrics=[
            Metric(
                name="revenue",
                agg="sum",
                sql="amount",
                having=["SUM(amount) > 100"],
            ),
            Metric(
                name="order_count",
                agg="count",
                having=["COUNT(*) > 5"],
            ),
        ],
    )

    layer.add_model(orders)

    layer.add_metric(
        Metric(
            name="avg_order_value",
            type="ratio",
            numerator="orders.revenue",
            denominator="orders.order_count",
        )
    )

    sql = layer.compile(
        metrics=["avg_order_value"],
        dimensions=["orders.region"],
    )

    assert "HAVING" in sql
    assert "SUM(amount) > 100" in sql
    assert "COUNT(*) > 5" in sql


def test_metric_without_having_produces_no_having(layer):
    """Sanity check: metrics without having produce no HAVING clause."""
    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="region", type="categorical"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    layer.add_model(orders)

    sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.region"],
    )

    assert "HAVING" not in sql
