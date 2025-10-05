"""Test relative date integration with queries."""

from sidemantic import Dimension, Metric, Model, SemanticLayer


def test_relative_date_in_filter():
    """Test using relative date expression in filter."""
    layer = SemanticLayer()

    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="created_at", type="time", granularity="day"),
            Dimension(name="status", type="categorical"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    layer.add_model(orders)

    # Filter with relative date
    sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.status"],
        filters=["orders_cte.created_at >= 'last 7 days'"],
    )

    print("SQL with relative date filter:")
    print(sql)

    # Should convert to SQL expression
    assert "CURRENT_DATE - 7" in sql
    assert "'last 7 days'" not in sql  # Original string should be replaced


def test_multiple_relative_date_filters():
    """Test multiple relative date filters."""
    layer = SemanticLayer()

    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="created_at", type="time", granularity="day"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    layer.add_model(orders)

    sql = layer.compile(
        metrics=["orders.revenue"],
        filters=["orders_cte.created_at >= 'last 30 days'", "orders_cte.created_at <= 'today'"],
    )

    # Should convert both
    assert "CURRENT_DATE - 30" in sql
    assert "CURRENT_DATE" in sql


def test_this_month_filter():
    """Test this month relative date."""
    layer = SemanticLayer()

    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="created_at", type="time", granularity="day"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    layer.add_model(orders)

    sql = layer.compile(
        metrics=["orders.revenue"], filters=["orders_cte.created_at = 'this month'"]
    )

    # Should expand to range (case insensitive since SQLGlot may uppercase)
    sql_upper = sql.upper()
    assert (
        "DATE_TRUNC('MONTH', CURRENT_DATE)" in sql_upper
        or "DATE_TRUNC('month', CURRENT_DATE)" in sql
    )
    assert "INTERVAL" in sql_upper and "MONTH" in sql_upper


def test_non_relative_date_unchanged():
    """Test that non-relative date expressions are unchanged."""
    layer = SemanticLayer()

    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="created_at", type="time", granularity="day"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    layer.add_model(orders)

    # Regular date literal
    sql = layer.compile(
        metrics=["orders.revenue"], filters=["orders_cte.created_at >= '2024-01-01'"]
    )

    # Should remain unchanged
    assert "'2024-01-01'" in sql or "2024-01-01" in sql


def test_mixed_filters():
    """Test mix of relative and absolute date filters."""
    layer = SemanticLayer()

    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="created_at", type="time", granularity="day"),
            Dimension(name="status", type="categorical"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    layer.add_model(orders)

    sql = layer.compile(
        metrics=["orders.revenue"],
        filters=["orders_cte.created_at >= 'last 7 days'", "orders_cte.status = 'completed'"],
    )

    # Relative date should be converted
    assert "CURRENT_DATE - 7" in sql
    # Regular filter should remain
    assert "status = 'completed'" in sql
