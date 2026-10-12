"""Test relative date integration with queries.

Relative dates expand to ``CURRENT_DATE`` expressions, which DuckDB compilation
then freezes to literals of one pinned instant (see ``sidemantic.sql.frozen_time``).
"""

from datetime import UTC, datetime, timedelta

import pytest

from sidemantic import Dimension, Metric, Model, SemanticLayer

FROZEN_AT = datetime(2026, 10, 11, 12, 0, tzinfo=UTC)
# CURRENT_DATE follows the local timezone, like DuckDB's default session TimeZone.
TODAY = FROZEN_AT.astimezone().date()


def _date_literal(days_ago: int = 0) -> str:
    return f"CAST('{(TODAY - timedelta(days=days_ago)).isoformat()}' AS DATE)"


@pytest.fixture
def layer():
    layer = SemanticLayer(auto_register=False)
    layer.clock = lambda: FROZEN_AT
    return layer


def test_relative_date_in_filter(layer):
    """Test using relative date expression in filter."""
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

    # Should convert to SQL expression, folded to a literal date
    assert f"created_at >= {_date_literal(7)}" in sql
    assert "CURRENT_DATE" not in sql
    assert "'last 7 days'" not in sql  # Original string should be replaced


def test_multiple_relative_date_filters(layer):
    """Test multiple relative date filters."""
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
    assert f"created_at >= {_date_literal(30)}" in sql
    assert f"created_at <= {_date_literal()}" in sql
    assert "CURRENT_DATE" not in sql


def test_this_month_filter(layer):
    """Test this month relative date."""
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

    sql = layer.compile(metrics=["orders.revenue"], filters=["orders_cte.created_at = 'this month'"])

    # Should expand to range (case insensitive since SQLGlot may uppercase)
    sql_upper = sql.upper()
    assert f"DATE_TRUNC('MONTH', {_date_literal()})" in sql_upper
    assert "INTERVAL" in sql_upper and "MONTH" in sql_upper


def test_less_than_relative_date(layer):
    """Test < against a relative date uses the period start, not the raw literal."""
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

    sql = layer.compile(metrics=["orders.revenue"], filters=["orders_cte.created_at < 'this month'"])

    assert "DATE_TRUNC" in sql.upper()
    assert "'this month'" not in sql


def test_less_equal_relative_date(layer):
    """Test <= against a relative date is converted, not passed through raw."""
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

    sql = layer.compile(metrics=["orders.revenue"], filters=["orders_cte.created_at <= 'last month'"])

    assert "DATE_TRUNC" in sql.upper()
    assert "'last month'" not in sql


def test_not_equal_relative_date_single_day(layer):
    """Test != against a single-day relative date negates the point comparison."""
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

    sql = layer.compile(metrics=["orders.revenue"], filters=["orders_cte.created_at != 'today'"])

    assert _date_literal() in sql
    assert "'today'" not in sql
    assert "NOT" in sql.upper() or "<>" in sql


def test_not_equal_relative_date_range(layer):
    """Test <> against a ranged relative date negates the full range."""
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

    sql = layer.compile(metrics=["orders.revenue"], filters=["orders_cte.created_at <> 'this month'"])

    assert "NOT (" in sql.upper()
    assert "DATE_TRUNC" in sql.upper()
    assert "'this month'" not in sql


def test_non_relative_date_unchanged(layer):
    """Test that non-relative date expressions are unchanged."""
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
    sql = layer.compile(metrics=["orders.revenue"], filters=["orders_cte.created_at >= '2024-01-01'"])

    # Should remain unchanged
    assert "'2024-01-01'" in sql or "2024-01-01" in sql


def test_mixed_filters(layer):
    """Test mix of relative and absolute date filters."""
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
    assert f"created_at >= {_date_literal(7)}" in sql
    # Regular filter should remain
    assert "status = 'completed'" in sql
