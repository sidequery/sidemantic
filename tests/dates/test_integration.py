"""Test relative date integration with queries."""

from sidemantic import Dimension, Metric, Model


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

    # Should convert to SQL expression
    assert "CURRENT_DATE - 7" in sql
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
    assert "CURRENT_DATE - 30" in sql
    assert "CURRENT_DATE" in sql


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
    assert "DATE_TRUNC('MONTH', CURRENT_DATE)" in sql_upper or "DATE_TRUNC('month', CURRENT_DATE)" in sql
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

    assert "CURRENT_DATE" in sql
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
    assert "CURRENT_DATE - 7" in sql
    # Regular filter should remain
    assert "status = 'completed'" in sql


def _tz_orders_model():
    """Model over a TIMESTAMPTZ instant that crosses a day boundary in America/New_York.

    2024-03-01 02:30:00+00 is still 2024-02-29 21:30 in America/New_York.
    """
    return Model(
        name="orders",
        sql="SELECT 1 AS order_id, TIMESTAMPTZ '2024-03-01 02:30:00+00' AS created_at, 100 AS amount",
        primary_key="order_id",
        dimensions=[Dimension(name="created_at", type="time", granularity="day")],
        metrics=[Metric(name="revenue", agg="sum", sql="amount")],
    )


def test_query_timezone_shifts_date_trunc_bucket(layer):
    """timezone= wraps the time-dim column so DATE_TRUNC buckets by the query timezone."""
    from tests.utils import fetch_dicts

    layer.add_model(_tz_orders_model())
    # Pin the session timezone so DATE_TRUNC on TIMESTAMPTZ is deterministic.
    layer.adapter.execute("SET TimeZone='UTC'")

    sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.created_at__day"],
        timezone="America/New_York",
    )

    # Timezone conversion is wrapped inside the truncation.
    assert "timezone('America/New_York'" in sql

    records = fetch_dicts(layer.adapter.execute(sql))
    assert len(records) == 1
    # In America/New_York the UTC instant falls on the previous calendar day.
    assert str(records[0]["created_at__day"]).startswith("2024-02-29")


def test_query_timezone_default_none_unchanged(layer):
    """Without timezone=, no timezone conversion is emitted and the row buckets in UTC."""
    from tests.utils import fetch_dicts

    layer.add_model(_tz_orders_model())
    # Pin the session timezone so the no-timezone baseline buckets in UTC.
    layer.adapter.execute("SET TimeZone='UTC'")

    sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.created_at__day"],
    )

    assert "timezone(" not in sql
    assert "AT TIME ZONE" not in sql

    records = fetch_dicts(layer.adapter.execute(sql))
    assert str(records[0]["created_at__day"]).startswith("2024-03-01")


def test_query_timezone_generic_dialect(layer):
    """Non-duckdb dialects render the conversion as standard AT TIME ZONE."""
    layer.add_model(_tz_orders_model())

    sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.created_at__day"],
        dialect="postgres",
        timezone="America/New_York",
    )

    assert "AT TIME ZONE 'America/New_York'" in sql


def test_query_timezone_forces_python_engine():
    """With engine=auto, timezone= still emits the conversion (Python path is used)."""
    from sidemantic import SemanticLayer

    layer = SemanticLayer(auto_register=False, engine="auto")
    layer.add_model(_tz_orders_model())

    sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.created_at__day"],
        timezone="America/New_York",
    )

    assert "timezone('America/New_York'" in sql
