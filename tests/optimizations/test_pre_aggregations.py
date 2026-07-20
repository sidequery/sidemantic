"""Tests for pre-aggregation functionality."""

from datetime import datetime

import duckdb
import pytest

from sidemantic import Dimension, Metric, Model
from sidemantic.core.pre_aggregation import Index, PreAggregation, RefreshKey, RefreshResult
from sidemantic.core.preagg_matcher import PreAggregationMatcher


def test_create_preaggregation():
    """Test creating a pre-aggregation."""
    preagg = PreAggregation(
        name="daily_rollup",
        type="rollup",
        measures=["count", "revenue"],
        dimensions=["status", "region"],
        time_dimension="created_at",
        granularity="day",
        partition_granularity="month",
        refresh_key=RefreshKey(every="1 hour", incremental=True),
    )

    assert preagg.name == "daily_rollup"
    assert preagg.type == "rollup"
    assert preagg.measures == ["count", "revenue"]
    assert preagg.dimensions == ["status", "region"]
    assert preagg.time_dimension == "created_at"
    assert preagg.granularity == "day"
    assert preagg.refresh_key.every == "1 hour"
    assert preagg.refresh_key.incremental is True


def test_preaggregation_table_name():
    """Test pre-aggregation table name generation."""
    preagg = PreAggregation(
        name="daily_summary",
        measures=["count"],
        dimensions=["status"],
    )

    table_name = preagg.get_table_name("orders")
    assert table_name == "orders_preagg_daily_summary"


def test_model_with_preaggregation():
    """Test model with pre-aggregations."""
    preagg = PreAggregation(
        name="daily_rollup",
        measures=["count", "revenue"],
        dimensions=["status"],
        time_dimension="created_at",
        granularity="day",
    )

    model = Model(
        name="orders",
        table="public.orders",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical"),
            Dimension(name="created_at", type="time"),
        ],
        metrics=[
            Metric(name="count", agg="count"),
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
        pre_aggregations=[preagg],
    )

    assert len(model.pre_aggregations) == 1
    assert model.get_pre_aggregation("daily_rollup") is not None
    assert model.get_pre_aggregation("nonexistent") is None


def test_preagg_matcher_exact_match():
    """Test pre-aggregation matching with exact dimension match."""
    model = Model(
        name="orders",
        table="orders",
        dimensions=[
            Dimension(name="status", type="categorical"),
            Dimension(name="region", type="categorical"),
            Dimension(name="created_at", type="time"),
        ],
        metrics=[
            Metric(name="count", agg="count"),
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
        pre_aggregations=[
            PreAggregation(
                name="daily_rollup",
                measures=["count", "revenue"],
                dimensions=["status", "region"],
                time_dimension="created_at",
                granularity="day",
            )
        ],
    )

    matcher = PreAggregationMatcher(model)

    # Exact match
    preagg = matcher.find_matching_preagg(
        metrics=["revenue"],
        dimensions=["status", "region"],
        time_granularity="day",
    )

    assert preagg is not None
    assert preagg.name == "daily_rollup"


def test_preagg_matcher_subset_dimensions():
    """Test pre-aggregation matching with subset of dimensions."""
    model = Model(
        name="orders",
        table="orders",
        dimensions=[
            Dimension(name="status", type="categorical"),
            Dimension(name="region", type="categorical"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
        pre_aggregations=[
            PreAggregation(
                name="rollup",
                measures=["revenue"],
                dimensions=["status", "region"],
            )
        ],
    )

    matcher = PreAggregationMatcher(model)

    # Query only uses 'status' - should match (subset)
    preagg = matcher.find_matching_preagg(
        metrics=["revenue"],
        dimensions=["status"],
    )

    assert preagg is not None
    assert preagg.name == "rollup"


def test_preagg_matcher_no_match_extra_dimension():
    """Test pre-aggregation matching fails with extra dimension."""
    model = Model(
        name="orders",
        table="orders",
        dimensions=[
            Dimension(name="status", type="categorical"),
            Dimension(name="region", type="categorical"),
            Dimension(name="customer_id", type="categorical"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
        pre_aggregations=[
            PreAggregation(
                name="rollup",
                measures=["revenue"],
                dimensions=["status"],  # Only has status
            )
        ],
    )

    matcher = PreAggregationMatcher(model)

    # Query uses customer_id which isn't in pre-agg
    preagg = matcher.find_matching_preagg(
        metrics=["revenue"],
        dimensions=["customer_id"],
    )

    assert preagg is None


def test_preagg_matcher_granularity_rollup():
    """Test pre-aggregation matching with granularity rollup."""
    model = Model(
        name="orders",
        table="orders",
        dimensions=[
            Dimension(name="created_at", type="time"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
        pre_aggregations=[
            PreAggregation(
                name="daily",
                measures=["revenue"],
                dimensions=[],
                time_dimension="created_at",
                granularity="day",
            )
        ],
    )

    matcher = PreAggregationMatcher(model)

    # Query at month level (coarser) - should match
    preagg = matcher.find_matching_preagg(
        metrics=["revenue"],
        dimensions=[],
        time_granularity="month",
    )

    assert preagg is not None

    # Query at hour level (finer) - should NOT match
    preagg = matcher.find_matching_preagg(
        metrics=["revenue"],
        dimensions=[],
        time_granularity="hour",
    )

    assert preagg is None


def test_preagg_matcher_prefers_total_rollup_over_time_rollup_for_total_query():
    """Test total queries choose the smallest compatible rollup."""
    model = Model(
        name="orders",
        table="orders",
        dimensions=[
            Dimension(name="created_at", type="time"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
        pre_aggregations=[
            PreAggregation(
                name="daily",
                measures=["revenue"],
                dimensions=[],
                time_dimension="created_at",
                granularity="day",
            ),
            PreAggregation(
                name="total",
                measures=["revenue"],
                dimensions=[],
            ),
        ],
    )

    matcher = PreAggregationMatcher(model)

    preagg = matcher.find_matching_preagg(
        metrics=["revenue"],
        dimensions=[],
    )

    assert preagg is not None
    assert preagg.name == "total"


def test_preagg_matcher_measure_not_available():
    """Test pre-aggregation matching fails when measure not available."""
    model = Model(
        name="orders",
        table="orders",
        dimensions=[
            Dimension(name="status", type="categorical"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
            Metric(name="profit", agg="sum", sql="profit"),
        ],
        pre_aggregations=[
            PreAggregation(
                name="rollup",
                measures=["revenue"],  # Only has revenue
                dimensions=["status"],
            )
        ],
    )

    matcher = PreAggregationMatcher(model)

    # Query for profit which isn't in pre-agg
    preagg = matcher.find_matching_preagg(
        metrics=["profit"],
        dimensions=["status"],
    )

    assert preagg is None


def test_preagg_matcher_best_match_selection():
    """Test that matcher selects most specific pre-aggregation."""
    model = Model(
        name="orders",
        table="orders",
        dimensions=[
            Dimension(name="status", type="categorical"),
            Dimension(name="region", type="categorical"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
        pre_aggregations=[
            # General rollup with many dimensions
            PreAggregation(
                name="general",
                measures=["revenue"],
                dimensions=["status", "region"],
            ),
            # Specific rollup with just status
            PreAggregation(
                name="specific",
                measures=["revenue"],
                dimensions=["status"],
            ),
        ],
    )

    matcher = PreAggregationMatcher(model)

    # Query only needs status - should match 'specific' (exact match)
    preagg = matcher.find_matching_preagg(
        metrics=["revenue"],
        dimensions=["status"],
    )

    assert preagg is not None
    assert preagg.name == "specific"  # More specific match preferred


def test_sql_generation_with_preagg(layer):
    """Test SQL generation using pre-aggregation."""
    layer.use_preaggregations = True

    model = Model(
        name="orders",
        table="public.orders",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical", sql="status"),
            Dimension(name="created_at", type="time", sql="created_at", granularity="day"),
        ],
        metrics=[
            Metric(name="count", agg="count"),
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
        pre_aggregations=[
            PreAggregation(
                name="daily_rollup",
                measures=["count", "revenue"],
                dimensions=["status"],
                time_dimension="created_at",
                granularity="day",
            )
        ],
    )

    layer.add_model(model)

    # Query that matches the pre-aggregation
    sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.status", "orders.created_at__day"],
    )

    # Should use pre-aggregation table
    assert "orders_preagg_daily_rollup" in sql
    assert "SUM(revenue_raw)" in sql
    # Should NOT use CTEs (direct query on pre-agg)
    assert "WITH" not in sql or "_cte" not in sql


def test_sql_generation_without_preagg(layer):
    """Test SQL generation falls back when no pre-aggregation matches."""
    model = Model(
        name="orders",
        table="public.orders",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical", sql="status"),
            Dimension(name="customer_id", type="categorical", sql="customer_id"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
        pre_aggregations=[
            PreAggregation(
                name="by_status",
                measures=["revenue"],
                dimensions=["status"],  # Only status
            )
        ],
    )

    layer.add_model(model)

    # Query uses customer_id which isn't in pre-agg
    sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.customer_id"],
    )

    # Should NOT use pre-aggregation
    assert "orders_preagg_" not in sql
    # Should use normal CTE-based approach
    assert "orders_cte" in sql or "FROM public.orders" in sql


def test_preagg_with_filters(layer):
    """Test pre-aggregation usage with filters."""
    layer.use_preaggregations = True

    model = Model(
        name="orders",
        table="public.orders",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical", sql="status"),
            Dimension(name="region", type="categorical", sql="region"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
        pre_aggregations=[
            PreAggregation(
                name="by_status_region",
                measures=["revenue"],
                dimensions=["status", "region"],
            )
        ],
    )

    layer.add_model(model)

    # Query with filter
    sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.status"],
        filters=["orders.region = 'US'"],
    )

    # Should use pre-aggregation with filter applied
    assert "orders_preagg_by_status_region" in sql
    assert "region = 'US'" in sql or "region='US'" in sql


@pytest.mark.parametrize(
    "filter_expr",
    [
        "orders.region IN ('US', 'EU')",
        "orders.created_at BETWEEN DATE '2024-01-01' AND DATE '2024-01-31'",
        "orders.region IS NOT NULL",
        "LOWER(orders.region) LIKE 'u%'",
        "\"Order Region\" = 'US'",
    ],
)
def test_preagg_filter_column_extraction_uses_sqlglot(filter_expr):
    model = Model(
        name="orders",
        table="public.orders",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical", sql="status"),
            Dimension(name="region", type="categorical", sql="region"),
            Dimension(name="created_at", type="time", sql="created_at"),
            Dimension(name="Order Region", type="categorical", sql='"Order Region"'),
        ],
        metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        pre_aggregations=[
            PreAggregation(
                name="wide",
                measures=["revenue"],
                dimensions=["status", "region", "created_at", "Order Region"],
            )
        ],
    )
    matcher = PreAggregationMatcher(model)

    preagg = matcher.find_matching_preagg(
        metrics=["revenue"],
        dimensions=["status"],
        filters=[filter_expr],
    )

    assert preagg is not None
    assert preagg.name == "wide"


def test_preagg_filter_column_extraction_ignores_subquery_columns():
    model = Model(
        name="orders",
        table="public.orders",
        primary_key="order_id",
        dimensions=[Dimension(name="status", type="categorical", sql="status")],
        metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        pre_aggregations=[
            PreAggregation(
                name="by_status",
                measures=["revenue"],
                dimensions=["status"],
            )
        ],
    )
    matcher = PreAggregationMatcher(model)

    preagg = matcher.find_matching_preagg(
        metrics=["revenue"],
        dimensions=["status"],
        filters=["status IN (SELECT status FROM allowed_statuses)"],
    )

    assert preagg is not None
    assert preagg.name == "by_status"


def test_preagg_granularity_conversion(layer):
    """Test pre-aggregation with granularity conversion."""
    layer.use_preaggregations = True

    model = Model(
        name="orders",
        table="public.orders",
        primary_key="order_id",
        dimensions=[
            Dimension(name="created_at", type="time", sql="created_at", granularity="day"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
        pre_aggregations=[
            PreAggregation(
                name="daily",
                measures=["revenue"],
                dimensions=[],
                time_dimension="created_at",
                granularity="day",
            )
        ],
    )

    layer.add_model(model)

    # Query at month level (coarser than day)
    sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.created_at__month"],
    )

    # Should use pre-aggregation and convert granularity
    assert "orders_preagg_daily" in sql
    assert "DATE_TRUNC('month'" in sql or "DATE_TRUNC('MONTH'" in sql


def test_preagg_disabled_by_default(layer):
    """Test that pre-aggregations are disabled by default."""
    # layer fixture has use_preaggregations=False by default

    model = Model(
        name="orders",
        table="public.orders",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical", sql="status"),
            Dimension(name="created_at", type="time", sql="created_at", granularity="day"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
        pre_aggregations=[
            PreAggregation(
                name="daily_rollup",
                measures=["revenue"],
                dimensions=["status"],
                time_dimension="created_at",
                granularity="day",
            )
        ],
    )

    layer.add_model(model)

    # Query that could match pre-aggregation
    sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.status", "orders.created_at__day"],
    )

    # Should NOT use pre-aggregation (disabled by default)
    assert "orders_preagg_" not in sql
    # Should use normal CTE-based approach
    assert "orders_cte" in sql


def test_preagg_per_query_override(layer):
    """Test per-query override of pre-aggregation setting."""
    layer.use_preaggregations = False  # Disabled globally

    model = Model(
        name="orders",
        table="public.orders",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical", sql="status"),
            Dimension(name="created_at", type="time", sql="created_at", granularity="day"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
        pre_aggregations=[
            PreAggregation(
                name="daily_rollup",
                measures=["revenue"],
                dimensions=["status"],
                time_dimension="created_at",
                granularity="day",
            )
        ],
    )

    layer.add_model(model)

    # Override to enable for this query
    sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.status", "orders.created_at__day"],
        use_preaggregations=True,
    )

    # Should use pre-aggregation (overridden)
    assert "orders_preagg_daily_rollup" in sql


def test_refresh_key_configuration():
    """Test refresh key configuration."""
    refresh_key = RefreshKey(
        every="1 hour",
        sql="SELECT MAX(updated_at) FROM orders",
        incremental=True,
        update_window="7 day",
    )

    assert refresh_key.every == "1 hour"
    assert refresh_key.sql == "SELECT MAX(updated_at) FROM orders"
    assert refresh_key.incremental is True
    assert refresh_key.update_window == "7 day"


def test_index_configuration():
    """Test index configuration."""
    index = Index(
        name="status_idx",
        columns=["status", "created_at"],
        type="regular",
    )

    assert index.name == "status_idx"
    assert index.columns == ["status", "created_at"]
    assert index.type == "regular"


def test_refresh_full():
    """Test full refresh mode."""
    conn = duckdb.connect(":memory:")

    conn.execute("""
        CREATE TABLE orders AS
        SELECT
            DATE '2024-01-01' + INTERVAL (i) DAY as order_date,
            100 + i as revenue
        FROM generate_series(0, 9) as t(i)
    """)

    preagg = PreAggregation(name="daily_rollup", measures=["revenue"], time_dimension="order_date", granularity="day")

    result = preagg.refresh(
        connection=conn,
        source_sql="SELECT order_date, SUM(revenue) as total_revenue FROM orders GROUP BY order_date",
        table_name="orders_preagg_daily",
        mode="full",
    )

    assert result.mode == "full"
    assert result.rows_inserted == 10
    assert conn.execute("SELECT COUNT(*) FROM orders_preagg_daily").fetchone()[0] == 10


def test_refresh_incremental_stateless():
    """Test incremental refresh with stateless watermark derivation."""
    conn = duckdb.connect(":memory:")

    conn.execute("""
        CREATE TABLE orders AS
        SELECT
            DATE '2024-01-01' + INTERVAL (i) DAY as order_date,
            100 + i as revenue
        FROM generate_series(0, 9) as t(i)
    """)

    preagg = PreAggregation(
        name="daily_rollup",
        measures=["revenue"],
        time_dimension="order_date",
        granularity="day",
        refresh_key=RefreshKey(incremental=True),
    )

    # First refresh
    result1 = preagg.refresh(
        connection=conn,
        source_sql="SELECT order_date, SUM(revenue) as total_revenue FROM orders WHERE order_date > {WATERMARK} GROUP BY order_date",
        table_name="orders_preagg_daily",
        mode="incremental",
        watermark_column="order_date",
    )

    count1 = conn.execute("SELECT COUNT(*) FROM orders_preagg_daily").fetchone()[0]
    assert count1 == 10

    # Add more data
    conn.execute("""
        INSERT INTO orders
        SELECT
            DATE '2024-01-11' + INTERVAL (i) DAY as order_date,
            110 + i as revenue
        FROM generate_series(0, 4) as t(i)
    """)

    # Second refresh - derives watermark from table
    result2 = preagg.refresh(
        connection=conn,
        source_sql="SELECT order_date, SUM(revenue) as total_revenue FROM orders WHERE order_date > {WATERMARK} GROUP BY order_date",
        table_name="orders_preagg_daily",
        mode="incremental",
        watermark_column="order_date",
    )

    count2 = conn.execute("SELECT COUNT(*) FROM orders_preagg_daily").fetchone()[0]
    assert count2 == 15
    assert result2.new_watermark > result1.new_watermark


def test_refresh_incremental_with_lookback():
    """Test incremental refresh with lookback window for late-arriving data."""
    conn = duckdb.connect(":memory:")

    conn.execute("""
        CREATE TABLE orders AS
        SELECT
            DATE '2024-01-01' + INTERVAL (i) DAY as order_date,
            100 + i as revenue
        FROM generate_series(0, 9) as t(i)
    """)

    preagg = PreAggregation(name="daily_rollup", measures=["revenue"], time_dimension="order_date", granularity="day")

    # First refresh
    preagg.refresh(
        connection=conn,
        source_sql="SELECT order_date, SUM(revenue) as total_revenue FROM orders WHERE order_date > {WATERMARK} GROUP BY order_date",
        table_name="orders_preagg_daily",
        mode="incremental",
        watermark_column="order_date",
    )

    # Update old data (simulating late-arriving data)
    conn.execute("""
        UPDATE orders
        SET revenue = revenue + 1000
        WHERE order_date = DATE '2024-01-05'
    """)

    # Refresh with 5-day lookback (use >= to include the lookback boundary)
    preagg.refresh(
        connection=conn,
        source_sql="SELECT order_date, SUM(revenue) as total_revenue FROM orders WHERE order_date >= {WATERMARK} GROUP BY order_date",
        table_name="orders_preagg_daily",
        mode="incremental",
        watermark_column="order_date",
        lookback="5 days",
    )

    # Lookback refresh replaces the overlapping range instead of appending duplicates.
    total_revenue = conn.execute("""
        SELECT SUM(total_revenue)
        FROM orders_preagg_daily
        WHERE order_date = DATE '2024-01-05'
    """).fetchone()[0]

    assert total_revenue == 1104


def test_refresh_incremental_uses_update_window_as_default_lookback():
    """refresh_key.update_window supplies the reprocessing window when no lookback is passed."""
    conn = duckdb.connect(":memory:")

    conn.execute("""
        CREATE TABLE orders AS
        SELECT
            DATE '2024-01-01' + INTERVAL (i) DAY as order_date,
            100 + i as revenue
        FROM generate_series(0, 9) as t(i)
    """)

    # update_window is declared on the model; lookback is NOT passed to refresh()
    preagg = PreAggregation(
        name="daily_rollup",
        measures=["revenue"],
        time_dimension="order_date",
        granularity="day",
        refresh_key=RefreshKey(incremental=True, update_window="5 days"),
    )

    preagg.refresh(
        connection=conn,
        source_sql="SELECT order_date, SUM(revenue) as total_revenue FROM orders WHERE order_date > {WATERMARK} GROUP BY order_date",
        table_name="orders_preagg_daily",
        mode="incremental",
        watermark_column="order_date",
    )

    # Late-arriving update to a row inside the 5-day window
    conn.execute("""
        UPDATE orders
        SET revenue = revenue + 1000
        WHERE order_date = DATE '2024-01-05'
    """)

    # No lookback argument: the declared 5-day update_window must drive reprocessing
    preagg.refresh(
        connection=conn,
        source_sql="SELECT order_date, SUM(revenue) as total_revenue FROM orders WHERE order_date >= {WATERMARK} GROUP BY order_date",
        table_name="orders_preagg_daily",
        mode="incremental",
        watermark_column="order_date",
    )

    total_revenue = conn.execute("""
        SELECT SUM(total_revenue)
        FROM orders_preagg_daily
        WHERE order_date = DATE '2024-01-05'
    """).fetchone()[0]

    assert total_revenue == 1104  # late-arriving value replaced because update_window covered it


def test_explicit_lookback_overrides_update_window():
    """An explicit lookback argument takes precedence over refresh_key.update_window."""
    conn = duckdb.connect(":memory:")

    conn.execute("""
        CREATE TABLE orders AS
        SELECT
            DATE '2024-01-01' + INTERVAL (i) DAY as order_date,
            100 + i as revenue
        FROM generate_series(0, 9) as t(i)
    """)

    preagg = PreAggregation(
        name="daily_rollup",
        measures=["revenue"],
        time_dimension="order_date",
        granularity="day",
        refresh_key=RefreshKey(incremental=True, update_window="5 days"),
    )

    preagg.refresh(
        connection=conn,
        source_sql="SELECT order_date, SUM(revenue) as total_revenue FROM orders WHERE order_date > {WATERMARK} GROUP BY order_date",
        table_name="orders_preagg_daily",
        mode="incremental",
        watermark_column="order_date",
    )

    conn.execute("""
        UPDATE orders
        SET revenue = revenue + 1000
        WHERE order_date = DATE '2024-01-05'
    """)

    # Explicit zero-day lookback overrides the 5-day update_window: the old row is not reprocessed
    preagg.refresh(
        connection=conn,
        source_sql="SELECT order_date, SUM(revenue) as total_revenue FROM orders WHERE order_date >= {WATERMARK} GROUP BY order_date",
        table_name="orders_preagg_daily",
        mode="incremental",
        watermark_column="order_date",
        lookback="0 days",
    )

    total_revenue = conn.execute("""
        SELECT SUM(total_revenue)
        FROM orders_preagg_daily
        WHERE order_date = DATE '2024-01-05'
    """).fetchone()[0]

    assert total_revenue == 104  # explicit 0-day lookback won; the late update was excluded


def test_refresh_merge_idempotent():
    """Test merge/upsert refresh mode for idempotent updates."""
    conn = duckdb.connect(":memory:")

    conn.execute("""
        CREATE TABLE orders AS
        SELECT
            DATE '2024-01-01' + INTERVAL (i) DAY as order_date,
            100 + i as revenue
        FROM generate_series(0, 9) as t(i)
    """)

    preagg = PreAggregation(name="daily_rollup", measures=["revenue"], time_dimension="order_date", granularity="day")

    # First refresh
    preagg.refresh(
        connection=conn,
        source_sql="SELECT order_date, SUM(revenue) as total_revenue FROM orders WHERE order_date > {WATERMARK} GROUP BY order_date",
        table_name="orders_preagg_daily",
        mode="merge",
        watermark_column="order_date",
    )

    count1 = conn.execute("SELECT COUNT(*) FROM orders_preagg_daily").fetchone()[0]
    revenue1 = conn.execute("""
        SELECT total_revenue
        FROM orders_preagg_daily
        WHERE order_date = DATE '2024-01-05'
    """).fetchone()[0]

    # Update old data
    conn.execute("""
        UPDATE orders
        SET revenue = revenue + 1000
        WHERE order_date = DATE '2024-01-05'
    """)

    # Merge refresh with lookback - should update (no duplicates, use >= for lookback boundary)
    preagg.refresh(
        connection=conn,
        source_sql="SELECT order_date, SUM(revenue) as total_revenue FROM orders WHERE order_date >= {WATERMARK} GROUP BY order_date",
        table_name="orders_preagg_daily",
        mode="merge",
        watermark_column="order_date",
        lookback="5 days",
    )

    count2 = conn.execute("SELECT COUNT(*) FROM orders_preagg_daily").fetchone()[0]
    revenue2 = conn.execute("""
        SELECT total_revenue
        FROM orders_preagg_daily
        WHERE order_date = DATE '2024-01-05'
    """).fetchone()[0]

    assert count1 == count2 == 10  # No duplicates
    assert revenue1 == 104
    assert revenue2 == 1104  # Updated value


def test_refresh_external_watermark():
    """Test stateless refresh with external watermark management."""
    conn = duckdb.connect(":memory:")

    conn.execute("""
        CREATE TABLE orders AS
        SELECT
            DATE '2024-01-01' + INTERVAL (i) DAY as order_date,
            100 + i as revenue
        FROM generate_series(0, 14) as t(i)
    """)

    preagg = PreAggregation(name="daily_rollup", measures=["revenue"], time_dimension="order_date", granularity="day")

    # Simulating orchestrator storing watermark
    watermark_store = {}

    # First refresh
    result1 = preagg.refresh(
        connection=conn,
        source_sql="SELECT order_date, SUM(revenue) as total_revenue FROM orders WHERE order_date > {WATERMARK} GROUP BY order_date",
        table_name="orders_preagg_daily",
        mode="incremental",
        watermark_column="order_date",
    )

    watermark_store["daily_orders"] = result1.new_watermark
    count1 = conn.execute("SELECT COUNT(*) FROM orders_preagg_daily").fetchone()[0]
    assert count1 == 15

    # Table gets dropped (simulating failure)
    conn.execute("DROP TABLE orders_preagg_daily")

    # Refresh with external watermark (stateless)
    preagg.refresh(
        connection=conn,
        source_sql="SELECT order_date, SUM(revenue) as total_revenue FROM orders WHERE order_date > {WATERMARK} GROUP BY order_date",
        table_name="orders_preagg_daily",
        mode="incremental",
        watermark_column="order_date",
        from_watermark=f"'{watermark_store['daily_orders']}'",
    )

    count2 = conn.execute("SELECT COUNT(*) FROM orders_preagg_daily").fetchone()[0]
    assert count2 == 0  # No new data after the watermark


def test_refresh_result_dataclass():
    """Test RefreshResult dataclass."""
    result = RefreshResult(
        mode="incremental",
        rows_inserted=100,
        rows_updated=0,
        new_watermark="2024-01-15",
        duration_seconds=1.23,
        timestamp=datetime.now(),
    )

    assert result.mode == "incremental"
    assert result.rows_inserted == 100
    assert result.new_watermark == "2024-01-15"
    assert result.duration_seconds == 1.23


def test_generate_materialization_sql():
    """Test generate_materialization_sql() method."""
    model = Model(
        name="orders",
        table="public.orders",
        dimensions=[
            Dimension(name="status", type="categorical", sql="status"),
            Dimension(name="region", type="categorical", sql="region"),
            Dimension(name="created_at", type="time", sql="created_at"),
        ],
        metrics=[
            Metric(name="count", agg="count"),
            Metric(name="revenue", agg="sum", sql="amount"),
            Metric(name="unique_customers", agg="count_distinct", sql="customer_id"),
        ],
    )

    # Test with time dimension and granularity
    preagg = PreAggregation(
        name="daily_rollup",
        measures=["count", "revenue", "unique_customers"],
        dimensions=["status", "region"],
        time_dimension="created_at",
        granularity="day",
    )

    sql = preagg.generate_materialization_sql(model)

    # Verify SQL structure
    assert "SELECT" in sql
    assert "FROM public.orders" in sql
    assert "GROUP BY" in sql

    # Verify time dimension with granularity
    assert "DATE_TRUNC('day', created_at) as created_at_day" in sql

    # Verify dimensions
    assert "status as status" in sql
    assert "region as region" in sql

    # Verify measures with _raw suffix
    assert "COUNT(*) as count_raw" in sql
    assert "SUM(amount) as revenue_raw" in sql
    assert "COUNT(DISTINCT customer_id) as unique_customers_raw" in sql


def test_generate_materialization_sql_no_time_dimension():
    """Test generate_materialization_sql() without time dimension."""
    model = Model(
        name="products",
        table="products",
        dimensions=[
            Dimension(name="category", type="categorical", sql="category"),
            Dimension(name="brand", type="categorical", sql="brand"),
        ],
        metrics=[
            Metric(name="avg_price", agg="avg", sql="price"),
        ],
    )

    preagg = PreAggregation(
        name="category_rollup",
        measures=["avg_price"],
        dimensions=["category", "brand"],
    )

    sql = preagg.generate_materialization_sql(model)

    # Should not have DATE_TRUNC
    assert "DATE_TRUNC" not in sql

    # Should have dimensions and measures
    assert "category as category" in sql
    assert "brand as brand" in sql
    assert "SUM(price) as avg_price_raw" in sql


def test_total_rollup_materializes_filtered_metrics_without_empty_group_by():
    conn = duckdb.connect(":memory:")
    conn.execute(
        "CREATE TABLE orders AS SELECT * FROM (VALUES "
        "(1, 'completed', 100), (2, 'pending', 50), (3, 'completed', 25)) "
        "t(id, status, amount)"
    )
    model = Model(
        name="orders",
        table="orders",
        primary_key="id",
        metrics=[
            Metric(
                name="completed_revenue",
                agg="sum",
                sql="amount",
                filters=["{model}.status = 'completed'"],
            ),
            Metric(
                name="completed_count",
                agg="count",
                filters=["{model}.status = 'completed'"],
            ),
        ],
    )
    preagg = PreAggregation(
        name="totals",
        measures=["completed_revenue", "completed_count"],
        dimensions=[],
    )

    sql = preagg.generate_materialization_sql(model)

    assert "GROUP BY" not in sql
    assert "SUM(CASE WHEN status = 'completed' THEN amount ELSE NULL END)" in sql
    assert "COUNT(CASE WHEN status = 'completed' THEN 1 ELSE NULL END)" in sql
    assert conn.execute(sql).fetchall() == [(125, 2)]


def test_non_decomposable_preaggregation_requires_exact_grain():
    metric = Metric(name="median_price", agg="median", sql="price")
    preagg = PreAggregation(name="by_category", measures=["median_price"], dimensions=["category"])
    model = Model(
        name="products",
        table="products",
        dimensions=[Dimension(name="category", type="categorical")],
        metrics=[metric],
        pre_aggregations=[preagg],
    )
    matcher = PreAggregationMatcher(model)

    assert matcher.can_satisfy_query(preagg, ["median_price"], ["category"])
    assert not matcher.can_satisfy_query(preagg, ["median_price"], [])


def test_avg_preaggregation_rolls_up_with_sum_count_state(layer):
    layer.use_preaggregations = True
    layer.conn.execute("""
        CREATE TABLE products (
            id INTEGER,
            category VARCHAR,
            price DECIMAL(10, 2)
        )
    """)
    layer.conn.execute("""
        INSERT INTO products VALUES
            (1, 'hardware', 10.00),
            (2, 'hardware', 20.00),
            (3, 'software', 50.00)
    """)
    layer.conn.execute("""
        CREATE TABLE products_preagg_by_category AS
        SELECT
            category,
            SUM(price) AS avg_price_raw,
            COUNT(*) AS count_raw
        FROM products
        GROUP BY category
    """)
    model = Model(
        name="products",
        table="products",
        primary_key="id",
        dimensions=[Dimension(name="category", type="categorical", sql="category")],
        metrics=[
            Metric(name="avg_price", agg="avg", sql="price"),
            Metric(name="count", agg="count"),
        ],
        pre_aggregations=[
            PreAggregation(
                name="by_category",
                measures=["avg_price", "count"],
                dimensions=["category"],
            )
        ],
    )
    layer.add_model(model)

    preagg_sql = layer.compile(
        metrics=["products.avg_price"],
        dimensions=["products.category"],
        order_by=["category"],
    )
    baseline_rows = layer.query(
        metrics=["products.avg_price"],
        dimensions=["products.category"],
        order_by=["category"],
        use_preaggregations=False,
    ).fetchall()
    preagg_rows = layer.adapter.execute(preagg_sql).fetchall()

    assert "products_preagg_by_category" in preagg_sql
    assert "(SUM(avg_price_raw)) / NULLIF(SUM(count_raw), 0)" in preagg_sql
    assert preagg_rows == baseline_rows


def test_avg_preaggregation_rejects_missing_count_state(layer):
    layer.use_preaggregations = True
    model = Model(
        name="products",
        table="products",
        primary_key="id",
        dimensions=[Dimension(name="category", type="categorical", sql="category")],
        metrics=[Metric(name="avg_price", agg="avg", sql="price")],
        pre_aggregations=[
            PreAggregation(
                name="by_category",
                measures=["avg_price"],
                dimensions=["category"],
            )
        ],
    )
    layer.add_model(model)

    sql = layer.compile(
        metrics=["products.avg_price"],
        dimensions=["products.category"],
    )

    assert "products_preagg_by_category" not in sql


def test_ratio_metric_preaggregation_rebuilds_from_additive_leaves(layer):
    layer.use_preaggregations = True
    layer.conn.execute("""
        CREATE TABLE orders (
            id INTEGER,
            status VARCHAR,
            amount DECIMAL(10, 2)
        )
    """)
    layer.conn.execute("""
        INSERT INTO orders VALUES
            (1, 'completed', 100.00),
            (2, 'completed', 300.00),
            (3, 'pending', 50.00)
    """)
    layer.conn.execute("""
        CREATE TABLE orders_preagg_by_status AS
        SELECT
            status,
            SUM(amount) AS revenue_raw,
            COUNT(*) AS count_raw
        FROM orders
        GROUP BY status
    """)
    model = Model(
        name="orders",
        table="orders",
        primary_key="id",
        dimensions=[Dimension(name="status", type="categorical", sql="status")],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
            Metric(name="count", agg="count"),
            Metric(name="revenue_per_order", type="ratio", numerator="revenue", denominator="count"),
        ],
        pre_aggregations=[
            PreAggregation(
                name="by_status",
                measures=["revenue", "count"],
                dimensions=["status"],
            )
        ],
    )
    layer.add_model(model)

    preagg_sql = layer.compile(
        metrics=["orders.revenue_per_order"],
        dimensions=["orders.status"],
        order_by=["status"],
    )
    baseline_rows = layer.query(
        metrics=["orders.revenue_per_order"],
        dimensions=["orders.status"],
        order_by=["status"],
        use_preaggregations=False,
    ).fetchall()
    preagg_rows = layer.adapter.execute(preagg_sql).fetchall()

    assert "orders_preagg_by_status" in preagg_sql
    assert "(SUM(revenue_raw)) / NULLIF(SUM(count_raw), 0)" in preagg_sql
    assert preagg_rows == baseline_rows


def test_lambda_preaggregation_fields_default():
    """The lambda union fields default to inert (None / False)."""
    preagg = PreAggregation(name="lam", type="lambda")

    assert preagg.rollups is None
    assert preagg.union_with_source_data is False


def _lambda_orders_layer(layer):
    """Source orders with old (pre-2024-04) and recent (2024-06) rows, plus a batch
    rollup that covers ONLY the older buckets. Recent rows live only in the source.
    """
    layer.use_preaggregations = True
    layer.conn.execute("""
        CREATE TABLE orders (
            id INTEGER,
            status VARCHAR,
            amount DECIMAL(10, 2),
            created_at TIMESTAMP
        )
    """)
    layer.conn.execute("""
        INSERT INTO orders VALUES
            (1, 'completed', 100.00, '2024-01-15'),
            (2, 'completed', 300.00, '2024-02-10'),
            (3, 'pending', 50.00, '2024-03-05'),
            (4, 'completed', 7.00, '2024-06-20'),
            (5, 'pending', 9.00, '2024-06-21')
    """)
    # Batch rollup covers only buckets < 2024-04-01; recent rows are absent here.
    layer.conn.execute("""
        CREATE TABLE orders_preagg_lam AS
        SELECT
            DATE_TRUNC('day', created_at) AS created_at_day,
            status,
            SUM(amount) AS revenue_raw,
            COUNT(*) AS count_raw
        FROM orders
        WHERE created_at < '2024-04-01'
        GROUP BY 1, 2
    """)


def _lambda_orders_model(*, union_with_source_data=True, build_range_end="'2024-04-01'"):
    return Model(
        name="orders",
        table="orders",
        primary_key="id",
        dimensions=[
            Dimension(name="status", type="categorical", sql="status"),
            Dimension(name="created_at", type="time", granularity="day", sql="created_at"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
            Metric(name="count", agg="count"),
        ],
        pre_aggregations=[
            PreAggregation(
                name="lam",
                type="lambda",
                measures=["revenue", "count"],
                dimensions=["status"],
                time_dimension="created_at",
                granularity="day",
                union_with_source_data=union_with_source_data,
                build_range_end=build_range_end,
            )
        ],
    )


def test_lambda_preaggregation_unions_batch_rollup_with_fresh_source(layer):
    """A lambda preagg unions its batch rollup (older buckets) with a fresh source
    aggregation (recent buckets), re-aggregated at the query grain. Totals must match
    the full-source baseline: older buckets come from the rollup, recent-only rows
    from the source, counted exactly once."""
    _lambda_orders_layer(layer)
    layer.add_model(_lambda_orders_model())

    preagg_sql = layer.compile(
        metrics=["orders.revenue", "orders.count"],
        dimensions=["orders.status"],
        order_by=["status"],
    )
    baseline_rows = layer.query(
        metrics=["orders.revenue", "orders.count"],
        dimensions=["orders.status"],
        order_by=["status"],
        use_preaggregations=False,
    ).fetchall()
    preagg_rows = layer.adapter.execute(preagg_sql).fetchall()

    # Reads the batch rollup table, the source table, and unions them.
    assert "orders_preagg_lam" in preagg_sql
    assert "UNION ALL" in preagg_sql
    assert "FROM orders" in preagg_sql
    # Partial-state columns are re-aggregated over the union.
    assert "SUM(revenue_raw)" in preagg_sql
    assert "SUM(count_raw)" in preagg_sql
    # Disjoint split aligned to the build_range_end bucket boundary (no double count / gap).
    assert "created_at_day < DATE_TRUNC('day', CAST(('2024-04-01') AS TIMESTAMP))" in preagg_sql
    assert "created_at >= DATE_TRUNC('day', CAST(('2024-04-01') AS TIMESTAMP))" in preagg_sql

    assert preagg_rows == baseline_rows


def test_lambda_preaggregation_unions_with_granularity_rollup(layer):
    """A day-grain lambda union answers a month query: both legs carry the
    {time}_{day} column, and the outer DATE_TRUNC re-truncates the union to months."""
    _lambda_orders_layer(layer)
    layer.add_model(_lambda_orders_model())

    preagg_sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.created_at__month"],
        order_by=["orders.created_at__month"],
    )
    baseline_rows = layer.query(
        metrics=["orders.revenue"],
        dimensions=["orders.created_at__month"],
        order_by=["orders.created_at__month"],
        use_preaggregations=False,
    ).fetchall()
    preagg_rows = layer.adapter.execute(preagg_sql).fetchall()

    assert "UNION ALL" in preagg_sql
    assert "DATE_TRUNC('MONTH', created_at_day)" in preagg_sql
    assert preagg_rows == baseline_rows


def test_lambda_preaggregation_without_build_range_end_is_plain_rollup(layer):
    """union_with_source_data=True but no build_range_end has no split point, so it
    degrades to a plain rollup read (no UNION) and still serves the query."""
    _lambda_orders_layer(layer)
    # Add the recent rows to the rollup so a plain read still matches the baseline.
    layer.conn.execute("""
        INSERT INTO orders_preagg_lam
        SELECT DATE_TRUNC('day', created_at), status, SUM(amount), COUNT(*)
        FROM orders WHERE created_at >= '2024-04-01' GROUP BY 1, 2
    """)
    layer.add_model(_lambda_orders_model(union_with_source_data=True, build_range_end=None))

    preagg_sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.status"],
        order_by=["status"],
    )

    assert "UNION ALL" not in preagg_sql
    assert "FROM orders_preagg_lam" in preagg_sql


def test_derived_metric_preaggregation_rebuilds_from_additive_leaves(layer):
    layer.use_preaggregations = True
    layer.conn.execute("""
        CREATE TABLE orders (
            id INTEGER,
            status VARCHAR,
            amount DECIMAL(10, 2),
            discount DECIMAL(10, 2)
        )
    """)
    layer.conn.execute("""
        INSERT INTO orders VALUES
            (1, 'completed', 100.00, 5.00),
            (2, 'completed', 300.00, 10.00),
            (3, 'pending', 50.00, 0.00)
    """)
    layer.conn.execute("""
        CREATE TABLE orders_preagg_by_status AS
        SELECT
            status,
            SUM(amount) AS revenue_raw,
            SUM(discount) AS discounts_raw
        FROM orders
        GROUP BY status
    """)
    model = Model(
        name="orders",
        table="orders",
        primary_key="id",
        dimensions=[Dimension(name="status", type="categorical", sql="status")],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
            Metric(name="discounts", agg="sum", sql="discount"),
            Metric(name="net_revenue", type="derived", sql="revenue - discounts"),
        ],
        pre_aggregations=[
            PreAggregation(
                name="by_status",
                measures=["revenue", "discounts"],
                dimensions=["status"],
            )
        ],
    )
    layer.add_model(model)

    preagg_sql = layer.compile(
        metrics=["orders.net_revenue"],
        dimensions=["orders.status"],
        order_by=["status"],
    )
    baseline_rows = layer.query(
        metrics=["orders.net_revenue"],
        dimensions=["orders.status"],
        order_by=["status"],
        use_preaggregations=False,
    ).fetchall()
    preagg_rows = layer.adapter.execute(preagg_sql).fetchall()

    assert "orders_preagg_by_status" in preagg_sql
    assert "SUM(revenue_raw) - SUM(discounts_raw)" in preagg_sql
    assert preagg_rows == baseline_rows


def test_ratio_metric_preaggregation_rejects_count_distinct_leaf(layer):
    layer.use_preaggregations = True
    model = Model(
        name="orders",
        table="orders",
        primary_key="id",
        dimensions=[Dimension(name="status", type="categorical", sql="status")],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
            Metric(name="unique_customers", agg="count_distinct", sql="customer_id"),
            Metric(name="revenue_per_customer", type="ratio", numerator="revenue", denominator="unique_customers"),
        ],
        pre_aggregations=[
            PreAggregation(
                name="by_status",
                measures=["revenue", "unique_customers"],
                dimensions=["status"],
            )
        ],
    )
    layer.add_model(model)

    sql = layer.compile(
        metrics=["orders.revenue_per_customer"],
        dimensions=["orders.status"],
    )

    assert "orders_preagg_by_status" not in sql


def test_generate_materialization_sql_with_duckdb():
    """Test generated SQL actually works with DuckDB."""
    conn = duckdb.connect(":memory:")

    conn.execute("""
        CREATE TABLE orders (
            order_id INTEGER,
            customer_id INTEGER,
            created_at TIMESTAMP,
            status VARCHAR,
            amount DECIMAL(10, 2)
        )
    """)

    conn.execute("""
        INSERT INTO orders VALUES
            (1, 101, '2024-01-01 10:00:00', 'completed', 100.00),
            (2, 102, '2024-01-01 11:00:00', 'completed', 200.00),
            (3, 103, '2024-01-01 12:00:00', 'pending', 150.00),
            (4, 104, '2024-01-02 10:00:00', 'completed', 300.00),
            (5, 105, '2024-01-02 11:00:00', 'cancelled', 50.00),
            (6, 106, '2024-01-03 10:00:00', 'completed', 400.00)
    """)

    model = Model(
        name="orders",
        table="orders",
        dimensions=[
            Dimension(name="status", type="categorical", sql="status"),
            Dimension(name="created_at", type="time", sql="created_at"),
        ],
        metrics=[
            Metric(name="count", agg="count"),
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    preagg = PreAggregation(
        name="daily_rollup",
        measures=["count", "revenue"],
        dimensions=["status"],
        time_dimension="created_at",
        granularity="day",
    )

    # Generate SQL
    source_sql = preagg.generate_materialization_sql(model)

    # Execute it to create table
    table_name = preagg.get_table_name("orders")
    conn.execute(f"CREATE TABLE {table_name} AS {source_sql}")

    # Verify results
    rows = conn.execute(f"SELECT * FROM {table_name} ORDER BY created_at_day, status").fetchall()

    # Should have 5 rows (3 days * statuses)
    # Day 1: completed (2), pending (1)
    # Day 2: completed (1), cancelled (1)
    # Day 3: completed (1)
    assert len(rows) == 5

    # Check specific aggregation
    result = conn.execute(f"""
        SELECT count_raw, revenue_raw
        FROM {table_name}
        WHERE created_at_day = '2024-01-01' AND status = 'completed'
    """).fetchone()

    assert result[0] == 2  # 2 orders
    assert result[1] == 300.00  # 100 + 200


def test_refresh_engine_mode_snowflake():
    """Test engine mode refresh with Snowflake DYNAMIC TABLE."""

    # Mock connection
    class MockSnowflakeConnection:
        def __init__(self):
            self.executed_sql = []

        def execute(self, sql):
            self.executed_sql.append(sql)
            return self

    conn = MockSnowflakeConnection()

    preagg = PreAggregation(
        name="daily_rollup",
        measures=["revenue"],
        dimensions=["status"],
        time_dimension="created_at",
        granularity="day",
        refresh_key=RefreshKey(every="1 hour"),
    )

    model = Model(
        name="orders",
        table="orders",
        dimensions=[
            Dimension(name="status", type="categorical", sql="status"),
            Dimension(name="created_at", type="time", sql="created_at"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    source_sql = preagg.generate_materialization_sql(model)

    result = preagg.refresh(
        connection=conn,
        source_sql=source_sql,
        table_name="orders_preagg_daily_rollup",
        mode="engine",
        dialect="snowflake",
    )

    assert result.mode == "engine"
    assert len(conn.executed_sql) == 1
    assert "CREATE OR REPLACE DYNAMIC TABLE" in conn.executed_sql[0]
    assert "TARGET_LAG = '1 HOUR'" in conn.executed_sql[0]
    assert "orders_preagg_daily_rollup" in conn.executed_sql[0]


def test_refresh_engine_mode_clickhouse():
    """Test engine mode refresh with ClickHouse MATERIALIZED VIEW."""

    class MockClickHouseConnection:
        def __init__(self):
            self.executed_sql = []

        def execute(self, sql):
            self.executed_sql.append(sql)
            return self

    conn = MockClickHouseConnection()

    preagg = PreAggregation(
        name="daily_rollup",
        measures=["revenue"],
        dimensions=["status"],
        time_dimension="created_at",
        granularity="day",
    )

    model = Model(
        name="orders",
        table="orders",
        dimensions=[
            Dimension(name="status", type="categorical", sql="status"),
            Dimension(name="created_at", type="time", sql="created_at"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    source_sql = preagg.generate_materialization_sql(model)

    result = preagg.refresh(
        connection=conn,
        source_sql=source_sql,
        table_name="orders_preagg_daily_rollup",
        mode="engine",
        dialect="clickhouse",
    )

    assert result.mode == "engine"
    assert len(conn.executed_sql) == 1
    assert "CREATE MATERIALIZED VIEW" in conn.executed_sql[0]
    assert "TO orders_preagg_daily_rollup_data" in conn.executed_sql[0]


def test_refresh_engine_mode_bigquery():
    """Test engine mode refresh with BigQuery MATERIALIZED VIEW."""

    class MockBigQueryConnection:
        def __init__(self):
            self.executed_sql = []

        def execute(self, sql):
            self.executed_sql.append(sql)
            return self

    conn = MockBigQueryConnection()

    preagg = PreAggregation(
        name="daily_rollup",
        measures=["revenue"],
        dimensions=["status"],
        time_dimension="created_at",
        granularity="day",
        refresh_key=RefreshKey(every="2 hours"),
    )

    model = Model(
        name="orders",
        table="orders",
        dimensions=[
            Dimension(name="status", type="categorical", sql="status"),
            Dimension(name="created_at", type="time", sql="created_at"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    source_sql = preagg.generate_materialization_sql(model)

    result = preagg.refresh(
        connection=conn,
        source_sql=source_sql,
        table_name="orders_preagg_daily_rollup",
        mode="engine",
        dialect="bigquery",
    )

    assert result.mode == "engine"
    assert len(conn.executed_sql) == 1
    assert "CREATE MATERIALIZED VIEW" in conn.executed_sql[0]
    assert "refresh_interval_minutes = 120" in conn.executed_sql[0]  # 2 hours


def test_validate_sql_for_engine_rejects_window_functions():
    """Test SQL validation rejects window functions for engine mode."""
    preagg = PreAggregation(
        name="rollup",
        measures=["revenue"],
        dimensions=["status"],
    )

    # SQL with window function
    sql_with_window = """
        SELECT
            status,
            SUM(revenue) as total_revenue,
            ROW_NUMBER() OVER (PARTITION BY status ORDER BY revenue DESC) as rank
        FROM orders
        GROUP BY status
    """

    is_valid, error = preagg._validate_sql_for_engine(sql_with_window, "snowflake")
    assert is_valid is False
    assert "Window functions not supported" in error


def test_validate_sql_for_engine_accepts_simple_aggregation():
    """Test SQL validation accepts simple aggregations for engine mode."""
    preagg = PreAggregation(
        name="rollup",
        measures=["revenue"],
        dimensions=["status"],
    )

    simple_sql = """
        SELECT
            status,
            SUM(revenue) as total_revenue
        FROM orders
        GROUP BY status
    """

    is_valid, error = preagg._validate_sql_for_engine(simple_sql, "snowflake")
    assert is_valid is True
    assert error is None


def test_refresh_engine_mode_unsupported_dialect():
    """Test engine mode raises error for unsupported dialect."""
    conn = duckdb.connect(":memory:")

    preagg = PreAggregation(
        name="rollup",
        measures=["revenue"],
        dimensions=["status"],
    )

    model = Model(
        name="orders",
        table="orders",
        dimensions=[Dimension(name="status", type="categorical", sql="status")],
        metrics=[Metric(name="revenue", agg="sum", sql="amount")],
    )

    source_sql = preagg.generate_materialization_sql(model)

    with pytest.raises(ValueError, match="Unsupported dialect"):
        preagg.refresh(
            connection=conn,
            source_sql=source_sql,
            table_name="orders_preagg",
            mode="engine",
            dialect="mysql",  # Unsupported
        )


def test_preagg_injection_in_model_name_rejected():
    """get_table_name rejects SQL injection in model name."""
    preagg = PreAggregation(
        name="daily_summary",
        measures=["count"],
        dimensions=["status"],
    )
    with pytest.raises(ValueError, match="Invalid model name"):
        preagg.get_table_name("orders; DROP TABLE--")


def test_preagg_injection_in_preagg_name_rejected():
    """get_table_name rejects SQL injection in preagg name."""
    preagg = PreAggregation(
        name="daily; DROP TABLE--",
        measures=["count"],
        dimensions=["status"],
    )
    with pytest.raises(ValueError, match="Invalid preagg name"):
        preagg.get_table_name("orders")


def test_generate_materialization_sql_rejects_window_dimension():
    """Test that generate_materialization_sql raises ValueError for window dimensions."""
    model = Model(
        name="events",
        table="events",
        dimensions=[
            Dimension(name="event", type="categorical", sql="event"),
            Dimension(
                name="next_event",
                type="categorical",
                sql="event",
                window="LEAD(event) OVER (PARTITION BY person_id ORDER BY timestamp)",
            ),
            Dimension(name="status", type="categorical", sql="status"),
        ],
        metrics=[
            Metric(name="count", agg="count"),
        ],
    )

    # Pre-aggregation that references a window dimension should raise
    preagg = PreAggregation(
        name="by_next_event",
        measures=["count"],
        dimensions=["next_event"],
    )

    with pytest.raises(ValueError, match="window dimension.*next_event.*incompatible"):
        preagg.generate_materialization_sql(model)


def test_generate_materialization_sql_rejects_window_time_dimension():
    """Test that generate_materialization_sql raises ValueError for window time dimensions."""
    model = Model(
        name="events",
        table="events",
        dimensions=[
            Dimension(
                name="next_timestamp",
                type="time",
                sql="timestamp",
                window="LEAD(timestamp) OVER (PARTITION BY person_id ORDER BY timestamp)",
            ),
        ],
        metrics=[
            Metric(name="count", agg="count"),
        ],
    )

    preagg = PreAggregation(
        name="daily_next",
        measures=["count"],
        dimensions=[],
        time_dimension="next_timestamp",
        granularity="day",
    )

    with pytest.raises(ValueError, match="window dimension.*next_timestamp.*incompatible"):
        preagg.generate_materialization_sql(model)


def test_generate_materialization_sql_normal_dimensions_still_work():
    """Test that pre-aggregations with normal (non-window) dimensions still work."""
    model = Model(
        name="events",
        table="events",
        dimensions=[
            Dimension(name="event", type="categorical", sql="event"),
            Dimension(name="status", type="categorical", sql="status"),
            Dimension(
                name="next_event",
                type="categorical",
                sql="event",
                window="LEAD(event) OVER (PARTITION BY person_id ORDER BY timestamp)",
            ),
        ],
        metrics=[
            Metric(name="count", agg="count"),
        ],
    )

    # Pre-aggregation with only normal dimensions should work fine
    preagg = PreAggregation(
        name="by_status",
        measures=["count"],
        dimensions=["status"],
    )

    sql = preagg.generate_materialization_sql(model)
    assert "status as status" in sql
    assert "GROUP BY" in sql
    assert "LEAD" not in sql
    assert "OVER" not in sql


def test_original_sql_materializes_base_query_without_group_by():
    """original_sql pre-aggregations stage the cube's base query verbatim (no GROUP BY)."""
    model = Model(
        name="orders",
        sql="SELECT * FROM raw.orders WHERE status != 'deleted'",
        primary_key="order_id",
        dimensions=[Dimension(name="status", type="categorical")],
        metrics=[Metric(name="order_count", agg="count")],
    )
    preagg = PreAggregation(name="base", type="original_sql")

    sql = preagg.generate_materialization_sql(model)

    assert "GROUP BY" not in sql.upper()
    assert sql == "SELECT * FROM raw.orders WHERE status != 'deleted'"


def test_original_sql_honors_explicit_sql_field():
    """original_sql uses its own sql field when set, overriding the model base query."""
    model = Model(
        name="orders",
        sql="SELECT * FROM base",
        primary_key="order_id",
        dimensions=[],
        metrics=[Metric(name="order_count", agg="count")],
    )
    preagg = PreAggregation(name="base", type="original_sql", sql="SELECT * FROM custom_view")

    assert preagg.generate_materialization_sql(model) == "SELECT * FROM custom_view"


def test_original_sql_refreshes_in_duckdb():
    """A real DuckDB refresh of an original_sql preagg materializes the full base query (no aggregation)."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE raw_orders AS SELECT i as order_id, i % 3 as status FROM generate_series(1, 9) t(i)")

    model = Model(
        name="orders",
        sql="SELECT * FROM raw_orders",
        primary_key="order_id",
        dimensions=[Dimension(name="status", type="categorical")],
        metrics=[Metric(name="order_count", agg="count")],
    )
    preagg = PreAggregation(name="base", type="original_sql")

    src = preagg.generate_materialization_sql(model)
    preagg.refresh(connection=conn, source_sql=src, table_name="orders_preagg_base", mode="full")

    rows = conn.execute("SELECT COUNT(*) FROM orders_preagg_base").fetchone()[0]
    assert rows == 9  # all base rows materialized, not collapsed by a GROUP BY


def test_original_sql_not_routed_for_metric_queries():
    """The matcher never selects an original_sql staged table to answer a metric query."""
    model = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        dimensions=[Dimension(name="status", type="categorical")],
        metrics=[Metric(name="order_count", agg="count")],
        pre_aggregations=[PreAggregation(name="base", type="original_sql")],
    )
    matcher = PreAggregationMatcher(model)

    assert matcher.find_matching_preagg(metrics=["order_count"], dimensions=["status"]) is None


def test_indexes_materialized_as_create_index_on_duckdb():
    """Declared indexes are emitted as CREATE INDEX during a DuckDB refresh."""
    conn = duckdb.connect(":memory:")
    conn.execute(
        "CREATE TABLE orders AS SELECT i as order_id, i % 3 as status, i * 10 as amount FROM generate_series(1, 9) t(i)"
    )

    preagg = PreAggregation(
        name="by_status",
        measures=["revenue"],
        dimensions=["status"],
        indexes=[Index(name="status_idx", columns=["status"])],
    )
    preagg.refresh(
        connection=conn,
        source_sql="SELECT status, SUM(amount) as revenue_raw FROM orders GROUP BY status",
        table_name="orders_preagg_by_status",
        mode="full",
        dialect="duckdb",
    )

    names = {
        row[0]
        for row in conn.execute(
            "SELECT index_name FROM duckdb_indexes() WHERE table_name = 'orders_preagg_by_status'"
        ).fetchall()
    }
    assert "orders_preagg_by_status_status_idx" in names, names


def test_indexes_skipped_for_unsupported_dialect():
    """Index DDL is skipped (no error, no index) for engines without standard CREATE INDEX."""
    conn = duckdb.connect(":memory:")
    conn.execute(
        "CREATE TABLE orders AS SELECT i as order_id, i % 3 as status, i * 10 as amount FROM generate_series(1, 9) t(i)"
    )

    preagg = PreAggregation(
        name="by_status",
        measures=["revenue"],
        dimensions=["status"],
        indexes=[Index(name="status_idx", columns=["status"])],
    )
    # Snowflake is not in the index-DDL dialect set: refresh must succeed and create no index.
    preagg.refresh(
        connection=conn,
        source_sql="SELECT status, SUM(amount) as revenue_raw FROM orders GROUP BY status",
        table_name="orders_preagg_by_status",
        mode="full",
        dialect="snowflake",
    )

    idx = conn.execute(
        "SELECT index_name FROM duckdb_indexes() WHERE table_name = 'orders_preagg_by_status'"
    ).fetchall()
    assert idx == []


def _layer_with_unbuilt_rollup(extra_dims=None):
    """A SemanticLayer with raw data and a rollup defined but NOT materialized."""
    from sidemantic import SemanticLayer

    layer = SemanticLayer()
    layer.adapter.execute(
        "CREATE TABLE orders AS SELECT i as id, i % 2 as status, i * 10 as amount FROM generate_series(1, 6) t(i)"
    )
    dims = [Dimension(name="status", type="categorical")]
    if extra_dims:
        dims.extend(extra_dims)
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=dims,
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
            pre_aggregations=[PreAggregation(name="by_status", measures=["revenue"], dimensions=["status"])],
        )
    )
    return layer


def test_preagg_falls_back_to_raw_when_table_missing():
    """Routing on, rollup matched but not built: fall back to raw and return correct results."""
    layer = _layer_with_unbuilt_rollup()

    result = layer.query(metrics=["orders.revenue"], dimensions=["orders.status"], use_preaggregations=True)
    rows = sorted(result.fetchall())

    # status 0 -> 20+40+60=120, status 1 -> 10+30+50=90 (computed from raw, since the rollup is absent)
    assert rows == [(0, 120), (1, 90)]


def test_preagg_strict_raises_when_no_rollup_matches():
    """Rollup-only mode errors when no pre-aggregation can serve the query."""
    from sidemantic.core.semantic_layer import PreaggregationStrictError

    layer = _layer_with_unbuilt_rollup(extra_dims=[Dimension(name="region", type="categorical", sql="'x'")])

    with pytest.raises(PreaggregationStrictError):
        layer.query(
            metrics=["orders.revenue"],
            dimensions=["orders.region"],  # not covered by the by_status rollup
            use_preaggregations=True,
            preagg_strict=True,
        )


def test_preagg_strict_raises_when_table_missing():
    """Rollup-only mode errors (no silent fallback) when the matching rollup table is not built."""
    from sidemantic.core.semantic_layer import PreaggregationStrictError

    layer = _layer_with_unbuilt_rollup()

    with pytest.raises(PreaggregationStrictError):
        layer.query(
            metrics=["orders.revenue"],
            dimensions=["orders.status"],
            use_preaggregations=True,
            preagg_strict=True,
        )


def test_sql_path_falls_back_to_raw_when_rollup_missing():
    """layer.sql() (the SQL/CLI path) also falls back to raw when the rollup table is missing."""
    layer = _layer_with_unbuilt_rollup()
    layer.use_preaggregations = True

    result = layer.sql("SELECT orders.revenue, orders.status FROM orders")

    # The rollup table is absent, so rows come back only if it fell back to raw.
    # The rewriter orders columns dimensions-first, so each row is (status, revenue).
    assert set(result.fetchall()) == {(0, 120), (1, 90)}


def test_sql_path_strict_raises_when_rollup_missing():
    """Strict (rollup-only) mode also applies on the SQL/CLI path."""
    from sidemantic.core.semantic_layer import PreaggregationStrictError

    layer = _layer_with_unbuilt_rollup()
    layer.use_preaggregations = True
    layer.preagg_strict = True

    with pytest.raises(PreaggregationStrictError):
        layer.sql("SELECT orders.revenue, orders.status FROM orders")


def _monthly_partitioned_model():
    model = Model(
        name="orders",
        table="orders",
        primary_key="id",
        dimensions=[Dimension(name="created_at", type="time", granularity="day")],
        metrics=[Metric(name="revenue", agg="sum", sql="amount")],
    )
    preagg = PreAggregation(
        name="monthly",
        measures=["revenue"],
        time_dimension="created_at",
        granularity="month",
        partition_granularity="month",
    )
    return model, preagg


def test_build_partitions_creates_one_table_per_bucket_and_covering_view():
    """A partitioned build materializes one table per month plus a covering view with correct totals."""
    conn = duckdb.connect(":memory:")
    conn.execute(
        "CREATE TABLE orders AS "
        "SELECT (DATE '2024-01-01' + INTERVAL (i) MONTH) AS created_at, (i + 1) * 100 AS amount "
        "FROM generate_series(0, 2) t(i)"
    )  # 2024-01, 2024-02, 2024-03
    model, preagg = _monthly_partitioned_model()

    built = preagg.build_partitions(conn, model)

    assert len(built) == 3  # one partition table per month bucket

    total = conn.execute("SELECT SUM(revenue_raw) FROM orders_preagg_monthly").fetchone()[0]
    assert total == 100 + 200 + 300  # covering view aggregates all partitions

    kind = conn.execute(
        "SELECT table_type FROM information_schema.tables WHERE table_name = 'orders_preagg_monthly'"
    ).fetchone()[0]
    assert kind == "VIEW"  # base name resolves to the covering view


def test_build_partitions_incremental_leaves_old_partitions_immutable():
    """With a lookback, only recent partitions rebuild; older ones are immutable."""
    conn = duckdb.connect(":memory:")
    conn.execute(
        "CREATE TABLE orders AS "
        "SELECT DATE_TRUNC('month', CURRENT_DATE) - INTERVAL (i) MONTH AS created_at, 100 AS amount "
        "FROM generate_series(0, 3) t(i)"
    )  # this month and the 3 prior months
    model, preagg = _monthly_partitioned_model()

    preagg.build_partitions(conn, model)  # full build: 4 partitions, each summing 100
    assert conn.execute("SELECT SUM(revenue_raw) FROM orders_preagg_monthly").fetchone()[0] == 400

    conn.execute("UPDATE orders SET amount = 999")  # source changes everywhere

    rebuilt = preagg.build_partitions(conn, model, lookback="2 months")
    assert len(rebuilt) < 4  # older partitions were not rebuilt

    oldest_bucket = conn.execute("SELECT DATE_TRUNC('month', MIN(created_at)) FROM orders").fetchone()[0]
    oldest_val = conn.execute(
        f"SELECT revenue_raw FROM orders_preagg_monthly WHERE created_at_month = TIMESTAMP '{oldest_bucket}'"
    ).fetchone()[0]
    assert oldest_val == 100  # immutable: still the pre-update value


def test_partitioned_refresh_requires_model():
    """refresh() never silently skips partitioning: a partitioned preagg without model errors."""
    conn = duckdb.connect(":memory:")
    _, preagg = _monthly_partitioned_model()

    with pytest.raises(ValueError, match="partitioned"):
        preagg.refresh(connection=conn, source_sql="SELECT 1", table_name="orders_preagg_monthly", mode="full")


def test_partitioned_refresh_with_model_delegates_to_build_partitions():
    """refresh(model=...) builds partitions and reports mode='partitioned'."""
    conn = duckdb.connect(":memory:")
    conn.execute(
        "CREATE TABLE orders AS "
        "SELECT (DATE '2024-01-01' + INTERVAL (i) MONTH) AS created_at, 100 AS amount FROM generate_series(0, 1) t(i)"
    )
    model, preagg = _monthly_partitioned_model()

    result = preagg.refresh(
        connection=conn, source_sql="", table_name="orders_preagg_monthly", mode="full", model=model
    )

    assert result.mode == "partitioned"
    assert conn.execute("SELECT SUM(revenue_raw) FROM orders_preagg_monthly").fetchone()[0] == 200


def _ungrouped_orders_layer(layer):
    """Build an orders base table + a PK-carrying detail rollup for ungrouped tests."""
    layer.use_preaggregations = True
    layer.conn.execute("CREATE TABLE orders (order_id INTEGER, status VARCHAR, amount DECIMAL(10, 2))")
    layer.conn.execute(
        "INSERT INTO orders VALUES "
        "(1, 'completed', 100.00), (2, 'completed', 300.00), (3, 'pending', 50.00), (4, 'pending', 25.00)"
    )
    # PK in GROUP BY => one row per order_id (count_raw == 1, revenue_raw == amount).
    layer.conn.execute(
        """
        CREATE TABLE orders_preagg_detail AS
        SELECT order_id, status, COUNT(*) AS count_raw, SUM(amount) AS revenue_raw
        FROM orders
        GROUP BY order_id, status
        """
    )
    model = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical", sql="status"),
            Dimension(name="order_id", type="categorical", sql="order_id"),
        ],
        metrics=[
            Metric(name="count", agg="count"),
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
        pre_aggregations=[
            PreAggregation(name="detail", measures=["count", "revenue"], dimensions=["order_id", "status"])
        ],
    )
    layer.add_model(model)
    return model


def test_ungrouped_routes_to_pk_carrying_rollup(layer):
    """An ungrouped query is served from a rollup that stores the primary key, returning stored rows."""
    _ungrouped_orders_layer(layer)

    preagg_sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.order_id", "orders.status"],
        order_by=["order_id"],
        ungrouped=True,
    )

    assert "orders_preagg_detail" in preagg_sql
    assert "used_preagg=true" in preagg_sql

    # Stored rows must match the raw ungrouped query (one row per order_id).
    baseline_rows = layer.query(
        metrics=["orders.revenue"],
        dimensions=["orders.order_id", "orders.status"],
        order_by=["order_id"],
        ungrouped=True,
        use_preaggregations=False,
    ).fetchall()
    preagg_rows = layer.adapter.execute(preagg_sql).fetchall()
    assert preagg_rows == baseline_rows


def test_ungrouped_preagg_sql_has_no_group_by(layer):
    """Ungrouped routing selects the raw measure column with no GROUP BY / HAVING / re-aggregation."""
    _ungrouped_orders_layer(layer)

    preagg_sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.order_id", "orders.status"],
        order_by=["order_id"],
        ungrouped=True,
    )

    assert "GROUP BY" not in preagg_sql.upper()
    assert "HAVING" not in preagg_sql.upper()
    # Raw column selected directly, NOT re-aggregated.
    assert "SUM(revenue_raw)" not in preagg_sql
    assert "revenue_raw" in preagg_sql


def test_ungrouped_explain_reports_pk_rollup_match(layer):
    """explain() reflects that a PK-carrying rollup serves the ungrouped query (no stale reason)."""
    _ungrouped_orders_layer(layer)

    plan = layer.explain(
        metrics=["orders.revenue"],
        dimensions=["orders.order_id", "orders.status"],
        ungrouped=True,
        use_preaggregations=True,
    )

    assert plan.used_preaggregation is True
    assert plan.selected_preagg == "detail"
    assert "preaggs require aggregation" not in (plan.routing_reason or "")


def test_ungrouped_rollup_without_pk_falls_to_raw(layer):
    """A rollup lacking the primary key cannot serve ungrouped (rows not unique); falls to raw tables."""
    layer.use_preaggregations = True
    layer.conn.execute("CREATE TABLE orders (order_id INTEGER, status VARCHAR, amount DECIMAL(10, 2))")
    layer.conn.execute(
        "INSERT INTO orders VALUES (1, 'completed', 100.00), (2, 'completed', 300.00), (3, 'pending', 50.00)"
    )
    # No order_id in the rollup => aggregated rows, must NOT serve ungrouped.
    layer.conn.execute(
        """
        CREATE TABLE orders_preagg_by_status AS
        SELECT status, COUNT(*) AS count_raw, SUM(amount) AS revenue_raw
        FROM orders
        GROUP BY status
        """
    )
    model = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical", sql="status"),
            Dimension(name="order_id", type="categorical", sql="order_id"),
        ],
        metrics=[
            Metric(name="count", agg="count"),
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
        pre_aggregations=[PreAggregation(name="by_status", measures=["count", "revenue"], dimensions=["status"])],
    )
    layer.add_model(model)

    sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.order_id", "orders.status"],
        ungrouped=True,
    )

    assert "orders_preagg_by_status" not in sql
    # Compiled against raw tables instead.
    assert "orders_cte" in sql
    assert "used_preagg=true" not in sql

    plan = layer.explain(
        metrics=["orders.revenue"],
        dimensions=["orders.order_id", "orders.status"],
        ungrouped=True,
        use_preaggregations=True,
    )
    assert plan.used_preaggregation is False


def test_ungrouped_composite_pk_partial_rollup_falls_to_raw(layer):
    """A rollup carrying only part of a composite primary key cannot guarantee unique rows."""
    layer.use_preaggregations = True
    model = Model(
        name="orders",
        table="orders",
        primary_key=["order_id", "line_id"],
        dimensions=[
            Dimension(name="order_id", type="categorical", sql="order_id"),
            Dimension(name="line_id", type="categorical", sql="line_id"),
            Dimension(name="status", type="categorical", sql="status"),
        ],
        metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        pre_aggregations=[PreAggregation(name="partial", measures=["revenue"], dimensions=["order_id", "status"])],
    )
    layer.add_model(model)

    sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.order_id", "orders.status"],
        ungrouped=True,
    )

    assert "orders_preagg_partial" not in sql


def test_ungrouped_avg_metric_bails_to_raw(layer):
    """avg under ungrouped is not a per-row value, so even a PK rollup must fall to raw."""
    layer.use_preaggregations = True
    layer.conn.execute("CREATE TABLE orders (order_id INTEGER, status VARCHAR, amount DECIMAL(10, 2))")
    layer.conn.execute("INSERT INTO orders VALUES (1, 'completed', 100.00), (2, 'pending', 50.00)")
    layer.conn.execute(
        """
        CREATE TABLE orders_preagg_detail AS
        SELECT order_id, status, SUM(amount) AS avg_amount_raw, COUNT(*) AS count_raw
        FROM orders
        GROUP BY order_id, status
        """
    )
    model = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical", sql="status"),
            Dimension(name="order_id", type="categorical", sql="order_id"),
        ],
        metrics=[
            Metric(name="avg_amount", agg="avg", sql="amount"),
            Metric(name="count", agg="count"),
        ],
        pre_aggregations=[
            PreAggregation(name="detail", measures=["avg_amount", "count"], dimensions=["order_id", "status"])
        ],
    )
    layer.add_model(model)

    sql = layer.compile(
        metrics=["orders.avg_amount"],
        dimensions=["orders.order_id", "orders.status"],
        ungrouped=True,
    )

    assert "orders_preagg_detail" not in sql
    assert "orders_cte" in sql


def test_ungrouped_strict_without_pk_rollup_raises(layer):
    """Strict mode + ungrouped errors when no PK-carrying rollup can serve the query."""
    from sidemantic.core.semantic_layer import PreaggregationStrictError

    layer.use_preaggregations = True
    layer.preagg_strict = True
    layer.conn.execute("CREATE TABLE orders (order_id INTEGER, status VARCHAR, amount DECIMAL(10, 2))")
    layer.conn.execute("INSERT INTO orders VALUES (1, 'completed', 100.00)")
    layer.conn.execute(
        """
        CREATE TABLE orders_preagg_by_status AS
        SELECT status, COUNT(*) AS count_raw, SUM(amount) AS revenue_raw
        FROM orders
        GROUP BY status
        """
    )
    model = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical", sql="status"),
            Dimension(name="order_id", type="categorical", sql="order_id"),
        ],
        metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        pre_aggregations=[PreAggregation(name="by_status", measures=["revenue"], dimensions=["status"])],
    )
    layer.add_model(model)

    with pytest.raises(PreaggregationStrictError):
        layer.query(
            metrics=["orders.revenue"],
            dimensions=["orders.order_id", "orders.status"],
            ungrouped=True,
        )


def test_full_refresh_rebuilds_all_partitions_ignoring_update_window():
    """mode='full' rebuilds every partition even when an update_window is declared."""
    conn = duckdb.connect(":memory:")
    # Four monthly buckets, all far older than a 1-month window.
    conn.execute(
        "CREATE TABLE orders AS "
        "SELECT (DATE '2020-01-01' + INTERVAL (i) MONTH) created_at, 100 amount FROM generate_series(0, 3) t(i)"
    )
    model, _ = _monthly_partitioned_model()
    preagg = PreAggregation(
        name="monthly",
        measures=["revenue"],
        time_dimension="created_at",
        granularity="month",
        partition_granularity="month",
        refresh_key=RefreshKey(incremental=True, update_window="1 month"),
    )

    result = preagg.refresh(
        connection=conn, source_sql="", table_name="orders_preagg_monthly", mode="full", model=model
    )

    assert result.mode == "partitioned"
    # All four months built despite the 1-month window (a full refresh ignores it).
    assert conn.execute("SELECT SUM(revenue_raw) FROM orders_preagg_monthly").fetchone()[0] == 400


def test_full_rebuild_drops_partitions_for_removed_buckets():
    """A full rebuild drops partition tables whose source bucket no longer exists."""
    conn = duckdb.connect(":memory:")
    conn.execute(
        "CREATE TABLE orders AS "
        "SELECT (DATE '2024-01-01' + INTERVAL (i) MONTH) created_at, 100 amount FROM generate_series(0, 2) t(i)"
    )
    model, preagg = _monthly_partitioned_model()

    preagg.build_partitions(conn, model, full_rebuild=True)
    assert conn.execute("SELECT SUM(revenue_raw) FROM orders_preagg_monthly").fetchone()[0] == 300

    # Remove the latest month's source rows, then full rebuild.
    conn.execute("DELETE FROM orders WHERE created_at = DATE '2024-03-01'")
    preagg.build_partitions(conn, model, full_rebuild=True)

    n_partitions = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE 'orders_preagg_monthly_p%' ESCAPE '\\'"
    ).fetchone()[0]
    assert n_partitions == 2  # the dropped March bucket's partition is gone
    assert conn.execute("SELECT SUM(revenue_raw) FROM orders_preagg_monthly").fetchone()[0] == 200  # no stale data


def test_ungrouped_with_metric_filter_bails_to_raw():
    """An ungrouped query with a metric (HAVING) filter falls through to raw, not the rollup."""
    from sidemantic import SemanticLayer

    layer = SemanticLayer()
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[Dimension(name="id", type="numeric"), Dimension(name="status", type="categorical")],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
            pre_aggregations=[PreAggregation(name="by_id", measures=["revenue"], dimensions=["id", "status"])],
        )
    )

    sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.status"],
        filters=["orders.revenue > 100"],
        ungrouped=True,
        use_preaggregations=True,
    )

    # The rollup carries the PK, but the metric filter cannot be applied ungrouped,
    # so routing must fall back to raw rather than silently drop the predicate.
    assert "orders_preagg_by_id" not in sql


def test_full_rebuild_with_no_buckets_drops_covering_view():
    """A full rebuild that finds no source rows drops the covering view (no stale references)."""
    conn = duckdb.connect(":memory:")
    conn.execute(
        "CREATE TABLE orders AS "
        "SELECT (DATE '2024-01-01' + INTERVAL (i) MONTH) created_at, 100 amount FROM generate_series(0, 1) t(i)"
    )
    model, preagg = _monthly_partitioned_model()

    preagg.build_partitions(conn, model, full_rebuild=True)
    assert conn.execute("SELECT SUM(revenue_raw) FROM orders_preagg_monthly").fetchone()[0] == 200

    # Empty the source, then full rebuild: no buckets -> view and partitions all gone.
    conn.execute("DELETE FROM orders")
    preagg.build_partitions(conn, model, full_rebuild=True)

    remaining = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE 'orders_preagg_monthly%' ESCAPE '\\'"
    ).fetchone()[0]
    assert remaining == 0  # covering view + partitions dropped; nothing left pointing at removed tables


def test_ungrouped_approx_count_distinct_bails_to_raw():
    """An ungrouped query using approx_count_distinct falls back to raw (the _raw state is not per-row)."""
    from sidemantic import SemanticLayer

    layer = SemanticLayer()
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[Dimension(name="id", type="numeric"), Dimension(name="status", type="categorical")],
            metrics=[Metric(name="uniq", agg="approx_count_distinct", sql="user_id")],
            pre_aggregations=[PreAggregation(name="by_id", measures=["uniq"], dimensions=["id", "status"])],
        )
    )

    sql = layer.compile(
        metrics=["orders.uniq"],
        dimensions=["orders.status"],
        ungrouped=True,
        use_preaggregations=True,
    )

    # The stored uniq_raw is an approximate-distinct state for the PK group, not a
    # per-row value, so the ungrouped drill must fall back to raw.
    assert "orders_preagg_by_id" not in sql


def test_query_with_post_process_falls_back_to_raw_when_rollup_missing():
    """post_process strips the routing marker, but fallback still triggers (detected pre-post-process)."""
    layer = _layer_with_unbuilt_rollup()
    layer.use_preaggregations = True

    result = layer.query(
        metrics=["orders.revenue"],
        dimensions=["orders.status"],
        order_by=["orders.status"],
        offset=1,
        post_process="SELECT * FROM ({inner})",
    )

    # Offset survives both the post-process recompile and the raw-table fallback.
    assert result.fetchall() == [(1, 90)]


def test_is_missing_relation_error_recognizes_bigquery_not_found():
    """BigQuery surfaces missing tables as '404 Not found: Table ...', which must trigger fallback."""
    from sidemantic.core.semantic_layer import SemanticLayer

    err = Exception("404 Not found: Table myproj:ds.orders_preagg_by_status was not found in location US")
    assert SemanticLayer._is_missing_relation_error(err)


def test_build_partitions_scopes_discovery_to_target_schema():
    """Partition discovery is scoped to the target schema, ignoring same-named tables elsewhere."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA s")
    conn.execute(
        "CREATE TABLE s.orders AS "
        "SELECT (DATE '2024-01-01' + INTERVAL (i) MONTH) created_at, 100 amount FROM generate_series(0, 1) t(i)"
    )
    # A stray same-named partition in the default schema must NOT pollute the s view.
    conn.execute("CREATE TABLE orders_preagg_monthly_p99999999 AS SELECT 1 AS x")

    model = Model(
        name="orders",
        table="s.orders",
        primary_key="id",
        dimensions=[Dimension(name="created_at", type="time", granularity="day")],
        metrics=[Metric(name="revenue", agg="sum", sql="amount")],
    )
    preagg = PreAggregation(
        name="monthly",
        measures=["revenue"],
        time_dimension="created_at",
        granularity="month",
        partition_granularity="month",
    )
    preagg.build_partitions(conn, model, schema="s")

    # The covering view references only the two s-schema partitions (not the mis-qualified stray).
    assert conn.execute("SELECT SUM(revenue_raw) FROM s.orders_preagg_monthly").fetchone()[0] == 200


def test_lambda_union_does_not_double_count_boundary_bucket():
    """A mid-bucket build_range_end must not double-count the boundary bucket's post-cutoff rows."""
    from sidemantic import SemanticLayer

    layer = SemanticLayer()
    layer.adapter.execute(
        "CREATE TABLE orders AS SELECT * FROM (VALUES "
        "(TIMESTAMP '2024-01-01 09:00', 100), "  # day 1
        "(TIMESTAMP '2024-01-02 09:00', 200), "  # day 2, before the noon cutoff
        "(TIMESTAMP '2024-01-02 15:00', 300)  "  # day 2, after the cutoff
        ") t(created_at, amount)"
    )
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[Dimension(name="created_at", type="time", granularity="day")],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
            pre_aggregations=[
                PreAggregation(
                    name="lam",
                    type="lambda",
                    union_with_source_data=True,
                    measures=["revenue"],
                    time_dimension="created_at",
                    granularity="day",
                    build_range_end="TIMESTAMP '2024-01-02 12:00'",
                )
            ],
        )
    )
    model = layer.graph.get_model("orders")
    # Materialize the batch rollup over the FULL source (so day 2 = 200 + 300 = 500).
    layer.adapter.execute(
        f"CREATE TABLE orders_preagg_lam AS {model.pre_aggregations[0].generate_materialization_sql(model)}"
    )

    result = layer.query(
        metrics=["orders.revenue"],
        dimensions=["orders.created_at__day"],
        use_preaggregations=True,
    )
    by_day = {str(row[0])[:10]: row[1] for row in result.fetchall()}

    assert by_day["2024-01-01"] == 100  # from the batch leg
    assert by_day["2024-01-02"] == 500  # boundary bucket re-aggregated once from source, not 800


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


def _multi_fact_layer():
    """orders (hub) with one_to_many line_items: metrics from both models fan out."""
    from sidemantic import Relationship, SemanticLayer

    layer = SemanticLayer()
    con = layer.adapter.conn
    con.execute("CREATE TABLE orders (id INTEGER, region VARCHAR, status VARCHAR, amount DOUBLE)")
    con.execute("INSERT INTO orders VALUES (1,'US','completed',100),(2,'EU','completed',200)")
    con.execute("CREATE TABLE line_items (id INTEGER, order_id INTEGER, qty INTEGER)")
    con.execute("INSERT INTO line_items VALUES (1,1,5),(2,2,7)")
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[
                Dimension(name="status", type="categorical"),
                Dimension(name="region", type="categorical"),
            ],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
            relationships=[Relationship(name="line_items", type="one_to_many", foreign_key="order_id")],
        )
    )
    layer.add_model(
        Model(
            name="line_items",
            table="line_items",
            primary_key="id",
            dimensions=[Dimension(name="qty_d", sql="qty", type="numeric")],
            metrics=[Metric(name="total_qty", agg="sum", sql="qty")],
            relationships=[Relationship(name="orders", type="many_to_one", foreign_key="order_id")],
        )
    )
    return layer


def test_fanout_preagg_applies_query_filters_to_every_child():
    """A query-level filter must scope EVERY metric model, not just its owner.

    Regression: filters were partitioned by owning model, so orders.region='US'
    filtered the orders child while the line_items child aggregated all regions --
    one result row mixing two different populations.
    """
    layer = _multi_fact_layer()

    rows = layer.query(
        metrics=["orders.revenue", "line_items.total_qty"],
        dimensions=["orders.status"],
        filters=["orders.region = 'US'"],
    ).fetchall()
    # revenue AND total_qty both restricted to US orders (order 1: qty 5).
    assert rows == [("completed", 100.0, 5)]

    # Unfiltered sanity: both metrics cover the full population.
    rows = layer.query(
        metrics=["orders.revenue", "line_items.total_qty"],
        dimensions=["orders.status"],
    ).fetchall()
    assert rows == [("completed", 300.0, 12)]


def test_fanout_preagg_metric_filter_stays_on_outer_query():
    """Metric-referencing filters filter the joined result, not the child inputs."""
    layer = _multi_fact_layer()

    rows = layer.query(
        metrics=["orders.revenue", "line_items.total_qty"],
        dimensions=["orders.status"],
        filters=["orders.revenue > 50"],
    ).fetchall()
    assert rows == [("completed", 300.0, 12)]

    rows = layer.query(
        metrics=["orders.revenue", "line_items.total_qty"],
        dimensions=["orders.status"],
        filters=["orders.revenue > 500"],
    ).fetchall()
    assert rows == []
