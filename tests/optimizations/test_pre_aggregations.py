"""Tests for pre-aggregation functionality."""

from datetime import datetime

import duckdb
import pytest

from sidemantic import Dimension, Metric, Model, SemanticLayer
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


def test_sql_generation_with_preagg():
    """Test SQL generation using pre-aggregation."""
    sl = SemanticLayer(use_preaggregations=True)

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

    sl.add_model(model)

    # Query that matches the pre-aggregation
    sql = sl.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.status", "orders.created_at__day"],
    )

    # Should use pre-aggregation table
    assert "orders_preagg_daily_rollup" in sql
    assert "SUM(revenue_raw)" in sql
    # Should NOT use CTEs (direct query on pre-agg)
    assert "WITH" not in sql or "_cte" not in sql


def test_sql_generation_without_preagg():
    """Test SQL generation falls back when no pre-aggregation matches."""
    sl = SemanticLayer()

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

    sl.add_model(model)

    # Query uses customer_id which isn't in pre-agg
    sql = sl.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.customer_id"],
    )

    # Should NOT use pre-aggregation
    assert "orders_preagg_" not in sql
    # Should use normal CTE-based approach
    assert "orders_cte" in sql or "FROM public.orders" in sql


def test_preagg_with_filters():
    """Test pre-aggregation usage with filters."""
    sl = SemanticLayer(use_preaggregations=True)

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

    sl.add_model(model)

    # Query with filter
    sql = sl.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.status"],
        filters=["orders.region = 'US'"],
    )

    # Should use pre-aggregation with filter applied
    assert "orders_preagg_by_status_region" in sql
    assert "region = 'US'" in sql or "region='US'" in sql


def test_preagg_granularity_conversion():
    """Test pre-aggregation with granularity conversion."""
    sl = SemanticLayer(use_preaggregations=True)

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

    sl.add_model(model)

    # Query at month level (coarser than day)
    sql = sl.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.created_at__month"],
    )

    # Should use pre-aggregation and convert granularity
    assert "orders_preagg_daily" in sql
    assert "DATE_TRUNC('month'" in sql


def test_preagg_disabled_by_default():
    """Test that pre-aggregations are disabled by default."""
    sl = SemanticLayer()  # No use_preaggregations flag

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

    sl.add_model(model)

    # Query that could match pre-aggregation
    sql = sl.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.status", "orders.created_at__day"],
    )

    # Should NOT use pre-aggregation (disabled by default)
    assert "orders_preagg_" not in sql
    # Should use normal CTE-based approach
    assert "orders_cte" in sql


def test_preagg_per_query_override():
    """Test per-query override of pre-aggregation setting."""
    sl = SemanticLayer(use_preaggregations=False)  # Disabled globally

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

    sl.add_model(model)

    # Override to enable for this query
    sql = sl.compile(
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

    # With append mode, we'll have duplicates
    total_revenue = conn.execute("""
        SELECT SUM(total_revenue)
        FROM orders_preagg_daily
        WHERE order_date = DATE '2024-01-05'
    """).fetchone()[0]

    assert total_revenue == 1208  # 104 + 1104 from lookback


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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
