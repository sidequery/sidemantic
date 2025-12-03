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
    assert "AVG(price) as avg_price_raw" in sql


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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
