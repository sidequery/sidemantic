"""Test metric-level filters."""

import duckdb

from sidemantic import Dimension, Metric, Model, SemanticLayer
from tests.utils import df_rows


def test_metric_level_filter_basic(layer):
    """Test basic metric-level filter.

    Metric-level filters are applied via CASE WHEN in the CTE on the raw column,
    NOT in the WHERE clause or the outer aggregation. This ensures:
    1. Each metric's filter only affects that specific metric
    2. Filters are applied to raw columns, not transformed CTE columns
    """
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
            Metric(
                name="completed_revenue",
                agg="sum",
                sql="amount",
                filters=["{model}.status = 'completed'"],
                description="Revenue from completed orders only",
            ),
        ],
    )

    layer.add_model(orders)

    # Query the filtered metric
    sql = layer.compile(metrics=["orders.completed_revenue"], dimensions=["orders.region"])

    print("SQL with metric-level filter:")
    print(sql)

    # Filter should be in CTE's CASE WHEN on raw column, not in outer query
    assert "CASE WHEN status = 'completed' THEN amount" in sql
    # Outer query should just aggregate the pre-filtered raw column
    assert "SUM(orders_cte.completed_revenue_raw)" in sql


def test_metric_level_multiple_filters(layer):
    """Test metric with multiple filters."""
    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical"),
            Dimension(name="amount", type="numeric"),
            Dimension(name="region", type="categorical"),
        ],
        metrics=[
            Metric(
                name="high_value_completed_revenue",
                agg="sum",
                sql="amount",
                filters=["{model}.status = 'completed'", "{model}.amount > 100"],
                description="Revenue from high-value completed orders",
            ),
        ],
    )

    layer.add_model(orders)

    sql = layer.compile(metrics=["orders.high_value_completed_revenue"])

    # Should contain both filters combined via CASE WHEN in CTE
    assert "CASE WHEN status = 'completed' AND amount > 100" in sql
    assert "high_value_completed_revenue_raw" in sql


def test_metric_filters_combined_with_query_filters(layer):
    """Test metric-level filters combined with query-level filters."""
    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical"),
            Dimension(name="region", type="categorical"),
        ],
        metrics=[
            Metric(
                name="completed_revenue",
                agg="sum",
                sql="amount",
                filters=["{model}.status = 'completed'"],
            ),
        ],
    )

    layer.add_model(orders)

    # Add query-level filter on top of metric-level filter
    sql = layer.compile(metrics=["orders.completed_revenue"], filters=["orders_cte.region = 'US'"])

    # Metric filter should be in CASE WHEN, query filter pushed down to CTE WHERE
    assert "CASE WHEN status = 'completed'" in sql  # Metric filter in CTE
    assert "region = 'US'" in sql  # Query filter pushed down into CTE


def test_mixed_filtered_and_unfiltered_metrics(layer):
    """Test querying both filtered and unfiltered metrics together.

    This is the key use case for CASE WHEN filtered measures:
    - total_revenue: SUM(amount) - no filter, uses all rows
    - completed_revenue: SUM(CASE WHEN status='completed' THEN amount END) - filtered
    Both coexist in same query with different filtering behavior.
    """
    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical"),
        ],
        metrics=[
            Metric(name="total_revenue", agg="sum", sql="amount"),
            Metric(
                name="completed_revenue",
                agg="sum",
                sql="amount",
                filters=["{model}.status = 'completed'"],
            ),
        ],
    )

    layer.add_model(orders)

    sql = layer.compile(metrics=["orders.total_revenue", "orders.completed_revenue"])

    # Completed filter should be in CASE WHEN for completed_revenue only
    assert "CASE WHEN status = 'completed'" in sql
    assert "completed_revenue_raw" in sql
    # Total revenue should have no CASE WHEN
    assert "amount AS total_revenue_raw" in sql
    # Both metrics should be in the SELECT
    assert "total_revenue" in sql
    assert "completed_revenue" in sql


def test_metric_filter_with_time_dimension(layer):
    """Test metric filters work with time dimensions."""
    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="created_at", type="time", granularity="day"),
            Dimension(name="status", type="categorical"),
        ],
        metrics=[
            Metric(
                name="recent_completed_revenue",
                agg="sum",
                sql="amount",
                filters=["{model}.status = 'completed'", "{model}.created_at >= CURRENT_DATE - 30"],
            ),
        ],
    )

    layer.add_model(orders)

    sql = layer.compile(metrics=["orders.recent_completed_revenue"], dimensions=["orders.created_at__month"])

    # Both filters should be in CASE WHEN combined with AND
    assert "CASE WHEN status = 'completed' AND created_at >= CURRENT_DATE" in sql
    assert "recent_completed_revenue_raw" in sql


def test_metric_filter_column_not_in_query_dimensions(layer):
    """Test that metric filter columns are included in CTE even when not in query dimensions.

    This is a regression test for the bug where filters like:
        filters: ["state IN ('confirmed', 'completed')"]
    would fail because the 'state' column wasn't added to the CTE SELECT list
    when 'state' wasn't explicitly requested as a dimension in the query.
    """
    bookings = Model(
        name="bookings",
        table="wide_bookings",
        primary_key="booking_id",
        dimensions=[
            Dimension(name="state", type="categorical"),
            Dimension(name="region", type="categorical"),
        ],
        metrics=[
            Metric(
                name="gross_booking_value",
                agg="sum",
                sql="gross_booking_value",
                filters=["{model}.state IN ('confirmed', 'completed', 'cancelled')"],
            ),
        ],
    )

    layer.add_model(bookings)

    # Query the metric WITHOUT including 'state' in dimensions
    # This should still work because the filter needs 'state' in the CTE
    sql = layer.compile(metrics=["bookings.gross_booking_value"], dimensions=["bookings.region"])

    print("Generated SQL:")
    print(sql)

    # The 'state' column must be in the CTE for the CASE WHEN filter to work
    # Check that state appears in the CTE SELECT (before FROM)
    cte_match = sql.split("FROM")[0]  # Get the CTE SELECT part
    assert "state" in cte_match, f"'state' column should be in CTE SELECT for filter to work. CTE: {cte_match}"

    # The filter should be in a CASE WHEN expression
    assert "CASE WHEN state IN ('confirmed', 'completed', 'cancelled')" in sql


def test_metric_filter_multiple_columns_not_in_dimensions(layer):
    """Test multiple filter columns are included in CTE when not in query dimensions."""
    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical"),
            Dimension(name="payment_method", type="categorical"),
            Dimension(name="region", type="categorical"),
        ],
        metrics=[
            Metric(
                name="card_completed_revenue",
                agg="sum",
                sql="amount",
                filters=[
                    "{model}.status = 'completed'",
                    "{model}.payment_method IN ('visa', 'mastercard')",
                ],
            ),
        ],
    )

    layer.add_model(orders)

    # Query with only 'region' dimension - both status and payment_method need to be in CTE
    sql = layer.compile(metrics=["orders.card_completed_revenue"], dimensions=["orders.region"])

    print("Generated SQL:")
    print(sql)

    cte_match = sql.split("FROM")[0]
    assert "status" in cte_match, f"'status' should be in CTE SELECT. CTE: {cte_match}"
    assert "payment_method" in cte_match, f"'payment_method' should be in CTE SELECT. CTE: {cte_match}"


def test_query_level_metric_filters_use_having(layer):
    """Test that query-level filters on metrics use HAVING clause, not WHERE.

    Bug: Query filters like orders.revenue > 100 were applied in WHERE clause
    against _raw columns before aggregation, giving wrong results.

    Fix: Detect when filter references a metric and use HAVING instead.
    """
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

    # Filter on aggregated metric should use HAVING
    sql = layer.compile(metrics=["orders.revenue"], dimensions=["orders.region"], filters=["orders.revenue > 100"])

    # Should have HAVING clause, not WHERE clause with revenue_raw
    assert "HAVING" in sql
    assert "revenue > 100" in sql
    # Should NOT filter on raw column before aggregation
    assert "revenue_raw > 100" not in sql


def test_dimension_filters_use_where(layer):
    """Test that filters on dimensions still use WHERE clause."""
    layer = SemanticLayer()

    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="region", type="categorical"),
            Dimension(name="status", type="categorical"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    layer.add_model(orders)

    # Filter on dimension should use WHERE
    sql = layer.compile(
        metrics=["orders.revenue"], dimensions=["orders.region"], filters=["orders.status = 'completed'"]
    )

    # Should have WHERE clause
    assert "WHERE" in sql
    assert "status = 'completed'" in sql
    # Should NOT have HAVING for dimension filter
    assert "HAVING" not in sql


def test_mixed_filters_separate_where_and_having(layer):
    """Test that mixed metric and dimension filters use both WHERE and HAVING."""
    layer = SemanticLayer()

    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="region", type="categorical"),
            Dimension(name="status", type="categorical"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    layer.add_model(orders)

    sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.region"],
        filters=["orders.status = 'completed'", "orders.revenue > 100"],
    )

    # Should have both WHERE (for dimension) and HAVING (for metric)
    assert "WHERE" in sql
    assert "status = 'completed'" in sql
    assert "HAVING" in sql
    assert "revenue > 100" in sql


def test_metric_level_filters_use_case_when(layer):
    """Test that Metric.filters are applied via CASE WHEN inside aggregation.

    Metric.filters are applied via CASE WHEN so each metric's filter only affects
    that specific metric. This allows querying multiple metrics with different
    filters in the same query without them interfering with each other.

    Example: SUM(CASE WHEN status = 'completed' THEN amount END) AS completed_revenue
    """
    layer = SemanticLayer()

    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="region", type="categorical"),
            Dimension(name="status", type="categorical"),
        ],
        metrics=[
            Metric(name="completed_revenue", agg="sum", sql="amount", filters=["{model}.status = 'completed'"]),
        ],
    )

    layer.add_model(orders)

    sql = layer.compile(metrics=["orders.completed_revenue"], dimensions=["orders.region"])

    # Metric-level filter should be inside CASE WHEN, not in WHERE clause
    assert "CASE WHEN" in sql
    assert "status = 'completed'" in sql
    # Should NOT have a WHERE clause for this query (no query-level dimension filters)
    assert "WHERE" not in sql


def test_having_filter_with_actual_data(layer):
    """Integration test with real DuckDB data to verify HAVING filter correctness."""
    conn = duckdb.connect(":memory:")

    # Create test data
    conn.execute("""
        CREATE TABLE orders (
            order_id INTEGER,
            customer_id INTEGER,
            region VARCHAR,
            status VARCHAR,
            amount DECIMAL(10, 2)
        )
    """)

    conn.execute("""
        INSERT INTO orders VALUES
        (1, 101, 'US', 'completed', 50),
        (2, 101, 'US', 'completed', 150),  -- customer 101: 200 total
        (3, 102, 'EU', 'completed', 300),
        (4, 102, 'EU', 'pending', 75),      -- EU total: 375, completed: 300
        (5, 103, 'US', 'cancelled', 25)
    """)

    layer = SemanticLayer()

    orders = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        dimensions=[
            Dimension(name="region", type="categorical"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    layer.conn = conn
    layer.add_model(orders)

    # Test HAVING filter (should filter aggregated revenue)
    result = layer.query(
        metrics=["orders.revenue"],
        dimensions=["orders.region"],
        filters=["orders.revenue >= 200"],  # Should use HAVING
    )
    rows = df_rows(result)

    # Should only return regions with total revenue >= 200
    # US: 50+150+25 = 225, EU: 300+75 = 375
    # Both should be included
    assert len(rows) == 2
    revenues = {row[0]: row[1] for row in rows}
    assert revenues["US"] == 225.0
    assert revenues["EU"] == 375.0
