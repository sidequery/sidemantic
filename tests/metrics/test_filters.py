"""Test metric-level filters."""

from sidemantic import Dimension, Metric, Model


def test_metric_level_filter_basic(layer):
    """Test basic metric-level filter."""
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

    # Should contain the metric's filter
    assert "orders_cte.status = 'completed'" in sql
    assert "WHERE" in sql


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

    # Should contain both filters
    assert "orders_cte.status = 'completed'" in sql
    assert "orders_cte.amount > 100" in sql


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

    # Should contain both metric filter and query filter
    # Note: query filter gets pushed down into CTE, metric filter stays in main query
    assert "orders_cte.status = 'completed'" in sql  # Metric-level filter in main query
    assert "region = 'US'" in sql  # Query filter pushed down into CTE


def test_mixed_filtered_and_unfiltered_metrics(layer):
    """Test querying both filtered and unfiltered metrics together."""
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

    # Should have the completed filter for completed_revenue
    # but total_revenue shouldn't be affected
    assert "orders_cte.status = 'completed'" in sql
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

    # Should contain both filters
    assert "orders_cte.status = 'completed'" in sql
    assert "CURRENT_DATE - 30" in sql or "CURRENT_DATE-30" in sql  # SQLGlot might format differently


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

    # The 'state' column must be in the CTE for the filter to work
    # Check that state appears in the CTE SELECT (before FROM)
    cte_match = sql.split("FROM")[0]  # Get the CTE SELECT part
    assert "state" in cte_match, f"'state' column should be in CTE SELECT for filter to work. CTE: {cte_match}"

    # The filter should be in the WHERE clause
    assert (
        "state IN ('confirmed', 'completed', 'cancelled')" in sql
        or "state IN ('cancelled', 'completed', 'confirmed')" in sql
    )  # Order might vary


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
