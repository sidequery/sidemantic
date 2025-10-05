"""Test metric-level filters."""

from sidemantic import Dimension, Metric, Model, SemanticLayer


def test_metric_level_filter_basic():
    """Test basic metric-level filter."""
    layer = SemanticLayer()

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


def test_metric_level_multiple_filters():
    """Test metric with multiple filters."""
    layer = SemanticLayer()

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


def test_metric_filters_combined_with_query_filters():
    """Test metric-level filters combined with query-level filters."""
    layer = SemanticLayer()

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


def test_mixed_filtered_and_unfiltered_metrics():
    """Test querying both filtered and unfiltered metrics together."""
    layer = SemanticLayer()

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


def test_metric_filter_with_time_dimension():
    """Test metric filters work with time dimensions."""
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
            Metric(
                name="recent_completed_revenue",
                agg="sum",
                sql="amount",
                filters=["{model}.status = 'completed'", "{model}.created_at >= CURRENT_DATE - 30"],
            ),
        ],
    )

    layer.add_model(orders)

    sql = layer.compile(
        metrics=["orders.recent_completed_revenue"], dimensions=["orders.created_at__month"]
    )

    # Should contain both filters
    assert "orders_cte.status = 'completed'" in sql
    assert (
        "CURRENT_DATE - 30" in sql or "CURRENT_DATE-30" in sql
    )  # SQLGlot might format differently
