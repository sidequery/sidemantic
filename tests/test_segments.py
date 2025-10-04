"""Test segment functionality."""

from sidemantic import Dimension, Metric, Model, Segment, SemanticLayer


def test_segment_basic():
    """Test basic segment usage."""
    layer = SemanticLayer()

    # Create a model with a segment
    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical"),
            Dimension(name="amount", type="numeric"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
            Metric(name="order_count", agg="count"),
        ],
        segments=[
            Segment(
                name="completed_orders", sql="{model}.status = 'completed'", description="Only completed orders"
            ),
            Segment(name="high_value", sql="{model}.amount > 100", description="High value orders"),
        ],
    )

    layer.add_model(orders)

    # Test segment resolution
    sql = layer.compile(metrics=["orders.revenue"], dimensions=["orders.status"], segments=["orders.completed_orders"])

    # Should contain the segment filter
    assert "orders_cte.status = 'completed'" in sql
    assert "WHERE" in sql


def test_multiple_segments():
    """Test multiple segments combined with AND."""
    layer = SemanticLayer()

    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical"),
            Dimension(name="amount", type="numeric"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
        segments=[
            Segment(name="completed", sql="{model}.status = 'completed'"),
            Segment(name="high_value", sql="{model}.amount > 100"),
        ],
    )

    layer.add_model(orders)

    sql = layer.compile(metrics=["orders.revenue"], segments=["orders.completed", "orders.high_value"])

    # Should contain both segment filters
    assert "orders_cte.status = 'completed'" in sql
    assert "orders_cte.amount > 100" in sql
    assert "WHERE" in sql


def test_segment_with_filters():
    """Test segments combined with regular filters."""
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
        ],
        segments=[
            Segment(name="completed", sql="{model}.status = 'completed'"),
        ],
    )

    layer.add_model(orders)

    sql = layer.compile(
        metrics=["orders.revenue"],
        segments=["orders.completed"],
        filters=["orders_cte.region = 'US'"]
    )

    # Should contain both segment and regular filter
    assert "orders_cte.status = 'completed'" in sql
    assert "orders_cte.region = 'US'" in sql
    assert "WHERE" in sql
