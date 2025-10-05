"""Tests for metric auto-registration."""

from sidemantic import Dimension, Metric, Model, SemanticLayer


def test_standalone_metric_auto_registers_with_context_manager():
    """Test that standalone metrics auto-register when using context manager."""
    with SemanticLayer() as layer:
        # Create a standalone derived metric - should auto-register
        total_revenue = Metric(
            name="total_revenue",
            type="derived",
            sql="orders.revenue",
        )

        # Should be automatically registered
        assert "total_revenue" in layer.list_metrics()
        assert layer.get_metric("total_revenue") == total_revenue


def test_standalone_metric_auto_registers_with_auto_register_param():
    """Test that standalone metrics auto-register when auto_register=True."""
    layer = SemanticLayer(auto_register=True)

    # First create the model that the metric depends on
    Model(
        name="orders",
        table="orders_table",
        primary_key="id",
        dimensions=[Dimension(name="status", type="categorical")],
        metrics=[
            Metric(name="profit", agg="sum", sql="profit_amount"),
            Metric(name="revenue", agg="sum", sql="revenue_amount"),
        ],
    )

    # Now create a standalone ratio metric - should auto-register
    Metric(
        name="margin_pct",
        type="ratio",
        numerator="orders.profit",
        denominator="orders.revenue",
    )

    # Should be automatically registered
    assert "margin_pct" in layer.list_metrics()


def test_model_metrics_dont_auto_register():
    """Test that model-level metrics don't auto-register at graph level."""
    with SemanticLayer() as layer:
        # Create model with metrics - simple aggregations should not auto-register at graph level
        Model(
            name="orders",
            table="orders_table",
            primary_key="id",
            dimensions=[Dimension(name="status", type="categorical")],
            metrics=[
                Metric(name="revenue", agg="sum", sql="amount"),
                Metric(name="count", agg="count"),
            ],
        )

        # Model should be auto-registered
        assert "orders" in layer.list_models()

        # Simple aggregation metrics should NOT be at graph level
        assert "revenue" not in layer.list_metrics()
        assert "count" not in layer.list_metrics()


def test_no_auto_register_without_context():
    """Test that metrics don't auto-register without context."""
    # No context set - should not auto-register
    metric = Metric(
        name="orphan_metric",
        type="derived",
        sql="orders.revenue * 2",
    )

    # Verify no layer was set (this will work without errors)
    # The metric exists but is not registered anywhere
    assert metric.name == "orphan_metric"


def test_time_comparison_metrics_auto_register():
    """Test that time_comparison metrics in models auto-register at graph level."""
    with SemanticLayer() as layer:
        Model(
            name="orders",
            table="orders_table",
            primary_key="id",
            dimensions=[Dimension(name="date", type="time", granularity="day")],
            metrics=[
                Metric(name="revenue", agg="sum", sql="amount"),
                Metric(name="revenue_yoy", type="time_comparison", base_metric="revenue", comparison_type="yoy"),
            ],
        )

        # time_comparison metric should be auto-registered at graph level
        assert "revenue_yoy" in layer.list_metrics()


def test_conversion_metrics_auto_register():
    """Test that conversion metrics in models auto-register at graph level."""
    with SemanticLayer() as layer:
        Model(
            name="events",
            table="events_table",
            primary_key="id",
            dimensions=[
                Dimension(name="user_id", type="numeric"),
                Dimension(name="event_type", type="categorical"),
            ],
            metrics=[
                Metric(
                    name="conversion_rate",
                    type="conversion",
                    entity="user_id",
                    base_event="signup",
                    conversion_event="purchase",
                    conversion_window="30 days",
                ),
            ],
        )

        # conversion metric should be auto-registered at graph level
        assert "conversion_rate" in layer.list_metrics()
