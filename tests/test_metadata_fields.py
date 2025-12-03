"""Test metadata fields on Metrics and Dimensions."""

from sidemantic import Dimension, Metric, Model


def test_metric_format_fields():
    """Test format and value_format_name fields on Metric."""
    metric = Metric(name="revenue", agg="sum", sql="amount", format="$#,##0.00", value_format_name="usd")

    assert metric.format == "$#,##0.00"
    assert metric.value_format_name == "usd"


def test_dimension_format_fields():
    """Test format and value_format_name fields on Dimension."""
    dimension = Dimension(name="discount_rate", type="numeric", format="0.00%", value_format_name="percent")

    assert dimension.format == "0.00%"
    assert dimension.value_format_name == "percent"


def test_metric_drill_fields():
    """Test drill_fields on Metric."""
    metric = Metric(
        name="revenue",
        agg="sum",
        sql="amount",
        drill_fields=["customer.name", "order.id", "order.created_at"],
    )

    assert metric.drill_fields == ["customer.name", "order.id", "order.created_at"]
    assert len(metric.drill_fields) == 3


def test_metric_non_additive_dimension():
    """Test non_additive_dimension for metrics that can't be summed."""
    # Average metrics shouldn't be summed across time
    avg_metric = Metric(name="avg_order_value", agg="avg", sql="order_amount", non_additive_dimension="order_date")

    assert avg_metric.non_additive_dimension == "order_date"

    # Count distinct also shouldn't be summed
    distinct_metric = Metric(
        name="unique_customers",
        agg="count_distinct",
        sql="customer_id",
        non_additive_dimension="order_date",
    )

    assert distinct_metric.non_additive_dimension == "order_date"


def test_model_default_time_dimension_and_grain():
    """Test default_time_dimension and default_grain on Model."""
    model = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        default_time_dimension="order_date",
        default_grain="day",
        dimensions=[
            Dimension(name="order_date", type="time", granularity="day"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    assert model.default_time_dimension == "order_date"
    assert model.default_grain == "day"

    # Monthly model
    monthly_model = Model(
        name="monthly_orders",
        table="orders_table",
        primary_key="order_id",
        default_time_dimension="order_date",
        default_grain="month",
        dimensions=[
            Dimension(name="order_date", type="time", granularity="day"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    assert monthly_model.default_grain == "month"


def test_all_metadata_fields_together():
    """Test all metadata fields can be set together."""
    metric = Metric(
        name="revenue",
        agg="sum",
        sql="amount",
        format="$#,##0.00",
        value_format_name="usd",
        drill_fields=["customer.name", "product.name"],
        description="Total revenue from completed orders",
        label="Revenue (USD)",
    )

    # Verify all fields
    assert metric.format == "$#,##0.00"
    assert metric.value_format_name == "usd"
    assert metric.drill_fields == ["customer.name", "product.name"]
    assert metric.description == "Total revenue from completed orders"
    assert metric.label == "Revenue (USD)"


def test_metadata_fields_in_model(layer):
    """Test metadata fields work when Metric is part of a Model."""
    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        default_time_dimension="order_date",
        default_grain="day",
        dimensions=[
            Dimension(name="status", type="categorical", label="Order Status"),
            Dimension(name="discount_pct", type="numeric", format="0.0%", value_format_name="percent"),
            Dimension(name="order_date", type="time", granularity="day"),
        ],
        metrics=[
            Metric(
                name="revenue",
                agg="sum",
                sql="amount",
                format="$#,##0.00",
                value_format_name="usd",
                drill_fields=["status", "customer_id"],
            ),
            Metric(
                name="avg_order_value",
                agg="avg",
                sql="amount",
                format="$#,##0.00",
                non_additive_dimension="order_date",
                description="Average order value - do not sum across time",
            ),
        ],
    )

    layer.add_model(orders)

    # Verify we can retrieve metrics with metadata
    revenue = orders.get_metric("revenue")
    assert revenue is not None
    assert revenue.format == "$#,##0.00"
    assert revenue.drill_fields == ["status", "customer_id"]

    # Verify model-level defaults
    assert orders.default_time_dimension == "order_date"
    assert orders.default_grain == "day"

    avg_value = orders.get_metric("avg_order_value")
    assert avg_value is not None
    assert avg_value.non_additive_dimension == "order_date"

    # Verify dimensions
    discount = orders.get_dimension("discount_pct")
    assert discount is not None
    assert discount.format == "0.0%"


def test_metadata_fields_optional():
    """Test that all metadata fields are optional."""
    # Should work with no metadata fields
    metric = Metric(name="revenue", agg="sum", sql="amount")

    assert metric.format is None
    assert metric.value_format_name is None
    assert metric.drill_fields is None
    assert metric.non_additive_dimension is None

    dimension = Dimension(name="status", type="categorical")

    assert dimension.format is None
    assert dimension.value_format_name is None

    # Model-level defaults are also optional
    model = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        metrics=[metric],
    )
    assert model.default_time_dimension is None
    assert model.default_grain is None


def test_default_grain_validation():
    """Test default_grain accepts valid time granularities on Model."""
    valid_grains = ["hour", "day", "week", "month", "quarter", "year"]

    for grain in valid_grains:
        model = Model(
            name="orders",
            table="orders_table",
            primary_key="order_id",
            default_grain=grain,
        )
        assert model.default_grain == grain


def test_metadata_survives_query_compilation(layer):
    """Test that metadata fields don't break query compilation."""
    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical"),
        ],
        metrics=[
            Metric(
                name="revenue",
                agg="sum",
                sql="amount",
                format="$#,##0.00",
                drill_fields=["status"],
                default_time_dimension="created_at",
                default_grain="day",
            ),
        ],
    )

    layer.add_model(orders)

    # Should compile successfully even with metadata fields
    sql = layer.compile(metrics=["orders.revenue"], dimensions=["orders.status"])

    assert "SUM" in sql
    assert "status" in sql
