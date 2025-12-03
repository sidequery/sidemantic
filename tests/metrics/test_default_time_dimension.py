"""Test default_time_dimension auto-inclusion from Model."""

from sidemantic import Dimension, Metric, Model


def test_default_time_dimension_auto_included(layer):
    """Test that model's default_time_dimension is auto-included when not specified."""
    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        default_time_dimension="order_date",
        dimensions=[
            Dimension(name="order_date", type="time", granularity="day"),
            Dimension(name="status", type="categorical"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    layer.add_model(orders)

    # Query without specifying time dimension
    sql = layer.compile(metrics=["orders.revenue"])

    print("SQL with auto-included time dimension:")
    print(sql)

    # Should include order_date in the query
    assert "order_date" in sql
    # Should be in GROUP BY
    assert "GROUP BY" in sql


def test_default_time_dimension_with_grain(layer):
    """Test that default_grain is applied to auto-included time dimension."""
    orders = Model(
        name="orders",
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

    layer.add_model(orders)

    sql = layer.compile(metrics=["orders.revenue"])

    print("SQL with default grain:")
    print(sql)

    # Should include order_date__month
    assert "order_date__month" in sql


def test_default_time_dimension_override(layer):
    """Test that user-specified time dimension overrides default."""
    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        default_time_dimension="order_date",
        default_grain="month",
        dimensions=[
            Dimension(name="order_date", type="time", granularity="day"),
            Dimension(name="created_at", type="time", granularity="day"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    layer.add_model(orders)

    # User explicitly specifies a different time dimension
    sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.created_at__week"],
    )

    print("SQL with user override:")
    print(sql)

    # Should use user's choice, not default
    assert "created_at__week" in sql
    # Should NOT auto-add order_date__month since user provided a time dim
    assert "order_date__month" not in sql


def test_default_time_dimension_same_override(layer):
    """Test that user can override default with different granularity."""
    orders = Model(
        name="orders",
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

    layer.add_model(orders)

    # User specifies same dimension but different grain
    sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.order_date__week"],
    )

    print("SQL with same dim different grain:")
    print(sql)

    # Should use user's week granularity
    assert "order_date__week" in sql
    # Should NOT add month since user already specified order_date
    assert "order_date__month" not in sql


def test_no_default_time_dimension(layer):
    """Test models without default_time_dimension work normally."""
    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="order_date", type="time", granularity="day"),
            Dimension(name="status", type="categorical"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    layer.add_model(orders)

    # Query without time dimension - should work, just aggregate all
    sql = layer.compile(metrics=["orders.revenue"])

    print("SQL without default time dimension:")
    print(sql)

    # Should not have GROUP BY since no dimensions
    assert "revenue" in sql


def test_multiple_models_different_defaults(layer):
    """Test multiple models with different default_time_dimensions."""
    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        default_time_dimension="order_date",
        default_grain="month",
        dimensions=[
            Dimension(name="order_date", type="time", granularity="day"),
            Dimension(name="customer_id", type="categorical"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    customers = Model(
        name="customers",
        table="customers_table",
        primary_key="customer_id",
        default_time_dimension="created_at",
        default_grain="week",
        dimensions=[
            Dimension(name="created_at", type="time", granularity="day"),
        ],
        metrics=[
            Metric(name="customer_count", agg="count"),
        ],
    )

    layer.add_model(orders)
    layer.add_model(customers)

    # Query orders metric - should use orders' default
    sql = layer.compile(metrics=["orders.revenue"])
    print("Orders SQL:")
    print(sql)
    assert "order_date__month" in sql

    # Query customers metric - should use customers' default
    sql2 = layer.compile(metrics=["customers.customer_count"])
    print("Customers SQL:")
    print(sql2)
    assert "created_at__week" in sql2
