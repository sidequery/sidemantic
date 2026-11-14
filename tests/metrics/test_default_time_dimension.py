"""Test default_time_dimension auto-inclusion from Model."""

import pytest

from sidemantic import Dimension, Metric, Model, Relationship


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


@pytest.fixture
def dated_orders(layer):
    """Orders and customers with data; orders declares a default time dimension."""
    layer.conn.execute("create table dated_customers (id integer, region varchar)")
    layer.conn.execute("insert into dated_customers values (1, 'EU'), (2, 'US')")
    layer.conn.execute(
        "create table dated_orders (id integer, customer_id integer, status varchar, amount integer, order_date date)"
    )
    layer.conn.execute("""
        insert into dated_orders values
            (1, 1, 'completed', 100, '2026-01-05'),
            (2, 1, 'completed', 50, '2026-01-20'),
            (3, 2, 'completed', 200, '2026-02-03'),
            (4, 2, 'cancelled', 80, '2026-02-14'),
            (5, 2, 'pending', 40, '2026-03-01')
    """)
    layer.add_model(
        Model(
            name="customers",
            table="dated_customers",
            primary_key="id",
            dimensions=[Dimension(name="region", type="categorical")],
        )
    )
    layer.add_model(
        Model(
            name="orders",
            table="dated_orders",
            primary_key="id",
            default_time_dimension="order_date",
            relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
            dimensions=[
                Dimension(name="order_date", type="time", granularity="day"),
                Dimension(name="status", type="categorical"),
            ],
            metrics=[
                Metric(name="revenue", agg="sum", sql="amount"),
                Metric(name="order_count", agg="count"),
                Metric(name="running_revenue", type="cumulative", sql="revenue"),
            ],
        )
    )
    return layer


def _columns_and_rows(result):
    return [column[0] for column in result.description], sorted(result.fetchall(), key=repr)


def _expected_rows(layer, sql):
    return sorted(layer.conn.execute(sql).fetchall(), key=repr)


def test_default_time_dimension_does_not_split_explicit_dimension_groups(dated_orders):
    """Selected dimensions are the whole grain: one row per status, not per status per day."""
    columns, rows = _columns_and_rows(
        dated_orders.query(metrics=["orders.order_count", "orders.revenue"], dimensions=["orders.status"])
    )

    assert columns == ["status", "order_count", "revenue"]
    assert rows == _expected_rows(dated_orders, "select status, count(*), sum(amount) from dated_orders group by 1")
    assert rows == [("cancelled", 1, 80), ("completed", 3, 350), ("pending", 1, 40)]


def test_default_time_dimension_does_not_split_joined_dimension_groups(dated_orders):
    columns, rows = _columns_and_rows(dated_orders.query(metrics=["orders.revenue"], dimensions=["customers.region"]))

    assert columns == ["region", "revenue"]
    assert rows == [("EU", 150), ("US", 320)]


@pytest.mark.parametrize(
    ("query", "expected_columns", "expected_sql"),
    [
        (
            "select orders.status, orders.order_count, orders.revenue from orders",
            ["status", "order_count", "revenue"],
            "select status, count(*), sum(amount) from dated_orders group by 1",
        ),
        ("select orders.revenue from orders", ["revenue"], "select sum(amount) from dated_orders"),
        (
            "select orders.revenue, orders.order_count from orders where orders.status = 'completed'",
            ["revenue", "order_count"],
            "select sum(amount), count(*) from dated_orders where status = 'completed'",
        ),
    ],
    ids=["grouped", "scalar", "filtered"],
)
def test_default_time_dimension_is_not_a_hidden_sql_group_key(dated_orders, query, expected_columns, expected_sql):
    """A SQL SELECT owns its projection, so it is never grouped by a column it does not return."""
    columns, rows = _columns_and_rows(dated_orders.sql(query))

    assert columns == expected_columns
    assert rows == _expected_rows(dated_orders, expected_sql)


def test_default_time_dimension_still_orders_cumulative_metric_with_explicit_dimension(dated_orders):
    """A running total cannot be computed without a time axis, so the default still supplies it."""
    columns, rows = _columns_and_rows(
        dated_orders.query(metrics=["orders.running_revenue"], dimensions=["orders.status"])
    )

    assert columns[:2] == ["status", "order_date"]
    running = {(row[0], str(row[1])): row[columns.index("running_revenue")] for row in rows}
    assert running == {
        ("cancelled", "2026-02-14"): 80,
        ("completed", "2026-01-05"): 100,
        ("completed", "2026-01-20"): 150,
        ("completed", "2026-02-03"): 350,
        ("pending", "2026-03-01"): 40,
    }


def test_default_time_dimension_still_groups_metric_only_query(dated_orders):
    columns, rows = _columns_and_rows(dated_orders.query(metrics=["orders.revenue"]))

    assert columns == ["order_date", "revenue"]
    assert rows == _expected_rows(dated_orders, "select order_date, sum(amount) from dated_orders group by 1")
