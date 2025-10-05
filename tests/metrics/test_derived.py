"""Test derived metrics with formula parsing.

Derived metrics combine other metrics using formulas like:
revenue_per_order = total_revenue / total_orders
"""

import duckdb
import pytest

from sidemantic import Dimension, Metric, Model, SemanticLayer


@pytest.fixture
def orders_db():
    """Create test database with orders."""
    conn = duckdb.connect(":memory:")

    conn.execute("""
        CREATE TABLE orders (
            order_id INTEGER,
            customer_id INTEGER,
            order_amount DECIMAL(10, 2),
            created_at DATE
        )
    """)

    conn.execute("""
        INSERT INTO orders VALUES
            (1, 101, 100.00, '2024-01-15'),
            (2, 102, 200.00, '2024-01-20'),
            (3, 101, 150.00, '2024-02-01'),
            (4, 103, 300.00, '2024-02-10')
    """)

    return conn


def test_simple_derived_metric(orders_db):
    """Test basic derived metric formula."""
    layer = SemanticLayer()
    layer.conn = orders_db

    orders = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        metrics=[
            Metric(name="revenue", agg="sum", sql="order_amount"),
            Metric(name="order_count", agg="count"),
        ],
    )

    layer.add_model(orders)

    # Create a graph-level derived metric using ratio type
    # (derived metrics with sql expressions need to reference raw table columns)
    revenue_per_order = Metric(
        name="revenue_per_order",
        type="ratio",
        numerator="orders.revenue",
        denominator="orders.order_count",
    )
    layer.add_metric(revenue_per_order)

    result = layer.query(metrics=["revenue_per_order"])
    df = result.fetchdf()

    # Total revenue: 100+200+150+300 = 750
    # Total orders: 4
    # revenue_per_order = 750/4 = 187.5
    assert len(df) == 1
    assert df["revenue_per_order"][0] == 187.5


def test_derived_metric_by_dimension(orders_db):
    """Test derived metric grouped by dimension."""
    layer = SemanticLayer()
    layer.conn = orders_db

    orders = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        dimensions=[Dimension(name="created_at", type="time", granularity="month")],
        metrics=[
            Metric(name="revenue", agg="sum", sql="order_amount"),
            Metric(name="order_count", agg="count"),
        ],
    )

    layer.add_model(orders)

    revenue_per_order = Metric(
        name="revenue_per_order",
        type="ratio",
        numerator="orders.revenue",
        denominator="orders.order_count",
    )
    layer.add_metric(revenue_per_order)

    result = layer.query(
        metrics=["revenue_per_order"], dimensions=["orders.created_at__month"]
    )
    df = result.fetchdf()

    # January: (100+200)/2 = 150
    # February: (150+300)/2 = 225
    assert len(df) == 2
    monthly_avg = {str(row["created_at__month"])[:7]: row["revenue_per_order"] for _, row in df.iterrows()}
    assert monthly_avg["2024-01"] == 150.0
    assert monthly_avg["2024-02"] == 225.0


def test_nested_derived_metrics(orders_db):
    """Test derived metrics referencing other derived metrics."""
    layer = SemanticLayer()
    layer.conn = orders_db

    orders = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        metrics=[
            Metric(name="revenue", agg="sum", sql="order_amount"),
            Metric(name="order_count", agg="count"),
        ],
    )

    layer.add_model(orders)

    # First level ratio metric
    revenue_per_order = Metric(
        name="revenue_per_order",
        type="ratio",
        numerator="orders.revenue",
        denominator="orders.order_count",
    )
    layer.add_metric(revenue_per_order)

    # Second level: derived from another derived metric
    double_avg = Metric(
        name="double_avg",
        type="derived",
        sql="revenue_per_order * 2",
    )
    layer.add_metric(double_avg)

    result = layer.query(metrics=["double_avg"])
    df = result.fetchdf()

    # revenue_per_order = 750/4 = 187.5
    # double_avg = 187.5 * 2 = 375
    assert len(df) == 1
    assert df["double_avg"][0] == 375.0


def test_all_metrics_together(orders_db):
    """Test querying base + derived metrics together."""
    layer = SemanticLayer()
    layer.conn = orders_db

    orders = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        metrics=[
            Metric(name="revenue", agg="sum", sql="order_amount"),
            Metric(name="order_count", agg="count"),
        ],
    )

    layer.add_model(orders)

    revenue_per_order = Metric(
        name="revenue_per_order",
        type="ratio",
        numerator="orders.revenue",
        denominator="orders.order_count",
    )
    layer.add_metric(revenue_per_order)

    result = layer.query(
        metrics=["orders.revenue", "orders.order_count", "revenue_per_order"]
    )
    df = result.fetchdf()

    # All metrics in same result set
    assert len(df) == 1
    assert "revenue" in df.columns
    assert "order_count" in df.columns
    assert "revenue_per_order" in df.columns
    assert df["revenue"][0] == 750.0
    assert df["order_count"][0] == 4
    assert df["revenue_per_order"][0] == 187.5


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
