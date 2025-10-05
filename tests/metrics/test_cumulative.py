"""Test cumulative metrics with window functions.

Cumulative metrics use window functions for running totals and rolling windows.
Examples: running_total_revenue, 7_day_rolling_average
"""

import duckdb
import pytest

from sidemantic import Dimension, Metric, Model


@pytest.fixture
def timeseries_db():
    """Create test database with time-series order data."""
    conn = duckdb.connect(":memory:")

    conn.execute("""
        CREATE TABLE orders (
            order_id INTEGER,
            order_date DATE,
            order_amount DECIMAL(10, 2)
        )
    """)

    conn.execute("""
        INSERT INTO orders VALUES
            (1, '2024-01-01', 100.00),
            (2, '2024-01-02', 150.00),
            (3, '2024-01-03', 200.00),
            (4, '2024-01-04', 120.00),
            (5, '2024-01-05', 180.00)
    """)

    return conn


def test_running_total(timeseries_db, layer):
    """Test running total cumulative metric."""
    layer.conn = timeseries_db

    # Define model
    orders = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        dimensions=[Dimension(name="order_date", type="time", granularity="day", sql="order_date")],
        metrics=[Metric(name="daily_revenue", agg="sum", sql="order_amount")],
    )
    layer.add_model(orders)

    # Define cumulative metric
    running_total = Metric(
        name="running_total_revenue",
        type="cumulative",
        sql="orders.daily_revenue",
    )
    layer.add_metric(running_total)

    # Query with cumulative metric
    result = layer.query(
        metrics=["orders.daily_revenue", "running_total_revenue"],
        dimensions=["orders.order_date"],
        order_by=["orders.order_date"],
    )

    df = result.df()
    print("\nRunning Total Results:")
    print(df)

    # Verify running total
    assert len(df) == 5
    assert df["running_total_revenue"].iloc[0] == 100.00  # Day 1: 100
    assert df["running_total_revenue"].iloc[1] == 250.00  # Day 2: 100 + 150
    assert df["running_total_revenue"].iloc[2] == 450.00  # Day 3: 100 + 150 + 200
    assert df["running_total_revenue"].iloc[3] == 570.00  # Day 4: 100 + 150 + 200 + 120
    assert df["running_total_revenue"].iloc[4] == 750.00  # Day 5: 100 + 150 + 200 + 120 + 180


def test_rolling_window(timeseries_db, layer):
    """Test rolling window cumulative metric."""
    layer.conn = timeseries_db

    # Define model
    orders = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        dimensions=[Dimension(name="order_date", type="time", granularity="day", sql="order_date")],
        metrics=[Metric(name="daily_revenue", agg="sum", sql="order_amount")],
    )
    layer.add_model(orders)

    # Define 3-day rolling window metric
    rolling_metric = Metric(
        name="rolling_3day_revenue",
        type="cumulative",
        sql="orders.daily_revenue",
        window="2 days",  # Current + 2 preceding = 3 days total
    )
    layer.add_metric(rolling_metric)

    # Query
    result = layer.query(
        metrics=["orders.daily_revenue", "rolling_3day_revenue"],
        dimensions=["orders.order_date"],
        order_by=["orders.order_date"],
    )

    df = result.df()
    print("\nRolling Window Results:")
    print(df)

    # Verify rolling window
    assert len(df) == 5
    # Day 1: just 100
    assert df["rolling_3day_revenue"].iloc[0] == 100.00
    # Day 2: 100 + 150 = 250
    assert df["rolling_3day_revenue"].iloc[1] == 250.00
    # Day 3: 100 + 150 + 200 = 450
    assert df["rolling_3day_revenue"].iloc[2] == 450.00
    # Day 4: 150 + 200 + 120 = 470 (drops day 1)
    assert df["rolling_3day_revenue"].iloc[3] == 470.00
    # Day 5: 200 + 120 + 180 = 500 (drops days 1-2)
    assert df["rolling_3day_revenue"].iloc[4] == 500.00


def test_cumulative_with_regular_metric(timeseries_db, layer):
    """Test cumulative metric alongside regular metric."""
    layer.conn = timeseries_db

    # Define model
    orders = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        dimensions=[Dimension(name="order_date", type="time", granularity="day", sql="order_date")],
        metrics=[Metric(name="daily_revenue", agg="sum", sql="order_amount")],
    )
    layer.add_model(orders)

    # Define both regular and cumulative metrics
    total_revenue = Metric(name="total_revenue", sql="orders.daily_revenue")
    running_total = Metric(name="running_total", type="cumulative", sql="orders.daily_revenue")

    layer.add_metric(total_revenue)
    layer.add_metric(running_total)

    # Query with both metric types
    result = layer.query(
        metrics=["total_revenue", "running_total"],
        dimensions=["orders.order_date"],
        order_by=["orders.order_date"],
    )

    df = result.df()
    print("\nCumulative with Regular Metric:")
    print(df)

    # Verify both metrics work together
    assert len(df) == 5
    # Note: daily_revenue appears because it's the base measure for both metrics
    assert "order_date" in df.columns
    assert "total_revenue" in df.columns
    assert "running_total" in df.columns
    # Day 1
    assert df["total_revenue"].iloc[0] == 100.00
    assert df["running_total"].iloc[0] == 100.00
    # Day 3
    assert df["total_revenue"].iloc[2] == 200.00
    assert df["running_total"].iloc[2] == 450.00


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
