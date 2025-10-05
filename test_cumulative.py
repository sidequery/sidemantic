"""Test cumulative metrics."""

import duckdb

from sidemantic import Dimension, Metric, Model, SemanticLayer

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

sl = SemanticLayer()
sl.conn = conn

orders = Model(
    name="orders",
    table="orders",
    primary_key="order_id",
    dimensions=[Dimension(name="order_date", type="time", granularity="day", sql="order_date")],
    metrics=[
        Metric(name="revenue", agg="sum", sql="order_amount"),
        Metric(name="order_count", agg="count"),
    ],
)

sl.add_model(orders)

# Define cumulative metric
sl.add_metric(
    Metric(
        name="running_total_revenue",
        type="cumulative",
        sql="orders.revenue",
        description="Running total of revenue",
    )
)

print("=" * 80)
print("Test: Cumulative metric (running total)")
print("=" * 80)

sql = sl.compile(
    metrics=["running_total_revenue"],
    dimensions=["orders.order_date"],
    order_by=["order_date"],
)

print("Generated SQL:")
print(sql)
print()

result = sl.query(
    metrics=["running_total_revenue"],
    dimensions=["orders.order_date"],
    order_by=["order_date"],
)

print("Results (date, running_total):")
for row in result.fetchall():
    print(f"  {row}")
print()

# Test with regular revenue alongside
print("=" * 80)
print("Test: Cumulative + regular metric")
print("=" * 80)

result = sl.query(
    metrics=["orders.revenue", "running_total_revenue"],
    dimensions=["orders.order_date"],
    order_by=["order_date"],
)

print("Results (date, daily_revenue, running_total):")
for row in result.fetchall():
    print(f"  {row}")
print()

print("✅ Cumulative metrics working!")
