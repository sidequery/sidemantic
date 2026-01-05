"""Test cross-model metrics."""

import duckdb

from sidemantic import Dimension, Metric, Model, Relationship, SemanticLayer
from tests.utils import fetch_rows

# Create test data
conn = duckdb.connect(":memory:")

conn.execute("""
    CREATE TABLE orders (
        order_id INTEGER,
        customer_id INTEGER,
        order_amount DECIMAL(10, 2)
    )
""")

conn.execute("""
    CREATE TABLE customers (
        customer_id INTEGER,
        customer_name VARCHAR,
        region VARCHAR
    )
""")

conn.execute("""
    INSERT INTO orders VALUES
        (1, 101, 150.00),
        (2, 102, 200.00),
        (3, 101, 100.00),
        (4, 103, 300.00)
""")

conn.execute("""
    INSERT INTO customers VALUES
        (101, 'Alice', 'US'),
        (102, 'Bob', 'EU'),
        (103, 'Charlie', 'US')
""")

sl = SemanticLayer()
sl.conn = conn

orders = Model(
    name="orders",
    table="orders",
    primary_key="order_id",
    relationships=[
        Relationship(name="customers", type="many_to_one", foreign_key="customer_id"),
    ],
    metrics=[
        Metric(name="revenue", agg="sum", sql="order_amount"),
        Metric(name="order_count", agg="count"),
    ],
)

customers = Model(
    name="customers",
    table="customers",
    primary_key="customer_id",
    dimensions=[Dimension(name="region", type="categorical")],
    metrics=[
        Metric(name="customer_count", agg="count_distinct", sql="customer_id"),
    ],
)

sl.add_model(orders)
sl.add_model(customers)

# Test 1: Simple cross-model metric (ratio using measures from different models)
print("=" * 80)
print("Test 1: Cross-model ratio metric (revenue per customer)")
print("=" * 80)

sl.add_metric(
    Metric(
        name="revenue_per_customer",
        type="ratio",
        numerator="orders.revenue",
        denominator="customers.customer_count",
        description="Revenue per customer",
    )
)

sql = sl.compile(
    metrics=["revenue_per_customer"],
    dimensions=["customers.region"],
)

print("Generated SQL:")
print(sql)
print()

result = sl.query(
    metrics=["revenue_per_customer"],
    dimensions=["customers.region"],
)

print("Results:")
for row in fetch_rows(result):
    print(f"  {row}")
print()

# Test 2: Derived metric mixing models
print("=" * 80)
print("Test 2: Derived metric with cross-model references")
print("=" * 80)

sl.add_metric(Metric(name="total_revenue", sql="orders.revenue"))
sl.add_metric(Metric(name="total_customers", sql="customers.customer_count"))

sl.add_metric(
    Metric(
        name="revenue_per_customer_derived",
        type="derived",
        sql="total_revenue / total_customers",
        metrics=["total_revenue", "total_customers"],
    )
)

sql = sl.compile(
    metrics=["total_revenue", "total_customers", "revenue_per_customer_derived"],
    dimensions=["customers.region"],
)

print("Generated SQL:")
print(sql)
print()

result = sl.query(
    metrics=["total_revenue", "total_customers", "revenue_per_customer_derived"],
    dimensions=["customers.region"],
)

print("Results (region, revenue, customers, revenue_per_customer):")
for row in fetch_rows(result):
    print(f"  {row}")
print()

print("✅ Cross-model metrics working!")
