"""Test cross-model metrics."""

import duckdb
import pytest

from sidemantic import Dimension, Entity, Measure, Model, SemanticLayer, Model, SemanticLayer

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
    entities=[
        Entity(name="order", type="primary", expr="order_id"),
        Entity(name="customer", type="foreign", expr="customer_id"),
    ],
    measures=[
        Measure(name="revenue", agg="sum", expr="order_amount"),
        Measure(name="order_count", agg="count"),
    ],
)

customers = Model(
    name="customers",
    table="customers",
    entities=[Entity(name="customer", type="primary", expr="customer_id")],
    dimensions=[Dimension(name="region", type="categorical")],
    measures=[
        Measure(name="customer_count", agg="count_distinct", expr="customer_id"),
    ],
)

sl.add_model(orders)
sl.add_model(customers)

# Test 1: Simple cross-model metric (ratio using measures from different models)
print("=" * 80)
print("Test 1: Cross-model ratio metric (revenue per customer)")
print("=" * 80)

sl.add_metric(
    Measure(
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
for row in result.fetchall():
    print(f"  {row}")
print()

# Test 2: Derived metric mixing models
print("=" * 80)
print("Test 2: Derived metric with cross-model references")
print("=" * 80)

sl.add_metric(Measure(name="total_revenue", type="simple", expr="orders.revenue"))
sl.add_metric(Measure(name="total_customers", type="simple", expr="customers.customer_count"))

sl.add_metric(
    Measure(
        name="revenue_per_customer_derived",
        type="derived",
        expr="total_revenue / total_customers",
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
for row in result.fetchall():
    print(f"  {row}")
print()

print("✅ Cross-model metrics working!")
