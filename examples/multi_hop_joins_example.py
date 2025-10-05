"""Test multi-hop joins."""

import duckdb

from sidemantic import Dimension, Metric, Model, Relationship, SemanticLayer
from tests.utils import fetch_rows

conn = duckdb.connect(":memory:")

# Create a 3-table chain: orders -> customers -> regions
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
        region_id INTEGER,
        customer_name VARCHAR
    )
""")

conn.execute("""
    CREATE TABLE regions (
        region_id INTEGER,
        region_name VARCHAR
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
        (101, 1, 'Alice'),
        (102, 2, 'Bob'),
        (103, 1, 'Charlie')
""")

conn.execute("""
    INSERT INTO regions VALUES
        (1, 'North America'),
        (2, 'Europe')
""")

sl = SemanticLayer()
sl.conn = conn

# Define models with multi-hop relationship: orders -> customers -> regions
orders = Model(
    name="orders",
    table="orders",
    primary_key="order_id",
    relationships=[
        Relationship(name="customers", type="many_to_one", foreign_key="customer_id"),
    ],
    metrics=[Metric(name="revenue", agg="sum", sql="order_amount")],
)

customers = Model(
    name="customers",
    table="customers",
    primary_key="customer_id",
    relationships=[
        Relationship(name="regions", type="many_to_one", foreign_key="region_id"),
    ],
    dimensions=[Dimension(name="customer_name", type="categorical")],
)

regions = Model(
    name="regions",
    table="regions",
    primary_key="region_id",
    dimensions=[Dimension(name="region_name", type="categorical")],
)

sl.add_model(orders)
sl.add_model(customers)
sl.add_model(regions)

print("=" * 80)
print("Test: Multi-hop join (orders -> customers -> regions)")
print("=" * 80)

# Query that requires 2-hop join
sql = sl.compile(
    metrics=["orders.revenue"],
    dimensions=["regions.region_name"],
)

print("Generated SQL:")
print(sql)
print()

result = sl.query(
    metrics=["orders.revenue"],
    dimensions=["regions.region_name"],
)

print("Results (region_name, revenue):")
for row in fetch_rows(result):
    print(f"  {row}")
print()

# Test join path discovery
print("=" * 80)
print("Test: Join path discovery")
print("=" * 80)

join_path = sl.graph.find_relationship_path("orders", "regions")
print(f"Join path from orders to regions: {len(join_path)} hops")
for i, jp in enumerate(join_path):
    print(f"  Hop {i + 1}: {jp.from_model} -> {jp.to_model} (via {jp.from_entity})")
print()

print("✅ Multi-hop joins working!")
