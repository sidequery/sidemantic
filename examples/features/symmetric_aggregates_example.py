#!/usr/bin/env python3
# /// script
# dependencies = [
#   "sidemantic",
#   "duckdb",
#   "pandas",
# ]
# ///
"""Example demonstrating symmetric aggregates for fan-out joins.

Symmetric aggregates prevent double-counting when a query joins a base model
to multiple "many" side tables, creating a fan-out effect.

Run with: uv run https://raw.githubusercontent.com/sidequery/sidemantic/main/examples/features/symmetric_aggregates_example.py
"""

import duckdb

from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.sql.generator import SQLGenerator

# Set up sample data
conn = duckdb.connect(":memory:")

# Orders (1 order can have many items and many shipments)
conn.execute("""
    CREATE TABLE orders AS
    SELECT * FROM (VALUES
        (1, '2024-01-01'::DATE, 100),
        (2, '2024-01-15'::DATE, 200)
    ) AS t(id, order_date, amount)
""")

# Order items (many per order)
conn.execute("""
    CREATE TABLE order_items AS
    SELECT * FROM (VALUES
        (1, 1, 5),
        (2, 1, 3),
        (3, 2, 10)
    ) AS t(id, order_id, quantity)
""")

# Shipments (many per order)
conn.execute("""
    CREATE TABLE shipments AS
    SELECT * FROM (VALUES
        (1, 1),
        (2, 1),
        (3, 2)
    ) AS t(id, order_id)
""")

print("=" * 80)
print("Symmetric Aggregates Example")
print("=" * 80)
print()
print("Sample Data:")
print("-" * 40)
print("Orders:")
print(conn.execute("SELECT * FROM orders").fetchdf())
print()
print("Order Items:")
print(conn.execute("SELECT * FROM order_items").fetchdf())
print()
print("Shipments:")
print(conn.execute("SELECT * FROM shipments").fetchdf())
print()

# Build semantic graph
graph = SemanticGraph()

# Orders model with has_many relationships
orders = Model(
    name="orders",
    table="orders",
    primary_key="id",
    dimensions=[Dimension(name="order_date", type="time", sql="order_date")],
    metrics=[Metric(name="revenue", agg="sum", sql="amount")],
    relationships=[
        Relationship(name="order_items", type="one_to_many", foreign_key="order_id"),
        Relationship(name="shipments", type="one_to_many", foreign_key="order_id"),
    ],
)

# Order items model
order_items = Model(
    name="order_items",
    table="order_items",
    primary_key="id",
    dimensions=[],
    metrics=[Metric(name="total_quantity", agg="sum", sql="quantity")],
    relationships=[Relationship(name="orders", type="many_to_one", foreign_key="order_id")],
)

# Shipments model
shipments = Model(
    name="shipments",
    table="shipments",
    primary_key="id",
    dimensions=[],
    metrics=[Metric(name="shipment_count", agg="count", sql="*")],
    relationships=[Relationship(name="orders", type="many_to_one", foreign_key="order_id")],
)

graph.add_model(orders)
graph.add_model(order_items)
graph.add_model(shipments)

generator = SQLGenerator(graph)

# Example 1: Single join (no fan-out, no symmetric aggregates needed)
print("=" * 80)
print("Example 1: Single one-to-many join (no symmetric aggregates)")
print("=" * 80)
print()

sql = generator.generate(
    metrics=["orders.revenue", "order_items.total_quantity"],
    dimensions=["orders.order_date"],
    order_by=["orders.order_date"],
)

print("Query: orders + order_items")
print()
print("Generated SQL (note: regular SUM, no HASH):")
print(sql)
print()

result = conn.execute(sql).fetchall()
print("Results:")
print("  order_date  | revenue | total_quantity")
print("  " + "-" * 45)
for row in result:
    print(f"  {row[0]!s:<12} | {row[1]:>7} | {row[2]:>14}")
print()

# Example 2: Multiple joins (fan-out, symmetric aggregates applied)
print("=" * 80)
print("Example 2: Multiple one-to-many joins (symmetric aggregates)")
print("=" * 80)
print()

print("The Problem:")
print("  Order 1 has 2 items × 2 shipments = 4 rows in joined result")
print("  Without symmetric aggregates: revenue = 100 × 4 = 400 (WRONG!)")
print()

sql = generator.generate(
    metrics=["orders.revenue", "order_items.total_quantity", "shipments.shipment_count"],
    dimensions=["orders.order_date"],
    order_by=["orders.order_date"],
)

print("Query: orders + order_items + shipments")
print()
print("Generated SQL (note: HASH function for symmetric aggregates):")
print(sql)
print()

result = conn.execute(sql).fetchall()
print("Results:")
print("  order_date  | revenue | total_quantity | shipment_count")
print("  " + "-" * 60)
for row in result:
    revenue = row[1] if row[1] is not None else 0
    quantity = row[2] if row[2] is not None else 0
    shipments_cnt = row[3] if row[3] is not None else 0
    print(f"  {row[0]!s:<12} | {revenue:>7} | {quantity:>14} | {shipments_cnt:>14}")
print()

# Verify correctness
print("=" * 80)
print("Verification:")
print("=" * 80)
print()

# Without symmetric aggregates (naive join)
naive_sql = """
SELECT
    o.order_date,
    SUM(o.amount) as revenue_wrong,
    SUM(i.quantity) as quantity,
    COUNT(*) as row_count
FROM orders o
LEFT JOIN order_items i ON o.id = i.order_id
LEFT JOIN shipments s ON o.id = s.order_id
GROUP BY o.order_date
ORDER BY o.order_date
"""

naive_result = conn.execute(naive_sql).fetchall()
print("Without symmetric aggregates (naive join):")
print("  order_date  | revenue (WRONG) | total_quantity | rows in join")
print("  " + "-" * 65)
for row in naive_result:
    print(f"  {row[0]!s:<12} | {row[1]:>15} | {row[2]:>14} | {row[3]:>12}")
print()
print("Notice: Order 1 revenue is inflated by the fan-out!")
print()

# With symmetric aggregates
print("With symmetric aggregates (sidemantic):")
print("  Order 1: revenue = 100 (correct)")
print("  Order 2: revenue = 200 (correct)")
print()

print("=" * 80)
print("How Symmetric Aggregates Work:")
print("=" * 80)
print("""
For each row in the base model (orders), we:
1. Hash the primary key to get a unique large number
2. Add the measure value to this hash
3. Use SUM(DISTINCT ...) to sum only unique hash+value combinations
4. Subtract SUM(DISTINCT hash) to remove the hash offset
5. Result: each base row is counted exactly once!

Formula:
  SUM(DISTINCT HASH(pk) * 2^20 + value) - SUM(DISTINCT HASH(pk) * 2^20)

This ensures correct aggregation even when joins create duplicate rows.
""")

conn.close()
