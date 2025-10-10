#!/usr/bin/env python3
"""Query examples using MotherDuck with pre-aggregations.

Prerequisites:
- Set MOTHERDUCK_TOKEN environment variable
- Run setup_data.py first
- Run: sidemantic preagg refresh sidemantic.yaml

Usage:
    uv run python query_examples.py
"""

from sidemantic import SemanticLayer

print("=" * 80)
print("MotherDuck Query Examples")
print("=" * 80)

# Load semantic layer from YAML config
# This loads connection, models, and pre-agg settings all from one file
print("\nLoading semantic layer from sidemantic.yaml...")
layer = SemanticLayer.from_yaml("sidemantic.yaml", connection="duckdb://md:sidemantic_demo")
print("✓ Loaded semantic layer")
print("  Database: sidemantic_demo (MotherDuck)")
print(f"  Models: {len(layer.list_models())}")
print(f"  Pre-agg schema: {layer.preagg_schema}")

# Query 1: Revenue by status (uses daily_status pre-agg)
print("\n" + "=" * 80)
print("Query 1: Revenue by order status (uses pre-aggregation)")
print("=" * 80)

sql = """
SELECT orders.revenue, orders.order_count, orders.status
FROM orders
"""
print(f"\nSQL:\n{sql}")

result = layer.sql(sql)
print("\nResults:")
print(result.df())

# Query 2: Revenue by month and status (uses monthly_summary pre-agg)
print("\n" + "=" * 80)
print("Query 2: Revenue by month and status (uses pre-aggregation)")
print("=" * 80)

sql = """
SELECT orders.revenue, orders.order_count, orders.status, orders.order_date__month
FROM orders
ORDER BY orders.order_date__month
"""
print(f"\nSQL:\n{sql}")

result = layer.sql(sql)
print("\nResults (first 12 rows):")
df = result.df()
print(df.head(12))

# Query 3: Daily trends for completed orders (uses daily_status pre-agg)
print("\n" + "=" * 80)
print("Query 3: Daily completed order trends (last 10 days)")
print("=" * 80)

sql = """
SELECT orders.revenue, orders.order_count, orders.order_date__day
FROM orders
WHERE orders.status = 'completed'
ORDER BY orders.order_date__day DESC
LIMIT 10
"""
print(f"\nSQL:\n{sql}")

result = layer.sql(sql)
print("\nResults:")
print(result.df())

# Query 4: Revenue by customer region (with join)
print("\n" + "=" * 80)
print("Query 4: Revenue by customer region (demonstrates join)")
print("=" * 80)

sql = """
SELECT orders.revenue, orders.order_count, customers.region
FROM orders
"""
print(f"\nSQL:\n{sql}")

result = layer.sql(sql)
print("\nResults:")
print(result.df())

# Query 5: Premium tier customers
print("\n" + "=" * 80)
print("Query 5: Premium tier customers by region")
print("=" * 80)

sql = """
SELECT orders.revenue, orders.order_count, customers.region
FROM orders
WHERE customers.tier = 'premium'
ORDER BY customers.region
"""
print(f"\nSQL:\n{sql}")

result = layer.sql(sql)
print("\nResults:")
print(result.df())

print("\n" + "=" * 80)
print("Query Examples Complete!")
print("=" * 80)
print("\n✓ All queries used pre-aggregations when available")
print("✓ Queries are much faster than scanning 10k raw rows")
print("\nBenefits:")
print("  - Automatic pre-aggregation routing")
print("  - Fast queries on large datasets")
print("  - Refresh periodically to keep data fresh")
