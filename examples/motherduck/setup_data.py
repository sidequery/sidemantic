#!/usr/bin/env python3
"""Setup sample data in MotherDuck.

Prerequisites:
- Set MOTHERDUCK_TOKEN environment variable
- Run: export MOTHERDUCK_TOKEN=your_token_here

Usage:
    python setup_data.py
"""

import duckdb

print("=" * 80)
print("MotherDuck Data Setup")
print("=" * 80)

# Create database
print("\nCreating database...")
conn = duckdb.connect("md:")
conn.execute("CREATE DATABASE IF NOT EXISTS sidemantic_demo")
conn.close()

# Connect to database
print("✓ Database created")
print("\nConnecting to sidemantic_demo...")
conn = duckdb.connect("md:sidemantic_demo")

# Create schema
print("\nCreating schema...")
conn.execute("CREATE SCHEMA IF NOT EXISTS sidemantic_demo.analytics")
print("✓ Schema created")

# Create orders table with 10k rows
print("\nCreating orders table (10,000 rows)...")
conn.execute("""
    CREATE OR REPLACE TABLE sidemantic_demo.analytics.orders AS
    SELECT
        'ORD-' || LPAD(CAST(seq AS VARCHAR), 6, '0') as order_id,
        'CUST-' || LPAD(CAST((seq % 20) AS VARCHAR), 4, '0') as customer_id,
        status,
        ROUND((random() * 500 + 10)::DECIMAL, 2) as amount,
        (DATE '2024-01-01' + INTERVAL (seq % 90) DAY)::DATE as order_date
    FROM (
        SELECT
            generate_series as seq,
            ['pending', 'completed', 'cancelled'][1 + (CAST(random() * 2.9 AS INTEGER))] as status
        FROM generate_series(1, 10000)
    )
""")
print("✓ Orders table created")

# Create customers table
print("\nCreating customers table (20 customers)...")
conn.execute("""
    CREATE OR REPLACE TABLE sidemantic_demo.analytics.customers AS
    SELECT
        'CUST-' || LPAD(CAST(seq AS VARCHAR), 4, '0') as customer_id,
        region,
        tier
    FROM (
        SELECT
            generate_series as seq,
            ['North', 'South', 'East', 'West'][1 + (CAST(random() * 3.9 AS INTEGER))] as region,
            ['basic', 'premium'][1 + (CAST(random() * 1.9 AS INTEGER))] as tier
        FROM generate_series(1, 20)
    )
""")
print("✓ Customers table created")

# Verify data
result = conn.execute("SELECT COUNT(*) FROM sidemantic_demo.analytics.orders").fetchone()
order_count = result[0]
result = conn.execute("SELECT COUNT(*) FROM sidemantic_demo.analytics.customers").fetchone()
customer_count = result[0]

print("\n" + "=" * 80)
print("Data Setup Complete!")
print("=" * 80)
print(f"\n✓ Created {order_count:,} orders spanning 90 days")
print(f"✓ Created {customer_count} customers")
print("\nDatabase: sidemantic_demo")
print("Schema: analytics")
print("Tables: orders, customers")
print("\nNext steps:")
print("  1. Run: python refresh_preaggs.py")
print("  2. Run: python query_examples.py")

conn.close()
