"""MotherDuck example - cloud DuckDB with Sidemantic.

This example demonstrates how to use Sidemantic with MotherDuck,
a cloud-based DuckDB service, including pre-aggregations for fast queries.

Prerequisites:
- Set MOTHERDUCK_TOKEN environment variable (sign up at motherduck.com)
- Example creates a new database called 'sidemantic_demo' if it doesn't exist

Features demonstrated:
- Connect to MotherDuck cloud database
- Create sample data in the cloud
- Define semantic models with relationships
- Create and use pre-aggregations for faster queries
- Query data with automatic pre-aggregation routing

Usage:
    export MOTHERDUCK_TOKEN=your_token_here
    uv run python examples/motherduck_example.py
"""

import duckdb

from sidemantic import Dimension, Metric, Model, PreAggregation, Relationship, SemanticLayer

# First, connect to MotherDuck default to create database if needed
print("=" * 80)
print("MotherDuck + Sidemantic Example")
print("=" * 80)
print("\nCreating database in MotherDuck...")
conn = duckdb.connect("md:")
conn.execute("CREATE DATABASE IF NOT EXISTS sidemantic_demo")
conn.close()

# Connect to MotherDuck using duckdb://md:database_name format
# Token is read from MOTHERDUCK_TOKEN environment variable
# Enable pre-aggregations for automatic query optimization
sl = SemanticLayer(
    connection="duckdb://md:sidemantic_demo",
    use_preaggregations=True,
    preagg_schema="preagg",
)

print("✓ Connected to MotherDuck!")
print(f"  Dialect: {sl.dialect}")
print("  Pre-aggregations: enabled")
print("  Pre-agg schema: preagg")

# Create sample data in MotherDuck
print("\nCreating sample tables...")
sl.adapter.execute("""
    CREATE SCHEMA IF NOT EXISTS sidemantic_demo.analytics
""")

sl.adapter.execute("""
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

sl.adapter.execute("""
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

result = sl.adapter.execute("SELECT COUNT(*) FROM sidemantic_demo.analytics.orders")
order_count = result.fetchone()[0]
print(f"✓ Created {order_count:,} orders spanning 90 days")

# Define semantic models with pre-aggregations
orders = Model(
    name="orders",
    table="sidemantic_demo.analytics.orders",
    primary_key="order_id",
    relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
    dimensions=[
        Dimension(name="status", type="categorical", sql="status"),
        Dimension(name="order_date", type="time", granularity="day", sql="order_date"),
    ],
    metrics=[
        Metric(name="order_count", agg="count"),
        Metric(name="revenue", agg="sum", sql="amount"),
        Metric(name="avg_order_value", agg="avg", sql="amount"),
        Metric(
            name="completed_revenue",
            agg="sum",
            sql="amount",
            filters=["status = 'completed'"],
        ),
    ],
    pre_aggregations=[
        PreAggregation(
            name="daily_status",
            measures=["order_count", "revenue"],
            dimensions=["status"],
            time_dimension="order_date",
            granularity="day",
        ),
        PreAggregation(
            name="monthly_summary",
            measures=["order_count", "revenue", "avg_order_value"],
            dimensions=["status"],
            time_dimension="order_date",
            granularity="month",
        ),
    ],
)

customers = Model(
    name="customers",
    table="sidemantic_demo.analytics.customers",
    primary_key="customer_id",
    dimensions=[
        Dimension(name="region", type="categorical", sql="region"),
        Dimension(name="tier", type="categorical", sql="tier"),
    ],
)

sl.add_model(orders)
sl.add_model(customers)

# Create pre-aggregation schema
print("\n" + "=" * 80)
print("Setting up Pre-Aggregations")
print("=" * 80)
sl.adapter.execute("CREATE SCHEMA IF NOT EXISTS sidemantic_demo.preagg")
print("✓ Created preagg schema")

# Refresh pre-aggregations
print("\nRefreshing pre-aggregations...")
for preagg in orders.pre_aggregations:
    print(f"  Refreshing {preagg.name}...")

    # Generate the SQL for this pre-aggregation
    source_sql = preagg.generate_materialization_sql(orders)
    table_name = preagg.get_table_name(model_name="orders", database="sidemantic_demo", schema="preagg")

    # Refresh the pre-aggregation
    result = preagg.refresh(
        connection=sl.adapter.raw_connection,
        source_sql=source_sql,
        table_name=table_name,
        mode="full",
    )
    print(f"    ✓ Materialized {result.rows_inserted:,} rows in {result.duration_seconds:.2f}s")

print("\n✓ All pre-aggregations refreshed!")

# Query 1: Revenue by status (uses daily_status pre-agg)
print("\n" + "=" * 80)
print("Query 1: Revenue by order status (uses pre-aggregation)")
print("=" * 80)
sql = sl.compile(
    metrics=["orders.revenue", "orders.order_count"],
    dimensions=["orders.status"],
)
print("Generated SQL:")
print(sql[:200] + "..." if len(sql) > 200 else sql)
print()
result = sl.query(
    metrics=["orders.revenue", "orders.order_count"],
    dimensions=["orders.status"],
)
print(result.df())

# Query 2: Revenue by month and status (uses monthly_summary pre-agg)
print("\n" + "=" * 80)
print("Query 2: Revenue by month and status (uses pre-aggregation)")
print("=" * 80)
result = sl.query(
    metrics=["orders.revenue", "orders.order_count"],
    dimensions=["orders.status", "orders.order_date__month"],
    order_by=["orders.order_date__month"],
)
df = result.df()
print(df.head(12))

# Query 3: Daily trends for completed orders (uses daily_status pre-agg)
print("\n" + "=" * 80)
print("Query 3: Daily completed order trends (last 10 days)")
print("=" * 80)
result = sl.query(
    metrics=["orders.revenue", "orders.order_count"],
    dimensions=["orders.order_date__day"],
    filters=["orders.status = 'completed'"],
    order_by=["orders.order_date__day DESC"],
    limit=10,
)
print(result.df())

# Query 4: Revenue by customer region (with join)
print("\n" + "=" * 80)
print("Query 4: Revenue by customer region (demonstrates join)")
print("=" * 80)
result = sl.query(
    metrics=["orders.revenue", "orders.order_count"],
    dimensions=["customers.region"],
)
print(result.df())

# Query 5: Premium tier customers
print("\n" + "=" * 80)
print("Query 5: Premium tier customers by region")
print("=" * 80)
result = sl.query(
    metrics=["orders.revenue", "orders.order_count"],
    dimensions=["customers.region"],
    filters=["customers.tier = 'premium'"],
    order_by=["customers.region"],
)
print(result.df())

# Show pre-aggregation tables
print("\n" + "=" * 80)
print("Pre-Aggregation Tables Created")
print("=" * 80)
tables_result = sl.adapter.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'preagg'
      AND table_catalog = 'sidemantic_demo'
    ORDER BY table_name
""")
for row in tables_result.fetchall():
    table_name = row[0]
    count_result = sl.adapter.execute(f"SELECT COUNT(*) FROM sidemantic_demo.preagg.{table_name}")
    count = count_result.fetchone()[0]
    print(f"  {table_name}: {count:,} rows")

print("\n" + "=" * 80)
print("MotherDuck + Pre-Aggregations Example Complete!")
print("=" * 80)
print("\n✓ Data persists in MotherDuck cloud:")
print("  - 10,000 raw orders")
print("  - Pre-aggregated tables for fast queries")
print("  - Access from anywhere with your token")
print("  - Share with your team")
print("\n✓ Resources created:")
print("  Database: sidemantic_demo")
print("  Schema: analytics (raw tables)")
print("  Schema: preagg (pre-aggregation tables)")
print("  Tables: orders, customers")
print(f"  Pre-aggregations: {len(orders.pre_aggregations)}")
print("\n✓ Benefits:")
print("  - Queries use pre-aggregated tables automatically")
print("  - Much faster than scanning 10k rows")
print("  - Refresh periodically to keep data fresh")
