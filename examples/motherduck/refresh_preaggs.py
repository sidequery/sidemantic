#!/usr/bin/env python3
"""Refresh pre-aggregations for MotherDuck example.

This is a workaround script since `sidemantic preagg refresh` expects separate model files.
Use this script to refresh pre-aggregations defined in sidemantic.yaml.

Prerequisites:
- Set MOTHERDUCK_TOKEN environment variable
- Run setup_data.py first

Usage:
    uv run python refresh_preaggs.py
"""

from sidemantic import SemanticLayer

print("=" * 80)
print("MotherDuck Pre-Aggregation Refresh")
print("=" * 80)

# Load semantic layer from YAML config
print("\nLoading semantic layer from sidemantic.yaml...")
layer = SemanticLayer.from_yaml("sidemantic.yaml", connection="duckdb://md:sidemantic_demo")
print("✓ Loaded semantic layer")
print("  Connection: duckdb://md:sidemantic_demo")
print(f"  Pre-agg schema: {layer.preagg_schema}")
print(f"  Models: {len(layer.list_models())}")

# Create pre-aggregation schema
print("\nCreating preagg schema...")
layer.adapter.execute("CREATE SCHEMA IF NOT EXISTS sidemantic_demo.preagg")
print("✓ Schema created")

# Get orders model
orders = layer.get_model("orders")
print(f"\nFound {len(orders.pre_aggregations)} pre-aggregations in orders model")

# Refresh each pre-aggregation
print("\nRefreshing pre-aggregations...")
for preagg in orders.pre_aggregations:
    print(f"\n  Refreshing {preagg.name}...")

    # Generate SQL for this pre-aggregation
    source_sql = preagg.generate_materialization_sql(orders)
    table_name = preagg.get_table_name(model_name="orders", database="sidemantic_demo", schema="preagg")

    # Refresh
    result = preagg.refresh(
        connection=layer.adapter.raw_connection,
        source_sql=source_sql,
        table_name=table_name,
        mode="full",
    )

    print(f"    ✓ Materialized {result.rows_inserted:,} rows in {result.duration_seconds:.2f}s")

print("\n" + "=" * 80)
print("Pre-Aggregations Refreshed!")
print("=" * 80)

# Show pre-aggregation tables
print("\nPre-Aggregation Tables:")
tables_result = layer.adapter.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'preagg'
      AND table_catalog = 'sidemantic_demo'
    ORDER BY table_name
""")
for row in tables_result.fetchall():
    table_name = row[0]
    count_result = layer.adapter.execute(f"SELECT COUNT(*) FROM sidemantic_demo.preagg.{table_name}")
    count = count_result.fetchone()[0]
    print(f"  {table_name}: {count:,} rows")

print("\nNext step: Run query_examples.py to see queries using pre-aggregations")
