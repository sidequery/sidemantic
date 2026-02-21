"""Example: Using Sidemantic with Hive-partitioned parquet files.

DuckDB natively supports reading Hive-partitioned parquet directories.
Sidemantic works with them via the `sql` field on a Model, which lets you
use read_parquet() with hive_partitioning=true.

This example:
1. Creates a Hive-partitioned parquet dataset (year/month directories)
2. Defines a semantic model on top of it
3. Queries it using both the semantic API and the SQL interface

Directory structure created:
    data/events/year=2024/month=01/data.parquet
    data/events/year=2024/month=02/data.parquet
    data/events/year=2024/month=03/data.parquet
    data/events/year=2025/month=01/data.parquet
"""

import os
import shutil

import duckdb

from sidemantic import Dimension, Metric, Model, SemanticLayer


def create_hive_partitioned_data(base_dir: str):
    """Create a sample Hive-partitioned parquet dataset."""
    # Clean up any previous run
    if os.path.exists(base_dir):
        shutil.rmtree(base_dir)
    os.makedirs(base_dir, exist_ok=True)

    conn = duckdb.connect()

    # Sample event data
    conn.execute("""
        CREATE TABLE raw_events AS
        SELECT * FROM (VALUES
            (1,  'page_view',  'us', 100, '2024-01-05'::DATE),
            (2,  'purchase',   'us', 250, '2024-01-12'::DATE),
            (3,  'page_view',  'eu', 80,  '2024-01-20'::DATE),
            (4,  'purchase',   'eu', 300, '2024-02-03'::DATE),
            (5,  'page_view',  'us', 120, '2024-02-14'::DATE),
            (6,  'signup',     'ap', 0,   '2024-02-28'::DATE),
            (7,  'purchase',   'us', 450, '2024-03-01'::DATE),
            (8,  'page_view',  'eu', 90,  '2024-03-15'::DATE),
            (9,  'signup',     'us', 0,   '2024-03-22'::DATE),
            (10, 'purchase',   'ap', 180, '2025-01-08'::DATE),
            (11, 'page_view',  'us', 110, '2025-01-15'::DATE),
            (12, 'purchase',   'eu', 320, '2025-01-25'::DATE)
        ) AS t(event_id, event_type, region, amount, event_date)
    """)

    # Write as Hive-partitioned parquet
    conn.execute(f"""
        COPY (
            SELECT
                event_id, event_type, region, amount, event_date,
                YEAR(event_date) AS year,
                LPAD(MONTH(event_date)::VARCHAR, 2, '0') AS month
            FROM raw_events
        )
        TO '{base_dir}'
        (FORMAT PARQUET, PARTITION_BY (year, month))
    """)

    conn.close()
    print(f"Created Hive-partitioned parquet at: {base_dir}/")
    for root, dirs, files in os.walk(base_dir):
        for f in files:
            rel = os.path.relpath(os.path.join(root, f), base_dir)
            print(f"  {rel}")
    print()


def main():
    # Resolve paths relative to this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, "data", "events")

    # Step 1: Create sample Hive-partitioned parquet data
    create_hive_partitioned_data(data_dir)

    # Step 2: Define semantic model pointing at the partitioned parquet
    #
    # The key is using `sql` with read_parquet(..., hive_partitioning=true).
    # This tells DuckDB to infer partition columns (year, month) from the
    # directory structure and expose them as regular columns.
    layer = SemanticLayer()

    events = Model(
        name="events",
        sql=f"SELECT * FROM read_parquet('{data_dir}/**/*.parquet', hive_partitioning=true)",
        primary_key="event_id",
        dimensions=[
            Dimension(name="event_type", type="categorical", sql="event_type"),
            Dimension(name="region", type="categorical", sql="region"),
            Dimension(name="event_date", type="time", sql="event_date", granularity="day"),
            # Partition columns are available as regular dimensions
            Dimension(name="year", type="categorical", sql="year"),
            Dimension(name="month", type="categorical", sql="month"),
        ],
        metrics=[
            Metric(name="event_count", agg="count"),
            Metric(name="total_amount", agg="sum", sql="amount"),
            Metric(name="avg_amount", agg="avg", sql="amount"),
        ],
    )

    layer.add_model(events)

    # Step 3: Query using the semantic API
    print("=" * 70)
    print("Semantic API Queries")
    print("=" * 70)
    print()

    print("1. Event count and total amount by type:")
    print("-" * 40)
    result = layer.query(
        metrics=["events.event_count", "events.total_amount"],
        dimensions=["events.event_type"],
    )
    print(result.fetchdf())
    print()

    print("2. Revenue by region:")
    print("-" * 40)
    result = layer.query(
        metrics=["events.total_amount"],
        dimensions=["events.region"],
        order_by=["events.total_amount DESC"],
    )
    print(result.fetchdf())
    print()

    print("3. Monthly event counts (using Hive partition column):")
    print("-" * 40)
    result = layer.query(
        metrics=["events.event_count"],
        dimensions=["events.year", "events.month"],
        order_by=["events.year", "events.month"],
    )
    print(result.fetchdf())
    print()

    print("4. Filter to purchases only:")
    print("-" * 40)
    result = layer.query(
        metrics=["events.total_amount", "events.avg_amount"],
        dimensions=["events.region"],
        filters=["events.event_type = 'purchase'"],
    )
    print(result.fetchdf())
    print()

    # Step 4: Query using the SQL interface
    print("=" * 70)
    print("SQL Interface Queries")
    print("=" * 70)
    print()

    print("5. SQL: Total amount by event type")
    print("-" * 40)
    sql = "SELECT events.total_amount, events.event_type FROM events"
    print(f"  {sql}")
    result = layer.sql(sql)
    print(result.fetchdf())
    print()

    print("6. SQL: 2024 purchases by region (filter on partition column)")
    print("-" * 40)
    sql = """
        SELECT events.total_amount, events.region
        FROM events
        WHERE events.event_type = 'purchase' AND events.year = 2024
    """
    print(f"  {sql.strip()}")
    result = layer.sql(sql)
    print(result.fetchdf())
    print()

    # Step 5: Show the generated SQL for transparency
    print("=" * 70)
    print("Generated SQL (compile only)")
    print("=" * 70)
    print()
    compiled = layer.compile(
        metrics=["events.event_count", "events.total_amount"],
        dimensions=["events.year", "events.month"],
        order_by=["events.year", "events.month"],
    )
    print(compiled)

    # Cleanup
    shutil.rmtree(os.path.join(script_dir, "data"))
    print()
    print("(Cleaned up sample data)")


if __name__ == "__main__":
    main()
