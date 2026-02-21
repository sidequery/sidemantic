"""Example: DuckDB pre-aggregation layer on top of Hive-partitioned parquet.

This builds on the basic hive_parquet_example.py by adding a pre-aggregation
layer that materializes rollup tables in DuckDB. Queries are automatically
routed to the pre-aggregated tables when they match.

The flow:
1. Create Hive-partitioned parquet data (raw event logs)
2. Define a semantic model with pre-aggregation definitions
3. Materialize the pre-aggregations into DuckDB tables
4. Query -- Sidemantic automatically routes to preagg tables when possible

This pattern is useful for:
- Large parquet lakes where full scans are expensive
- Dashboards that need sub-second response times on common queries
- Reducing I/O on shared storage (S3, GCS, etc.)
"""

import os
import shutil

import duckdb

from sidemantic import Dimension, Metric, Model, SemanticLayer
from sidemantic.core.pre_aggregation import PreAggregation


def create_hive_partitioned_data(base_dir: str):
    """Create a larger Hive-partitioned parquet dataset."""
    if os.path.exists(base_dir):
        shutil.rmtree(base_dir)
    os.makedirs(base_dir, exist_ok=True)

    conn = duckdb.connect()

    # Generate 5,000 events across 12 months
    conn.execute(f"""
        COPY (
            SELECT
                seq AS event_id,
                event_type,
                region,
                CASE
                    WHEN event_type = 'purchase' THEN ROUND((random() * 500 + 10)::DECIMAL, 2)
                    ELSE 0
                END AS amount,
                event_date,
                YEAR(event_date) AS year,
                LPAD(MONTH(event_date)::VARCHAR, 2, '0') AS month
            FROM (
                SELECT
                    generate_series AS seq,
                    ['page_view', 'purchase', 'signup']
                        [1 + CAST(abs(hash(seq || 'type')) % 3 AS BIGINT)] AS event_type,
                    ['us', 'eu', 'ap']
                        [1 + CAST(abs(hash(seq || 'region')) % 3 AS BIGINT)] AS region,
                    ('2024-01-01'::DATE + INTERVAL (CAST(random() * 364 AS INTEGER)) DAY)::DATE AS event_date
                FROM generate_series(1, 5000)
            )
        )
        TO '{base_dir}'
        (FORMAT PARQUET, PARTITION_BY (year, month))
    """)

    conn.close()
    partitions = sum(1 for r, d, f in os.walk(base_dir) for _ in f)
    print(f"Created {partitions} parquet files in: {base_dir}/")


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, "data", "events")

    # Step 1: Generate sample Hive-partitioned parquet data
    print("=" * 70)
    print("Step 1: Create Hive-partitioned parquet data")
    print("=" * 70)
    create_hive_partitioned_data(data_dir)
    print()

    # Step 2: Define semantic model with pre-aggregations
    print("=" * 70)
    print("Step 2: Define semantic model with pre-aggregations")
    print("=" * 70)

    layer = SemanticLayer(
        preagg_schema="preagg",
        use_preaggregations=True,
    )

    events = Model(
        name="events",
        sql=f"SELECT * FROM read_parquet('{data_dir}/**/*.parquet', hive_partitioning=true)",
        primary_key="event_id",
        dimensions=[
            Dimension(name="event_type", type="categorical", sql="event_type"),
            Dimension(name="region", type="categorical", sql="region"),
            Dimension(name="event_date", type="time", sql="event_date", granularity="day"),
        ],
        metrics=[
            Metric(name="event_count", agg="count"),
            Metric(name="total_amount", agg="sum", sql="amount"),
            Metric(name="avg_amount", agg="avg", sql="amount"),
        ],
        pre_aggregations=[
            # Daily rollup by event_type: good for dashboards showing
            # "events by type over time"
            PreAggregation(
                name="daily_by_type",
                measures=["event_count", "total_amount"],
                dimensions=["event_type"],
                time_dimension="event_date",
                granularity="day",
            ),
            # Daily rollup by region: good for geo dashboards
            PreAggregation(
                name="daily_by_region",
                measures=["event_count", "total_amount"],
                dimensions=["region"],
                time_dimension="event_date",
                granularity="day",
            ),
            # Monthly summary with all dimensions: good for high-level reports
            PreAggregation(
                name="monthly_summary",
                measures=["event_count", "total_amount"],
                dimensions=["event_type", "region"],
                time_dimension="event_date",
                granularity="month",
            ),
        ],
    )

    layer.add_model(events)
    print(f"Model 'events' added with {len(events.pre_aggregations)} pre-aggregations")
    for pa in events.pre_aggregations:
        print(f"  - {pa.name}: {pa.granularity} by {pa.dimensions}")
    print()

    # Step 3: Materialize the pre-aggregations into DuckDB
    print("=" * 70)
    print("Step 3: Materialize pre-aggregations")
    print("=" * 70)

    # Create the preagg schema in DuckDB
    layer.adapter.execute("CREATE SCHEMA IF NOT EXISTS preagg")

    for preagg in events.pre_aggregations:
        source_sql = preagg.generate_materialization_sql(events)
        table_name = preagg.get_table_name(model_name="events", schema="preagg")

        result = preagg.refresh(
            connection=layer.adapter.raw_connection,
            source_sql=source_sql,
            table_name=table_name,
            mode="full",
        )
        print(f"  {table_name}: {result.rows_inserted:,} rows ({result.duration_seconds:.2f}s)")

    print()

    # Show what the preagg tables look like
    print("Pre-aggregation table contents (sample from daily_by_type):")
    print("-" * 70)
    sample = layer.adapter.execute("""
        SELECT * FROM preagg.events_preagg_daily_by_type
        ORDER BY event_date_day
        LIMIT 5
    """)
    print(sample.fetchdf())
    print()

    # Step 4: Query WITH pre-aggregation routing
    print("=" * 70)
    print("Step 4: Query with automatic preagg routing")
    print("=" * 70)
    print()

    # This query matches daily_by_type: event_count + event_type dimension
    print("4a. Event count by type (routes to daily_by_type preagg):")
    print("-" * 70)
    compiled = layer.compile(
        metrics=["events.event_count", "events.total_amount"],
        dimensions=["events.event_type"],
    )
    # The compiled SQL will reference the preagg table instead of raw parquet
    print("Generated SQL:")
    print(compiled)
    print()
    result = layer.query(
        metrics=["events.event_count", "events.total_amount"],
        dimensions=["events.event_type"],
    )
    print("Result:")
    print(result.fetchdf())
    print()

    # This query matches daily_by_region: total_amount + region
    print("4b. Amount by region (routes to daily_by_region preagg):")
    print("-" * 70)
    compiled = layer.compile(
        metrics=["events.total_amount"],
        dimensions=["events.region"],
    )
    print("Generated SQL:")
    print(compiled)
    print()
    result = layer.query(
        metrics=["events.total_amount"],
        dimensions=["events.region"],
        order_by=["events.total_amount DESC"],
    )
    print("Result:")
    print(result.fetchdf())
    print()

    # This query matches monthly_summary (monthly granularity, type+region dims)
    print("4c. Monthly summary by type and region (routes to monthly_summary preagg):")
    print("-" * 70)
    compiled = layer.compile(
        metrics=["events.event_count"],
        dimensions=["events.event_type", "events.event_date__month"],
        order_by=["events.event_date__month"],
    )
    print("Generated SQL:")
    print(compiled)
    print()
    result = layer.query(
        metrics=["events.event_count"],
        dimensions=["events.event_type", "events.event_date__month"],
        order_by=["events.event_date__month"],
    )
    print("Result:")
    print(result.fetchdf())
    print()

    # Step 5: Explain query routing
    print("=" * 70)
    print("Step 5: Explain query routing")
    print("=" * 70)
    print()

    print("5a. Query that matches a preagg:")
    print("-" * 70)
    plan = layer.explain(
        metrics=["events.event_count", "events.total_amount"],
        dimensions=["events.event_type"],
    )
    print(plan)
    print()

    print("5b. Query with a dimension not in any preagg:")
    print("-" * 70)
    # Add a dimension that no preagg covers, forcing raw scan
    plan = layer.explain(
        metrics=["events.event_count"],
        dimensions=["events.event_type", "events.region", "events.event_date__day"],
    )
    print(plan)

    # Cleanup
    shutil.rmtree(os.path.join(script_dir, "data"))
    print()
    print("(Cleaned up sample data)")


if __name__ == "__main__":
    main()
