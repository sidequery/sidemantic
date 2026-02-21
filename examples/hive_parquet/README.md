# Hive-Partitioned Parquet Files

DuckDB natively supports [Hive-partitioned parquet](https://duckdb.org/docs/data/partitioning/hive_partitioning) directories. Sidemantic works with them by using the `sql` field on a model with `read_parquet(..., hive_partitioning=true)`.

## Quick Start

```bash
uv run python examples/hive_parquet/hive_parquet_example.py
```

## How It Works

Given a Hive-partitioned directory structure like:

```
data/events/
  year=2024/month=01/data.parquet
  year=2024/month=02/data.parquet
  year=2025/month=01/data.parquet
```

Define a model using `sql` instead of `table`:

```python
from sidemantic import Model, Dimension, Metric

events = Model(
    name="events",
    sql="SELECT * FROM read_parquet('data/events/**/*.parquet', hive_partitioning=true)",
    dimensions=[
        Dimension(name="event_type", type="categorical", sql="event_type"),
        # Partition columns are available as regular dimensions
        Dimension(name="year", type="categorical", sql="year"),
        Dimension(name="month", type="categorical", sql="month"),
    ],
    metrics=[
        Metric(name="event_count", agg="count"),
        Metric(name="total_amount", agg="sum", sql="amount"),
    ],
)
```

Or in YAML:

```yaml
models:
  - name: events
    sql: "SELECT * FROM read_parquet('data/events/**/*.parquet', hive_partitioning=true)"
    dimensions:
      - name: event_type
        type: categorical
        sql: event_type
      - name: year
        type: categorical
        sql: year
      - name: month
        type: categorical
        sql: month
    metrics:
      - name: event_count
        agg: count
      - name: total_amount
        agg: sum
        sql: amount
```

Partition columns (`year`, `month`) become regular columns you can use as dimensions and filter on.

## S3 / GCS / HTTP

DuckDB also supports reading parquet from remote storage:

```python
Model(
    name="events",
    sql="SELECT * FROM read_parquet('s3://my-bucket/events/**/*.parquet', hive_partitioning=true)",
    ...
)
```

See the [DuckDB docs](https://duckdb.org/docs/extensions/httpfs/overview) for configuring S3 credentials and other remote sources.

## Alternative: Create a View

If you prefer using `table` instead of `sql`, create a DuckDB view first:

```python
layer = SemanticLayer()
layer.conn.execute("""
    CREATE VIEW events AS
    SELECT * FROM read_parquet('data/events/**/*.parquet', hive_partitioning=true)
""")

events = Model(name="events", table="events", ...)
```
