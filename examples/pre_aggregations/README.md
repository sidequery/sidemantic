# Pre-Aggregations Example

This example demonstrates how to discover and use pre-aggregations to dramatically speed up query performance.

## What are Pre-Aggregations?

Pre-aggregations are materialized tables that store pre-computed aggregations. Instead of scanning millions of raw rows every time, queries can read from smaller, pre-aggregated tables.

**Benefits:**
- 10-100x faster queries on large datasets
- Reduced database load
- Automatic query routing - queries transparently use pre-aggs when available
- Data-driven - discover patterns from actual query usage

## Quick Start

Run the complete demo:

```bash
cd examples/pre_aggregations
chmod +x demo.sh
./demo.sh
```

Or follow the steps manually:

### 1. Setup Sample Data

Generate 10,000 sample orders:

```bash
cd examples/pre_aggregations
uv run setup_data.py
```

### 2. Discover Pre-Aggregation Opportunities

Analyze query patterns to find which pre-aggregations would be most beneficial:

```bash
# Analyze simulated query history
uvx sidemantic preagg recommend --queries query_history.sql

# Or analyze real query history from your database (BigQuery, Snowflake, etc.)
# uvx sidemantic preagg recommend --connection "bigquery://project/dataset" --days 7
```

**Output:**
```
✓ Analyzed 310 queries
  Found 4 unique patterns
  3 patterns above threshold

Pre-Aggregation Recommendations (found 3)

1. daily_status
   Model: orders
   Query Count: 150
   Benefit Score: 0.89
   Metrics: order_count, revenue
   Dimensions: order_date, status
   Granularities: day

2. daily_region
   Model: orders
   Query Count: 80
   Benefit Score: 0.78
   ...
```

### 3. Apply Recommendations

Automatically add pre-aggregations to your model YAML files:

```bash
# Dry run to see what would be added
uvx sidemantic preagg apply models/ --queries query_history.sql --dry-run

# Apply the recommendations
uvx sidemantic preagg apply models/ --queries query_history.sql
```

This updates `models/orders.yml` with pre-aggregation definitions:

```yaml
pre_aggregations:
  - name: daily_status
    measures: [revenue, order_count]
    dimensions: [status]
    time_dimension: order_date
    granularity: day

  - name: daily_region
    measures: [revenue, order_count]
    dimensions: [region]
    time_dimension: order_date
    granularity: day

  - name: monthly_summary
    measures: [revenue, order_count]
    dimensions: [status, region]
    time_dimension: order_date
    granularity: month
```

### 4. Refresh Pre-Aggregations

Materialize the pre-aggregation tables:

```bash
# Refresh all pre-aggregations (uses sidemantic.yaml config)
uvx sidemantic preagg refresh

# Or specify connection explicitly
uvx sidemantic preagg refresh --db data/warehouse.db
```

This creates tables like:
- `preagg.orders_preagg_daily_status` - Daily metrics by status
- `preagg.orders_preagg_daily_region` - Daily metrics by region
- `preagg.orders_preagg_monthly_summary` - Monthly rollup

### 5. Query with Automatic Routing

Queries automatically use pre-aggregations when available:

```bash
# Query from CLI - automatically uses pre-aggregations
uvx sidemantic query "SELECT status, revenue FROM orders"

# Or use the interactive workbench
uvx sidemantic workbench
```

The config in `sidemantic.yaml` enables pre-aggregations and sets the schema.

## How It Works

### 1. Query Pattern Analysis

The recommendation system analyzes query logs to find:
- **Frequently used combinations** of metrics + dimensions
- **Common granularities** (daily, weekly, monthly)
- **Query volume** for each pattern

### 2. Benefit Scoring

Each pattern gets a score based on:
- Query frequency (higher = more beneficial)
- Data reduction potential (how much smaller the pre-agg is vs raw data)
- Query complexity

### 3. Automatic Application

Pre-aggregations are added to model YAML files with sensible defaults:
- Refresh frequency based on granularity (hourly for daily, daily for monthly)
- All metrics from the pattern
- Proper dimension and time dimension selection

### 4. Intelligent Query Routing

At query time, Sidemantic:
1. Checks if a pre-agg matches the query
2. Verifies all metrics are available
3. Confirms dimensions are compatible
4. Routes to pre-agg if all checks pass

## Refresh Modes

### Full Refresh
Rebuilds entire table from scratch:
```bash
uvx sidemantic preagg refresh --mode full
```

### Incremental Refresh (Default)
Only processes new data since last refresh:
```bash
uvx sidemantic preagg refresh --mode incremental
```

### Merge Refresh
Updates existing rows and adds new ones (idempotent):
```bash
uvx sidemantic preagg refresh --mode merge
```

## Working with Real Query History

### BigQuery

```bash
uvx sidemantic preagg recommend \
  --connection "bigquery://my-project/my-dataset" \
  --days 30 \
  --min-count 50
```

### Snowflake

```bash
uvx sidemantic preagg recommend \
  --connection "snowflake://user:pass@account/database/schema" \
  --days 7
```

### Query Log Files

```bash
# From a SQL file with instrumentation comments
uvx sidemantic preagg recommend --queries /path/to/queries.sql

# From a directory of SQL files
uvx sidemantic preagg recommend --queries /path/to/queries/
```

## Configuration

Store config in `sidemantic.yaml`:

```yaml
models_dir: models

connection:
  type: duckdb
  path: data/warehouse.db

# Pre-aggregation configuration
preagg_schema: preagg  # Store in 'preagg' schema

# Or use separate database
# preagg_database: analytics
# preagg_schema: preagg
```

## Scheduling Refreshes

### Cron
```bash
# Refresh every hour
0 * * * * cd /path/to/project && uvx sidemantic preagg refresh --mode incremental
```

### Airflow
```python
from airflow.operators.bash import BashOperator

refresh_preaggs = BashOperator(
    task_id='refresh_preaggs',
    bash_command='cd /path && uvx sidemantic preagg refresh --mode incremental',
)
```

## Best Practices

1. **Start with recommendations** - Let query patterns guide which pre-aggs to create
2. **Use appropriate granularity** - Daily for recent data, monthly for historical
3. **Incremental refresh** - Much faster than full refresh for large datasets
4. **Monitor usage** - Check for `used_preagg=true` in query instrumentation
5. **Tune thresholds** - Adjust `--min-count` and `--min-score` based on your workload

## Troubleshooting

### Query not using pre-aggregation?

Query from workbench or check the compiled SQL to see if pre-agg was used:
```bash
uvx sidemantic workbench
# Run a query and look for "used_preagg=true" in the generated SQL
```

### No recommendations found?

Try lowering thresholds:
```bash
uvx sidemantic preagg recommend --queries query_history.sql --min-count 5 --min-score 0.1
```

### Pre-aggregation refresh fails?

- Ensure database connection is valid in `sidemantic.yaml`
- Check that time_dimension column exists
- Use `--mode full` to rebuild from scratch:
```bash
uvx sidemantic preagg refresh --mode full
```

## Next Steps

- Learn about [query patterns](../../docs/queries.qmd)
- Read [pre-aggregation docs](../../docs/preagg-recommendations.qmd)
- See [configuration docs](../../docs/configuration.qmd)
