# Model Generation & Schema Introspection Reference

## The Migrator

Reverse-engineers semantic models from existing SQL queries. Located in `sidemantic/core/migrator.py`.

```python
from sidemantic.core.migrator import Migrator

migrator = Migrator(layer, connection=db_connection)  # connection is optional
```

When `connection` is provided, the Migrator queries `information_schema` for primary keys, foreign keys, and column metadata, improving inference accuracy.

CLI-first for normal usage:

```bash
sidemantic migrator --queries queries/ --generate-models output/
sidemantic migrator models/ --queries queries/ --verbose
```

### Core Methods

#### `analyze_queries(queries: list[str]) -> MigrationReport`

Parses SQL queries with sqlglot. For each query extracts:
- Tables and aliases (including subqueries)
- Column references grouped by table
- Aggregations with type mapping (sum, avg, count, count_distinct, min, max, stddev, median)
- Derived metrics (expressions combining aggregations, e.g., `SUM(a) / COUNT(b)`)
- Cross-model derived metrics (aggregations spanning multiple tables)
- Cumulative/window function metrics (running totals, rolling windows, period-to-date)
- GROUP BY columns (including ordinal position resolution like `GROUP BY 1`)
- Time dimensions from DATE_TRUNC and EXTRACT
- JOINs with type detection (LEFT, RIGHT, FULL OUTER)
- Relationships from JOIN ON conditions
- Filters, HAVING, ORDER BY, LIMIT

#### `analyze_folder(folder_path: str, pattern: str = "*.sql") -> MigrationReport`

Loads all `.sql` files from a directory, splits by semicolons, feeds through `analyze_queries()`.

#### `generate_models(report: MigrationReport) -> dict[str, dict]`

Aggregates patterns across all analyzed queries. Returns a dict keyed by model name where each value has this structure:

```python
{
    "name": "orders",
    "table": "orders",
    "description": "Auto-generated...",
    "dimensions": [{"name": "status", "sql": "status", "type": "categorical"}, ...],
    "metrics": [{"name": "revenue", "agg": "sum", "sql": "amount"}, ...],
    "relationships": [{"name": "customers", "type": "many_to_one", "foreign_key": "customer_id"}, ...],
}
```

Auto-detection:
- GROUP BY columns become dimensions (`type: time` for DATE_TRUNC/EXTRACT, `type: categorical` otherwise)
- Metric naming: `count` for COUNT(\*), `{col}_count` for COUNT(col), `{col}_count_distinct`, `{agg}_{col}` for others
- Preserves user aliases (e.g., `SUM(amount) AS revenue` -> name is `revenue`)
- Derived metrics get `type: derived`, cumulative metrics get `type: cumulative`

#### `generate_graph_metrics(report: MigrationReport, models: dict) -> list[dict]`

Generates graph-level metrics for cross-model derived expressions. When `SUM(o.amount) / COUNT(DISTINCT c.id)` spans multiple tables, it becomes a graph-level `type: derived` metric referencing `orders.revenue / customers.unique_customers`.

#### `generate_rewritten_queries(report: MigrationReport) -> dict[str, str]`

Rewrites original SQL into semantic layer syntax:
- Dimensions become `model.dimension_name`
- Metrics become `model.metric_name`
- Time dimensions use `model.column__granularity`
- JOINs, WHERE, HAVING, ORDER BY, LIMIT preserved

#### `write_model_files(models: dict, output_dir: str)`

Writes each model to a separate YAML file (`{model_name}.yml`) in native parser format:

```yaml
models:
  - name: orders
    table: orders
    ...
```

#### `write_graph_metrics_file(graph_metrics: list[dict], output_dir: str, filename: str = "graph_metrics.yml") -> str | None`

Writes graph-level metrics to `graph_metrics.yml`:

```yaml
models: []
metrics:
  - name: revenue_per_customer
    type: derived
    sql: orders.revenue / customers.customer_count
```

#### `write_rewritten_queries(queries: dict, output_dir: str)`

Writes each rewritten query to `query_{n}.sql`.

#### `print_report(report: MigrationReport, verbose: bool = False)`

Console coverage report: total/parseable/rewritable counts, missing models/dimensions/metrics. With `--verbose`, per-query details.

### Report fields you actually use

`MigrationReport` fields most useful in practice:
- `total_queries`, `parseable_queries`, `rewritable_queries`, `coverage_percentage`
- `missing_models`, `missing_dimensions`, `missing_metrics`
- `query_analyses` for per-query failures and suggested rewrites

## CLI Commands

### Bootstrap: generate models from queries

```bash
sidemantic migrator --queries queries/ --generate-models output/
```

Creates `output/models/` (YAML per model) and `output/rewritten_queries/` (semantic SQL).

### Coverage analysis: check existing models against queries

```bash
sidemantic migrator models/ --queries queries/ --verbose
```

Reports which queries can be rewritten, which cannot, and what's missing.

### Parameters

| Flag | Type | Description |
|------|------|-------------|
| `directory` | Path | Semantic layer files (default: `.`) |
| `--queries / -q` | Path | Required. SQL file or directory |
| `--verbose / -v` | bool | Per-query analysis details |
| `--generate-models / -g` | Path | Output directory for generated models |

## Database Schema Introspection

All database adapters implement `get_tables()` and `get_columns()`:

```python
layer = SemanticLayer(connection="duckdb:///data.duckdb")
tables = layer.adapter.get_tables()       # [{"table_name": "orders", "schema": "main"}, ...]
columns = layer.adapter.get_columns("orders")  # [{"column_name": "id", "data_type": "INTEGER"}, ...]
```

| Adapter | `get_tables()` source | `get_columns()` source |
|---------|----------------------|----------------------|
| DuckDB | `duckdb_tables()` | `duckdb_columns()` |
| PostgreSQL | `information_schema.tables` | `information_schema.columns` |
| BigQuery | `client.list_tables()` | `client.get_table()` |
| Snowflake | `SHOW TABLES` | `SHOW COLUMNS IN TABLE` |
| ClickHouse | `system.tables` | `system.columns` |
| Databricks | `SHOW TABLES` | `DESCRIBE TABLE` |
| Spark | `SHOW TABLES` | `DESCRIBE TABLE` |

The Migrator's `_load_schema_metadata()` additionally queries `information_schema` for:
- Primary keys (`key_column_usage` + `table_constraints`)
- Foreign keys (`referential_constraints` + `key_column_usage`)
- All columns with data types

## Auto-Model from Arrow Schema

For interactive/exploratory use (used by the workbench TUI):

```python
from sidemantic.widget._auto_model import build_auto_model
import pyarrow as pa

schema = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("amount", pa.float64()),
    pa.field("created_at", pa.timestamp("us")),
    pa.field("status", pa.string()),
])

graph, time_dim = build_auto_model(schema, table_name="orders")
# Creates: dimensions for all columns (time/numeric/categorical inferred from Arrow types)
#          metrics: row_count, sum_amount, avg_amount (auto-generated for numeric cols)
```

Type inference from Arrow:
- `is_date/is_timestamp/is_time` -> `type: time`, `granularity: day`
- `is_integer/is_floating/is_decimal` -> `type: numeric` + auto metrics (sum, avg)
- `is_boolean` -> `type: boolean`
- Everything else -> `type: categorical`

Optional `max_dimension_cardinality` parameter skips high-cardinality columns.

## Relationship Inference in Loader

`load_from_directory()` auto-infers relationships after loading all models:

1. Scans dimensions for names ending in `_id`
2. Strips suffix: `customer_id` -> tries `customer`, `customers`
3. If matching model exists, creates `many_to_one` + reverse `one_to_many`
4. Skips if relationship already exists from adapter data

## Pre-Aggregation Recommender

Analyzes query patterns to recommend materialized rollup tables:

```python
from sidemantic.core.preagg_recommender import PreAggregationRecommender

recommender = PreAggregationRecommender(min_query_count=10, min_benefit_score=0.0)
recommender.parse_query_log(queries)
# Or: recommender.fetch_and_parse_query_history(connection, days_back=7)

for rec in recommender.get_recommendations(top_n=5):
    definition = recommender.generate_preagg_definition(rec)
    print(definition)
```

CLI: `sidemantic preagg recommend` and `sidemantic preagg apply`.

## End-to-End Example: Queries to Semantic Layer

```python
from sidemantic import SemanticLayer, Model, Dimension, Metric, Relationship
from sidemantic.loaders import load_from_directory
from sidemantic.core.migrator import Migrator

# 1. Connect to database
layer = SemanticLayer(connection="duckdb:///warehouse.duckdb", auto_register=False)

# 2. Collect existing queries
queries = [
    "SELECT status, SUM(amount) AS revenue FROM orders GROUP BY status",
    "SELECT DATE_TRUNC('month', created_at) AS month, SUM(amount) FROM orders GROUP BY 1",
    "SELECT c.region, SUM(o.amount) / COUNT(DISTINCT c.id) AS rev_per_customer "
    "FROM orders o JOIN customers c ON o.customer_id = c.id GROUP BY 1",
]

# 3. Generate models
migrator = Migrator(layer, connection=layer.conn)
report = migrator.analyze_queries(queries)
models = migrator.generate_models(report)
graph_metrics = migrator.generate_graph_metrics(report, models)

# 4. Review what was generated
for name, model_def in models.items():
    print(f"\n=== {name} ===")
    for d in model_def.get("dimensions", []):
        print(f"  dim: {d['name']} ({d['type']})")
    for m in model_def.get("metrics", []):
        print(f"  metric: {m['name']} ({m.get('agg', m.get('type'))})")

# 5. Write to disk and refine
migrator.write_model_files(models, "output/models/")
migrator.write_graph_metrics_file(graph_metrics, "output/models/")

# 6. Load refined models and query
layer2 = SemanticLayer(connection="duckdb:///warehouse.duckdb", auto_register=False)
load_from_directory(layer2, "output/models/")
result = layer2.sql("SELECT revenue, status FROM orders")

# 7. Check coverage
migrator2 = Migrator(layer2, connection=layer2.conn)
report2 = migrator2.analyze_queries(queries)
print(f"Coverage: {report2.coverage_percentage:.0f}%")
```
