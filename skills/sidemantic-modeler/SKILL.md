---
name: sidemantic-modeler
description: "Build, validate, and manage semantic models using Sidemantic. Use when asked to create a semantic layer, define metrics/dimensions, model a database schema, generate models from SQL queries, import from Cube/dbt/LookML, or set up analytics definitions. Prioritizes CLI-first workflows, with YAML and optional Python API usage for advanced automation."
license: Apache-2.0
metadata:
  author: sidemantic
  version: "1.0"
---

# Sidemantic Modeler

Build semantic layers that map physical database tables to business-friendly dimensions and metrics. Sidemantic generates SQL from these definitions, handling joins, aggregations, granularity, and dialect differences automatically.

## Quick Start

### 2-Minute First Success (CLI-first onboarding path)

```bash
uv add sidemantic duckdb

mkdir -p models

cat > models/orders.yml <<'YAML'
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: order_amount
      - name: order_count
        agg: count
YAML

uv run sidemantic validate models/ --verbose
uv run sidemantic info models/
uv run sidemantic query models/ -c duckdb:///data.duckdb \
  "SELECT revenue, status FROM orders ORDER BY revenue DESC LIMIT 5"
```

Assumes an `orders` table already exists in `data.duckdb` with `status` and `order_amount` columns.

### YAML (preferred for file-based models)

```yaml
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
      - name: order_date
        type: time
        sql: created_at
        granularity: day
    metrics:
      - name: revenue
        agg: sum
        sql: order_amount
      - name: order_count
        agg: count
```

Load and query:

```python
from sidemantic import SemanticLayer

layer = SemanticLayer.from_yaml("models.yml", connection="duckdb:///data.duckdb")
result = layer.sql("SELECT revenue, status FROM orders")
```

### Python API (advanced, optional)

```python
from sidemantic import Model, Dimension, Metric, SemanticLayer

layer = SemanticLayer(connection="duckdb:///data.duckdb")
Model(
    name="orders",
    table="orders",
    primary_key="order_id",
    dimensions=[
        Dimension(name="status", type="categorical"),
        Dimension(name="order_date", type="time", sql="created_at", granularity="day"),
    ],
    metrics=[
        Metric(name="revenue", agg="sum", sql="order_amount"),
        Metric(name="order_count", agg="count"),
    ],
)
result = layer.sql("SELECT revenue, status FROM orders")
```

## Generate Models from SQL Queries

The fastest path when existing queries are available. The Migrator reverse-engineers semantic models by analyzing SQL: it extracts tables, columns, aggregations, joins, time dimensions, derived metrics, and window functions automatically.

### CLI (bootstrap from a folder of .sql files)

```bash
# Generate model YAML + rewritten queries from raw SQL
sidemantic migrator --queries queries/ --generate-models output/

# Check coverage: how well do existing models handle these queries?
sidemantic migrator models/ --queries queries/ --verbose
```

### Python API (advanced/automation only)

```python
from sidemantic import SemanticLayer
from sidemantic.core.migrator import Migrator

# Connect to your database (optional but improves inference via information_schema)
layer = SemanticLayer(connection="duckdb:///data.duckdb", auto_register=False)
migrator = Migrator(layer, connection=layer.conn)

# Feed it SQL queries (strings, not files)
queries = [
    "SELECT status, SUM(amount) AS revenue, COUNT(*) AS orders FROM orders GROUP BY status",
    "SELECT DATE_TRUNC('month', created_at), SUM(amount) FROM orders GROUP BY 1",
    "SELECT c.region, SUM(o.amount) / COUNT(DISTINCT c.id) AS rev_per_customer "
    "FROM orders o JOIN customers c ON o.customer_id = c.id GROUP BY 1",
]

report = migrator.analyze_queries(queries)
models = migrator.generate_models(report)          # YAML-ready model dicts
graph_metrics = migrator.generate_graph_metrics(report, models)  # cross-model metrics
rewritten = migrator.generate_rewritten_queries(report)          # semantic SQL

# Write to disk
migrator.write_model_files(models, "output/models/")
migrator.write_rewritten_queries(rewritten, "output/rewritten_queries/")

# Print coverage report
migrator.print_report(report, verbose=True)
```

### What the Migrator auto-detects

| Pattern in SQL | What it generates |
|----------------|-------------------|
| `SUM(amount)` / `COUNT(*)` / `AVG(price)` | Metric with matching `agg` |
| `COUNT(DISTINCT user_id)` | Metric with `agg: count_distinct` |
| `SUM(amount) AS revenue` | Metric named `revenue` (preserves aliases) |
| `GROUP BY status` | Dimension `type: categorical` |
| `DATE_TRUNC('month', created_at)` | Dimension `type: time`, granularity extracted from SQL (here: `month`) |
| `JOIN customers ON o.customer_id = c.id` | Relationship `many_to_one`, `foreign_key: customer_id` |
| `SUM(a) / NULLIF(COUNT(b), 0)` | Derived metric with formula |
| `SUM(x) OVER (ORDER BY date ROWS ...)` | Cumulative metric with `window` |
| `SUM(x) OVER (PARTITION BY DATE_TRUNC(...))` | Cumulative metric with `grain_to_date` |
| Cross-model expressions | Graph-level derived metrics |

### Workflow: queries first, then refine

1. Collect existing SQL queries (dashboards, reports, ad-hoc analyses)
2. Run `migrator.analyze_queries(queries)` to generate a first pass
3. Review generated models: rename metrics, add descriptions, fix types
4. Run coverage check to verify queries can be rewritten through the semantic layer
5. Iterate until coverage is high

For the full Migrator API (all methods, outputs, edge cases), load `references/generation.md`.

## Core Workflow

Follow these steps when building a semantic model from a database schema.

### Step 1: Analyze the database schema

Inspect tables, columns, data types, and foreign key relationships. Identify which tables hold transactional/event data (fact tables) and which hold descriptive attributes (dimension tables).

### Step 2: Create Model definitions

For each table, create a Model with:
- `name`: a short, snake_case identifier
- `table`: schema-qualified table name (e.g., `public.orders`)
- `primary_key`: the table's primary key column (default: `id`)

Use `sql` instead of `table` for derived/virtual tables built from a SQL expression.

### Step 3: Define Dimensions

Add dimensions for columns used in GROUP BY or WHERE clauses. Choose the correct type:

| Type | When to use | Example |
|------|-------------|---------|
| `categorical` | Strings, enums, IDs for grouping | `status`, `region` |
| `time` | Dates/timestamps (enables granularity) | `created_at`, `order_date` |
| `boolean` | Computed true/false from SQL expression | `sql: "amount > 100"` |
| `numeric` | Numbers used for grouping, not aggregation | `quantity_bucket` |

Time dimensions require `granularity` (one of: `second`, `minute`, `hour`, `day`, `week`, `month`, `quarter`, `year`). Queries use double-underscore syntax: `orders.order_date__month`.

Use `sql` when the dimension maps to a different column name or a computed expression. If omitted, defaults to a column matching `name`.

Set `parent` on dimensions to create drill-down hierarchies (e.g., country > state > city).

### Step 4: Define Metrics

Add metrics for columns that should be aggregated.

**Simple aggregations** (model-level):

| agg | SQL generated | Notes |
|-----|---------------|-------|
| `sum` | `SUM(col)` | Revenue, quantities |
| `count` | `COUNT(*)` | Row counts (no `sql` needed) |
| `count_distinct` | `COUNT(DISTINCT col)` | Unique values |
| `avg` | `AVG(col)` | Averages |
| `min` / `max` | `MIN(col)` / `MAX(col)` | Extremes |
| `median` | `MEDIAN(col)` | Median |

Model-level simple metrics currently validate against: `sum`, `count`, `count_distinct`, `avg`, `min`, `max`, `median`.

Use `filters` on a metric to create filtered aggregations (e.g., `filters: ["status = 'completed'"]`). These become CASE WHEN expressions, not WHERE clauses.

**Complex metrics** (usually graph-level, in top-level `metrics:` section):

| type | Purpose | Required fields |
|------|---------|-----------------|
| `ratio` | Division of two measures | `numerator`, `denominator` |
| `derived` | Arbitrary SQL formula | `sql` (references other metrics) |
| `cumulative` | Rolling/running totals | `sql`, optional `window` or `grain_to_date` |
| `time_comparison` | Period-over-period | `base_metric`, `comparison_type` (yoy/mom/wow/dod/qoq) |
| `conversion` | Funnel analysis | `entity`, `base_event`, `conversion_event` |

Graph-level metrics sit in the top-level `metrics:` section (outside `models:`). They reference model-level measures using `model.metric` syntax.

### Step 5: Define Relationships

Connect models with relationships so Sidemantic can auto-generate JOINs.

| Type | Direction | Example |
|------|-----------|---------|
| `many_to_one` | This model has FK to other | orders -> customers |
| `one_to_one` | Unique FK | user -> user_profile |
| `one_to_many` | Other model has FK to this | customer -> orders |
| `many_to_many` | Through junction table | students <-> courses |

Declare relationships on the model that owns the foreign key. For `many_to_one`, `foreign_key` defaults to `{related_model}_id`.

For `many_to_many`, specify `through` (junction model), `through_foreign_key`, and `related_foreign_key`.

### Step 6: Validate and inspect

```bash
# Validate definitions (checks for errors and warnings)
sidemantic validate models/ --verbose

# Quick summary of what's defined
sidemantic info models/
```

### Step 7: Test with queries

```bash
# Validate and inspect without writing code
uv run sidemantic validate models/ --verbose
uv run sidemantic info models/

# Execute semantic SQL through CLI
uv run sidemantic query models/ -c duckdb:///data.duckdb \
  "SELECT revenue, status FROM orders WHERE status = 'completed'"
```

Python API (optional):

```python
# Structured query API
result = layer.query(
    metrics=["orders.revenue"],
    dimensions=["orders.status", "orders.order_date__month"],
    filters=["orders.status = 'completed'"],
    order_by=["orders.revenue DESC"],
    limit=10,
)

# SQL interface (auto-rewrites through semantic layer)
result = layer.sql("SELECT revenue, status FROM orders WHERE status = 'completed'")

# Compile to SQL without executing
sql = layer.compile(metrics=["orders.revenue"], dimensions=["customers.region"])
```

## Segments

Reusable named WHERE filters applied at query time. Unlike metric filters, segments affect all metrics in the query.

```yaml
models:
  - name: orders
    table: orders
    segments:
      - name: completed_orders
        sql: "status = 'completed'"
      - name: us_only
        sql: "{model}.region = 'US'"
```

Segments are model-scoped and used as `model.segment` references at query time:

```bash
uv run sidemantic query models/ -c duckdb:///data.duckdb \
  "SELECT revenue, status FROM orders WHERE completed_orders"
```

Python API (optional):

```python
result = layer.query(
    metrics=["orders.revenue"],
    dimensions=["orders.status"],
    segments=["orders.completed_orders"],
)
```

## Loading from Other Formats

CLI-first:

```bash
uv run sidemantic info path/to/models/
uv run sidemantic validate path/to/models/ --verbose
```

Python API (optional):

```python
from sidemantic import SemanticLayer, load_from_directory

layer = SemanticLayer(connection="duckdb:///data.duckdb")
load_from_directory(layer, "path/to/models/")
```

Auto-detects: Cube (.yml with `cubes:`), dbt MetricFlow (.yml with `semantic_models:`), LookML (.lkml), Malloy (.malloy), Rill, Hex, Snowflake Cortex, and more.

For detailed field mappings from each format, load `references/migration.md`.

## Auto-Registration

When `SemanticLayer()` is created with `auto_register=True` (the default), it sets itself as the "current layer." Any `Model()` or `Metric()` constructed while a layer is active auto-registers with it. This is why the Quick Start examples don't call `layer.add_model()`.

If you create Models before creating a SemanticLayer, they won't be registered. Either create the layer first, or use `layer.add_model(model)` explicitly.

## Jinja2 Parameters

SQL expressions in models support Jinja2 templating:

```yaml
models:
  - name: orders
    sql: "SELECT * FROM orders WHERE region = '{{ region }}'"
```

Pass values at query time:

```python
result = layer.query(metrics=["orders.revenue"], parameters={"region": "US"})
```

## CLI Reference

All commands are run as `sidemantic <command>`. Use `--config path/to/sidemantic.yaml` to load a config file with connection and model path settings.

| Command | Purpose |
|---------|---------|
| `validate [DIR] --verbose` | Validate definitions, show errors and warnings |
| `info [DIR]` | Summary of models, dimensions, metrics, relationships |
| `query [DIR] -c CONNECTION SQL` | Execute SQL through the semantic layer (`--format table/json/csv`, `--limit N`) |
| `migrator [DIR] --queries PATH` | Coverage analysis: check how well models handle SQL queries |
| `migrator --queries PATH --generate-models OUT` | Bootstrap: generate model YAML from SQL queries |
| `preagg recommend [DIR]` | Recommend pre-aggregation tables from query patterns |
| `preagg apply [DIR]` | Apply pre-aggregation recommendations |
| `serve [DIR] -c CONNECTION` | Start PostgreSQL wire-protocol server |
| `mcp-serve [DIR] -c CONNECTION` | Start MCP server for AI tool integration |
| `workbench [DIR] -c CONNECTION` | Interactive TUI with SQL editor and charting |
| `lsp` | Start LSP server for Sidemantic SQL files |

## Connection Strings

```
duckdb:///:memory:                             # In-memory DuckDB
duckdb:///path/to/db.duckdb                    # File-based DuckDB
duckdb://md:database_name                      # MotherDuck
postgres://user:pass@host:port/dbname          # PostgreSQL
bigquery://project_id/dataset_id               # BigQuery
snowflake://user:pass@account/database/schema  # Snowflake
clickhouse://user:pass@host:port/database      # ClickHouse
databricks://token@server-hostname/http-path   # Databricks
spark://host:port/database                     # Spark SQL
adbc://driver/uri                              # ADBC
```

## Reference Files

Load these when you need deeper detail:

- `references/yaml-schema.md`: Field-level YAML schema with every field, type, default, and constraint
- `references/patterns.md`: Complete YAML templates for e-commerce, SaaS, marketing, IoT, and star schema patterns
- `references/validation.md`: All validation rules, error messages, and fixes
- `references/migration.md`: Field-by-field mappings from Cube, dbt, LookML, and other formats
- `references/generation.md`: Migrator API, schema introspection, auto-model generation, pre-aggregation recommendations

## Common Mistakes

1. **Missing `granularity` on time dimensions.** Every `type: time` dimension needs `granularity: day` (or similar).
2. **Simple metric without `agg`.** Metrics that are not complex types need an `agg` field (sum, count, avg, etc.) or a full SQL expression like `sql: "SUM(amount)"`.
3. **Unqualified fields in multi-model queries.** Single-model SQL can use unqualified names (`SELECT revenue FROM orders`), but cross-model queries should use explicit `model.field`.
4. **No relationship path between models.** Cross-model queries require a chain of relationships connecting all involved models.
5. **Using `type: string` or `type: number` for dimensions.** The valid types are `categorical`, `time`, `boolean`, `numeric`.
6. **Confusing model-level vs graph-level metrics.** Model-level metrics use `agg`. Graph-level metrics (ratio, derived, etc.) go in the top-level `metrics:` section.
7. **Missing required fields on complex metrics.** ratio needs `numerator` + `denominator`. derived needs `sql`. time_comparison needs `base_metric`. conversion needs `entity`, `base_event`, `conversion_event`.
8. **Plural relationship names create wrong FK defaults.** Relationship named `customers` defaults FK to `customers_id`, not `customer_id`. Always set `foreign_key` explicitly.
9. **SQL expressions with YAML special characters.** Quote SQL containing `:`, `#`, `{`, or `>`.
10. **Duplicate model or metric names.** Names must be unique across the entire semantic layer.
11. **Creating Models before SemanticLayer.** With auto-registration (default), Models must be created after the SemanticLayer, or they won't register.
