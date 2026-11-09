---
name: modeler
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

No data yet? `uvx sidemantic demo` writes a complete project (models, DuckDB data,
golden tests) to `./sidemantic-demo` and runs a first query. To start from existing
data, `sidemantic init --from data.duckdb` (or CSV/Parquet/JSON files, or
`--from-connection URL`) writes `sidemantic.yaml`, generated models, and smoke tests.
Inside such a project, commands find the models and connection from the current
directory, so `--models` and `--db` can be omitted.

To write models by hand:

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
uv run sidemantic query \
  "SELECT revenue, status FROM orders ORDER BY revenue DESC LIMIT 5" \
  --models models/ --db data.duckdb --format json
```

Assumes an `orders` table already exists in `data.duckdb` with `status` and `order_amount` columns.
Use `--format json` or `--format jsonl` when consuming CLI results in automation;
use `--plain` for stable tab-separated text. Primary data is stdout and status is
stderr, so do not merge the streams before parsing.

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
from pathlib import Path

from sidemantic import SemanticLayer

# Give DuckDB URLs an absolute path; resolve relative database files first.
connection = f"duckdb:///{Path('data.duckdb').resolve()}"
layer = SemanticLayer.from_yaml("models.yml", connection=connection)
result = layer.sql("SELECT revenue, status FROM orders")
```

### Python API (advanced, optional)

```python
from pathlib import Path

from sidemantic import Model, Dimension, Metric, SemanticLayer

layer = SemanticLayer(connection=f"duckdb:///{Path('data.duckdb').resolve()}")
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
sidemantic migrate generate queries/ --output output/

# Check coverage: how well do existing models handle these queries?
sidemantic migrate check queries/ --models models/ --verbose
```

### Python API (advanced/automation only)

```python
from pathlib import Path

from sidemantic import SemanticLayer
from sidemantic.core.migrator import Migrator

# Connect to your database (optional but improves inference via information_schema)
layer = SemanticLayer(connection=f"duckdb:///{Path('data.duckdb').resolve()}", auto_register=False)
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
2. Run `sidemantic migrate generate queries/ --output output/` (or `migrator.analyze_queries(queries)`) to generate a first pass
3. Review generated models: rename metrics, add descriptions, fix types
4. Run `sidemantic migrate check queries/ --models output/models/` to verify queries can be rewritten through the semantic layer
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
- `primary_key`: the table's primary key column (no default; required for any model that is joined to)

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
| `approx_count_distinct` | `APPROX_COUNT_DISTINCT(col)` | Approximate unique values |
| `avg` | `AVG(col)` | Averages |
| `min` / `max` | `MIN(col)` / `MAX(col)` | Extremes |
| `median` | `MEDIAN(col)` | Median |
| `stddev` / `stddev_pop` | `STDDEV(col)` / `STDDEV_POP(col)` | Spread |
| `variance` / `variance_pop` | `VARIANCE(col)` / `VAR_POP(col)` | Spread |

Model-level simple metrics validate against exactly these 12 aggregations.

Use `filters` on a metric to create filtered aggregations (e.g., `filters: ["status = 'completed'"]`). These become CASE WHEN expressions, not WHERE clauses.

**Complex metrics** (usually graph-level, in top-level `metrics:` section):

| type | Purpose | Required fields |
|------|---------|-----------------|
| `ratio` | Division of two measures | `numerator`, `denominator` |
| `derived` | Arbitrary SQL formula | `sql` (references other metrics) |
| `cumulative` | Rolling/running totals | `sql` (a base metric name, not an aggregate), optional `window` or `grain_to_date` |
| `time_comparison` | Period-over-period | `base_metric`, `comparison_type` (yoy/mom/wow/dod/qoq) |
| `conversion` | Funnel analysis | `entity`, `base_event`, `conversion_event` |
| `retention` | Cohort retention by period | `entity`, `cohort_event`, optional `activity_event`, `periods` |
| `cohort` | Count entities matching a per-entity condition | `entity`, `inner_metrics`, `having`, `agg` |

Graph-level metrics sit in the top-level `metrics:` section (outside `models:`). They reference model-level measures using `model.metric` syntax.

### Step 5: Define Relationships

Connect models with relationships so Sidemantic can auto-generate JOINs.

| Type | Direction | Example |
|------|-----------|---------|
| `many_to_one` | This model has FK to other | orders -> customers |
| `one_to_one` | Unique FK | user -> user_profile |
| `one_to_many` | Other model has FK to this | customer -> orders |
| `many_to_many` | Through junction table | students <-> courses |

Declare relationships on the model that owns the foreign key, and always set `foreign_key`: it is not inferred, and a relationship without it adds no join (`No join path found`). `primary_key` defaults to the related model's `primary_key`.

For `many_to_many`, specify `through` (junction model), `through_foreign_key`, and `related_foreign_key`.

### Step 6: Validate and inspect

```bash
# Validate definitions (checks for errors and warnings)
sidemantic validate models/ --verbose

# Also check that every table and column exists in the database
sidemantic validate models/ --live --db data.duckdb

# Quick summary of what's defined
sidemantic info models/
```

### Step 7: Test with queries

```bash
# Execute semantic SQL through CLI
uv run sidemantic query \
  "SELECT revenue, status FROM orders WHERE status = 'completed'" \
  --models models/ --db data.duckdb

# Structured query, no SQL (repeat --metric/--dimension/--filter as needed)
uv run sidemantic query --models models/ --db data.duckdb \
  --metric orders.revenue --dimension orders.status \
  --filter "orders.status = 'completed'" --order-by "orders.revenue DESC" --limit 10

# Show the generated SQL without executing (or add --dry-run to query)
uv run sidemantic rewrite "SELECT revenue, status FROM orders" --models models/

# Explain how the rewrite was planned (candidate plans, applied rules)
uv run sidemantic explain "SELECT revenue, status FROM orders" --models models/

# Run golden-query tests (tests/*.yml with expected results)
uv run sidemantic test --models models/ --db data.duckdb
```

`--limit` and `--offset` apply to structured queries; put `LIMIT` in the SQL for
SQL queries. Inside a project with `sidemantic.yaml`, drop `--models` and `--db`.

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
        sql: "{model}.status = 'completed'"
      - name: us_only
        sql: "{model}.region = 'US'"

saved_queries:
  - name: completed_revenue_by_status
    metrics: [orders.revenue]
    dimensions: [orders.status]
    segments: [orders.completed_orders]
```

Segments are model-scoped and selected as `model.segment` references in structured
queries. Semantic SQL cannot reference them (`WHERE completed_orders` fails with a
binder error), and `sidemantic query` has no segment flag, so on the CLI apply them
through a saved query:

```bash
uv run sidemantic query --saved-query completed_revenue_by_status \
  --models models/ --db data.duckdb
```

The HTTP API (`/query`) and the MCP `run_query` tool also accept a `segments` list.

Python API (optional):

```python
result = layer.query(
    metrics=["orders.revenue"],
    dimensions=["orders.status"],
    segments=["orders.completed_orders"],
)
```

## Loading from Other Formats

CLI-first (other formats load in place; `convert` writes native YAML):

```bash
uv run sidemantic info path/to/models/
uv run sidemantic validate path/to/models/ --verbose
uv run sidemantic convert path/to/models/ --output models/converted.yml
```

Python API (optional):

```python
from pathlib import Path

from sidemantic import SemanticLayer, load_from_directory

layer = SemanticLayer(connection=f"duckdb:///{Path('data.duckdb').resolve()}")
load_from_directory(layer, "path/to/models/")
```

Auto-detects: Cube (.yml with `cubes:`), dbt MetricFlow (.yml with `semantic_models:`), LookML (.lkml), Malloy (.malloy), Rill, Hex, Snowflake Cortex, and more.

For detailed field mappings from each format, load `references/migration.md`.

## Auto-Registration

When `SemanticLayer()` is created with `auto_register=True` (the default), it sets itself as the "current layer." Any `Model()` or `Metric()` constructed while a layer is active auto-registers with it. This is why the Quick Start examples don't call `layer.add_model()`.

If you create Models before creating a SemanticLayer, they won't be registered. Either create the layer first, or use `layer.add_model(model)` explicitly.

## Parameters

Declare parameters at the top level and reference them as `{{ name }}` in segment
SQL and in query filters. Values are formatted by type (`string` and `date` values
are quoted for you, so do not add quotes); Jinja blocks such as `{% if %}` also work
there.

```yaml
parameters:
  - name: min_amount
    type: number          # string, number, date, unquoted, yesno
    default_value: 100

models:
  - name: orders
    table: orders
    segments:
      - name: large_orders
        sql: "{model}.amount >= {{ min_amount }}"

saved_queries:
  - name: large_order_count
    metrics: [orders.order_count]
    segments: [orders.large_orders]
    parameters:
      min_amount: 500
```

Supply values through a saved query's `parameters` (CLI: `sidemantic query --saved-query
large_order_count`) or in Python; omitted values use `default_value`:

```python
result = layer.query(
    metrics=["orders.order_count"],
    filters=["orders.amount >= {{ min_amount }}"],
    parameters={"min_amount": 500},
)
```

Only segment SQL and query filters are rendered. Templates in model `sql`, dimension
or metric `sql`, and metric `filters` are not rendered and produce broken or
silently wrong SQL (a quoted `'{{ region }}'` matches no rows). Undeclared names
are left as-is.

## CLI Reference

All commands are run as `sidemantic <command>` (`sidemantic help <command>` for details). Global options go before the command (`--project` and `--format` are also accepted after it): `--project/-p DIR` (project root; default: discovered from the current directory), `--config/-c FILE` (a `sidemantic.yaml` with `models_dir` and `connection`), `--format table|csv|json|jsonl`, `--plain`, `--quiet`, `--verbose`. Commands that touch data take `--db FILE` for DuckDB files or `--connection URL` for anything else; models come from `--models DIR` or a positional `[DIR]` as shown.

| Command | Purpose |
|---------|---------|
| `demo [DIR]` | Create a self-contained demo project (models + DuckDB data) and run a first query |
| `init [DIR] [--from PATH] [--from-connection URL]` | Scaffold a project; `--from` generates models from CSV/Parquet/JSON files or a DuckDB database |
| `validate [DIR] [--verbose] [--live] [--json]` | Validate definitions; `--live` also probes tables and columns in the database |
| `info [DIR] [--json]` | Summary of models, dimensions, metrics, relationships |
| `query "SQL" [--models DIR]` | Execute semantic SQL (`--dry-run` prints the generated SQL instead) |
| `query --metric M --dimension D [--filter F] [--order-by F] [--limit N]` | Structured query; `--saved-query NAME` and `--explore NAME` run named definitions |
| `rewrite "SQL" [--models DIR]` | Print the generated SQL without executing |
| `explain "SQL" [--models DIR]` | Explain rewrite planning: candidate plans, applied rules, pre-aggregation routing |
| `test [PATHS]` | Run golden-query tests (default: the project's `tests/`) |
| `convert SOURCE --output FILE` | Convert Cube, dbt, LookML, and other formats to native YAML (`--from`, `--to`) |
| `migrate generate [QUERIES] --output DIR` | Bootstrap models and rewritten queries from SQL files |
| `migrate check [QUERIES] --models DIR` | Coverage: how well models handle existing SQL |
| `preagg recommend --queries FILE` | Recommend pre-aggregations from a log of Sidemantic-generated SQL (it reads the `-- sidemantic:` comments), or from warehouse history via `--connection` on Snowflake, BigQuery, Databricks, ClickHouse |
| `preagg apply [DIR] --queries FILE` | Add recommended pre-aggregations to model YAML (`--dry-run` to preview) |
| `preagg refresh [DIR]` | Build or refresh pre-aggregation tables |
| `serve [DIR]` | Web UI, HTTP API, and MCP endpoint on one port (needs `sidemantic[api]`; MCP also needs `[mcp]`) |
| `server postgres [DIR]` | PostgreSQL wire-protocol server (needs `sidemantic[serve]`) |
| `server mcp [DIR]` | MCP server over stdio, or `--http` (needs `sidemantic[mcp]`) |
| `server api [DIR]` | HTTP API only (needs `sidemantic[api]`) |
| `workbench [DIR]` | Interactive TUI with SQL editor and charting (needs `sidemantic[workbench]`) |
| `lsp` | LSP server for Sidemantic SQL definition files (needs `sidemantic[lsp]`) |
| `dashboard`, `generate` | Dashboard specs; typed TypeScript client generation |

## Connection Strings

DuckDB files: use `--db path/to/file.duckdb` on the CLI, or a `connection:` block in
`sidemantic.yaml` whose relative `path` resolves against that file:

```yaml
connection:
  type: duckdb
  path: data/warehouse.duckdb
```

URL forms (for `--connection` and Python):

```
duckdb:///:memory:                             # In-memory DuckDB
duckdb:////abs/path/to/db.duckdb               # File-based DuckDB: absolute path only
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
8. **Relationships without `foreign_key`.** The key is not inferred from the relationship name; without it the relationship adds no join and cross-model queries fail with `No join path found`. Always set `foreign_key` explicitly.
9. **SQL expressions with YAML special characters.** Quote SQL containing `:`, `#`, `{`, or `>`.
10. **Duplicate model or metric names.** Names must be unique across the entire semantic layer.
11. **Creating Models before SemanticLayer.** With auto-registration (default), Models must be created after the SemanticLayer, or they won't register.
