# Sidemantic

SQL-first semantic layer for consistent metrics across your data stack. Compatible with many other semantic model formats.

- **Formats:** Sidemantic, Cube, MetricFlow (dbt), LookML, Hex, Rill, Superset, Omni, BSL, GoodData LDM, Snowflake Cortex, Malloy, OSI, AtScale SML, ThoughtSpot TML
- **Databases:** DuckDB, MotherDuck, PostgreSQL, BigQuery, Snowflake, ClickHouse, Databricks, Spark SQL

[Documentation](https://sidemantic.com) | [GitHub](https://github.com/sidequery/sidemantic) | [Discord](https://discord.com/invite/7MZ4UgSVvF) | [Demo](https://sidemantic.com/demo) (50+ MB download)

![Jupyter Widget Preview](preview.png)

## Should I use Sidemantic

Sidemantic is a very ambitious and young semantic layer project. You may encounter rough patches, especially with the more exotic features like converting between semantic model formats.

Issue reports are much appreciated if you try out Sidemantic and hit a snag 🫡

## Quickstart

Install:
```bash
uv add sidemantic
```

Malloy support (uv):
```bash
uv add "sidemantic[malloy]"
```

Notebook widget (uv):
```bash
uv add "sidemantic[widget]" jupyterlab
uv run jupyter lab
```

Marimo (uv):
```bash
uv add "sidemantic[widget]" marimo
uv run marimo edit
```

```python
import duckdb
from sidemantic.widget import MetricsExplorer

conn = duckdb.connect(":memory:")
conn.execute("create table t as select 1 as value, 'a' as category, date '2024-01-01' as d")
MetricsExplorer(conn.table("t"), time_dimension="d")
```

Define models in SQL, YAML, or Python:

<details>
<summary><b>SQL</b> (orders.sql)</summary>

```sql
MODEL (name orders, table orders, primary_key order_id);
DIMENSION (name status, type categorical);
DIMENSION (name order_date, type time, granularity day);
METRIC (name revenue, agg sum, sql amount);
METRIC (name order_count, agg count);
```
</details>

<details>
<summary><b>YAML</b> (orders.yml)</summary>

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
        granularity: day
    metrics:
      - name: revenue
        agg: sum
        sql: amount
      - name: order_count
        agg: count
```
</details>

<details>
<summary><b>Python</b> (programmatic)</summary>

```python
from sidemantic import Model, Dimension, Metric

orders = Model(
    name="orders",
    table="orders",
    primary_key="order_id",
    dimensions=[
        Dimension(name="status", type="categorical"),
        Dimension(name="order_date", type="time", granularity="day"),
    ],
    metrics=[
        Metric(name="revenue", agg="sum", sql="amount"),
        Metric(name="order_count", agg="count"),
    ]
)
```
</details>

Query via CLI:
```bash
sidemantic query "SELECT revenue, status FROM orders" --db data.duckdb
```

Or Python API:
```python
from sidemantic import SemanticLayer, load_from_directory

layer = SemanticLayer(connection="duckdb:///data.duckdb")
load_from_directory(layer, "models/")
result = layer.sql("SELECT revenue, status FROM orders")
```

## CLI

```bash
# Query
sidemantic query "SELECT revenue FROM orders" --db data.duckdb

# Interactive workbench (TUI with SQL editor + charts)
sidemantic workbench models/ --db data.duckdb

# PostgreSQL server (connect Tableau, DBeaver, etc.)
sidemantic serve models/ --port 5433

# Validate definitions
sidemantic validate models/

# Model info
sidemantic info models/

# Pre-aggregation recommendations
sidemantic preagg recommend --db data.duckdb

# Migrate SQL queries to semantic layer
sidemantic migrator --queries legacy/ --generate-models output/
```

## Demos

**Workbench** (TUI with SQL editor + charts):
```bash
uvx sidemantic workbench --demo
```

**PostgreSQL server** (connect Tableau, DBeaver, etc.):
```bash
uvx sidemantic serve --demo --port 5433
```

**Colab notebooks:**

[![Open in Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/sidequery/sidemantic/blob/main/examples/notebooks/sidemantic_sql_duckdb_demo.ipynb) SQL + DuckDB

[![Open in Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/sidequery/sidemantic/blob/main/examples/notebooks/lookml_multi_entity_duckdb_demo.ipynb) LookML multi-entity

**SQL syntax:**
```bash
uv run https://raw.githubusercontent.com/sidequery/sidemantic/main/examples/sql/sql_syntax_example.py
```

**Comprehensive demo:**
```bash
uv run https://raw.githubusercontent.com/sidequery/sidemantic/main/examples/advanced/comprehensive_demo.py
```

**Symmetric aggregates:**
```bash
uv run https://raw.githubusercontent.com/sidequery/sidemantic/main/examples/features/symmetric_aggregates_example.py
```

**Superset with DuckDB:**
```bash
git clone https://github.com/sidequery/sidemantic.git && cd sidemantic
uv run examples/superset_demo/run_demo.py
```

**Cube Playground:**
```bash
git clone https://github.com/sidequery/sidemantic.git && cd sidemantic
uv run examples/cube_demo/run_demo.py
```

**Rill Developer:**
```bash
git clone https://github.com/sidequery/sidemantic.git && cd sidemantic
uv run examples/rill_demo/run_demo.py
```

**OSI (complex adtech semantic model):**
```bash
git clone https://github.com/sidequery/sidemantic.git && cd sidemantic
uv run examples/osi_demo/run_demo.py
```

**OSI widget notebook (percent-cell Python notebook):**
```bash
git clone https://github.com/sidequery/sidemantic.git && cd sidemantic
uv run examples/osi_demo/osi_widget_notebook.py
```

See `examples/` for more.

## Core Features

- SQL query interface with automatic rewriting
- Automatic joins across models
- Multi-format adapters (Cube, MetricFlow, LookML, Hex, Rill, Superset, Omni, BSL, GoodData LDM, OSI, AtScale SML, ThoughtSpot TML)
- SQLGlot-based SQL generation and transpilation
- Pydantic validation and type safety
- Pre-aggregations with automatic routing
- Predicate pushdown for faster queries
- Segments and metric-level filters
- Jinja2 templating for dynamic SQL
- PostgreSQL wire protocol server for BI tools

## Multi-Format Support

Auto-detects: Sidemantic (SQL/YAML), Cube, MetricFlow (dbt), LookML, Hex, Rill, Superset, Omni, BSL, GoodData LDM, OSI, AtScale SML, ThoughtSpot TML

```bash
sidemantic query "SELECT revenue FROM orders" --models ./my_models
```

```python
from sidemantic import SemanticLayer, load_from_directory

layer = SemanticLayer(connection="duckdb:///data.duckdb")
load_from_directory(layer, "my_models/")  # Auto-detects formats
```

## Databases

| Database | Status | Installation |
|----------|:------:|--------------|
| DuckDB | ✅ | built-in |
| MotherDuck | ✅ | built-in |
| PostgreSQL | ✅ | `uv add sidemantic[postgres]` |
| BigQuery | ✅ | `uv add sidemantic[bigquery]` |
| Snowflake | ✅ | `uv add sidemantic[snowflake]` |
| ClickHouse | ✅ | `uv add sidemantic[clickhouse]` |
| Databricks | ✅ | `uv add sidemantic[databricks]` |
| Spark SQL | ✅ | `uv add sidemantic[spark]` |

## Docker

Build the image (includes all database drivers, PG server, and MCP server):

```bash
docker build -t sidemantic .
```

### PostgreSQL server (default)

Mount your models directory and expose port 5433:

```bash
docker run -p 5433:5433 -v ./models:/app/models sidemantic
```

Connect with any PostgreSQL client:

```bash
psql -h localhost -p 5433 -U any -d sidemantic
```

With a backend database connection:

```bash
docker run -p 5433:5433 \
  -v ./models:/app/models \
  -e SIDEMANTIC_CONNECTION="postgres://user:pass@host:5432/db" \
  sidemantic
```

### MCP server

```bash
docker run -v ./models:/app/models -e SIDEMANTIC_MODE=mcp sidemantic
```

### Both servers simultaneously

Runs the PG server in the background and MCP on stdio:

```bash
docker run -p 5433:5433 -v ./models:/app/models -e SIDEMANTIC_MODE=both sidemantic
```

### Demo mode

```bash
docker run -p 5433:5433 sidemantic --demo
```

### Baking models into the image

Create a `Dockerfile` that copies your models at build time:

```dockerfile
FROM sidemantic
COPY my_models/ /app/models/
```

### Environment variables

| Variable | Description |
|----------|-------------|
| `SIDEMANTIC_MODE` | `serve` (default), `mcp`, or `both` |
| `SIDEMANTIC_CONNECTION` | Database connection string |
| `SIDEMANTIC_DB` | Path to DuckDB file (inside container) |
| `SIDEMANTIC_USERNAME` | PG server auth username |
| `SIDEMANTIC_PASSWORD` | PG server auth password |
| `SIDEMANTIC_PORT` | PG server port (default 5433) |

## Testing

```bash
uv run pytest -v
```
