# Sidemantic

SQL-first semantic layer for consistent metrics across your data stack.

- **Formats:** Sidemantic, Cube, MetricFlow (dbt), LookML, Hex, Rill, Superset, Omni, BSL
- **Databases:** DuckDB, MotherDuck, PostgreSQL, BigQuery, Snowflake, ClickHouse, Databricks, Spark SQL

[Documentation](https://sidemantic.com) | [GitHub](https://github.com/sidequery/sidemantic) | [Discord](https://discord.com/invite/7MZ4UgSVvF)

## Quickstart

Install:
```bash
uv add sidemantic
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
from sidemantic import SemanticLayer

layer = SemanticLayer.from_directory("models/", connection="duckdb:///data.duckdb")
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

```bash
# Interactive workbench with demo data
uvx sidemantic workbench --demo

# PostgreSQL server with demo data
uvx sidemantic serve --demo --port 5433
```

Runnable scripts:
```bash
uv run https://raw.githubusercontent.com/sidequery/sidemantic/main/examples/sql/sql_syntax_example.py
uv run https://raw.githubusercontent.com/sidequery/sidemantic/main/examples/advanced/comprehensive_demo.py
uv run https://raw.githubusercontent.com/sidequery/sidemantic/main/examples/features/symmetric_aggregates_example.py
```

Rill integration (requires Docker):
```bash
git clone https://github.com/sidequery/sidemantic && cd sidemantic
uv run examples/rill_demo/run_demo.py
```

Notebooks:
| Demo | Open in Colab |
|---|---|
| SQL model definitions + DuckDB | [![Open in Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/sidequery/sidemantic/blob/main/examples/notebooks/sidemantic_sql_duckdb_demo.ipynb) |
| LookML multi-entity + DuckDB + chart | [![Open in Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/sidequery/sidemantic/blob/main/examples/notebooks/lookml_multi_entity_duckdb_demo.ipynb) |

## Core Features

- SQL query interface with automatic rewriting
- Automatic joins across models
- Multi-format adapters (Cube, MetricFlow, LookML, Hex, Rill, Superset, Omni, BSL)
- SQLGlot-based SQL generation and transpilation
- Pydantic validation and type safety
- Pre-aggregations with automatic routing
- Predicate pushdown for faster queries
- Segments and metric-level filters
- Jinja2 templating for dynamic SQL
- PostgreSQL wire protocol server for BI tools

## Multi-Format Support

Auto-detects: Sidemantic (SQL/YAML), Cube, MetricFlow (dbt), LookML, Hex, Rill, Superset, Omni, BSL

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

## Examples

See `examples/` directory:
- `basic/` - Getting started examples
- `sql/` - SQL query interface demonstrations
- `yaml/` - YAML syntax examples
- `features/` - Specific features (parameters, symmetric aggregates, multi-hop joins)
- `advanced/` - Comprehensive demos
- `integrations/` - Streamlit, charts, export adapters
- `notebooks/` - Jupyter notebooks
- `rill_demo/` - Export to Rill and run in Docker
- `multi_format_demo/` - Same model in different formats (Cube, Hex, LookML, BSL)

## Testing

```bash
uv run pytest -v
```
