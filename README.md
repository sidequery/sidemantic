# Sidemantic

SQL-first semantic layer for consistent metrics across your data stack.

Formats: Sidemantic, Cube, MetricFlow (dbt), LookML, Hex, Rill, Superset, Omni.  
Databases: DuckDB, MotherDuck, PostgreSQL, BigQuery, Snowflake, ClickHouse, Databricks, Spark SQL.

[Documentation](https://sidemantic.com) • [GitHub](https://github.com/sidequery/sidemantic)

## Quickstart

Install:
```bash
uv add sidemantic
```

Define your semantic layer (SQL shown; YAML and Python are also supported):
```sql
-- semantic_layer.sql
MODEL (name orders, table orders, primary_key order_id);

DIMENSION (name status, type categorical, sql status);
DIMENSION (name order_date, type time, sql created_at, granularity day);

METRIC (name revenue, agg sum, sql amount);
METRIC (name order_count, agg count);
```

Query with familiar SQL:
```python
from sidemantic import SemanticLayer

layer = SemanticLayer.from_yaml("semantic_layer.sql", connection="duckdb:///data.duckdb")

result = layer.sql("""
    select revenue, status
    from orders
    where status = 'completed'
""")

rows = result.fetchall()
```

## Demos

Colab:
| Demo | Open in Colab |
|---|---|
| SQL model definitions + DuckDB | [![Open in Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/sidequery/sidemantic/blob/main/examples/sidemantic_sql_duckdb_demo.ipynb) |
| LookML multi-entity + DuckDB + chart | [![Open in Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/sidequery/sidemantic/blob/main/examples/lookml_multi_entity_duckdb_demo.ipynb) |

Local notebooks:
- `examples/sidemantic_sql_duckdb_demo.ipynb`
- `examples/lookml_multi_entity_duckdb_demo.ipynb`

## Core Features

- SQL query interface with automatic rewriting
- Automatic joins across models
- Multi-format adapters (Cube, MetricFlow, LookML, Hex, Rill, Superset, Omni)
- SQLGlot-based SQL generation and transpilation
- Pydantic validation and type safety
- Pre-aggregations with automatic routing
- Predicate pushdown for faster queries
- Segments and metric-level filters
- Jinja2 templating for dynamic SQL
- PostgreSQL wire protocol server for BI tools

## Formats and Databases

### Formats
Import from:
Sidemantic (native), Cube, MetricFlow (dbt), LookML (Looker), Hex, Rill, Superset (Apache), Omni.

Adapter compatibility details: https://sidemantic.com

### Databases

| Database | Status | Installation |
|----------|:------:|--------------|
| DuckDB | ✅ | built-in |
| MotherDuck | ✅ | built-in |
| PostgreSQL | ✅ | `pip install sidemantic[postgres]` |
| BigQuery | ✅ | `pip install sidemantic[bigquery]` |
| Snowflake | ✅ | `pip install sidemantic[snowflake]` |
| ClickHouse | ✅ | `pip install sidemantic[clickhouse]` |
| Databricks | ✅ | `pip install sidemantic[databricks]` |
| Spark SQL | ✅ | `pip install sidemantic[spark]` |

## CLI

```bash
# Interactive workbench
uvx sidemantic workbench --demo

# Run a query
sidemantic query semantic_models/ --sql "select orders.revenue from orders"

# PostgreSQL wire protocol server
sidemantic serve semantic_models/ --port 5433

# Validate definitions
sidemantic validate semantic_models/
```

## Load From Multiple Formats

```python
from sidemantic import SemanticLayer, load_from_directory

layer = SemanticLayer(connection="duckdb:///data.duckdb")
load_from_directory(layer, "semantic_models/")

result = layer.query(
    metrics=["orders.revenue"],
    dimensions=["customers.region"]
)
```

## Examples

See `examples/` directory:
- `sql_query_example.py` - SQL query interface demonstration
- `basic_example.py` - Core usage patterns
- `sidemantic/orders.yml` - Native YAML example
- `cube/orders.yml` - Cube format example
- `metricflow/semantic_models.yml` - MetricFlow format example

## Testing

```bash
uv run pytest -v
```
