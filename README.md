# Sidemantic

SQLGlot-based semantic layer with multi-format adapter support.

## Features

- **Simple API**: Define measures once, use them everywhere
- **SQL query interface**: Write familiar SQL that gets rewritten to use semantic layer
- **Automatic joins**: Define relationships, joins happen automatically via graph traversal
- **Multi-format adapters**: Import/export from Cube, MetricFlow (dbt), and native YAML
- **Rich measure types**: Aggregations, ratios, formulas, cumulative, time comparisons, conversions
- **SQLGlot-powered**: Dialect-agnostic SQL generation with transpilation support
- **Multi-hop joins**: Automatic 2+ hop join discovery with intermediate models
- **Type-safe**: Pydantic models with validation

## Quick Start

### Define your semantic layer (YAML)

```yaml
# semantic_layer.yml
# yaml-language-server: $schema=./sidemantic-schema.json

models:
  - name: orders
    table: orders
    primary_key: id

    dimensions:
      - name: status
        type: categorical
        sql: status

      - name: order_date
        type: time
        sql: order_date
        granularity: day

    measures:
      - name: revenue
        agg: sum
        expr: amount

      - name: order_count
        agg: count
```

### Query with SQL

```python
from sidemantic import SemanticLayer

# Load semantic layer
layer = SemanticLayer.from_yaml("semantic_layer.yml")

# Query with familiar SQL - automatically rewritten
result = layer.sql("""
    SELECT revenue, status
    FROM orders
    WHERE status = 'completed'
""")

df = result.fetchdf()
```

<details>
<summary>Alternative: Python API</summary>

```python
from sidemantic import SemanticLayer, Model, Measure, Dimension

layer = SemanticLayer()

orders = Model(
    name="orders",
    table="orders",
    primary_key="id",
    dimensions=[
        Dimension(name="status", type="categorical", sql="status"),
        Dimension(name="order_date", type="time", sql="order_date", granularity="day"),
    ],
    measures=[
        Measure(name="revenue", agg="sum", expr="amount"),
        Measure(name="order_count", agg="count"),
    ]
)
layer.add_model(orders)

# Programmatic query
result = layer.query(
    metrics=["orders.revenue"],
    dimensions=["orders.status"],
    filters=["orders.status = 'completed'"]
)
df = result.fetchdf()
```
</details>

## Editor Support

Generate JSON Schema for autocomplete in VS Code, IntelliJ, etc:

```bash
uv run python -m sidemantic.schema
```

Add to your YAML files:
```yaml
# yaml-language-server: $schema=./sidemantic-schema.json
```

## Adapters

### Import
```python
from sidemantic.adapters import CubeAdapter, MetricFlowAdapter, SidemanticAdapter

# From Cube
cube_adapter = CubeAdapter()
graph = cube_adapter.parse("cube_schema.yml")

# From MetricFlow (dbt)
mf_adapter = MetricFlowAdapter()
graph = mf_adapter.parse("semantic_models.yml")

# From native Sidemantic
native_adapter = SidemanticAdapter()
graph = native_adapter.parse("semantic_layer.yml")
```

### Export
```python
# Export to Cube
cube_adapter.export(sl.graph, "output_cube.yml")

# Export to MetricFlow
mf_adapter.export(sl.graph, "output_metricflow.yml")

# Export to native
sl.to_yaml("output_sidemantic.yml")
```

Full round-trip support: Sidemantic ↔ Cube ↔ MetricFlow

## Advanced Features

### Complex Measures

Define ratios, formulas, cumulative metrics:

```yaml
models:
  - name: orders
    table: orders
    primary_key: id

    measures:
      # Simple aggregation
      - name: revenue
        agg: sum
        expr: amount

      # Ratio
      - name: conversion_rate
        type: ratio
        numerator: completed_revenue
        denominator: total_revenue

      # Formula
      - name: profit_margin
        type: derived
        expr: "(revenue - cost) / revenue"

      # Cumulative
      - name: running_total
        type: cumulative
        expr: revenue
        window: "7 days"
```

<details>
<summary>Python alternative</summary>

```python
Measure(name="conversion_rate", type="ratio",
        numerator="completed_revenue", denominator="total_revenue")

Measure(name="profit_margin", type="derived",
        expr="(revenue - cost) / revenue")

Measure(name="running_total", type="cumulative",
        expr="revenue", window="7 days")
```
</details>

### Automatic Joins

Define relationships once, query across models:

```yaml
models:
  - name: orders
    table: orders
    primary_key: id
    joins:
      - name: customers
        type: belongs_to
        foreign_key: customer_id

  - name: customers
    table: customers
    primary_key: id
    joins:
      - name: regions
        type: belongs_to
        foreign_key: region_id
```

Query spans 2 hops automatically:

```python
# Automatically joins orders -> customers -> regions
result = layer.sql("""
    SELECT orders.revenue, regions.region_name
    FROM orders
""")
```

## Test Coverage

- 117 passing tests
- Real DuckDB integration
- SQL query rewriting
- Round-trip adapter tests
- Multi-hop join verification
- Formula parsing validation

Run tests:
```bash
uv run pytest -v
```

## Status

See [docs/STATUS.md](docs/STATUS.md) for detailed implementation status.

**Completed:**
- ✅ SQL query interface with automatic rewriting
- ✅ Core semantic layer with SQLGlot generation
- ✅ Entity-based automatic joins
- ✅ Multi-hop join discovery
- ✅ Derived metrics with formula parsing
- ✅ Native YAML format with import/export
- ✅ Cube and MetricFlow adapters (import/export)
- ✅ DuckDB integration

**In Progress:**
- ⚠️ Cumulative metrics (basic structure exists, needs subquery pattern)

**Future:**
- Query optimization
- Pre-aggregations/caching
- LookML adapter (requires grammar parser)

## Examples

See `examples/` directory:
- `sql_query_example.py` - SQL query interface demonstration
- `basic_example.py` - Core usage patterns
- `export_example.py` - Multi-format export demonstration
- `sidemantic/orders.yml` - Native YAML example
- `cube/orders.yml` - Cube format example
- `metricflow/semantic_models.yml` - MetricFlow format example
