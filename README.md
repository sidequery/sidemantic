# Sidemantic

SQLGlot-based semantic layer with multi-format adapter support.

## Features

- **Simple API**: Define metrics once, use them everywhere
- **SQL query interface**: Write familiar SQL that gets rewritten to use semantic layer
- **Automatic joins**: Define relationships, joins happen automatically via graph traversal
- **Multi-format adapters**: Import/export from Cube, MetricFlow (dbt), and native YAML
- **Rich metric types**: Aggregations, ratios, formulas, cumulative, time comparisons, conversions
- **Auto-detected dependencies**: No manual dependency declarations needed
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
    primary_key: order_id

    relationships:
      - name: customer
        type: many_to_one
        foreign_key: customer_id

    dimensions:
      - name: status
        type: categorical
        sql: status

      - name: order_date
        type: time
        sql: created_at
        granularity: day

    metrics:
      - name: revenue
        agg: sum
        sql: amount

      - name: order_count
        agg: count

# Graph-level metrics (dependencies auto-detected!)
metrics:
  - name: total_revenue
    sql: orders.revenue
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
from sidemantic import SemanticLayer, Model, Metric, Dimension, Relationship

layer = SemanticLayer()

orders = Model(
    name="orders",
    table="orders",
    primary_key="order_id",
    relationships=[
        Relationship(name="customer", type="many_to_one", foreign_key="customer_id")
    ],
    dimensions=[
        Dimension(name="status", type="categorical", sql="status"),
        Dimension(name="order_date", type="time", sql="created_at", granularity="day"),
    ],
    metrics=[
        Metric(name="revenue", agg="sum", sql="amount"),
        Metric(name="order_count", agg="count"),
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

### Complex Metrics

Define ratios, formulas, cumulative metrics with **automatic dependency detection**:

```yaml
models:
  - name: orders
    table: orders
    primary_key: order_id

    metrics:
      # Model-level aggregations
      - name: revenue
        agg: sum
        sql: amount

      - name: completed_revenue
        agg: sum
        sql: amount
        filters: ["status = 'completed'"]

# Graph-level metrics
metrics:
  # Simple reference (dependencies auto-detected)
  - name: total_revenue
    sql: orders.revenue

  # Ratio
  - name: conversion_rate
    type: ratio
    numerator: orders.completed_revenue
    denominator: orders.revenue

  # Derived (dependencies auto-detected from formula!)
  - name: profit_margin
    type: derived
    sql: "(revenue - cost) / revenue"

  # Cumulative
  - name: running_total
    type: cumulative
    sql: orders.revenue
    window: "7 days"
```

<details>
<summary>Python alternative</summary>

```python
Metric(name="total_revenue", sql="orders.revenue")

Metric(name="conversion_rate", type="ratio",
       numerator="orders.completed_revenue",
       denominator="orders.revenue")

Metric(name="profit_margin", type="derived",
       sql="(revenue - cost) / revenue")

Metric(name="running_total", type="cumulative",
       sql="orders.revenue", window="7 days")
```
</details>

### Automatic Joins

Define relationships once, query across models:

```yaml
models:
  - name: orders
    table: orders
    primary_key: order_id
    relationships:
      - name: customer
        type: many_to_one
        foreign_key: customer_id

  - name: customers
    table: customers
    primary_key: customer_id
    relationships:
      - name: region
        type: many_to_one
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

### Relationship Types

Use explicit, readable relationship types:

- **many_to_one**: Many records in THIS table → one record in OTHER table (e.g., orders → customer)
- **one_to_many**: One record in THIS table → many records in OTHER table (e.g., customer → orders)
- **one_to_one**: One record in THIS table → one record in OTHER table (e.g., order → invoice)

## Test Coverage

- 117 passing tests
- Real DuckDB integration
- SQL query rewriting
- Round-trip adapter tests
- Multi-hop join verification
- Formula parsing validation
- Automatic dependency detection

Run tests:
```bash
uv run pytest -v
```

## Status

See [docs/STATUS.md](docs/STATUS.md) for detailed implementation status.

**Completed:**
- ✅ SQL query interface with automatic rewriting
- ✅ Core semantic layer with SQLGlot generation
- ✅ Relationship-based automatic joins (many_to_one, one_to_many, one_to_one)
- ✅ Multi-hop join discovery
- ✅ Derived metrics with automatic dependency detection
- ✅ Cumulative metrics (running totals, rolling windows)
- ✅ Native YAML format with import/export
- ✅ Cube and MetricFlow adapters (import/export)
- ✅ DuckDB integration
- ✅ Unified metrics terminology (no more measures/metrics confusion!)

**Future:**
- Query optimization
- Pre-aggregations/caching
- Time comparison metrics (YoY, MoM)
- Conversion funnel metrics
- LookML adapter (requires grammar parser)

## Examples

See `examples/` directory:
- `sql_query_example.py` - SQL query interface demonstration
- `basic_example.py` - Core usage patterns
- `export_example.py` - Multi-format export demonstration
- `sidemantic/orders.yml` - Native YAML example
- `cube/orders.yml` - Cube format example
- `metricflow/semantic_models.yml` - MetricFlow format example
