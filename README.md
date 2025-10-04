# Sidemantic

SQLGlot-based semantic layer with multi-format adapter support.

## Features

### Core Capabilities
- **Simple API**: Define metrics once, use them everywhere
- **SQL query interface**: Write familiar SQL that gets rewritten to use semantic layer
- **Automatic joins**: Define relationships, joins happen automatically via graph traversal
- **Multi-format adapters**: Import/export from Cube, MetricFlow (dbt), and native YAML
- **SQLGlot-powered**: Dialect-agnostic SQL generation with transpilation support
- **Type-safe**: Pydantic models with validation

### Rich Metric Types
- **Aggregations**: sum, avg, count, count_distinct, min, max
- **Ratios**: revenue / order_count
- **Derived formulas**: (revenue - cost) / revenue
- **Cumulative**: running totals, rolling windows
- **Time comparisons**: YoY, MoM, WoW with LAG window functions
- **Conversion funnels**: signup → purchase rate

### Advanced Features
- **Segments**: Reusable named filters with template placeholders
- **Metric-level filters**: Auto-applied filters for consistent business logic
- **Jinja2 templating**: Full conditional logic and loops in SQL
- **Inheritance**: Extend models and metrics (DRY principles)
- **Hierarchies**: Parent/child dimensions with drill-down API
- **Relative dates**: Natural language like "last 7 days", "this month"
- **Ungrouped queries**: Raw row access without aggregation
- **Multi-hop joins**: Automatic 2+ hop join discovery
- **Auto-detected dependencies**: No manual dependency declarations needed

### Metadata & Governance
- **Display formatting**: Format strings and named formats (USD, percent, etc.)
- **Drill fields**: Define drill-down paths for BI tools
- **Non-additivity markers**: Prevent incorrect aggregation
- **Default dimensions**: Default time dimensions and granularity
- **Comprehensive descriptions**: Labels, descriptions on all objects

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

## Feature Examples

### Segments - Reusable Filters
```yaml
models:
  - name: orders
    segments:
      - name: completed
        sql: "{model}.status = 'completed'"
        description: "Only completed orders"
      - name: high_value
        sql: "{model}.amount > 100"

# Use in queries
layer.compile(metrics=["orders.revenue"], segments=["orders.completed"])
```

### Metric-Level Filters
```yaml
metrics:
  - name: completed_revenue
    agg: sum
    sql: amount
    filters: ["{model}.status = 'completed'"]  # Auto-applied!
```

### Jinja2 Templates
```yaml
metrics:
  - name: taxed_revenue
    agg: sum
    sql: "{% if include_tax %}amount * 1.1{% else %}amount{% endif %}"

# Use with parameters
layer.compile(metrics=["orders.taxed_revenue"], parameters={"include_tax": True})
```

### Inheritance
```yaml
models:
  - name: base_sales
    table: sales
    dimensions: [...]

  - name: filtered_sales
    extends: base_sales  # Inherits all dimensions!
    segments: [...]
```

### Hierarchies & Drill-Down
```python
# Define hierarchy
Dimension(name="country", type="categorical")
Dimension(name="state", type="categorical", parent="country")
Dimension(name="city", type="categorical", parent="state")

# Navigate hierarchy
model.get_hierarchy_path("city")  # ['country', 'state', 'city']
model.get_drill_down("country")   # 'state'
model.get_drill_up("city")        # 'state'
```

### Relative Dates
```python
# Natural language date filters
layer.compile(
    metrics=["orders.revenue"],
    filters=["orders_cte.created_at >= 'last 7 days'"]
)
# Auto-converts to: created_at >= CURRENT_DATE - 7

# Supports: "last N days/weeks/months", "this/last/next month/quarter/year", "today", etc.
```

### Ungrouped Queries
```python
# Get raw rows without aggregation (for detail views)
sql = layer.compile(
    metrics=["orders.revenue"],
    dimensions=["orders.customer_id"],
    ungrouped=True  # Returns raw rows
)
```

## Test Coverage

- **202 passing tests** - comprehensive coverage
- Real DuckDB integration
- SQL query rewriting
- Round-trip adapter tests
- Multi-hop join verification
- Formula parsing validation
- Automatic dependency detection
- Jinja template integration
- Inheritance resolution
- Hierarchy navigation

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
- ✅ Conversion funnel metrics
- ✅ Time comparison metrics (YoY, MoM, WoW)
- ✅ Segments (reusable filters)
- ✅ Metric-level filters
- ✅ Jinja2 templating
- ✅ Model and metric inheritance
- ✅ Hierarchies with drill-down API
- ✅ Relative date parsing
- ✅ Ungrouped queries (raw row access)
- ✅ Metadata fields (format, drill_fields, non-additivity, defaults)
- ✅ Native YAML format with import/export
- ✅ Cube and MetricFlow adapters (import/export)
- ✅ DuckDB integration

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
