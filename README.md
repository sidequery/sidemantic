# Sidemantic

SQL-first semantic layer for consistent metrics across your data stack. Define once, query anywhere.

## Features

- **SQL query interface**: Write familiar SQL that gets rewritten to use semantic layer
- **Automatic joins**: Define relationships, joins happen automatically via graph traversal
- **Multi-format adapters**: Import from 8 semantic layer formats (Cube, dbt, Looker, Hex, and more)
- **SQLGlot-powered**: Dialect-agnostic SQL generation with transpilation support
- **Type-safe**: Pydantic models with validation

## Supported Formats

Sidemantic can import semantic models from:

- **Sidemantic** (native) - Full feature support
- **Cube** - Import with segment support
- **MetricFlow** (dbt) - Import with metric types
- **LookML** (Looker) - Import with Liquid templating
- **Hex** - Import data models
- **Rill** - Import metrics views
- **Superset** (Apache) - Import datasets
- **Omni** - Import views and models

See the [Adapter Compatibility](#adapter-compatibility) section for feature support details.

### Metric Types

- **Aggregations**: sum, avg, count, count_distinct, min, max
- **Ratios**: revenue / order_count
- **Derived formulas**: (revenue - cost) / revenue
- **Cumulative**: running totals, rolling windows
- **Time comparisons**: YoY, MoM, WoW with LAG window functions
- **Conversion funnels**: signup → purchase rate

### Additional Features

- **Pre-aggregations**: Automatic query routing to materialized rollups (disabled by default)
- **Predicate pushdown**: Filters pushed into CTEs for improved performance (always enabled)
- **Segments**: Reusable named filters with template placeholders
- **Metric-level filters**: Auto-applied filters for consistent business logic
- **Jinja2 templating**: Conditional logic and loops in SQL
- **Inheritance**: Extend models and metrics
- **Hierarchies**: Parent/child dimensions with drill-down API
- **Relative dates**: Natural language like "last 7 days", "this month"
- **Ungrouped queries**: Raw row access without aggregation
- **Multi-hop joins**: Automatic 2+ hop join discovery

## Quick Start

### Define your semantic layer (YAML)

```yaml
# semantic_layer.yml

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

## Advanced Features

### Complex Metrics

Define ratios, formulas, cumulative metrics with automatic dependency detection:

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

## Adapter Compatibility

### Supported Formats

| Format | Import | Notes |
|--------|:------:|-------|
| **Sidemantic** (native) | ✅ | Full feature support |
| **Cube** | ✅ | No native segments |
| **MetricFlow** (dbt) | ✅ | No native segments or hierarchies |
| **LookML** (Looker) | ✅ | Liquid templating (not Jinja) |
| **Hex** | ✅ | No segments or cross-model derived metrics |
| **Rill** | ✅ | No relationships, segments, or cross-model metrics; single-model only |
| **Superset** (Apache) | ✅ | No relationships in datasets |
| **Omni** | ✅ | Relationships in separate model file |

### Feature Compatibility

This table shows which Sidemantic features are supported when importing from other formats:

| Feature | Sidemantic | Cube | MetricFlow | LookML | Hex | Rill | Superset | Omni | Notes |
|---------|------------|------|------------|--------|-----|------|----------|------|-------|
| **Models** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | All formats support models/tables |
| **Dimensions** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | All formats support dimensions |
| **Simple Metrics** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | All formats support sum, count, avg, min, max |
| **Time Dimensions** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | All formats support time dimensions with granularity |
| **Relationships** | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | Rill/Superset: single-model only; Omni: in model file |
| **Derived Metrics** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | All formats support calculated metrics |
| **Metric Filters** | ✅ | ✅ | ❌ | ✅ | ✅ | ⚠️ | ❌ | ✅ | Rill has basic support; Superset lacks filters |
| **Ratio Metrics** | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | Rill/Superset don't have native ratio metric type |
| **Segments** | ✅ | ✅ | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ | Only Cube and LookML have native segment support |
| **Cumulative Metrics** | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | Cube has rolling_window; MetricFlow has cumulative; others lack native support |
| **Time Comparison** | ✅ | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | Only MetricFlow has native time comparison metrics |
| **Jinja Templates** | ✅ | ✅ | ✅ | ⚠️ | ✅ | ✅ | ✅ | ✅ | LookML uses Liquid templating |
| **Hierarchies** | ✅ | ⚠️ | ❌ | ⚠️ | ❌ | ❌ | ❌ | ⚠️ | Cube/LookML/Omni: via drill_fields |
| **Inheritance** | ✅ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ | Only LookML has native extends support |
| **Metadata Fields** | ✅ | ⚠️ | ⚠️ | ⚠️ | ⚠️ | ⚠️ | ✅ | ✅ | Label and description support varies by format |
| **Parameters** | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | Sidemantic-only feature |
| **Ungrouped Queries** | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | Sidemantic-only feature |

**Legend:**

- ✅ Full support - feature fully supported on import
- ⚠️ Partial support - feature works with limitations
- ❌ Not supported - feature not available in source format

## Testing

Run tests:
```bash
uv run pytest -v
```

## Database Support

**DuckDB** is the primary supported database (via [Sidequery](https://github.com/sidequery/sidequery), our parent project).

While the SQL generation layer uses SQLGlot and can transpile to other dialects, DuckDB is the only actively tested and supported execution engine. Additional database support may be added based on Sidequery's roadmap.

## Status

See [docs/STATUS.md](docs/STATUS.md) for detailed implementation status.

**Core Features (Complete):**
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
- ✅ Native YAML format
- ✅ Adapters for 8 semantic layer formats (Cube, MetricFlow, LookML, Hex, Rill, Superset, Omni)
- ✅ DuckDB integration
- ✅ Pre-aggregations with automatic query routing (disabled by default)
- ✅ Predicate pushdown with SQLGlot parsing (always enabled)

**Cloud Features (Coming Soon):**
- Pre-aggregation materialization and refresh scheduling
- Additional database connectors beyond DuckDB

## Examples

See `examples/` directory:
- `sql_query_example.py` - SQL query interface demonstration
- `basic_example.py` - Core usage patterns
- `sidemantic/orders.yml` - Native YAML example
- `cube/orders.yml` - Cube format example
- `metricflow/semantic_models.yml` - MetricFlow format example
