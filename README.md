# Sidemantic

SQLGlot-based semantic layer with multi-format adapter support.

## Features

- **SQL query interface**: Write familiar SQL queries that get automatically rewritten to use the semantic layer
- **Entity-based automatic joins**: Define entities once, joins discovered automatically via BFS graph traversal
- **Multi-format adapters**: Import/export from Cube, MetricFlow (dbt), and native Sidemantic YAML
- **Rich metric types**: Simple, ratio, derived (formula-based), and cumulative metrics
- **SQLGlot-powered**: Dialect-agnostic SQL generation with transpilation support
- **Multi-hop joins**: Automatic discovery of 2+ hop join paths with intermediate model inclusion
- **Python-first API**: Type-safe with Pydantic models

## Quick Start

```python
from sidemantic import SemanticLayer

# Load from native YAML
sl = SemanticLayer.from_yaml("semantic_layer.yml")

# Option 1: Write SQL queries (automatically rewritten to use semantic layer)
result = sl.sql("""
    SELECT orders.revenue, customers.region
    FROM orders
    WHERE orders.status = 'completed'
""")
df = result.fetchdf()

# Option 2: Programmatic API
result = sl.query(
    metrics=["orders.revenue", "total_revenue"],
    dimensions=["orders.status", "orders.order_date__month"],
    filters=["orders.status = 'completed'"]
)
df = result.df()

# Option 3: Just compile to SQL
sql = sl.compile(
    metrics=["revenue_per_order"],
    dimensions=["customers.region"]
)
print(sql)
```

## YAML Format

```yaml
models:
  - name: orders
    table: public.orders

    entities:
      - name: order
        type: primary
        expr: order_id
      - name: customer
        type: foreign
        expr: customer_id

    dimensions:
      - name: status
        type: categorical

      - name: order_date
        type: time
        granularity: day
        expr: created_at

    measures:
      - name: revenue
        agg: sum
        expr: order_amount

metrics:
  - name: total_revenue
    type: simple
    measure: orders.revenue

  - name: revenue_per_order
    type: derived
    expr: "total_revenue / order_count"
    metrics:
      - total_revenue
      - order_count
```

See [docs/YAML_FORMAT.md](docs/YAML_FORMAT.md) for complete format specification.

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

### SQL Query Interface
Write familiar SQL that gets rewritten to use the semantic layer:

```python
# SQL queries are automatically rewritten
result = sl.sql("""
    SELECT
        orders.revenue,
        orders.order_count,
        customers.region
    FROM orders
    WHERE orders.status = 'completed'
    ORDER BY orders.revenue DESC
    LIMIT 10
""")

# Metrics are automatically aggregated
# Joins happen automatically when you reference multiple models
# All semantic layer features work transparently
```

See `examples/sql_query_example.py` for comprehensive examples.

### Cross-Model Metrics
Metrics can reference measures from multiple models:

```python
# Metric using measure from different model
conversion_rate = Metric(
    name="conversion_rate",
    type="ratio",
    numerator="orders.completed_revenue",  # from orders model
    denominator="orders.total_revenue"      # from orders model
)
```

### Multi-Hop Joins
Automatic join path discovery across 2+ models:

```python
# Query across orders -> customers -> regions (2 hops)
result = sl.query(
    metrics=["orders.revenue"],
    dimensions=["regions.region_name"]  # Automatically joins through customers
)
```

### Derived Metrics
Formula-based metrics with recursive dependency resolution:

```python
Metric(
    name="revenue_per_order",
    type="derived",
    expr="total_revenue / order_count",
    metrics=["total_revenue", "order_count"]  # Dependencies
)
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
