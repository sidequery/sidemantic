# Adapter Migration Guide

Convert semantic models from Cube.js, dbt MetricFlow, LookML, and other formats into Sidemantic.

## Auto-Import (Recommended)

```python
from sidemantic import SemanticLayer, load_from_directory

layer = SemanticLayer(connection="duckdb:///data.duckdb")
load_from_directory(layer, "path/to/models/")
```

Auto-detects format by file extension and content:
- `.lkml`: LookML
- `.malloy`: Malloy
- `.aml`: Holistics
- `.tml`: ThoughtSpot
- `.json`: GoodData
- `.yml`/`.yaml` with `cubes:`: Cube.js
- `.yml`/`.yaml` with `semantic_models:`: dbt MetricFlow
- `.yml`/`.yaml` with `base_sql_table:` + `measures:`: Hex
- `.yml`/`.yaml` with `type: metrics_view`: Rill
- `.yml`/`.yaml` with `tables:` + `base_table:`: Snowflake Cortex
- `.yml`/`.yaml` with `models:`: Sidemantic native

Relationships are auto-inferred from foreign key naming conventions (e.g., `customer_id` on `orders` creates `many_to_one` to `customers`).

### CLI

```bash
sidemantic info ./models                                    # Inspect
sidemantic query "SELECT revenue FROM orders" --models ./models  # Query
sidemantic query "..." --models ./models --dry-run          # SQL only
```

## Cube.js to Sidemantic

### Concept Mapping

| Cube.js | Sidemantic |
|---------|------------|
| Cube | Model |
| `sql_table` | `table` |
| `sql` (cube-level) | `sql` |
| Dimension | Dimension |
| Measure | Metric |
| Join | Relationship |
| Segment | Segment |
| Pre-aggregation | PreAggregation |
| `${CUBE}` | `{model}` |

### Dimension Type Mapping

| Cube.js | Sidemantic |
|---------|------------|
| `type: string` | `type: categorical` |
| `type: number` | `type: numeric` |
| `type: time` | `type: time` |
| `type: boolean` | `type: categorical` |
| `primary_key: true` | Model-level `primary_key` |

### Measure Type Mapping

| Cube.js | Sidemantic |
|---------|------------|
| `type: count` | `agg: count` |
| `type: count_distinct` | `agg: count_distinct` |
| `type: count_distinct_approx` | `agg: count_distinct` |
| `type: sum` | `agg: sum` |
| `type: avg` | `agg: avg` |
| `type: min` / `max` | `agg: min` / `max` |
| `type: number` | `type: derived` |
| `rolling_window` | `type: cumulative` + `window` |
| `drill_members` | `drill_fields` |

Ratio auto-detection: if a `type: number` measure's SQL matches `${measure1} / NULLIF(${measure2}, 0)`, it becomes `type: ratio` with `numerator` and `denominator`.

### Join Mapping

| Cube.js | Sidemantic |
|---------|------------|
| `joins[].name` | `relationships[].name` |
| `joins[].relationship` | `relationships[].type` |
| `joins[].sql` | FK extracted from SQL expression |

### Side-by-Side Example

**Cube.js:**
```yaml
cubes:
  - name: orders
    sql_table: public.orders
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: status
        sql: status
        type: string
      - name: created_at
        sql: created_at
        type: time
    measures:
      - name: count
        type: count
      - name: revenue
        sql: amount
        type: sum
      - name: completed_revenue
        sql: amount
        type: sum
        filters:
          - sql: "${CUBE}.status = 'completed'"
    joins:
      - name: customers
        sql: "${CUBE}.customer_id = ${customers.id}"
        relationship: many_to_one
```

**Sidemantic:**
```yaml
models:
  - name: orders
    table: public.orders
    primary_key: id
    dimensions:
      - name: status
        type: categorical
      - name: created_at
        type: time
        granularity: day
    metrics:
      - name: count
        agg: count
      - name: revenue
        agg: sum
        sql: amount
      - name: completed_revenue
        agg: sum
        sql: amount
        filters:
          - "{model}.status = 'completed'"
    relationships:
      - name: customers
        type: many_to_one
        foreign_key: customer_id
```

### Features that don't map

- JavaScript cubes (`.js`): convert to YAML first
- `SECURITY_CONTEXT` (multi-tenancy): not supported
- Data source config: use Sidemantic connection config

## dbt MetricFlow to Sidemantic

### Concept Mapping

| MetricFlow | Sidemantic |
|------------|------------|
| `semantic_models[]` | `models[]` |
| `model: ref('table')` | `table` (ref stripped) |
| Entity (primary) | `primary_key` |
| Entity (foreign) | `relationships[]` (many_to_one) |
| `defaults.agg_time_dimension` | `default_time_dimension` |
| Dimension | Dimension |
| Measure | Metric (model-level) |
| Metric (graph-level) | Metric (graph-level) |

### Entity Handling

- `type: primary, expr: col` becomes `primary_key: col`
- `type: foreign, name: X, expr: col` becomes relationship: `{name: X, type: many_to_one, foreign_key: col}`
- Entity name auto-pluralized: `customer` resolves to `customers` model

### Dimension Mapping

| MetricFlow | Sidemantic |
|------------|------------|
| `type: categorical` | `type: categorical` |
| `type: time` | `type: time` |
| `expr` | `sql` |
| `type_params.time_granularity` | `granularity` |
| `meta.parent` | `parent` |

### Measure Mapping

| MetricFlow | Sidemantic |
|------------|------------|
| `agg: sum/count/count_distinct/min/max/median` | Direct |
| `agg: average` | `agg: avg` |
| `agg: sum_boolean` | `agg: sum` |
| `expr` | `sql` |
| `non_additive_dimension.name` | `non_additive_dimension` |

### Graph-Level Metric Mapping

| MetricFlow | Sidemantic |
|------------|------------|
| `type: simple` | No type, `sql: model.measure` |
| `type: ratio` | `type: ratio` |
| `type: derived` | `type: derived` |
| `type: cumulative` | `type: cumulative` |
| `type: conversion` | Not supported, skipped |
| `type_params.measure.name` | `sql` |
| `type_params.numerator` | `numerator` |
| `type_params.denominator` | `denominator` |
| `type_params.expr` | `sql` |
| `type_params.window` | `window` |
| `type_params.grain_to_date` | `grain_to_date` |

### Side-by-Side Example

**MetricFlow:**
```yaml
semantic_models:
  - name: orders
    model: ref('orders')
    defaults:
      agg_time_dimension: order_date
    entities:
      - name: order
        type: primary
        expr: order_id
      - name: customer
        type: foreign
        expr: customer_id
    dimensions:
      - name: order_date
        type: time
        type_params:
          time_granularity: day
        expr: created_at
      - name: status
        type: categorical
    measures:
      - name: order_count
        agg: count
      - name: revenue
        agg: sum
        expr: order_amount

metrics:
  - name: total_revenue
    type: simple
    type_params:
      measure:
        name: revenue
  - name: average_order_value
    type: ratio
    type_params:
      numerator: revenue
      denominator: order_count
```

**Sidemantic:**
```yaml
models:
  - name: orders
    table: orders
    primary_key: order_id
    default_time_dimension: order_date
    dimensions:
      - name: order_date
        type: time
        granularity: day
        sql: created_at
      - name: status
        type: categorical
    metrics:
      - name: order_count
        agg: count
      - name: revenue
        agg: sum
        sql: order_amount
    relationships:
      - name: customers
        type: many_to_one
        foreign_key: customer_id

metrics:
  - name: total_revenue
    sql: orders.revenue
  - name: average_order_value
    type: ratio
    numerator: revenue
    denominator: order_count
```

### MetricFlow features that don't map

- `create_metric: true` on measures: not modeled (measures always queryable)
- `saved_queries`: not modeled
- `conversion` metric type: skipped during import

## LookML to Sidemantic

### Concept Mapping

| LookML | Sidemantic |
|--------|------------|
| View | Model |
| `sql_table_name` | `table` |
| `derived_table.sql` | `sql` |
| Dimension | Dimension |
| `dimension_group` (time) | Multiple time Dimensions (one per timeframe) |
| `dimension_group` (duration) | Multiple numeric Dimensions |
| Measure | Metric |
| Explore join | Relationship |
| Filter (view-level) | Segment |
| `${TABLE}` | `{model}` |
| `${dimension_name}` | Resolved to actual SQL recursively |

### Dimension Type Mapping

| LookML | Sidemantic |
|--------|------------|
| `type: string` | `type: categorical` |
| `type: number` | `type: numeric` |
| `type: yesno` | `type: categorical` |
| `type: tier` | `type: categorical` |
| `primary_key: yes` | Model-level `primary_key` |

### Dimension Group Expansion

A `dimension_group` with `type: time` creates one Sidemantic dimension per timeframe:

| Timeframe | Dimension name | Granularity |
|-----------|---------------|-------------|
| `date` | `{group}_date` | `day` |
| `week` | `{group}_week` | `week` |
| `month` | `{group}_month` | `month` |
| `quarter` | `{group}_quarter` | `quarter` |
| `year` | `{group}_year` | `year` |
| `time` | `{group}_time` | `hour` |
| `raw` | (skipped) | -- |

### Measure Type Mapping

| LookML | Sidemantic |
|--------|------------|
| `type: count` | `agg: count` |
| `type: count_distinct` | `agg: count_distinct` |
| `type: sum` | `agg: sum` |
| `type: average` | `agg: avg` |
| `type: min` / `max` | `agg: min` / `max` |
| `type: median` | `agg: median` |
| `type: number` | `type: derived` |
| `type: percentile` / `list` | `type: derived` |

### Filter Value Conversion

| LookML filter | Generated SQL |
|---------------|---------------|
| `[status: "completed"]` | `{model}.status = 'completed'` |
| `[status: "val1,val2"]` | `{model}.status IN ('val1', 'val2')` |
| `[status: "-cancelled"]` | `{model}.status != 'cancelled'` |
| `[amount: ">500"]` | `{model}.amount > 500` |
| `[active: "yes"]` | `{model}.active = true` |
| `[name: "%pattern%"]` | `{model}.name LIKE '%pattern%'` |
| `[field: "NULL"]` | `{model}.field IS NULL` |

### LookML features that don't map

- Liquid templates (`{% %}`): imported as-is, not evaluated
- View refinements (`+view_name`): first definition wins
- `extends`: not parsed by LookML adapter
- `sql_trigger_value`, `datagroup_trigger`, `access_filter`, `sets`, `parameters`: not mapped

## Other Formats (Quick Reference)

### Rill

| Rill | Sidemantic |
|------|------------|
| `type: metrics_view` | Detection marker |
| `table` / `model` | `table` |
| `timeseries` | `default_time_dimension` |
| `smallest_time_grain` | `default_grain` |
| `dimensions[].column` | `dimensions[].sql` |
| `dimensions[].expression` | `dimensions[].sql` |
| `dimensions[].display_name` | `dimensions[].label` |
| `measures[].expression` | `metrics[].sql` (full agg expression, auto-parsed) |
| `measures[].type: derived` | `metrics[].type: derived` |
| `measures[].format_preset: currency_usd` | `metrics[].value_format_name: usd` |

### Hex

| Hex | Sidemantic |
|-----|------------|
| `base_sql_table` | `table` |
| `base_sql_query` | `sql` |
| `dimensions[].type: string` | `type: categorical` |
| `dimensions[].type: number` | `type: numeric` |
| `dimensions[].type: timestamp_tz` | `type: time` (granularity: `hour`) |
| `dimensions[].type: date` | `type: time` (granularity: `day`) |
| `dimensions[].expr_sql` | `dimensions[].sql` |
| `dimensions[].unique: true` | Model-level `primary_key` |
| `measures[].func` | `metrics[].agg` |
| `measures[].of` | `metrics[].sql` |
| `measures[].func_sql` | `metrics[].sql` + `type: derived` |

### Snowflake Cortex

| Snowflake | Sidemantic |
|-----------|------------|
| `tables[].base_table.{database,schema,table}` | `table` (joined with `.`) |
| `tables[].primary_key.columns[0]` | `primary_key` |
| `tables[].dimensions[].data_type: NUMBER` | `type: numeric` |
| `tables[].dimensions[].data_type: TEXT` | `type: categorical` |
| `tables[].time_dimensions[]` | Dimensions with `type: time` |
| `tables[].facts[].default_aggregation` | `agg` (`average` -> `avg`) |
| `tables[].metrics[].expr` | `sql` (single-agg parsed; multi-agg becomes `type: derived`) |
| `tables[].filters[]` | Segments (bare columns auto-qualified with `{model}.`) |

## Programmatic Import

### Using adapters directly

```python
from sidemantic.adapters.cube import CubeAdapter
from sidemantic.adapters.metricflow import MetricFlowAdapter
from sidemantic.adapters.lookml import LookMLAdapter

adapter = CubeAdapter()
graph = adapter.parse("path/to/cube_models/")

for name, model in graph.models.items():
    print(f"{name}: {len(model.dimensions)} dims, {len(model.metrics)} metrics")
```

### Cross-format conversion

```python
cube_adapter = CubeAdapter()
graph = cube_adapter.parse("cube_models/")

mf_adapter = MetricFlowAdapter()
mf_adapter.export(graph, "output/metricflow_models.yml")
```

### Building a layer from parsed models

```python
from sidemantic import SemanticLayer
from sidemantic.adapters.lookml import LookMLAdapter

layer = SemanticLayer(connection="duckdb:///data.duckdb")
adapter = LookMLAdapter()
graph = adapter.parse("lookml_views/")
for model in graph.models.values():
    layer.add_model(model)

result = layer.sql("SELECT revenue, status FROM orders")
```

## Supported Adapters

| Format | Adapter Class | File Extensions |
|--------|--------------|-----------------|
| Sidemantic | SidemanticAdapter | `.yml`, `.yaml`, `.sql` |
| Cube.js | CubeAdapter | `.yml` (with `cubes:`) |
| dbt MetricFlow | MetricFlowAdapter | `.yml` (with `semantic_models:`) |
| LookML | LookMLAdapter | `.lkml` |
| Hex | HexAdapter | `.yml` (with `base_sql_table:`) |
| Rill | RillAdapter | `.yml` (with `type: metrics_view`) |
| Malloy | MalloyAdapter | `.malloy` |
| Holistics | HolisticsAdapter | `.aml` |
| ThoughtSpot | ThoughtSpotAdapter | `.tml` |
| Snowflake Cortex | SnowflakeAdapter | `.yml` (with `tables:` + `base_table:`) |
| Superset | SupersetAdapter | `.yml` (with `table_name:` + `columns:`) |
| GoodData | GoodDataAdapter | `.json` |
| Omni | OmniAdapter | `.yml` |
| BSL | BSLAdapter | `.yml` |
| OSI | OSIAdapter | `.yml` |
| AtScale SML | AtScaleSMLAdapter | `.yml` |
