# Sidemantic YAML Schema Reference

Complete field-by-field reference for the native YAML format.

## Top-Level Structure

```yaml
connection:       # Optional: database connection
models:           # List of model definitions
metrics:          # Optional: graph-level metrics
```

All three sections are optional. Environment variable substitution is supported: `${VAR}`, `${VAR:-default}`, `$VAR`.

## Connection

String form: `connection: "duckdb:///path/to/db.duckdb"`

Dictionary form:

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `type` | No | `"duckdb"` | `duckdb`, `postgres`, `bigquery`, `snowflake`, `clickhouse`, `databricks`, `spark`, `adbc` |

### DuckDB

| Field | Default | Description |
|-------|---------|-------------|
| `path` | `":memory:"` | Database file path. `"md:dbname"` for MotherDuck |

### PostgreSQL

| Field | Default |
|-------|---------|
| `host` | `"localhost"` |
| `port` | `5432` |
| `database` | `"postgres"` |
| `user` | (none) |
| `password` | (none) |

### BigQuery

| Field | Required |
|-------|----------|
| `project` | Yes |
| `dataset` | No (default: `""`) |

### Snowflake

| Field | Required |
|-------|----------|
| `account` | Yes |
| `user`, `password`, `database`, `schema` | No |

### ClickHouse

| Field | Default |
|-------|---------|
| `host` | `"localhost"` |
| `port` | `8123` |
| `database` | `"default"` |
| `user`, `password` | (none) |

### Databricks

| Field | Required |
|-------|----------|
| `server` (or `host`) | Yes |
| `http_path` | Yes |
| `token` | No |

### Spark

| Field | Default |
|-------|---------|
| `host` | `"localhost"` |
| `port` | `10000` |
| `database` | `"default"` |

### ADBC

| Field | Required |
|-------|----------|
| `driver` | Yes (e.g., `"postgresql"`, `"snowflake"`) |
| `uri` | No |
| (other keys) | Passed as query parameters |

## Model

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | string | Yes | -- | Unique model identifier |
| `table` | string | No* | -- | Physical table (e.g., `"public.orders"`) |
| `sql` | string | No* | -- | SQL expression for derived tables |
| `source_uri` | string | No | -- | Remote data source URI (DuckDB) |
| `description` | string | No | -- | Human-readable description |
| `extends` | string | No | -- | Parent model name for inheritance |
| `primary_key` | string or list | No | `"id"` | Primary key column(s) |
| `unique_keys` | list[list[string]] | No | -- | Unique constraints (Pydantic only, not in YAML adapter) |
| `default_time_dimension` | string | No | -- | Auto-included time dimension for queries |
| `default_grain` | string | No | -- | Default granularity: `hour`, `day`, `week`, `month`, `quarter`, `year` |
| `metadata` | dict | No | -- | Adapter-specific metadata (round-trips through YAML) |
| `meta` | dict | No | -- | Arbitrary metadata (Pydantic only, not in YAML adapter) |
| `relationships` | list | No | `[]` | Relationships to other models |
| `dimensions` | list | No | `[]` | Dimension definitions |
| `metrics` | list | No | `[]` | Metric definitions. `measures` accepted as alias |
| `segments` | list | No | `[]` | Named filter definitions |
| `pre_aggregations` | list | No | `[]` | Pre-aggregation definitions |

*One of `table` or `sql` must be provided.

## Dimension

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | string | Yes | -- | Unique within model. Referenced as `model.name` |
| `type` | string | Yes | `"categorical"` (YAML) | `categorical`, `time`, `boolean`, `numeric` |
| `sql` | string | No | column matching `name` | SQL expression. `expr` accepted as alias |
| `granularity` | string | No | -- | Required for time dims: `second`, `minute`, `hour`, `day`, `week`, `month`, `quarter`, `year` |
| `supported_granularities` | list[string] | No | all | Override available granularities |
| `description` | string | No | -- | Human-readable description |
| `label` | string | No | -- | Display label |
| `format` | string | No | -- | Display format (e.g., `"$#,##0.00"`) |
| `value_format_name` | string | No | -- | Named format (e.g., `"usd"`, `"percent"`) |
| `parent` | string | No | -- | Parent dimension for drill hierarchies |
| `metadata` | dict | No | -- | Adapter-specific metadata |
| `meta` | dict | No | -- | Arbitrary metadata for extensions |

### Type guidance

- **categorical**: Strings, enums, IDs for grouping. Default when `type` is omitted in YAML.
- **time**: Dates/timestamps. Queried as `model.dim__granularity` (e.g., `orders.order_date__month`).
- **boolean**: Computed true/false. Use `sql` for the expression (e.g., `"amount > 100"`).
- **numeric**: Numbers for grouping (not aggregation). Use `sql` for computed buckets.

### Hierarchy example

```yaml
- name: country
  type: categorical
- name: state
  type: categorical
  parent: country
- name: city
  type: categorical
  parent: state
```

Enables: `model.get_hierarchy_path("city")` returns `["country", "state", "city"]`.

## Metric

### Common Fields (all metric types)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Unique within scope (model or graph) |
| `extends` | string | No | Parent metric for inheritance |
| `description` | string | No | Human-readable description |
| `label` | string | No | Display label |
| `format` | string | No | Display format (e.g., `"$#,##0.00"`) |
| `value_format_name` | string | No | Named format |
| `filters` | list[string] | No | WHERE clauses for this metric |
| `fill_nulls_with` | any | No | Default for NULL results |
| `drill_fields` | list[string] | No | Fields for drill-down |
| `non_additive_dimension` | string | No | Dimension across which this metric cannot be summed |
| `metadata` | dict | No | Adapter-specific metadata |
| `meta` | dict | No | Arbitrary metadata for extensions |

### Simple Aggregation (no `type` field)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `agg` | string | Yes* | `sum`, `count`, `count_distinct`, `avg`, `min`, `max`, `median` |
| `sql` | string | No | Expression to aggregate. `expr` accepted as alias. Defaults to column matching `name`. For `count` without `sql`, generates `COUNT(*)` |

*Not required if `sql` contains a recognized aggregation (e.g., `sql: "SUM(amount)"` auto-extracts `agg: sum, sql: amount`).

Current model-level validation aligns with this set (`sum`, `count`, `count_distinct`, `avg`, `min`, `max`, `median`).

```yaml
- name: revenue
  agg: sum
  sql: order_amount

- name: order_count
  agg: count

- name: unique_customers
  agg: count_distinct
  sql: customer_id
```

### Ratio (`type: ratio`)

| Field | Required | Description |
|-------|----------|-------------|
| `numerator` | Yes | Measure reference (e.g., `"orders.completed_revenue"`) |
| `denominator` | Yes | Measure reference |
| `offset_window` | No | Time offset for denominator (e.g., `"1 month"`) |

```yaml
- name: conversion_rate
  type: ratio
  numerator: orders.completed_revenue
  denominator: orders.revenue
```

### Derived (`type: derived`)

| Field | Required | Description |
|-------|----------|-------------|
| `sql` | Yes | Formula expression referencing other metrics. `expr` and `measure` accepted as aliases |

```yaml
- name: revenue_per_order
  type: derived
  sql: "total_revenue / order_count"
```

### Cumulative (`type: cumulative`)

| Field | Required | Description |
|-------|----------|-------------|
| `sql` | Yes* | Base aggregation (e.g., `"SUM(amount)"`). Auto-parsed |
| `window_expression` | Yes* | Raw SQL window function (alternative to `sql`) |
| `window` | No | Time window (e.g., `"7 days"`) |
| `grain_to_date` | No | Period reset: `day`, `week`, `month`, `quarter`, `year` |
| `window_frame` | No | Raw SQL window frame clause |
| `window_order` | No | ORDER BY column. Defaults to model's `default_time_dimension` |

*One of `sql` or `window_expression` required.

```yaml
# Rolling 7-day sum
- name: rolling_revenue
  type: cumulative
  sql: "SUM(amount)"
  window: "7 days"

# Month-to-date
- name: mtd_revenue
  type: cumulative
  sql: "SUM(amount)"
  grain_to_date: month
```

### Time Comparison (`type: time_comparison`)

| Field | Required | Description |
|-------|----------|-------------|
| `base_metric` | Yes | Metric to compare |
| `comparison_type` | No | `yoy`, `mom`, `wow`, `dod`, `qoq`, `prior_period` |
| `time_offset` | No | Custom offset (e.g., `"1 month"`, `"7 days"`) |
| `calculation` | No (default: `percent_change`) | `difference`, `percent_change`, `ratio` |

```yaml
- name: revenue_yoy
  type: time_comparison
  base_metric: orders.revenue
  comparison_type: yoy
  calculation: percent_change
```

### Conversion (`type: conversion`)

| Field | Required | Description |
|-------|----------|-------------|
| `entity` | Yes | Entity to track (e.g., `"user_id"`) |
| `base_event` | Yes | Starting event filter (SQL expression) |
| `conversion_event` | Yes | Target event filter (SQL expression) |
| `conversion_window` | No | Max time between events (e.g., `"7 days"`) |

```yaml
- name: signup_to_purchase
  type: conversion
  entity: user_id
  base_event: "event_type = 'signup'"
  conversion_event: "event_type = 'purchase'"
  conversion_window: "30 days"
```

## Relationship

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | string | Yes | -- | Name of the related model |
| `type` | string | Yes | -- | `many_to_one`, `one_to_one`, `one_to_many`, `many_to_many` |
| `foreign_key` | string or list | No | `{name}_id` (many_to_one) | FK column(s) in this model |
| `primary_key` | string or list | No | `"id"` | PK in related model |
| `through` | string | No | -- | Junction model for many_to_many |
| `through_foreign_key` | string | No | -- | FK in junction pointing to this model |
| `related_foreign_key` | string | No | -- | FK in junction pointing to related model |

```yaml
# many_to_one (most common)
- name: customers
  type: many_to_one
  foreign_key: customer_id

# many_to_many through junction
- name: courses
  type: many_to_many
  through: enrollments
  through_foreign_key: student_id
  related_foreign_key: course_id

# Composite keys
- name: line_items
  type: one_to_many
  foreign_key: [order_id, line_number]
  primary_key: [order_id, line_number]
```

## Segment

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | string | Yes | -- | Unique within model |
| `sql` | string | Yes | -- | SQL WHERE clause. Use `{model}` placeholder |
| `description` | string | No | -- | Human-readable description |
| `public` | boolean | No | `true` | Whether visible in API/UI |

Segments are model-scoped and must be nested under a model's `segments:` list.

```yaml
models:
  - name: orders
    table: orders
    segments:
      - name: completed
        sql: "status = 'completed'"
      - name: internal_only
        sql: "is_test = false"
        public: false
```

## Pre-Aggregation

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | string | Yes | -- | Unique within model |
| `type` | string | No | `"rollup"` | `rollup`, `original_sql`, `rollup_join`, `lambda` |
| `measures` | list[string] | No | -- | Metrics to pre-aggregate |
| `dimensions` | list[string] | No | -- | Dimensions to group by |
| `time_dimension` | string | No | -- | Time dimension for grouping |
| `granularity` | string | No | -- | `hour`, `day`, `week`, `month`, `quarter`, `year` |
| `partition_granularity` | string | No | -- | Partition size for incremental refresh |
| `refresh_key` | object | No | -- | Refresh strategy (see below) |
| `scheduled_refresh` | boolean | No | `true` | Enable scheduled refresh |
| `indexes` | list | No | -- | Index definitions |
| `build_range_start` | string | No | -- | SQL for start of range |
| `build_range_end` | string | No | -- | SQL for end of range |

### RefreshKey

| Field | Type | Description |
|-------|------|-------------|
| `every` | string | Interval (e.g., `"1 hour"`) |
| `sql` | string | SQL that triggers refresh on value change |
| `incremental` | boolean | Use incremental refresh (default: false) |
| `update_window` | string | Window for incremental refresh (e.g., `"7 day"`) |

## Graph-Level Metrics

Top-level `metrics:` section holds metrics that span models or compose model-level measures.

Key differences from model-level metrics:
1. Referenced by name alone (e.g., `total_revenue`), not `model.metric`
2. Typically use `type` for complex calculations
3. `sql` references model measures using `model.measure` syntax
4. `measure` accepted as alias for `sql` in this context
5. Do not use `agg` (aggregation happens at the model level)

```yaml
metrics:
  - name: total_revenue
    sql: orders.revenue

  - name: aov
    type: ratio
    numerator: orders.revenue
    denominator: orders.order_count

  - name: net_revenue
    type: derived
    sql: "revenue - refund_amount"
```

## Field Aliases

| Canonical | Alias(es) | Context |
|-----------|-----------|---------|
| `sql` | `expr` | Dimensions and Metrics |
| `sql` | `measure` | Graph-level Metrics only |
| `metrics` | `measures` | Model-level list |

## Reference Syntax in Queries

| Reference | Format | Example |
|-----------|--------|---------|
| Dimension | `model.dimension` | `orders.status` |
| Time dimension with granularity | `model.dimension__granularity` | `orders.order_date__month` |
| Model-level metric | `model.metric` | `orders.revenue` |
| Graph-level metric | `metric_name` | `total_revenue` |
| Segment | `model.segment` | `orders.completed` |
| Filter | SQL expression | `orders.status = 'completed'` |
