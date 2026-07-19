# Native Sidemantic Format

This document defines the native Sidemantic project format consumed by the Rust native runtime and the Python native adapter.

The native format has two source forms:

- YAML semantic project files.
- SQL semantic definition files.

The native format is the runtime contract. External formats such as LookML, MetricFlow, Hex, Rill, Malloy, Omni, Superset, GoodData, Snowflake Cortex, ThoughtSpot, Holistics, Tableau, AtScale SML, BSL, Yardstick, and Graphene GSQL should be converted into this format by Python importers before they are expected to run through the Rust native runtime.

## Rust Loader Scope

The Rust runtime and Rust CLI directory loader intentionally have a smaller direct
input surface than Python:

- `.yml` / `.yaml`: native Sidemantic YAML or Cube YAML.
- `.sql`: native Sidemantic SQL definition files.

They do not auto-detect LookML, MetricFlow/dbt manifests, Hex, Rill, Malloy,
Omni, Superset, GoodData, Snowflake Cortex, ThoughtSpot, Holistics, Tableau,
AtScale SML, BSL, Yardstick, or other external source formats. Convert those
formats through the Python CLI/API first, then load the exported native YAML/SQL
with the Rust runtime.

## Versioning

Current native format version: `1`.

New native YAML exports must include:

```yaml
version: 1
```

Existing files without `version` are treated as version `1` for compatibility.

Unsupported versions fail early. A file with `version: 2` must not be silently interpreted as version `1`.

## Top-Level YAML Shape

```yaml
version: 1

parameters:
  - name: status
    type: string
    default_value: paid

models:
  - name: orders
    table: public.orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: total_revenue
        agg: sum
        sql: amount

metrics:
  - name: revenue_per_order
    type: ratio
    numerator: orders.total_revenue
    denominator: orders.order_count
```

Top-level sections:

| Field | Required | Notes |
|---|---:|---|
| `version` | No | Missing means `1`. New exports should include it. |
| `models` | No | List of model definitions. Most useful projects define at least one model. |
| `metrics` | No | Graph-level metrics. Rust assigns these to exactly one owning model when possible. |
| `parameters` | No | Graph-level parameters for templates and query-time substitution. |

Top-level metrics are graph-scoped in the Python runtime. The Rust runtime does not
store a separate graph-metric namespace at execution time; it assigns each top-level
metric to one owning model by resolving explicit model references, metric dependencies,
entity dimensions, or a single-model project fallback. If Rust cannot infer exactly
one owner, loading fails. Portable native files should therefore make top-level metric
dependencies explicit, for example `orders.total_revenue` rather than `total_revenue`
when multiple models define the same local metric name. Dotted top-level metric names
are allowed and are resolved by exact metric name before `model.metric` parsing.

Top-level parameters remain graph-scoped in both runtimes. Query APIs interpolate
parameter values before SQL compilation.

## Models

Models describe physical or logical query sources.

```yaml
models:
  - name: orders
    table: public.orders
    primary_key: order_id
    default_time_dimension: created_at
    default_grain: day
    freshness:
      watermark: updated_at
      ttl_seconds: 3600
```

At least one of `table`, `sql`, or `source_uri` should be present unless the model extends another model that provides one.

| Field | Required | Notes |
|---|---:|---|
| `name` | Yes | Unique model name. |
| `table` | Conditional | Physical table name. |
| `sql` | Conditional | SQL subquery used as the model source. |
| `source_uri` | Conditional | Source URI for external data discovery or file-backed sources. Native loaders preserve it; execution is adapter/runtime-specific until a concrete backend maps URI sources. |
| `extends` | No | Parent model name. Child fields override or extend parent fields. |
| `primary_key` | No | Single primary key string or list of columns. Omitted means the model's entity key is unknown. |
| `primary_key_columns` | No | Explicit list form for primary keys. |
| `unique_keys` | No | List of unique key column lists. |
| `description` | No | Human-readable description. |
| `dimensions` | No | List of dimensions. |
| `metrics` | No | List of model-local metrics. |
| `relationships` | No | List of relationships from this model to other models. |
| `segments` | No | List of model-local named filters. |
| `pre_aggregations` | No | List of pre-aggregation definitions. |
| `default_time_dimension` | No | Time dimension to add by default when the query needs time grouping. |
| `default_grain` | No | Default time grain for the default time dimension. |
| `freshness` | No | Source freshness policy for live chart/dashboard runtimes. |
| `security` | No | Row/access `SecurityPolicy` (`access` gate + `row_filters`) enforced per query when `user_attributes` are supplied. See [Security](security.md). |
| `auto_dimensions` | No | Python auto-discovery flag. Rust accepts `false` for compatibility and rejects `true` because it does not perform schema discovery. |

Canonical CLI-authored files should use `metrics` and `sql`. The native loaders
also accept compatibility input aliases: model-level `measures` for `metrics`,
dimension/metric `expr` for `sql`, and metric `measure` for `sql`. Exports use
canonical field names.

Single-column primary key:

```yaml
primary_key: order_id
```

Composite primary key:

```yaml
primary_key: [order_id, line_item_id]
```

Equivalent explicit list form:

```yaml
primary_key_columns:
  - order_id
  - line_item_id
```

### Model Freshness

Model-level `freshness` describes how live chart and dashboard runtimes should
decide whether source-backed results are fresh, stale, or unknown.

```yaml
freshness:
  watermark: updated_at
  ttl_seconds: 3600
```

Fields:

| Field | Required | Notes |
|---|---:|---|
| `watermark` | Conditional | Dimension or source column whose `MAX` value represents source freshness. Prefer this for normal semantic-model usage. |
| `sql` | Conditional | Advanced SQL query that returns one scalar freshness marker. Use only when a source freshness marker cannot be expressed as a model dimension or physical column. |
| `ttl_seconds` | No | Positive integer maximum age before data is stale. Native input also accepts compatibility alias `ttlSeconds`; exports use `ttl_seconds`. |

`watermark` and `sql` are mutually exclusive. A TTL without a real source
watermark is accepted as configuration, but live runtimes report freshness as
unknown when there is no real `data_as_of` timestamp to compare.

## Dimensions

Dimensions are groupable attributes or reusable SQL expressions.

```yaml
dimensions:
  - name: created_at
    type: time
    sql: created_at
    granularity: day
    supported_granularities: [day, week, month, quarter, year]
```

| Field | Required | Notes |
|---|---:|---|
| `name` | Yes | Unique within model. |
| `type` | No | `categorical`, `time`, `boolean`, or `numeric`. Defaults to `categorical`. |
| `sql` | No | SQL expression. If omitted, the dimension name is used. |
| `granularity` | No | Default grain for time dimensions. |
| `supported_granularities` | No | Allowed grains for time dimensions. |
| `description` | No | Human-readable description. |
| `label` | No | Display label. |
| `format` | No | Display format string. |
| `value_format_name` | No | Named display format. |
| `parent` | No | Parent dimension for hierarchy navigation. |
| `window` | No | Window expression. |
| `public` | No | Visibility flag (default true). When a layer is built with `enforce_visibility=True`, a `public: false` dimension cannot be projected, filtered, or ordered on. |
| `uri` | No | Rendering hint (default false): UIs may render this dimension's values as links. Metadata only — no effect on generated SQL. |

The `{model}` placeholder can be used in SQL expressions that need the generated table alias:

```yaml
sql: "{model}.status"
```

## Metrics

Metrics describe aggregations or derived semantic measures.

### Simple Metrics

```yaml
metrics:
  - name: total_revenue
    type: simple
    agg: sum
    sql: amount
```

`type: simple` is optional when `agg` is present.

Supported aggregations in the native contract:

- `count`
- `count_distinct`
- `sum`
- `avg`
- `min`
- `max`
- `median`

Additional aggregation names must be added deliberately to the Rust enum and tested before they are considered part of the native contract.

### Derived Metrics

```yaml
metrics:
  - name: net_revenue
    type: derived
    sql: total_revenue - refunds
```

Derived metrics can reference other metrics by name or by qualified reference.

### Ratio Metrics

```yaml
metrics:
  - name: revenue_per_order
    type: ratio
    numerator: total_revenue
    denominator: order_count
```

The runtime is responsible for safe divide behavior.

### Cumulative Metrics

```yaml
metrics:
  - name: cumulative_revenue
    type: cumulative
    base_metric: total_revenue
    window: unbounded
```

Supported cumulative behavior must be validated before SQL generation. Queries missing a usable time dimension should fail before producing SQL.

### Time Comparison Metrics

```yaml
metrics:
  - name: revenue_yoy
    type: time_comparison
    base_metric: total_revenue
    comparison_type: yoy
    calculation: percent_change
    time_offset: "1 year"
```

Supported comparison types:

- `yoy`
- `mom`
- `wow`
- `dod`
- `qoq`
- `prior_period`

Supported calculations:

- `difference`
- `percent_change`
- `ratio`

### Conversion Metrics

```yaml
metrics:
  - name: signup_to_purchase
    type: conversion
    entity: user_id
    base_event: "event_type = 'signup'"
    conversion_event: "event_type = 'purchase'"
    conversion_window: "7 days"
```

Initial native-runtime rule:

- One conversion metric per query.
- Conversion metrics do not mix with regular metrics.

### Retention Metrics

```yaml
metrics:
  - name: signup_retention
    type: retention
    entity: user_id
    cohort_event: "event_type = 'signup'"
    activity_event: "event_type = 'active'"
    periods: 7
    retention_granularity: day
```

Initial native-runtime rule:

- One retention metric per query.
- Retention metrics do not mix with regular metrics.

### Cohort Metrics

```yaml
metrics:
  - name: multi_platform_users
    type: cohort
    entity: user_id
    inner_metrics:
      - name: platform_count
        agg: count_distinct
        sql: platform
    having: platform_count >= 2
    agg: count
```

Initial native-runtime rule:

- One cohort metric per query.
- Cohort metrics do not mix with regular metrics.

### Common Metric Fields

| Field | Notes |
|---|---|
| `name` | Required. Unique within owner scope. |
| `type` | `simple`, `derived`, `ratio`, `cumulative`, `time_comparison`, `conversion`, `retention`, or `cohort`. |
| `agg` | Aggregation for simple metrics and some cohort inner metrics. |
| `sql` | SQL expression or derived expression. |
| `filters` | List of SQL predicates applied to the metric. |
| `fill_nulls_with` | Value used to fill null metric results. |
| `description` | Human-readable description. |
| `label` | Display label. |
| `format` | Display format string. |
| `value_format_name` | Named display format. |
| `drill_fields` | Suggested drill fields. |
| `non_additive_dimension` | Semi-additive time dimension. The measure is aggregated over only the last (or first) snapshot per group — implemented with a `QUALIFY` on QUALIFY-capable engines (DuckDB, Snowflake, BigQuery, Databricks, Spark, ClickHouse). Raises `UnsupportedMetricError` on other dialects or when combined with a fan-out (symmetric-aggregate) join, unless `SemanticLayer(allow_non_additive_unsafe=True)`. |
| `non_additive_window` | `"max"` (default, last value) or `"min"` (first value) for `non_additive_dimension`. |
| `non_additive_window_groupings` | Dimensions the semi-additive snapshot is taken per (MetricFlow `window_groupings`, e.g. balance-per-user). When set, the last/first snapshot partitions by these regardless of the query's grouping; when unset, it partitions by the query's own non-time grouping dimensions. |

## Relationships

Relationships define how models join.

```yaml
relationships:
  - name: customers
    type: many_to_one
    foreign_key: customer_id
    primary_key: customer_id
```

| Field | Required | Notes |
|---|---:|---|
| `name` | Yes | Target model name. |
| `type` | No | Defaults to `many_to_one`. |
| `foreign_key` | Conditional | Single column or list of source columns. |
| `foreign_key_columns` | Conditional | Explicit source-column list. |
| `primary_key` | Conditional | Single column or list of target columns. |
| `primary_key_columns` | Conditional | Explicit target-column list. |
| `through` | For many-to-many | Junction model. |
| `through_foreign_key` | For many-to-many | Source-to-through key. |
| `through_foreign_key_columns` | For many-to-many | Explicit source-to-through key columns. |
| `related_foreign_key` | For many-to-many | Through-to-target key. |
| `related_foreign_key_columns` | For many-to-many | Explicit through-to-target key columns. |
| `sql` | No | Custom join SQL using `{from}` and `{to}` runtime placeholders. |

Join columns are never inferred from names. Every non-cross relationship without
custom `sql` must declare `foreign_key`. A `many_to_one` relationship may omit
`primary_key` only when the target model declares its `primary_key`; a
`one_to_many` or `one_to_one` relationship may omit it only when the source model
declares its `primary_key`. Use relationship `primary_key` for a scoped alternate
unique key, or model `unique_keys` when the uniqueness applies model-wide.

Cardinality is a data contract: the key on the "one" side must be unique. Run
warehouse validation with `--check-keys` to verify the declared contract against
data. Structural validation rejects missing keys and mismatched composite-key
arity before SQL compilation.

See [Explicit model keys and warehouse validation](model-key-migration.md) for
the compatibility and migration path from implicit keys.

When `sql` is present, Python and Rust use it instead of the FK/PK-generated
predicate. `{from}` is replaced with the source model's runtime alias and `{to}`
with the target model's runtime alias. Reverse graph traversal swaps the
placeholders automatically.

Relationship types:

- `many_to_one`
- `one_to_one`
- `one_to_many`
- `many_to_many`

Composite relationship:

```yaml
relationships:
  - name: order_items
    type: many_to_one
    foreign_key: [order_id, line_item_id]
    primary_key: [order_id, line_item_id]
```

Many-to-many relationship:

```yaml
relationships:
  - name: products
    type: many_to_many
    through: order_items
    through_foreign_key: order_id
    related_foreign_key: product_id
    primary_key: product_id
```

## Segments

Segments are named model-local filters.

```yaml
segments:
  - name: completed
    sql: "{model}.status = 'completed'"
    description: Completed orders
```

| Field | Required | Notes |
|---|---:|---|
| `name` | Yes | Unique within model. |
| `sql` | Yes | SQL predicate. |
| `description` | No | Human-readable description. |
| `public` | No | Visibility flag. Defaults to true. |

## Parameters

Parameters are graph-level values that can be used in templates and filters.

```yaml
parameters:
  - name: status
    type: string
    default_value: paid
    allowed_values: [paid, refunded, pending]
```

Supported types:

- `string`
- `number`
- `date`
- `unquoted`
- `yesno`

| Field | Required | Notes |
|---|---:|---|
| `name` | Yes | Unique parameter name. |
| `type` | Yes | Parameter type. |
| `description` | No | Human-readable description. |
| `label` | No | Display label. |
| `default_value` | No | Default value. |
| `allowed_values` | No | Allowed values. |
| `default_to_today` | No | Date helper flag. |

## Pre-Aggregations

Pre-aggregations describe materialized rollups.

```yaml
pre_aggregations:
  - name: daily_revenue
    type: rollup
    measures: [total_revenue]
    dimensions: [status]
    time_dimension: created_at
    granularity: day
    refresh_key:
      every: 1 hour
```

| Field | Required | Notes |
|---|---:|---|
| `name` | Yes | Unique within model. |
| `type` | No | Defaults to `rollup`. |
| `measures` | No | Metric names. |
| `dimensions` | No | Dimension names. |
| `time_dimension` | No | Time dimension name. |
| `granularity` | No | Time grain. |
| `partition_granularity` | No | Partition grain. |
| `build_range_start` | No | Build range SQL or expression. |
| `build_range_end` | No | Build range SQL or expression. |
| `scheduled_refresh` | No | Defaults to true. |
| `refresh_key` | No | Refresh settings. |
| `indexes` | No | Index definitions. |

Supported pre-aggregation types:

- `rollup`
- `original_sql`
- `rollup_join`
- `lambda`

Refresh key:

```yaml
refresh_key:
  every: 1 hour
  sql: "select max(updated_at) from orders"
  incremental: true
  update_window: 7 days
```

Index:

```yaml
indexes:
  - name: by_status
    columns: [status]
    type: regular
```

## SQL Definition Files

SQL definition files are the second native source form. They can define models and graph objects without YAML.

Example:

```sql
MODEL (
  name orders,
  table public.orders,
  primary_key order_id
);

DIMENSION (
  model orders,
  name status,
  type categorical,
  sql status
);

METRIC (
  model orders,
  name total_revenue,
  agg sum,
  sql amount
);
```

SQL files may also use YAML frontmatter for the model and SQL definitions for metrics, segments, parameters, and pre-aggregations.

```sql
---
version: 1
name: orders
table: public.orders
primary_key: order_id
---

METRIC (
  name total_revenue,
  agg sum,
  sql amount
);
```

Versioning for SQL frontmatter follows the same YAML contract when a version field is present. Missing frontmatter version means version `1`; unsupported frontmatter versions fail early; `version` is removed before model metadata parsing.

## Environment Variables

Native YAML supports environment substitution:

```yaml
table: ${ORDERS_TABLE:-public.orders}
```

Supported forms:

- `${ENV_VAR}`
- `${ENV_VAR:-default}`
- `$ENV_VAR`

Missing variables without defaults are preserved for later handling.

## Unknown Fields

Native format version `1` is a strict runtime contract for native YAML and native SQL frontmatter. Rust rejects unknown fields in documented native objects instead of silently dropping them.

- New native fields must be documented here before being treated as supported.
- Known enum-like fields, including dimension `type`, dimension `granularity`, dimension `supported_granularities`, model `default_grain`, metric `type`, metric `agg`, metric `retention_granularity`, relationship `type`, pre-aggregation `type`, pre-aggregation `granularity`, and pre-aggregation `partition_granularity`, reject unsupported values.
- Source-adapter-specific fields belong under `metadata` or `meta` rather than new top-level native fields.
- Python importers should not leak external adapter concepts into native YAML unless they are part of this document.

## Compatibility Rules

1. Missing `version` means version `1`.
2. New exports include `version: 1`.
3. Unsupported versions fail early.
4. Native YAML and native SQL are the only direct Rust runtime input contracts.
5. External formats should be normalized to native YAML/SQL before Rust runtime use.
6. Rust and Python may generate different SQL text, but shared fixtures should prove equivalent validation and result behavior.

## Minimal Valid Project

```yaml
version: 1
models:
  - name: orders
    table: orders
```

## Recommended Project Shape

```yaml
version: 1

parameters:
  - name: start_date
    type: date

models:
  - name: orders
    table: public.orders
    primary_key: order_id
    default_time_dimension: created_at
    default_grain: day
    dimensions:
      - name: order_id
        type: categorical
      - name: customer_id
        type: categorical
      - name: status
        type: categorical
      - name: created_at
        type: time
        sql: created_at
        supported_granularities: [day, week, month, quarter, year]
    metrics:
      - name: order_count
        agg: count
      - name: total_revenue
        agg: sum
        sql: amount
      - name: average_order_value
        type: ratio
        numerator: total_revenue
        denominator: order_count
    segments:
      - name: paid
        sql: "{model}.status = 'paid'"
    relationships:
      - name: customers
        type: many_to_one
        foreign_key: customer_id
        primary_key: customer_id

  - name: customers
    table: public.customers
    primary_key: customer_id
    dimensions:
      - name: customer_id
        type: categorical
      - name: country
        type: categorical

metrics:
  - name: revenue_per_customer
    type: ratio
    numerator: orders.total_revenue
    denominator: customers.customer_count
```
