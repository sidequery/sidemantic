# Validation and Error Handling

Every validation rule, error message, and common pitfall in Sidemantic model definitions.

## Pre-Flight Checklist

Verify before loading any model:

1. Every model has `table` or `sql` (not neither)
2. Every model has a `primary_key` (defaults to `"id"`)
3. Every dimension has a valid `type`: `categorical`, `time`, `boolean`, `numeric`
4. Every `time` dimension has `granularity` set
5. Every simple metric has a valid `agg` or SQL containing an aggregation function
6. Complex metrics have their required fields (see metric rules below)
7. Relationship `name` values point to models that exist
8. No duplicate model or metric names
9. Dimension references in queries use `model.dimension` format
10. Cross-model queries have a relationship path connecting involved models

## Model Validation

### primary_key is required

Default is `"id"`. Only fails if explicitly set to empty.

**Error:** `Model 'orders' must have a primary_key defined`
**Fix:** `primary_key: order_id`

### table or sql required

Every model needs one. Having both is allowed (sql takes precedence).

**Error:** `Model 'orders' must have either 'table' or 'sql' defined`
**Fix:** Add `table: public.orders` or `sql: "SELECT * FROM ..."`

### Duplicate model names

Different definitions with the same name raise an error. Re-adding the exact same instance or identical definition silently succeeds (idempotency).

**Error:** `Model orders already exists`

## Dimension Validation

### type must be valid

**Error:** `Input should be 'categorical', 'time', 'boolean' or 'numeric'`
**Fix:** Use one of the four valid types. Common mistake: `type: string` should be `type: categorical`.

### time dimensions need granularity

**Error:** `Model 'orders': time dimension 'order_date' should have a granularity defined`
**Fix:** Add `granularity: day` (or `second`, `minute`, `hour`, `week`, `month`, `quarter`, `year`)

### sql vs expr conflict

Both `sql` and `expr` are accepted as aliases. If both provided with different values:

**Error:** `Cannot specify both sql='order_status' and expr='status_code' with different values`
**Fix:** Use only one.

### Granularity on non-time dimension

**Error:** `Cannot apply granularity to non-time dimension status`
**Fix:** Only use `__month`, `__year`, etc. on `type: time` dimensions.

### Unsupported granularity

**Error:** `Granularity weekly not supported for order_date. Supported: ['day', 'week', 'month', 'quarter', 'year']`
**Fix:** Use a valid granularity value.

## Metric Validation

### Simple metrics need valid aggregation

For metrics without a complex `type`, `agg` must be one of: `sum`, `count`, `count_distinct`, `avg`, `min`, `max`, `median`.

Exception: if `sql` contains a recognized function (e.g., `SUM(amount)`), agg is auto-extracted.

**Error:** `Model 'orders': measure 'revenue' has invalid aggregation 'None'. Must be one of: sum, count, count_distinct, avg, min, max, median`
**Fix:** Add `agg: sum` or use `sql: "SUM(amount)"`

Note: `stddev`, `stddev_pop`, `variance`, `variance_pop` are accepted by Pydantic (Phase 1) but may fail `validate_model()` (Phase 2). See "Two-Phase Validation" below.

### Auto-parsing behavior

`sql: "SUM(amount)"` without `agg` auto-extracts to `agg: sum, sql: amount`. Only simple top-level aggregations are parsed. `SUM(x) / SUM(y)` is left as-is.

### ratio requires numerator and denominator

**Error:** `ratio metric requires 'numerator' field` / `ratio metric requires 'denominator' field`

If references use `model.measure` syntax, both the model and measure must exist:

**Error:** `Ratio measure 'margin': numerator model 'orders' not found`

### derived requires sql

**Error:** `derived metric requires 'sql' field`

Cannot self-reference:
**Error:** `Derived measure 'metric_a' cannot reference itself`

Cannot have circular dependencies:
**Error:** `Derived measure 'metric_a' has circular dependency: metric_a -> metric_b -> metric_a`

### cumulative requires sql or window_expression

**Error:** `cumulative metric requires 'sql' or 'window_expression' field`

### time_comparison requires base_metric

**Error:** `time_comparison metric requires 'base_metric' field`

### conversion requires entity, base_event, conversion_event

**Errors:**
- `conversion metric requires 'entity' field`
- `conversion metric requires 'base_event' field`
- `conversion metric requires 'conversion_event' field`

### Invalid metric type

**Error:** `Input should be 'ratio', 'derived', 'cumulative', 'time_comparison' or 'conversion'`

There is no `type: simple`. Simple aggregations omit `type` and use `agg`.

### Duplicate metric names

**Error:** `Measure revenue already exists`

## Relationship Validation

### type must be valid

Must be one of: `many_to_one`, `one_to_one`, `one_to_many`, `many_to_many`.

### foreign_key defaults

- `many_to_one`: defaults to `{name}_id` (e.g., relationship named `customers` defaults to `customers_id`)
- `one_to_one` / `one_to_many`: defaults to `id`
- `many_to_many`: specify `through`, `through_foreign_key`, `related_foreign_key`

### Related model must exist

Relationships to missing models are silently skipped during adjacency building. Queries across unresolved relationships fail at query time:

**Error:** `No join path found between orders and customers`
**Fix:** Ensure both models are added and the relationship's `name` matches the target model's `name` exactly.

## Query Validation

### Metric reference format

- Qualified: `model.metric_name` (model-level metric)
- Unqualified: `metric_name` (graph-level metric)

### Dimension reference format

Must be qualified: `model.dimension_name`.

**Error:** `Dimension reference 'status' must be in 'model.dimension' format`

### Time granularity in queries

Use double-underscore: `model.dimension__granularity`.

Valid at query time: `hour`, `day`, `week`, `month`, `quarter`, `year`.

Note: `second` and `minute` are valid for dimension definition but not in the query-time validation list.

### Join path validation

When a query involves multiple models, the graph must have a relationship path connecting them (BFS).

**Error:** `No join path found between models 'orders' and 'products'. Add relationships to enable joining these models.`

## YAML Parsing Pitfalls

### Missing model name

Models without `name` are silently skipped.

### Missing metric name

Metrics without `name` are silently skipped.

### metrics vs measures field

Model-level: both `metrics` and `measures` work. Graph-level (top-level): only `metrics` works.

### Type defaults

Dimensions in YAML default to `type: categorical` when omitted. Primary key defaults to `"id"`.

### Indentation

Dimensions and metrics must be nested under their model, not at root level:

```yaml
# WRONG
models:
  - name: orders
    table: orders
dimensions:           # Root level, not under the model
  - name: status

# RIGHT
models:
  - name: orders
    table: orders
    dimensions:       # Nested under model
      - name: status
        type: categorical
```

### Quoting SQL expressions

SQL with YAML special characters (`:`, `#`, `{`, `>`) needs quotes:

```yaml
# WRONG
sql: first_name || ': ' || last_name

# RIGHT
sql: "first_name || ': ' || last_name"
```

### Boolean YAML values

Bare `yes`, `no`, `true`, `false` are parsed as booleans. Quote them if used as names:

```yaml
- name: "yes"
  type: categorical
```

## Error Quick Reference

| Error | Fix |
|-------|-----|
| `must have a primary_key defined` | Set `primary_key: column_name` |
| `must have either 'table' or 'sql' defined` | Add `table` or `sql` |
| `Model X already exists` | Use unique names |
| `Measure X already exists` | Use unique metric names |
| `has invalid type 'string'` | Use: categorical, time, boolean, numeric |
| `should have a granularity defined` | Add `granularity: day` |
| `Cannot specify both sql and expr` | Use only one |
| `has invalid aggregation 'None'` | Add `agg` or use `sql: SUM(col)` |
| `ratio metric requires 'numerator'` | Add `numerator: model.measure` |
| `derived metric requires 'sql'` | Add `sql: "expression"` |
| `cannot reference itself` | Remove self-reference |
| `has circular dependency` | Break the cycle |
| `cumulative requires 'sql' or 'window_expression'` | Add `sql` or `window_expression` |
| `time_comparison requires 'base_metric'` | Add `base_metric: metric_name` |
| `conversion requires 'entity'` | Add `entity: user_id` |
| `must be in 'model.dimension' format` | Use `model.dimension` |
| `No join path found` | Add relationships between models |
| `Cannot apply granularity to non-time dimension` | Only use `__month` on time dims |

## Two-Phase Validation

Sidemantic validates in two distinct phases. Understanding which phase produces an error is essential for debugging.

### Phase 1: Pydantic (at construction time)

When you call `Model(...)` or `Metric(...)`, Pydantic validators run immediately:

- `expr` alias converted to `sql`
- Aggregation functions auto-parsed from SQL (e.g., `sql: "SUM(amount)"` extracts `agg: sum, sql: amount`)
- Type-specific required fields checked: conversion needs `entity`/`base_event`/`conversion_event`, ratio needs `numerator`/`denominator`, derived needs `sql`, cumulative needs `sql` or `window_expression`, time_comparison needs `base_metric`

These raise `ValueError` on failure. The object is never created.

### Phase 2: validate_model / validate_metric (at registration time)

When `SemanticLayer.add_model()` or `SemanticLayer.add_metric()` is called (including via auto-registration), the `validate_model()` and `validate_metric()` functions in `sidemantic/validation.py` check semantic correctness against the graph:

- Does the model have `primary_key`, `table` or `sql`?
- Are dimension types valid? Do time dimensions have granularity?
- Do measures have valid aggregation types?
- Do ratio `numerator`/`denominator` references point to existing models and measures?
- Are there circular dependencies in derived metrics?
- Does the join path exist for cross-model references?

These raise `ModelValidationError` or `MetricValidationError`.

**Key difference:** Phase 1 catches structural errors (missing fields, wrong types). Phase 2 catches reference errors (model not found, measure not found, circular dependency). A metric can pass Phase 1 and fail Phase 2 if its references don't resolve.

**Known inconsistency:** `stddev`, `stddev_pop`, `variance`, `variance_pop` are accepted by Pydantic (Phase 1) but may fail `validate_model()` (Phase 2) which has a more restrictive aggregation list.

## Error Class Hierarchy

```
ValidationError
  |-- ModelValidationError     (SemanticLayer.add_model, Phase 2)
  |-- MetricValidationError    (SemanticLayer.add_metric, Phase 2)
  |-- QueryValidationError     (SemanticLayer.compile / query)
```

`SemanticGraph` raises `ValueError` for duplicates and `KeyError` for not-found lookups.
Pydantic validators raise `ValueError` (Phase 1).

Import: `from sidemantic.validation import ValidationError, ModelValidationError, MetricValidationError, QueryValidationError`

## Auto-Registration Gotchas

1. With `SemanticLayer(auto_register=True)` (default), `Model()` construction triggers `layer.add_model()` and validation immediately.
2. Graph-level metrics (with `type` like `derived`, `ratio`) auto-register. Simple aggregation metrics (with `agg`, no `type`) do NOT.
3. `time_comparison` and `conversion` metrics inside a model are auto-registered at graph level when the model is added.
4. Without an active layer context, models/metrics are created without registration.
5. `add_model()` is idempotent for identical definitions; different definitions with the same name raise `ValueError`.
