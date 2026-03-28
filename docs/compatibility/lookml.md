# LookML Compatibility

Sidemantic's LookML adapter parses `.lkml` files via the `lkml` Python library and maps LookML concepts to Sidemantic's semantic model (Model, Dimension, Metric, Segment, Relationship). It also supports exporting back to LookML for roundtrip workflows.

Features are marked **supported**, **partial support**, or **unsupported**. Partial support entries include notes explaining the limitation. Properties that parse without error but have no Sidemantic equivalent (display hints, UI metadata, caching config) are grouped together per section rather than listed individually.

---

## Views

| Feature | Status |
|---------|--------|
| `view: name {}` | Supported |
| `sql_table_name` | Supported |
| `description` | Supported |
| Multi-view files | Supported |
| Directory parsing (recursive `.lkml` discovery) | Supported |
| Empty/minimal views (zero dimensions) | Supported |
| `view: +name {}` (refinements) | Supported (merged into the base view; multiple refinements are applied in order) |
| `extension: required` (abstract views) | Supported (stored in `model.meta["extension_required"]`) |
| `extends: [base_view]` | Supported (inheritance resolved using `core/inheritance.py`; child inherits all parent fields with child values taking precedence) |
| `label` | Supported (stored in `model.meta["label"]`) |
| `hidden` | Supported (stored in `model.meta["hidden"]`) |
| `tags` | Supported (stored in `model.meta["tags"]`) |

Multi-extends (`extends: [a, b]`) uses only the first parent. This is rare in practice.

---

## Dimensions

| Feature | Status |
|---------|--------|
| `dimension: name {}` | Supported |
| `type: string` | Supported (maps to `categorical`) |
| `type: number` | Supported (maps to `numeric`) |
| `type: yesno` | Supported (maps to `categorical`) |
| `type: tier` | Supported (maps to `categorical`) |
| `type: location` | Supported (maps to `categorical`, no special geo handling) |
| `type: zipcode` | Supported (maps to `categorical`) |
| `type: date` | Supported (maps to `categorical`) |
| `sql: ${TABLE}.column` | Supported (`${TABLE}` replaced with `{model}` placeholder) |
| Complex SQL expressions | Supported (subqueries, CASE, window functions, regex, JSON extraction preserved verbatim) |
| `primary_key: yes` | Supported |
| `${dimension_name}` references | Supported (resolved recursively, max depth 10; circular references degrade gracefully) |
| `${other_view.field}` cross-view refs | Supported (preserved as-is in SQL) |
| `description` | Supported |
| `label` | Supported (stored in `Dimension.label`) |
| `value_format_name` | Supported (stored in `Dimension.value_format_name`) |
| `value_format` | Supported (stored in `Dimension.format`) |
| `case: { when: {} }` (case dimension) | Supported |
| `hidden` | Supported (stored in `Dimension.meta["hidden"]`) |
| `group_label` | Supported (stored in `Dimension.meta["group_label"]`) |
| `tags` | Supported (stored in `Dimension.meta["tags"]`) |
| `order_by_field` | Supported (stored in `Dimension.meta["order_by_field"]`) |
| `can_filter` | Supported (stored in `Dimension.meta["can_filter"]`) |

Not mapped: `drill_fields`, `suggest_dimension`, `suggest_explore`, `map_layer_name`, `alpha_sort`, `html:`, `link:`, `action:`.

---

## Dimension Groups

### Time Groups

| Feature | Status |
|---------|--------|
| `dimension_group type: time` | Supported (one `Dimension(type="time")` per timeframe) |
| `timeframes: [...]` | Supported (`raw` explicitly skipped) |
| Default timeframe | Supported (defaults to `["date"]` if unspecified) |
| `label` | Supported (propagated to each generated dimension) |
| `description` | Supported (propagated to each generated dimension) |

Naming convention: `{group_name}_{timeframe}`.

Timeframe granularity mapping:

| LookML | Sidemantic |
|--------|------------|
| `time` | `hour` |
| `date` | `day` |
| `week` | `week` |
| `month` | `month` |
| `quarter` | `quarter` |
| `year` | `year` |

Not mapped: `convert_tz`, `datatype`.

### Duration Groups

| Feature | Status |
|---------|--------|
| `dimension_group type: duration` | Supported (numeric dimensions named `{group_name}_{interval}s`) |
| `sql_start` / `sql_end` | Supported (required; group skipped if missing) |
| Intervals: `second`, `minute`, `hour`, `day` | Supported (SQL uses `DATE_DIFF`) |

---

## Measures

| Feature | Status |
|---------|--------|
| `type: count` | Supported |
| `type: count_distinct` | Supported |
| `type: sum` | Supported |
| `type: average` | Supported (maps to `avg`) |
| `type: min` | Supported |
| `type: max` | Supported |
| `type: median` | Supported |
| `type: number` (derived) | Supported (placeholder measures with no SQL correctly skipped) |
| `type: string` (derived) | Supported |
| `type: yesno` (boolean measure) | Supported (maps to derived) |
| `type: period_over_period` | Supported (maps to `time_comparison` with `based_on`, `comparison_type`, `calculation`) |
| `type: percentile` | Supported (generates `PERCENTILE_CONT` SQL as a derived metric; `percentile:` parameter value is preserved) |
| `type: list` | Supported (generates `STRING_AGG(DISTINCT ...)` SQL as a derived metric; measures without SQL are skipped) |
| `sql: ${dimension_ref}` | Supported (resolved to dimension SQL) |
| `sql: ${measure_ref}` in type:number | Supported (converted to plain names for dependency resolution) |
| No explicit type (sql present) | Supported (treated as derived) |
| `description` | Supported |
| `label` | Supported (stored in `Metric.label`) |
| `value_format_name` | Supported (stored in `Metric.value_format_name`) |
| `value_format` | Supported (stored in `Metric.format`) |
| `drill_fields` | Supported (stored in `Metric.drill_fields`) |
| `hidden` | Supported (stored in `Metric.meta["hidden"]`) |
| `group_label` | Supported (stored in `Metric.meta["group_label"]`) |
| `tags` | Supported (stored in `Metric.meta["tags"]`) |
| `type: date` | Partial support: becomes derived rather than a date-aware aggregation. |

Not mapped: `link:`.

---

## Measure Filters

Both LookML filter syntaxes are supported:

- **Shorthand:** `filters: [field: "value"]`
- **Block:** `filters: { field: x value: y }`

Multiple filters combine as AND conditions.

### Filter Expression Support

| Expression | Example | SQL Output |
|------------|---------|------------|
| String equality | `"completed"` | `field = 'completed'` |
| Comma-separated IN | `"val1,val2"` | `field IN ('val1', 'val2')` |
| Negation | `"-cancelled"` | `field != 'cancelled'` |
| Negated list | `"-cancelled,-refunded"` | `field NOT IN ('cancelled', 'refunded')` |
| Boolean | `"yes"` / `"no"` | `field = true` / `field = false` |
| Numeric comparison | `">1000"`, `"<=100"`, `"!=0"` | Preserved as SQL operator |
| Wildcard | `"A%"` | `field LIKE 'A%'` |
| NULL check | `"NULL"` / `"-NULL"` | `IS NULL` / `IS NOT NULL` |
| Empty check | `"EMPTY"` / `"-EMPTY"` | `= ''` / `!= ''` |
| Numeric IN | `"1,2,3"` | `IN (1, 2, 3)` (unquoted) |
| Decimal IN | `"10.5,20.0"` | Unquoted decimals |
| Date range | `"last 30 days"` | Partial support: preserved as string literal, not converted to SQL date expressions. |

---

## Segments (View-Level Filters)

| Feature | Status |
|---------|--------|
| `filter: name { sql: ... }` | Supported (mapped to `Segment`) |
| `${TABLE}` replacement in filter SQL | Supported |
| Filter without `sql:` (parameterized) | Supported (correctly skipped, not added as segment) |
| Query-time segment application | Supported |

---

## Derived Tables

| Feature | Status |
|---------|--------|
| `derived_table: { sql: ... }` | Supported (SQL stored in `Model.sql`, `Model.table = None`) |
| SQL with CTEs | Supported |
| `explore_source` (native derived tables) | Supported (converted to SQL comment + `SELECT * FROM explore_name`) |
| `create_process` / `sql_create` | Unsupported |

Not mapped: `persist_for`, `datagroup_trigger`, `sql_trigger_value`, `materialized_view`, `distribution_style`, `sortkeys`, `indexes`.

---

## Explores and Joins

| Feature | Status |
|---------|--------|
| `explore: name {}` | Supported |
| `join: name {}` | Supported (creates `Relationship` on the base model) |
| `sql_on: ${a.col} = ${b.col}` | Supported (foreign key extracted from `${model.column}` pattern) |
| `relationship:` | Supported (all four: `many_to_one`, `one_to_one`, `one_to_many`, `many_to_many`) |
| Multi-hop join detection | Supported (transitive joins skipped; adjacency graph computes path) |
| `from:` (explore-level) | Supported (resolves to the actual view for model lookup) |
| `from:` (join-level) | Supported (relationship points to actual view name, not the join alias) |
| `description` | Supported (set on base model if model has no description) |
| `label`, `group_label` | Supported (stored in `model.meta["explore_label"]` / `model.meta["explore_group_label"]`) |
| `sql_always_where` | Supported (converted to a Segment on the base model) |
| `always_filter` | Supported (each filter converted to a Segment on the base model) |
| Explore-level `extends:` | Unsupported |

| join `type` (`left_outer`, `inner`, `full_outer`, `cross`) | Supported (stored in `Relationship.metadata["join_type"]`) |

Not mapped: `fields`, `sql_always_having`, `access_filter`, `required_joins`, `cancel_grouping_fields`, `conditionally_filter`.

---

## Model Files

| Feature | Status |
|---------|--------|
| `include:` | Partial support: files must be explicitly provided to the parser. Include directives are not followed. |

Not mapped: `connection`, `label`, `datagroup` definitions.

Unsupported: `access_grant`, `map_layer`, `test:` (data tests).

---

## Manifest Files

Unsupported. `constant:` definitions, `${constant_name}` substitution, `local_dependency`, `remote_dependency`, `override_constant`, and `project_name` are all unhandled.

---

## Liquid Templating

Unsupported. Liquid blocks (`{% if %}`, `{% parameter %}`, `{{ value }}`) are passed through verbatim in SQL strings. They are not evaluated or stripped. `parameter:` field definitions are parsed by lkml but not stored.

---

## Sets

Unsupported. `set:` blocks are parsed by lkml without error but not stored. Views containing sets parse correctly.

---

## Aggregate Tables

Unsupported. `aggregate_table:` blocks in explores parse without error but no semantics are captured.

---

## LookML Export (Roundtrip)

Sidemantic can export its semantic model back to LookML.

| Feature | Status |
|---------|--------|
| Views with `sql_table_name` | Supported |
| Views with `derived_table` | Supported |
| Dimensions (non-time) | Supported (`{model}` replaced back with `${TABLE}`) |
| Dimension groups (time) | Supported (re-grouped by base name, timeframes reconstructed) |
| Standard measures | Supported (type names mapped back, e.g. `avg` -> `average`) |
| Derived measures (type: number) | Supported |
| Ratio metrics | Supported (exported as `type: number` with `${numerator} / ${denominator}`) |
| Time comparison metrics | Supported (exported as `period_over_period`) |
| Filtered measures | Supported (`filters__all` format) |
| Segments | Supported (exported as `filter:` blocks) |
| Primary key | Supported |
| `label` | Supported (roundtrips on dimensions and measures) |
| `value_format_name` | Supported (roundtrips on dimensions and measures) |
| `value_format` | Supported (roundtrips on dimensions and measures) |
| `drill_fields` | Supported (roundtrips on measures) |
| `hidden`, `group_label`, `tags` | Supported (roundtrips via `meta` dict) |
| Roundtrip fidelity | Supported (LookML -> parse -> export -> re-parse produces semantically equivalent graphs) |
