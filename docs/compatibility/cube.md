# Cube Compatibility

Sidemantic's Cube adapter parses YAML schema files (`.yml` / `.yaml`) and maps Cube concepts to Sidemantic's semantic model (Model, Dimension, Metric, Segment, Relationship, PreAggregation). It also supports exporting back to Cube YAML for roundtrip workflows.

Features are marked **supported**, **partial support**, or **unsupported**. Partial support entries include notes explaining the limitation. Properties that parse without error but have no Sidemantic equivalent (UI hints, caching config, access control) are grouped together per section rather than listed individually.

---

## Schema Format

| Feature | Status |
|---------|--------|
| YAML schema (`.yml`, `.yaml`) | Supported |
| Directory parsing (recursive YAML discovery) | Supported |
| Multiple cubes in one file | Supported |
| Empty YAML files | Supported (silently skipped) |
| JavaScript schema (`.js`) | Unsupported |

The adapter only reads YAML. Cube's original JavaScript schema format (`cube.js` files with `cube()` function calls) is not parsed. Projects using JS schemas must convert to YAML first (Cube supports both natively since v0.31).

---

## Cubes

| Feature | Status |
|---------|--------|
| `name` | Supported |
| `sql_table` | Supported (stored as `Model.table`) |
| `sql` (inline query) | Supported (stored as `Model.sql`, `Model.table = None`) |
| `description` | Supported |
| `extends` | Supported (full inheritance resolution: child inherits all dimensions, metrics, relationships, segments, and pre-aggregations from parent, with child values taking precedence) |
| `meta` | Supported (stored on `Model.meta`) |
| `public` / `shown` | Unsupported (cube-level visibility) |
| `data_source` | Unsupported |
| `sql_alias` | Unsupported |
| `refresh_key` (cube-level) | Unsupported |
| `calendar: true` | Partial support: parses without error but the flag is not stored. The cube is treated as a regular model. |

Not mapped: `title`, `rewrite_queries`, `context_members`.

---

## Dimensions

| Feature | Status |
|---------|--------|
| `name` | Supported |
| `type: string` | Supported (maps to `categorical`) |
| `type: number` | Supported (maps to `numeric`) |
| `type: time` | Supported (maps to `time`, default granularity `day`) |
| `type: boolean` | Supported (maps to `categorical`) |
| `type: geo` | Partial support: parses without error but latitude/longitude sub-fields are not stored. Maps to `categorical`. |
| `type: switch` | Partial support: parses without error. The `values` list is not stored. Maps to `categorical`. |
| `sql: ${CUBE}.column` | Supported (`${CUBE}` replaced with `{model}` placeholder) |
| `sql: {CUBE}.column` (no dollar sign) | Supported (also replaced with `{model}`) |
| `sql: ${cube_name}.column` | Supported (cube-name references replaced with `{model}`) |
| `primary_key: true` | Supported |
| `description` | Supported |
| `title` | Supported (stored as `Dimension.label`) |
| `format` | Supported (stored on `Dimension.format`) |
| `shown` / `public` | Supported (stored as `Dimension.public`) |
| `meta` | Supported (stored on `Dimension.meta`) |
| Cross-cube dimension references (`${other_cube.field}`) | Supported (preserved as-is in SQL) |
| Path-qualified references (`{b.d.id}`) | Supported (preserved as-is in SQL) |
| `case: { when: [...] }` (case dimensions) | Supported (converted to SQL `CASE WHEN ... THEN ... ELSE ... END` expression) |
| Custom `granularities` on time dimensions | Supported (stored in `Dimension.supported_granularities`) |
| `sub_query: true` | Unsupported (flag ignored, dimension SQL preserved verbatim) |

Not mapped: `propagate_filters_to_sub_query`.

---

## Measures

| Feature | Status |
|---------|--------|
| `type: count` | Supported |
| `type: count_distinct` | Supported |
| `type: count_distinct_approx` | Supported (maps to `count_distinct`, approximation semantics lost) |
| `type: sum` | Supported |
| `type: avg` | Supported |
| `type: min` | Supported |
| `type: max` | Supported |
| `type: number` (calculated/derived) | Supported (see Derived Measures below) |
| `sql: ${CUBE}.column` | Supported (normalized to `{model}` placeholder) |
| `sql: ${dimension_ref}` (referencing a dimension) | Supported (preserved for resolution) |
| `description` | Supported |
| `title` | Supported (stored as `Metric.label`) |
| `format` | Supported (stored on `Metric.format`) |
| `shown` / `public` | Supported (stored as `Metric.public`) |
| `meta` | Supported (stored on `Metric.meta`) |
| `drill_members` | Supported (stored as `Metric.drill_fields`) |
| `type: rank` | Partial support: stored as `type=derived` with rank semantics (`order_by`, `reduce_by`) preserved in `Metric.meta["cube_type"]`. Does not execute rank window function. |
| `type: string` | Unsupported (no mapping; falls back to `count`) |
| `type: boolean` | Unsupported (no mapping; falls back to `count`) |
| `type: running_total` | Unsupported (no mapping) |

Not mapped: `drill_filters`.

---

## Derived Measures (`type: number`)

Derived measures are detected by `type: number` and handled with several strategies depending on the SQL pattern.

| Pattern | Status |
|---------|--------|
| Simple ratio: `${measure1} / NULLIF(${measure2}, 0)` | Supported (converted to `ratio` metric type with `numerator`/`denominator`) |
| Ratio with type cast: `${measure1}::float / NULLIF(${measure2}, 0)` | Supported |
| Complex derived: `${measure1} - ${measure2} + ${measure3}` | Supported (measure references converted to `cube_name.measure_name` format) |
| Inline aggregation: `COUNT(CASE WHEN ... THEN 1 END)::float / NULLIF(COUNT(*), 0)` | Supported (detected as SQL expression metric, `agg_type=None`) |
| Scalar multiplication: `${mrr} * 12` | Supported (treated as derived with measure reference replacement) |

---

## Measure Filters

| Feature | Status |
|---------|--------|
| `filters: [{ sql: "..." }]` | Supported (SQL expressions normalized and stored in `Metric.filters`) |
| Multiple filters | Supported (all filters stored; combined as AND at query time) |
| `${CUBE}` / `{CUBE}` in filter SQL | Supported (normalized to `{model}`) |
| `${cube_name}` in filter SQL | Supported (normalized to `{model}`) |

---

## Rolling Window / Cumulative Measures

| Feature | Status |
|---------|--------|
| `rolling_window: { trailing: unbounded }` | Supported (metric type set to `cumulative`, window stored) |
| `rolling_window: { trailing: "1 month" }` | Supported (trailing value stored in `Metric.window`) |
| `rolling_window: { offset: end }` | Partial support: the `offset` value is not stored. Only `trailing` is captured. |
| `rolling_window: { leading: "-1 month" }` | Partial support: the `leading` value is not stored. |
| `rolling_window: { type: to_date, granularity: year }` | Supported (maps to `Metric.grain_to_date`, e.g., `grain_to_date="year"` for YTD) |

---

## Segments

| Feature | Status |
|---------|--------|
| `segments: [{ name, sql }]` | Supported (mapped to `Segment`) |
| `${CUBE}` / `{CUBE}` replacement in segment SQL | Supported |
| `${cube_name}` replacement in segment SQL | Supported |
| `description` | Supported |
| `shown` / `public` | Supported (stored as `Segment.public`) |
| Query-time segment application | Supported |
| Segments without `sql:` | Supported (correctly skipped, not added) |

---

## Joins

| Feature | Status |
|---------|--------|
| `joins: [{ name, sql, relationship }]` | Supported (creates `Relationship` on the base model) |
| `relationship: many_to_one` | Supported |
| `relationship: one_to_many` | Supported |
| `relationship: one_to_one` | Supported |
| `relationship: many_to_many` | Supported (stored as-is) |
| Foreign key extraction from `${CUBE}.column = ${target.id}` | Supported (regex-based extraction for both `many_to_one` and `one_to_many`) |
| FK fallback | Supported (falls back to `{join_name}_id` convention if regex parse fails) |
| Diamond join patterns (A -> B -> D, A -> C -> D) | Supported (each cube's joins parsed independently) |
| Multi-hop transitive joins | Supported (graph traversal computes paths at query time) |
| Nullable foreign keys | Supported (join SQL preserved; LEFT JOIN semantics at query time) |

Not mapped: join `type` (not the same as `relationship`; Cube does not use this concept).

---

## Pre-Aggregations

Pre-aggregations are fully mapped to Sidemantic's `PreAggregation` model, including refresh configuration, partitioning, and indexes.

| Feature | Status |
|---------|--------|
| `type: rollup` | Supported |
| `type: rollupJoin` / `rollup_join` | Supported (type normalized to `rollup_join`) |
| `type: rollupLambda` / `lambda` | Supported (type normalized to `lambda`) |
| `type: original_sql` | Supported |
| `measures` (list of measure references) | Supported (`CUBE.` prefix stripped) |
| `dimensions` (list of dimension references) | Supported (`CUBE.` prefix stripped) |
| `time_dimension` | Supported (`CUBE.` prefix stripped) |
| `granularity` (hour, day, week, month, quarter, year) | Supported |
| `partition_granularity` | Supported |
| `refresh_key: { every }` | Supported |
| `refresh_key: { sql }` | Supported |
| `refresh_key: { incremental }` | Supported |
| `refresh_key: { update_window }` | Supported |
| `scheduled_refresh` | Supported (defaults to `true`) |
| `indexes: [{ name, columns, type }]` | Supported |
| `build_range_start` / `build_range_end` | Supported (SQL expression extracted) |
| Cross-cube dimension references in pre-aggs (e.g., `visitors.source`) | Partial support: parsed as dimension name string; the cross-cube prefix is not stripped. |
| `rollups` (list of rollup references for rollupJoin/rollupLambda) | Unsupported (not stored) |
| Empty pre-aggregation sections (YAML null) | Supported (treated as empty list) |

---

## Views

Supported. The `views:` top-level section in Cube YAML is parsed after all cubes are loaded. Each view is resolved into a composite Model by projecting members from source cubes.

| Feature | Status |
|---------|--------|
| `join_path: cube_name` (single cube) | Supported (resolves to target cube by last segment of path) |
| `join_path: cube_a.cube_b` (multi-level) | Supported (resolves to the last cube in the path) |
| `includes: "*"` (wildcard) | Supported (imports all dimensions and metrics from source cube) |
| `includes:` (selective list) | Supported (imports named dimensions and metrics) |
| `includes:` with `{ name, alias }` | Supported (renames members via `alias`) |
| `excludes:` list | Supported (removes named members from includes) |
| `prefix: true` | Supported (prefixes member names with `{cube_name}_`) |
| `alias` on cube entry | Supported (used as prefix when `prefix: true`) |
| View-only files (no `cubes:` section) | Supported (views that reference cubes from other files resolve correctly when parsed from a directory) |
| Empty view (no resolvable cubes) | Supported (silently skipped, no model created) |
| `extends` on views | Unsupported |
| `folders` | Unsupported (UI grouping concept) |
| View-level `access_policy` | Unsupported |

View models are marked with `meta={"cube_type": "view"}` and are excluded from Cube export.

---

## Hierarchies

Supported. `hierarchies:` blocks on cubes are parsed and used to set `Dimension.parent` chains.

| Feature | Status |
|---------|--------|
| `hierarchies: [{ name, levels }]` | Supported (level ordering sets `Dimension.parent` on each child level) |
| Multiple hierarchies per cube | Supported |
| Cross-cube level references (e.g., `users.city`) | Partial support: cross-cube references (containing dots) are silently skipped. Only same-cube levels are linked. |
| `title` on hierarchies | Not stored (used for display only) |

---

## Access Control (`access_policy` / `accessPolicy`)

Unsupported. Access policy blocks on cubes and views are parsed by YAML without error but not stored. This includes:

- `role` / `group` definitions
- `row_level` filters with `member`, `operator`, `values`
- `row_level: { allow_all: true }`
- `member_level: { includes, excludes }`
- `conditions` with security context expressions (`{ security_context.* }`)

---

## Multi-Stage Calculations

Partial support. The `multi_stage: true` flag on measures is parsed by YAML without error but not stored. Multi-stage calculations in Cube enable measures that reference other measures as inputs, run in separate query stages, and support features like `group_by` (for percent-of-total) and `time_shift` (for period comparisons). The adapter parses the measure's `sql` and `type` normally, so the measure still appears in the graph, but multi-stage execution semantics are not reproduced.

| Feature | Status |
|---------|--------|
| `time_shift: [{ time_dimension, interval, type: prior }]` | Supported (maps to `Metric.type="time_comparison"` with `comparison_type` and `time_offset`) |
| `group_by` (percent-of-total grouping) | Unsupported |
| `order_by` / `reduce_by` (ranking) | Partial support: stored in `Metric.meta` for `type: rank` measures |
| `case` / `switch` / `when` / `else` on measures | Unsupported |

---

## Custom Calendars and Granularities

Partial support. Cube supports custom calendar cubes (`calendar: true`) with custom granularity definitions on time dimensions. The `calendar: true` flag is not stored, but custom granularities are now parsed.

| Feature | Status |
|---------|--------|
| `granularities: [{ name, ... }]` on time dimensions | Supported (granularity names stored in `Dimension.supported_granularities`) |
| `granularities[].sql`, `interval`, `origin` | Unsupported (only the name is stored) |
| `calendar: true` on cubes | Partial support (parsed without error, not stored) |
| Dimension-level `time_shift` | Unsupported |

---

## SQL Syntax Normalization

The adapter normalizes three Cube SQL reference patterns to Sidemantic's `{model}` placeholder:

| Cube Pattern | Sidemantic Output |
|-------------|-------------------|
| `${CUBE}` | `{model}` |
| `{CUBE}` (no dollar sign) | `{model}` |
| `${cube_name}` (e.g., `${orders}`) | `{model}` |
| `{cube_name}` (no dollar sign) | `{model}` |

This normalization applies to dimension SQL, measure SQL, filter SQL, and segment SQL.

Measure-to-measure references (`${measure_name}`) in derived measures are converted to `cube_name.measure_name` format for Sidemantic's dependency resolution.

---

## Cube Export (Roundtrip)

Sidemantic can export its semantic model back to Cube YAML format.

| Feature | Status |
|---------|--------|
| Cubes with `sql_table` | Supported |
| Cubes with `sql` (inline query) | Supported |
| `description` | Supported |
| `meta` on cubes | Supported |
| Dimensions (string, number, time, boolean) | Supported (`{model}` mapped back to Cube types) |
| `primary_key: true` on dimensions | Supported |
| `format` on dimensions | Supported |
| `title` on dimensions | Supported (exported from `Dimension.label`) |
| `meta` on dimensions | Supported |
| `shown: false` on dimensions | Supported (exported when `Dimension.public` is False) |
| Standard measures (count, count_distinct, sum, avg, min, max) | Supported |
| Derived measures (`type: number`) | Supported |
| Ratio metrics | Supported (exported as `type: number` with `${numerator}::float / NULLIF(${denominator}, 0)`) |
| Cumulative metrics | Supported (exported with `rolling_window: { trailing }` or `rolling_window: { type: to_date, granularity }`) |
| Time comparison metrics | Partial support: exported as `type: number` with a description annotation; no `time_shift` block generated. |
| Measures with filters | Supported (exported as `filters: [{ sql }]`) |
| `format` on measures | Supported |
| `title` on measures | Supported (exported from `Metric.label`) |
| `meta` on measures | Supported |
| `shown: false` on measures | Supported (exported when `Metric.public` is False) |
| `drill_members` on measures | Supported (exported from `Metric.drill_fields` or hierarchy dimensions) |
| Segments | Supported (`{model}` replaced back with `${CUBE}`) |
| `shown: false` on segments | Supported (exported when `Segment.public` is False) |
| Joins (many_to_one, one_to_one) | Supported (generates `${CUBE}.fk = ${target}.pk` join SQL) |
| Joins (one_to_many) | Supported (generates `${CUBE}.pk = ${target}.fk` with swapped direction) |
| Joins (many_to_many) | Unsupported (skipped; requires junction table info) |
| Pre-aggregations | Supported (all fields exported including refresh_key, indexes, build_range) |
| Model inheritance resolution | Supported (inheritance resolved before export) |
| View models | Supported (skipped during export, identified by `meta.cube_type == "view"`) |
| Roundtrip fidelity (Cube -> parse -> export -> re-parse) | Supported for dimensions, metrics, segments, and pre-aggregations. |
