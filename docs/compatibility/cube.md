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
| `extends` | Partial support: the extending cube is parsed with only its own fields. Inherited fields from the base cube are not merged. Each cube in an extends chain is independent. |
| `public` / `shown` | Unsupported |
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
| `format` | Supported (stored on `Dimension.format`) |
| Cross-cube dimension references (`${other_cube.field}`) | Supported (preserved as-is in SQL) |
| Path-qualified references (`{b.d.id}`) | Supported (preserved as-is in SQL) |
| `case: { when: [...] }` (case dimensions) | Partial support: the `case` block is parsed by YAML without error but the case/when/else structure is not evaluated or stored. The dimension has no SQL expression. |
| `sub_query: true` | Unsupported (flag ignored, dimension SQL preserved verbatim) |
| Custom `granularities` on time dimensions | Unsupported (parsed by YAML but not stored) |
| `meta` | Unsupported |

Not mapped: `shown`, `title`, `propagate_filters_to_sub_query`, `case` labels.

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
| `format` | Supported (stored on `Metric.format`) |
| `type: rank` | Partial support: parses without error but rank semantics (`order_by`, `reduce_by`) are not stored. Becomes a regular measure with `agg_type=count` (default fallback). |
| `type: string` | Unsupported (no mapping; falls back to `count`) |
| `type: boolean` | Unsupported (no mapping; falls back to `count`) |
| `type: running_total` | Unsupported (no mapping) |

Not mapped: `shown`, `title`, `drill_members` (on import; used on export), `meta`, `drill_filters`.

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
| `rolling_window: { type: to_date, granularity: year }` | Partial support: treated as cumulative. The `type` and `granularity` sub-fields are not stored. |

---

## Segments

| Feature | Status |
|---------|--------|
| `segments: [{ name, sql }]` | Supported (mapped to `Segment`) |
| `${CUBE}` / `{CUBE}` replacement in segment SQL | Supported |
| `${cube_name}` replacement in segment SQL | Supported |
| `description` | Supported |
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

Unsupported. The `views:` top-level section in Cube YAML is silently ignored during parsing. Files that contain only views (no `cubes:` section) parse without error and produce an empty graph.

Cube views are a composition layer that project and rename members from cubes via `join_path`, `includes`, `excludes`, `prefix`, and `alias`. None of these concepts are mapped:

- `join_path` traversal
- `includes: "*"` wildcard and selective includes
- `excludes` list
- `prefix: true` namespacing
- `alias` renaming
- View-level `access_policy`
- `folders` (grouping members in views)
- `extends` on views

---

## Hierarchies

Unsupported. `hierarchies:` blocks on cubes are parsed by YAML without error but not stored. Hierarchies define drill-down level ordering (e.g., year -> quarter -> month) and cross-cube level references. They have no Sidemantic equivalent.

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

Unsupported. The `multi_stage: true` flag on measures is parsed by YAML without error but not stored. Multi-stage calculations in Cube enable measures that reference other measures as inputs, run in separate query stages, and support features like `group_by` (for percent-of-total) and `time_shift` (for period comparisons). The adapter parses the measure's `sql` and `type` normally, so the measure still appears in the graph, but multi-stage execution semantics are not reproduced.

Related unsupported sub-features:
- `time_shift: [{ time_dimension, interval, type }]` on measures
- `group_by` (percent-of-total grouping)
- `order_by` / `reduce_by` (ranking)
- `case` / `switch` / `when` / `else` on measures

---

## Custom Calendars and Granularities

Unsupported. Cube supports custom calendar cubes (`calendar: true`) with custom granularity definitions on time dimensions (`granularities: [{ name, sql, interval, origin }]`) and dimension-level `time_shift` definitions. The adapter parses these structures without error but does not store them. Time dimensions always default to `granularity: day`.

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
| Dimensions (string, number, time, boolean) | Supported (`{model}` mapped back to Cube types) |
| `primary_key: true` on dimensions | Supported |
| `format` on dimensions | Supported |
| Standard measures (count, count_distinct, sum, avg, min, max) | Supported |
| Derived measures (`type: number`) | Supported |
| Ratio metrics | Supported (exported as `type: number` with `${numerator}::float / NULLIF(${denominator}, 0)`) |
| Cumulative metrics | Supported (exported with `rolling_window: { trailing }`) |
| Time comparison metrics | Partial support: exported as `type: number` with a description annotation; no `time_shift` block generated. |
| Measures with filters | Supported (exported as `filters: [{ sql }]`) |
| `format` on measures | Supported |
| `drill_members` on measures | Supported (exported from hierarchy dimensions when available) |
| Segments | Supported (`{model}` replaced back with `${CUBE}`) |
| Joins (many_to_one) | Supported (generates `sql` join expression from foreign key and primary key) |
| Joins (one_to_many, one_to_one) | Partial support: only `many_to_one` relationships are exported as joins. Other relationship types are omitted. |
| Pre-aggregations | Unsupported (not exported) |
| Model inheritance resolution | Supported (inheritance resolved before export) |
| Roundtrip fidelity (Cube -> parse -> export -> re-parse) | Supported for dimensions, metrics, and segments. Relationships are not fully round-tripped due to the export limitation above. |

