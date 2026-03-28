# OSI Compatibility

Sidemantic's OSI adapter parses [Open Semantic Interface](https://github.com/open-semantic-interchange/OSI) YAML files and maps OSI concepts to Sidemantic's semantic model (Model, Dimension, Metric, Relationship). It also supports exporting back to OSI YAML, including multi-dialect SQL transpilation via sqlglot.

Features are marked **supported**, **partial support**, or **unsupported**. Partial support entries include notes explaining the limitation. Properties that parse without error but have no Sidemantic equivalent are grouped together per section rather than listed individually.

---

## Semantic Model (Top Level)

| Feature | Status |
|---------|--------|
| `semantic_model` (list of models) | Supported |
| Multiple semantic models in one file | Supported (each iterated independently) |
| `name` (semantic model name) | Partial support: parsed but not stored. The graph has no concept of a top-level model name. Individual datasets become named Models. |
| `description` (semantic model description) | Partial support: parsed but not stored on the graph. |

Not mapped: `version`.

---

## Datasets

| Feature | Status |
|---------|--------|
| `name` | Supported |
| `source` (table reference) | Supported (stored as `Model.table`) |
| `description` | Supported |
| `primary_key` (single column, e.g. `[id]`) | Supported (stored as string) |
| `primary_key` (composite, e.g. `[col_a, col_b]`) | Supported (stored as list) |
| `unique_keys` (list of column lists) | Supported |
| `fields` | Supported (mapped to Dimensions) |
| `ai_context` | Supported (stored in `Model.meta["ai_context"]`) |
| `custom_extensions` (dataset level) | Supported (stored in `Model.meta["custom_extensions"]`) |
| `custom_extensions` (semantic_model level) | Unsupported |
| Dataset without `name` | Supported (gracefully skipped) |
| Multi-file directory parsing (recursive `.yml`/`.yaml` discovery) | Supported |
| Default primary key when omitted | Supported (defaults to `"id"`) |

---

## Fields (Dimensions)

| Feature | Status |
|---------|--------|
| `name` | Supported |
| `expression.dialects` (SQL expressions per dialect) | Supported |
| `description` | Supported |
| `label` | Supported |
| `dimension.is_time: true` | Supported (maps to `Dimension(type="time", granularity="day")`) |
| `dimension.is_time: false` | Supported (maps to `Dimension(type="categorical")`) |
| No `dimension` block | Supported (defaults to `categorical`) |
| `ai_context` | Supported (stored in `Dimension.meta["ai_context"]`) |
| `custom_extensions` | Supported (stored in `Dimension.meta["custom_extensions"]`) |
| Computed/derived expressions (e.g. `c_first_name \|\| ' ' \|\| c_last_name`) | Supported (expression preserved verbatim from chosen dialect) |
| Field without `name` | Supported (gracefully skipped) |

### Dialect Preference

When multiple dialects are present on a field expression, the adapter selects one using a fixed preference order: `ANSI_SQL` > `SNOWFLAKE` > `DATABRICKS`. If none of these are present, the first dialect in the list is used as fallback.

Not mapped: `dimension.granularity`, `dimension.time_zone`, field `type` (all fields become either `time` or `categorical` based solely on `is_time`).

---

## Metrics

| Feature | Status |
|---------|--------|
| Simple aggregations (`SUM(...)`, `COUNT(...)`, `AVG(...)`, `MIN(...)`, `MAX(...)`) | Supported (aggregation type auto-detected by Metric's model_validator via sqlglot) |
| `COUNT(DISTINCT ...)` | Supported (detected as `count_distinct`) |
| Complex/ratio expressions (e.g. `SUM(x) / NULLIF(COUNT(DISTINCT y), 0)`) | Supported (parsed as derived metric) |
| `CASE WHEN` inside aggregations | Supported (expression preserved verbatim) |
| `FILTER (WHERE ...)` clause | Supported (expression preserved verbatim) |
| Cross-dataset references (e.g. `SUM(orders.amount)`) | Supported (dot-qualified references preserved in expression) |
| `description` | Supported |
| `ai_context` | Supported (stored in `Metric.meta["ai_context"]`) |
| `custom_extensions` | Supported (stored in `Metric.meta["custom_extensions"]`) |
| Metric without `name` | Supported (gracefully skipped) |
| Metric without `expression` | Supported (gracefully skipped) |

Not mapped: `type` (the OSI `type` hint on metrics, e.g. `type: count`, is ignored; aggregation type is inferred from the SQL expression instead).

---

## Relationships

| Feature | Status |
|---------|--------|
| `from` / `to` (dataset references) | Supported |
| `from_columns` / `to_columns` (single column) | Supported |
| `from_columns` / `to_columns` (multi-column composite keys) | Supported |
| Relationship type | Partial support: all relationships are imported as `many_to_one`. The OSI spec does not define cardinality on the `from`/`to` relationship format, and the adapter always assumes many-to-one. |
| Missing `from_columns` | Supported (defaults foreign key to `{to_model}_id`) |
| Missing `to_columns` | Supported (defaults primary key to `id`) |
| Relationship with missing `from` or `to` | Supported (gracefully skipped) |
| Relationship to non-existent model | Supported (gracefully skipped if the `from` model is not in the graph) |
| `left_dataset` / `right_dataset` / `cardinality` format | Unsupported |

The `left_dataset`/`right_dataset`/`cardinality` relationship format (used by some community OSI files, e.g. mdb-engine models) is not parsed. Only the `from`/`to`/`from_columns`/`to_columns` format is recognized. Files using the alternative format will parse without error, but relationships will be silently skipped.

Not mapped: relationship `name`, `ai_context` on relationships.

---

## Expression Dialects

### Import

| Feature | Status |
|---------|--------|
| `ANSI_SQL` dialect | Supported (preferred) |
| `SNOWFLAKE` dialect | Supported (second preference) |
| `DATABRICKS` dialect | Supported (third preference) |
| `BIGQUERY` dialect | Supported (used as fallback if preferred dialects absent) |
| Other/custom dialects | Supported (used as fallback if preferred dialects absent) |
| Multiple dialects per expression | Supported (single dialect selected by preference order) |
| Missing `expression` block | Supported (field SQL becomes `None`) |

### Export

| Feature | Status |
|---------|--------|
| Export to `ANSI_SQL` | Supported (default, expression passed through as-is) |
| Export to `SNOWFLAKE` | Supported (transpiled from DuckDB/ANSI via sqlglot) |
| Export to `DATABRICKS` | Supported (transpiled via sqlglot) |
| Export to `BIGQUERY` | Supported (transpiled via sqlglot) |
| Multiple dialects in single export | Supported (pass `dialects=["ANSI_SQL", "SNOWFLAKE", ...]`) |
| Unknown dialect on export | Supported (falls back to original expression) |
| Transpilation failure | Supported (falls back to original expression) |

---

## AI Context

`ai_context` is an OSI extension for providing hints to AI/LLM systems. Sidemantic preserves the full structure at all levels.

| Level | Status |
|-------|--------|
| Dataset `ai_context` | Supported (stored in `Model.meta["ai_context"]`) |
| Field `ai_context` | Supported (stored in `Dimension.meta["ai_context"]`) |
| Metric `ai_context` | Supported (stored in `Metric.meta["ai_context"]`) |
| Semantic model level `ai_context` | Partial support: parsed by YAML but not stored on the graph. |
| Relationship `ai_context` | Unsupported |

Common sub-keys (`synonyms`, `instructions`, `examples`, `description_for_ai`) are all preserved as-is in the meta dictionary. No sub-key receives special handling.

---

## Custom Extensions

| Level | Status |
|-------|--------|
| Dataset `custom_extensions` | Supported (stored in `Model.meta["custom_extensions"]`) |
| Field `custom_extensions` | Supported (stored in `Dimension.meta["custom_extensions"]`) |
| Metric `custom_extensions` | Supported (stored in `Metric.meta["custom_extensions"]`) |
| Semantic model level `custom_extensions` | Unsupported |

Nested structures within `custom_extensions` (vendor configs, tag lists, etc.) are preserved verbatim as Python dicts/lists.

---

## OSI Export (Roundtrip)

Sidemantic can export its semantic model back to OSI YAML format.

| Feature | Status |
|---------|--------|
| Datasets with `source` (table name) | Supported |
| Datasets with `sql` (derived/subquery source) | Supported (wrapped in parentheses) |
| Fields (dimensions) | Supported |
| Time dimension `is_time` flag | Supported |
| Field `label` | Supported |
| Field `description` | Supported |
| Primary key (single column) | Supported (exported as list) |
| Primary key (composite) | Supported (exported as list) |
| Unique keys | Supported |
| Standard aggregation metrics (sum, count, avg, min, max) | Supported (reconstructed as `AGG(inner)`) |
| `count_distinct` metrics | Supported (exported as `COUNT(DISTINCT inner)`) |
| Ratio metrics | Supported (exported as `numerator / NULLIF(denominator, 0)`) |
| Derived metrics | Supported (SQL expression exported as-is) |
| Model-level metrics | Supported (promoted to semantic_model-level metrics, field refs qualified with model name) |
| Graph-level metrics | Supported |
| Relationships (`many_to_one` only) | Supported |
| Non-`many_to_one` relationships | Unsupported (silently omitted from export) |
| Multi-column relationship keys | Supported |
| Related model PK lookup on export | Supported (when `rel.primary_key` is None, the related model's actual PK columns are used for `to_columns`) |
| `ai_context` roundtrip (dataset, field, metric) | Supported |
| `custom_extensions` roundtrip (dataset, field, metric) | Supported |
| Multi-dialect export via sqlglot | Supported |
| Model inheritance resolution before export | Supported |
| Roundtrip fidelity (OSI -> parse -> export -> re-parse) | Supported (semantically equivalent graphs) |

---

## Version Field

Unsupported. The `version: "1.0"` key at the file root is ignored on import and not emitted on export.

---

## Alternative Relationship Formats

Unsupported. The `left_dataset`/`right_dataset`/`cardinality` relationship format found in some community OSI files (mdb-engine, etc.) is not recognized by the adapter. These relationships are silently skipped during parsing. Only the canonical `from`/`to`/`from_columns`/`to_columns` format is handled.
