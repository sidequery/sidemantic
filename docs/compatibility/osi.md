# OSI Compatibility

Sidemantic's OSI adapter parses [Open Semantic Interchange](https://github.com/open-semantic-interchange/OSI) YAML files and maps OSI concepts to Sidemantic's semantic model (Model, Dimension, Metric, Relationship). It also supports exporting back to OSI YAML, including multi-dialect SQL transpilation via sqlglot.

Features are marked **supported**, **partial support**, or **unsupported**. Partial support entries include notes explaining the limitation. Properties that parse without error but have no Sidemantic equivalent are grouped together per section rather than listed individually.

---

## Semantic Model (Top Level)

| Feature | Status |
|---------|--------|
| `semantic_model` (list of models) | Supported |
| Multiple semantic models in one file | Supported (each iterated independently) |
| `version` | Supported. Current exports emit `0.2.0.dev0`; imports preserve the source version in `SemanticGraph.metadata["osi"]["version"]`. |
| `name` (semantic model name) | Supported as metadata. Stored in `SemanticGraph.metadata["osi"]["semantic_models"]` and reused on export. Individual datasets still become named Models. |
| `description` (semantic model description) | Supported as graph metadata and reused on export. |
| `ai_context` (semantic model level) | Supported as graph metadata and reused on export. |
| `custom_extensions` (semantic model level) | Supported as graph metadata and reused on export. |

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
| Dataset without `name` | Supported (gracefully skipped) |
| Multi-file directory parsing (recursive `.yml`/`.yaml` discovery) | Supported |
| Omitted primary key | Supported (preserved as unknown / `None`) |

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
| Relationship `name` | Supported (stored in `Relationship.metadata["osi_name"]` and reused on export) |
| Relationship `ai_context` | Supported (stored in `Relationship.metadata["ai_context"]`) |
| Relationship `custom_extensions` | Supported (stored in `Relationship.metadata["custom_extensions"]`) |
| Missing `from_columns` | Preserved as an unknown foreign key; structural validation rejects joins that need it |
| Missing `to_columns` | Uses the target model's declared key when available; otherwise remains unknown |
| Relationship with missing `from` or `to` | Supported (gracefully skipped) |
| Relationship to non-existent model | Supported (gracefully skipped if the `from` model is not in the graph) |
| `left_dataset` / `right_dataset` / `cardinality` format | Unsupported |

The `left_dataset`/`right_dataset`/`cardinality` relationship format (used by some community OSI files, e.g. mdb-engine models) is not parsed. Only the `from`/`to`/`from_columns`/`to_columns` format is recognized. Files using the alternative format will parse without error, but relationships will be silently skipped.

---

## Expression Dialects

### Import

| Feature | Status |
|---------|--------|
| `ANSI_SQL` dialect | Supported (preferred) |
| `SNOWFLAKE` dialect | Supported (second preference) |
| `DATABRICKS` dialect | Supported (third preference) |
| `MAQL`, `TABLEAU`, `MDX` dialects | Supported as fallback expressions if no preferred SQL dialect is present. The expression is preserved as text; it is not translated. |
| Other/custom dialects | Supported on import as fallback if preferred dialects absent |
| Multiple dialects per expression | Supported (single dialect selected by preference order) |
| Missing `expression` block | Supported (field SQL becomes `None`) |

### Export

| Feature | Status |
|---------|--------|
| Export to `ANSI_SQL` | Supported (default, expression passed through as-is) |
| Export to `SNOWFLAKE` | Supported (transpiled from DuckDB/ANSI via sqlglot) |
| Export to `DATABRICKS` | Supported (transpiled via sqlglot) |
| Export to `BIGQUERY` | Unsupported (not in the current OSI dialect enum) |
| Export to `MAQL`, `TABLEAU`, `MDX` | Unsupported (not safely generated from Sidemantic SQL) |
| Multiple dialects in single export | Supported (pass `dialects=["ANSI_SQL", "SNOWFLAKE", ...]`) |
| Unknown dialect on export | Unsupported (raises `ValueError`) |
| Transpilation failure | Supported (falls back to original expression) |

---

## AI Context

`ai_context` is an OSI extension for providing hints to AI/LLM systems. Sidemantic preserves the full structure at all levels.

| Level | Status |
|-------|--------|
| Dataset `ai_context` | Supported (stored in `Model.meta["ai_context"]`) |
| Field `ai_context` | Supported (stored in `Dimension.meta["ai_context"]`) |
| Metric `ai_context` | Supported (stored in `Metric.meta["ai_context"]`) |
| Semantic model level `ai_context` | Supported (stored in `SemanticGraph.metadata["osi"]["semantic_models"]`) |
| Relationship `ai_context` | Supported (stored in `Relationship.metadata["ai_context"]`) |

Common sub-keys (`synonyms`, `instructions`, `examples`, `description_for_ai`) are all preserved as-is in the meta dictionary. No sub-key receives special handling.

---

## Custom Extensions

| Level | Status |
|-------|--------|
| Dataset `custom_extensions` | Supported (stored in `Model.meta["custom_extensions"]`) |
| Field `custom_extensions` | Supported (stored in `Dimension.meta["custom_extensions"]`) |
| Metric `custom_extensions` | Supported (stored in `Metric.meta["custom_extensions"]`) |
| Relationship `custom_extensions` | Supported (stored in `Relationship.metadata["custom_extensions"]`) |
| Semantic model level `custom_extensions` | Supported (stored in `SemanticGraph.metadata["osi"]["semantic_models"]`) |

Current OSI schema represents `custom_extensions` as a list of `{vendor_name, data}` objects where `data` is a JSON string. Sidemantic still imports legacy dict/list extension shapes. On export, non-standard extension payloads are wrapped as a `SIDEMANTIC` extension with stringified JSON so emitted OSI stays schema-shaped.

---

## OSI Export (Roundtrip)

Sidemantic can export its semantic model back to OSI YAML format.

| Feature | Status |
|---------|--------|
| Current OSI version | Supported (`version: "0.2.0.dev0"` emitted at file root) |
| Semantic-model-level metadata | Supported (`name`, `description`, `ai_context`, `custom_extensions`) |
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
| Relationship `name`, `ai_context`, `custom_extensions` | Supported |
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

Supported. Imports preserve the source value in graph metadata. Exports always emit the current supported OSI draft version, `0.2.0.dev0`.

---

## Ontology Files

OSI's ontology spec defines conceptual `ontology` entries plus `ontology_mappings` that embed logical semantic models. Sidemantic does not implement ontology reasoning, but it does:

| Feature | Status |
|---------|--------|
| Top-level `ontology` | Partial support: preserved in `SemanticGraph.metadata["osi"]["ontology"]`; not converted to Models. |
| `ontology_mappings[].semantic_model` | Supported: parsed into Sidemantic Models, Relationships, and Metrics. |
| `ontology_mappings[].concept_mappings` | Partial support: preserved in graph metadata; not used for query planning. |

---

## Alternative Relationship Formats

Unsupported. The `left_dataset`/`right_dataset`/`cardinality` relationship format found in some community OSI files (mdb-engine, etc.) is not recognized by the adapter. These relationships are silently skipped during parsing. Only the canonical `from`/`to`/`from_columns`/`to_columns` format is handled.
