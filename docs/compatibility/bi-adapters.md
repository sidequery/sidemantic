# Power BI, ThoughtSpot and GoodData conversion boundaries

These adapters read native model files. Preserving a native property for export does not make it executable in SQL or portable to another vendor.

All three exporters reject graph-level metrics without a model-owned native representation rather than silently dropping them. Graph registry aliases of the same model-owned metric are not treated as additional metrics.

## Power BI TMDL

TMDL import/export supports individual scripts and split `definition/` directories. BIM/TMSL JSON and PBIP manifest resolution are not supported by this adapter.

Imported DAX is retained for native export. Export preserves the original text while the metric's SQL, aggregation, type and expression-language state are unchanged. If those semantics are edited, update `dax` explicitly or clear it to request SQL-to-DAX conversion; an old preserved expression is not silently replayed over the edit.

SQL-to-DAX export supports single-column SUM, AVG, MIN, MAX, MEDIAN, COUNT and COUNT DISTINCT, plus row counts. A column must resolve to exactly one exported dimension; the emitted DAX uses that dimension's display name. Counts use COUNTA or DISTINCTCOUNTNOBLANK and COALESCE to preserve SQL's treatment of NULL and zero result on empty input. Original native DISTINCTCOUNT expressions retain their own BLANK semantics.

Derived SQL, aggregates over arithmetic or foreign-table references, unknown columns, unsupported aggregations, filters, null filling, semi-additivity and advanced metric modifiers are rejected before writing the project. Supply an explicit DAX expression for an otherwise unsupported expression. This does not bypass rejection of independent metric modifiers.

Regression tests parse exported DAX and execute reimported SQL against Boolean, NULL and empty inputs. They do not replace Power BI engine validation.

## ThoughtSpot TML

Table, Worksheet and Model TML import remain supported. SQL View documents are explicitly rejected; they must not appear to import successfully as an empty graph. A document with nonempty `rls_rules` is rejected because this adapter cannot enforce those restrictions in SQL queries.

Native Table round trips retain connection metadata, original data types (including `db_column_properties`) and original `joins_with`. Edited native joins are rejected instead of replaying stale join definitions. Changing a native Table to a SQL source requires SQL View export, which is unsupported. Modern Model export still uses the existing Worksheet/Table conversion rather than producing a modern Model document set.

Native Table field SQL must remain a simple column binding. Dimension or metric expressions that need formulas are rejected before writing rather than replaced with a column name.

A destination file accepts one model. Use an output directory for multiple models. Rejected native exports are checked before writing any document in the set.

## GoodData LDM

Compound Cloud LDM grains retain every physical key column. Each `references.sources` entry resolves its explicit target attribute in source order, independently of the target grain's order. Attribute keys use the attribute's source column even when a display label reads another column. Unknown target attributes and malformed reference sources are rejected.

For attributes originally imported without labels, edits to the dimension SQL, native key source column or corresponding model primary-key column update the shared attribute and generated-label binding. Conflicting edits are rejected. Attributes with native labels retain their separate key and display-column bindings.

Cloud and legacy shapes, SQL datasets, date instances and aggregated facts retain their existing support. Numeric facts still become SUM metrics by default; this is an adapter assumption rather than imported MAQL. MAQL analytical-model metrics and workspace security enforcement are not implemented. Preserving workspace filter metadata for a native round trip does not enforce it in SQL queries.
