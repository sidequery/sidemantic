# Hex, Holistics and Omni fidelity

Use an output directory when exporting multiple models. Hex `.yaml`/`.yml`
and Holistics `.aml` output files accept one model and reject multiple models
before writing. They no longer select the first model implicitly.
All three adapters also reject graph-only metrics without a model placement;
attach those metrics to a model before exporting instead of dropping them.

Hex retains Boolean and timestamp types and exports median, standard deviation
and variance aggregates under their original aggregation names. An unknown
aggregation fails explicitly instead of becoming a count. Hex relationships
retain complete SQL predicates, including conjunctions and non-equality
comparisons, through the native custom-join representation.

Holistics imports raw AML, including supported aggregate AQL expressions.
Table-shaping AQL functions (`filter`, `where`, `group`, `select`) and metric
scope modifiers (`of_all`, `exclude`, `keep_grains`, `relative_period`,
`period_to_date`, `running_total`) are rejected at import. Removing these
functions changes query results. Rewrite the definition as explicit SQL with
the required filtering, grouping or window behavior before importing it.
Unsupported definitions also produce an import fidelity note when reporting
is enabled.

Omni imports `.view`, `.topic`, and their YAML-suffixed forms recursively,
including views outside the conventional `views` directory. A nested directory
with its own `model.yaml` is treated as a separate project. Canonical reference
headers determine view names when present. Native export retains source paths,
model configuration and topics, and writes joins in `relationships.yaml`.
New views use `views/<name>.view.yaml`.
Edits to mapped fields take precedence over preserved vendor metadata, including
removal of filters, SQL expressions and primary-key flags.
Query views and extends-only views are rejected because their vendor query
semantics cannot be reduced to a base table. Materialize them or provide an
explicit SQL source before importing.

Omni relationship predicates and declared join types are executable native
semantics. The `reversible` property must be Boolean and is retained for native
roundtrips. Topic configuration is retained for native export, but topic field
curation does not restrict Sidemantic queries; fidelity reporting identifies
that distinction. Retaining vendor configuration is not a guarantee that every
vendor feature is executable outside that vendor.
