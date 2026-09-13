# Python-to-Rust compiler input

The Python bridge sends a versioned JSON snapshot of its existing semantic graph
to the Rust compiler. This replaces the native-YAML conversion previously used
by the compile, validate, and rewrite paths. Native YAML/SQL and external adapters
remain authoring inputs. The Python and Rust compilers still have separate graph
implementations; this bridge does not yet consolidate them into one compiler.

The Python producer snapshots existing definitions without constructing new
models, parsing SQL, assigning graph metrics to models, or invoking source
adapters. Rust validates the versioned input before projecting supported
definitions into its graph. The received JSON snapshot remains available alongside
that projection in `SemanticInput.source`. It is not the original authoring file
and does not preserve source syntax, comments, or formatting.

## Encoding

The JSON envelope contains:

| Field | Meaning |
| --- | --- |
| `version` | Integer contract version; currently `1`. |
| `input_dialect` | SQL expression context for the input definitions. |
| `models` | Model definitions, including dimensions, metrics, relationships and restrictions. |
| `metrics` | Graph-scoped metrics, retained separately from model metrics. |
| `metric_owners` | Only explicitly declared model ownership for graph-addressable metrics. |
| `parameters` | Parameter definitions. |
| `table_calculations`, `explores`, `saved_queries` | Retained graph definitions, with explicit rejection when unsupported. |
| `metadata`, `import_warnings` | Source and descriptive information. |
| `required_capabilities` | Declared special requirements, independently checked against the definitions by the receiver. |

Definition fields use the existing Python objects' default values when omitted.
Nondefault values are copied, including fields excluded from ordinary authoring
exports: logical data types, declared temporal roles, and relationship edge
identities. A model's `primary_key` is always either null or an ordered list of
columns. Unknown uniqueness never becomes an implicit `id` column.

Metric SQL is copied verbatim. `sql_is_complete` distinguishes a complete
expression from aggregation shorthand. Source dialect metadata is preserved;
unsupported expression contexts must be rejected rather than reinterpreted.

Descriptions and display metadata may have no execution effect. Their original
values remain in the received graph snapshot. Mandatory restrictions and unknown
semantic fields cannot be silently discarded during projection.

## Executable boundary

The first version accepts DuckDB input expressions and provides Rust compile,
reference-validation, and semantic-SQL rewrite entrypoints. Structured query
output dialect selection belongs to Rust SQL generation. Support for another
output dialect is not evidence that its warehouse has passed live execution
tests.

The structured compiler supports basic aggregations, filtered measures, declared
keyed joins, complete aggregate expressions, and graph metric binding. It also
supports:

- Independent source aggregation for cross-model ratios and derived metrics,
  including source filters, aggregate filters, and grouped results.
- Separate relationship role instances, nested and scoped role names, inactive
  edge exclusion, and rejection of ambiguous join paths. Explicit adapter join
  kinds retain their direction and row-preservation behavior.
- Model access policies, SQL-literal-safe user attributes, row policies,
  invariant filters, and opt-in field visibility. Restrictions reach the source
  CTEs of aggregate and temporal child queries.
- Running, rolling, and grain-to-date aggregates over period outputs, plus
  calendar comparisons and offset ratios. Windows partition by the selected
  non-time dimensions. Named comparisons use calendar intervals even without
  a declared grain; `prior_period` without a resolved grain retains previous-row
  semantics. Summing period-level distinct counts is not a distinct count over
  the combined underlying rows.

Configured rollups can be bypassed for raw queries. Active row policies always
bypass rollups, including when routing was requested. Rollup routing through
this boundary is not yet qualified. The legacy Rust materialization helper
rejects models with invariant filters instead of discarding those filters.

Remaining capability gates include policy-bearing SQL rewrite requests,
non-DuckDB policy output, many-to-many role paths, computed primary-key
dimensions, unsafe dimension/measure alias collisions across aggregate grains,
complete-expression measure filters, null-fill options, raw cumulative windows,
and unqualified conversion, retention, cohort, and non-additive metric shapes.
Deserialization alone is not evidence of executable support.

The existing YAML-based Rust utility entrypoints remain for compatibility.
They are not an automatic fallback for the new compiler boundary and do not
establish version-1 conformance for other host integrations.

For CLI semantic SQL, scoped `SELECT ... FROM metrics` queries with column
projections and aliases use the structured compiler, including cross-model
calculations, role dimensions and temporal metrics. The wrapper preserves the
requested output columns, ordering by projected fields, and pagination.
Expressions or additional clauses outside this subset report an unsupported
rewrite capability. This is a bounded rewrite path, not full semantic-SQL parity.

## Engine selection

- `python` uses the Python path.
- Strict `rust` either executes the supported path or reports unavailable or
  unsupported requirements.
- `auto`, or explicitly permitted fallback, may select Python for a known
  unsupported requirement or unavailable versioned backend.

Malformed definitions, invalid references, and unexpected compiler failures
remain errors. They do not trigger speculative execution through another
engine. `last_engine_selection` records the selected engine and fallback reason
on the Python layer and rewriter. CLI fallback diagnostics go to stderr so SQL
and data stdout remain usable in pipelines.

## Acceptance evidence

`tests/semantic_conformance/` loads actual native, Cube, and Ossie source fixtures into
synthetic DuckDB data and compares result columns and rows against independently
specified expectations. Unsafe or unsupported cases assert errors explicitly.
Rust tests require an installed extension; skipped Rust parameters do not count
as acceptance.

`tests/core/test_semantic_handoff.py` checks inert snapshots and typed errors.
Rust's `semantic_input` tests check decoding, keys, scope, dialects and rejection
without relying on Python preprocessing.

This contract does not change the default engine, retire the Python compiler,
or claim that WASM, the DuckDB extension, and the Python binding already expose
identical capabilities. Each host needs corresponding acceptance evidence
before its default or implementation ownership changes.
