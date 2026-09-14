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
| `table_calculations`, `explores`, `saved_queries` | Retained named catalogs; unused entries do not block query compilation. |
| `metadata`, `import_warnings` | Source and descriptive information. |
| `required_capabilities` | Declared special requirements, independently checked against the definitions by the receiver. |

Definition fields use the existing Python objects' default values when omitted.
Nondefault values are copied, including fields excluded from ordinary authoring
exports: logical data types, declared temporal roles, and relationship edge
identities. A model's `primary_key` is always either null or an ordered list of
columns. Unknown uniqueness never becomes an implicit `id` column.

Metric SQL is copied verbatim into the source snapshot. `sql_is_complete` distinguishes a complete
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
  including source filters, aggregate filters, and grouped results. Child outputs
  are normalized before recombination, so dimensions and measures from different
  owners can share a basename without changing their grouping or population.
  Public collisions use owner-qualified names; qualified ordering and filters
  retain their semantic binding. Truly duplicate child output names remain an
  explicit unsupported shape.
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
- Numeric and string null defaults on simple, derived and ratio results. Defaults
  apply to metric dependencies after aggregation, including absent source leaves
  after cross-source recombination; they do not create policy-excluded groups.
  Cumulative and time-comparison defaults apply after the window or comparison
  calculation, including missing prior periods and zero prior denominators for
  ratios/percent changes. They do not fill the underlying period values or create
  missing periods. Existing window frames, partitions, ordering and policies
  remain in effect. Offset-ratio defaults apply after division by the prior-period
  denominator, preserving NULLIF protection for zero denominators. Source-local
  snapshot defaults apply after snapshot selection and the final aggregation;
  they do not replace null inputs before count or average. Source-local cohort
  defaults likewise wrap only the outer result, preserving inner values and HAVING.
  Conversion and retention defaults remain explicitly unsupported.
  Filled cumulative metrics reject comparison
  offsets; filled time comparisons reject cumulative windows and grain-to-date
  controls rather than silently ignoring them.
- Existing cumulative `window_expression` fields accept `SUM`, `AVG`, `MIN`,
  `MAX`, or `COUNT` of one `base.output` metric reference, with an optionally
  quoted simple output identifier. The input is a grouped period metric value;
  it is not a physical source column. `window_order` names a selected period
  output column or an unambiguous selected dimension name, resolving its explicit
  or default grain, and defaults to the selected time dimension. Windows retain
  selected non-time partitions. `window_frame` accepts preceding `ROWS` frames
  or calendar `RANGE` frames (day, week, month, year), ending at `CURRENT ROW`;
  the default is unbounded preceding rows. Frames also apply to cumulative metrics
  with an aggregate and base-metric reference. Combining a frame with `window` or
  `grain_to_date` is rejected. Other strict expressions or frames remain unsupported. Legacy Rust
  utility expression discovery and Python's broader expression passthrough
  remain available through their existing paths.


Configured ordinary rollups reach the Rust graph through this boundary. Routing
supports single-source sum/count/min/max queries over compatible stored dimensions
and time grains. Simple predicates on stored non-time dimensions are checked through
the parsed expression, including IN, BETWEEN and IS NULL. Uncovered dimensions,
time predicates, functions, unsupported measure states and incomplete rollup
populations use raw source SQL, with `used_preagg=false`. A bucketed timestamp cannot
serve a finer grain or an untruncated timestamp, and week buckets cannot serve months.
Cross-source aggregate child queries retain raw-source planning.

Active row policies and invariant filters always bypass rollups, including when
routing was requested. Access and visibility checks still run before routing.
Python materialization preserves invariants; the legacy Rust materialization helper
continues to reject models with invariant filters instead of discarding them. Rust
materialization supports filtered sum/count/min/max states, including distinct row
and non-null value count populations, and rejects unsupported states such as AVG,
custom SQL, partitioned builds and partial build ranges. Lambda freshness behavior
remains explicitly unsupported by the versioned boundary.

Filtered complete measures are supported when their SQL AST is exactly
`SUM(input)`, `AVG(input)`, `COUNT(input)`, `COUNT(DISTINCT input)`,
`MIN(input)`, or `MAX(input)` over local physical row inputs, or exactly
`COUNT(*)`, `COUNT(1)`, or `COUNT(NULL)`. Row inputs may use addition, subtraction,
multiplication, modulo, `CASE`, and `COALESCE`, with local columns, literals,
comparisons, boolean conditions, null checks, ranges, and literal lists. The whole
input is filtered after evaluation, so excluded rows cannot contribute a `CASE`
or `COALESCE` fallback. Each new expression must reference a local physical column.
These aggregates can be declared inside a model or as graph metrics with an explicit
model owner. The source declaration remains unchanged; its executable copy
uses the ordinary per-measure filtered aggregate path. Local qualified columns
are normalized without changing string literals, and each filter is parenthesized
before conjunction. Filters use physical values even when a semantic dimension
shares the column name. Independent measures retain independent populations.
Average counts each qualifying non-null source row in its denominator, while
distinct count collapses repeated qualifying values and excludes nulls. These
states use the keyed fanout-safe aggregate planner. Filtered `COUNT(*)` and `COUNT(1)` count
qualifying source rows even when their values are null; it returns zero for an
empty population, a group without matches, or an absent cross-source count leaf.
Keyed joins count each qualifying source key once per selected group. `COUNT(NULL)`
returns zero while still enforcing the owner's mandatory restrictions; it does
not create groups excluded by those restrictions. Explicitly owned graph aggregates
use the same source population and retain their public output names.
For fanout SUM and AVG,
that planner selects one joined row per requested group and source primary key
before aggregating the original filtered value. It preserves floating-point
values and returns null for groups containing no qualifying non-null values.

This checked lowering accepts ordinary local comparisons, boolean combinations,
null checks, ranges, and literal lists. Other filtered complete constant inputs,
division (whose integer semantics vary by dialect), string literals containing the legacy `{model}` placeholder, casts,
arbitrary functions, aggregate combinations, distinct averages, windows,
subqueries, foreign-model inputs or predicates, and unresolved templates remain
explicitly unsupported. Unowned graph measures also remain unsupported on this path.
Row-count execution coverage targets DuckDB and PostgreSQL. TSQL count widths
require separate qualification, so Python TSQL retains its existing
complete-expression path for `COUNT` and `COUNT_BIG`. TSQL filtered complete row
counts are not qualified by this change.

Remaining capability gates include policy-bearing SQL outside the scoped
`FROM metrics` subset, policy output outside DuckDB/PostgreSQL, many-to-many paths without explicit keyed junctions or with custom join SQL,
unsupported computed-key query shapes, genuinely duplicate child output aliases,
unsupported complete-expression filter shapes, temporal/null-fill combinations, cumulative windows outside the bounded subset,
and unqualified conversion, cohort, and non-additive metric shapes. Retention
has a bounded dedicated path described below.

Deserialization alone is not evidence of executable support.

Structured query compilation, query-reference validation, and SQL rewriting
preserve unused table-calculation, Explore, and saved-query declarations without
executing them. Catalog entries must be objects with unique non-empty names;
their execution fields remain in the source snapshot. Whole-graph
`SemanticInput::from_json` validation still rejects unsupported catalogs.

The Python layer resolves saved queries and Explores before dispatch, preserving
visibility checks, selection and expression allowlists, mandatory filters,
parameters, defaults, row limits, and saved-query immutability. The resolved
`consumption_base_model` query field anchors ordinary Rust queries to the Explore
model even when only related-model fields are selected. Base and intermediate
model policies remain mandatory; related-only metrics and dimensions preserve
the authorized base population. Same-owner derived and ratio metrics use this
ordinary route. Independent cross-source aggregates, temporal metrics, and
snapshot routes reject anchored requests with precise
`query.consumption_base_model.*` capability errors until their population
semantics are qualified.

Raw runtime requests that select `explore`, `saved_query`, or nonempty
`table_calculations` fail with typed capability errors, including requests through
rewrite context. Resolve supported consumption contracts through the Python
layer; raw `consumption_base_model` supplies the population anchor, not named
contract authorization. Catalog presence alone never activates a contract.
Model policies and invariant filters remain mandatory.

Model-owned retention metrics support one source in DuckDB, with `entity`,
`cohort_event`, optional `activity_event`, and day/week/month periods. The first
qualifying event determines each entity's cohort. Activity is distinct per entity
and period; entities without returning activity remain in the cohort denominator.
Only observed activity periods are emitted, through the inclusive `periods`
bound (default 28). NULL entities and cohorts without a non-NULL event date do
not produce retention rows.

Query filters, metric filters, model invariants, and rendered row policies scope
both cohort and activity populations before either is calculated. The model's
default time dimension is preferred, otherwise its first time dimension is used.
Entity and time dimensions may map to physical source expressions. The fixed
outputs are `cohort_date`, `days_since`/`weeks_since`/`months_since`, `active_users`,
`cohort_size`, and `retention_pct`. Ordering and pagination apply to these outputs.

Selected dimensions, graph-level or wrapped retention metrics, combinations with
other metrics, joined populations, aggregate predicates, window/subquery source
expressions, ungrouped queries, table calculations, and non-DuckDB outputs remain
explicitly unsupported. Result acceptance is in
`tests/semantic_conformance/test_retention_parity.py`; enabling this path requires
the freshly built Rust extension to pass those cases, not only SQL compilation.

The existing YAML-based Rust utility entrypoints remain for compatibility.
They are not an automatic fallback for the new compiler boundary and do not
establish version-1 conformance for other host integrations.

For CLI semantic SQL, scoped `SELECT ... FROM metrics` queries with column
projections and aliases use the structured compiler, including cross-model
calculations, role dimensions and temporal metrics. The wrapper preserves the
requested output columns, ordering by projected fields, and pagination.
Caller attributes and opt-in visibility reach the same Rust policy planner as
structured queries. Access gates, row filters, invariants, and relationship-role
populations therefore also apply to supported scoped rewrites. Policy-bearing
SQL can nest those semantic leaves inside derived-table SELECTs and
nonrecursive CTEs. Every leaf, including unused CTE bodies, runs policy
preparation independently. Outer projections, DISTINCT, WHERE, GROUP BY,
HAVING, ORDER BY and pagination operate on the secured results. Each wrapper
has one derived-table or in-scope CTE source. CTE column aliases and nested
shadowing are retained; user CTE bindings are renamed internally so they cannot
capture physical reads introduced by the compiler.

Set operations, recursive CTEs, wrapper joins, scalar/predicate subqueries,
physical source reads, DML and other source shapes remain unsupported; they
cannot enter the legacy Rust rewrite path. Security failures never trigger
fallback.

CLI `query` and `rewrite` accept `--user-attrs-file attributes.json` containing a
JSON object and `--enforce-visibility`. These apply equally to execution,
`query --dry-run`, and SQL-only rewriting. Attribute files supply local caller
context; missing required attributes fail closed. Secured CLI requests bypass
pre-aggregations.

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

`test_rollup_routing.py` additionally creates materialized tables through the Python
and Rust helpers and compares real raw/routed execution against independent expected
rows. Poisoned unscoped rollups verify mandatory restriction bypass. These are
synthetic fixtures, not production workload or performance qualification.

`tests/core/test_semantic_handoff.py` checks inert snapshots and typed errors.
Rust's `semantic_input` tests check decoding, keys, scope, dialects and rejection
without relying on Python preprocessing.

This contract does not change the default engine, retire the Python compiler,
or claim that WASM, the DuckDB extension, and the Python binding already expose
identical capabilities. Each host needs corresponding acceptance evidence
before its default or implementation ownership changes.

### WASM host

The generated WASM module exposes `wasm_compile_with_semantic_input`,
`wasm_validate_with_semantic_input`, `wasm_rewrite_with_semantic_input`, and
`wasm_rewrite_with_semantic_input_context`. Arguments are the same JSON strings
as the Python boundary; compilation returns SQL and failures throw a message.
Validation returns a JSON array of reference errors and does not authorize a
query. Caller attributes and visibility enforcement belong in the structured
query or rewrite context. The context-free rewrite does not bypass mandatory
policies.

CI executes the generated Node module and runs its SQL in DuckDB against an
independently expected tenant-filtered population. This qualifies that host
boundary for the tested subset, not browser packaging, DuckDB-extension host
parity, or every semantic feature. WASM uses its host stack directly; it does
not spawn the native semantic compiler worker thread.

### Snapshot measures

Direct simple measures from one model can declare `non_additive_dimension`,
`non_additive_window` (`min` or `max`, default `max`), and optional
`non_additive_window_groupings`. The compiler selects the first or last snapshot
independently for each measure before aggregation. Snapshot values use the declared
dimension expression and grain; a day-grain timestamp selects every tied row on
the selected day. Omit that grain to select by the timestamp itself. Additive siblings retain all
rows. Without explicit groupings, selected query dimensions partition snapshot
selection; with explicit groupings, those fields and any selected coarse bucket
of the snapshot dimension partition it. Grouping by the raw snapshot dimension
needs no masking. Row restrictions apply before snapshot selection.

Fanout, calculated wrappers, multiple metric owners, aggregate predicates,
colliding output aliases, ungrouped output, and rollup routing
remain gated for this snapshot path. Null defaults apply only to the final
aggregate after snapshot selection; null source inputs and additive sibling
populations are preserved. This does not add raw-row cumulative
semantics: cumulative references still operate on period outputs.
### Two-event conversion

Source-local two-event conversion metrics (`base_event`, `conversion_event`,
`entity`, `conversion_window`) can use the strict Rust DuckDB path. Conversion
counts distinct base entities with a target event inside the inclusive interval,
and divides by distinct base entities. Empty denominators return null. Grouping
attributes are attributed to the base event. Query filters, metric filters,
invariants, and caller policies constrain both event populations.

Joined populations, graph-scoped two-event conversion metrics,
calculated wrappers, mapped entity/event/time source names, quoted output names,
and other output dialects remain gated in this first qualified subset.

### Multi-step conversion

Direct model conversion metrics with two or more `steps` predicates use the
existing sequential funnel algorithm through the strict DuckDB boundary. Each
step finds the earliest qualifying event at or after the previous step's timestamp;
equal timestamps qualify. There is no conversion-window option for this form.
Selected grouping values belong to the first step and remain attached when later
events have different values. The first event is chosen per entity and selected
group, so an entity entering multiple groups belongs to each group's population.
Each group advances from its own first-step timestamp; the same later event can
qualify for multiple groups. Group counts therefore need not sum to the ungrouped
distinct count. Repeated events within a group do not multiply entity counts.

Outputs are selected dimensions, `total_entities`, each `step_N_count`, and the
metric name containing the final step count. The denominator includes only
distinct non-null first-step entities. A first-step entity with a null timestamp
can enter the denominator but cannot advance. Empty ungrouped populations return
zero counts. Ordering and pagination use these output columns.

Entity and time dimensions can map to row-local source expressions. Step predicates
refer to physical source columns; query and metric filters resolve declared dimensions.
All steps read the same policy-, invariant-, query-, and metric-filtered population.
The strict wrapper validates expressions and projects internal source columns before
calling the existing sequential generator. Graph-scoped metrics, mixed metrics,
joined populations, aggregate/window/subquery source expressions, colliding output
names (including dimensions named `entity` or `step_N_ts`), rollup routing,
null-fill options, and other output dialects remain gated.

## PostgreSQL policy output

The versioned bridge can generate PostgreSQL output for policy-bearing structured
queries (`query.dialect = "postgres"`) and scoped semantic SQL rewrites
(`rewrite` context `output_dialect = "postgres"`; Python bridge keyword
`output_dialect="postgres"`). Input expressions and input semantic SQL remain
DuckDB dialect. This does not enable PostgreSQL input expressions or change CLI
engine defaults. Access checks, typed caller attributes, row filters, invariants,
and opt-in visibility use the same policy preparation as DuckDB output.

The Rust CI job executes both output paths against a PostgreSQL 16 service using
its newly built Python extension wheel. The synthetic acceptance population
checks tenant and invariant exclusions, quoted/injection-shaped values, numeric,
boolean and null attributes, access denial, and visibility. Local runs without
`SIDEMANTIC_TEST_POSTGRES_DSN` skip this host test; a configured job fails if the
service, driver, extension or expected rows are unavailable. PostgreSQL parity
is not qualified until that execution job passes. Other policy output dialects
remain typed unsupported requirements.

PostgreSQL policy predicates emit target SQL before structured CTE assembly.
The rewrite path preserves its DuckDB intermediate and emits PostgreSQL only
from the final AST. `year(date)` policies explicitly lower to `extract(year
from date)` and have a PostgreSQL row-result test. Date-difference predicates
remain unsupported: PostgreSQL elapsed-duration lowering does not preserve
DuckDB calendar-boundary counting for timestamps. Raw SQL nodes and unresolved
generic function nodes are also rejected for PostgreSQL policy output.

Public PostgreSQL layers keep graph definitions in the version-1 DuckDB input
contract while selecting PostgreSQL output. At the Python bridge, PostgreSQL
request SQL and structured filter/order expressions are normalized explicitly
to the compiler's input syntax. This conversion preserves quoted identifiers,
escaped strings and resolved null ordering; it does not bind semantic references
or rewrite graph definitions. Multiple statements and trailing scalar clauses
are rejected. Explicit non-DuckDB graph-expression dialect metadata remains
unsupported. The PostgreSQL CI corpus exercises `SemanticLayer.compile`,
`query`, `sql`, and `QueryRewriter` with a real PostgreSQL adapter and Rust runtime.
Named many-to-many relationships use a separate junction SQL instance for each
role, even when roles share the same physical junction table. They require an
explicit `through` model, `through_foreign_key` and `related_foreign_key`, and
known endpoint primary keys with matching arities. Composite key arrays are
accepted for junction matching, including ungrouped queries. Fanout aggregation
requiring symmetric deduplication still requires a single-column measure primary
key; composite measure keys fail with `aggregation.requires_single_primary_key`.
Junction policies are applied to each role instance using the canonical
junction declaration. Measures retain their source-key grain across duplicate
junction rows. Inactive relationships remain excluded.
### Source-local cohorts

Direct model cohort metrics and graph metrics with an explicit `metric_owners`
entry can aggregate source rows per entity, apply `having`
to declared inner result columns, and aggregate the surviving groups. Query and
entity dimensions are carried through both levels, including explicit time
buckets. Outer `count` counts inner groups (including a qualifying null-entity
group); `count_distinct` without SQL counts non-null entities. Outer expressions
bind inner aliases, not physical source columns. Filters and mandatory policies
apply before the inner aggregation, with each predicate parenthesized.

This subset requires row-local scalar source expressions. Subqueries, windows,
and aggregates hidden in source dimensions, inner SQL or row filters are
rejected. Graph cohorts require their declared source owner; entity or output
column names do not infer ownership. The declared owner also determines the
mandatory restrictions applied to the source population. Joined populations,
unowned graph cohorts and calculated wrappers remain gated. HAVING and outer expressions must reference
available inner columns; other aggregate contexts are not silently inferred.

The WASM SQL parser has a host-specific admission limit of 16 nested
parenthesis/bracket/brace/CASE constructs, 32 operators per expression chain,
16 set operations per parser input, and 48 combined live nesting/operator
ancestors. Flat projections and independent
clauses do not share an operator budget; long unary chains do.
These are conservative fixed-host-stack limits, not limits on native compilation
or source-file bytes. Dialect tokenization keeps strings, quoted identifiers,
and comments out of structural nesting counts. Excess inputs return a SQL parse
error before recursive parsing; generated intermediate SQL is subject to the same
limits. Native hosts retain the existing larger worker stack.
