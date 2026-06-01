# Rust Native Runtime Roadmap

Audit baseline: `5db9bd7c9d8b052ed573ede1bb8faa172b078512`

Date: 2026-05-22

## Scope Assumption

This plan assumes the Rust port only needs to support the native Sidequery/Sidemantic adapter formats:

- Native YAML semantic projects.
- Native SQL semantic definitions.

It does not assume Rust needs to parse LookML, MetricFlow, Hex, Rill, Malloy, Omni, Superset, GoodData, Snowflake Cortex, ThoughtSpot, Holistics, Tableau, AtScale SML, BSL, Yardstick, or Python definition files directly.

That assumption changes the product design. Rust should not be treated as a full Python parity port. It should become the canonical runtime for native semantic projects, while Python remains the import, migration, orchestration, and compatibility layer for non-native ecosystems.

## Target Architecture

The clean split is:

| Layer | Owner | Responsibility |
|---|---|---|
| Native YAML and SQL format contract | Shared, but Rust-driven | Define the stable semantic schema and behavior contract. |
| Native project validation | Rust | Validate models, fields, joins, metrics, parameters, pre-aggregations, and query references. |
| Native query compilation | Rust | Compile structured semantic queries into SQL. |
| Native semantic SQL rewrite | Rust | Rewrite a documented subset of semantic SQL into ordinary SQL. |
| Native execution | Rust | Execute generated SQL through ADBC where enabled. |
| DuckDB extension | Rust | Embed semantic definitions and query rewrite directly in DuckDB. |
| WASM/runtime embedding | Rust | Provide browser and embedding surfaces for native projects. |
| External format import | Python | Parse third-party semantic formats and normalize them into native YAML/SQL. |
| Existing Python CLI/API compatibility | Python | Preserve current user workflows while Rust becomes available behind explicit modes. |

The strategic goal is not "Rust parses everything Python parses." The strategic goal is:

> Native YAML/SQL is the contract. Rust is the canonical engine for that contract. Python is the adapter layer that converts the outside world into that contract.

## Current Reality Summary

Rust already has a substantial base:

- A real model graph.
- Native YAML loading.
- SQL semantic definition parsing.
- Partial Cube YAML loading.
- Structured query compilation.
- Semantic SQL rewriting for a constrained subset.
- Advanced metric generator paths.
- Pre-aggregation planning helpers.
- ADBC execution behind features.
- Python extension bindings.
- C ABI.
- WASM exports.
- HTTP/MCP/LSP/workbench feature surfaces.
- DuckDB extension integration.

Rust is not yet the full native runtime because:

- The native format is not formally frozen as a contract.
- There is no canonical fixture suite shared by Python and Rust.
- Rust validation and SQL generation are not yet the explicit source of truth for native projects.
- Python still owns the documented product CLI.
- Rust package and extension publication are not first-class release paths.
- CI does not yet enforce native Rust behavior on every repository change.

## Guiding Principles

1. Do not chase broad adapter parity in Rust.

   External adapters should remain Python-owned until there is a specific reason to port one. The Rust runtime should consume native YAML/SQL only.

2. Make native YAML/SQL boring and stable.

   The native format should be documented, versioned, tested, and treated like an ABI between Python, Rust, DuckDB, WASM, and future clients.

3. Prefer behavioral parity over SQL string parity.

   Rust and Python SQL strings may differ. The important contract is that the same native project and query produce the same result rows, validation behavior, and user-facing errors where feasible.

4. Be explicit about unsupported SQL.

   Rust should not become a general SQL rewriter. A narrow semantic SQL subset with excellent errors is better than a broad, surprising rewrite layer.

5. Promote Rust through explicit product modes before defaults.

   Rust should first be selectable with `--engine rust` or config, then eventually default for native projects once test coverage supports it.

6. Keep Python useful.

   Python is still the import ecosystem and mature workflow layer. Rust should reduce runtime risk for native projects, not delete Python's value.

## Phase 1: Freeze The Native Format Contract

The first step is to define what "native" means exactly.

### Deliverables

- `docs/native-format.md`
- A machine-readable schema or schema-like test corpus.
- A compatibility statement for native format version `1`.
- A native-format changelog policy.

### Format Versioning

Add a native format version field:

```yaml
version: 1
models:
  - name: orders
    table: orders
```

Decide whether the field is:

- Required for new files, optional for old files.
- Warned when absent.
- Inferred as version `1` when absent.

Recommended behavior:

- For now: absent means version `1`.
- New docs and exports always include `version: 1`.
- Rust warns, but does not fail, when the version is absent.
- Future breaking changes require `version: 2`.

### Model Contract

The native model contract should cover:

| Field | Required | Rust support target |
|---|---:|---|
| `name` | Yes | Stable. |
| `table` | One of `table`, `sql`, `source_uri` | Stable. |
| `sql` | One of `table`, `sql`, `source_uri` | Stable, including multiline SQL. |
| `source_uri` | One of `table`, `sql`, `source_uri` | Stable. |
| `description` | No | Preserve and expose. |
| `label` | No | Preserve and expose. |
| `extends` | No | Resolve inheritance before validation/query. |
| `primary_key` | No | Support string key. |
| `composite_primary_key` | No | Support list key. |
| `unique_keys` | No | Support list of key sets. |
| `relationships` | No | Validate targets and key presence. |
| `dimensions` | No | Validate names and SQL refs. |
| `metrics` | No | Validate names, dependencies, and types. |
| `segments` | No | Validate SQL shape enough to catch bad refs. |
| `pre_aggregations` | No | Validate referenced fields. |
| `default_time_dimension` | No | Validate it is a time dimension. |
| `default_grain` | No | Validate supported grain. |
| `meta` | No | Preserve as opaque JSON/YAML. |

### Dimension Contract

Native dimensions should support:

| Field | Notes |
|---|---|
| `name` | Unique within model. |
| `type` | `categorical`, `time`, `boolean`, `numeric`. |
| `sql` | SQL expression, may use `{model}` placeholder. |
| `description` | Preserved. |
| `label` | Preserved. |
| `granularity` | For time dimensions. |
| `supported_granularities` | For allowed grains. |
| `format` | Display formatting metadata. |
| `value_format_name` | Compatibility formatting metadata. |
| `parent` | Hierarchy parent. |
| `window` | Window expression if supported. |
| `public` | Visibility flag. |
| `meta` | Opaque metadata. |

Validation rules:

- Dimension names must be unique within the model.
- Time dimensions may specify `granularity` and `supported_granularities`.
- Non-time dimensions should reject time-only fields or warn consistently.
- `{model}` placeholders should be normalized before SQL generation.
- Unsupported raw references should produce a clear validation error.

### Metric Contract

Native metrics should support these types:

| Type | Required fields | Notes |
|---|---|---|
| `simple` | `agg` plus optional `sql` | Includes `sum`, `count`, `count_distinct`, `avg`, `min`, `max`, `median`, and any intentionally supported variance/stddev functions. |
| `derived` | `sql` or expression | Depends on other metrics or SQL expressions. |
| `ratio` | `numerator`, `denominator` | Should handle safe division. |
| `cumulative` | inner metric, window/time fields | Must be explicit about supported windows. |
| `time_comparison` | metric, comparison/period fields | Must define partition and time behavior. |
| `conversion` | entity, base event, conversion event | Single conversion metric per query initially. |
| `retention` | entity, cohort/event fields | Single retention metric per query initially. |
| `cohort` | cohort fields and inner metric | Single cohort metric per query initially. |

Additional fields:

- `filters`
- `fill_nulls_with`
- `drill_fields`
- `non_additive_dimension`
- `format`
- `value_format_name`
- `public`
- `meta`

Validation rules:

- Metric names must be unique within the model or graph scope, based on the declared placement.
- Every referenced metric must resolve.
- Ratio numerator and denominator must resolve.
- Derived metric refs must be acyclic.
- Complex metrics must reject unsupported combinations clearly.
- Metric filters must be resolvable or pass through under documented rules.

### Relationship Contract

Native relationships should support:

| Relationship type | Required data |
|---|---|
| `many_to_one` | `model`, `foreign_key`, target `primary_key` or explicit `primary_key`. |
| `one_to_one` | Same key support as many-to-one. |
| `one_to_many` | Reverse key support. |
| `many_to_many` | `through`, source/through/target key fields. |

Also support:

- Composite foreign keys.
- Composite primary keys.
- Custom relationship SQL.
- Optional join type if the project decides to expose it.

Validation rules:

- Related model must exist.
- Key columns should exist when schema metadata is available.
- Composite key lengths must match.
- Many-to-many through model must exist.
- Custom SQL relationships must still define enough metadata for graph reasoning, or they should be marked as opaque and limited.

### Segment Contract

Segments should support:

- `name`
- `sql`
- `description`
- `public`
- `meta`

Validation rules:

- Segment names must be unique within a model.
- `{model}` placeholder should be normalized.
- References to semantic fields inside segment SQL should either be supported or explicitly unsupported.

### Parameter Contract

Parameters should support:

- `name`
- `type`
- `default`
- `allowed_values`
- `description`

Supported parameter types should be kept intentionally small:

- `string`
- `number`
- `date`
- `unquoted`
- `yesno`

Validation rules:

- Defaults must match type.
- Allowed values must match type.
- Template interpolation must be deterministic.
- Unsafe interpolation modes should be documented.

### Pre-Aggregation Contract

Pre-aggregations should support:

| Field | Notes |
|---|---|
| `name` | Unique within model. |
| `type` | `rollup`, `original_sql`, `rollup_join`, `lambda` if all remain in scope. |
| `measures` | Referenced metrics. |
| `dimensions` | Referenced dimensions. |
| `time_dimension` | Referenced time dimension. |
| `granularity` | Time grain. |
| `partition_granularity` | Partition grain. |
| `refresh_key` | SQL or interval-based refresh. |
| `scheduled_refresh` | Boolean. |
| `indexes` | Name, columns, type. |
| `sql` | For original SQL. |
| `meta` | Opaque metadata. |

Validation rules:

- Referenced fields must exist.
- Time dimension must be time typed.
- Granularity must be valid and supported.
- Preaggregation matching should reject ambiguous matches.

### Unknown Fields Policy

Recommended policy:

- Rust should fail on unknown fields by default in strict mode.
- Rust should warn and preserve unknown `meta` fields only when they are under `meta`.
- Python exports should not emit unknown top-level fields.
- Migration tools should map unknown source-adapter fields into `meta.source` only when they are intentionally preserved.

This keeps native YAML/SQL clean and prevents accidental adapter-specific leakage.

## Phase 2: Make Rust Native Loading Definitive

Rust native loading should become stricter, more complete, and better tested than Python loading for native projects.

### YAML Loading Work

Add or harden coverage for:

- Single model files.
- Multi-model files.
- Directory recursion.
- Empty files.
- Mixed YAML and SQL directories.
- Graph-level metrics.
- Model-level metrics.
- Model inheritance.
- Parameters.
- Default time dimensions.
- Composite keys.
- Many-to-many relationships.
- Pre-aggregations.
- Metadata preservation.
- Duplicate handling.
- Invalid field names.

### SQL Definition Loading Work

The SQL definition format should cover:

- `MODEL`
- `DIMENSION`
- `METRIC`
- `SEGMENT`
- `RELATIONSHIP`
- `PREAGGREGATION`
- Graph-level metric definitions.
- Model-local metric definitions.
- SQL model body definitions.
- SQL with YAML frontmatter.
- Quoted identifiers.
- Comments.
- Multiline SQL.
- Mixed shorthand and verbose syntax.

### Required Negative Tests

Rust should reject:

- Duplicate models.
- Duplicate fields in the same namespace.
- Ambiguous unqualified metrics.
- Missing relationship targets.
- Missing metric dependencies.
- Invalid metric type payloads.
- Invalid aggregation names.
- Invalid time granularity.
- Circular inheritance.
- Circular metric dependencies.
- Invalid many-to-many through definitions.
- Invalid pre-aggregation field references.
- Multiple statements where only one definition is allowed.

### Round-Trip Tests

Add round-trip tests for:

- YAML -> graph -> canonical YAML.
- SQL -> graph -> canonical YAML.
- Directory -> graph -> canonical YAML.
- Python external adapter output -> native YAML -> Rust graph.

The goal is not byte-for-byte preservation. The goal is semantic preservation and canonical output.

## Phase 3: Build A Canonical Native Fixture Suite

The fixture suite is the most important practical deliverable. It should be shared by Python and Rust.

Suggested path:

```text
tests/native-fixtures/
  basic_model/
  sql_model/
  default_time_dimension/
  parameters/
  segments/
  metric_filters/
  derived_metrics/
  ratio_metrics/
  cumulative_metrics/
  time_comparison/
  conversion/
  retention/
  cohort/
  composite_keys/
  many_to_many/
  multi_hop_joins/
  fanout_symmetric_agg/
  preaggregations/
  table_calculations/
  jinja_templates/
  native_sql_definitions/
  mixed_yaml_sql_directory/
```

Each fixture should include:

```text
fixture/
  models/
    *.yml
    *.sql
  queries/
    *.query.yml
  expected/
    validation.json
    compiled.duckdb.sql
    result.json
  seed/
    duckdb.sql
  README.md
```

### Fixture Categories

#### Basic Model

Proves:

- One model loads.
- One dimension compiles.
- One metric compiles.
- Simple query executes.

#### SQL Model

Proves:

- `Model.sql` works.
- Generated SQL wraps source SQL safely.
- Column refs still resolve.

#### Default Time Dimension

Proves:

- Default time dimension is applied.
- Default grain is respected.
- Query can opt out.
- Explicit grain overrides default.

#### Parameters

Proves:

- Parameter defaults.
- Parameter overrides.
- Template interpolation.
- Allowed value validation.
- Type-specific formatting.

#### Segments

Proves:

- Model-local segments.
- Segment filters compose with normal filters.
- Segment SQL placeholder handling.

#### Metric Filters

Proves:

- Filtered metrics.
- Multiple metric filters.
- Filters on joined models.
- Filters with parameters.

#### Derived Metrics

Proves:

- Metric-to-metric dependencies.
- Expression dependencies.
- Dependency ordering.
- Cycle detection.

#### Ratio Metrics

Proves:

- Numerator and denominator resolution.
- Safe divide behavior.
- Ratio with joined dependencies.
- Ratio with filters.

#### Cumulative Metrics

Proves:

- Unbounded cumulative.
- Trailing-window cumulative.
- Grain-to-date if supported.
- Partitioning with non-time dimensions.

#### Time Comparison

Proves:

- Previous period.
- Previous year.
- Offset ratios.
- Partitioning by non-time dimensions.
- Missing time dimension error.

#### Conversion Metrics

Proves:

- Base event.
- Conversion event.
- Entity key.
- Window.
- Filters.
- Unsupported mixing with regular metrics.

#### Retention Metrics

Proves:

- Cohort event.
- Return event.
- Day, week, and month retention.
- Entity key.
- Unsupported mixing with regular metrics.

#### Cohort Metrics

Proves:

- Cohort assignment.
- Inner metric aggregation.
- Outer aggregation.
- Unsupported mixing with other metrics.

#### Composite Keys

Proves:

- Composite primary keys.
- Composite foreign keys.
- Join path resolution.
- SQL generation.

#### Many-To-Many

Proves:

- Through model.
- Two-hop join path.
- Fanout behavior.
- Symmetric aggregation where needed.

#### Multi-Hop Joins

Proves:

- Join path search.
- Ambiguous path handling.
- Filter on joined model.
- Dimension on joined model.

#### Fanout Symmetric Aggregation

Proves:

- Fanout detection.
- Symmetric sum.
- Symmetric count.
- Composite key support.
- Dialect-specific limitations.

#### Pre-Aggregations

Proves:

- Matching.
- Rejection when dimensions/metrics do not fit.
- Materialization SQL.
- Refresh planning.
- Fallback to base query.

#### Table Calculations

Proves:

- Formula calculations.
- Percent of total.
- Running total.
- Rank.
- Moving average if supported.

#### Jinja Templates

Proves:

- Template detection.
- Parameter interpolation.
- Safe defaults.
- Invalid template errors.

#### Native SQL Definitions

Proves:

- Native SQL syntax loads.
- SQL definitions and YAML definitions produce equivalent graphs.
- Mixed syntax works.

#### Mixed YAML/SQL Directory

Proves:

- Directory loading order is deterministic.
- Cross-file references resolve.
- Graph-level metrics can reference model-local metrics.

### Comparison Strategy

Each fixture should support three comparison modes:

1. Validation comparison:

   Python and Rust should agree whether the fixture is valid.

2. SQL shape comparison:

   SQL should be compared with normalization, not byte-for-byte equality. Compare selected clauses or parsed structure where practical.

3. Result comparison:

   Execute against DuckDB seed data and compare rows. This should be the highest-confidence check.

## Phase 4: Decide Rust vs Python Source Of Truth

Once native-only scope is accepted, Python does not need to remain the permanent authority for native query compilation.

Recommended transition:

1. Short term: Rust matches Python behavior on native fixtures.
2. Medium term: Rust becomes the canonical engine for native compile/rewrite/validation.
3. Long term: Python delegates native compile/rewrite/validation to Rust by default when `sidemantic_rs` is installed.

This avoids freezing Rust around Python implementation details that may be accidental.

### Source Of Truth Rules

| Concern | Short-term owner | Long-term owner |
|---|---|---|
| Native schema | Shared | Rust-driven shared contract |
| External adapters | Python | Python |
| Native validation | Python and Rust | Rust |
| Native SQL generation | Python and Rust | Rust |
| Native semantic SQL rewrite | Python and Rust | Rust |
| Native query execution | Python DB adapters and Rust ADBC | Split by runtime |
| Import migration | Python | Python |
| DuckDB parser extension | Rust | Rust |

## Phase 5: Close Rust Semantic Gaps Inside Native Scope

Even with native-only scope, the Rust runtime still needs to be semantically complete.

### Validation Correctness

Rust validation should cover:

- Model existence.
- Field existence.
- Metric dependency existence.
- Metric dependency cycles.
- Dimension SQL requirements.
- Join path existence.
- Relationship target existence.
- Relationship key compatibility.
- Parameter existence and type.
- Segment existence.
- Pre-aggregation field existence.
- Default time dimension validity.
- Complex metric required fields.
- Unsupported metric combinations.

Validation should return structured errors:

```json
{
  "code": "missing_metric_dependency",
  "message": "Metric orders.net_revenue references missing metric orders.gross_revenue",
  "model": "orders",
  "field": "net_revenue",
  "reference": "orders.gross_revenue"
}
```

### SQL Generation Completeness

Rust compile should cover:

- Dimensions.
- Metrics.
- Filters.
- Segments.
- Parameters.
- Joins.
- Composite joins.
- Many-to-many joins.
- Symmetric aggregation.
- Default time dimensions.
- Ungrouped queries.
- `order_by`.
- `limit`.
- `offset`.
- Derived metrics.
- Ratio metrics.
- Cumulative metrics.
- Time comparison metrics.
- Conversion metrics.
- Retention metrics.
- Cohort metrics.
- Table calculations.
- Pre-aggregation routing.

### Complex Metric Rules

Keep complex metric limitations explicit.

Recommended initial rules:

- Only one conversion metric per query.
- Conversion metrics cannot mix with regular metrics.
- Only one retention metric per query.
- Retention metrics cannot mix with regular metrics.
- Only one cohort metric per query.
- Cohort metrics cannot mix with regular metrics.
- Time comparison metrics can mix only where partitioning is unambiguous.
- Cumulative metrics require a time dimension unless the metric defines one.

These rules should be in docs and validation errors.

### Symmetric Aggregation

Rust should define:

- When fanout protection is required.
- Which aggregations can be made symmetric.
- Which dialects are supported.
- What happens when a dialect cannot support the needed operation.

Recommended behavior:

- DuckDB support first.
- PostgreSQL and BigQuery support only where tested.
- Clear error for unsupported dialects instead of unsafe SQL.

## Phase 6: Define The Rust Semantic SQL Subset

Rust should not try to rewrite arbitrary SQL.

### Supported Semantic SQL

Support:

```sql
select
  orders.status,
  orders.total_revenue
from orders
where orders.created_at >= date '2026-01-01'
order by orders.total_revenue desc
limit 10
```

Support:

```sql
select
  orders.total_revenue,
  orders.count
from metrics
where orders.status = 'paid'
```

Support:

```sql
with paid_orders as (
  select orders.id, orders.total_revenue
  from orders
  where orders.status = 'paid'
)
select *
from paid_orders
```

Support where semantics are clear:

- CTEs.
- Subqueries.
- Aliases.
- Semantic refs in `select`.
- Semantic refs in `where`.
- Semantic refs in `having`.
- Semantic refs in `order by`.
- `limit`.
- `offset`.

### Rejected Semantic SQL

Reject:

- Explicit `join` syntax.
- Raw aggregate functions not modeled as metrics.
- `select * from metrics`.
- DML.
- DDL, outside native semantic definitions.
- Multiple runtime query statements.
- Ambiguous unqualified refs.
- Unsupported correlated subqueries.
- SQL expressions that cannot be mapped to semantic fields safely.

### Error Philosophy

Errors should teach the supported subset.

Bad:

```text
Unsupported SQL
```

Good:

```text
Explicit JOIN syntax is not supported in semantic SQL. Reference fields from related models instead and let Sidemantic infer the join path.
```

## Phase 7: Make ADBC The Rust Execution Boundary

Rust should not recreate Python's named DB adapter ecosystem yet.

ADBC should be the execution boundary for Rust:

- DuckDB.
- SQLite.
- PostgreSQL.
- Other ADBC drivers only when real tests exist.

### Required ADBC UX

Add:

- Driver discovery diagnostics.
- Clear missing-driver errors.
- Connection URL normalization.
- Driver-specific option parsing.
- JSON output.
- CSV output.
- Pretty table output.
- Arrow IPC output.
- Row limit safeguards.
- Read-only mode where the driver supports it.

### Always-On Execution Tests

Always test:

- DuckDB ADBC.
- SQLite ADBC if stable in CI.

Gate with env/credentials:

- PostgreSQL ADBC.
- Snowflake ADBC.
- BigQuery ADBC if viable.

### CLI Shape

Recommended:

```bash
sidemantic-rs query \
  --models ./models \
  --metric orders.total_revenue \
  --dimension orders.status \
  --connection duckdb://data.db
```

Also support direct ADBC:

```bash
sidemantic-rs query \
  --models ./models \
  --metric orders.total_revenue \
  --driver adbc_driver_duckdb \
  --uri data.db
```

## Phase 8: Promote The DuckDB Extension

The DuckDB extension is the strongest Rust-only product path. It should become a first-class deployment target.

### Extension Goals

The extension should support:

- `sidemantic_load(yaml)`
- `sidemantic_load_file(path)`
- `sidemantic_models()`
- `sidemantic_rewrite_sql(sql)`
- `SEMANTIC SELECT`
- `SEMANTIC CREATE MODEL`
- `SEMANTIC CREATE METRIC`
- `SEMANTIC CREATE DIMENSION`
- `SEMANTIC CREATE SEGMENT`
- Active model switching.
- File-backed persistence.
- Recovery from invalid persistence files.

### Extension Alignment

Align extension syntax with the native SQL definition syntax. Avoid maintaining a separate DuckDB-only definition language.

Recommended rule:

> If a definition is accepted by the DuckDB extension, the same definition should be accepted by the Rust SQL loader.

### Extension Test Expansion

Add sqllogictests for:

- Multiple models.
- Model relationships.
- Composite joins.
- Many-to-many joins.
- Metric filters.
- Segments.
- Derived metrics.
- Ratio metrics.
- Default time dimensions.
- Pre-aggregation rewrite selection.
- Invalid definition errors.
- Active model switching.
- Persistence across restarts.
- Invalid persistence recovery.
- In-memory non-persistence.

### Extension Release Work

Add:

- Release workflow.
- Version compatibility check with Rust runtime.
- DuckDB community extension packaging if that is the target.
- Install docs that do not say "when published" once publication exists.

## Phase 9: Make Python Importers Emit Canonical Native

Since Rust will not parse external formats directly, Python importers should become compilers into native YAML/SQL.

### Importer Flow

For each external format:

1. Python parses the external project.
2. Python normalizes into the internal semantic graph.
3. Python exports canonical native YAML/SQL.
4. Rust validates the exported native project.
5. Rust compiles smoke queries.
6. Optional: Rust executes against DuckDB seed data when available.

### Adapter Output Contract

Every Python adapter should be able to answer:

- What native fields did it emit?
- What source features were fully preserved?
- What source features were partially mapped?
- What source features were dropped?
- What source metadata was preserved under `meta.source`?

### Migration Command

Recommended CLI:

```bash
sidemantic migrator convert \
  --from lookml \
  --input ./lookml_project \
  --output ./native_models \
  --validate-with rust
```

Or:

```bash
sidemantic convert \
  --from cube \
  --input ./cube_models \
  --output ./models \
  --engine rust
```

### Adapter Compatibility Docs

The existing compatibility docs should remain Python-owned. Add a native export status section to each:

```markdown
## Native Export

| Source feature | Native output | Fidelity |
|---|---|---|
| Cube measures | `Metric` | Full for common aggregate measures |
| Cube rolling windows | `Metric(type=cumulative)` | Partial |
| Cube access policies | `meta.source.cube.access_policy` or dropped | Unsupported at runtime |
```

## Phase 10: Replace Env Flags With Product Modes

Current Rust routing through environment variables is useful for internal testing, but it is not the right user interface.

### CLI Modes

Add explicit engine selection:

```bash
sidemantic validate ./models --engine rust
sidemantic query ./models --engine rust --metric orders.total_revenue
sidemantic rewrite ./models --engine rust "select orders.total_revenue from metrics"
```

### Config Mode

Support:

```yaml
runtime:
  engine: rust
  fallback: false
```

Recommended semantics:

- `engine: python`: use Python engine.
- `engine: rust`: use Rust engine and fail if unavailable.
- `engine: auto`: use Rust for native projects when installed, otherwise Python.

### Python API Mode

Support:

```python
SemanticLayer(runtime="rust", fallback=False)
```

or:

```python
SemanticLayer(engine="rust")
```

Pick one term and use it consistently.

### Env Vars

Keep environment variables only for:

- CI overrides.
- Strict parity tests.
- Temporary migration flags.

They should not be the main documented path for users.

## Phase 11: Native Compatibility CI

CI should treat native Rust compatibility as a repository-wide contract, not only a Rust-path check.

### Always-On CI Jobs

Add an always-on native compatibility job:

```bash
uv run pytest tests/native_compat
cd sidemantic-rs && cargo test
```

More specific:

- Python loads native fixtures.
- Rust loads native fixtures.
- Python and Rust agree on validation pass/fail.
- Rust compiles all query fixtures.
- Python and Rust execute selected fixtures against DuckDB.
- Result rows match.

### Rust Feature CI

On Rust path changes, keep the broader Rust matrix:

- `cargo fmt --check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test`
- feature checks for:
  - `python`
  - `python-adbc`
  - `wasm`
  - `mcp-server`
  - `runtime-server`
  - `runtime-lsp`
  - `workbench-tui`
  - `adbc-exec`
- maturin wheel build/smoke.
- WASM build/test.
- C ABI smoke.

### DuckDB Extension CI

On DuckDB or Rust path changes:

- Build Rust static/cdylib.
- Build DuckDB extension.
- Run sqllogictests.
- Run persistence tests.
- Run invalid persistence recovery tests.

### External Adapter CI

For Python adapter changes:

- Parse source fixture.
- Export native.
- Validate native with Rust.
- Run smoke compile with Rust.

This makes Rust a quality gate for adapter output without requiring Rust to parse adapters.

## Phase 12: Packaging And Release Strategy

The current Rust package is buildable but not yet productized as a normal user path.

### Artifacts

Keep these as separate artifacts:

| Artifact | Purpose |
|---|---|
| `sidemantic` Python package | Main Python CLI, adapters, existing UX. |
| `sidemantic-rs` Python extension | Rust runtime bridge for Python users. |
| Rust crate | Native runtime library and CLI. |
| DuckDB extension | Embedded DuckDB semantic runtime. |
| WASM package | Browser/runtime embedding. |

### Python Package Install Paths

Recommended:

```bash
uv add sidemantic
uv add sidemantic-rs
```

or optional extra if publication allows:

```bash
uv add "sidemantic[rust]"
```

The main Python package should not require Rust until Rust installation is reliable on all target platforms.

### Rust CLI Install Paths

Potential options:

```bash
cargo install sidemantic
```

or GitHub release binaries:

```bash
curl -LsSf https://.../sidemantic-rs/install.sh | sh
```

The install path should be decided before documenting Rust as a primary runtime.

### DuckDB Extension Install Path

Target:

```sql
install sidemantic from community;
load sidemantic;
```

Until then, docs should clearly say "build from source."

### Versioning

Track:

- Native format version: `1`.
- Python package version: `0.10.x`.
- Rust runtime version: `0.1.x`.
- DuckDB extension version: `0.1.x`.

Add compatibility docs:

| Python package | Rust runtime | Native format | DuckDB extension |
|---|---|---|---|
| `0.10.0` | `0.1.0` | `1` | `0.1.0` |

## Phase 13: Documentation Plan

Docs should be explicit that Rust is native-format only.

### New Docs

Add:

- `docs/native-format.md`
- `docs/rust-runtime.md`
- `docs/rust-engine-mode.md`
- `docs/duckdb-extension.md`
- `docs/native-fixtures.md`
- `docs/external-format-migration.md`
- `docs/runtime-feature-matrix.md`
- `docs/adbc-execution.md`
- `docs/wasm-runtime.md`

### Required Runtime Feature Matrix

Example:

| Capability | Python | Rust native runtime | DuckDB extension | WASM |
|---|---:|---:|---:|---:|
| Native YAML load | Yes | Yes | Via load function | Yes |
| Native SQL definitions | Yes | Yes | Yes | Partial/yes |
| LookML import | Yes | No | No | No |
| MetricFlow import | Yes | No | No | No |
| DuckDB execution | Yes | Via ADBC | Native DuckDB | No |
| PostgreSQL wire server | Yes | No | No | No |
| HTTP API | Yes | Feature-gated | No | No |
| Semantic SQL rewrite | Yes | Native subset | Native subset | Narrow subset |

### Unsupported In Rust

Document clearly:

- External format parsing.
- Python semantic definition files.
- PostgreSQL wire server replacement.
- Python widget/notebook UX.
- Python MCP app-mode resources, unless implemented separately.
- Broad named DB adapter behavior.
- General SQL rewrite.

## Phase 14: Migration Sequence

Recommended order:

1. Write native format spec.
2. Build native fixture suite.
3. Harden Rust YAML loader.
4. Harden Rust SQL definition loader.
5. Add Rust validation completeness.
6. Add Rust compile coverage for every fixture.
7. Add DuckDB result comparison fixtures.
8. Add explicit Python `--engine rust` mode.
9. Add Python API runtime selection.
10. Make Python importers export canonical native format.
11. Add Rust validation to adapter export tests.
12. Harden Rust semantic SQL rewrite subset.
13. Harden ADBC execution UX.
14. Expand DuckDB extension tests.
15. Add Rust package/release workflow.
16. Add DuckDB extension release workflow.
17. Document native-only Rust runtime.
18. Make Rust the recommended engine for native-only projects.
19. Later, consider making Rust the default for native projects when installed.
20. Only after a long overlap, consider deprecating Python-native SQL generation paths.

## Phase 15: Acceptance Criteria

Rust native runtime is ready to recommend when all of the following are true.

### Native Format

- Native format version is documented.
- YAML schema behavior is documented.
- SQL definition syntax is documented.
- Unknown-field behavior is documented.
- Deprecation behavior is documented.

### Loader

- Rust loads every valid native fixture.
- Rust rejects every invalid native fixture.
- Error messages identify the model/field/reference involved.
- Rust and Python agree on native validation pass/fail for the shared fixture suite.

### Compiler

- Rust compiles every query fixture.
- Compiled SQL executes against DuckDB seed data for all executable fixtures.
- Result rows match expected outputs.
- Complex metric limitations are validated before SQL generation.

### Rewriter

- Supported semantic SQL subset is documented.
- Every supported rewrite shape has tests.
- Every unsupported shape has a targeted error test.
- WASM rewrite limitations are documented separately.

### Execution

- DuckDB ADBC execution works in CI.
- SQLite ADBC execution works in CI if included.
- Missing driver errors are clear.
- JSON, CSV, table, and Arrow IPC outputs are tested if exposed.

### DuckDB Extension

- Extension builds in CI.
- sqllogictests cover model load, rewrite, semantic select, definitions, relationships, persistence, and errors.
- Install path is documented accurately.
- Persistence format is stable.

### Python Integration

- `--engine rust` exists for validate/query/rewrite.
- Python API supports explicit Rust runtime selection.
- Python external adapters can export canonical native files.
- Adapter output can be validated by Rust.

### Release

- Rust runtime has a release workflow.
- Python extension wheel has a release workflow.
- DuckDB extension has a release workflow or clearly documented source build path.
- Version compatibility is documented.

## Work Breakdown

### Track A: Native Contract

Tasks:

- Write native format spec.
- Add native format version.
- Define unknown-field policy.
- Define metadata preservation policy.
- Define SQL definition grammar.
- Define semantic SQL subset.

Output:

- Stable native contract.

### Track B: Fixture Suite

Tasks:

- Build fixture directory structure.
- Add seed data.
- Add expected validation files.
- Add expected result files.
- Add Python fixture runner.
- Add Rust fixture runner.
- Add result comparison harness.

Output:

- Shared Python/Rust contract tests.

### Track C: Rust Loader And Validation

Tasks:

- Fill YAML schema gaps.
- Fill SQL parser gaps.
- Add structured validation errors.
- Add inheritance validation.
- Add dependency cycle validation.
- Add pre-aggregation validation.

Output:

- Rust can reliably load and reject native projects.

### Track D: Rust SQL Compiler

Tasks:

- Fill structured query gaps.
- Harden joins and composite joins.
- Harden fanout protection.
- Harden derived and ratio metrics.
- Harden cumulative/time comparison metrics.
- Harden conversion/retention/cohort metrics.
- Add pre-aggregation routing coverage.

Output:

- Rust can compile native query corpus.

### Track E: Rust Semantic SQL Rewriter

Tasks:

- Document supported subset.
- Add rewrite tests for supported shapes.
- Add explicit rejection tests.
- Improve error messages.
- Keep WASM fallback documented separately.

Output:

- Predictable native semantic SQL behavior.

### Track F: ADBC Execution

Tasks:

- Improve driver config UX.
- Add connection URL normalization.
- Add output modes.
- Add always-on DuckDB ADBC tests.
- Add optional driver matrix.

Output:

- Rust can execute compiled SQL in controlled environments.

### Track G: DuckDB Extension

Tasks:

- Align parser syntax with native SQL definitions.
- Expand sqllogictests.
- Harden persistence.
- Add release packaging.
- Update docs.

Output:

- DuckDB extension becomes a credible user-facing Rust product.

### Track H: Python Bridge

Tasks:

- Add explicit engine mode.
- Add Python API runtime selection.
- Add no-fallback behavior.
- Improve missing Rust extension errors.
- Add native fixture parity tests.

Output:

- Python users can choose Rust without env vars.

### Track I: Adapter Export Pipeline

Tasks:

- Export canonical native YAML from each Python adapter.
- Validate adapter output with Rust.
- Add adapter compatibility docs for native export.
- Add migration command UX.

Output:

- External ecosystems flow into Rust through native files.

### Track J: Packaging And Release

Tasks:

- Decide crate/package names.
- Add Rust release workflow.
- Add `sidemantic-rs` wheel release workflow.
- Add DuckDB extension release workflow.
- Add version compatibility docs.

Output:

- Rust is installable without local maturin builds.

## Risk Register

| Risk | Impact | Mitigation |
|---|---|---|
| Native schema remains informal | Rust/Python drift | Version and document native format. |
| Fixture suite is too small | False confidence | Build fixtures by semantic capability, not by implementation module. |
| SQL string parity becomes a time sink | Slow migration | Prefer row-result parity and structured SQL shape checks. |
| Rust semantic SQL grows too broad | Unbounded complexity | Define supported subset and reject the rest. |
| ADBC driver variability | Runtime support appears flaky | Always test DuckDB, gate other drivers, improve diagnostics. |
| DuckDB extension diverges from Rust SQL definitions | Two native languages | Require extension definitions to parse with Rust loader. |
| Python adapter metadata leaks into native schema | Native format becomes unstable | Preserve source-specific data under `meta.source` only. |
| Rust package remains hard to install | Users cannot adopt runtime | Add release workflows before defaulting Rust. |
| Feature-gated surfaces look more mature than they are | User confusion | Maintain a runtime feature matrix with support levels. |

## Recommended Product Messaging

Use:

> The Rust runtime supports native Sidemantic YAML and SQL projects. Use Python to import or convert third-party semantic formats into native projects.

Avoid:

> The Rust runtime is the Python implementation rewritten in Rust.

Use:

> Rust is the canonical native runtime target.

Avoid:

> Rust supports every Sidemantic adapter.

Use:

> DuckDB extension support is a separate Rust-powered deployment path.

Avoid:

> The DuckDB extension has the same behavior as the Python CLI.

## Final Design Recommendation

Design the next stage around a narrow, strong promise:

> Native YAML/SQL projects should validate, compile, rewrite, and execute through Rust reliably across CLI, Python bridge, WASM, ADBC, and DuckDB extension surfaces.

Do not spend the next phase porting external adapters. Spend it making native projects a stable contract, making Rust authoritative for that contract, and making Python export into that contract.

The practical end state is:

1. Users with external semantic projects use Python to convert to native.
2. Users with native projects can run Rust directly.
3. Python can delegate native runtime work to Rust.
4. DuckDB can embed the Rust runtime without Python.
5. WASM can expose native validation/compile/rewrite in browser contexts.
6. CI proves all of this with shared fixtures.

That is a smaller claim than full Python parity, but it is much more credible and much more shippable.
