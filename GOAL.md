# Sidemantic Deep Review Remediation Goal

Date: 2026-05-31

This backlog captures the deep repo review across Python correctness, Python/Rust parity,
DuckDB extension wiring, packaging, CI, and docs. The goal is to make Sidemantic's
CLI-first workflows reliable, keep Python and Rust semantics aligned where both claim
support, and harden the DuckDB extension persistence/loading paths.

## Stacked PR Split

- PR #183 / `review/cli-ci-release-hardening`: CLI validation, strict loading, CI/release wiring, optional-extra
  docs, Pyodide/Rust release documentation, and Rust parity matrix restoration.
- PR #184 / `review/python-native-contract`: Python/Rust native contract alignment, shared native fixtures,
  schema alias generation, graph-level variance SQL normalization, DB adapter safety, and query correctness fixes.
- PR #185 / `review/duckdb-extension-hardening`: Rust FFI and DuckDB extension persistence, active-model/context
  handling, parser-aware sidecar editing, extension build/link wiring, and sqllogictest coverage.
- `review/remediation-goal-log`: this remediation ledger, kept separate from runtime/code changes.

## Validation Snapshot

- `uv run ruff check ...`: passed on 2026-05-30 with the CI exclude list.
- `uv run ruff format --check ...`: passed on 2026-05-30 with the CI exclude list.
- `uv run pytest -v` on local CPython 3.13.0: passed on 2026-05-30 with 4035 passed,
  39 skipped, 77 deselected, 18 xfailed, and 50 warnings.
- `CARGO_TARGET_DIR=/tmp/sidemantic-dd3e-target cargo fmt --check`: passed on 2026-05-30.
- `CARGO_TARGET_DIR=/tmp/sidemantic-dd3e-target cargo test --test native_fixtures`: passed on 2026-05-30.
- Rust DuckDB ADBC native fixture execution passed on 2026-05-30 against DuckDB `v1.4.2`
  using `libduckdb-osx-universal.zip`.
- DuckDB extension `make deps DUCKDB_VERSION=v1.4.2`: passed on 2026-05-30.
- DuckDB extension `make`: passed on 2026-05-30 with the default CMake generator after restoring
  bare `make` to the upstream `all: release` target. `GEN=ninja make` was not used locally because
  Ninja is not installed on this machine; CI still exercises the Ninja generator.
- DuckDB extension `make test`: passed on 2026-05-30 with 69 assertions across 4 sqllogictests.
- Targeted remediation checks on 2026-05-30 passed for native key parsing/bridge serialization,
  zero pagination, PostgreSQL encoded credentials, Rust many-to-many graph keys, and Rust inline
  aggregate metric YAML parsing.
- Query-history adapter parameter validation checks passed on 2026-05-30 for BigQuery, Snowflake,
  ClickHouse, and Databricks.
- Strict directory loading checks passed on 2026-05-30, including explicit lenient mode and CLI
  `info` failure on malformed detected semantic files.
- Base-install Python `sidemantic validate` checks passed on 2026-05-30, including workbench-extra
  import blocking and nonzero exit on validation errors.
- Native Python model and metric inheritance checks passed on 2026-05-30 for inherited model
  sources/keys and inherited model-local plus graph-level metric fields.
- Statistical aggregation parity checks passed on 2026-05-30 for Python metric parsing, Rust enum/YAML/SQL parser/runtime/generator paths,
  and shared Python/Rust native fixtures including DuckDB ADBC fixture execution.
- Compact SQL and multi-model SQL loader checks passed on 2026-05-30 for Rust parser/loader unit tests,
  shared Python/Rust native fixture compilation, and DuckDB ADBC fixture execution.
- Relationship default-key parity checks passed on 2026-05-30 for Python relationship helpers,
  Python/Rust graph path inference, shared fixture compilation, and DuckDB ADBC fixture execution.
- Native compatibility alias checks passed on 2026-05-30 for Python adapter parsing, Rust native YAML parsing,
  shared fixture compilation, and DuckDB ADBC fixture execution.
- Python native strictness checks passed on 2026-05-30 for unknown model/dimension/metric/relationship/pre-aggregation
  field rejection, compatibility alias preservation, and shared invalid fixture loading.
- Python native metadata/visibility round-trip checks passed on 2026-05-30 for model `meta`,
  dimension `meta`/`public`/`supported_granularities`, and metric `meta`/`public`.
- Top-level metric/parameter native contract checks passed on 2026-05-30 for Python parameter export,
  dotted top-level metric names, Rust exact metric-name resolution, shared fixture compilation, and DuckDB
  ADBC fixture execution.
- Custom relationship SQL parity checks passed on 2026-05-30 for Python native parse/export, Python generated
  joins, Rust graph custom joins, shared fixture compilation, and DuckDB ADBC fixture execution.
- Python native pre-aggregation round-trip checks passed on 2026-05-30 for `sql`, `meta`, refresh keys,
  indexes, scheduling, partitioning, build ranges, existing shared routing fixtures, and DuckDB ADBC fixture execution.
- Rust directory loader scope checks passed on 2026-05-30 for documented native/Cube-only YAML plus native SQL
  loading, Rust CLI help text, shared fixture compilation, and formatting/lint gates.
- Shared table-calculation contract checks passed on 2026-05-30 for Python post-query fixture execution,
  Rust SQL/window fixture execution through DuckDB ADBC, and formatting/lint gates.
- Rust parity matrix checks passed on 2026-05-30 for committed strict-mode subsystem statuses,
  SQL-generator strict mode, query-validation strict mode, and formatting/lint gates.
- DuckDB extension FFI state checks passed on 2026-05-30 for atomic model/item persistence,
  autoload clearing/replacement, deterministic active-model behavior, parser-aware sidecar grouping,
  segment shorthand parsing, shared native fixture compilation, DuckDB ADBC fixture execution, and lint/format gates.
- DuckDB extension build/docs checks passed on 2026-05-30 for Makefile dry-run dependency acquisition and
  CMake target-triple/static-library path wiring review; final local `make deps`, `make`, and `make test`
  also passed after fixing the Makefile default target and sqllogictest isolation/error-message expectations.
- Adversarial follow-up checks on 2026-05-31 passed:
  - `uv run pytest tests/test_metric_expressions.py tests/adapters/sidemantic_adapter/test_parsing.py tests/test_loaders.py -q`
    with 45 passed and 5 warnings.
  - `uv run ruff check` and `uv run ruff format --check` on the touched Python files.
  - GitHub workflow YAML parse check for the edited workflow files.
  - `cargo fmt --check`, targeted Rust `auto_dimensions` regression test, and
    `CARGO_TARGET_DIR=/tmp/sidemantic-dd3e-target cargo clippy --all-targets --all-features -- -D warnings`.
  - DuckDB extension `make` and `make test`, with 71 assertions across 4 sqllogictests.
- Sidecar hardening checks on 2026-05-31 passed:
  - `CARGO_TARGET_DIR=/tmp/sidemantic-dd3e-target cargo test ffi::tests` with 23 passed.
  - `cargo fmt --check`.
  - `CARGO_TARGET_DIR=/tmp/sidemantic-dd3e-target cargo clippy --all-targets --all-features -- -D warnings`.
  - DuckDB extension CMake rebuild target `sidemantic_loadable_extension`.
  - DuckDB extension `make test`, with 71 assertions across 4 sqllogictests.

## Adversarial Subagent Follow-Up (2026-05-31)

- [x] Fix ratio metrics resolving dotted references by splitting before exact graph metric lookup.
  - Source: Python adversarial review.
  - Done: ratio refs now use `SemanticGraph.resolve_metric_reference()` so exact graph-level metric names win
    over `model.metric` splitting; executable regression covers the invalid `orders_cte.revenue_raw` SQL case.

- [x] Resolve native model inheritance after directory-wide Python loading.
  - Source: Python adversarial review.
  - Done: adapter parsing is isolated from auto-registration during directory loads, native inheritance resolves
    after all files are parsed, and strict mode raises on unresolved native parents without registering partial models.

- [x] Reject native custom relationship SQL without `{from}` and `{to}` placeholders.
  - Source: Python/DuckDB adversarial review.
  - Done: Python native parsing fails loudly for placeholder-free custom relationship SQL instead of preserving
    a value the graph intentionally ignores for backwards-compatible FK relationships.

- [x] Fix DuckDB extension quoted model-name mismatch checks and unsigned local-load docs.
  - Source: DuckDB adversarial review.
  - Done: parser-extension body-name extraction handles quoted names and rejects mismatches; sqllogictests cover
    the case, and docs/demo commands consistently use `duckdb -unsigned`.

- [x] Harden release and base-install CI wiring.
  - Source: CI/docs adversarial review.
  - Done: PyPI publish builds before pushing the version commit/tag, base-install `serve` smoke is timeout-guarded,
    optional CLI docs use the right extras, chart example uses `sidemantic[charts]`, Pyodide metadata checks include
    chart heavies, Rust binary release tags must match crate version, and DuckDB extension release input is guarded
    to the vendored DuckDB/tooling version.

- [x] Make unsupported Rust `auto_dimensions: true` fail instead of being dropped.
  - Source: Rust adversarial review.
  - Done: Rust native schema accepts `auto_dimensions: false` for compatibility but rejects `true` with a structured
    validation error and documentation now states the runtime limitation.

- [x] Implement cross-platform and concurrent sidecar persistence hardening.
  - Source: DuckDB adversarial review.
  - Done: sidecar writes now acquire a same-directory lock file across the full read/validate/atomic-replace
    sequence, stale locks are cleaned after a timeout window, Windows replacement uses `MoveFileExW` with
    `MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH`, and autoloading an in-memory/no-path database clears
    any stale context state. DuckDB CMake now links platform native libraries for the Rust staticlib on macOS,
    Linux, and Windows-targeted builds, with an override cache variable for additional target-specific libraries.
  - Caveat: Windows behavior was implemented behind `cfg(windows)` and compile-guarded by dependency resolution,
    but not executed locally on Windows hardware in this run.

## Phase 1: Correctness and Runtime Parity

- [x] Fix Rust many-to-many-through joins with composite primary keys.
  - Symptom: Rust truncates source and target PK lists to the first column, while Python keeps full key lists.
  - Impact: composite many-to-many joins can produce incorrect rows.
  - Evidence: `sidemantic/core/semantic_graph.py:243`, `sidemantic-rs/src/core/graph.rs:293`.
  - Acceptance: add a shared fixture combining composite keys and many-to-many-through joins; Python and Rust compile equivalent join predicates.
  - Done 2026-05-30: native/Python/Rust now accept `through_foreign_key_columns` and `related_foreign_key_columns`; graph paths and generated SQL preserve full composite junction predicates, with Python/Rust focused tests and a shared native fixture.

- [x] Make Python native YAML parse documented explicit key-list fields.
  - Symptom: Python ignores `primary_key_columns`, relationship `foreign_key_columns`, and relationship `primary_key_columns`.
  - Impact: documented native YAML can silently fall back to `id` or single-key defaults.
  - Evidence: `docs/native-format.md:86`, `docs/native-format.md:329`, `sidemantic/adapters/sidemantic.py:261`, `sidemantic/adapters/sidemantic.py:402`.
  - Acceptance: native YAML using only explicit `*_columns` fields preserves composite keys in Python, Rust bridge YAML, and generated joins.
  - Done 2026-05-30: parser, Rust bridge YAML, and generated join predicates are covered by focused tests.

- [x] Align Python and Rust handling of aggregate SQL metrics without explicit `agg`.
  - Symptom: Python normalizes `sql: SUM(amount)` into a simple `sum` metric; Rust classifies it as a derived metric.
  - Impact: fanout protection, filters, pre-aggregation matching, validation, and SQL generation can diverge.
  - Evidence: `sidemantic/core/metric.py:87`, `sidemantic-rs/src/config/schema.rs:604`, `sidemantic-rs/src/sql/generator.rs:504`.
  - Acceptance: shared native fixture for `sql: SUM(amount)` without `agg` compiles the same logical aggregate in both runtimes.
  - Done 2026-05-30: Rust native YAML now normalizes top-level inline aggregate calls into simple metrics; complex aggregate expressions remain derived.

- [x] Make CLI loading strict where correctness matters.
  - Symptom: `load_from_directory()` catches parse errors, logs warnings, and returns partial graphs.
  - Impact: `validate`, `query`, `info`, and export flows can operate on incomplete semantic layers.
  - Evidence: `sidemantic/loaders.py:140`, `sidemantic/loaders.py:153`, `sidemantic/cli.py:114`.
  - Acceptance: CLI commands fail if any semantic file fails to parse, with an explicit lenient mode only where needed.
  - Done 2026-05-30: detected semantic file parse errors now raise by default before partial graph registration; `strict=False` preserves the old skip-and-warn behavior for explicit lenient callers.

- [x] Make `sidemantic validate` a base-install, noninteractive command.
  - Symptom: Python validation imports the optional Textual workbench UI.
  - Impact: the primary CLI validation workflow requires `sidemantic[workbench]` and is not CI-friendly.
  - Evidence: `sidemantic/cli.py:995`, `sidemantic/workbench/__init__.py:41`, `sidemantic/workbench/validation_app.py:5`.
  - Acceptance: base `sidemantic validate` returns nonzero on validation errors and does not require Textual; workbench can render the same structured result.
  - Done 2026-05-30: Python validation uses a shared noninteractive validation runner; the Textual validation app renders the same report when the optional workbench extra is installed.

- [x] Resolve native model and metric inheritance in the Python native adapter.
  - Symptom: the adapter preserves `extends` but does not apply model or metric inheritance resolution before validation/loading.
  - Impact: valid inherited native models can fail or be dropped; inherited metric fields are not applied.
  - Evidence: `sidemantic/adapters/sidemantic.py:193`, `sidemantic/adapters/sidemantic.py:395`, `sidemantic/core/inheritance.py:127`.
  - Acceptance: native fixtures cover child models/metrics that omit inherited required fields.
  - Done 2026-05-30: native parsing resolves model inheritance, model-local metric inheritance, and graph-level metric inheritance while preserving explicit-null override behavior.

- [x] Fix dotted graph-level metric reference resolution.
  - Symptom: validators and generators split dotted references as exactly `model.metric`.
  - Impact: graph-level metrics whose names contain multiple dots can crash or be misclassified.
  - Evidence: `sidemantic/validation.py:311`, `sidemantic/sql/generator.py:330`, `sidemantic/sql/generator.py:1981`.
  - Acceptance: central metric resolver checks exact graph metric names before interpreting `model.metric`.
  - Done 2026-05-30: `SemanticGraph.resolve_metric_reference()` now resolves exact graph metrics before model-scoped metrics, and validation/generation use it for multi-dot and exact-collision regressions.

- [x] Stop swallowing malformed embedded SQL definition blocks.
  - Symptom: `parse_sql_graph_definitions()` catches broad exceptions and returns empty metrics/segments/parameters.
  - Impact: typos inside `sql_metrics` or `sql_segments` can silently remove definitions.
  - Evidence: `sidemantic/core/sql_definitions.py:676`, `sidemantic/adapters/sidemantic.py:210`, `sidemantic/adapters/sidemantic.py:358`.
  - Acceptance: malformed embedded SQL fails validation with file/block context.
  - Done 2026-05-30: SQL definition parsing now propagates parse errors, rejects unsupported top-level SQL statements, and native YAML embedded block errors include file plus `sql_metrics`/`sql_segments` context.

- [x] Preserve `limit=0` and `offset=0`.
  - Symptom: generator and rewriter paths use truthiness checks for numeric pagination values.
  - Impact: callers requesting `LIMIT 0` or explicit `OFFSET 0` get different SQL.
  - Evidence: `sidemantic/sql/generator.py:1902`, `sidemantic/sql/generator.py:2273`, `sidemantic/sql/query_rewriter.py:5565`.
  - Acceptance: use `is not None` consistently and add zero-value tests.
  - Done 2026-05-30: generator and post-processing rewriter paths use explicit `None` checks, with direct compile/rewrite regressions.

- [x] Decode PostgreSQL URL credentials.
  - Symptom: `PostgreSQLAdapter.from_url()` passes percent-encoded usernames/passwords through unchanged.
  - Impact: passwords containing reserved URL characters fail, especially when config dicts are converted to URLs.
  - Evidence: `sidemantic/db/postgres.py:190`, `sidemantic/core/semantic_layer.py:1136`.
  - Acceptance: percent-encoded credentials round-trip from config dict to adapter.
  - Done 2026-05-30: Postgres URL parsing decodes credentials, and dict-to-URL conversion percent-encodes userinfo with `safe=""`.

- [x] Validate query-history adapter parameters.
  - Symptom: `days_back` and `limit` are interpolated directly into warehouse SQL in multiple adapters.
  - Impact: public Python API callers can create invalid or unsafe SQL with non-int, huge, or negative values.
  - Evidence: `sidemantic/db/bigquery.py:179`, `sidemantic/db/snowflake.py:235`, `sidemantic/db/clickhouse.py:226`, `sidemantic/db/databricks.py:188`.
  - Acceptance: shared coercion/bounds helper or bound parameters where supported.
  - Done 2026-05-30: shared validator coerces safe integer strings, rejects bool/negative/zero/non-int/oversized values, and enforces Snowflake's 7-day `INFORMATION_SCHEMA` lookback limit.

## Phase 2: Rust Native Runtime Parity

- [x] Add Rust support for Python/schema-supported statistical aggregations.
  - Symptom: Python and `sidemantic-schema.json` support `stddev`, `stddev_pop`, `variance`, and `variance_pop`; Rust does not.
  - Impact: native files accepted by Python/schema fail Rust validation or are unrepresentable.
  - Evidence: `sidemantic/core/metric.py:36`, `sidemantic-schema.json:248`, `sidemantic-rs/src/core/model.rs:119`.
  - Acceptance: Rust enum, YAML validation, SQL parser, SQL rendering, and fixtures support the same aggregation set.
  - Done 2026-05-30: Rust now accepts and renders the full statistical aggregation set across native YAML, SQL parser, runtime helpers, generator, catalog/materialization paths, and shared fixtures; Python inline parsing and SQL rendering also normalize `variance_pop` to DuckDB-compatible `VAR_POP`.

- [x] Support or explicitly reject Python compact SQL model syntax in Rust.
  - Symptom: Python supports `model orders from orders (...)`; Rust only detects legacy `MODEL (` files.
  - Impact: SQL-first native files that work in Python fail in Rust.
  - Evidence: `sidemantic/core/sql_definitions.py:203`, `sidemantic-rs/src/config/loader.rs:228`, `sidemantic-rs/src/config/sql_parser.rs:982`.
  - Acceptance: Rust parses compact syntax or returns a precise unsupported-syntax error; shared fixtures cover compact SQL.
  - Done 2026-05-30: Rust now parses compact `model name from source (...)` blocks, including multiple compact models and derived SQL sources; legacy multi-`MODEL(...)` files load as separate models instead of collapsing definitions onto the last model, with parser/loader tests and a shared executable native fixture.

- [x] Align default relationship key semantics.
  - Symptom: Python defaults omitted `foreign_key` differently by relationship type; Rust uses `{name}_id` for every relationship type.
  - Impact: same YAML can generate different joins for `one_to_many` or `one_to_one`.
  - Evidence: `sidemantic/core/relationship.py:63`, `sidemantic-rs/src/core/model.rs:714`, `sidemantic-rs/src/core/graph.rs:355`.
  - Acceptance: either matching defaults or explicit-key validation for relationship types where defaults are ambiguous.
  - Done 2026-05-30: Rust relationship helpers and graph paths now match Python compatibility defaults; docs recommend explicit CLI-authored keys while documenting omitted-key behavior, and shared fixtures cover omitted `one_to_many`, omitted `one_to_one`, omitted `many_to_one`, and explicit FK with target-PK inference.

- [x] Decide whether Python compatibility aliases are native contract.
  - Symptom: Python accepts aliases such as `measures`, dimension/metric `expr`, top-level metric `measure`, and `auto_dimensions`; Rust rejects them with `deny_unknown_fields`.
  - Impact: Python-compatible native YAML can fail Rust loading.
  - Evidence: `sidemantic/adapters/sidemantic.py:279`, `sidemantic/adapters/sidemantic.py:294`, `sidemantic/adapters/sidemantic.py:436`, `sidemantic-rs/src/config/schema.rs:38`.
  - Acceptance: either Rust serde aliases match Python or Python rejects these aliases for the native contract.
  - Done 2026-05-30: compatibility aliases are accepted input and canonical exports still use `metrics`/`sql`; Rust now accepts model `measures`, dimension/metric `expr`, metric `measure`, and model `auto_dimensions`, with docs and a shared native fixture covering the contract.

- [x] Align Python native strictness with the documented/Rust native contract.
  - Symptom: docs describe native v1 as strict and Rust uses `deny_unknown_fields`, while Python manually copies known fields and silently ignores misspellings.
  - Impact: invalid YAML can pass Python workflows, fail Rust workflows, or silently drop semantic intent.
  - Evidence: `docs/native-format.md:539`, `sidemantic-rs/src/config/schema.rs:21`, `sidemantic/adapters/sidemantic.py:320`, `sidemantic/adapters/sidemantic.py:448`.
  - Acceptance: Python native loading rejects unknown model/dimension/metric/relationship/pre-aggregation fields, while preserving any deliberate alias whitelist.
  - Done 2026-05-30: Python native loading now rejects unknown root, model, dimension, metric, relationship, segment, pre-aggregation, refresh-key, index, and parameter fields before permissive model construction; accepted alias fields remain whitelisted and shared invalid fixtures verify Python/Rust rejection.

- [x] Preserve schema-supported metadata and visibility fields in Python native parse/export.
  - Symptom: core and Rust support `meta`, `public`, and dimension granularity metadata, but Python native parse/export omits or drops parts of that surface.
  - Impact: Python native round-trips can lose governance/visibility metadata even when the schema and Rust preserve it.
  - Evidence: `sidemantic/core/dimension.py:32`, `sidemantic/core/metric.py:328`, `sidemantic-rs/src/config/schema.rs:113`, `sidemantic/adapters/sidemantic.py:588`, `sidemantic/adapters/sidemantic.py:720`.
  - Acceptance: YAML round-trip tests preserve model/dimension/metric `meta`, `public`, and dimension `supported_granularities`.
  - Done 2026-05-30: Python native parse/export now preserves model `meta`, dimension `meta`/`public`/`supported_granularities`, and metric `meta`/`public`; graph-level derived metric export no longer writes a non-contract `metrics` dependency field that strict native parsing rejects.

- [x] Define graph-level metric and parameter runtime contract across Python and Rust.
  - Symptom: Python stores graph-level metrics and parameters, while Rust has no graph-metric store and coerces top-level metrics to a model owner; Python export also omits parameters.
  - Impact: shared native files with true graph-level metrics or parameters are not portable across runtime engines.
  - Evidence: `sidemantic/core/semantic_graph.py:41`, `sidemantic/adapters/sidemantic.py:205`, `sidemantic-rs/src/core/graph.rs:80`, `sidemantic-rs/src/config/loader.rs:859`, `sidemantic/adapters/sidemantic.py:275`.
  - Acceptance: shared fixtures cover true graph-level metric names and top-level `parameters`; either both runtimes support them equivalently or unsupported cases fail loudly.
  - Done 2026-05-30: Python native export now preserves top-level parameters; the native contract documents that
    Rust assigns top-level metrics to exactly one inferred owner and fails otherwise; Rust query/default-time-dimension
    resolution now checks exact metric names before `model.metric` parsing, so dotted top-level metric names assigned
    to one model compile and execute through the shared Python/Rust fixture suite.

- [x] Align custom relationship SQL support.
  - Symptom: Rust supports relationship `sql` placeholders; Python native relationships ignore `sql`.
  - Impact: same YAML can compile with custom joins in Rust and default FK/PK joins in Python.
  - Evidence: `docs/native-format.md:336`, `sidemantic-rs/src/config/schema.rs:198`, `sidemantic/adapters/sidemantic.py:260`, `sidemantic/core/relationship.py:18`.
  - Acceptance: Python supports custom join SQL end to end or the field is Rust-only and explicitly rejected/flagged in Python.
  - Done 2026-05-30: Python `Relationship` now preserves `sql`, native parse/export round-trips it, graph
    adjacency carries custom conditions with reversed placeholders, and generated joins use `{from}`/`{to}`
    custom SQL instead of FK/PK predicates; shared fixtures verify null-safe custom joins compile and execute
    in both Python and Rust.

- [x] Export Python native pre-aggregations and align fields.
  - Symptom: Python parses `pre_aggregations` but `_export_model()` does not emit them; Rust supports extra `sql` and `meta` fields.
  - Impact: Python native round-trips drop pre-aggregations and some Rust fields.
  - Evidence: `sidemantic/adapters/sidemantic.py:379`, `sidemantic/adapters/sidemantic.py:673`, `sidemantic/core/pre_aggregation.py:49`, `sidemantic-rs/src/config/schema.rs:214`.
  - Acceptance: parse/export round-trip preserves pre-aggregations, or unsupported fields fail loudly.
  - Done 2026-05-30: Python `PreAggregation` now preserves native `sql` and `meta`, native parsing passes them
    through, and native export emits pre-aggregations with measures, dimensions, time grain, partitioning, build
    ranges, scheduled refresh, refresh keys, indexes, and metadata; focused round-trip tests and the existing shared
    pre-aggregation routing fixture both pass.

- [x] Clarify Rust directory loader scope.
  - Symptom: Python directory loading auto-detects many formats; Rust loads only `.yml/.yaml/.sql` and only Sidemantic/Cube YAML.
  - Impact: users may expect Rust CLI/runtime loaders to support the same "universal importer" surface.
  - Evidence: `sidemantic/loaders.py:16`, `sidemantic-rs/src/config/loader.rs:344`, `sidemantic-rs/src/config/loader.rs:504`.
  - Acceptance: docs and CLI behavior make Rust loader scope explicit, or unsupported formats route through Python adapters before Rust.
  - Done 2026-05-30: native-format docs and Rust loader comments now state the Rust loader only accepts native
    Sidemantic YAML/SQL plus Cube YAML; Rust CLI help tells users to convert LookML, MetricFlow, Hex, Rill,
    Malloy, and other external formats through the Python CLI/API before using the Rust runtime.

- [x] Define shared table-calculation contract.
  - Symptom: Python and Rust support different calculation types, and Python fixture execution strips Rust table calculations.
  - Impact: table calculations are not a shared query feature.
  - Evidence: `sidemantic/core/table_calculation.py:23`, `sidemantic-rs/src/core/table_calc.rs:11`, `tests/native_compat/test_basic_model_fixture.py:114`.
  - Acceptance: a documented shared subset with parity fixtures, or Rust-only status kept outside shared fixtures.
  - Done 2026-05-30: table calculations now have a documented shared subset; the shared fixture runs through
    Python's post-query `TableCalculationProcessor` and Rust's SQL/window implementation, with Python/Rust
    result checks covering the same expected output.

- [x] Restore or remove stale Rust parity matrix wiring.
  - Symptom: `sidemantic/rust_parity.py` expects `docs/rust-parity-matrix.json`, but the file is absent.
  - Impact: strict parity helpers can default subsystems to `python_only` and the gating surface can rot.
  - Evidence: `sidemantic/rust_parity.py:20`.
  - Acceptance: committed matrix plus tests, or removal of unused matrix code.
  - Done 2026-05-30: restored `docs/rust-parity-matrix.json` as the strict-mode source of truth, marked
    `sql_generator_entrypoint` and `semantic_core_query_validation` as Rust-backed, kept the semantic SQL
    rewriter opt-in, and added a regression test that loads the committed matrix.

## Phase 3: DuckDB Extension Wiring

- [x] Make DuckDB semantic definition persistence atomic.
  - Symptom: FFI appends model definitions to disk before `state.graph.add_model()` succeeds, while metric/dimension/segment writes have separate graph/persistence ordering risks.
  - Impact: failed duplicate creates or failed item persistence can leave the sidecar file and in-memory graph inconsistent.
  - Evidence: `sidemantic-rs/src/ffi.rs:240`, `sidemantic-rs/src/ffi.rs:253`, `sidemantic-rs/src/ffi.rs:820`, `sidemantic-duckdb/src/sidemantic_extension.cpp:468`.
  - Acceptance: every `SEMANTIC CREATE` path validates and mutates atomically with persistence, or rolls back both graph and sidecar on failure.
  - Done 2026-05-30: model and model-item create paths now stage graph mutations, validate candidate sidecar
    content, write sidecars via temp-file rename, and swap in-memory state only after persistence succeeds; focused
    FFI tests cover duplicate creates, invalid replacements, and item persistence failures.

- [x] Fix `sidemantic_load_file` for multi-model SQL files.
  - Symptom: SQL files with multiple `MODEL` blocks are parsed as one final model with accumulated fields.
  - Impact: public file loader corrupts or drops multi-model SQL definitions.
  - Evidence: `sidemantic-rs/src/config/loader.rs:238`, `sidemantic-rs/src/config/sql_parser.rs:948`.
  - Acceptance: multiple model blocks produce multiple graph models; sidecar-style SQL round-trips through `sidemantic_load_file`.
  - Done 2026-05-30: Rust SQL parsing now returns multiple legacy models and `load_from_file()`/`sidemantic_load_file` share the corrected loader path; focused loader tests verify model-local metrics remain attached to their original model.

- [x] Make DuckDB autoload reconcile state, not only add/replace parsed definitions.
  - Symptom: missing, removed, or invalid sidecar definitions do not clear existing in-memory FFI state for the same DB path.
  - Impact: stale semantic definitions can survive database reloads in the same process.
  - Evidence: `sidemantic-rs/src/ffi.rs:615`, `sidemantic-rs/src/ffi.rs:631`, `sidemantic-rs/src/ffi.rs:646`.
  - Acceptance: autoload starts from a fresh graph for that context or explicitly reconciles deletions/errors.
  - Done 2026-05-30: autoload now replaces the context state from a full sidecar parse, clears state for missing
    or empty sidecars, and clears plus returns an error for invalid persisted definitions.

- [x] Set `active_model` deterministically after YAML/file loads.
  - Symptom: `sidemantic_load_yaml_for_context()` and `sidemantic_load_file_for_context()` merge models but do not set a deterministic active model.
  - Impact: unqualified `SEMANTIC CREATE METRIC/DIMENSION/SEGMENT` can attach to an arbitrary HashMap iteration model.
  - Evidence: `sidemantic-rs/src/ffi.rs:102`, `sidemantic-rs/src/ffi.rs:155`, `sidemantic-rs/src/ffi.rs:756`.
  - Acceptance: require explicit `model.metric` after multi-model loads or set active model deterministically and document it.
  - Done 2026-05-30: FFI YAML/file loaders use loader metadata order; single-model loads set that model active,
    multi-model loads clear implicit active state, and unqualified model-item creates now require an active model
    or explicit `model.name` target.

- [x] Make DuckDB extension persistence parsing parser-aware.
  - Symptom: splitting/removing persisted definitions relies on text searches for `MODEL`.
  - Impact: comments or string literals containing `MODEL` can corrupt sidecar rewrite/loading.
  - Evidence: `sidemantic-rs/src/ffi.rs:319`, `sidemantic-rs/src/ffi.rs:659`.
  - Acceptance: persisted definitions use structured blocks or the SQL parser's statement blocks.
  - Done 2026-05-30: autoload uses the full SQL loader instead of keyword splitting, and sidecar block editing
    now groups statement ranges while ignoring comments/quoted strings; focused tests cover `MODEL` inside
    comments and SQL string literals.

- [x] Fix or remove accepted-but-unparseable DuckDB segment shorthand.
  - Symptom: C++ accepts `CREATE SEGMENT name AS ...`; Rust parser does not support that form.
  - Impact: SQL-first users get a late Rust parse error after C++ accepts the statement shape.
  - Evidence: `sidemantic-duckdb/src/sidemantic_extension.cpp:269`, `sidemantic-rs/src/config/sql_parser.rs:859`.
  - Acceptance: Rust parses segment shorthand or C++ rejects it before FFI.
  - Done 2026-05-30: Rust SQL parser now supports `SEGMENT name AS expr` and prefixed segment property syntax;
    FFI and DuckDB sqllogictest coverage exercise simple segment creation.

- [x] Improve `SEMANTIC CREATE MODEL name (...)` SQL ergonomics.
  - Symptom: C++ discards the model identifier and requires `name` to be repeated inside the body.
  - Impact: natural SQL syntax fails unless docs' duplicate-name pattern is followed.
  - Evidence: `sidemantic-duckdb/src/sidemantic_extension.cpp:434`, `sidemantic-duckdb/README.md:33`.
  - Acceptance: identifier is used as model name when body lacks `name`, or docs explicitly call out the limitation.
  - Done 2026-05-30: the DuckDB parser shim injects the outer model identifier when the body omits `name`,
    rejects outer/body name mismatches, and docs/tests now use the natural `CREATE MODEL name (...)` form.

- [x] Fix DuckDB cross-target Rust static-library wiring.
  - Symptom: CMake ignores DuckDB extension makefile `Rust_CARGO_TARGET` and hardcodes `target/<profile>` artifact paths.
  - Impact: cross-platform extension builds can link the host static library or fail to find the target artifact.
  - Evidence: `sidemantic-duckdb/CMakeLists.txt:31`, `sidemantic-duckdb/CMakeLists.txt:14`.
  - Acceptance: cargo build uses the target triple and artifact path for each DuckDB target.
  - Done 2026-05-30: CMake now consumes `Rust_CARGO_TARGET`/`SIDEMANTIC_CARGO_TARGET`, passes `--target` to
    Cargo, resolves target-specific artifact directories, and chooses MSVC vs GNU/staticlib names from the Rust target.

- [x] Reconcile DuckDB extension dependency acquisition docs with CI.
  - Symptom: README mentions submodule setup, while CI deletes/clones `duckdb` and `extension-ci-tools`; local checkout has `extension-ci-tools` vendored and no `duckdb/`.
  - Impact: local build instructions are not reproducible from a normal clone.
  - Evidence: `sidemantic-duckdb/.gitmodules:1`, `.github/workflows/ci.yml:270`.
  - Acceptance: one documented, tested setup path for local builds.
  - Done 2026-05-30: removed stale submodule metadata, added `make deps DUCKDB_VERSION=...` to fetch the DuckDB
    source used by CI, reused that target in CI/release workflows, and updated local build docs to match.

## Phase 4: Packaging, CI, Docs, and Release Wiring

- [x] Repair Python test failures on supported/newer Python versions.
  - Symptom: local CPython 3.13 run failed 66 tests.
  - Main clusters: `sqlglot.dialects.postgres/bigquery` lookup failures, API server `LookupError` failures, schema generation failures, planner/security/join execution cases.
  - Evidence: `sidemantic/adapters/yardstick.py:41`, `sidemantic/core/semantic_layer.py:744`, `tests/server/test_api_server.py`.
  - Acceptance: either declare Python 3.13 unsupported or make the full suite pass under 3.13 in addition to CI's 3.11/3.12.
  - Done 2026-05-30: full local CPython 3.13.0 `uv run pytest -v` passed after fixing the SQL dialect lookup
    compatibility path, strict native parsing edge cases, inheritance handling, cumulative metric aliasing,
    custom relationship SQL compatibility, and symmetric aggregate composite-key SQL generation regressions.

- [x] Update obsolete CLI query examples.
  - Symptom: examples use `--sql` and `-c` forms that no longer match the Typer command.
  - Impact: CLI-first onboarding commands fail before query execution.
  - Evidence: `sidemantic/cli.py:540`, `examples/ecommerce/README.md:46`.
  - Acceptance: docs use positional SQL plus `--models`, `--connection`, or `--db`.
  - Done 2026-05-30: ecommerce examples now use positional SQL with `--models`/`--db`, matching the Typer
    command surface covered by CLI dry-run tests.

- [x] Fix `serve` docs and missing-extra behavior.
  - Symptom: README shows base `uvx sidemantic serve`, but `serve` needs optional `riffq`/`pyarrow` dependencies and lacks a friendly missing-extra guard.
  - Impact: documented server demo fails on base install.
  - Evidence: `README.md:160`, `pyproject.toml:52`, `sidemantic/cli.py:664`.
  - Acceptance: docs use `uvx --from "sidemantic[serve]" ...` or command prints a clear install hint.
  - Done 2026-05-30: README uses `uvx --from "sidemantic[serve]" ...`, and `sidemantic serve` now traps
    missing optional server dependencies with a direct `sidemantic[serve]` install hint.

- [x] Align pre-aggregation routing docs with CLI defaults.
  - Symptom: docs/examples claim automatic query routing, but CLI defaults `use_preaggregations=False`.
  - Impact: users materialize pre-aggregations but still query the base path unless they discover `--use-preaggregations`.
  - Evidence: `sidemantic/cli.py:540`, `sidemantic/cli.py:552`, `sidemantic/config.py:186`.
  - Acceptance: docs require the flag, config can enable routing, or CLI default changes intentionally.
  - Done 2026-05-30: pre-aggregation examples and demo commands now include `--use-preaggregations`, preserving
    the CLI default while documenting the required opt-in.

- [x] Strengthen Pyodide CI to test published metadata assumptions.
  - Symptom: Pyodide workflow installs selected deps manually and installs the wheel with `deps=False`.
  - Impact: CI proves a custom no-deps path, not that declared dependencies are Pyodide-installable.
  - Evidence: `.github/workflows/pyodide-test.yml:38`, `pyproject.toml:8`.
  - Acceptance: metadata-driven install is tested where possible, or docs explicitly state the supported no-deps Pyodide loading path.
  - Done 2026-05-30: Pyodide CI now inspects wheel metadata for guarded heavy optional dependencies and the
    docs explicitly define the supported no-deps wheel install path and core API surface.

- [x] Reorder Python publish workflow.
  - Symptom: publish workflow mutates version files, builds, publishes to PyPI, then commits/tags.
  - Impact: PyPI can receive an artifact before source commit/tag durability; the workflow also skips required gates after mutation.
  - Evidence: `.github/workflows/publish.yml:67`, `.github/workflows/publish.yml:84`.
  - Acceptance: validate, commit/tag, build from durable source, then publish.
  - Done 2026-05-30: publish now bumps version, updates the lock, runs the same lint/format/full-test gates used
    by CI, commits and tags the durable source, then builds and publishes from that committed tree.

- [x] Separate Python package versioning from Rust binary asset names.
  - Symptom: Python publish workflow triggers Rust binary assets named with the Python package version while `sidemantic-rs` has its own version.
  - Impact: release assets can misrepresent the Rust runtime version.
  - Evidence: `.github/workflows/publish.yml:102`, `.github/workflows/release-rust-binaries.yml:120`, `sidemantic-rs/Cargo.toml:1`.
  - Acceptance: asset naming and release tags reflect the Rust crate/runtime version.
  - Done 2026-05-30: Python publish no longer triggers Rust binary publishing; the Rust binary release workflow
    resolves `sidemantic-rs/Cargo.toml` and names binary archives with the Rust crate version.

- [x] Back the documented DuckDB community install path with release automation.
  - Symptom: README shows `INSTALL sidemantic FROM community`, but release workflow only uploads a raw Linux AMD64 artifact.
  - Impact: documented install path is not produced by the repo automation.
  - Evidence: `sidemantic-duckdb/README.md:13`, `.github/workflows/duckdb-extension-release.yml:71`.
  - Acceptance: either implement community-extension publishing/signing across targets or document the actual artifact install process.
  - Done 2026-05-30: DuckDB extension docs now describe local/GitHub-release `LOAD` usage, keep community
    installation explicitly planned, and the release workflow builds/tests a source-package artifact.

- [x] Fix MCP apps/charts extras split.
  - Symptom: `apps` installs `mcp[cli]`, charts deps live in `charts`, and the chart missing-dependency hint points at `serve`.
  - Impact: users installing `sidemantic[apps]` can still fail interactive chart paths.
  - Evidence: `pyproject.toml:45`, `pyproject.toml:48`, `sidemantic/charts.py:43`, `sidemantic/mcp_server.py:394`.
  - Acceptance: extras compose as advertised and dependency hints name the correct extra.
  - Done 2026-05-30: `apps` now includes chart rendering dependencies, and chart dependency errors point at
    `sidemantic[charts]` instead of the server extra.

## Phase 5: Fixture and CI Coverage

- [x] Add parity fixtures for every high-risk cross-runtime case.
  - Cases: explicit `*_columns`, compact SQL model syntax, aggregate SQL without explicit `agg`, stddev/variance, custom relationship SQL, composite many-to-many-through, relationship default keys, malformed embedded SQL, table calculations, strict unknown-field rejection, metadata/visibility round-trips, graph-level metrics, parameters.
  - Acceptance: Python and Rust fixture runners cover both compile and, for high-risk queries, row-result execution.
  - Done 2026-05-30: shared native fixtures now cover the listed cases with Python compile/execution, Rust compile,
    and DuckDB ADBC row-result execution for high-risk query fixtures, including advanced metrics and composite
    many-to-many-through joins.

- [x] Exercise DuckDB extension build/test in a reproducible local or CI path.
  - Current blocker: local checkout lacks `sidemantic-duckdb/duckdb/`.
  - Acceptance: documented command fetches DuckDB dependency and runs `make test`, or CI artifact proves all supported targets.
  - Done 2026-05-30: `make deps DUCKDB_VERSION=v1.4.2`, `make`, and `make test` passed locally after making
    `.DEFAULT_GOAL := all` explicit; CI and release workflows use the same `make deps`, build, and test sequence.

- [x] Add base-install CLI tests.
  - Cases: `sidemantic validate`, `sidemantic serve`, `sidemantic query`, missing optional extras, docs examples.
  - Acceptance: tests run in an environment without optional workbench/serve/apps/charts dependencies and assert helpful behavior.
  - Done 2026-05-30: added a CI base-install CLI smoke job and locally verified `sidemantic --version`,
    `validate`, dry-run `query`, and friendly `serve` missing-extra behavior under `uv run --no-project --with .`.

- [x] Add negative tests for warning-only/empty-output parser paths.
  - Cases: mixed valid+invalid model directories, malformed `sql_metrics`, malformed `sql_segments`, invalid persisted DuckDB sidecars.
  - Acceptance: correctness-critical commands fail loudly rather than silently dropping definitions.
  - Done 2026-05-30: strict directory loading, malformed embedded SQL metric/segment blocks, and invalid persisted
    DuckDB sidecar autoload now fail loudly, with Python parser regressions and DuckDB sqllogictest coverage.

## Completed Fix Order

1. Composite many-to-many Rust joins.
2. Python native explicit key-list parsing and shared fixtures.
3. Python/Rust aggregate SQL metric classification.
4. Strict CLI loading and base `validate` semantics.
5. DuckDB persistence ordering and multi-model SQL loading.
6. Python test suite failures on the chosen supported Python versions.
7. Remaining Rust parity gaps and docs/package release cleanup.
