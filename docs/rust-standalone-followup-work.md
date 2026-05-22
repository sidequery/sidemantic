# Rust Standalone Follow-Up Work

This document is the follow-up inventory for the Rust standalone runtime PR. It separates work completed in this PR from remaining productization and parity work that should not be overclaimed.

## Priority Legend

- `P0`: fix before PR notes are finalized, because current docs or gate behavior would mislead reviewers.
- `P1`: important product or parity work, but not required for this PR if called out clearly.
- `P2`: hardening or breadth work after the baseline lands.

## Completed In This PR

| Area | Result | Evidence |
| --- | --- | --- |
| Rewriter `ORDER BY` correctness | Semantic metric/dimension refs in `ORDER BY`, projected aliases, and order-only semantic refs are covered | `sidemantic-rs/src/sql/rewriter.rs` tests |
| Time-comparison partitioning | LAG paths now partition by non-time dimensions to avoid cross-group leakage | `sidemantic-rs/src/sql/generator.rs` tests |
| MCP parameters | `compile_query`, `run_query`, and chart query paths accept parameter maps and interpolate filters | `sidemantic-rs/src/bin/sidemantic-mcp.rs`, `sidemantic-rs/tests/mcp_protocol.rs` |
| Strict-mode matrix loading | Missing/invalid matrix fallback, `all`, comma-separated targets, pass, and fail-closed behavior are tested | `tests/core/test_rust_parity.py` |
| Strict rewriter policy | `semantic_sql_rewriter` remains explicit opt-in through `SIDEMANTIC_RS_REWRITER=1`; it is not included in `SIDEMANTIC_RS_STRICT_SUBSYSTEMS=all` until fallback behavior is broadened | `docs/rust-parity-matrix.json`, `tests/core/test_rust_parity.py` |
| DuckDB persistence replace | Repeated metric/dimension/segment replacement edits persisted definitions without stale shadowing; prefixed definitions persist inside the target model block | `sidemantic-rs/src/ffi.rs` tests |
| DuckDB autoload visibility | Invalid persisted definitions are best-effort and emit a visible warning from the DuckDB extension | `sidemantic-duckdb/src/sidemantic_extension.cpp`, `sidemantic-rs/src/ffi.rs` tests |
| Conversion/retention/cohort modeling | Rust model/schema/parser/runtime/bridge/generator paths now model conversion, retention, and cohort metrics, including graph-level owner inference by entity dimension | `sidemantic-rs/src/core/model.rs`, `sidemantic-rs/src/sql/generator.rs`, `sidemantic/rust_bridge.py` |
| HTTP Arrow IPC | `/query`, `/query/run`, `/sql`, and `/raw` negotiate buffered Arrow IPC stream-format responses; `?format=arrow&transport=chunked` and `?format=arrow&stream=true` opt into chunked transport streaming | `sidemantic-rs/src/db/adbc.rs`, `sidemantic-rs/src/bin/sidemantic-server.rs`, `sidemantic-rs/tests/adbc_duckdb_e2e.rs` |
| ADBC breadth | DuckDB and SQLite are fail-closed in Rust CI; Postgres and ClickHouse are fail-closed in integration CI; BigQuery/Snowflake are secret-gated with documented reasons | `.github/workflows/ci.yml`, `.github/workflows/integration.yml`, `sidemantic-rs/tests/adbc_driver_matrix.rs` |

## P1: SQL Generation And Rewrite Parity

Rust now has a substantial DuckDB-oriented SQL generator and parser-backed rewriter. The next parity work is to make the differences explicit in tests before expanding behavior.

| Work item | Why it matters | Suggested next step | Evidence files |
| --- | --- | --- | --- |
| Differential compile fixtures | Prevents silent drift between Python and Rust SQL generation. | Add Python-vs-Rust fixture tests normalized with `sqlglot`; execute on DuckDB where possible. | `sidemantic-rs/src/sql/generator.rs`, `sidemantic/core/semantic_layer.py` |
| Dialect contract | Rust emits DuckDB-shaped SQL in several paths. | Decide whether Rust compile is DuckDB-only for now or add dialect to `SqlGenerator`. | `sidemantic-rs/src/sql/generator.rs` |
| Python bridge payload drift | Python now passes native Rust `offset`; `skip_default_time_dimensions` is still not passed. | Decide and test `skip_default_time_dimensions`. | `sidemantic/core/semantic_layer.py` |
| Conversion metrics | Rust now supports legacy and multi-step conversion generation, filters, entity/window validation, and owner inference, but execution parity coverage is still much thinner than Python's advanced metric tests. | Add DuckDB execution fixtures for chronological multi-step ordering, derived dimensions, repeated actions, OR predicates, and graph-level ambiguity/error cases. | `sidemantic-rs/src/sql/generator.rs`, `tests/metrics/test_advanced.py` |
| Retention metrics | Rust now models and generates retention SQL, including entity/cohort/activity/period/granularity fields. | Add DuckDB execution parity for retention defaults, placeholder expansion, YAML aliases, filters, and ambiguous graph-level cases. | `sidemantic-rs/src/sql/generator.rs`, `tests/metrics/test_retention.py` |
| Cohort metrics | Rust now models and generates cohort SQL, preserves SQL-parser `agg`, resolves `entity_dimensions`, resolves inner metric dimension SQL, and infers graph-level ownership by entity. | Add DuckDB execution parity for reserved words, outer SQL, missing dimension errors, and ambiguous model cases. | `sidemantic-rs/src/sql/generator.rs`, `tests/metrics/test_cohort.py` |

## P1: Service Surface Parity

HTTP, MCP, LSP, and workbench are executable and tested. They should still be described as experimental until these items are resolved.

| Surface | Follow-up | Acceptance check |
| --- | --- | --- |
| HTTP | Add materialized pre-aggregation execution fixtures. Current tests assert compile-path flag and segment/parameter behavior, not a real preagg table selection. | A fixture proves `use_preaggregations` picks a materialized preagg when available. |
| HTTP | Replace `/raw` select-only string guard with parser-backed statement classification if raw SQL becomes a product promise. | Non-SELECT statements are rejected by parser classification, not substring scanning. |
| HTTP | Decide whether chunked Arrow responses should expose row counts through trailers or a side channel. Current chunked mode intentionally omits `X-Sidemantic-Row-Count`. | Clients that depend on row-count headers need either buffered Arrow or a trailer-aware path. |
| MCP | Add richer MCP Apps chart UI resources only if clients need embedded UI. | `create_chart` either stays Vega-Lite plus PNG or emits a tested UI resource. |
| LSP | Add unknown-method assertions if docs claim them. | `sidemantic-rs/tests/lsp_protocol_smoke.py` proves behavior. |
| LSP | Add cross-file indexing only if Rust LSP scope grows beyond opened SQL documents. | Definition/references/rename tests span multiple files. |
| Workbench | Add deeper interactive/product tests if workbench graduates beyond experimental. | Tests cover more than deterministic state and PTY launch. |

## P1: Python Bridge And Strict Mode

The Python integration is opt-in. It is useful as a migration bridge, not a transparent replacement.

| Work item | Why it matters | Suggested next step |
| --- | --- | --- |
| Rewriter strict subsystem | Policy decision made: `SIDEMANTIC_RS_STRICT_SUBSYSTEMS=all` still does not enable the Rust rewriter; explicit `SIDEMANTIC_RS_REWRITER=1` remains required. | Add `semantic_sql_rewriter` to strict `all` only after no-fallback behavior and skipped rewrite shapes are accepted. |
| Matrix loading tests | Matrix loading and fail-closed behavior are covered. | Keep tests updated as new subsystems become rust-backed. |
| Default enablement policy | Reviewers may ask whether Rust should be default. | Keep default Python behavior unchanged until differential parity is broader. |
| Error shape parity | Strict Rust failures are intentionally fail-closed, but messages differ from Python. | Decide where exact Python error text matters and add tests only there. |

## P1: ADBC Breadth

DuckDB remains the protocol-level proof. SQLite, Postgres, and ClickHouse are now fail-closed driver-manager probes in CI. BigQuery and Snowflake remain secret-gated because they require real external credentials.

| Driver | Current Rust state | Follow-up |
| --- | --- | --- |
| DuckDB | Proven across CLI, HTTP, MCP, workbench, and Python wheel smoke when driver is available; CI downloads `libduckdb` and requires it for Rust driver matrix. | Keep as the always-on execution proof. |
| SQLite | Rust driver matrix is fail-closed in CI through `adbc-driver-sqlite`; local transient driver probe passed. | Add protocol-level SQLite execution only if service surfaces must prove more than driver-manager execution. |
| PostgreSQL | Rust driver matrix is fail-closed in integration CI through the live Postgres service and `adbc-driver-postgresql`. | Add a seeded Rust CLI/HTTP semantic query Postgres E2E if reviewers require more than `select 1` driver proof. |
| BigQuery | ADBC Foundry/Apache drivers exist. The emulator-backed Python adapter tests do not prove Rust ADBC because the BigQuery driver expects Google Cloud authentication and project/dataset options. | CI runs a Rust probe when `BIGQUERY_ADBC_CREDENTIALS_JSON` plus `BIGQUERY_ADBC_PROJECT`/`BIGQUERY_ADBC_DATASET` are configured. |
| Snowflake | ADBC Foundry/Apache drivers exist. Fakesnow does not prove C-driver compatibility. | CI runs a Rust probe when `SNOWFLAKE_ADBC_URI` is configured. |
| ClickHouse | Rust driver matrix is fail-closed in integration CI through `dbc install --pre clickhouse` and the live ClickHouse service. | Promote beyond probe coverage only after the prerelease driver stabilizes and protocol-level semantic queries are needed. |

Also add Arrow complex-type handling if result sets need lists, structs, maps, or richer timestamp semantics. The current executor is scalar-focused.

## P1: DuckDB Extension And C ABI

| Work item | Why it matters | Suggested next step |
| --- | --- | --- |
| Autoload error visibility | Invalid persisted definitions are best-effort and now warn to stderr. | Add a queryable warning surface only if stderr is not enough for DuckDB users. |
| Persistence editing | Repeated field replacement is fixed for metric/dimension/segment definitions, but text-based block editing remains fragile for large-scale mutation. | Move persistence to an AST or structured model file format before heavy mutation use. |
| Context lifecycle | FFI state is process-global and keyed by string; there is no eviction for closed file-backed DBs. | Add context cleanup hooks or documented lifecycle API. |
| ABI versioning | Header is smoke-tested but not versioned. | Add ABI version function and layout/static assertions for C consumers. |
| Rewrite detection | `query_references_models` uses heuristic substring matching. | Replace with parser-backed model reference detection where possible. |

## P2: CI And Packaging Hardening

| Work item | Why it matters | Suggested next step |
| --- | --- | --- |
| External downloads | CI downloads DuckDB ADBC, installs wasm-bindgen, builds wheels, and clones DuckDB extension tools. | Pin versions, improve caching, and make failures easier to diagnose. |
| Package tarball build | `cargo package --no-verify` proves packaging shape, not tarball buildability. | Add a tarball build check if publishing the crate. |
| Browser WASM integration | WASM tests run wasm-bindgen runtime paths, not browser bundlers. | Add browser/bundler smoke only if WASM becomes a product artifact. |
| Release automation | Explicitly out of current scope. | Add after standalone surfaces and version policy are accepted. |

## PR Wording Guardrails

Use:

- "Executable and tested Rust standalone baseline."
- "DuckDB is the protocol-level ADBC proof; SQLite, Postgres, and ClickHouse are CI driver probes; BigQuery and Snowflake are secret-gated."
- "Python bridge is optional and strict-mode capable."
- "HTTP/MCP/LSP/workbench are experimental service surfaces with protocol/UI smoke coverage."

Avoid:

- "Full Python parity."
- "Drop-in Python replacement."
- "All ADBC backends pass."
- "Production-ready service APIs."
- "HTTP and MCP share identical schemas."
