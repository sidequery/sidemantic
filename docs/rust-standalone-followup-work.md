# Rust Standalone Follow-Up Work

This document is the follow-up inventory for the Rust standalone runtime PR. It separates work that should be fixed before opening the PR from work that can be called out as future productization.

## Priority Legend

- `P0`: fix before PR notes are finalized, because current docs or gate behavior would mislead reviewers.
- `P1`: important product or parity work, but not required for this PR if called out clearly.
- `P2`: hardening or breadth work after the baseline lands.

## P0: Contract And Documentation Cleanup

| Area | Current state | Follow-up | Acceptance check |
| --- | --- | --- | --- |
| Parity matrix path | `sidemantic/rust_parity.py` reads `docs/rust-parity-matrix.json`; strict probe writes `docs/rust-strict-failure-matrix.json`. | Keep both files with separate roles: `rust-parity-matrix.json` for subsystem gating, `rust-strict-failure-matrix.json` for latest probe output. | `uv run scripts/rust_strict_probe.py` updates both files without a no-op. |
| HTTP command naming | Rust implements `server` / `serve`; docs previously mentioned `api-serve`. | Document `api-serve` as a naming follow-up, not a current command. | README and contract docs do not present `api-serve` as available. |
| MCP query schema | HTTP accepts `parameters`; MCP structured tools do not yet accept `parameters`. | Document MCP as a narrower schema until `parameters` is implemented. | Service matrix does not claim HTTP/MCP schema identity. |
| Test coverage wording | Some docs described all structured fields and LSP save/unknown-method behavior as covered. | Phrase exact coverage: opened-document LSP diagnostics; tested HTTP/MCP fields; untested fields tracked below. | Matrix rows avoid broad "all fields covered" claims. |
| Workbench shortcut docs | README described `Ctrl+M/V/L` as focus shortcuts. | Document them as chart controls. | README matches `workbench.rs` key handling. |

## P1: SQL Generation And Rewrite Parity

Rust now has a substantial DuckDB-oriented SQL generator and parser-backed rewriter. The next parity work is to make the differences explicit in tests before expanding behavior.

| Work item | Why it matters | Suggested next step | Evidence files |
| --- | --- | --- | --- |
| Differential compile fixtures | Prevents silent drift between Python and Rust SQL generation. | Add Python-vs-Rust fixture tests normalized with `sqlglot`; execute on DuckDB where possible. | `sidemantic-rs/src/sql/generator.rs`, `sidemantic/core/semantic_layer.py` |
| Dialect contract | Rust emits DuckDB-shaped SQL in several paths. | Decide whether Rust compile is DuckDB-only for now or add dialect to `SqlGenerator`. | `sidemantic-rs/src/sql/generator.rs` |
| Python bridge payload drift | Python appends `OFFSET` manually and does not pass `skip_default_time_dimensions`. | Pass native Rust `offset`; decide and test `skip_default_time_dimensions`. | `sidemantic/core/semantic_layer.py` |
| Rewriter `ORDER BY` semantics | Current native rewriter coverage mostly checks presence, not semantic rewrite correctness. | Add tests for `ORDER BY orders.metric`, `ORDER BY model.dimension`, and order-only references. | `sidemantic-rs/src/sql/rewriter.rs` |
| Conversion metrics | Rust conversion generation is simpler than Python. | Bring over conversion filters, multi-step conversion, and matching Python error behavior. | `sidemantic-rs/src/sql/generator.rs` |
| Time comparison metrics | Rust LAG paths do not yet partition by non-time dimensions like Python. | Add fixture showing cross-group leakage, then port partitioning behavior. | `sidemantic-rs/src/sql/generator.rs` |
| Retention/cohort metrics | Rust does not model these metric types yet. | Either implement or reject explicitly with tests and clear errors. | `sidemantic-rs/src/core/model.rs`, `sidemantic-rs/src/sql/generator.rs` |

## P1: Service Surface Parity

HTTP, MCP, LSP, and workbench are executable and tested. They should still be described as experimental until these items are resolved.

| Surface | Follow-up | Acceptance check |
| --- | --- | --- |
| HTTP | Add direct tests for `segments`, `use_preaggregations`, and every claimed structured-query field. | `sidemantic-rs/tests/http_server.rs` asserts those fields, not only that code supports them. |
| HTTP | Replace `/raw` select-only string guard with parser-backed statement classification if raw SQL becomes a product promise. | Non-SELECT statements are rejected by parser classification, not substring scanning. |
| HTTP | Decide whether Arrow/streaming output belongs in the Rust API. | Contract either marks Arrow as non-goal or includes tested output mode. |
| MCP | Add `parameters` to structured query tools or keep documenting MCP as narrower than HTTP. | MCP tests cover parameter interpolation or docs continue to call it out as a gap. |
| MCP | Add richer MCP Apps chart UI resources only if clients need embedded UI. | `create_chart` either stays Vega-Lite plus PNG or emits a tested UI resource. |
| LSP | Add `didSave` and unknown-method assertions if docs claim them. | `sidemantic-rs/tests/lsp_protocol_smoke.py` proves both behaviors. |
| LSP | Add cross-file indexing only if Rust LSP scope grows beyond opened SQL documents. | Definition/references/rename tests span multiple files. |
| Workbench | Add deeper interactive/product tests if workbench graduates beyond experimental. | Tests cover more than deterministic state and PTY launch. |

## P1: Python Bridge And Strict Mode

The Python integration is opt-in. It is useful as a migration bridge, not a transparent replacement.

| Work item | Why it matters | Suggested next step |
| --- | --- | --- |
| Rewriter strict subsystem | `SIDEMANTIC_RS_STRICT_SUBSYSTEMS=all` does not by itself enable the Rust rewriter. | Add a `semantic_sql_rewriter` strict target or keep requiring `SIDEMANTIC_RS_REWRITER=1` and document that explicitly. |
| Matrix loading tests | `require_rust_subsystem()` was untested. | Add tests for matrix loading, missing matrix fallback, and non-`rust_backed` failures. |
| Default enablement policy | Reviewers may ask whether Rust should be default. | Keep default Python behavior unchanged until differential parity is broader. |
| Error shape parity | Strict Rust failures are intentionally fail-closed, but messages differ from Python. | Decide where exact Python error text matters and add tests only there. |

## P1: ADBC Breadth

DuckDB is proven locally and in CI. Other drivers are represented by env-gated probes only.

| Driver | Current Rust state | Follow-up |
| --- | --- | --- |
| DuckDB | Proven across CLI, HTTP, MCP, workbench, and Python wheel smoke when driver is available. | Keep as the always-on execution proof. |
| SQLite | Env-gated; local native `libsqlite3` is not an ADBC driver. | Add real SQLite ADBC driver path in CI when available. |
| PostgreSQL | Env-gated; no local service/credentials. | Add service container plus driver once CI credentials/config are chosen. |
| BigQuery | Env-gated; credentials required. | Add opt-in CI job with secret-backed credentials if this becomes required. |
| Snowflake | Env-gated; credentials required. | Add opt-in CI job with secret-backed credentials if this becomes required. |
| ClickHouse | Env-gated; no local service/driver. | Add service container plus driver once required. |

Also add Arrow complex-type handling if result sets need lists, structs, maps, or richer timestamp semantics. The current executor is scalar-focused.

## P1: DuckDB Extension And C ABI

| Work item | Why it matters | Suggested next step |
| --- | --- | --- |
| Autoload error visibility | Invalid persisted definitions are best-effort and mostly hidden from users. | Surface warnings or add an explicit best-effort mode. |
| Persistence editing | Text-based `MODEL` block editing can leave old field definitions after repeated field replacement. | Move persistence to an AST or structured model file format before heavy mutation use. |
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
- "DuckDB ADBC is proven; other drivers are env-gated."
- "Python bridge is optional and strict-mode capable."
- "HTTP/MCP/LSP/workbench are experimental service surfaces with protocol/UI smoke coverage."

Avoid:

- "Full Python parity."
- "Drop-in Python replacement."
- "All ADBC backends pass."
- "Production-ready service APIs."
- "HTTP and MCP share identical schemas."
