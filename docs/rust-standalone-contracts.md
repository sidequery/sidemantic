# Sidemantic Rust Standalone Contracts

This document defines the intended standalone contract for `sidemantic-rs`.

The Rust runtime is a Rust-first product surface. Python compatibility is a reference point, not a requirement to duplicate every Python packaging decision. When command names or payloads overlap with the Python package, the Rust behavior should either match exactly or document an intentional difference here.

## Product Surfaces

| Surface | Rust entrypoint | Intended contract | Current status |
| --- | --- | --- | --- |
| CLI | `sidemantic-rs/src/main.rs`, binary `sidemantic` | Primary standalone command-line interface for compile, rewrite, validate, query, pre-aggregation, migrator, info, workbench, service launchers | Supported baseline for compile/rewrite/validate/query dry-run/preagg/migrator/info; service launchers remain feature-gated |
| HTTP API | `sidemantic-server` with `runtime-server`; DB execution requires `runtime-server-adbc` | Rust-native JSON API for model metadata, graph discovery, semantic compile/query, semantic SQL rewrite/run, and raw select-only SQL run | Experimental, feature-gated; real server protocol tests cover startup failure, readiness/health, graph/models, compile/query/sql/raw routes, built-without-ADBC errors, and DuckDB ADBC execution |
| MCP | `sidemantic-mcp` with `mcp-server`; DB execution requires `mcp-adbc` | Rust-native MCP server backed by `SidemanticRuntime` | Experimental, feature-gated; stdio JSON-RPC tests cover startup failure, initialize, resources, tools/list, list/get/graph/validate/compile/run/sql/chart calls, built-without-ADBC errors, and DuckDB ADBC execution |
| LSP | `sidemantic-lsp` with `runtime-lsp` feature | Stdio LSP for SQL definition authoring | Experimental, feature-gated; stdio LSP tests cover initialize, diagnostics, completion, hover, formatting, symbols, signature help, definition/references/rename/code action, unsupported adapter-format diagnostics, and shutdown; current scope is SQL definitions in opened documents |
| Workbench | `sidemantic-workbench` and `sidemantic workbench` with `workbench-tui`; execution requires `workbench-adbc` | Standalone terminal UI for inspecting models, rewriting SQL, and optionally executing against ADBC | Experimental, feature-gated; deterministic app/key/render tests and PTY smoke cover launch, initial render, keypresses, quit, bad model path, no-DB disabled execution, and DuckDB ADBC execution |
| DuckDB extension | `sidemantic-rs/src/ffi.rs`, `sidemantic-duckdb/` | C ABI and DuckDB extension functions for loading definitions and rewriting SQL | Supported baseline; mutation/replace/repeated-load, context-keyed FFI state, persistence, in-memory isolation, and invalid persisted-definition handling are tested |
| WASM | `sidemantic-rs/src/wasm.rs` with `wasm` feature | Browser-compatible semantic helpers and limited compile/rewrite support | Supported limited subset; host parity, wasm build, wasm-bindgen happy paths, error paths, and unsupported rewrite shapes are tested |
| Python extension | `sidemantic-rs/src/python.rs`; default wheel uses `python-adbc`, lower-level lightweight builds can use `python` | Optional sibling package exposing `sidemantic_rs`; not required for standalone Rust use | Supported baseline for isolated default wheel build/install/import/use/error smoke; ADBC execution is preserved in the default wheel and explicit `python-adbc` build, while lightweight `python` builds expose a disabled `execute_with_adbc` stub with feature guidance |

## CLI Contract

The standalone Rust CLI owns these commands:

- `compile`
- `rewrite`
- `validate`
- `query`
- `run`
- `preagg`
- `migrator`
- `info`
- `workbench`
- `tree`
- `mcp` / `mcp-serve`
- `server` / `serve`
- `lsp`

Expected general conventions:

- `--models <path>` accepts a directory, YAML file, or SQL file.
- A positional model path should be accepted for commands where Python already treats it as the primary argument.
- Long flags are canonical.
- Short flags should be added only where they are already established user-facing Sidemantic behavior, such as `-h`, `-v`, and `-c`.
- Feature-gated commands must fail with actionable install/build guidance when unavailable.

Known contract decisions:

- `serve` is ambiguous. Python `serve` means PostgreSQL wire protocol. Rust `serve` currently launches the HTTP API. Until Rust implements PostgreSQL wire protocol, `api-serve` should be the HTTP command and `serve` should either be reserved or clearly documented as Rust HTTP-only.
- `api-serve` is not implemented in this branch. Treat it as a follow-up naming decision, not an available command.
- `mcp-serve ./models` should work as an alias for launching MCP with a positional models path. The direct `sidemantic-mcp` binary accepts both `--models <path>` and a positional model path.
- Pre-aggregation flags should normalize toward Python/user-facing names: `--queries`, `--min-count`, `--min-score`, `--top`, with Rust-specific aliases kept only for backward compatibility.

Current status: the top-level `mcp`/`mcp-serve` launcher passes arguments through to the feature-gated `sidemantic-mcp` binary, which accepts both `--models <path>` and a positional model path.

## HTTP API Contract

Standalone Rust HTTP API routes:

- `GET /readyz`
- `GET /health`
- `GET /models`
- `POST /models`
- `GET /models/{model}`
- `GET /graph`
- `POST /compile`
- `POST /query/compile`
- `POST /query`
- `POST /query/run`
- `POST /sql/compile`
- `POST /sql`
- `POST /raw`

The protocol tests start the real server on a dynamic loopback port, verify startup failure for missing model paths, verify success and error JSON for these routes, and kill the child process as the lifecycle contract for this baseline. In a non-ADBC build, execution routes return clear `runtime-server-adbc` build guidance errors after successful compile/rewrite. The ADBC E2E test supplies DuckDB `libduckdb` plus `duckdb_adbc_init`, seeds a real DuckDB file, and proves successful structured query, semantic SQL, and raw SQL execution using `--dbopt path=<file>`.

Current intentional differences:

- `/query` and `/raw` return JSON rows only. Arrow output is not implemented.
- `/raw` uses a conservative select-only string guard rather than full parser-backed SQL statement classification.
- Bearer auth, CORS allow-list headers, and JSON 413 body-size rejection are implemented and covered. Application routes require bearer auth when configured, except `/readyz`; unauthenticated `OPTIONS` preflight is allowed for CORS. Streaming output and graceful application shutdown remain unimplemented.

Structured query payload should cover:

- `dimensions`
- `metrics`
- `where` / `filters`
- `segments`
- `order_by`
- `limit`
- `offset`
- `ungrouped`
- `parameters`
- pre-aggregation routing options

## MCP Contract

Standalone Rust MCP tools/resources:

- `list_models`
- `get_models`
- `get_semantic_graph`
- `compile_query`
- `run_query`
- `validate_query`
- `run_sql`
- `create_chart`
- `semantic://catalog` resource

Tool schemas should eventually share the same structured query payload as the HTTP API. Current MCP query tools are intentionally narrower than HTTP because they do not accept `parameters` yet, and `create_chart` fixes some query options internally. This is tracked as follow-up work rather than claimed parity.

The current protocol tests use newline-delimited JSON-RPC over stdio and check startup failure for missing model paths, initialize, initialized notification handling, tool/resource listing, catalog resource reads, successful list/get/graph/validate/compile calls, built-without-ADBC errors for execution/chart tools, invalid metric errors, missing-resource errors, and unknown-tool errors. In a non-ADBC build, execution and chart tools return clear `mcp-adbc` build guidance errors after successful compile/rewrite. The ADBC E2E test supplies DuckDB `libduckdb` plus `duckdb_adbc_init`, seeds a real DuckDB file, and proves successful `run_query`, `run_sql`, and `create_chart` execution using `--dbopt path=<file>`.

Current intentional differences:

- `create_chart` returns a Vega-Lite spec and a Rust-rendered PNG preview data URL. It does not use Altair/vl-convert, and it does not attach the Python MCP Apps `ui://sidemantic/chart` embedded UI resource.
- MCP resources expose `semantic://catalog`; no resource templates or subscriptions are needed for the static catalog baseline.

## LSP Contract

Rust LSP scope:

- SQL definition diagnostics.
- Completion for known SQL definition constructs and SQL definition properties.
- Hover for known SQL definition keywords and properties.
- Formatting for SQL definition documents.
- Document symbols for SQL definitions.
- Signature help for SQL definition constructs.
- Current-document definition, references, rename, and missing-name code actions.
- Open-document diagnostics only.

Out of scope until explicitly implemented:

- Full Python project indexing.
- Workspace folder loading.
- Cross-file symbol/index resolution.
- Adapter-specific semantic model authoring beyond SQL definitions.
- Cross-file workspace refactors.
- Python constructor completions/signatures/symbol extraction.

The current smoke test uses LSP `Content-Length` framing over stdio. It proves initialize capabilities, log notification after `initialized`, diagnostics on open/change, diagnostic clearing after repair, keyword/property hover, completion items in top-level and model contexts, formatting, document symbols, signature help, current-document definition/references/rename/code action, unopened-document completion behavior, unsupported YAML-style adapter-format diagnostics, and shutdown/exit.

## DuckDB Contract

DuckDB extension functions must be scoped and predictable:

- Loading a model file or YAML string must update the active semantic graph for the correct DuckDB context.
- Loading a same-name model file or YAML string again replaces that model in the active semantic graph instead of failing on duplicate names.
- `SEMANTIC CREATE MODEL` and `CREATE OR REPLACE MODEL` must update both persistence and in-memory graph state.
- `SEMANTIC CREATE METRIC`, `SEMANTIC CREATE DIMENSION`, and `SEMANTIC CREATE SEGMENT` must update existing models without duplicate-model errors.
- `sidemantic_clear` must clear all active graph state, including active model selection.
- In-memory DuckDB databases should keep `SEMANTIC CREATE` definitions session-local and must not persist or autoload `./sidemantic_definitions.sql` from the working directory.
- Autoload errors should be visible unless an explicit best-effort mode is selected.
- C++ must include `sidemantic-rs/include/sidemantic.h` rather than duplicating ABI declarations.
- The DuckDB extension build must build or import the Rust staticlib as a declared dependency, not as an undocumented manual pre-step.

Current implementation keys Rust FFI state by caller-provided context. The DuckDB extension derives a database/session context key from the active DuckDB database path or in-memory database instance, including parser-extension state for `SEMANTIC` statements and `ClientContext`/`ExpressionState` for table/scalar functions. Context-free C APIs remain compatibility wrappers over a default context. Autoload currently behaves best-effort for invalid persisted blocks and mainly reports skipped blocks to stderr; user-visible autoload warning/reporting remains follow-up work.

## WASM Contract

WASM supports runtime helpers that do not require filesystem, process, ADBC, or native parser features.

Supported:

- YAML string loading.
- SQL definition parsing from strings.
- Compile helpers.
- Validation helpers.
- Metadata/catalog helpers.
- Model/metric/dimension/relationship helper APIs.
- Pre-aggregation recommendation/planning helpers that are pure string/JSON operations.

Limited:

- SQL rewrite uses a fallback subset when native parsing is unavailable.
- The fallback subset is simple single-table `SELECT ... FROM ...` with optional `WHERE`, `ORDER BY`, and `LIMIT`.

Unsupported until explicitly implemented:

- Directory loading.
- ADBC execution.
- DuckDB extension state.
- Full native SQL rewrite parity.

The current wasm-bindgen runtime tests also assert malformed JSON/YAML errors, query-reference validation payloads, refresh-planner errors, and rejection of joins, CTEs, subqueries, grouped queries, and `SELECT *` in the fallback rewrite path.

## Python Extension Contract

`sidemantic_rs` is optional. It should not be required for the root Python package or Pyodide-compatible install path.

Required before Python extension release:

- Keep pure Python bindings split from ADBC bindings so lightweight builds can avoid Arrow/ADBC without changing the default wheel contract.
- Lightweight `python` builds must either omit execution entirely or expose a stable disabled-execution stub. This baseline keeps `execute_with_adbc` present and returns a clear `python-adbc` build guidance error.
- Validate with maturin in CI, then install the produced wheel into an isolated environment and smoke-test import/compile/rewrite/validation behavior.
- Validate the `python-adbc` wheel separately and prove the exported `execute_with_adbc` path executes against DuckDB `libduckdb` when `SIDEMANTIC_TEST_ADBC_DUCKDB_DRIVER` is available, while still failing missing-driver cases through the Rust ADBC layer.
- Define whether versions are independent from Python `sidemantic` or coordinated.

Current CI checks `sidemantic-rs/Cargo.toml` and `sidemantic-rs/pyproject.toml` version consistency, keeps a lightweight `python` cargo feature check, and runs isolated wheel smoke tests for the default ADBC-enabled wheel, explicit lightweight `python` build, and explicit `python-adbc` build.

## Versioning Decision

For the standalone Rust track, version ownership is split deliberately:

- `sidemantic-rs/Cargo.toml` and `sidemantic-rs/pyproject.toml` must match. They define the Rust crate and optional `sidemantic_rs` Python extension version.
- The root Python package keeps its existing root `pyproject.toml` and `sidemantic/__init__.py` version contract.
- The DuckDB extension follows the Rust runtime version, because it links the Rust C ABI and should ship with the same ABI expectation.
- A future coordinated product release may align all versions, but that should be an explicit release decision, not an incidental consequence of editing one package.

## Release Contract

Standalone Rust release readiness requires:

- CI passes Rust formatting, clippy, tests, split feature checks, WASM check, lightweight and ADBC maturin builds, and DuckDB extension build/tests.
- Version ownership is explicit for Rust crate, Python extension, DuckDB extension, and root Python package.
- Artifact list is explicit: CLI binary, Rust crate, Python wheel, WASM package, DuckDB extension.
- Release notes identify which surfaces are stable, experimental, or unsupported.

For this baseline, CLI, DuckDB, default Python extension, and the documented WASM subset are the supported surfaces once their gates pass. Rust-native ADBC execution has a DuckDB proof across CLI, HTTP, MCP, and Python, but the execution surfaces stay experimental until compatibility, shutdown/lifecycle, and deeper interactive behavior are defined and tested.

## Follow-Up Work

Detailed follow-up work is tracked in `docs/rust-standalone-followup-work.md`. That document is the source of truth for known SQL parity gaps, MCP/HTTP schema differences, Python strict-mode cleanup, ADBC breadth, DuckDB/C ABI hardening, and CI/package hardening that are intentionally not completed in this baseline.
