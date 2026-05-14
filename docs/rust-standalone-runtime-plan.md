# Sidemantic Rust Standalone Runtime Plan

## Goal

Make `sidemantic-rs` viable as a standalone Rust-first Sidemantic runtime and CLI, not just a Python extension or accelerator.

That means the Rust crate should be able to:

- Build and test independently from the Python package.
- Serve as the primary CLI for core semantic-layer workflows.
- Expose Rust-native runtime surfaces for HTTP, MCP, LSP, DuckDB, WASM, and optional Python bindings.
- Ship with clear versioning, CI gates, release ownership, and product docs.

## Current Inventory

The full local Rust port was copied from:

- `/Users/nico/conductor/workspaces/sidemantic/lahore-v6/sidemantic-rs`

into:

- `sidemantic-rs/`

Current copied shape:

- The imported Rust port is staged on branch `rust-standalone-runtime`.
- Required new Rust package/runtime files are tracked in the index: `README.md`, `pyproject.toml`, `src/runtime.rs`, `src/python.rs`, `src/wasm.rs`, `src/workbench.rs`, `src/db/`, `src/bin/`, and `tests/`.

Main Rust surfaces:

- `src/core/`: model, dimension, metric, relationship, graph, inheritance, parameters, segments, table calculations, relative dates, symmetric aggregates.
- `src/config/`: native YAML, limited Cube YAML, SQL definitions, directory/file loading, env substitution.
- `src/sql/`: semantic query generator and SQL rewriter.
- `src/runtime.rs`: broad standalone facade for compile, rewrite, validate, load, catalog, migrator, pre-aggregation, chart, table-calculation, dependency, and helper APIs.
- `src/main.rs`: standalone `sidemantic` CLI.
- `src/bin/`: feature-gated MCP, HTTP server, LSP, workbench, and demo binaries.
- `src/db/adbc.rs`: optional ADBC execution.
- `src/ffi.rs` and `include/sidemantic.h`: C ABI for DuckDB extension integration.
- `src/wasm.rs`: wasm-bindgen exports.
- `src/python.rs`: optional PyO3 module named `sidemantic_rs`.

## Validation Snapshot

Commands run in this worktree:

- `cargo fmt --manifest-path sidemantic-rs/Cargo.toml --check`: pass.
- `cargo clippy --manifest-path sidemantic-rs/Cargo.toml --all-targets -- -D warnings`: pass.
- `cargo clippy --manifest-path sidemantic-rs/Cargo.toml --all-targets --all-features -- -D warnings`: pass.
- `RUST_MIN_STACK=16777216 cargo test --manifest-path sidemantic-rs/Cargo.toml`: pass.
  - 203 library tests.
  - 1 CLI unit test.
  - 26 CLI smoke tests.
  - 15 WASM parity subset tests.
  - 4 doctests.
- Split feature checks pass for no-default-features, lightweight Python, Python+ADBC, MCP without/with ADBC, HTTP without/with ADBC, LSP, and workbench without/with ADBC.
- `cargo test --manifest-path sidemantic-rs/Cargo.toml --test package_metadata`: pass.
  - Checks Rust crate/Python extension version alignment, expected PyO3 module metadata, required library crate types, package metadata, and explicit opt-in ADBC features.
- `cargo package --manifest-path sidemantic-rs/Cargo.toml --locked --allow-dirty --no-verify`: pass.
  - Yanked wasm-bindgen/js-sys lock entries may still warn until the lockfile is refreshed around compatible upstream releases.
- `cargo test --manifest-path sidemantic-rs/Cargo.toml --features mcp-server --test mcp_protocol`: pass.
  - Exercises MCP positional and `--models` startup, missing-model startup failure, initialize, initialized notification, tools/list, resources/list, resources/read, list/get/graph/validate/compile tool calls, structured responses, invalid metric errors, missing ADBC run/sql/chart errors, missing resource errors, and unknown-tool errors against the actual binary.
- `cargo test --manifest-path sidemantic-rs/Cargo.toml --features runtime-server --test http_server`: pass.
  - Verifies HTTP missing-model startup failure, then starts the actual server on a dynamic loopback port and verifies readiness/health, models, graph, compile, parameter interpolation, semantic SQL rewrite, run errors, auth, CORS, body-size rejection, and error JSON paths.
- `cargo test --manifest-path sidemantic-rs/Cargo.toml --features runtime-lsp --test lsp_protocol_smoke`: pass.
  - Uses real LSP framing over stdio to verify initialize, diagnostics, hover, completions, formatting, symbols, signature help, current-document definition/references/rename/code action, repair clearing, unsupported YAML-style diagnostics, shutdown, and exit.
- `cargo test --manifest-path sidemantic-rs/Cargo.toml --features workbench-tui --bin sidemantic workbench`: pass.
  - Covers deterministic workbench startup, key handling, output switching, and ratatui rendering on normal and small terminal buffers.
- `cargo test --manifest-path sidemantic-rs/Cargo.toml --features workbench-tui --test workbench_pty_smoke`: pass.
  - Exercises actual PTY launch, initial render, no-DB execution disabled state, output-view keypress, bad model path, and quit behavior.
- `SIDEMANTIC_TEST_ADBC_DUCKDB_DRIVER=/opt/homebrew/Cellar/duckdb/1.5.2/lib/libduckdb.dylib cargo test --manifest-path sidemantic-rs/Cargo.toml --features workbench-adbc --test workbench_pty_smoke -- --nocapture`: pass locally on macOS.
  - Seeds DuckDB through Rust ADBC and exercises DB-backed workbench execution in a PTY.
- `cargo check --manifest-path sidemantic-rs/Cargo.toml --no-default-features --features wasm --target wasm32-unknown-unknown --lib`: pass.
- `CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner cargo test --manifest-path sidemantic-rs/Cargo.toml --target wasm32-unknown-unknown --features wasm --test wasm_bindgen_runtime`: pass after installing `wasm-bindgen-cli v0.2.110`.
  - 8 wasm-bindgen runtime tests passed under the wasm test runner, including malformed input and unsupported rewrite-shape errors.
- `uvx maturin build --out dist` from `sidemantic-rs/`: pass.
  - Built `sidemantic-rs/dist/sidemantic_rs-0.1.0-cp311-abi3-macosx_11_0_arm64.whl`.
- `uv run --no-project --with dist/sidemantic_rs-0.1.0-cp311-abi3-macosx_11_0_arm64.whl tests/python_wheel_smoke.py` from `sidemantic-rs/`: pass.
  - Verified isolated default `sidemantic_rs` import, root `sidemantic` absence, compile, rewrite, graph load, validation, registry ContextVar behavior, and `execute_with_adbc` export.
- `uvx maturin build --manifest-path sidemantic-rs/Cargo.toml --no-default-features --features python --out sidemantic-rs/dist-python`: pass.
- `uv run --no-project --with sidemantic-rs/dist-python/sidemantic_rs-0.1.0-cp311-abi3-macosx_11_0_arm64.whl sidemantic-rs/tests/python_wheel_python_smoke.py`: pass.
  - Verifies the lightweight PyO3 build compiles without ADBC/Arrow, imports in isolation, compiles a query, and exposes `execute_with_adbc` as a disabled stub with `python-adbc` build guidance.
- `uvx maturin build --features python-adbc --out dist-adbc` from `sidemantic-rs/`: pass.
- `SIDEMANTIC_TEST_ADBC_DUCKDB_DRIVER=/opt/homebrew/Cellar/duckdb/1.5.2/lib/libduckdb.dylib uv run --no-project --with dist-adbc/sidemantic_rs-0.1.0-cp311-abi3-macosx_11_0_arm64.whl tests/python_wheel_adbc_smoke.py` from `sidemantic-rs/`: pass.
  - Verifies the ADBC-enabled wheel exports `execute_with_adbc`, routes missing-driver failures through the Rust ADBC layer, and executes against DuckDB when `SIDEMANTIC_TEST_ADBC_DUCKDB_DRIVER` points at `libduckdb`.
- `SIDEMANTIC_TEST_ADBC_DUCKDB_DRIVER=/opt/homebrew/Cellar/duckdb/1.5.2/lib/libduckdb.dylib cargo test --manifest-path sidemantic-rs/Cargo.toml --features mcp-adbc,runtime-server-adbc --test adbc_duckdb_e2e -- --nocapture`: pass locally on macOS.
  - Seeds a DuckDB database through Rust ADBC and proves CLI `run`, HTTP `/query`, `/sql`, `/raw`, and MCP `run_query`, `run_sql`, and `create_chart` execute against the same DB-backed model.
- `SIDEMANTIC_TEST_ADBC_DUCKDB_DRIVER=/opt/homebrew/Cellar/duckdb/1.5.2/lib/libduckdb.dylib cargo test --manifest-path sidemantic-rs/Cargo.toml --features adbc-exec --test adbc_driver_matrix`: pass locally on macOS.
  - Proves DuckDB through the generic Rust ADBC matrix and records concrete skip reasons for SQLite/Postgres/BigQuery/Snowflake/ClickHouse until driver libraries or credentials are provided.
- `CARGO_TARGET_DIR=sidemantic-rs/target cargo build --manifest-path sidemantic-rs/Cargo.toml --release --lib`: pass.
  - Produced `sidemantic-rs/target/release/libsidemantic.a`.
- `cc -std=c11 -Wall -Wextra -I sidemantic-rs/include sidemantic-rs/tests/c_abi_smoke.c sidemantic-rs/target/release/libsidemantic.a -framework Security -framework CoreFoundation -o /tmp/sidemantic_c_abi_smoke && /tmp/sidemantic_c_abi_smoke`: pass on macOS.
  - Exercises context-aware and context-free C ABI load/list/rewrite/define/clear/error/free paths through the public header.
- `cmake -S sidemantic-duckdb -B sidemantic-duckdb/build`: blocked outside DuckDB extension harness.
  - Fails with `Unknown CMake command "build_static_extension"` because that command is provided by the DuckDB extension build harness.
- `make` from `sidemantic-duckdb/` with DuckDB v1.4.2 harness cloned into ignored `sidemantic-duckdb/duckdb/`: pass.
  - Built `sidemantic.duckdb_extension` and the DuckDB unittest binary.
- `make test` from `sidemantic-duckdb/`: pass.
  - 48 assertions in 3 SQLLogicTest cases.
- `./build/release/test/unittest test/sql/sidemantic_persistence.test` from `sidemantic-duckdb/`: pass.
  - Proves file-backed persistence, autoload after restart, replace behavior, and query continuity.
- `./build/release/test/unittest test/sql/sidemantic_memory_and_invalid_persistence.test` from `sidemantic-duckdb/`: pass.
  - Proves in-memory definitions do not persist and invalid persisted definitions do not break extension load or later recreation.
- Root Python project gates: pass.
  - `uv run ruff check . --exclude docs/_extensions --exclude sidemantic-duckdb/extension-ci-tools --exclude sidemantic-duckdb/scripts --exclude sidemantic-duckdb/duckdb --exclude sidemantic/adapters/malloy_grammar --exclude sidemantic/adapters/holistics_grammar`.
  - `uv run ruff format --check . --exclude docs/_extensions --exclude sidemantic-duckdb/extension-ci-tools --exclude sidemantic-duckdb/scripts --exclude sidemantic-duckdb/duckdb --exclude sidemantic/adapters/malloy_grammar --exclude sidemantic/adapters/holistics_grammar`.
  - `uv run pytest -v`: 3450 passed, 56 skipped, 75 deselected, 18 xfailed.

Unverified gates:

- Browser-specific WASM execution beyond the node-compatible wasm-bindgen runner.
- Full release publish automation for all artifacts.
- HTTP streaming output and graceful application shutdown. Auth, CORS, and body-size limits are implemented for the current HTTP baseline.
- External ADBC drivers beyond DuckDB `libduckdb`; Rust matrix tests are env-gated and skip with concrete reasons when driver libraries/credentials are unavailable.

## Standalone Readiness Assessment

### What Looks Real

The Rust port is not just bindings around Python. It has native implementations for core model types, graph traversal, YAML/SQL loading, query compilation, rewriting, runtime helpers, CLI flows, ADBC execution, WASM exports, and service binaries.

It can already compile and run a substantial default test suite. Feature-gated runtime surfaces also compile independently.

### What Is Not Ready

The port is not yet a reliable standalone product surface. The highest-risk blockers are correctness and product-contract issues, not mere missing packaging.

Key blockers:

- CLI and service contracts diverge from Python/user-facing expectations.
- Core/config parity is incomplete and sometimes semantically different.
- SQL generator/rewriter still need broader parity work for time comparison, conversion metrics, and fan-out policy.
- DuckDB FFI now uses context-keyed state, though full DuckDB harness tests still need to run in CI.
- MCP, HTTP, LSP, and workbench now have real protocol, PTY, or deterministic UI tests, including DuckDB execution where applicable, but remain experimental until compatibility, lifecycle, and interactive/product behavior are broader.
- Release/version ownership is split across Python, Rust, and DuckDB.

## Completed In This Branch

- Copied the fuller Rust port into `sidemantic-rs/`.
- Created the `rust-standalone-runtime` branch.
- Added standalone CI gates for Rust fmt, clippy, tests, feature checks, WASM check, maturin build, and DuckDB extension build/test.
- Documented standalone contracts in `docs/rust-standalone-contracts.md`.
- Fixed default and all-target clippy blockers.
- Added SQL generator regressions and fixes for:
  - `COUNT(*)` raw CTE generation.
  - derived metric raw dependency collection.
  - ratio metric raw dependency collection.
  - semantic `ORDER BY` alias rewriting.
- Added graph/FFI regressions and fixes for:
  - explicit `SemanticGraph::replace_model`.
  - `CREATE OR REPLACE MODEL` updating the in-memory graph.
  - `SEMANTIC CREATE METRIC/DIMENSION/SEGMENT` updating existing models without duplicate model errors.
  - `sidemantic_clear` clearing active model state.
  - context-keyed state isolation across DuckDB database/session keys.
  - same-name YAML/file loads replacing existing models instead of failing as duplicates.
  - autoload restoring active model state for subsequent unqualified definitions.
  - in-memory DuckDB definitions staying session-local instead of persisting to `./sidemantic_definitions.sql`.
  - multiline persisted model removal.
- Hardened DuckDB extension build wiring:
  - C++ includes `sidemantic-rs/include/sidemantic.h` instead of redeclaring the ABI.
  - C++ derives context from DuckDB parser info and active `ClientContext`/`ExpressionState`, not process-global mutable state.
  - CMake builds the Rust staticlib as an explicit dependency.
  - extension Makefile name is `sidemantic`.
- Added Python wheel smoke coverage for isolated `sidemantic_rs` install/import/use.
- Expanded Python wheel smoke coverage for graph helpers, relationship helpers, adapter detection, error paths, registry ContextVar behavior, lightweight no-ADBC behavior with a disabled `execute_with_adbc` stub, and ADBC-enabled binding exposure.
- Split `python`, `mcp-server`, `runtime-server`, and `workbench-tui` from ADBC/Arrow, with opt-in `python-adbc`, `mcp-adbc`, `runtime-server-adbc`, and `workbench-adbc` features.
- Added a DuckDB ADBC E2E path using native `libduckdb` plus `duckdb_adbc_init`, not the Python `_duckdb` module.
- Added MCP, HTTP, LSP, workbench, package metadata, C ABI, and WASM negative-path tests.
- Added startup failure tests for missing model paths in HTTP and MCP.
- Added MCP catalog resource and chart protocol coverage.
- Added LSP hover, formatting, symbols, signature help, current-document definition/references/rename/code action, and unsupported YAML-style adapter-format diagnostic tests.
- Added PTY-level workbench coverage for launch, render, keypresses, quit, bad model path, no-DB execution disabled state, and DB-backed ADBC execution.
- Fixed MCP structured result output so `sidemantic-mcp` starts and serves valid tool schemas.
- Fixed SQL-definition parse no-progress behavior that could hang diagnostics on invalid/incomplete alphabetic input.
- Fixed workbench header/footer rendering constraints so text appears in rendered buffers.
- Added CLI regression coverage for representative relationship rewrite and top-level ratio compile cases.
- Expanded DuckDB SQLLogicTest coverage for repeated YAML loads, active-model switching, and `CREATE OR REPLACE MODEL` mutation/rewrite behavior.
- Expanded DuckDB SQLLogicTest coverage for file-backed persistence/autoload, in-memory non-persistence, and invalid persisted-definition recovery.

## Priority Plan

### P0: Preserve And Stabilize The Imported Port

1. Put the copied Rust port on a real branch.
2. Stage all required tracked and untracked `sidemantic-rs` files together.
3. Fix default clippy failures.
4. Add CI checks for the standalone surfaces that already compile locally:
   - `cargo fmt --check`
   - `cargo clippy --all-targets -- -D warnings`
   - `RUST_MIN_STACK=16777216 cargo test`
   - feature-gated `cargo check` for ADBC, MCP, server, LSP, workbench
   - wasm32 check
   - maturin build
5. Document the standalone support status in `sidemantic-rs/README.md`.

Exit gate:

- A clean branch contains the full copied port.
- Default Rust tests and clippy pass in CI.
- Feature-gated compile checks are in CI.

### P1: Define The Standalone Product Contract

Decide whether Rust should be:

- A full standalone replacement CLI/runtime.
- A Rust-first runtime with intentionally different service APIs.
- A lower-level engine with separate product wrappers.

For a standalone product, define explicit contracts for:

- CLI command names and flags.
- Input model formats.
- Query payload shape.
- HTTP route shape.
- MCP tool shape.
- LSP scope.
- DuckDB session/state behavior.
- WASM supported subset.
- Python binding role, if any.

Immediate contract decisions:

- `serve`: Python uses this for PostgreSQL wire protocol, Rust uses it for HTTP. Rename or align before claiming CLI command parity.
- `api-serve`: Python exposes this as HTTP; Rust currently does not. Tracked as follow-up rather than current functionality.
- Positional model directories vs `--models` flags.
- Python-compatible short flags like `-v` and `-c`.
- Whether Rust MCP must match Python MCP tools.
- Whether Rust HTTP must match Python API routes.

Exit gate:

- A written CLI/API compatibility matrix exists before more behavior is added.

### P1: Fix High-Risk Correctness Bugs

SQL generator/rewriter:

- Add tests for derived and ratio metrics selected without their simple dependencies. Done.
- Fix `Metric::count()` builder output so it never emits invalid `* AS count_raw`. Done.
- Rewrite semantic `ORDER BY` references to output aliases. Done.
- Partition time comparison windows by non-time dimensions.
- Preserve filters in conversion metric generation.
- Either support or explicitly reject rewriter fan-out cases.

Core/config:

- Add fixtures for schema field parity: `metadata`, `meta`, `public`, `window`, `auto_dimensions`, metric `extends`, retention/cohort fields, stddev/variance aggregations.
- Fix directory inheritance and SQL `extends` handling.
- Decide whether graph-level metrics/table calculations remain graph-level or get attached to owner models.
- Harden SQL definition parsing so unknown syntax fails where public APIs return `Result`.
- Preserve complex aggregate expressions like `SUM(amount * price)`.

DuckDB/FFI:

- Add graph replace/update API. Done.
- Fix `SEMANTIC CREATE METRIC/DIMENSION/SEGMENT` update paths. Done.
- Fix `CREATE OR REPLACE MODEL` to update in-memory graph and persistence consistently. Done for in-memory replacement and persisted multiline removal.
- Clear `ACTIVE_MODEL` when graph state is cleared. Done.
- Replace process-global FFI state or explicitly key it by DuckDB database/session. Done in Rust FFI and DuckDB extension context plumbing.
- Include `sidemantic-rs/include/sidemantic.h` from C++ instead of redeclaring the ABI manually. Done.

Exit gate:

- New regression tests fail before fixes and pass after fixes.
- DuckDB mutation and autoload paths have direct tests.

### P2: Release And Packaging Readiness

Rust package:

- Decide whether crate name remains `sidemantic` or becomes a scoped/internal crate.
- Align Rust version with Python or define independent versioning.
- Add release notes and artifact expectations for the standalone CLI.

Python binding:

- Keep `python` and `python-adbc` split so a lightweight PyO3 build can avoid ADBC/Arrow, while the default wheel still preserves executable `execute_with_adbc`. Done.
- Validate with `maturin build` and an isolated wheel import/use smoke test. Done for the default `python-adbc` wheel, explicit lightweight `python` build, and explicit `python-adbc` feature build.
- Keep this optional. It should not compromise Pyodide-compatible Python installs.

DuckDB extension:

- Make CMake build or import the Rust staticlib as a proper dependency. Done.
- Remove hardcoded `target/release/libsidemantic.a` assumptions where possible.
- Fix `EXT_NAME=quack`. Done.
- Align DuckDB extension version with the selected release model.

WASM:

- Decide whether the regex rewrite fallback is a documented limited subset or should reach native parser parity.
- If limited, move fallback into its own module and test unsupported SQL forms. Unsupported forms are tested; module split remains optional cleanup.
- Run real wasm-bindgen tests in CI. Done.

Exit gate:

- Release process can build Rust CLI/runtime artifacts, optional Python extension, WASM package, and DuckDB extension without manual hidden steps.

## Recommended Immediate Work Order

1. Create a branch and checkpoint the copied Rust port.
2. Fix clippy.
3. Add CI coverage for standalone Rust feature checks.
4. Write a CLI/API compatibility matrix.
5. Add failing regression tests for the SQL and DuckDB correctness risks.
6. Fix SQL generator `COUNT(*)`, derived/ratio dependencies, and semantic `ORDER BY`.
7. Fix DuckDB FFI graph update/replace behavior.
8. Decide release ownership and versioning.

## Remaining Non-Blocking Work

P1:

- Broaden ADBC-backed execution coverage beyond DuckDB and add negative tests for driver-specific option mistakes.
- Finish SQL parity decisions for time comparison partitioning, conversion filters, and rewriter fan-out behavior.
- Add core/config parity fixtures for metadata, access flags, metric inheritance, retention/cohort fields, stddev/variance aggregations, and complex aggregate expressions.
- Add deeper compatibility and lifecycle tests before promoting MCP, HTTP, LSP, or workbench out of experimental status. Current HTTP/MCP/LSP/workbench tests prove executable baselines, not full product parity for secured HTTP deployments, cross-file LSP, or exhaustive TUI workflows.

P2:

- Add browser-specific WASM execution if browser packaging becomes a supported release target.
- Add release packaging automation for CLI binaries, Rust crate, Python wheel, WASM package, and DuckDB extension.

## Completion Criteria For Standalone Viability

The Rust port should not be considered standalone-ready until all of these are true:

- Clean branch with all required Rust files tracked.
- `cargo fmt`, clippy, default tests, feature checks, wasm check, maturin build, and DuckDB extension tests pass in CI.
- CLI/API/MCP/LSP/DuckDB/WASM contracts are documented.
- High-risk SQL and FFI correctness tests are present and passing.
- Packaging and versioning are explicit.
- `sidemantic-rs/README.md` explains current standalone usage and limits.
- MCP, HTTP, LSP, and workbench remain explicitly experimental until compatibility, lifecycle, successful execution, and full interactive behavior are tested beyond smoke coverage.
