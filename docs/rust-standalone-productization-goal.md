# Rust Standalone Productization Goal

This is the next goal after the standalone test-hardening pass. Release automation is explicitly out of scope for now.

## Objective

Productize the already-imported standalone Rust Sidemantic runtime surfaces while preserving the current passing baseline.

## Scope

- Start from the staged `rust-standalone-runtime` work.
- Do not revert existing staged or user changes.
- Keep compile/rewrite/validate/runtime surfaces useful without DB execution dependencies.
- Keep DB execution Rust-native and explicitly opt-in.
- Run all affected gates before calling the goal complete.
- Leave the repo staged and reviewable.
- Do not commit unless explicitly asked.

## P0: Feature Split

- Decouple ADBC/Arrow from non-execution features.
- Change `python` so it only enables PyO3 bindings.
- Add `python-adbc = ["python", "adbc-exec"]`.
- Change `mcp-server` so it does not imply `adbc-exec`.
- Add `mcp-adbc = ["mcp-server", "adbc-exec"]`.
- Change `runtime-server` so it does not imply `adbc-exec`.
- Add `runtime-server-adbc = ["runtime-server", "adbc-exec"]`.
- Change `workbench-tui` so it does not imply `adbc-exec`.
- Add `workbench-adbc = ["workbench-tui", "adbc-exec"]`.

## P0: Execution Gating

- Make non-ADBC builds support metadata, validation, compile, rewrite, diagnostics, and UI browsing.
- Gate execution-only code paths with the relevant ADBC feature.
- In Python, expose `execute_with_adbc` only under `python-adbc`, or expose a stub with a clear built-without-ADBC error.
- In HTTP, keep `/query/compile` and model metadata available without ADBC.
- In HTTP, make `/query/run` return a clear built-without-ADBC error unless built with `runtime-server-adbc`.
- In MCP, keep `list_models`, `get_models`, and `compile_query` available without ADBC.
- In MCP, make `run_query` return a clear built-without-ADBC error unless built with `mcp-adbc`.
- In workbench, make execution disabled or explicitly unavailable unless built with `workbench-adbc`.

## P0: Real ADBC Execution Proof

Use one local driver contract if viable, preferably DuckDB.

Add successful execution tests for:

- CLI `sidemantic run`.
- HTTP `POST /query/run`.
- MCP `run_query`.
- Python `execute_with_adbc` under `python-adbc`.

If no local Rust ADBC driver path is viable, document the blocker precisely and keep execution surfaces demoted.

## P1: Build Matrix

Add checks for:

- `cargo check --no-default-features`.
- `cargo check --features python`.
- `cargo check --features python-adbc`.
- `cargo check --features runtime-server`.
- `cargo check --features runtime-server-adbc`.
- `cargo check --features mcp-server`.
- `cargo check --features mcp-adbc`.
- `cargo check --features workbench-tui`.
- `cargo check --features workbench-adbc`.
- Existing default tests.
- Existing WASM tests.
- Existing DuckDB extension tests.
- Existing Python wheel smoke tests, adjusted for lightweight and ADBC-enabled builds if both wheels are built.

## P1: HTTP And MCP Productization

- Decide and document route/tool contracts.
- Add schema-level request/response tests.
- Test startup behavior.
- Test bad model path behavior.
- Add lifecycle/graceful shutdown behavior where feasible.
- Decide auth/CORS/body-limit behavior, or document them as non-goals.
- Keep unsupported Python compatibility routes/tools documented until implemented.

## P1: LSP Productization

- Decide whether LSP is SQL-definition-only or full Sidemantic project indexing.
- Add workspace loading tests.
- Add multi-file diagnostics if workspace indexing is in scope.
- Add hover assertions.
- Add completion assertions beyond the current smoke baseline where implemented.
- Add invalid request handling tests.
- Document unsupported adapter formats if SQL-only.

## P1: Workbench Productization

- Add PTY-level integration coverage.
- Test launch.
- Test initial render.
- Test keypresses.
- Test quit.
- Test bad model path behavior.
- Test no-DB execution disabled state.
- Test DB-backed execution if the ADBC driver contract is available.
- Keep deterministic `ratatui::TestBackend` state/render tests.

## P1: Cargo And Package Quality

- Add Cargo package metadata: `license`, `repository`, `homepage`, `readme`.
- Resolve or explicitly document yanked `wasm-bindgen/js-sys` lock entries.
- Make Rust, Python extension, and DuckDB extension version ownership explicit.
- Keep release automation out of scope.

## Docs

Update:

- `sidemantic-rs/README.md`.
- `docs/rust-standalone-contracts.md`.
- `docs/rust-standalone-runtime-plan.md`.

Every surface should be labeled supported, experimental, or unsupported based only on executable evidence.

## Completion Gates

- All affected Rust feature checks pass.
- Rust fmt passes.
- Rust clippy all-targets/all-features passes.
- Full Rust tests pass.
- WASM build and wasm-bindgen runtime tests pass.
- Python wheel smoke passes for the intended wheel variants.
- C ABI smoke still passes.
- DuckDB extension `make test` passes.
- Exact root AGENTS gates pass:
  - `uv run ruff check . --exclude docs/_extensions --exclude sidemantic-duckdb/extension-ci-tools --exclude sidemantic-duckdb/scripts --exclude sidemantic-duckdb/duckdb --exclude sidemantic/adapters/malloy_grammar --exclude sidemantic/adapters/holistics_grammar`
  - `uv run ruff format --check . --exclude docs/_extensions --exclude sidemantic-duckdb/extension-ci-tools --exclude sidemantic-duckdb/scripts --exclude sidemantic-duckdb/duckdb --exclude sidemantic/adapters/malloy_grammar --exclude sidemantic/adapters/holistics_grammar`
  - `uv run pytest -v`

