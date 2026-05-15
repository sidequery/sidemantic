# sidemantic-rs

Rust runtime and CLI for Sidemantic.

## Status

This crate is intended to run standalone. It owns the Rust CLI, runtime helpers, feature-gated HTTP/MCP/LSP/workbench binaries, WASM exports, C ABI for DuckDB, and optional `sidemantic_rs` Python extension.

Current limits:

- DuckDB FFI state is keyed by caller-provided context. The DuckDB extension passes a database/session context key; the legacy context-free C ABI remains as a compatibility wrapper around a default context.
- WASM rewrite support uses a limited fallback subset for simple single-table queries.
- MCP, HTTP server, LSP, and workbench are experimental feature-gated surfaces for this baseline. They compile and have real protocol or deterministic UI smoke tests, including DuckDB ADBC execution where applicable, but they are not compatibility-complete product surfaces yet.
- Python bindings are split: the normal wheel builds with `python-adbc` for compatibility and executes through `execute_with_adbc`; the lower-level `python` feature builds the lightweight PyO3 module without ADBC/Arrow and exposes an `execute_with_adbc` stub that fails with build-feature guidance.
- HTTP, MCP, and workbench compile without ADBC/Arrow. Database execution is explicitly opt-in through `runtime-server-adbc`, `mcp-adbc`, or `workbench-adbc`.
- Rust HTTP/MCP now cover the core Python-facing metadata/query/SQL/catalog/chart shapes, including HTTP bearer auth, CORS allow-list headers, and request body limits. Intentional differences remain: HTTP has no Arrow output yet, and MCP chart output uses a Rust-rendered PNG preview rather than Python Altair/vl-convert or MCP Apps UI resources.

## Build

```bash
cargo build --manifest-path sidemantic-rs/Cargo.toml
```

## CLI

The default binary is `sidemantic`.

```bash
cargo run --manifest-path sidemantic-rs/Cargo.toml --bin sidemantic -- --help
```

Core commands:

- `compile`: compile semantic query references to SQL
- `rewrite`: rewrite raw SQL with semantic references
- `validate`: validate model/query references
- `run`: compile + execute via ADBC (`adbc-exec` feature)
- `preagg materialize|recommend|apply|refresh`: pre-aggregation helpers
- `workbench`: interactive `ratatui` workbench (`workbench-tui` feature)
- `mcp`: passthrough launcher for `sidemantic-mcp`
- `server`: passthrough launcher for `sidemantic-server`
- `lsp`: passthrough launcher for `sidemantic-lsp`

Runtime passthrough subcommands require feature-gated sibling binaries to be built:

```bash
cargo build --manifest-path sidemantic-rs/Cargo.toml --features mcp-server,runtime-server,runtime-lsp --bins
```

Execution-enabled runtime binaries require their ADBC feature variants:

```bash
cargo build --manifest-path sidemantic-rs/Cargo.toml --features mcp-adbc,runtime-server-adbc,workbench-adbc --bins
```

## Runtime Binaries

Rust-native runtime surfaces are available as feature-gated binaries:

- `sidemantic-mcp` (`mcp-server` feature, `rmcp`; `mcp-adbc` adds `run_query`, `run_sql`, and `create_chart` execution)
- `sidemantic-server` (`runtime-server` feature, `axum` HTTP API; `runtime-server-adbc` adds `/query`, `/query/run`, `/sql`, and `/raw` execution)
- `sidemantic-lsp` (`runtime-lsp` feature, stdio LSP for SQL definition authoring)

Build checks:

```bash
cargo check --manifest-path sidemantic-rs/Cargo.toml --no-default-features --all-targets
cargo check --manifest-path sidemantic-rs/Cargo.toml --features python --lib
cargo check --manifest-path sidemantic-rs/Cargo.toml --features python-adbc --lib
cargo check --manifest-path sidemantic-rs/Cargo.toml --features mcp-server --bin sidemantic-mcp
cargo check --manifest-path sidemantic-rs/Cargo.toml --features mcp-adbc --bin sidemantic-mcp
cargo check --manifest-path sidemantic-rs/Cargo.toml --features runtime-server --bin sidemantic-server
cargo check --manifest-path sidemantic-rs/Cargo.toml --features runtime-server-adbc --bin sidemantic-server
cargo check --manifest-path sidemantic-rs/Cargo.toml --features runtime-lsp --bin sidemantic-lsp
cargo test --manifest-path sidemantic-rs/Cargo.toml --features mcp-server --test mcp_protocol
cargo test --manifest-path sidemantic-rs/Cargo.toml --features runtime-server --test http_server
cargo test --manifest-path sidemantic-rs/Cargo.toml --features runtime-lsp --test lsp_protocol_smoke
cargo test --manifest-path sidemantic-rs/Cargo.toml --features workbench-tui --test workbench_pty_smoke
```

HTTP routes covered by the standalone server:

- `GET /readyz`
- `GET /health`
- `GET /models`
- `GET /models/{model}`
- `GET /graph`
- `POST /compile`
- `POST /query/compile`
- `POST /query`
- `POST /query/run`
- `POST /sql/compile`
- `POST /sql`
- `POST /raw`

HTTP controls:

- `--auth-token <token>` or `SIDEMANTIC_SERVER_AUTH_TOKEN`
- repeated `--cors-origin <origin>` or comma-separated `SIDEMANTIC_SERVER_CORS_ORIGINS`
- `--max-request-body-bytes <bytes>` or `SIDEMANTIC_SERVER_MAX_REQUEST_BODY_BYTES`

MCP tools/resources covered by the standalone server:

- `list_models`
- `get_models`
- `get_semantic_graph`
- `validate_query`
- `compile_query`
- `run_query`
- `run_sql`
- `create_chart`
- resource `semantic://catalog`

`sidemantic-mcp` accepts either `--models <path>` or a positional model path.

## Models Input

`--models` accepts:

- a directory with `.yml`, `.yaml`, and `.sql` files
- a single `.yml` / `.yaml` file
- a single `.sql` file

`.sql` supports either:

- explicit `MODEL (...)` statements
- SQL definitions with YAML frontmatter model metadata

## ADBC Execution

Enable execution features explicitly:

```bash
cargo run --manifest-path sidemantic-rs/Cargo.toml --features adbc-exec --bin sidemantic -- run \
  --models ./models \
  --metric orders.revenue \
  --driver adbc_driver_duckdb \
  --uri :memory:
```

For DuckDB's native shared library, use the ADBC entrypoint and `path` database option:

```bash
cargo run --manifest-path sidemantic-rs/Cargo.toml --features adbc-exec --bin sidemantic -- run \
  --models ./models \
  --metric orders.revenue \
  --driver /path/to/libduckdb.so \
  --entrypoint duckdb_adbc_init \
  --dbopt path=/tmp/warehouse.duckdb
```

`run` also supports:

- `--entrypoint <symbol>`
- `--username` / `--password` (maps to ADBC db username/password options)
- repeated `--dbopt key=value` (or `--dbopt key=value,key2=value2`)
- repeated `--connopt key=value` (or `--connopt key=value,key2=value2`)
- shorthands: `--catalog`, `--schema`, `--autocommit`, `--read-only`, `--isolation-level`

Environment fallbacks:

- `SIDEMANTIC_ADBC_DRIVER`
- `SIDEMANTIC_ADBC_URI`
- `SIDEMANTIC_ADBC_ENTRYPOINT`
- `SIDEMANTIC_ADBC_USERNAME`
- `SIDEMANTIC_ADBC_PASSWORD`
- `SIDEMANTIC_ADBC_DBOPTS` (comma-separated `key=value` list)
- `SIDEMANTIC_ADBC_CONNOPTS` (comma-separated `key=value` list)

Feature split:

- CLI `run` and `query` execution use `adbc-exec`.
- Python `execute_with_adbc` executes only with `python-adbc`; this is the default `maturin build` feature for the wheel. Lightweight `python` builds keep the symbol present but return a built-without-ADBC error.
- MCP `run_query`, `run_sql`, and `create_chart` execution are available only with `mcp-adbc`; without it, compile, metadata, validation, graph, and catalog resource paths still work and execution/chart tools return built-without-ADBC errors. ADBC builds accept either `--uri` or driver-specific `--dbopt` values such as `path=<duckdb file>`.
- HTTP `/query`, `/query/run`, `/sql`, and `/raw` execution are available only with `runtime-server-adbc`; without it, model, graph, and compile/rewrite routes still work and execution returns a built-without-ADBC error. ADBC builds accept either `--uri` or driver-specific `--dbopt` values such as `path=<duckdb file>`.
- Workbench execution is available only with `workbench-adbc`; without it, model browsing and SQL rewrite still work.

## Preagg Refresh

Refresh SQL planning is available via:

```bash
cargo run --manifest-path sidemantic-rs/Cargo.toml --bin sidemantic -- preagg refresh \
  --models ./models \
  --model orders \
  --name daily_revenue \
  --mode incremental \
  --from-watermark 2026-01-01
```

Modes:

- `full`: delete + reload target preagg table
- `incremental`: append rows at/after watermark
- `merge`: idempotent delete+insert in watermark window

Add `--execute` plus ADBC options to run statements instead of dry-run SQL output.

## Workbench (ratatui)

Enable TUI support:

```bash
cargo run --manifest-path sidemantic-rs/Cargo.toml --features workbench-tui --bin sidemantic -- workbench ./models
```

Enable TUI plus database execution:

```bash
cargo run --manifest-path sidemantic-rs/Cargo.toml --features workbench-adbc --bin sidemantic -- workbench ./models --connection duckdb:///:memory:
```

PTY smoke coverage:

```bash
cargo test --manifest-path sidemantic-rs/Cargo.toml --features workbench-tui --test workbench_pty_smoke
SIDEMANTIC_TEST_ADBC_DUCKDB_DRIVER=/path/to/libduckdb.so \
  cargo test --manifest-path sidemantic-rs/Cargo.toml --features workbench-adbc --test workbench_pty_smoke
```

Shortcuts:

- `F5` or `Ctrl+R`: rewrite SQL
- `F6` or `Ctrl+E`: execute SQL when a connection is configured
- `F7`: switch output view
- `Tab`: switch focus between model list and SQL editor
- `Up` / `Down`: move model selection
- `Enter` (in model list): load `select * from <model>` template
- `Ctrl+1`, `Ctrl+2`, `Ctrl+3`: switch layout mode
- `Ctrl+M`, `Ctrl+V`, `Ctrl+L`: cycle chart mode, value column, or label column
- `Esc` or `Ctrl+C`: quit

## WASM

WASM exports live in `src/wasm.rs` behind the `wasm` feature. Build check:

```bash
cargo check --manifest-path sidemantic-rs/Cargo.toml --no-default-features --features wasm --target wasm32-unknown-unknown
cargo install wasm-bindgen-cli --version 0.2.110
CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner \
  cargo test --manifest-path sidemantic-rs/Cargo.toml --target wasm32-unknown-unknown --features wasm --test wasm_bindgen_runtime
```

Supported WASM APIs are pure string/JSON helpers: YAML loading, SQL definition parsing from strings, compile, validation, catalog metadata, model/metric/dimension/relationship helpers, and pre-aggregation planning helpers. The runtime tests cover both happy paths and malformed JSON/YAML, validation, refresh-planning, and unsupported rewrite-shape errors.

Unsupported in WASM: directory loading, ADBC execution, DuckDB extension state, and full native SQL parser parity. Rewrite support is limited to simple single-table `SELECT ... FROM ...` queries with optional `WHERE`, `ORDER BY`, and `LIMIT`; joins, CTEs, subqueries, window functions, and multi-table queries are not part of the supported rewrite subset.

## Python Extension

```bash
cd sidemantic-rs
uvx maturin build --out dist
uv run --no-project --with dist/*.whl tests/python_wheel_smoke.py
```

This builds and smoke-tests the default optional `sidemantic_rs` module in an isolated environment. It is not required for CLI or WASM use. The default wheel includes `execute_with_adbc` for compatibility.

Lightweight Python build check:

```bash
cd sidemantic-rs
uvx maturin build --no-default-features --features python --out dist-python
uv run --no-project --with dist-python/*.whl tests/python_wheel_python_smoke.py
```

This verifies the lower-level PyO3 module can be built without ADBC/Arrow while keeping a clear disabled-execution `execute_with_adbc` stub.

ADBC-enabled Python wheel check:

```bash
cd sidemantic-rs
uvx maturin build --no-default-features --features python-adbc --out dist-adbc
uv run --no-project --with dist-adbc/*.whl tests/python_wheel_adbc_smoke.py
```

## DuckDB FFI

The C ABI is declared in `include/sidemantic.h`. The DuckDB extension includes that header directly and its CMake build declares the Rust static library as a dependency.

Context-aware APIs such as `sidemantic_rewrite_for_context` and `sidemantic_define_for_context` isolate loaded models and active model selection per DuckDB database/session key. The DuckDB extension derives that key from the active database instance instead of process-global mutable state. Context-free APIs are retained for existing callers and use a default global context.

File-backed DuckDB databases persist `SEMANTIC CREATE` definitions next to the database as `<stem>.sidemantic.sql`. In-memory DuckDB databases keep definitions session-local and do not read or write `./sidemantic_definitions.sql`.

Standalone staticlib check from the repository root:

```bash
CARGO_TARGET_DIR=sidemantic-rs/target cargo build --manifest-path sidemantic-rs/Cargo.toml --release --lib
```

Expected artifact on macOS/Linux:

```bash
sidemantic-rs/target/release/libsidemantic.a
```

The public C header smoke test compiles `tests/c_abi_smoke.c` against the static library and exercises the context-aware and legacy C ABI entrypoints.
