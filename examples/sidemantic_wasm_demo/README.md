# Sidemantic WASM Browser Demo

This demo runs fully in the browser:

- Sidemantic Rust WASM compiles and validates semantic YAML.
- DuckDB-WASM creates local tables and executes the generated SQL.
- No Pyodide, Python server, API server, or backend process is used for query work.
- The UI uses the copied static component kit from `skills/sidemantic-webapp-builder`: metric cards, sparklines, filter pills, query debug, data preview, and dimension leaderboards.
- The data/model vocabulary matches the repo's e-commerce demo: `orders`, `customers`, and `products`.

## Run

Build the Sidemantic Rust WASM bundle:

```bash
./examples/sidemantic_wasm_demo/scripts/build_wasm.sh
```

Serve the static app:

```bash
bun examples/sidemantic_wasm_demo/server.ts --port 5174
```

Open:

```text
http://127.0.0.1:5174/
```

## What To Check

- `Compile` calls `wasm_compile_with_yaml_query` for totals, time series, all leaderboards, and preview queries.
- `Validate` calls `wasm_validate_query_with_yaml`.
- The generated SQL is executed by DuckDB-WASM against an in-browser `orders` table.
- Metric card selection, time grain changes, dimension filters, and reset all recompile and re-execute browser-local queries.
- Runtime-specific code is isolated under `src/runtime/`; component code stays runtime-agnostic.
