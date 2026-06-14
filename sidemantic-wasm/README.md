# sidemantic-wasm

In-browser [Sidemantic](https://sidemantic.com) semantic layer. Compile, validate, and rewrite semantic queries into SQL entirely in the browser via WebAssembly — no backend.

This package **generates SQL**; it does not execute it. Pair it with a browser query engine such as [DuckDB-WASM](https://github.com/duckdb/duckdb-wasm) to run the generated SQL.

## Install

```bash
bun add sidemantic-wasm
# or: npm install sidemantic-wasm
```

## Quick start

```js
import { createSidemanticRuntime } from "sidemantic-wasm";

const models = `
models:
  - name: orders
    table: orders
    primary_key: id
    dimensions:
      - name: status
        type: categorical
        sql: status
    metrics:
      - name: revenue
        agg: sum
        sql: amount
`;

const sidemantic = await createSidemanticRuntime();

// Validate a query — returns [] when valid, or an array of error messages.
const errors = sidemantic.validate(models, {
  metrics: ["orders.revenue"],
  dimensions: ["orders.status"],
});

// Compile a query into SQL.
const sql = sidemantic.compile(models, {
  metrics: ["orders.revenue"],
  dimensions: ["orders.status"],
  filters: ["orders.status = 'completed'"],
  limit: 100,
});

// Execute the SQL with your engine of choice (e.g. DuckDB-WASM).
```

A query can be a structured payload (`SidemanticQuery`) or a raw YAML/JSON string.

## API

| Method | Returns | Description |
|---|---|---|
| `createSidemanticRuntime(options?)` | `Promise<SidemanticRuntime>` | Initialize wasm and return the runtime. |
| `initSidemantic(options?)` | `Promise` | Initialize wasm only (idempotent). |
| `runtime.compile(models, query)` | `string` | Compile a query to SQL. |
| `runtime.validate(models, query)` | `string[]` | Validation errors; `[]` when valid. |
| `runtime.rewrite(models, sql)` | `string` | Rewrite raw SQL against the layer. |
| `runtime.loadGraph(models)` | `object` | Parse models into a semantic graph. |
| `runtime.generateCatalogMetadata(models, schema?)` | `string` | Catalog metadata for the model set. |

Pass `{ wasmUrl }` to `createSidemanticRuntime` / `initSidemantic` to control where the `.wasm` binary loads from. By default it resolves the bundled binary relative to the module, which modern bundlers (Vite, esbuild, bun) handle via `new URL(..., import.meta.url)`.

The full raw `wasm_*` function surface is also re-exported from the package root and from `sidemantic-wasm/wasm` for advanced use.

## Scope and limitations

- **SQL generation only** — no query execution. Bring your own engine (DuckDB-WASM, a backend, etc.).
- **Native Sidemantic YAML/SQL projects.** Importing external formats (Cube, LookML, dbt, …) stays in the Python package.
- **Feature parity is partial** vs. the Python layer; see the runtime feature matrix in the repo.
- **Size**: the binary is ~5 MB raw (~1.2 MB brotli). Serve it compressed and lazy-load alongside DuckDB-WASM.

## Building from source

The bundle is generated from the `sidemantic-rs` crate. Requires the Rust `wasm32-unknown-unknown` target, a `wasm-bindgen-cli` matching the crate's `wasm-bindgen` version, and (optionally) `wasm-opt` from binaryen.

```bash
bun run build   # runs scripts/build.sh -> wasm/
```

## License

AGPL-3.0-only. See [LICENSE](./LICENSE).
