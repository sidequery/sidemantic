# sqlctx-wasm

In-browser [sqlctx](https://github.com/sidequery/sqlctx) semantic layer. Compile, validate, and rewrite semantic queries into SQL entirely in the browser via WebAssembly — no backend.

This package **generates SQL**; it does not execute it. Pair it with a browser query engine such as [DuckDB-WASM](https://github.com/duckdb/duckdb-wasm) to run the generated SQL.

## Install

Build the package from the source checkout with Rust, the
`wasm32-unknown-unknown` target, and the `wasm-bindgen-cli` version in
`Cargo.lock`:

```bash
cd sqlctx-wasm
bun run build
bun pm pack --ignore-scripts --filename sqlctx-wasm.tgz
```

In the consumer project, run `bun add /path/to/sqlctx-wasm.tgz`.

## Quick start

```js
import { createSqlctxRuntime } from "sqlctx-wasm";

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

const sqlctx = await createSqlctxRuntime();

// Validate a query — returns [] when valid, or an array of error messages.
const errors = sqlctx.validate(models, {
  metrics: ["orders.revenue"],
  dimensions: ["orders.status"],
});

// Compile a query into SQL.
const sql = sqlctx.compile(models, {
  metrics: ["orders.revenue"],
  dimensions: ["orders.status"],
  filters: ["orders.status = 'completed'"],
  limit: 100,
});

// Execute the SQL with your engine of choice (e.g. DuckDB-WASM).
```

A query can be a structured payload (`SqlctxQuery`) or a raw YAML/JSON string.

### Querying with sqlctx SQL

If you'd rather write SQL than build a structured payload, `rewrite` takes a
sqlctx SQL query — `SELECT`ing metrics and dimensions by qualified name —
and expands it into executable SQL:

```js
const sqlctx = await createSqlctxRuntime();

const sql = sqlctx.rewrite(
  models,
  "SELECT orders.revenue AS rev, orders.status FROM orders ORDER BY rev DESC LIMIT 5",
);
// =>
// WITH orders_cte AS (SELECT *, amount AS revenue_raw FROM orders)
// SELECT orders_cte.status AS status, SUM(orders_cte.revenue_raw) AS revenue
// FROM orders_cte GROUP BY 1 ORDER BY revenue DESC LIMIT 5
```

Aliases, `ORDER BY` (by alias or position), `LIMIT`, and aggregate expressions
such as `SUM(orders.amount) / COUNT(*)` are supported, as are semantic `SELECT`s
nested in CTEs and subqueries. WASM rewrite parity vs. the Python engine is
partial — test the query shapes you depend on.

## API

| Method | Returns | Description |
|---|---|---|
| `createSqlctxRuntime(options?)` | `Promise<SqlctxRuntime>` | Initialize wasm and return the runtime. |
| `initSqlctx(options?)` | `Promise` | Initialize wasm only (idempotent). |
| `runtime.compile(models, query)` | `string` | Compile a query to SQL. |
| `runtime.validate(models, query)` | `string[]` | Validation errors; `[]` when valid. |
| `runtime.rewrite(models, sql)` | `string` | Rewrite a sqlctx SQL query into executable SQL. |
| `runtime.loadGraph(models)` | `object` | Parse models into a semantic graph. |
| `runtime.generateCatalogMetadata(models, schema?)` | `string` | Catalog metadata for the model set. |

Pass `{ wasmUrl }` to `createSqlctxRuntime` / `initSqlctx` to control where the `.wasm` binary loads from. By default it resolves the bundled binary relative to the module, which modern bundlers (Vite, esbuild, bun) handle via `new URL(..., import.meta.url)`.

The full raw `wasm_*` function surface is also re-exported from the package root and from `sqlctx-wasm/wasm` for advanced use.

## Scope and limitations

- **SQL generation only** — no query execution. Bring your own engine (DuckDB-WASM, a backend, etc.).
- **Model definitions are passed as YAML.** `compile` / `validate` / `rewrite` operate on YAML definitions; native SQL model definitions can be loaded via the raw `wasm_load_graph_with_sql` export. Importing external formats (Cube, LookML, dbt, …) stays in the Python package.
- **Feature parity is partial** vs. the Python layer; see the runtime feature matrix in the repo.
- **Size**: the binary is ~5.7 MB raw (~1.1 MB brotli). Serve it compressed and lazy-load alongside DuckDB-WASM.

## Building from source

The bundle is generated from the `sqlctx-rs` crate. Requires the Rust `wasm32-unknown-unknown` target and a `wasm-bindgen-cli` matching the crate's `wasm-bindgen` version.

```bash
bun run build   # runs scripts/build.sh -> wasm/
```

To check the distributable after building:

```bash
bun pm pack --ignore-scripts --filename sqlctx-wasm.tgz
bun run scripts/smoke_package.mjs sqlctx-wasm.tgz
```

The release workflow installs this tarball into an isolated consumer with lifecycle
scripts disabled, imports the public entry points, checks bundled declaration and
WASM files, and exercises compilation, rewriting, client transport, and the installed
codegen CLI. It uploads the tested tarball and uses that same file for an authorized
npm publish. These Node checks cover package completeness; browser runtime parity
still requires the browser tests.

## License

AGPL-3.0-only. See [LICENSE](./LICENSE).
