// End-to-end typed-client example for the in-repo WASM + DuckDB-WASM runtime.
//
// Wires the shared client runtime to the demo's browser runtimes:
//   semantic query --(Rust wasm compile/rewrite)--> SQL --(DuckDB-WASM execute)--> rows
//
// This module is browser-oriented (the DuckDB runtime pulls DuckDB-WASM from a CDN).
// For compile-time TypeScript safety, generate static types first and import the schema:
//   npx sidemantic-codegen types models.yml --no-yaml -o sidemantic.client.generated.ts
//   import { schema } from "./sidemantic.client.generated";  // instead of buildClientSchema()

import { createClient, createSqlClient } from "../../sidemantic-wasm/client.js";
import { createWasmTransport } from "../../sidemantic-wasm/adapters/wasm.js";
import { buildClientSchema } from "../../sidemantic-wasm/codegen.js";

import { MODEL_YAML, createDemoData } from "./src/demo/ecommerce.js";
import { createDuckDBRuntime } from "./src/runtime/duckdb-runtime.js";

export async function runTypedClientDemo() {
  // 1. Browser runtimes: Rust wasm (compile/rewrite) + DuckDB-WASM (execute).
  const duck = await createDuckDBRuntime(createDemoData());
  const transport = await createWasmTransport({
    models: MODEL_YAML,
    execute: async (sql) => (await duck.queryRows(sql)).rows,
  });

  // 2. Schema built from the loaded graph (the runtime counterpart to `sidemantic gen types`).
  const schema = buildClientSchema(transport.runtime.loadGraph(MODEL_YAML));

  // 3a. Structured typed client.
  const client = createClient(schema, { run: transport.run });
  const byRegion = await client.query({
    metrics: ["orders.total_revenue", "orders.order_count"],
    dimensions: ["customers.region"],
  });

  // 3b. sqlx-style SQL client (same transport; the wasm rewrites the semantic SQL).
  const sql = createSqlClient({ run: transport.runSql });
  const monthly = await sql.query("SELECT orders.created__month, orders.total_revenue FROM orders");

  return { byRegion, monthly };
}
