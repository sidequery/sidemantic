// Transport that compiles/rewrites queries with the in-browser Rust wasm and
// executes the resulting SQL with a caller-provided runner (e.g. DuckDB-WASM).
//
// Usage:
//   import { createWasmTransport } from "sidemantic-wasm/adapters/wasm";
//   const duck = await createDuckDBRuntime(demoData);            // your DuckDB-WASM wrapper
//   const t = await createWasmTransport({ models: SCHEMA_YAML, execute: (sql) => duck.queryRows(sql).then(r => r.rows) });
//   const client = createClient(schema, { run: t.run });
//   const sqlClient = createSqlClient({ run: t.runSql });

import { createSidemanticRuntime } from "../index.js";
import { interpolateParams } from "./params.js";

export { interpolateParams };

/**
 * @param {object} opts
 * @param {string} opts.models  Sidemantic YAML model definitions (the generated SCHEMA_YAML).
 * @param {(sql: string) => Promise<Record<string, unknown>[]>} opts.execute  Runs SQL, returns rows.
 * @param {string|URL} [opts.wasmUrl]  Override the wasm binary location.
 */
export async function createWasmTransport({ models, execute, wasmUrl } = {}) {
  if (typeof models !== "string") throw new Error("createWasmTransport requires { models } as a YAML string");
  if (typeof execute !== "function") throw new Error("createWasmTransport requires { execute } as a SQL runner");
  const runtime = await createSidemanticRuntime(wasmUrl ? { wasmUrl } : {});
  return {
    runtime,
    async run(query) {
      return execute(runtime.compile(models, query));
    },
    async runSql(sql, params, paramTypes) {
      const text = params ? interpolateParams(sql, params, paramTypes) : sql;
      return execute(runtime.rewrite(models, text));
    },
  };
}
