// Smoke test for the built bundle. Instantiates the wasm and exercises the
// core runtime operations. Exits nonzero on any failure.
//
// Run after `bun run build`:  bun run scripts/smoke.mjs  (or: node scripts/smoke.mjs)

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { createSidemanticRuntime } from "../index.js";

const wasmPath = fileURLToPath(new URL("../wasm/sidemantic_bg.wasm", import.meta.url));
const wasmBytes = readFileSync(wasmPath);

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

const query = { metrics: ["orders.revenue"], dimensions: ["orders.status"] };

function assert(condition, message) {
  if (!condition) {
    console.error(`FAIL: ${message}`);
    process.exit(1);
  }
}

const sidemantic = await createSidemanticRuntime({ wasmUrl: wasmBytes });

const errors = sidemantic.validate(models, query);
assert(Array.isArray(errors) && errors.length === 0, `expected no validation errors, got ${JSON.stringify(errors)}`);

const sql = sidemantic.compile(models, query);
assert(typeof sql === "string" && /select/i.test(sql), `expected SELECT SQL, got: ${sql}`);
assert(/revenue/i.test(sql) && /status/i.test(sql), `expected metric and dimension in SQL, got: ${sql}`);

const graph = sidemantic.loadGraph(models);
assert(graph && typeof graph === "object", "expected a parsed graph object");

console.log("compiled SQL:\n" + sql);
console.log("SMOKE_OK");
