// Copied outside the checkout by smoke_package.mjs. All runtime imports resolve
// through the installed tarball's public export map, including the WASM asset.
import assert from "node:assert/strict";
import { readFileSync, writeFileSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { createSidemanticRuntime } from "sidemantic-wasm";
import { createClient } from "sidemantic-wasm/client";
import { createWasmTransport } from "sidemantic-wasm/adapters/wasm";
import { generateClientSchema } from "sidemantic-wasm/codegen";

const packageRoot = new URL("./", import.meta.resolve("sidemantic-wasm"));
const manifest = JSON.parse(readFileSync(new URL("package.json", packageRoot), "utf8"));
for (const [subpath, entry] of Object.entries(manifest.exports)) {
  const specifier = subpath === "." ? manifest.name : manifest.name + subpath.slice(1);
  if (typeof entry === "string") {
    assert.ok(readFileSync(new URL(import.meta.resolve(specifier))).length > 0);
  } else {
    await import(specifier);
    assert.ok(readFileSync(new URL(entry.types, packageRoot)).length > 0, `Missing types for ${specifier}`);
  }
}

const models = `
models:
  - name: orders
    table: orders
    primary_key: id
    dimensions:
      - {name: status, type: categorical, sql: status}
    metrics:
      - {name: revenue, agg: sum, sql: amount}
`;
writeFileSync("models.yml", models);
const wasmUrl = readFileSync(new URL(import.meta.resolve("sidemantic-wasm/sidemantic_bg.wasm")));
const runtime = await createSidemanticRuntime({ wasmUrl });
const query = { metrics: ["orders.revenue"], dimensions: ["orders.status"] };
assert.deepEqual(runtime.validate(models, query), []);
assert.match(runtime.compile(models, query), /select/i);
assert.match(runtime.rewrite(models, "SELECT orders.revenue, orders.status FROM orders"), /select/i);
assert.ok(runtime.loadGraph(models));
assert.match(await generateClientSchema(models, { wasmUrl }), /revenue/);

let executedSql;
const transport = await createWasmTransport({
  models,
  wasmUrl,
  execute: async (sql) => {
    executedSql = sql;
    return [{ status: "complete", revenue: 12 }];
  },
});
const client = createClient({
  models: { orders: {
    dimensions: { status: { kind: "categorical", ts: "string" } },
    metrics: { revenue: { agg: "sum", ts: "number" } },
  } },
  topMetrics: [],
}, { run: transport.run });
assert.deepEqual(await client.query(query), [{ status: "complete", revenue: 12 }]);
assert.match(executedSql, /select/i);

execFileSync("node_modules/.bin/sidemantic-codegen", [
  "types", "models.yml", "--no-yaml", "--out", "schema.ts",
], { stdio: "inherit" });
assert.match(readFileSync("schema.ts", "utf8"), /revenue/);
