// Smoke test for the Vite plugin: its codegen (run on `buildStart`) writes the same client
// schema + sqlx query types the generators produce, into the configured output files.
// Needs a built wasm bundle.
//
// Run: node scripts/smoke_vite.mjs

import { existsSync, mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { sidemantic } from "../vite.js";
import { extractSqlLiterals, generateClientSchema, generateSqlTypes } from "../codegen.js";

function assert(condition, message) {
  if (!condition) {
    console.error(`FAIL: ${message}`);
    process.exit(1);
  }
}

const root = fileURLToPath(new URL("../../", import.meta.url));
const models = join(root, "examples/headless_dashboard/models.yml");
const queriesTs = join(root, "examples/headless_dashboard/queries.ts");
const wasmBytes = readFileSync(fileURLToPath(new URL("../wasm/sidemantic_bg.wasm", import.meta.url)));

const dir = mkdtempSync(join(tmpdir(), "sidemantic-vite-"));
const clientOut = join(dir, "client.generated.ts");
const sqlOut = join(dir, "queries.generated.ts");

const plugin = sidemantic({
  models,
  output: clientOut,
  includeYaml: false,
  sql: { sources: [queriesTs], output: sqlOut, call: "query" },
});
assert(plugin.name === "vite-plugin-sidemantic", "plugin should be named vite-plugin-sidemantic");

// `buildStart` runs the codegen (the same hook fires on dev-server start and `vite build`).
await plugin.buildStart();

const modelsYaml = readFileSync(models, "utf8");
assert(existsSync(clientOut), "plugin should write the client schema");
assert(
  readFileSync(clientOut, "utf8") === (await generateClientSchema(modelsYaml, { includeYaml: false, wasmUrl: wasmBytes })),
  "plugin client schema should match generateClientSchema",
);

const literals = extractSqlLiterals([readFileSync(queriesTs, "utf8")]);
assert(existsSync(sqlOut), "plugin should write the sql query types");
assert(
  readFileSync(sqlOut, "utf8") === (await generateSqlTypes(modelsYaml, literals, { wasmUrl: wasmBytes })),
  "plugin sql types should match generateSqlTypes",
);

console.log("SMOKE_VITE_OK");
