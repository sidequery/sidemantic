// Smoke test for wasm-backed codegen (B1/B2). Verifies:
//  - generateClientSchema matches the committed Python `gen types` output (cross-gen parity)
//  - the wasm graph carries the computed fidelity fields (metric return_type, time grains)
//  - the result-schema export types columns and applies the {model}_{leaf} collision rename
// Needs a built wasm bundle (bash scripts/build.sh).
//
// Run: node scripts/smoke_codegen.mjs

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { createSidemanticRuntime } from "../index.js";
import { extractSqlLiterals, generateClientSchema, generateSqlTypes } from "../codegen.js";

function assert(condition, message) {
  if (!condition) {
    console.error(`FAIL: ${message}`);
    process.exit(1);
  }
}

const wasmBytes = readFileSync(fileURLToPath(new URL("../wasm/sidemantic_bg.wasm", import.meta.url)));
const modelsPath = fileURLToPath(new URL("../../examples/headless_dashboard/models.yml", import.meta.url));
const committedPath = fileURLToPath(new URL("../../examples/headless_dashboard/sidemantic.client.generated.ts", import.meta.url));

// 1. Cross-generator parity: JS output is byte-identical to the committed Python output.
const models = readFileSync(modelsPath, "utf8");
const generated = await generateClientSchema(models, { includeYaml: false, wasmUrl: wasmBytes });
assert(generated === readFileSync(committedPath, "utf8"), "generateClientSchema must match the committed Python `gen types` output");

const TWO_MODELS = `
models:
  - name: orders
    table: orders
    primary_key: id
    dimensions:
      - name: status
        type: categorical
        sql: status
      - name: region
        type: categorical
        sql: region
      - name: created_at
        type: time
        granularity: day
        sql: created_at
      - name: customer_id
        type: numeric
        sql: customer_id
    metrics:
      - name: revenue
        agg: sum
        sql: amount
      - name: order_count
        agg: count
        sql: id
    relationships:
      - name: customers
        type: many_to_one
        foreign_key: customer_id
  - name: customers
    table: customers
    primary_key: id
    dimensions:
      - name: region
        type: categorical
        sql: region
    metrics:
      - name: customer_count
        agg: count
        sql: id
`;

const runtime = await createSidemanticRuntime({ wasmUrl: wasmBytes });

// 2. Computed wire fields injected into the loaded graph.
const graph = runtime.loadGraph(TWO_MODELS);
const orders = graph.models.find((model) => model.name === "orders");
assert(orders.metrics.find((m) => m.name === "revenue").return_type === "NUMERIC", "sum metric return_type should be NUMERIC");
assert(orders.metrics.find((m) => m.name === "order_count").return_type === "BIGINT", "count metric return_type should be BIGINT");
const createdAt = orders.dimensions.find((d) => d.name === "created_at");
assert(
  Array.isArray(createdAt.effective_granularities) && createdAt.effective_granularities.includes("month"),
  "time dim effective_granularities should be populated",
);

// 3. result-schema export types output columns.
const rs = runtime.resultSchema(TWO_MODELS, {
  metrics: ["orders.revenue", "orders.order_count"],
  dimensions: ["orders.status", "orders.created_at__month"],
});
const types = Object.fromEntries(rs.map((column) => [column.name, column.data_type]));
assert(types.revenue === "NUMERIC", `revenue type: ${JSON.stringify(rs)}`);
assert(types.order_count === "BIGINT", `order_count type: ${JSON.stringify(rs)}`);
assert(types.status === "VARCHAR", `status type: ${JSON.stringify(rs)}`);
assert(types.created_at__month === "DATE", `created_at__month type: ${JSON.stringify(rs)}`);

// 4. {model}_{leaf} collision rename when two models share a selected leaf.
const collide = runtime.resultSchema(TWO_MODELS, {
  metrics: ["orders.revenue"],
  dimensions: ["orders.region", "customers.region"],
});
const collideNames = collide.map((column) => column.name);
assert(collideNames.includes("orders_region") && collideNames.includes("customers_region"), `collision aliases: ${JSON.stringify(collide)}`);

// 5. JS gen sql is byte-identical to the committed Python `gen sql` output.
const queriesSource = readFileSync(fileURLToPath(new URL("../../examples/headless_dashboard/queries.ts", import.meta.url)), "utf8");
const committedSql = readFileSync(fileURLToPath(new URL("../../examples/headless_dashboard/sidemantic.queries.generated.ts", import.meta.url)), "utf8");
const generatedSql = await generateSqlTypes(models, extractSqlLiterals([queriesSource]), { wasmUrl: wasmBytes });
assert(generatedSql === committedSql, "generateSqlTypes must match the committed Python `gen sql` output");

// 6. Explicit `AS` aliases are carried into the generated row type (matches the runtime + Python).
const aliased = await generateSqlTypes(models, ["SELECT orders.revenue AS sales, orders.region FROM orders"], { wasmUrl: wasmBytes });
assert(aliased.includes('"sales": number') && aliased.includes('"region": string'), `alias not preserved: ${aliased}`);

// 7. Malformed SQL is rejected at generation time (the whole query is validated via the engine,
// not just the projection list).
let rejectedMalformed = false;
try {
  await generateSqlTypes(models, ["SELCT orders.revenue FRM orders"], { wasmUrl: wasmBytes });
} catch {
  rejectedMalformed = true;
}
assert(rejectedMalformed, "malformed SQL should be rejected at generation time");

// 8. Bare refs and table aliases resolve to the FROM model (like the engine + Python generator).
const bareRefs = await generateSqlTypes(models, ["SELECT revenue, region FROM orders"], { wasmUrl: wasmBytes });
assert(bareRefs.includes('"revenue": number') && bareRefs.includes('"region": string'), `bare refs not resolved: ${bareRefs}`);
const tableAlias = await generateSqlTypes(models, ["SELECT o.revenue, o.region FROM orders AS o"], { wasmUrl: wasmBytes });
assert(tableAlias.includes('"revenue": number') && tableAlias.includes('"region": string'), `table alias not resolved: ${tableAlias}`);

// 8b. Implicit (no `AS`) projection aliases resolve like the engine + Python `gen sql`.
const implicitAlias = await generateSqlTypes(models, ["SELECT orders.revenue sales, orders.region FROM orders"], { wasmUrl: wasmBytes });
assert(implicitAlias.includes('"sales": number') && implicitAlias.includes('"region": string'), `implicit alias not parsed: ${implicitAlias}`);

// 8c. extractSqlLiterals ignores commented-out calls but keeps `//` inside SQL strings.
const scanned = extractSqlLiterals([
  "const a = query(`SELECT orders.revenue FROM orders`);\n" +
    '// const dead = query("SELECT orders.revnue FROM orders");\n' +
    '/* query("SELECT bogus FROM nope") */\n' +
    "const b = query(\"SELECT orders.region FROM orders WHERE url LIKE 'http://x'\");\n",
]);
assert(scanned.includes("SELECT orders.revenue FROM orders"), `real call missing: ${JSON.stringify(scanned)}`);
assert(scanned.some((s) => s.includes("http://x")), `// inside SQL string must survive: ${JSON.stringify(scanned)}`);
assert(!scanned.some((s) => s.includes("revnue") || s.includes("bogus")), `commented-out calls scanned: ${JSON.stringify(scanned)}`);

// 8d. `SELECT DISTINCT` is accepted: the modifier is stripped for analysis (it does not change
// column types), and the original query string stays the generated type key.
const distinctTypes = await generateSqlTypes(models, ["SELECT DISTINCT orders.region, orders.revenue FROM orders"], { wasmUrl: wasmBytes });
assert(distinctTypes.includes('"SELECT DISTINCT orders.region, orders.revenue FROM orders"'), `DISTINCT query key missing: ${distinctTypes}`);
assert(distinctTypes.includes('"region": string') && distinctTypes.includes('"revenue": number'), `DISTINCT projections not typed: ${distinctTypes}`);

// 8e. `SELECT *` expands to the model's dimensions then metrics (definition order), matching the
// rewriter + Python `gen sql` — the wasm rewriter rejects `*`, so codegen expands it itself.
const starTypes = await generateSqlTypes(models, ["SELECT * FROM orders"], { wasmUrl: wasmBytes });
assert(/"created_at": string;[\s\S]*"region": string;[\s\S]*"revenue": number/.test(starTypes), `star not expanded in order: ${starTypes}`);
assert(starTypes.includes('"order_count": number'), `star missing a metric: ${starTypes}`);

// 8f. A quoted alias (for spaces / reserved words) becomes a string-literal key, like Python/sqlglot.
const quotedAlias = await generateSqlTypes(models, ['SELECT orders.revenue AS "total sales" FROM orders'], { wasmUrl: wasmBytes });
assert(quotedAlias.includes('"total sales": number'), `quoted alias not typed: ${quotedAlias}`);

// 9. A top-level metric whose owner model has a default time dimension: the engine inserts that
// dimension into the output, so an explicit `AS` alias must stay on the metric (not slide onto the
// inserted time column). Regression for the positional-overlay misalignment.
const defaultTimeModels = `
models:
  - name: orders
    table: orders
    primary_key: id
    default_time_dimension: created_at
    default_grain: month
    dimensions:
      - name: created_at
        type: time
        granularity: day
        sql: created_at
    metrics:
      - name: revenue
        agg: sum
        sql: amount
      - name: cnt
        agg: count
        sql: id
metrics:
  - name: revenue_per_order
    type: ratio
    numerator: orders.revenue
    denominator: orders.cnt
`;
const topMetricAliased = await generateSqlTypes(defaultTimeModels, ["SELECT revenue_per_order AS r FROM orders"], { wasmUrl: wasmBytes });
const topMetricRow = topMetricAliased.split("\n").find((line) => line.includes("row:")) || "";
assert(/"r": number/.test(topMetricRow), `alias must stay on the metric (number), got: ${topMetricRow}`);
assert(/"created_at__month": string/.test(topMetricRow), `engine-inserted default time dim must keep its name + type, got: ${topMetricRow}`);
assert(!/"r": string/.test(topMetricRow), `alias must not slide onto the inserted time column, got: ${topMetricRow}`);

// 10. Top-level metrics survive in the generated client schema. The wasm runtime assigns each
// to its owner model too, but they must appear under topMetrics by their real ref (matching the
// Python generator), not as owner-qualified model metrics.
const topMetricModels = `
models:
  - name: orders
    table: orders
    primary_key: id
    metrics:
      - name: revenue
        agg: sum
        sql: amount
      - name: cnt
        agg: count
        sql: id
metrics:
  - name: revenue_per_order
    type: ratio
    numerator: orders.revenue
    denominator: orders.cnt
`;
const topSchema = await generateClientSchema(topMetricModels, { includeYaml: false, wasmUrl: wasmBytes });
assert(topSchema.includes('"revenue_per_order"'), `top-level metric missing from schema: ${topSchema}`);
assert(!topSchema.includes('"revenue_per_order": {'), `top-level metric must not be an owned model metric: ${topSchema}`);

// 11. A genuine model metric that shares a name with a different top-level metric must be kept in
// model.metrics (only an owner-assigned *copy* of a top-level metric is dropped). Here `aov` is a
// real model metric (avg) AND a top-level metric (ratio); the model `aov` stays, `revenue_per_order`
// (a pure top-level metric copied onto orders) does not.
const collisionModels = `
models:
  - name: orders
    table: orders
    primary_key: id
    metrics:
      - {name: revenue, agg: sum, sql: amount}
      - {name: cnt, agg: count, sql: id}
      - {name: aov, agg: avg, sql: amount}
metrics:
  - {name: revenue_per_order, type: ratio, numerator: orders.revenue, denominator: orders.cnt}
  - {name: aov, type: ratio, numerator: orders.revenue, denominator: orders.cnt}
`;
const collisionSchema = await generateClientSchema(collisionModels, { includeYaml: false, wasmUrl: wasmBytes });
assert(/"aov": \{\s*"agg": "avg"/.test(collisionSchema), `genuine model metric aov must be kept: ${collisionSchema}`);
assert(!collisionSchema.includes('"revenue_per_order": {'), `assigned top-level copy must be dropped from model metrics: ${collisionSchema}`);
assert(/"topMetrics": \[\s*"aov",\s*"revenue_per_order"\s*\]/.test(collisionSchema), `both top metrics expected: ${collisionSchema}`);

console.log("SMOKE_CODEGEN_OK");
