// Smoke test for the typed-client runtime that needs no wasm build: exercises
// createClient / createSqlClient ref-checking + payload shaping, and the serve
// transport's semantic-SQL building + param interpolation, with fake executors.
//
// Run: node scripts/smoke_client.mjs  (or: bun run scripts/smoke_client.mjs)

import { createClient, createSqlClient } from "../client.js";
import { createServeTransport, interpolateParams } from "../adapters/serve.js";

function assert(condition, message) {
  if (!condition) {
    console.error(`FAIL: ${message}`);
    process.exit(1);
  }
}

const schema = {
  models: {
    orders: {
      dimensions: {
        status: { kind: "categorical", ts: "string" },
        created_at: { kind: "time", ts: "string", grains: ["day", "month"] },
      },
      metrics: { revenue: { agg: "sum", ts: "number" }, order_count: { agg: "count", ts: "number" } },
    },
  },
  topMetrics: ["finance.revenue_per_order"],
};

// createClient: payload shaping + ref validation against the schema.
let captured = null;
const client = createClient(schema, {
  run: async (payload) => {
    captured = payload;
    return [{ revenue: 1, order_count: 2, status: "completed", created__month: "2024-01-01" }];
  },
});

const rows = await client.query({
  metrics: ["orders.revenue", "orders.order_count"],
  dimensions: ["orders.status", "orders.created_at__month"],
  limit: 10,
});
assert(rows.length === 1, "client.query returns executor rows");
assert(captured.metrics.length === 2 && captured.dimensions.length === 2, "payload carries metrics + dimensions");
assert(captured.limit === 10, "payload carries limit");

await client.query({
  metrics: ["finance.revenue_per_order"],
  dimensions: ["orders.status"],
});
assert(captured.metrics[0] === "finance.revenue_per_order", "top-level dotted metric is accepted");

let rejectedMetric = false;
try {
  await client.query({ metrics: ["orders.nope"] });
} catch {
  rejectedMetric = true;
}
assert(rejectedMetric, "unknown metric is rejected");

let rejectedDim = false;
try {
  await client.query({ metrics: ["orders.revenue"], dimensions: ["orders.created_at__year"] });
} catch {
  rejectedDim = true;
}
assert(rejectedDim, "grain outside the declared set is rejected");

// createSqlClient: forwards sql + params to the executor.
let capturedSql = null;
let capturedParams = null;
let capturedParamTypes = null;
const sqlClient = createSqlClient({
  run: async (sql, params, paramTypes) => {
    capturedSql = sql;
    capturedParams = params;
    capturedParamTypes = paramTypes;
    return [{ status: "completed", revenue: 5 }];
  },
  paramTypes: {
    "SELECT orders.status, orders.revenue FROM orders WHERE orders.region = {{ region }}": { region: "unquoted" },
  },
});
const sqlRows = await sqlClient.query("SELECT orders.status, orders.revenue FROM orders WHERE orders.region = {{ region }}", {
  region: "us",
});
assert(sqlRows.length === 1, "sqlClient.query returns executor rows");
assert(/FROM orders/.test(capturedSql), "sqlClient forwards the SQL");
assert(capturedParams.region === "us", "sqlClient forwards params");
assert(capturedParamTypes.region === "unquoted", "sqlClient forwards generated param type metadata");

// serve transport: builds a semantic SELECT and interpolates {{params}}.
const seen = [];
const serve = createServeTransport({
  query: async (sql) => {
    seen.push(sql);
    return [{ status: "completed", revenue: 9 }];
  },
});
await serve.run({ metrics: ["orders.revenue"], dimensions: ["orders.status"], filters: ["orders.status = 'completed'"], limit: 5 });
assert(/^SELECT orders\.status, orders\.revenue FROM orders/.test(seen[0]), `serve.run builds semantic SQL, got: ${seen[0]}`);
assert(/WHERE orders\.status = 'completed'/.test(seen[0]) && /LIMIT 5/.test(seen[0]), "serve.run adds WHERE + LIMIT");

let rejectedSkipDefaultTime = false;
try {
  await serve.run({ metrics: ["orders.revenue"], skip_default_time_dimensions: true });
} catch (error) {
  rejectedSkipDefaultTime = /skip_default_time_dimensions/.test(error.message);
}
assert(rejectedSkipDefaultTime, "serve.run rejects skip_default_time_dimensions because SQL cannot carry it");

const seenWithSchema = [];
const serveWithSchema = createServeTransport({
  schema,
  query: async (sql) => {
    seenWithSchema.push(sql);
    return [];
  },
});
let rejectedTopMetricFrom = false;
try {
  await serveWithSchema.run({ metrics: ["finance.revenue_per_order"] });
} catch (error) {
  rejectedTopMetricFrom = /model-qualified field/.test(error.message);
}
assert(rejectedTopMetricFrom, "serve.run does not infer FROM from a top-level dotted metric");
await serveWithSchema.run({ metrics: ["finance.revenue_per_order"], dimensions: ["orders.status"] });
assert(
  /^SELECT orders\.status, finance\.revenue_per_order FROM orders/.test(seenWithSchema[0]),
  `serve.run uses a real model field to infer FROM, got: ${seenWithSchema[0]}`,
);

await serve.runSql("SELECT orders.revenue FROM orders WHERE orders.created_at >= {{ start }}", { start: "2024-01-01" });
assert(/>= '2024-01-01'/.test(seen[1]), `serve.runSql interpolates params, got: ${seen[1]}`);

assert(interpolateParams("a = {{ n }}", { n: 3 }) === "a = 3", "numbers interpolate unquoted");
assert(interpolateParams("a = {{ b }}", { b: true }) === "a = TRUE", "booleans interpolate as TRUE/FALSE");
assert(interpolateParams("a = {{ s }}", { s: "x'y" }) === "a = 'x''y'", "strings are escaped");
assert(interpolateParams("a = {{ ident }}", { ident: "orders.status" }, { ident: "unquoted" }) === "a = orders.status", "unquoted params interpolate raw identifiers");

console.log("SMOKE_CLIENT_OK");
