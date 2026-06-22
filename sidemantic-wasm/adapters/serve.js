// Transport that talks to a running `sidemantic serve` (PostgreSQL wire protocol)
// instance via a caller-provided SQL executor. `sidemantic serve` accepts
// *semantic* SQL and rewrites it server-side, so the SQL client sends queries
// through unchanged; the structured client builds a semantic SELECT.
//
// This is a server-side transport (browsers cannot speak the PG wire protocol).
// Provide `query` by wrapping a PG driver, e.g. with `pg`:
//   const pool = new Pool({ connectionString: "postgres://localhost:5433/sidemantic" });
//   const t = createServeTransport({ query: (sql) => pool.query(sql).then((r) => r.rows) });
//   const sqlClient = createSqlClient({ run: t.runSql });
//
// `run` (structured) is best-effort: the typed client's skip_default_time_dimensions flag has
// no semantic-SQL equivalent, so the server's own default-time-dimension behavior applies to
// the result. For exact typed-row semantics use createSqlClient for generated SQL types, or the
// wasm transport for createClient.

import { interpolateParams } from "./params.js";

export { interpolateParams };

function isModelQualifiedRef(ref, schema) {
  if (!ref.includes(".")) return false;
  if (!schema) return true;
  const dot = ref.indexOf(".");
  const modelName = ref.slice(0, dot);
  const model = schema.models?.[modelName];
  if (!model) return false;
  const fieldName = ref.slice(dot + 1).split("__", 1)[0];
  return Boolean(model.metrics?.[fieldName] || model.dimensions?.[fieldName]);
}

function buildSemanticSql(query, schema) {
  // `skip_default_time_dimensions` is a structured-compile flag with no semantic-SQL
  // equivalent, so it is ignored here (see the best-effort note at the top of the file).
  const dimensions = query.dimensions || [];
  const metrics = query.metrics || [];
  const refs = [...dimensions, ...metrics];
  if (!refs.length) throw new Error("query requires at least one metric or dimension");
  const qualified = refs.find((ref) => isModelQualifiedRef(ref, schema));
  if (!qualified) {
    throw new Error("serve transport needs a model-qualified field (e.g. orders.revenue) to infer FROM");
  }
  const fromModel = qualified.split(".")[0];
  let sql = `SELECT ${refs.join(", ")} FROM ${fromModel}`;
  if (query.filters && query.filters.length) sql += ` WHERE ${query.filters.join(" AND ")}`;
  if (query.order_by && query.order_by.length) sql += ` ORDER BY ${query.order_by.join(", ")}`;
  if (query.limit != null) sql += ` LIMIT ${Number(query.limit)}`;
  return sql;
}

/**
 * @param {object} opts
 * @param {(sql: string) => Promise<Record<string, unknown>[]>} opts.query  Sends SQL to the server, returns rows.
 * @param {object} [opts.schema] Generated typed-client schema. When provided,
 * top-level dotted metric names are not mistaken for model-qualified fields.
 */
export function createServeTransport({ query, schema } = {}) {
  if (typeof query !== "function") {
    throw new Error("createServeTransport requires { query } as (sql) => Promise<rows>");
  }
  return {
    async run(structuredQuery) {
      return query(buildSemanticSql(structuredQuery, schema));
    },
    async runSql(sql, params, paramTypes) {
      return query(params ? interpolateParams(sql, params, paramTypes) : sql);
    },
  };
}
