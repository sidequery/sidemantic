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
//   const client = createClient(schema, { run: t.run });

import { interpolateParams } from "./params.js";

export { interpolateParams };

function buildSemanticSql(query) {
  const dimensions = query.dimensions || [];
  const metrics = query.metrics || [];
  const refs = [...dimensions, ...metrics];
  if (!refs.length) throw new Error("query requires at least one metric or dimension");
  const qualified = refs.find((ref) => ref.includes("."));
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
 */
export function createServeTransport({ query } = {}) {
  if (typeof query !== "function") {
    throw new Error("createServeTransport requires { query } as (sql) => Promise<rows>");
  }
  return {
    async run(structuredQuery) {
      return query(buildSemanticSql(structuredQuery));
    },
    async runSql(sql, params) {
      return query(params ? interpolateParams(sql, params) : sql);
    },
  };
}
