// Sample typed semantic-SQL usage for the headless dashboard models.
//
// Regenerate the types after editing queries:
//   sidemantic gen types -m . --no-yaml -o sidemantic.client.generated.ts
//   sidemantic gen sql   -m . queries.ts  -o sidemantic.queries.generated.ts
//
// Structured client: autocomplete + typed rows over metrics/dimensions.
// SQL client: write semantic SQL, get typed rows keyed by the exact query string.

import { createClient, createSqlClient } from "sidemantic-wasm/client";
import { schema } from "./sidemantic.client.generated";
import { queryParamTypes } from "./sidemantic.queries.generated";
import type { GeneratedQueries } from "./sidemantic.queries.generated";

type Run = (query: unknown) => Promise<Record<string, unknown>[]>;
type SqlRun = (
  sql: string,
  params?: Record<string, unknown>,
  paramTypes?: Record<string, "string" | "number" | "date" | "yesno" | "unquoted">,
) => Promise<Record<string, unknown>[]>;

// --- structured client ---
export function makeClient(run: Run) {
  return createClient(schema, { run: run as never });
}

export async function revenueByRegionStructured(run: Run) {
  const client = makeClient(run);
  // rows: { region: string; revenue: number }[]
  return client.query({ metrics: ["orders.revenue"], dimensions: ["orders.region"] });
}

// --- sqlx-style SQL client ---
export function makeSqlClient(run: SqlRun) {
  return createSqlClient<GeneratedQueries>({ run, paramTypes: queryParamTypes });
}

export async function revenueByRegion(db: ReturnType<typeof makeSqlClient>) {
  // rows: { region: string; revenue: number }[]
  return db.query("SELECT orders.region, orders.revenue FROM orders");
}

export async function ordersByChannel(db: ReturnType<typeof makeSqlClient>) {
  // rows: { channel: string; order_count: number }[]
  return db.query("SELECT orders.channel, orders.order_count FROM orders");
}
