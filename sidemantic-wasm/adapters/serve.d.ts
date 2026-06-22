import type { QueryPayload } from "../client";
import type { SchemaShape } from "../client";
import type { ParamType } from "./params";

export interface ServeTransportOptions {
  /** Sends SQL to a running `sidemantic serve` instance and resolves to rows. */
  query: (sql: string) => Promise<Record<string, unknown>[]>;
  /** Generated typed-client schema, used to distinguish top-level dotted metrics from model refs. */
  schema?: SchemaShape;
}

export interface ServeTransport {
  /**
   * Best-effort structured-query executor for `createClient({ run })`: builds a semantic SELECT.
   * `skip_default_time_dimensions` has no semantic-SQL equivalent, so a model with a default time
   * dimension may have it added by the server — use the wasm transport for exact row semantics.
   */
  run: (query: QueryPayload) => Promise<Record<string, unknown>[]>;
  /** Semantic-SQL executor for `createSqlClient({ run })`. Sends SQL through unchanged. */
  runSql: (
    sql: string,
    params?: Record<string, unknown>,
    paramTypes?: Record<string, ParamType>,
  ) => Promise<Record<string, unknown>[]>;
}

export function createServeTransport(options: ServeTransportOptions): ServeTransport;

export function interpolateParams(
  sql: string,
  params: Record<string, unknown>,
  paramTypes?: Record<string, ParamType>,
): string;
