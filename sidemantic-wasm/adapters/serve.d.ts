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
  /** Structured-query executor for `createClient({ run })`. Builds a semantic SELECT. */
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
