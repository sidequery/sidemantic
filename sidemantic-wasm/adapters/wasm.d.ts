import type { QueryPayload } from "../client";
import type { ParamType } from "./params";

export interface WasmTransportOptions {
  /** Sidemantic YAML model definitions (the generated `SCHEMA_YAML`). */
  models: string;
  /** Runs SQL and resolves to result rows. */
  execute: (sql: string) => Promise<Record<string, unknown>[]>;
  /** Override the wasm binary location. */
  wasmUrl?: string | URL;
}

export interface WasmTransport {
  runtime: unknown;
  /** Structured-query executor for `createClient({ run })`. */
  run: (query: QueryPayload) => Promise<Record<string, unknown>[]>;
  /** Semantic-SQL executor for `createSqlClient({ run })`. */
  runSql: (
    sql: string,
    params?: Record<string, unknown>,
    paramTypes?: Record<string, ParamType>,
  ) => Promise<Record<string, unknown>[]>;
}

export function createWasmTransport(options: WasmTransportOptions): Promise<WasmTransport>;

export function interpolateParams(
  sql: string,
  params: Record<string, unknown>,
  paramTypes?: Record<string, ParamType>,
): string;
