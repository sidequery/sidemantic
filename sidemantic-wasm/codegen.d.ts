import type { SchemaShape } from "./client";

export interface GenerateClientSchemaOptions {
  /** Embed the models as `SCHEMA_YAML` (needed by the wasm transport). Default true. */
  includeYaml?: boolean;
  /** Override the wasm binary location. */
  wasmUrl?: string | URL | BufferSource;
}

/** Build the `schema` payload from a parsed wasm graph (JSON from `loadGraph`). */
export function buildClientSchema(graph: unknown): SchemaShape;

/** Generate the typed-client schema module from Sidemantic YAML model definitions. */
export function generateClientSchema(models: string, options?: GenerateClientSchemaOptions): Promise<string>;

export interface ExtractSqlOptions {
  /** Call name whose first string-literal argument is semantic SQL (default "query"). */
  call?: string;
}

/** Extract `<call>(...)` semantic-SQL literals from TypeScript source strings. */
export function extractSqlLiterals(sources: string[], options?: ExtractSqlOptions): string[];

export interface GenerateSqlTypesOptions {
  wasmUrl?: string | URL | BufferSource;
}

/** Generate a `GeneratedQueries` interface from semantic-SQL literals (sqlx-style). */
export function generateSqlTypes(models: string, queries: string[], options?: GenerateSqlTypesOptions): Promise<string>;
