import type { Plugin } from "vite";

export interface SidemanticSqlOptions {
  /** Files or directories to scan for `<call>(...)` semantic-SQL literals. */
  sources: string[];
  /** Where to write the generated `GeneratedQueries` interface. */
  output: string;
  /** Call name whose first string-literal argument is semantic SQL (default "query"). */
  call?: string;
}

export interface SidemanticPluginOptions {
  /** Path to the semantic-layer YAML (watched for changes). */
  models: string;
  /** Where to write the client schema module (default "src/sidemantic.client.generated.ts"). */
  output?: string;
  /** Embed `SCHEMA_YAML` for the wasm transport (default true). */
  includeYaml?: boolean;
  /** Optional sqlx-style query-type codegen. */
  sql?: SidemanticSqlOptions;
  /** Override the path to the wasm binary the plugin reads. */
  wasmUrl?: string | URL;
}

export function sidemantic(options: SidemanticPluginOptions): Plugin;
export default sidemantic;
