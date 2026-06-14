/**
 * Structured query payload. Field names match Sidemantic's native query
 * contract (snake_case), so the object is serialized and passed through as-is.
 */
export interface SidemanticQuery {
  metrics: string[];
  dimensions?: string[];
  /** SQL filter expressions, e.g. `"orders.status = 'completed'"`. */
  filters?: string[];
  order_by?: string[];
  limit?: number;
  ungrouped?: boolean;
  skip_default_time_dimensions?: boolean;
}

/** A query may be a structured payload or a raw YAML/JSON string. */
export type SidemanticQueryInput = SidemanticQuery | string;

/** Parsed semantic graph. Loosely typed; shape mirrors the Rust serialization. */
export type SemanticGraph = Record<string, unknown>;

export interface CreateRuntimeOptions {
  /**
   * Where to load the `.wasm` binary from. Defaults to the bundled binary
   * resolved relative to this module.
   */
  wasmUrl?: string | URL | Request | Response | BufferSource | WebAssembly.Module;
}

export interface SidemanticRuntime {
  /** Compile a query into SQL for the given model set. */
  compile(models: string, query: SidemanticQueryInput): string;
  /** Validate a query; returns error messages (empty array when valid). */
  validate(models: string, query: SidemanticQueryInput): string[];
  /** Rewrite a raw SQL string against the semantic layer. */
  rewrite(models: string, sql: string): string;
  /** Parse the model set into a semantic graph. */
  loadGraph(models: string): SemanticGraph;
  /** Generate catalog metadata for the model set under a schema name. */
  generateCatalogMetadata(models: string, schema?: string): string;
}

/** Initialize the underlying wasm module. Idempotent. */
export function initSidemantic(options?: CreateRuntimeOptions): Promise<unknown>;

/** Initialize wasm and return a typed runtime around the core operations. */
export function createSidemanticRuntime(options?: CreateRuntimeOptions): Promise<SidemanticRuntime>;

// Re-export the full raw wasm-bindgen surface (all `wasm_*` functions).
export * from "./wasm/sidemantic";
