import type { Catalog, DashboardSpec, QueryResult, StructuredQuery } from "./types";

// One interface, two implementations. The HTTP adapter targets the shared contract both the
// Python (`sidemantic api-serve`) and Rust (`sidemantic-server`) backends expose identically.
// A future `wasmAdapter` (Rust-WASM compile + DuckDB-WASM execute, fully in-browser) can drop
// in behind the same interface without any UI change.
export interface SidemanticBackend {
  /** GET /health — true when the backend is reachable and a layer is loaded. */
  health(): Promise<boolean>;
  /** Rich semantic catalog from /describe, falling back to /graph (+/models) when absent. */
  getCatalog(): Promise<Catalog>;
  /** GET /dashboard — configured declarative dashboard, or null for the generic explorer. */
  getDashboard(): Promise<DashboardSpec | null>;
  /** POST /compile — semantic query -> dialect SQL, no execution. */
  compile(query: StructuredQuery): Promise<string>;
  /** POST /query — execute (Arrow preferred, JSON fallback) and return rows + SQL. */
  runQuery(query: StructuredQuery): Promise<QueryResult>;
}
