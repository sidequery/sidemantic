// Field references are semantic refs the backend understands:
//   metric:     "orders.revenue"  (model metric)  |  "total_revenue" (graph-level metric)
//   dimension:  "orders.status"
//   time grain: "orders.created_at__month"
export type FieldRef = string;

export type Grain = "hour" | "day" | "week" | "month" | "quarter" | "year";

// Structured query, camelCase. Mapped to the backend's snake_case body at the HTTP boundary.
// Mirrors StructuredQueryRequest (Python) / QueryRequest (Rust).
export type StructuredQuery = {
  metrics?: FieldRef[];
  dimensions?: FieldRef[];
  filters?: string[];
  segments?: string[];
  orderBy?: string[];
  limit?: number;
  offset?: number;
  ungrouped?: boolean;
  parameters?: Record<string, unknown>;
  /** Route through materialized pre-aggregations when a rollup matches (graceful base-table
   *  fallback otherwise). The lever for million/billion-row datasets. */
  usePreaggregations?: boolean;
};

export type ResultRow = Record<string, string | number | boolean | null>;

export type QueryResult = {
  columns: string[];
  rows: ResultRow[];
  rowCount: number;
  sql: string;
};

// ---- Catalog: the UI-facing model derived from /describe (rich) or /graph (names only) ----

export type DimensionKind = "time" | "categorical" | "numeric" | "boolean" | string;

export type CatalogDimension = {
  ref: FieldRef; // "orders.status"
  name: string; // "status"
  model: string; // "orders"
  label: string;
  description?: string;
  type: DimensionKind;
  granularity?: string;
  supportedGranularities?: string[];
  format?: string;
};

export type CatalogMetric = {
  ref: FieldRef; // "orders.revenue" or "total_revenue"
  name: string; // "revenue"
  model?: string; // "orders" | undefined for graph-level metrics
  label: string;
  description?: string;
  agg?: string;
  type?: string;
  format?: string;
};

export type CatalogModel = {
  name: string;
  label: string;
  description?: string;
  table?: string;
  dimensions: CatalogDimension[];
  metrics: CatalogMetric[];
  timeDimension?: CatalogDimension;
  defaultGrain?: string;
};

export type Catalog = {
  models: CatalogModel[];
  graphMetrics: CatalogMetric[];
  dialect?: string;
};

// Convert a semantic ref to the output column alias the backend returns
// ("orders.revenue" -> "revenue", "orders.created_at__month" -> "created_at__month").
export function aliasOf(ref: FieldRef): string {
  const last = ref.split(".").at(-1);
  return last || ref;
}
