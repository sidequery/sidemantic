export type SidemanticResultRow = Record<string, string | number | boolean | null | undefined>;

export type SidemanticQueryResult = {
  columns: string[];
  sample_rows: SidemanticResultRow[];
  sample_row_count?: number;
};

export type SidemanticQuerySpec = {
  metrics?: string[];
  dimensions?: string[];
  filters?: string[];
  order_by?: string[];
  limit?: number;
  sql?: string;
  output_aliases?: Record<string, string>;
  result?: SidemanticQueryResult;
};

export type ExplorerFilterState = Record<string, string[]>;

export type MetricTone = "positive" | "negative" | "neutral";

export function labelize(value: string | undefined | null) {
  return String(value || "")
    .replaceAll("_", " ")
    .replaceAll(".", " ")
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

export function formatValue(value: unknown, maximumFractionDigits = 2) {
  if (value === null || value === undefined || value === "") return "—";
  const numeric = Number(value);
  if (Number.isFinite(numeric)) {
    return numeric.toLocaleString(undefined, { maximumFractionDigits });
  }
  return String(value);
}

export function aliasFor(query: SidemanticQuerySpec, ref: string | undefined) {
  if (!ref) return "";
  return query.output_aliases?.[ref] || ref.split(".").at(-1) || ref;
}
