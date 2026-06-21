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

export type MetricFormat = "currency" | "percent" | "number";

export type MetricConfig = {
  key: string;
  label?: string;
  format?: MetricFormat;
};

// Mirrors the static `formatValue` signature so currency/percent formatting is identical across both
// implementations. Keep the option names (`currency`, `style`, `maximumFractionDigits`) in sync.
export type ValueFormatOptions = {
  currency?: string;
  maximumFractionDigits?: number;
  style?: "currency" | "percent" | "decimal";
};

export function labelize(value: string | undefined | null) {
  return String(value || "")
    .replaceAll("_", " ")
    .replaceAll(".", " ")
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

export function formatValue(value: unknown, options: ValueFormatOptions = {}) {
  if (value === null || value === undefined || value === "") return "—";
  const numeric = Number(value);
  if (Number.isFinite(numeric)) {
    return numeric.toLocaleString(undefined, {
      currency: options.currency,
      maximumFractionDigits: options.maximumFractionDigits ?? 2,
      style: options.style,
    });
  }
  return String(value);
}

export function metricConfigFor(metrics: MetricConfig[] | undefined, metricKey: string): MetricConfig {
  return (metrics || []).find((metric) => metric.key === metricKey) || metrics?.[0] || { key: metricKey };
}

export function metricValueFormat(metrics: MetricConfig[] | undefined, metricKey: string): ValueFormatOptions {
  const metric = metricConfigFor(metrics, metricKey);
  if (metric.format === "currency") {
    return { currency: "USD", maximumFractionDigits: 0, style: "currency" };
  }
  if (metric.format === "percent") {
    return { maximumFractionDigits: 1, style: "percent" };
  }
  return { maximumFractionDigits: 0 };
}

export function aliasFor(query: SidemanticQuerySpec, ref: string | undefined) {
  if (!ref) return "";
  return query.output_aliases?.[ref] || ref.split(".").at(-1) || ref;
}

// Compact axis labels — 1.2k / 3.4M. Keep in sync with the static `formatCompact`.
export function formatCompact(value: number) {
  if (!Number.isFinite(value)) return "";
  return Intl.NumberFormat(undefined, { notation: "compact", maximumFractionDigits: 1 }).format(value);
}

// Evenly spaced y-axis ticks across [min, max]. Keep in sync with the static `axisTicks`.
export function axisTicks(min: number, max: number, count = 4): number[] {
  if (!(max > min)) return [min];
  const step = (max - min) / (count - 1);
  return Array.from({ length: count }, (_, index) => min + step * index);
}
