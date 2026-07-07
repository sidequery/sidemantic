import { NULL_TOKEN } from "../data/types";

export type Tone = "positive" | "negative" | "neutral";

/** Human display for a crossfilter dimension value (NULL/empty render as an em dash). */
export function displayDimValue(value: string): string {
  return value === NULL_TOKEN || value === "" ? "—" : value;
}

/** "order_count" -> "Order Count", "orders.revenue" -> "Orders Revenue". */
export function labelize(value?: string | null): string {
  return String(value ?? "")
    .replaceAll("_", " ")
    .replaceAll(".", " ")
    .replace(/\b\w/g, (char) => char.toUpperCase())
    .trim();
}

function isPercentFormat(format?: string, type?: string): boolean {
  const f = (format ?? "").toLowerCase();
  return f.includes("%") || f.includes("percent") || f.includes("pct") || type === "ratio";
}

function isCurrencyFormat(format?: string): boolean {
  const f = (format ?? "").toLowerCase();
  return f.includes("$") || f.includes("usd") || f.includes("currency") || f.includes("dollar");
}

export type FormatHint = { format?: string; type?: string; compact?: boolean };

/** Format a metric/dimension value for display, honoring optional format/type hints. */
export function formatValue(value: unknown, hint: FormatHint = {}): string {
  if (value === null || value === undefined || value === "") return "—";
  const numeric = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(numeric)) return String(value);

  if (isPercentFormat(hint.format, hint.type)) {
    const pct = Math.abs(numeric) <= 1 ? numeric * 100 : numeric;
    return `${pct.toLocaleString(undefined, { maximumFractionDigits: 1 })}%`;
  }

  const options: Intl.NumberFormatOptions = hint.compact
    ? { notation: "compact", maximumFractionDigits: 1 }
    : { maximumFractionDigits: 2 };
  const formatted = numeric.toLocaleString(undefined, options);
  return isCurrencyFormat(hint.format) ? `$${formatted}` : formatted;
}

/** Compact magnitude for axis ticks and dense labels (1.2K, 3.4M), honoring %/$ hints. */
export function formatCompact(value: unknown, hint: FormatHint = {}): string {
  if (value === null || value === undefined || value === "") return "—";
  const numeric = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(numeric)) return String(value);
  if (isPercentFormat(hint.format, hint.type)) {
    const pct = Math.abs(numeric) <= 1 ? numeric * 100 : numeric;
    return `${pct.toLocaleString(undefined, { maximumFractionDigits: 1 })}%`;
  }
  const formatted = numeric.toLocaleString(undefined, { notation: "compact", maximumFractionDigits: 1 });
  return isCurrencyFormat(hint.format) ? `$${formatted}` : formatted;
}

/** Period-over-period delta label + tone for a KPI comparison. */
export function formatDelta(current: number, previous: number | null | undefined): { label: string; tone: Tone } | null {
  if (previous === null || previous === undefined || !Number.isFinite(previous) || previous === 0) return null;
  if (!Number.isFinite(current)) return null;
  const change = (current - previous) / Math.abs(previous);
  const pct = (change * 100).toLocaleString(undefined, { maximumFractionDigits: 1, signDisplay: "exceptZero" });
  const tone: Tone = change > 0 ? "positive" : change < 0 ? "negative" : "neutral";
  return { label: `${pct}%`, tone };
}

/** Single-quote escape for SQL string literals used in filter expressions. */
export function sqlLiteral(value: string): string {
  return `'${value.replaceAll("'", "''")}'`;
}

/** Human summary of a dimension's filter for a pill, e.g. "is 3 values", "is not US",
 *  "contains 'acme'". `label` is the dimension label; the caller prefixes it. */
export function filterSummary(filter: { mode: "include" | "exclude" | "contains"; values: string[]; pattern?: string }): string {
  if (filter.mode === "contains") return `contains ${sqlLiteral(filter.pattern ?? "")}`;
  const { values } = filter;
  const verb = filter.mode === "exclude" ? "is not" : "is";
  if (values.length === 0) return verb; // transient empty state
  if (values.length === 1) return `${verb} ${displayDimValue(values[0])}`;
  if (values.length <= 2) return `${verb} ${values.map(displayDimValue).join(", ")}`;
  return `${verb} ${values.length} values`;
}
