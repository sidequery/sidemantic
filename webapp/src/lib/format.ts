import { NULL_TOKEN } from "../data/types";
import { formatUiCompact, formatUiValue, labelize as coreLabelize } from "./uiCore.js";

export type Tone = "positive" | "negative" | "neutral";

/** Human display for a crossfilter dimension value (NULL/empty render as an em dash). */
export function displayDimValue(value: string): string {
  return value === NULL_TOKEN || value === "" ? "—" : value;
}

/** "order_count" -> "Order Count", "orders.revenue" -> "Orders Revenue". */
export function labelize(value?: string | null): string {
  return coreLabelize(value);
}

export type FormatHint = { format?: string; type?: string; compact?: boolean };

/** Format a metric/dimension value for display, honoring optional format/type hints. */
export function formatValue(value: unknown, hint: FormatHint = {}): string {
  return formatUiValue(value, hint);
}

/** Compact magnitude for axis ticks and dense labels (1.2K, 3.4M), honoring %/$ hints. */
export function formatCompact(value: unknown, hint: FormatHint = {}): string {
  return formatUiCompact(value, hint);
}

/** Tone for a signed change: up is positive, down is negative, flat/NaN is neutral. */
function changeTone(change: number): Tone {
  if (!Number.isFinite(change) || change === 0) return "neutral";
  return change > 0 ? "positive" : "negative";
}

/** Share of a total as a percent label ("12.3%"), for the leaderboard "% of total" context column.
 *  A non-positive or non-finite total has no meaningful share → em dash. */
export function formatPercentOfTotal(value: number, total: number): { label: string; tone: Tone } {
  if (!Number.isFinite(value) || !Number.isFinite(total) || total <= 0) return { label: "—", tone: "neutral" };
  const pct = (value / total) * 100;
  return { label: `${pct.toLocaleString(undefined, { maximumFractionDigits: 1 })}%`, tone: "neutral" };
}

/** Absolute period-over-period change (current − previous) as a signed, format-aware label for the
 *  leaderboard Δ column. A missing/non-finite previous value has no delta → em dash. */
export function formatDeltaAbs(
  current: number,
  previous: number | null | undefined,
  hint: FormatHint = {},
): { label: string; tone: Tone } {
  if (previous === null || previous === undefined || !Number.isFinite(previous) || !Number.isFinite(current)) {
    return { label: "—", tone: "neutral" };
  }
  const change = current - previous;
  const sign = change > 0 ? "+" : change < 0 ? "−" : "";
  return { label: `${sign}${formatCompact(Math.abs(change), hint)}`, tone: changeTone(change) };
}

/** Percent period-over-period change for the leaderboard Δ% column. A missing/zero previous value
 *  has no defined percent change → em dash (never a fabricated 0% or ∞). */
export function formatDeltaPct(current: number, previous: number | null | undefined): { label: string; tone: Tone } {
  if (previous === null || previous === undefined || !Number.isFinite(previous) || previous === 0 || !Number.isFinite(current)) {
    return { label: "—", tone: "neutral" };
  }
  const change = (current - previous) / Math.abs(previous);
  const pct = (change * 100).toLocaleString(undefined, { maximumFractionDigits: 1, signDisplay: "exceptZero" });
  return { label: `${pct}%`, tone: changeTone(change) };
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
