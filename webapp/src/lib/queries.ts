import { NULL_TOKEN, type CatalogDimension, type FieldRef, type Grain, type StructuredQuery } from "../data/types";
import { sqlLiteral } from "./format";
import { timeFilters, type DateRange } from "./time";

// Crossfilter selections: dimension ref -> selected values. Values OR within a dimension,
// dimensions AND across each other (per the approved crossfilter rules). A value may be NULL_TOKEN.
export type FilterState = Record<FieldRef, string[]>;

/** dimension ref -> semantic type, used to emit correctly-typed filter literals. */
export type DimTypes = Record<FieldRef, string>;

export function dimTypes(dimensions: CatalogDimension[]): DimTypes {
  return Object.fromEntries(dimensions.map((dim) => [dim.ref, dim.type]));
}

/** Render a single filter value as a SQL literal, honoring the dimension's type so numeric and
 *  boolean dimensions aren't quoted (which type-strict backends reject). */
function filterLiteral(value: string, type?: string): string {
  if ((type === "numeric" || type === "number") && value.trim() !== "" && Number.isFinite(Number(value))) {
    return value;
  }
  if (type === "boolean") {
    const lower = value.toLowerCase();
    if (lower === "true" || lower === "false") return lower;
  }
  return sqlLiteral(value);
}

/**
 * Turn crossfilter selections into SQL filter expressions.
 * `excludeDim` drops a single dimension's own filter — used when ranking that very dimension's
 * leaderboard, so a leaderboard never filters itself out. `types` gives each dimension's semantic
 * type so values are quoted/cast correctly; NULL_TOKEN becomes `IS NULL`.
 */
export function filterExprs(filters: FilterState, opts: { types?: DimTypes; excludeDim?: FieldRef } = {}): string[] {
  const out: string[] = [];
  for (const [dimRef, values] of Object.entries(filters)) {
    if (!values.length || dimRef === opts.excludeDim) continue;
    const type = opts.types?.[dimRef];
    const hasNull = values.includes(NULL_TOKEN);
    const present = values.filter((value) => value !== NULL_TOKEN);
    const parts: string[] = [];
    if (present.length === 1) parts.push(`${dimRef} = ${filterLiteral(present[0], type)}`);
    else if (present.length > 1) parts.push(`${dimRef} IN (${present.map((v) => filterLiteral(v, type)).join(", ")})`);
    if (hasNull) parts.push(`${dimRef} IS NULL`);
    if (parts.length === 1) out.push(parts[0]);
    else if (parts.length > 1) out.push(`(${parts.join(" OR ")})`);
  }
  return out;
}

/**
 * Assemble the active filter expressions for a panel: crossfilter selections plus the time-range
 * bound. `excludeDim` drops one dimension's own selection (so a leaderboard never filters itself).
 */
export function composeFilters(
  filters: FilterState,
  opts: { timeRef?: FieldRef; range?: DateRange; excludeDim?: FieldRef; types?: DimTypes } = {},
): string[] {
  const base = filterExprs(filters, { types: opts.types, excludeDim: opts.excludeDim });
  if (opts.timeRef && opts.range) base.push(...timeFilters(opts.timeRef, opts.range));
  return base;
}

/** Aggregate totals for one or more metrics (no group-by). */
export function metricTotals(metrics: FieldRef[], filters: string[]): StructuredQuery {
  return { metrics, filters };
}

// Upper bound on buckets per grain — generous enough to cover any realistic range (so the series
// is never truncated to its oldest 500 buckets) while still bounding the points sent to the chart.
const GRAIN_BUCKET_CAP: Record<Grain, number> = {
  hour: 9600, // ~13 months hourly
  day: 4000, // ~11 years daily
  week: 800,
  month: 240,
  quarter: 120,
  year: 60,
};

/** A metric time series bucketed by `grain` on `timeRef`, ordered ascending. */
export function metricSeries(metrics: FieldRef[], timeRef: FieldRef, grain: Grain, filters: string[]): StructuredQuery {
  const timeDim = `${timeRef}__${grain}`;
  return { metrics, dimensions: [timeDim], filters, orderBy: [`${timeDim} ASC`], limit: GRAIN_BUCKET_CAP[grain] ?? 2000 };
}

/** Top-N values of one dimension ranked by a metric (a leaderboard panel). */
export function dimensionLeaderboard(
  metricRef: FieldRef,
  dimRef: FieldRef,
  filters: string[],
  limit = 6,
): StructuredQuery {
  return { metrics: [metricRef], dimensions: [dimRef], filters, orderBy: [`${metricRef} DESC`], limit };
}

/** Grouped pivot table: N dimensions x M metrics. */
export function pivotGroup(
  metrics: FieldRef[],
  dimensions: FieldRef[],
  filters: string[],
  orderBy?: string[],
  limit = 500,
): StructuredQuery {
  return { metrics, dimensions, filters, orderBy, limit };
}

/** Ungrouped raw rows for the inspect/preview view. */
export function previewRows(
  fields: { dimensions: FieldRef[]; metrics: FieldRef[] },
  filters: string[],
  limit = 50,
): StructuredQuery {
  return { dimensions: fields.dimensions, metrics: fields.metrics, filters, ungrouped: true, limit };
}
