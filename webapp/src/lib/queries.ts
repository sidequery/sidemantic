import type { FieldRef, Grain, StructuredQuery } from "../data/types";
import { sqlLiteral } from "./format";
import { timeFilters, type DateRange } from "./time";

// Crossfilter selections: dimension ref -> selected values. Values OR within a dimension,
// dimensions AND across each other (per the approved crossfilter rules).
export type FilterState = Record<FieldRef, string[]>;

/**
 * Turn crossfilter selections into SQL filter expressions.
 * `excludeDim` drops a single dimension's own filter — used when ranking that very dimension's
 * leaderboard, so a leaderboard never filters itself out.
 */
export function filterExprs(filters: FilterState, excludeDim?: FieldRef): string[] {
  const out: string[] = [];
  for (const [dimRef, values] of Object.entries(filters)) {
    if (!values.length || dimRef === excludeDim) continue;
    if (values.length === 1) out.push(`${dimRef} = ${sqlLiteral(values[0])}`);
    else out.push(`${dimRef} IN (${values.map(sqlLiteral).join(", ")})`);
  }
  return out;
}

/**
 * Assemble the active filter expressions for a panel: crossfilter selections plus the time-range
 * bound. `excludeDim` drops one dimension's own selection (so a leaderboard never filters itself).
 */
export function composeFilters(
  filters: FilterState,
  opts: { timeRef?: FieldRef; range?: DateRange; excludeDim?: FieldRef } = {},
): string[] {
  const base = filterExprs(filters, opts.excludeDim);
  if (opts.timeRef && opts.range) base.push(...timeFilters(opts.timeRef, opts.range));
  return base;
}

/** Aggregate totals for one or more metrics (no group-by). */
export function metricTotals(metrics: FieldRef[], filters: string[]): StructuredQuery {
  return { metrics, filters };
}

/** A metric time series bucketed by `grain` on `timeRef`, ordered ascending. */
export function metricSeries(metrics: FieldRef[], timeRef: FieldRef, grain: Grain, filters: string[]): StructuredQuery {
  const timeDim = `${timeRef}__${grain}`;
  return { metrics, dimensions: [timeDim], filters, orderBy: [`${timeDim} ASC`], limit: 500 };
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
