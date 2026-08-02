import { useMemo } from "react";
import { NULL_TOKEN, queryAlias, type CatalogDimension, type CatalogMetric, type CatalogModel } from "../data/types";
import { formatCompact, formatDeltaAbs, formatDeltaPct, formatPercentOfTotal, sqlLiteral, type Tone } from "../lib/format";
import { catalogDimTypes, composeFilters, dimensionLeaderboard } from "../lib/queries";
import type { DateRange } from "../lib/time";
import type { ContextColumn } from "../state/explorerState";
import { useExplorer } from "../state/ExplorerContext";
import { useQueryResult } from "../state/useQueryResult";
import { Leaderboard, type LeaderboardRow } from "./Leaderboard";
import { ErrorState } from "./States";

// The global context-column setting is one value in the reducer; the toggle lives in each panel
// header so it's reachable next to any leaderboard, and every panel reflects the shared choice.
const CONTEXT_OPTIONS: { key: ContextColumn; label: string; title: string }[] = [
  { key: "none", label: "—", title: "No context column" },
  { key: "pctTotal", label: "%", title: "Share of total" },
  { key: "delta", label: "Δ", title: "Absolute change vs comparison period" },
  { key: "deltaPct", label: "Δ%", title: "Percent change vs comparison period" },
];
const EMPTY_FILTERS: string[] = [];
const EMPTY_SEGMENTS: string[] = [];

/** A self-contained leaderboard: ranks one dimension by a metric, owning its own query so panels
 * load independently and crossfilter clicks toggle the dimension's filter. When a context column is
 * active it renders an extra compact figure per row (% of total, or the period-over-period delta). */
export function LeaderboardPanel({
  dim,
  model,
  timeDimensionRef,
  rankMetric,
  contextColumn,
  metricTotal,
  comparisonRange,
  baseFilters = EMPTY_FILTERS,
  baseSegments = EMPTY_SEGMENTS,
  usePreaggregations,
  limit = 6,
  expanded = false,
  onExpandedChange,
}: {
  dim: CatalogDimension;
  model: CatalogModel;
  timeDimensionRef?: string;
  rankMetric: CatalogMetric;
  contextColumn: ContextColumn;
  /** Focused-metric ungrouped total under the current filters (threaded from the scorecard strip). */
  metricTotal?: number;
  /** Resolved comparison window for the delta columns; undefined when comparison is off. */
  comparisonRange?: DateRange;
  /** Filters declared by a dashboard spec; always applied in addition to interactive filters. */
  baseFilters?: string[];
  /** Segments declared by a dashboard spec; always applied to current and comparison queries. */
  baseSegments?: string[];
  /** Per-dashboard override for materialized pre-aggregation routing. */
  usePreaggregations?: boolean;
  limit?: number;
  expanded?: boolean;
  onExpandedChange?: (expanded: boolean) => void;
}) {
  const { state, dispatch, backend, catalog } = useExplorer();
  const timeRef = timeDimensionRef ?? model.timeDimension?.ref;
  const types = useMemo(() => catalogDimTypes(catalog), [catalog]);
  const filters = useMemo(
    // Exclude this dimension's own filter so its leaderboard keeps showing every value.
    () => [...baseFilters, ...composeFilters(state.filters, { timeRef, range: state.dateRange, excludeDim: dim.ref, types })],
    [baseFilters, state.filters, timeRef, state.dateRange, dim.ref, types],
  );
  const { result, loading, error } = useQueryResult(
    backend,
    dimensionLeaderboard(rankMetric.ref, dim.ref, filters, limit, baseSegments, usePreaggregations),
  );

  // Only an include-mode selection is a "checked" row; exclude/contains filters don't highlight
  // rows (their meaning is inversion/substring, not row selection).
  const own = state.filters[dim.ref];
  const selectedValues = own?.mode === "include" ? own.values : undefined;

  const queryFields = [rankMetric.ref, dim.ref];
  const dimAlias = queryAlias(dim.ref, queryFields);
  const wantsDelta = contextColumn === "delta" || contextColumn === "deltaPct";
  // Constrain the comparison-period query to EXACTLY the current leaderboard's dimension values.
  // Ranking the prior period independently (top N) would miss any current row that wasn't also in
  // the prior top N, rendering a spurious em dash instead of its real prior value.
  const currentValueConstraint = useMemo(() => {
    if (!result || !result.columns.includes(dimAlias)) return null;
    const nonNull: string[] = [];
    let hasNull = false;
    for (const row of result.rows) {
      const raw = row[dimAlias];
      if (raw === null || raw === undefined) hasNull = true;
      else nonNull.push(String(raw));
    }
    const parts: string[] = [];
    // Cast to text so the match works on numeric/boolean dimensions too.
    if (nonNull.length) parts.push(`CAST(${dim.ref} AS VARCHAR) IN (${nonNull.map(sqlLiteral).join(", ")})`);
    if (hasNull) parts.push(`${dim.ref} IS NULL`);
    if (!parts.length) return null;
    return parts.length === 1 ? parts[0] : `(${parts.join(" OR ")})`;
  }, [result, dimAlias, dim.ref]);

  // A second query over the comparison window, joined to the current rows by dimension value below.
  // Only issued when a delta column is active, there's a comparison window, and we know the current
  // value set to constrain to.
  const prevFilters = useMemo(
    () =>
      wantsDelta && comparisonRange && timeRef && currentValueConstraint
        ? [
            ...baseFilters,
            ...composeFilters(state.filters, { timeRef, range: comparisonRange, excludeDim: dim.ref, types }),
            currentValueConstraint,
          ]
        : null,
    [wantsDelta, comparisonRange, timeRef, state.filters, dim.ref, types, currentValueConstraint, baseFilters],
  );
  const prev = useQueryResult(
    backend,
    prevFilters
      ? dimensionLeaderboard(rankMetric.ref, dim.ref, prevFilters, limit, baseSegments, usePreaggregations)
      : null,
  );
  const metricAlias = queryAlias(rankMetric.ref, queryFields);
  // While a slow reload is in flight, useQueryResult keeps the *previous* result visible. If the
  // ranking metric just changed, those kept rows carry the old metric's columns — reading the new
  // metric's column off them yields all-zeros. Treat that as still-loading and show the skeleton.
  const stale = !!result && result.rows.length > 0 && !result.columns.includes(metricAlias);

  // Previous-window metric keyed by dimension value, for the delta columns. A missing value here
  // means the row had no data last period → the formatters render an em dash, never a fabricated 0.
  const prevByValue = useMemo(() => {
    const map = new Map<string, number>();
    if (!prev.result || !prev.result.columns.includes(metricAlias)) return map;
    for (const row of prev.result.rows) {
      const raw = row[dimAlias];
      const key = raw === null || raw === undefined ? NULL_TOKEN : String(raw);
      map.set(key, Number(row[metricAlias]));
    }
    return map;
  }, [prev.result, dimAlias, metricAlias]);

  const hint = { format: rankMetric.format, type: rankMetric.type };
  function contextFor(value: string, metric: number): { label: string; tone: Tone } | undefined {
    switch (contextColumn) {
      case "pctTotal":
        return metricTotal === undefined ? undefined : formatPercentOfTotal(metric, metricTotal);
      case "delta":
        return formatDeltaAbs(metric, prevByValue.get(value), hint);
      case "deltaPct":
        return formatDeltaPct(metric, prevByValue.get(value));
      default:
        return undefined;
    }
  }

  const rows: LeaderboardRow[] =
    result && !stale
      ? result.rows.map((row) => {
          const raw = row[dimAlias];
          // Preserve NULL distinctly so the filter builder can emit `IS NULL`, not `= ''`.
          const value = raw === null || raw === undefined ? NULL_TOKEN : String(raw);
          const metric = Number(row[metricAlias] ?? 0);
          return { value, metric, context: contextFor(value, metric) };
        })
      : [];

  if (error) return <ErrorState title={dim.label} message={error} />;

  return (
    <Leaderboard
      dimension={dim.ref}
      title={dim.label}
      metricLabel={rankMetric.label}
      rows={rows}
      loading={loading || stale}
      selectedValues={selectedValues}
      formatMetric={(value) => formatCompact(value, hint)}
      onToggle={(value) => dispatch({ type: "toggleFilter", dim: dim.ref, value })}
      contextColumn={contextColumn}
      contextOptions={CONTEXT_OPTIONS}
      onContextColumn={(column) => dispatch({ type: "setContextColumn", column })}
      collapsedLimit={limit}
      expanded={expanded}
      onExpandedChange={onExpandedChange}
    />
  );
}
