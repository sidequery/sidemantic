import { useMemo } from "react";
import { aliasOf, NULL_TOKEN, type CatalogDimension, type CatalogMetric, type CatalogModel } from "../data/types";
import { formatCompact, formatDeltaAbs, formatDeltaPct, formatPercentOfTotal, type Tone } from "../lib/format";
import { composeFilters, dimTypes, dimensionLeaderboard } from "../lib/queries";
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

/** A self-contained leaderboard: ranks one dimension by a metric, owning its own query so panels
 * load independently and crossfilter clicks toggle the dimension's filter. When a context column is
 * active it renders an extra compact figure per row (% of total, or the period-over-period delta). */
export function LeaderboardPanel({
  dim,
  model,
  rankMetric,
  contextColumn,
  metricTotal,
  comparisonRange,
  limit = 6,
}: {
  dim: CatalogDimension;
  model: CatalogModel;
  rankMetric: CatalogMetric;
  contextColumn: ContextColumn;
  /** Focused-metric ungrouped total under the current filters (threaded from the scorecard strip). */
  metricTotal?: number;
  /** Resolved comparison window for the delta columns; undefined when comparison is off. */
  comparisonRange?: DateRange;
  limit?: number;
}) {
  const { state, dispatch, backend } = useExplorer();
  const timeRef = model.timeDimension?.ref;
  const types = useMemo(() => dimTypes(model.dimensions), [model]);
  const filters = useMemo(
    // Exclude this dimension's own filter so its leaderboard keeps showing every value.
    () => composeFilters(state.filters, { timeRef, range: state.dateRange, excludeDim: dim.ref, types }),
    [state.filters, timeRef, state.dateRange, dim.ref, types],
  );
  const { result, loading, error } = useQueryResult(
    backend,
    dimensionLeaderboard(rankMetric.ref, dim.ref, filters, limit),
  );

  const wantsDelta = contextColumn === "delta" || contextColumn === "deltaPct";
  // A second ranked query over the comparison window, joined to the rows by dimension value below.
  // Only issued when a delta column is active AND there's a comparison window to compare against.
  const prevFilters = useMemo(
    () =>
      wantsDelta && comparisonRange && timeRef
        ? composeFilters(state.filters, { timeRef, range: comparisonRange, excludeDim: dim.ref, types })
        : null,
    [wantsDelta, comparisonRange, timeRef, state.filters, dim.ref, types],
  );
  const prev = useQueryResult(backend, prevFilters ? dimensionLeaderboard(rankMetric.ref, dim.ref, prevFilters, limit) : null);

  const dimAlias = aliasOf(dim.ref);
  const metricAlias = aliasOf(rankMetric.ref);
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
      selectedValues={state.filters[dim.ref]}
      formatMetric={(value) => formatCompact(value, hint)}
      onToggle={(value) => dispatch({ type: "toggleFilter", dim: dim.ref, value })}
      contextColumn={contextColumn}
      contextOptions={CONTEXT_OPTIONS}
      onContextColumn={(column) => dispatch({ type: "setContextColumn", column })}
    />
  );
}
