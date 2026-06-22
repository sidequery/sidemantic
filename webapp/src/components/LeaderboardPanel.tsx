import { useMemo } from "react";
import { aliasOf, NULL_TOKEN, type CatalogDimension, type CatalogMetric, type CatalogModel } from "../data/types";
import { formatCompact } from "../lib/format";
import { composeFilters, dimTypes, dimensionLeaderboard } from "../lib/queries";
import { useExplorer } from "../state/ExplorerContext";
import { useQueryResult } from "../state/useQueryResult";
import { Leaderboard, type LeaderboardRow } from "./Leaderboard";
import { ErrorState } from "./States";

/** A self-contained leaderboard: ranks one dimension by a metric, owning its own query so panels
 * load independently and crossfilter clicks toggle the dimension's filter. */
export function LeaderboardPanel({
  dim,
  model,
  rankMetric,
  limit = 6,
}: {
  dim: CatalogDimension;
  model: CatalogModel;
  rankMetric: CatalogMetric;
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

  const dimAlias = aliasOf(dim.ref);
  const metricAlias = aliasOf(rankMetric.ref);
  // While a slow reload is in flight, useQueryResult keeps the *previous* result visible. If the
  // ranking metric just changed, those kept rows carry the old metric's columns — reading the new
  // metric's column off them yields all-zeros. Treat that as still-loading and show the skeleton.
  const stale = !!result && result.rows.length > 0 && !result.columns.includes(metricAlias);
  const rows: LeaderboardRow[] =
    result && !stale
      ? result.rows.map((row) => {
          const raw = row[dimAlias];
          // Preserve NULL distinctly so the filter builder can emit `IS NULL`, not `= ''`.
          return { value: raw === null || raw === undefined ? NULL_TOKEN : String(raw), metric: Number(row[metricAlias] ?? 0) };
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
      formatMetric={(value) => formatCompact(value, { format: rankMetric.format, type: rankMetric.type })}
      onToggle={(value) => dispatch({ type: "toggleFilter", dim: dim.ref, value })}
    />
  );
}
