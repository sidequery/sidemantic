import { useMemo } from "react";
import { aliasOf, type CatalogMetric } from "../data/types";
import { LeaderboardPanel } from "../components/LeaderboardPanel";
import { MetricCard } from "../components/MetricCard";
import { MetricTimeSeries } from "../components/MetricTimeSeries";
import { QueryDebugPanel } from "../components/QueryDebugPanel";
import { EmptyState, ErrorState } from "../components/States";
import type { BrushRange } from "../components/TimeSeriesChart";
import { formatDelta, formatValue } from "../lib/format";
import { composeFilters, dimTypes, metricSeries, metricTotals } from "../lib/queries";
import { endOfBucket, previousRange } from "../lib/time";
import { useExplorer } from "../state/ExplorerContext";
import { useQueryResult } from "../state/useQueryResult";

function metricHint(metric?: CatalogMetric) {
  return { format: metric?.format, type: metric?.type };
}

export function ExplorerView() {
  const { state, dispatch, catalog, backend } = useExplorer();
  const model = catalog.models.find((m) => m.name === state.model);

  const metrics = model?.metrics ?? [];
  const timeRef = model?.timeDimension?.ref;

  // Focused metric drives the chart + leaderboard ranking. It may be a graph-level metric that
  // isn't one of the model's strip metrics.
  const rankMetric =
    metrics.find((m) => m.ref === state.selectedMetric) ??
    catalog.graphMetrics.find((m) => m.ref === state.selectedMetric) ??
    metrics[0];
  const focusedInStrip = !!rankMetric && metrics.some((m) => m.ref === rankMetric.ref);
  // Make sure the focused metric is a column in the strip queries so the chart can reuse those
  // aggregates instead of issuing its own total/series queries.
  const stripMetricRefs = useMemo(() => {
    const refs = metrics.map((m) => m.ref);
    return rankMetric && !focusedInStrip ? [...refs, rankMetric.ref] : refs;
  }, [metrics, rankMetric, focusedInStrip]);

  const types = useMemo(() => dimTypes(model?.dimensions ?? []), [model]);
  const baseFilters = useMemo(
    () => composeFilters(state.filters, { timeRef, range: state.dateRange, types }),
    [state.filters, timeRef, state.dateRange, types],
  );
  const prevRange = state.dateRange ? previousRange(state.dateRange) : null;
  const prevFilters = useMemo(
    () => (prevRange && timeRef ? composeFilters(state.filters, { timeRef, range: prevRange, types }) : null),
    [state.filters, timeRef, prevRange, types],
  );

  // Strip queries — one aggregate per shape, covering every metric at once.
  const totals = useQueryResult(backend, stripMetricRefs.length ? metricTotals(stripMetricRefs, baseFilters) : null);
  const series = useQueryResult(
    backend,
    stripMetricRefs.length && timeRef ? metricSeries(stripMetricRefs, timeRef, state.grain, baseFilters) : null,
  );
  const comparison = useQueryResult(backend, stripMetricRefs.length && prevFilters ? metricTotals(stripMetricRefs, prevFilters) : null);
  // The single extra query the chart needs: the focused metric over the *previous* period (the
  // dashed overlay). Everything else is reused from the strip results above.
  const prevSeries = useQueryResult(
    backend,
    rankMetric && timeRef && prevFilters ? metricSeries([rankMetric.ref], timeRef, state.grain, prevFilters) : null,
  );

  if (!model) return <div className="p-4"><EmptyState message="No model available in this semantic layer." /></div>;

  const leaderboardDims = model.dimensions.filter((dim) => dim.type !== "time");

  // A kept result from a previous model has different metric columns. Ignore it until the fresh one
  // lands (cards/chart keep showing their skeleton) rather than reading missing columns as zeros.
  const shapeAlias = metrics[0] ? aliasOf(metrics[0].ref) : rankMetric ? aliasOf(rankMetric.ref) : null;
  const fresh = (r?: { columns: string[] }) => !r || !shapeAlias || r.columns.includes(shapeAlias);
  const totalsRow = fresh(totals.result) ? totals.result?.rows[0] : undefined;
  const prevRow = fresh(comparison.result) ? comparison.result?.rows[0] : undefined;
  const seriesRows = fresh(series.result) ? (series.result?.rows ?? []) : [];

  // Chart data derived from the strip aggregates (no duplicate total/series queries).
  const mAlias = rankMetric ? aliasOf(rankMetric.ref) : "";
  const tAlias = timeRef ? aliasOf(`${timeRef}__${state.grain}`) : "";
  const chartTotal = totalsRow && mAlias ? Number(totalsRow[mAlias]) : NaN;
  const chartPrevTotal = prevRow && mAlias ? Number(prevRow[mAlias]) : undefined;
  const chartPoints = mAlias ? seriesRows.map((row) => ({ x: String(row[tAlias] ?? ""), y: Number(row[mAlias]) })) : [];
  const chartComparison = mAlias
    ? (prevSeries.result?.rows ?? []).map((row) => ({ x: String(row[tAlias] ?? ""), y: Number(row[mAlias]) }))
    : [];

  function onBrush(range: BrushRange | null) {
    if (!range) dispatch({ type: "setDateRange", range: undefined });
    else dispatch({ type: "setDateRange", range: { from: range.from, to: endOfBucket(range.to, state.grain) } });
  }

  return (
    <div className="flex flex-col gap-4 p-4">
      {totals.error ? <ErrorState message={totals.error} /> : null}

      {/* KPI scorecard strip */}
      <div className="grid grid-cols-2 gap-2 sm:grid-cols-3 lg:grid-cols-4 xl:grid-cols-6" data-testid="metric-totals">
        {metrics.length === 0 ? (
          <div className="col-span-full"><EmptyState message="This model has no metrics." /></div>
        ) : (
          metrics.map((metric) => {
            const alias = aliasOf(metric.ref);
            const value = totalsRow ? Number(totalsRow[alias]) : NaN;
            const prev = prevRow ? Number(prevRow[alias]) : undefined;
            const sparkValues = seriesRows.map((row) => Number(row[alias])).filter(Number.isFinite);
            return (
              <MetricCard
                key={metric.ref}
                metric={metric.ref}
                label={metric.label}
                valueText={formatValue(value, metricHint(metric))}
                delta={prev !== undefined ? formatDelta(value, prev) : null}
                sparkValues={sparkValues}
                selected={state.selectedMetric === metric.ref}
                loading={totals.loading && !totalsRow}
                onSelect={(ref) => dispatch({ type: "setMetric", metric: ref })}
              />
            );
          })
        )}
      </div>

      {/* Time series for the focused metric — fed from the strip queries + one prev-period query */}
      {rankMetric ? (
        <MetricTimeSeries
          metric={rankMetric}
          points={chartPoints}
          comparisonPoints={prevRange ? chartComparison : undefined}
          total={chartTotal}
          prevTotal={chartPrevTotal}
          hasTime={!!timeRef}
          loading={series.loading}
          onBrush={onBrush}
        />
      ) : null}

      {/* Dimension leaderboards */}
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-3">
        {rankMetric && leaderboardDims.length ? (
          leaderboardDims.map((dim) => <LeaderboardPanel key={dim.ref} dim={dim} model={model} rankMetric={rankMetric} />)
        ) : (
          <EmptyState message="No categorical dimensions to break down." />
        )}
      </div>

      <QueryDebugPanel
        queries={{
          Totals: totals.result?.sql,
          Series: series.result?.sql,
          Comparison: comparison.result?.sql,
          "Prev series": prevSeries.result?.sql,
        }}
      />
    </div>
  );
}
