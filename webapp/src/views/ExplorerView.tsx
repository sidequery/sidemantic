import { useEffect, useMemo, useState } from "react";
import { aliasOf, type CatalogMetric, type Grain, type StructuredQuery } from "../data/types";
import { LeaderboardPanel } from "../components/LeaderboardPanel";
import { MetricCard } from "../components/MetricCard";
import { MetricTimeSeries } from "../components/MetricTimeSeries";
import { QueryDebugPanel } from "../components/QueryDebugPanel";
import { EmptyState, ErrorState } from "../components/States";
import type { BrushRange } from "../components/TimeSeriesChart";
import { formatDelta, formatValue } from "../lib/format";
import { graphMetricsForModel } from "../lib/catalog";
import { dashboardTabConfig } from "../lib/dashboard";
import { catalogDimTypes, composeFilters, metricSeries, metricTotals } from "../lib/queries";
import {
  bucketOffset,
  dateOnly,
  endOfBucket,
  formatBucketLabel,
  previousRange,
  previousYearRange,
  type DateRange,
} from "../lib/time";
import { useExplorer } from "../state/ExplorerContext";
import { useQueryResult } from "../state/useQueryResult";

function metricHint(metric?: CatalogMetric) {
  return { format: metric?.format, type: metric?.type };
}

export function resolveExpandedLeaderboard(
  expandedRef: string | null,
  dimensions: ReadonlyArray<{ ref: string }>,
): string | null {
  return expandedRef && dimensions.some((dimension) => dimension.ref === expandedRef) ? expandedRef : null;
}

export function chronologicalSeriesRows<T extends Record<string, unknown>>(rows: T[], timeAlias: string): T[] {
  return [...rows].sort((left, right) => String(left[timeAlias] ?? "").localeCompare(String(right[timeAlias] ?? "")));
}

export function brushDateRange(range: BrushRange, grain: Grain): DateRange {
  const fineGrain = grain === "second" || grain === "minute" || grain === "hour";
  return {
    from: fineGrain ? range.from.replace(" ", "T") : dateOnly(range.from),
    to: endOfBucket(range.to, grain),
  };
}

export function ExplorerView() {
  const { state, dispatch, catalog, backend, dashboard } = useExplorer();
  const [expandedLeaderboard, setExpandedLeaderboard] = useState<string | null>(null);
  const model = catalog.models.find((m) => m.name === state.model);
  const configured = useMemo(
    () => dashboardTabConfig(catalog, dashboard, state.dashboardTab),
    [catalog, dashboard, state.dashboardTab],
  );

  useEffect(() => {
    setExpandedLeaderboard(null);
  }, [state.dashboardTab, state.model]);

  const metrics = configured?.metrics ?? model?.metrics ?? [];
  const configuredMetricRefs = new Set(metrics.map((metric) => metric.ref));
  const graphMetrics = graphMetricsForModel(catalog, state.model).filter(
    (metric) => !configured || configuredMetricRefs.has(metric.ref),
  );
  const timeRef = configured?.timeDimension?.ref ?? model?.timeDimension?.ref;

  // Focused metric drives the chart + leaderboard ranking. It may be a graph-level metric that
  // isn't one of the model's strip metrics.
  const rankMetric =
    metrics.find((m) => m.ref === state.selectedMetric) ??
    graphMetrics.find((m) => m.ref === state.selectedMetric) ??
    metrics[0];
  const focusedInStrip = !!rankMetric && metrics.some((m) => m.ref === rankMetric.ref);
  // Make sure the focused metric is a column in the strip queries so the chart can reuse those
  // aggregates instead of issuing its own total/series queries.
  const stripMetricRefs = useMemo(() => {
    const refs = metrics.map((m) => m.ref);
    return rankMetric && !focusedInStrip ? [...refs, rankMetric.ref] : refs;
  }, [metrics, rankMetric, focusedInStrip]);

  const types = useMemo(() => catalogDimTypes(catalog), [catalog]);
  const baseFilters = useMemo(
    () => [...(configured?.filters ?? []), ...composeFilters(state.filters, { timeRef, range: state.dateRange, types })],
    [configured, state.filters, timeRef, state.dateRange, types],
  );
  // Resolve the chosen comparison mode into a concrete window. `off` (or no active date range, since
  // every comparison here is relative to one) means no comparison at all — the strip cards, chart
  // overlay, and leaderboard deltas all go dark together.
  const prevRange = useMemo<DateRange | null>(() => {
    if (state.comparison === "off" || !state.dateRange) return null;
    if (state.comparison === "year") return previousYearRange(state.dateRange);
    if (state.comparison === "custom") return state.comparisonRange ?? null;
    return previousRange(state.dateRange);
  }, [state.comparison, state.dateRange, state.comparisonRange]);
  const prevFilters = useMemo(
    () =>
      prevRange && timeRef
        ? [...(configured?.filters ?? []), ...composeFilters(state.filters, { timeRef, range: prevRange, types })]
        : null,
    [configured, state.filters, timeRef, prevRange, types],
  );

  // Stamp the selected timezone onto every query so the backend truncates time buckets in-zone
  // (UTC is elided at the wire boundary, matching the pre-E4 request shape).
  const tz = state.timezone;
  const withTz = (query: StructuredQuery | null): StructuredQuery | null =>
    query ? { ...query, timezone: tz } : null;

  // Strip queries — one aggregate per shape, covering every metric at once.
  const totals = useQueryResult(
    backend,
    stripMetricRefs.length
      ? withTz(metricTotals(stripMetricRefs, baseFilters, configured?.segments, configured?.usePreaggregations))
      : null,
  );
  const series = useQueryResult(
    backend,
    stripMetricRefs.length && timeRef
      ? withTz(
          metricSeries(
            stripMetricRefs,
            timeRef,
            state.grain,
            baseFilters,
            configured?.segments,
            configured?.usePreaggregations,
            Boolean(state.dateRange),
          ),
        )
      : null,
  );
  const comparison = useQueryResult(
    backend,
    stripMetricRefs.length && prevFilters
      ? withTz(metricTotals(stripMetricRefs, prevFilters, configured?.segments, configured?.usePreaggregations))
      : null,
  );
  // The single extra query the chart needs: the focused metric over the *previous* period (the
  // dashed overlay). Everything else is reused from the strip results above.
  const prevSeries = useQueryResult(
    backend,
    rankMetric && timeRef && prevFilters
      ? withTz(
          metricSeries(
            [rankMetric.ref],
            timeRef,
            state.grain,
            prevFilters,
            configured?.segments,
            configured?.usePreaggregations,
            true,
          ),
        )
      : null,
  );

  // Surface a failure from any strip query (totals/series/comparison/prev), not just totals, so a
  // backend error on the chart queries isn't silently shown as an empty chart.
  const queryError = totals.error ?? series.error ?? comparison.error ?? prevSeries.error;

  if (!model) return <div className="p-4"><EmptyState message="No model available in this semantic layer." /></div>;

  const leaderboardDims = (configured?.dimensions ?? model.dimensions).filter((dim) => dim.type !== "time");
  const activeExpandedLeaderboard = resolveExpandedLeaderboard(expandedLeaderboard, leaderboardDims);
  const comparisonLabel = state.comparison === "year" ? "Prev year" : state.comparison === "custom" ? "Comparison" : "Prev period";

  // A kept result from a previous model has different metric columns. Ignore it until the fresh one
  // lands (cards/chart keep showing their skeleton) rather than reading missing columns as zeros.
  const shapeAlias = metrics[0] ? aliasOf(metrics[0].ref) : rankMetric ? aliasOf(rankMetric.ref) : null;
  const fresh = (r?: { columns: string[] }) => !r || !shapeAlias || r.columns.includes(shapeAlias);
  const totalsRow = fresh(totals.result) ? totals.result?.rows[0] : undefined;
  const prevRow = fresh(comparison.result) ? comparison.result?.rows[0] : undefined;
  const rawSeriesRows = fresh(series.result) ? (series.result?.rows ?? []) : [];

  // Chart data derived from the strip aggregates (no duplicate total/series queries).
  const mAlias = rankMetric ? aliasOf(rankMetric.ref) : "";
  const tAlias = timeRef ? aliasOf(`${timeRef}__${state.grain}`) : "";
  const seriesRows = tAlias ? chronologicalSeriesRows(rawSeriesRows, tAlias) : rawSeriesRows;
  const chartTotal = totalsRow && mAlias ? Number(totalsRow[mAlias]) : NaN;
  const chartPrevTotal = prevRow && mAlias ? Number(prevRow[mAlias]) : undefined;
  const chartPoints = mAlias ? seriesRows.map((row) => ({ x: String(row[tAlias] ?? ""), y: Number(row[mAlias]) })) : [];
  // Align the previous-period series to the current buckets by position (bucketOffset), so a missing
  // bucket in either period doesn't shift the dashed overlay or hover delta onto the wrong bucket.
  const prevRows = tAlias ? chronologicalSeriesRows(prevSeries.result?.rows ?? [], tAlias) : [];
  const chartComparison =
    mAlias && chartPoints.length > 0 && prevRows.length > 0
      ? (() => {
          const prevFirst = String(prevRows[0][tAlias] ?? "");
          const curFirst = chartPoints[0].x;
          const prevByOffset = new Map<number, number>();
          for (const row of prevRows) {
            prevByOffset.set(bucketOffset(prevFirst, String(row[tAlias] ?? ""), state.grain), Number(row[mAlias]));
          }
          return chartPoints.map((point) => ({
            x: point.x,
            y: prevByOffset.get(bucketOffset(curFirst, point.x, state.grain)) ?? NaN,
          }));
        })()
      : [];

  function onBrush(range: BrushRange | null) {
    if (!range) dispatch({ type: "setDateRange", range: undefined });
    else dispatch({ type: "setDateRange", range: brushDateRange(range, state.grain) });
  }

  return (
    <div className="flex flex-col gap-4 p-4">
      {queryError ? <ErrorState message={queryError} /> : null}

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
          prevTotal={prevRange ? chartPrevTotal : undefined}
          hasTime={!!timeRef}
          loading={series.loading}
          activeRange={state.dateRange}
          comparisonLabel={comparisonLabel}
          formatLabel={(label) => formatBucketLabel(label, state.grain)}
          onBrush={onBrush}
        />
      ) : null}

      {/* Dimension leaderboards */}
      <div className="grid grid-cols-[repeat(auto-fit,minmax(220px,1fr))] gap-0 border-l border-t border-line">
        {rankMetric && leaderboardDims.length ? (
          leaderboardDims
            .filter((dim) => activeExpandedLeaderboard === null || activeExpandedLeaderboard === dim.ref)
            .map((dim) => (
              <LeaderboardPanel
                key={dim.ref}
                dim={dim}
                model={model}
                timeDimensionRef={timeRef}
                rankMetric={rankMetric}
                contextColumn={state.contextColumn}
                metricTotal={Number.isFinite(chartTotal) ? chartTotal : undefined}
                comparisonRange={prevRange ?? undefined}
                baseFilters={configured?.filters}
                baseSegments={configured?.segments}
                usePreaggregations={configured?.usePreaggregations}
                expanded={activeExpandedLeaderboard === dim.ref}
                onExpandedChange={(expanded) => setExpandedLeaderboard(expanded ? dim.ref : null)}
              />
            ))
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
