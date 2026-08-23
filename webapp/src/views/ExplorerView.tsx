import { useEffect, useMemo, useState } from "react";
import { queryAlias, type CatalogMetric, type Grain, type StructuredQuery } from "../data/types";
import { DataTable, type Column } from "../components/DataTable";
import { LeaderboardPanel } from "../components/LeaderboardPanel";
import { MetricCard } from "../components/MetricCard";
import { MetricTimeSeries } from "../components/MetricTimeSeries";
import { QueryDebugPanel } from "../components/QueryDebugPanel";
import { EmptyState, ErrorState } from "../components/States";
import type { BrushRange } from "../components/TimeSeriesChart";
import { formatDelta, formatValue, labelize } from "../lib/format";
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

const SINGLE_METRIC_QUERY_TYPES = new Set(["cohort", "conversion", "retention"]);

/** These metric types compile to dedicated result shapes and cannot share one query with other metrics. */
export function canBatchMetric(metric: Pick<CatalogMetric, "type">): boolean {
  return !metric.type || !SINGLE_METRIC_QUERY_TYPES.has(metric.type);
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
  const stripMetrics = metrics.filter(canBatchMetric);
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
  const singleQueryMetric = rankMetric && !canBatchMetric(rankMetric) ? rankMetric : undefined;
  const focusedInStrip = !!rankMetric && stripMetrics.some((m) => m.ref === rankMetric.ref);
  // Make sure the focused metric is a column in the strip queries so the chart can reuse those
  // aggregates instead of issuing its own total/series queries.
  const stripMetricRefs = useMemo(() => {
    const refs = stripMetrics.map((m) => m.ref);
    return rankMetric && canBatchMetric(rankMetric) && !focusedInStrip ? [...refs, rankMetric.ref] : refs;
  }, [stripMetrics, rankMetric, focusedInStrip]);

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
    !singleQueryMetric && stripMetricRefs.length
      ? withTz(metricTotals(stripMetricRefs, baseFilters, configured?.segments, configured?.usePreaggregations))
      : null,
  );
  const series = useQueryResult(
    backend,
    !singleQueryMetric && stripMetricRefs.length && timeRef
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
    !singleQueryMetric && stripMetricRefs.length && prevFilters
      ? withTz(metricTotals(stripMetricRefs, prevFilters, configured?.segments, configured?.usePreaggregations))
      : null,
  );
  // Cohort, conversion, and retention metrics compile to dedicated result shapes. Query the
  // focused one alone and render its native rows instead of poisoning the aggregate strip query.
  const singleMetricResult = useQueryResult(
    backend,
    singleQueryMetric
      ? withTz({
          ...metricTotals(
            [singleQueryMetric.ref],
            baseFilters,
            configured?.segments,
            configured?.usePreaggregations,
          ),
          limit: 500,
        })
      : null,
  );
  // The single extra query the chart needs: the focused metric over the *previous* period (the
  // dashed overlay). Everything else is reused from the strip results above.
  const prevSeries = useQueryResult(
    backend,
    !singleQueryMetric && rankMetric && timeRef && prevFilters
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
  const queryError = totals.error ?? series.error ?? comparison.error ?? prevSeries.error ?? singleMetricResult.error;

  if (!model) return <div className="p-4"><EmptyState message="No model available in this semantic layer." /></div>;

  const leaderboardDims = (configured?.dimensions ?? model.dimensions).filter((dim) => dim.type !== "time");
  const activeExpandedLeaderboard = resolveExpandedLeaderboard(expandedLeaderboard, leaderboardDims);
  const comparisonLabel = state.comparison === "year" ? "Prev year" : state.comparison === "custom" ? "Comparison" : "Prev period";

  // A kept result from a previous model has different metric columns. Ignore it until the fresh one
  // lands (cards/chart keep showing their skeleton) rather than reading missing columns as zeros.
  const seriesTimeRef = timeRef ? `${timeRef}__${state.grain}` : "";
  const seriesFields = seriesTimeRef ? [...stripMetricRefs, seriesTimeRef] : stripMetricRefs;
  const prevSeriesFields = rankMetric && seriesTimeRef ? [rankMetric.ref, seriesTimeRef] : [];
  const shapeAlias = stripMetrics[0]
    ? queryAlias(stripMetrics[0].ref, stripMetricRefs)
    : rankMetric
      ? queryAlias(rankMetric.ref, stripMetricRefs)
      : null;
  const fresh = (r?: { columns: string[] }) => !r || !shapeAlias || r.columns.includes(shapeAlias);
  const totalsRow = fresh(totals.result) ? totals.result?.rows[0] : undefined;
  const prevRow = fresh(comparison.result) ? comparison.result?.rows[0] : undefined;
  const rawSeriesRows = fresh(series.result) ? (series.result?.rows ?? []) : [];

  // Chart data derived from the strip aggregates (no duplicate total/series queries).
  const mAlias = rankMetric ? queryAlias(rankMetric.ref, stripMetricRefs) : "";
  const seriesMetricAlias = rankMetric ? queryAlias(rankMetric.ref, seriesFields) : "";
  const prevSeriesMetricAlias = rankMetric ? queryAlias(rankMetric.ref, prevSeriesFields) : "";
  const tAlias = seriesTimeRef ? queryAlias(seriesTimeRef, seriesFields) : "";
  const prevTimeAlias = seriesTimeRef ? queryAlias(seriesTimeRef, prevSeriesFields) : "";
  const seriesRows = tAlias ? chronologicalSeriesRows(rawSeriesRows, tAlias) : rawSeriesRows;
  const chartTotal = totalsRow && mAlias ? Number(totalsRow[mAlias]) : NaN;
  const chartPrevTotal = prevRow && mAlias ? Number(prevRow[mAlias]) : undefined;
  const chartPoints = seriesMetricAlias
    ? seriesRows.map((row) => ({ x: String(row[tAlias] ?? ""), y: Number(row[seriesMetricAlias]) }))
    : [];
  // Align the previous-period series to the current buckets by position (bucketOffset), so a missing
  // bucket in either period doesn't shift the dashed overlay or hover delta onto the wrong bucket.
  const prevRows = prevTimeAlias ? chronologicalSeriesRows(prevSeries.result?.rows ?? [], prevTimeAlias) : [];
  const chartComparison =
    prevSeriesMetricAlias && chartPoints.length > 0 && prevRows.length > 0
      ? (() => {
          const prevFirst = String(prevRows[0][prevTimeAlias] ?? "");
          const curFirst = chartPoints[0].x;
          const prevByOffset = new Map<number, number>();
          for (const row of prevRows) {
            prevByOffset.set(
              bucketOffset(prevFirst, String(row[prevTimeAlias] ?? ""), state.grain),
              Number(row[prevSeriesMetricAlias]),
            );
          }
          return chartPoints.map((point) => ({
            x: point.x,
            y: prevByOffset.get(bucketOffset(curFirst, point.x, state.grain)) ?? NaN,
          }));
        })()
      : [];

  const singleMetricColumns: Column[] = (singleMetricResult.result?.columns ?? []).map((column) => ({
    key: column,
    label: labelize(column),
    numeric: (singleMetricResult.result?.rows ?? []).some((row) => typeof row[column] === "number"),
  }));

  function onBrush(range: BrushRange | null) {
    if (!range) dispatch({ type: "setDateRange", range: undefined });
    else dispatch({ type: "setDateRange", range: brushDateRange(range, state.grain) });
  }

  return (
    <div className="flex flex-col gap-4 p-4">
      {queryError ? <ErrorState message={queryError} /> : null}

      {/* KPI scorecard strip. Dedicated-shape metrics own this surface while selected. */}
      {!singleQueryMetric ? (
        <div className="grid grid-cols-2 gap-2 sm:grid-cols-3 lg:grid-cols-4 xl:grid-cols-6" data-testid="metric-totals">
          {stripMetrics.length === 0 ? (
            <div className="col-span-full"><EmptyState message="This model has no metrics." /></div>
          ) : (
            stripMetrics.map((metric) => {
              const totalsAlias = queryAlias(metric.ref, stripMetricRefs);
              const seriesAlias = queryAlias(metric.ref, seriesFields);
              const value = totalsRow ? Number(totalsRow[totalsAlias]) : NaN;
              const prev = prevRow ? Number(prevRow[totalsAlias]) : undefined;
              const sparkValues = seriesRows.map((row) => Number(row[seriesAlias])).filter(Number.isFinite);
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
      ) : null}

      {/* Time series for the focused metric — fed from the strip queries + one prev-period query */}
      {singleQueryMetric ? (
        <section className="flex flex-col gap-2" data-testid="single-metric-result">
          <div>
            <h2 className="text-sm font-semibold text-ink">{singleQueryMetric.label}</h2>
            {singleQueryMetric.description ? (
              <p className="mt-0.5 text-xs text-muted">{singleQueryMetric.description}</p>
            ) : null}
          </div>
          <DataTable
            columns={singleMetricColumns}
            rows={singleMetricResult.result?.rows ?? []}
            loading={singleMetricResult.loading}
            searchable
            renderCell={(_column, value) => value === null || value === undefined || value === "" ? "—" : String(value)}
          />
        </section>
      ) : rankMetric ? (
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
      {!singleQueryMetric ? (
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
      ) : null}

      <QueryDebugPanel
        queries={{
          Totals: totals.result?.sql,
          Series: series.result?.sql,
          Comparison: comparison.result?.sql,
          "Prev series": prevSeries.result?.sql,
          "Focused metric": singleMetricResult.result?.sql,
        }}
      />
    </div>
  );
}
