import type { CatalogMetric } from "../data/types";
import { formatCompact, formatDelta, formatValue } from "../lib/format";
import { TimeSeriesChart, type BrushRange, type SeriesPoint } from "./TimeSeriesChart";

const TONE = { positive: "text-accent", negative: "text-danger", neutral: "text-faint" } as const;

type MetricTimeSeriesProps = {
  metric: CatalogMetric;
  /** Current-period series, derived from the all-metrics strip query (no extra round trip). */
  points: SeriesPoint[];
  /** Previous-period series (the dashed overlay) — the one query the chart actually adds. */
  comparisonPoints?: SeriesPoint[];
  total: number;
  prevTotal?: number;
  hasTime: boolean;
  loading?: boolean;
  onBrush: (range: BrushRange | null) => void;
};

/** Header (label + total + period-over-period delta) plus the interactive time series for the
 *  focused metric. Presentational: all data is fed by the parent, which reuses the KPI-strip
 *  queries instead of re-issuing the same aggregates. */
export function MetricTimeSeries({
  metric,
  points,
  comparisonPoints,
  total,
  prevTotal,
  hasTime,
  loading,
  onBrush,
}: MetricTimeSeriesProps) {
  const hint = { format: metric.format, type: metric.type };
  const delta = prevTotal !== undefined ? formatDelta(total, prevTotal) : null;

  return (
    <section className="flex flex-col gap-2">
      <header className="flex flex-wrap items-baseline gap-x-3 gap-y-0.5 px-0.5">
        <h2 className="text-2xs font-semibold uppercase tracking-wide text-faint">{metric.label}</h2>
        <span className="font-mono tnum text-xl font-semibold text-ink">{formatValue(total, hint)}</span>
        {delta ? <span className={`text-xs ${TONE[delta.tone]}`}>{delta.label} vs prev</span> : null}
        {!hasTime ? <span className="text-2xs text-faint">No time dimension</span> : null}
      </header>
      {!hasTime ? null : loading && points.length < 2 ? (
        <div className="skeleton h-[280px] w-full" />
      ) : (
        <TimeSeriesChart
          points={points}
          comparison={comparisonPoints}
          formatValue={(value) => formatValue(value, hint)}
          formatAxis={(value) => formatCompact(value, hint)}
          comparisonLabel="Prev period"
          onBrush={onBrush}
        />
      )}
    </section>
  );
}
