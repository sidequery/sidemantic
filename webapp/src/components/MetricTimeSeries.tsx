import type { CatalogMetric } from "../data/types";
import { formatCompact, formatDelta, formatValue } from "../lib/format";
import { TimeSeriesChart, type BrushRange, type SeriesPoint } from "./TimeSeriesChart";

const TONE = { positive: "text-success", negative: "text-danger", neutral: "text-faint" } as const;

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
  activeRange?: BrushRange;
  /** Short label for the comparison window ("Prev period" / "Prev year" / a custom range). */
  comparisonLabel?: string;
  /** Render a raw UTC bucket label into the selected timezone (axis ticks + tooltip). */
  formatLabel?: (label: string) => string;
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
  activeRange,
  comparisonLabel = "Prev period",
  formatLabel,
  onBrush,
}: MetricTimeSeriesProps) {
  const hint = { format: metric.format, type: metric.type };
  const delta = prevTotal !== undefined ? formatDelta(total, prevTotal) : null;
  const ariaSummary = `${metric.label} over time. Total ${formatValue(total, hint)}${
    delta ? `, ${delta.label} versus the ${comparisonLabel.toLowerCase()}` : ""
}.`;

  return (
    <section className="flex flex-col gap-2">
      <header className="flex flex-wrap items-baseline gap-x-3 gap-y-0.5 px-0.5">
        <h2 className="text-2xs font-semibold uppercase tracking-wide text-faint">{metric.label}</h2>
        <span className="font-mono tnum text-xl font-semibold text-ink">{formatValue(total, hint)}</span>
        {delta ? <span className={`text-xs ${TONE[delta.tone]}`}>{delta.label} vs {comparisonLabel.toLowerCase()}</span> : null}
        {!hasTime ? <span className="text-2xs text-faint">No time dimension</span> : null}
        {activeRange ? (
          <button
            type="button"
            onClick={() => onBrush(null)}
            className="ml-auto h-6 border border-line bg-surface px-2 text-2xs text-muted hover:border-faint hover:text-ink"
          >
            Reset zoom
          </button>
        ) : null}
      </header>
      {!hasTime ? null : loading && points.length < 2 ? (
        <div className="skeleton h-[280px] w-full" />
      ) : (
        <TimeSeriesChart
          points={points}
          comparison={comparisonPoints}
          formatValue={(value) => formatValue(value, hint)}
          formatAxis={(value) => formatCompact(value, hint)}
          formatLabel={formatLabel}
          comparisonLabel={comparisonLabel}
          ariaLabel={ariaSummary}
          onBrush={onBrush}
        />
      )}
    </section>
  );
}
