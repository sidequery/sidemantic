import { useState } from "react";
import { formatValue, type FormatHint, type Tone } from "../lib/format";
import { Sparkline, type SparklineBrushRange } from "./Sparkline";

type MetricCardProps = {
  metric: string; // ref, exposed as data-metric for verification + crossfilter
  label: string;
  value?: unknown;
  valueText?: string;
  format?: FormatHint;
  delta?: { label: string; tone: Tone } | null;
  /** Caption naming what the delta compares against, e.g. "vs previous month". */
  comparison?: string;
  /** 0..1 renders a thin progress/attainment bar under the value. */
  progress?: number;
  sparkValues?: number[];
  sparkLabels?: string[];
  selected?: boolean;
  loading?: boolean;
  onSelect?: (metric: string) => void;
  onSparkHover?: (point: { index: number; label?: string; value: number } | null) => void;
  onSparkBrush?: (range: SparklineBrushRange | null) => void;
};

const TONE_CLASS: Record<Tone, string> = {
  positive: "text-accent",
  negative: "text-danger",
  neutral: "text-faint",
};

const TONE_ARROW: Record<Tone, string> = { positive: "▲", negative: "▼", neutral: "·" };

export function MetricCard({
  metric,
  label,
  value,
  valueText,
  format,
  delta,
  comparison,
  progress,
  sparkValues = [],
  sparkLabels,
  selected,
  loading,
  onSelect,
  onSparkHover,
  onSparkBrush,
}: MetricCardProps) {
  const [sparkHover, setSparkHover] = useState<{ index: number; label?: string; value: number } | null>(null);
  const summary = (
    <>
      <div className="flex items-baseline justify-between gap-2">
        <span className="truncate text-2xs font-semibold uppercase tracking-wide text-faint">{label}</span>
        {sparkHover?.label ? (
          <span className="shrink-0 font-mono text-2xs text-faint">{sparkHover.label}</span>
        ) : delta ? (
          <span data-tone={delta.tone} className={`shrink-0 text-2xs font-medium ${TONE_CLASS[delta.tone]}`}>
            <span aria-hidden="true" className="mr-0.5 text-[8px]">{TONE_ARROW[delta.tone]}</span>
            {delta.label}
          </span>
        ) : null}
      </div>
      <div className="font-mono tnum text-base font-semibold text-ink">
        {loading ? (
          <span className="skeleton inline-block h-5 w-24 align-middle" />
        ) : sparkHover ? (
          formatValue(sparkHover.value, format)
        ) : (
          valueText ?? formatValue(value, format)
        )}
      </div>
      {comparison ? <div className="text-2xs text-faint">{comparison}</div> : null}
      {progress != null && Number.isFinite(progress) ? (
        <div
          role="progressbar"
          aria-valuemin={0}
          aria-valuemax={1}
          aria-valuenow={Math.min(Math.max(progress, 0), 1)}
          className="h-1 w-full bg-surface-soft"
        >
          <div className="h-full bg-chart-primary" style={{ width: `${Math.min(Math.max(progress, 0), 1) * 100}%` }} />
        </div>
      ) : null}
    </>
  );

  const className =
    "group flex w-full flex-col gap-1.5 border border-line bg-surface px-3 py-2.5 text-left data-[selected=true]:border-accent data-[selected=true]:ring-1 data-[selected=true]:ring-accent";
  const sparkline = (
    <Sparkline
      values={sparkValues}
      labels={sparkLabels}
      onHover={(point) => {
        setSparkHover(point);
        onSparkHover?.(point);
      }}
      onBrush={onSparkBrush}
      formatValue={(sparkValue) => formatValue(sparkValue, format)}
    />
  );

  if (!onSelect) {
    return (
      <article data-metric={metric} data-selected={selected || undefined} className={className}>
        {summary}
        {sparkline}
      </article>
    );
  }

  return (
    <article data-metric={metric} data-selected={selected || undefined} className={className}>
      <button
        type="button"
        data-metric={metric}
        aria-pressed={!!selected}
        onClick={() => onSelect(metric)}
        className="-m-1 flex flex-col gap-1 border-0 bg-transparent p-1 text-left transition hover:opacity-75"
      >
        {summary}
      </button>
      {sparkline}
    </article>
  );
}
