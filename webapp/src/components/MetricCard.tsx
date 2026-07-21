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
  /** Caption naming what the delta compares against, e.g. "vs previous month". Rendered inline
   *  after the delta as one quiet line — no bars, no pills. */
  comparison?: string;
  sparkValues?: number[];
  sparkLabels?: string[];
  selected?: boolean;
  loading?: boolean;
  onSelect?: (metric: string) => void;
  onSparkHover?: (point: { index: number; label?: string; value: number } | null) => void;
  onSparkBrush?: (range: SparklineBrushRange | null) => void;
};

const TONE_CLASS: Record<Tone, string> = {
  positive: "text-success",
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
        <span className="truncate text-2xs font-medium uppercase tracking-[0.08em] text-faint">{label}</span>
        {sparkHover?.label ? <span className="shrink-0 font-mono text-2xs text-faint">{sparkHover.label}</span> : null}
      </div>
      <div className="font-mono tnum text-[19px] font-semibold leading-tight tracking-tight text-ink">
        {loading ? (
          <span className="skeleton inline-block h-6 w-24 align-middle" />
        ) : sparkHover ? (
          formatValue(sparkHover.value, format)
        ) : (
          valueText ?? formatValue(value, format)
        )}
      </div>
      {delta || comparison ? (
        <div className="flex items-baseline gap-1 text-2xs">
          {delta ? (
            <span data-tone={delta.tone} className={`font-mono tnum font-medium ${TONE_CLASS[delta.tone]}`}>
              <span aria-hidden="true" className="mr-0.5 text-[8px]">{TONE_ARROW[delta.tone]}</span>
              {delta.label}
            </span>
          ) : null}
          {comparison ? <span className="truncate text-faint">{comparison}</span> : null}
        </div>
      ) : null}
    </>
  );

  const className =
    "group flex w-full flex-col gap-1.5 rounded-lg border border-line bg-surface px-3.5 py-3 text-left shadow-[var(--shadow-sm)] transition-colors hover:border-line-strong data-[selected=true]:border-accent";
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
