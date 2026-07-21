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
        <span className="truncate text-xs font-medium text-muted">{label}</span>
        {sparkHover?.label ? (
          <span className="shrink-0 font-mono text-2xs text-faint">{sparkHover.label}</span>
        ) : delta ? (
          <span data-tone={delta.tone} className={`shrink-0 text-2xs font-medium ${TONE_CLASS[delta.tone]}`}>
            <span aria-hidden="true" className="mr-0.5 text-[8px]">{TONE_ARROW[delta.tone]}</span>
            {delta.label}
          </span>
        ) : null}
      </div>
      <div className="font-mono tnum text-xl font-semibold tracking-[-0.025em] text-ink">
        {loading ? (
          <span className="skeleton inline-block h-5 w-24 align-middle" />
        ) : sparkHover ? (
          formatValue(sparkHover.value, format)
        ) : (
          valueText ?? formatValue(value, format)
        )}
      </div>
    </>
  );

  const className =
    "group flex min-h-36 w-full flex-col gap-2 rounded-xl bg-surface px-4 py-3.5 text-left shadow-sm transition-[transform,box-shadow] duration-150 ease-out hover:-translate-y-px hover:shadow-floating data-[selected=true]:ring-2 data-[selected=true]:ring-accent";
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
        className="-m-1 flex min-h-12 flex-col gap-1 rounded-lg border-0 bg-transparent p-1 text-left transition-colors hover:text-accent"
      >
        {summary}
      </button>
      {sparkline}
    </article>
  );
}
