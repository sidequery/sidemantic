import type { Tone } from "../lib/format";
import { Sparkline } from "./Sparkline";

type MetricCardProps = {
  metric: string; // ref, exposed as data-metric for verification + crossfilter
  label: string;
  valueText: string;
  delta?: { label: string; tone: Tone } | null;
  sparkValues?: number[];
  selected?: boolean;
  loading?: boolean;
  onSelect?: (metric: string) => void;
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
  valueText,
  delta,
  sparkValues = [],
  selected,
  loading,
  onSelect,
}: MetricCardProps) {
  return (
    <button
      type="button"
      data-metric={metric}
      data-selected={selected || undefined}
      onClick={() => onSelect?.(metric)}
      className="group flex w-full flex-col gap-1.5 border border-line bg-surface px-3 py-2.5 text-left transition hover:border-faint data-[selected=true]:border-accent data-[selected=true]:ring-1 data-[selected=true]:ring-accent"
    >
      <div className="flex items-baseline justify-between gap-2">
        <span className="truncate text-2xs font-semibold uppercase tracking-wide text-faint">{label}</span>
        {delta ? (
          <span data-tone={delta.tone} className={`shrink-0 text-2xs font-medium ${TONE_CLASS[delta.tone]}`}>
            <span aria-hidden="true" className="mr-0.5 text-[8px]">{TONE_ARROW[delta.tone]}</span>
            {delta.label}
          </span>
        ) : null}
      </div>
      <div className="font-mono tnum text-base font-semibold text-ink">
        {loading ? <span className="skeleton inline-block h-5 w-24 align-middle" /> : valueText}
      </div>
      <Sparkline values={sparkValues} />
    </button>
  );
}
