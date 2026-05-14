import { formatValue, type MetricTone } from "./types";

type MetricCardProps = {
  metric: string;
  label: string;
  value: unknown;
  delta?: { label: string; tone?: MetricTone };
  selected?: boolean;
  loading?: boolean;
  onSelect?: (metric: string) => void;
};

export function MetricCard({ metric, label, value, delta, selected, loading, onSelect }: MetricCardProps) {
  const content = (
    <>
      <h3 className="truncate text-xs font-semibold text-slate-600">{label}</h3>
      <div className="mt-2 text-2xl font-semibold tracking-normal text-slate-950">
        {loading ? <span className="block h-8 w-24 rounded bg-slate-100" /> : formatValue(value)}
      </div>
      {delta ? (
        <p
          data-tone={delta.tone || "neutral"}
          className="mt-1 text-xs text-slate-500 data-[tone=negative]:text-red-700 data-[tone=positive]:text-green-700"
        >
          {delta.label}
        </p>
      ) : null}
    </>
  );

  if (onSelect) {
    return (
      <button
        type="button"
        data-metric={metric}
        data-selected={selected || undefined}
        onClick={() => onSelect(metric)}
        className="min-h-[98px] rounded-lg border border-slate-200 bg-white p-3 text-left shadow-sm transition hover:border-slate-300 data-[selected=true]:border-indigo-500 data-[selected=true]:ring-1 data-[selected=true]:ring-indigo-500"
      >
        {content}
      </button>
    );
  }

  return (
    <article
      data-metric={metric}
      data-selected={selected || undefined}
      className="min-h-[98px] rounded-lg border border-slate-200 bg-white p-3 text-left shadow-sm transition hover:border-slate-300 data-[selected=true]:border-indigo-500 data-[selected=true]:ring-1 data-[selected=true]:ring-indigo-500"
    >
      {content}
    </article>
  );
}
