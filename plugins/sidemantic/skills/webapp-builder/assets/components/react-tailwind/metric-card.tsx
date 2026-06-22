import { formatValue, type MetricTone, type ValueFormatOptions } from "./types";

type MetricCardProps = {
  metric: string;
  label: string;
  value: unknown;
  delta?: { label: string; tone?: MetricTone };
  selected?: boolean;
  loading?: boolean;
  // Formatting for the value (currency / percent / decimals). Mirrors the static `valueFormat` option.
  format?: ValueFormatOptions;
  onSelect?: (metric: string) => void;
};

export function MetricCard({ metric, label, value, delta, selected, loading, format, onSelect }: MetricCardProps) {
  // Borderless: just label + value + delta on the page background. Selected = indigo value.
  const content = (
    <>
      <h3 className="truncate text-xs font-medium text-slate-500">{label}</h3>
      <div className={`mt-1 text-2xl font-semibold tracking-normal ${selected ? "text-indigo-600" : "text-slate-950"}`}>
        {loading ? <span className="block h-8 w-24 rounded bg-slate-100" /> : formatValue(value, format)}
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
        className="block w-full py-1 text-left transition hover:opacity-70"
      >
        {content}
      </button>
    );
  }

  return (
    <article data-metric={metric} data-selected={selected || undefined} className="py-1">
      {content}
    </article>
  );
}
