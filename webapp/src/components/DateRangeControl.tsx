import { useRef, useState } from "react";
import { DATE_PRESETS, presetRange, type DateRange } from "../lib/time";
import type { ComparisonMode } from "../state/explorerState";

type DateRangeControlProps = {
  range?: DateRange;
  disabled?: boolean;
  onChange: (range?: DateRange) => void;
  comparison: ComparisonMode;
  comparisonRange?: DateRange;
  onComparisonChange: (comparison: ComparisonMode, range?: DateRange) => void;
};

const COMPARISON_OPTIONS: { key: ComparisonMode; label: string }[] = [
  { key: "off", label: "Off" },
  { key: "previous", label: "Previous period" },
  { key: "year", label: "Previous year" },
  { key: "custom", label: "Custom range" },
];

export function DateRangeControl({
  range,
  disabled,
  onChange,
  comparison,
  comparisonRange,
  onComparisonChange,
}: DateRangeControlProps) {
  const details = useRef<HTMLDetailsElement>(null);
  const [from, setFrom] = useState(range?.from ?? "");
  const [to, setTo] = useState(range?.to ?? "");
  const [cmpFrom, setCmpFrom] = useState(comparisonRange?.from ?? "");
  const [cmpTo, setCmpTo] = useState(comparisonRange?.to ?? "");

  function close() {
    if (details.current) details.current.open = false;
  }
  function apply(next?: DateRange) {
    onChange(next);
    close();
  }
  // Comparison changes stay in the popover (don't close it) so the user can pick a mode and, for
  // custom, type its bounds without the panel collapsing between clicks.
  function applyComparison(mode: ComparisonMode) {
    if (mode === "custom") onComparisonChange("custom", cmpFrom && cmpTo ? { from: cmpFrom, to: cmpTo } : undefined);
    else onComparisonChange(mode);
  }

  const summary = range ? `${range.from} → ${range.to}` : "All time";
  // No comparison is possible without a bounded current range; reflect that in the disabled control.
  const comparisonDisabled = !range;

  return (
    <details ref={details} className="relative text-xs">
      <summary
        className={`flex cursor-pointer items-center h-7 gap-1.5 rounded-full border border-line bg-surface px-3 text-ink ${
          disabled ? "pointer-events-none opacity-50" : ""
        }`}
      >
        <span className="text-faint">Range</span>
        <span className="font-mono tnum">{summary}</span>
        <span aria-hidden="true" className="text-faint">▾</span>
      </summary>
      <div className="absolute right-0 z-50 mt-1 w-64 rounded-xl border border-line bg-surface p-2.5 shadow-[var(--shadow)]">
        <button
          type="button"
          onClick={() => apply(undefined)}
          className="mb-2 w-full rounded-lg border border-line px-2 py-1 text-left text-2xs text-muted hover:bg-surface-soft"
        >
          All time
        </button>
        <div className="grid grid-cols-2 gap-1">
          {DATE_PRESETS.map((preset) => (
            <button
              key={preset.key}
              type="button"
              onClick={() => apply(presetRange(preset.days))}
              className="rounded-lg border border-line px-2 py-1 text-2xs text-muted hover:bg-surface-soft"
            >
              {preset.label}
            </button>
          ))}
        </div>
        <div className="mt-2 border-t border-line pt-2">
          <div className="flex items-center gap-1">
            <input
              type="date"
              aria-label="From date"
              value={from}
              onChange={(event) => setFrom(event.target.value)}
              className="min-w-0 flex-1 border border-line bg-surface px-1.5 py-1 text-2xs text-ink"
            />
            <span className="text-faint">→</span>
            <input
              type="date"
              aria-label="To date"
              value={to}
              onChange={(event) => setTo(event.target.value)}
              className="min-w-0 flex-1 border border-line bg-surface px-1.5 py-1 text-2xs text-ink"
            />
          </div>
          <button
            type="button"
            disabled={!from || !to}
            onClick={() => apply({ from, to })}
            className="mt-2 w-full rounded-lg border border-accent bg-accent-soft px-2 py-1 text-2xs font-medium text-accent disabled:opacity-50"
          >
            Apply custom range
          </button>
        </div>

        <div className="mt-2 border-t border-line pt-2" data-testid="comparison-picker">
          <p className="mb-1 text-xs font-medium text-muted">Compare to</p>
          <div className={`grid grid-cols-2 gap-1 ${comparisonDisabled ? "pointer-events-none opacity-50" : ""}`}>
            {COMPARISON_OPTIONS.map((option) => (
              <button
                key={option.key}
                type="button"
                data-comparison={option.key}
                data-active={comparison === option.key || undefined}
                onClick={() => applyComparison(option.key)}
                className="rounded-lg border border-line px-2 py-1 text-2xs text-muted hover:bg-surface-soft data-[active=true]:border-accent data-[active=true]:bg-accent-soft data-[active=true]:text-accent"
              >
                {option.label}
              </button>
            ))}
          </div>
          {comparison === "custom" && !comparisonDisabled ? (
            <div className="mt-2">
              <div className="flex items-center gap-1">
                <input
                  type="date"
                  aria-label="Comparison from date"
                  value={cmpFrom}
                  onChange={(event) => setCmpFrom(event.target.value)}
                  className="min-w-0 flex-1 border border-line bg-surface px-1.5 py-1 text-2xs text-ink"
                />
                <span className="text-faint">→</span>
                <input
                  type="date"
                  aria-label="Comparison to date"
                  value={cmpTo}
                  onChange={(event) => setCmpTo(event.target.value)}
                  className="min-w-0 flex-1 border border-line bg-surface px-1.5 py-1 text-2xs text-ink"
                />
              </div>
              <button
                type="button"
                disabled={!cmpFrom || !cmpTo}
                onClick={() => onComparisonChange("custom", { from: cmpFrom, to: cmpTo })}
                className="mt-2 w-full rounded-lg border border-accent bg-accent-soft px-2 py-1 text-2xs font-medium text-accent disabled:opacity-50"
              >
                Apply comparison
              </button>
            </div>
          ) : null}
        </div>
      </div>
    </details>
  );
}
