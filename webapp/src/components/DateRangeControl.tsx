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
        <span className="tnum">{summary}</span>
        <span aria-hidden="true" className="text-faint">▾</span>
      </summary>
      {/* Menu idiom: options are borderless items that light up on hover; chrome is reserved for
          the panel itself and real inputs. */}
      <div className="absolute right-0 z-50 mt-1 w-64 rounded-xl border border-line bg-surface p-1.5 shadow-[var(--shadow)]">
        <button
          type="button"
          onClick={() => apply(undefined)}
          className="w-full rounded-lg px-2.5 py-1.5 text-left text-xs text-muted hover:bg-surface-soft hover:text-ink"
        >
          All time
        </button>
        <div className="grid grid-cols-2">
          {DATE_PRESETS.map((preset) => (
            <button
              key={preset.key}
              type="button"
              onClick={() => apply(presetRange(preset.days))}
              className="rounded-lg px-2.5 py-1.5 text-left text-xs text-muted hover:bg-surface-soft hover:text-ink"
            >
              {preset.label}
            </button>
          ))}
        </div>
        <div className="mt-2 px-1 pb-1">
          <div className="flex items-center gap-1.5">
            <input
              type="date"
              aria-label="From date"
              value={from}
              onChange={(event) => setFrom(event.target.value)}
              className="h-7 min-w-0 flex-1 rounded-lg border border-line bg-surface px-2 text-xs text-ink"
            />
            <span className="text-faint">→</span>
            <input
              type="date"
              aria-label="To date"
              value={to}
              onChange={(event) => setTo(event.target.value)}
              className="h-7 min-w-0 flex-1 rounded-lg border border-line bg-surface px-2 text-xs text-ink"
            />
          </div>
          <button
            type="button"
            disabled={!from || !to}
            onClick={() => apply({ from, to })}
            className="mt-1.5 inline-flex h-7 w-full items-center justify-center rounded-full border border-accent bg-accent-soft text-xs font-medium text-accent hover:bg-accent hover:text-surface disabled:pointer-events-none disabled:opacity-50"
          >
            Apply custom range
          </button>
        </div>

        <div className="mt-1 px-1 pb-1" data-testid="comparison-picker">
          <p className="mb-1 px-1.5 text-2xs text-faint">Compare to</p>
          <div className={`grid grid-cols-2 ${comparisonDisabled ? "pointer-events-none opacity-50" : ""}`}>
            {COMPARISON_OPTIONS.map((option) => (
              <button
                key={option.key}
                type="button"
                data-comparison={option.key}
                data-active={comparison === option.key || undefined}
                onClick={() => applyComparison(option.key)}
                className="rounded-lg px-2.5 py-1.5 text-left text-xs text-muted hover:bg-surface-soft hover:text-ink data-[active=true]:bg-accent-soft data-[active=true]:text-accent"
              >
                {option.label}
              </button>
            ))}
          </div>
          {comparison === "custom" && !comparisonDisabled ? (
            <div className="mt-1.5">
              <div className="flex items-center gap-1.5">
                <input
                  type="date"
                  aria-label="Comparison from date"
                  value={cmpFrom}
                  onChange={(event) => setCmpFrom(event.target.value)}
                  className="h-7 min-w-0 flex-1 rounded-lg border border-line bg-surface px-2 text-xs text-ink"
                />
                <span className="text-faint">→</span>
                <input
                  type="date"
                  aria-label="Comparison to date"
                  value={cmpTo}
                  onChange={(event) => setCmpTo(event.target.value)}
                  className="h-7 min-w-0 flex-1 rounded-lg border border-line bg-surface px-2 text-xs text-ink"
                />
              </div>
              <button
                type="button"
                disabled={!cmpFrom || !cmpTo}
                onClick={() => onComparisonChange("custom", { from: cmpFrom, to: cmpTo })}
                className="mt-1.5 inline-flex h-7 w-full items-center justify-center rounded-full border border-accent bg-accent-soft text-xs font-medium text-accent hover:bg-accent hover:text-surface disabled:pointer-events-none disabled:opacity-50"
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
