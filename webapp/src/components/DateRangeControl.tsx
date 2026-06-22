import { useRef, useState } from "react";
import { DATE_PRESETS, presetRange, type DateRange } from "../lib/time";

type DateRangeControlProps = {
  range?: DateRange;
  disabled?: boolean;
  onChange: (range?: DateRange) => void;
};

export function DateRangeControl({ range, disabled, onChange }: DateRangeControlProps) {
  const details = useRef<HTMLDetailsElement>(null);
  const [from, setFrom] = useState(range?.from ?? "");
  const [to, setTo] = useState(range?.to ?? "");

  function close() {
    if (details.current) details.current.open = false;
  }
  function apply(next?: DateRange) {
    onChange(next);
    close();
  }

  const summary = range ? `${range.from} → ${range.to}` : "All time";

  return (
    <details ref={details} className="relative text-2xs">
      <summary
        className={`flex cursor-pointer items-center gap-1.5 border border-line bg-surface px-2 py-1 text-ink ${
          disabled ? "pointer-events-none opacity-50" : ""
        }`}
      >
        <span className="text-faint">Range</span>
        <span className="font-mono tnum">{summary}</span>
        <span aria-hidden="true" className="text-faint">▾</span>
      </summary>
      <div className="absolute right-0 z-50 mt-1 w-64 border border-line bg-surface p-2 shadow-lg">
        <button
          type="button"
          onClick={() => apply(undefined)}
          className="mb-2 w-full border border-line px-2 py-1 text-left text-2xs text-muted hover:bg-surface-soft"
        >
          All time
        </button>
        <div className="grid grid-cols-2 gap-1">
          {DATE_PRESETS.map((preset) => (
            <button
              key={preset.key}
              type="button"
              onClick={() => apply(presetRange(preset.days))}
              className="border border-line px-2 py-1 text-2xs text-muted hover:bg-surface-soft"
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
            className="mt-2 w-full border border-accent bg-accent-soft px-2 py-1 text-2xs font-medium text-accent disabled:opacity-50"
          >
            Apply custom range
          </button>
        </div>
      </div>
    </details>
  );
}
