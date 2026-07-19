import type { Grain } from "../data/types";
import { labelize } from "../lib/format";

type GrainSelectProps = {
  grain: Grain;
  options: Grain[];
  disabled?: boolean;
  onChange: (grain: Grain) => void;
};

export function GrainSelect({ grain, options, disabled, onChange }: GrainSelectProps) {
  return (
    <label className="flex items-center gap-1.5 text-xs text-muted">
      <span className="hidden sm:inline">Grain</span>
      <select
        aria-label="Time grain"
        value={grain}
        disabled={disabled}
        onChange={(event) => onChange(event.target.value as Grain)}
        className="min-h-9 rounded-full border border-line bg-surface px-3 text-xs text-ink transition-colors hover:bg-surface-soft disabled:opacity-50"
      >
        {options.map((option) => (
          <option key={option} value={option}>
            {labelize(option)}
          </option>
        ))}
      </select>
    </label>
  );
}
