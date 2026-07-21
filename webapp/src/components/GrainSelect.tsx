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
    <label className="flex items-center gap-1.5 text-2xs text-faint">
      <span className="hidden sm:inline">Grain</span>
      <select
        aria-label="Time grain"
        value={grain}
        disabled={disabled}
        onChange={(event) => onChange(event.target.value as Grain)}
        className="rounded border border-line bg-surface px-1.5 py-1 text-2xs text-ink disabled:opacity-50"
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
