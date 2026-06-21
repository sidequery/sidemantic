import { displayDimValue, labelize } from "../lib/format";

type FilterPillProps = {
  dimension: string; // ref
  dimensionLabel?: string;
  value: string;
  onRemove?: () => void;
};

export function FilterPill({ dimension, dimensionLabel, value, onRemove }: FilterPillProps) {
  return (
    <span
      data-dimension={dimension}
      data-value={value}
      className="inline-flex max-w-full items-center gap-1.5 border border-line bg-surface px-2 py-0.5 text-2xs text-muted"
    >
      <span className="truncate">
        <span className="text-faint">{dimensionLabel ?? labelize(dimension)}:</span> {displayDimValue(value)}
      </span>
      {onRemove ? (
        <button
          type="button"
          aria-label={`Remove filter ${value}`}
          onClick={onRemove}
          className="grid size-3.5 place-items-center rounded-full bg-surface-soft text-faint hover:bg-line hover:text-ink"
        >
          ×
        </button>
      ) : null}
    </span>
  );
}
