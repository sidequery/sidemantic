import { labelize } from "./types";

type FilterPillProps = {
  dimension: string;
  value: string;
  onRemove?: (filter: { dimension: string; value: string }) => void;
};

export function FilterPill({ dimension, value, onRemove }: FilterPillProps) {
  return (
    <span
      data-dimension={dimension}
      data-value={value}
      className="inline-flex max-w-full items-center gap-1 rounded-full border border-slate-200 bg-white px-2 py-1 text-xs text-slate-600"
    >
      <span className="truncate">
        {labelize(dimension)}: {value}
      </span>
      {onRemove ? (
        <button
          type="button"
          aria-label={`Remove ${value}`}
          onClick={() => onRemove({ dimension, value })}
          className="grid size-4 place-items-center rounded-full bg-slate-100 text-slate-500 hover:bg-slate-200"
        >
          ×
        </button>
      ) : null}
    </span>
  );
}
