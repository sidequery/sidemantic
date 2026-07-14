import { useState } from "react";
import type { CatalogDimension, CatalogModel } from "../data/types";
import { filterSummary } from "../lib/format";
import type { DimFilter } from "../lib/queries";
import { FilterEditor } from "./FilterEditor";

type EditableFilterPillProps = {
  dim: CatalogDimension;
  model: CatalogModel;
  filter: DimFilter;
  onRemove: () => void;
};

type PortableFilterPillProps = {
  dimension: string;
  dimensionLabel?: string;
  value: string;
  onRemove?: () => void;
};

type FilterPillProps = EditableFilterPillProps | PortableFilterPillProps;

/** A per-dimension filter chip showing a mode-aware summary ("region is 3 values", "region is not
 *  US", "name contains 'acme'"). Clicking the label opens the editor popover; the × clears it. */
export function FilterPill(props: FilterPillProps) {
  const [open, setOpen] = useState(false);

  if (!("dim" in props)) {
    return (
      <span data-dimension={props.dimension} data-value={props.value} className="inline-flex max-w-full items-center gap-1.5 border border-line bg-surface px-2 py-0.5 text-2xs text-muted">
        <span className="truncate"><span className="text-faint">{props.dimensionLabel ?? props.dimension}:</span> {props.value}</span>
        {props.onRemove ? (
          <button type="button" aria-label={`Remove filter ${props.value}`} onClick={props.onRemove} className="grid size-3.5 place-items-center rounded-full bg-surface-soft text-faint hover:bg-line hover:text-ink">×</button>
        ) : null}
      </span>
    );
  }

  const { dim, model, filter, onRemove } = props;

  return (
    <span className="relative inline-flex max-w-full items-center" data-dimension={dim.ref} data-mode={filter.mode}>
      <span className="inline-flex max-w-full items-center gap-1.5 border border-line bg-surface px-2 py-0.5 text-2xs text-muted">
        <button
          type="button"
          aria-label={`Edit filter ${dim.label}`}
          aria-haspopup="dialog"
          aria-expanded={open}
          onClick={() => setOpen((v) => !v)}
          className="min-w-0 truncate text-left hover:text-ink"
        >
          <span className="text-faint">{dim.label}</span> {filterSummary(filter)}
        </button>
        <button
          type="button"
          aria-label={`Remove filter ${dim.label}`}
          onClick={onRemove}
          className="grid size-3.5 shrink-0 place-items-center rounded-full bg-surface-soft text-faint hover:bg-line hover:text-ink"
        >
          ×
        </button>
      </span>
      {open ? <FilterEditor dim={dim} model={model} onClose={() => setOpen(false)} /> : null}
    </span>
  );
}
