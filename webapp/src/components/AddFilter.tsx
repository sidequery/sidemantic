import { useEffect, useRef, useState } from "react";
import type { CatalogDimension, CatalogModel } from "../data/types";
import { FilterEditor } from "./FilterEditor";

/** "+ Filter" affordance: pick a dimension, then edit its filter in the same popover. Non-time
 *  dimensions only (time is filtered via the date-range control). */
export function AddFilter({ model }: { model: CatalogModel }) {
  const dims = model.dimensions.filter((dim) => dim.type !== "time");
  const [open, setOpen] = useState(false);
  const [picked, setPicked] = useState<CatalogDimension | null>(null);
  const rootRef = useRef<HTMLSpanElement>(null);

  useEffect(() => {
    if (!open) return;
    function onKey(event: KeyboardEvent) {
      if (event.key === "Escape") close();
    }
    function onPointer(event: MouseEvent) {
      // The editor's own outside-click handler covers the picked state; only the menu needs this.
      if (picked) return;
      if (rootRef.current && !rootRef.current.contains(event.target as Node)) close();
    }
    document.addEventListener("keydown", onKey, true);
    document.addEventListener("mousedown", onPointer, true);
    return () => {
      document.removeEventListener("keydown", onKey, true);
      document.removeEventListener("mousedown", onPointer, true);
    };
  }, [open, picked]);

  function close() {
    setOpen(false);
    setPicked(null);
  }

  if (dims.length === 0) return null;

  return (
    <span ref={rootRef} className="relative inline-flex">
      <button
        type="button"
        aria-haspopup="dialog"
        aria-expanded={open}
        onClick={() => (open ? close() : setOpen(true))}
        className="min-h-8 shrink-0 rounded-full border border-dashed border-line px-3 text-xs font-medium text-muted transition-colors hover:bg-surface-soft hover:text-ink"
      >
        + Filter
      </button>
      {open && !picked ? (
        <div
          role="menu"
          aria-label="Add a filter for a dimension"
          className="absolute left-0 z-50 mt-2 max-h-64 w-52 overflow-y-auto rounded-xl bg-surface p-1.5 text-xs shadow-floating"
        >
          {dims.map((dim) => (
            <button
              key={dim.ref}
              type="button"
              role="menuitem"
              onClick={() => setPicked(dim)}
              className="block min-h-9 w-full truncate rounded-lg px-2.5 text-left text-muted hover:bg-surface-soft hover:text-ink"
            >
              {dim.label}
            </button>
          ))}
        </div>
      ) : null}
      {open && picked ? <FilterEditor dim={picked} model={model} onClose={close} /> : null}
    </span>
  );
}
