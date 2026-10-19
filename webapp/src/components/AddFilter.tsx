import { useEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import type { CatalogDimension, CatalogModel } from "../data/types";
import { joinedDimensionsForModel } from "../lib/catalog";
import { useExplorer } from "../state/ExplorerContext";
import { FilterEditor } from "./FilterEditor";
import { useFilterPopover } from "./useFilterPopover";

/** "+ Filter" affordance: pick a dimension, then edit its filter in the same popover. Non-time
 *  dimensions only (time is filtered via the date-range control). */
export function AddFilter({ model }: { model: CatalogModel }) {
  const { catalog } = useExplorer();
  const dims = [...model.dimensions, ...joinedDimensionsForModel(catalog, model.name)].filter((dim) => dim.type !== "time");
  const [open, setOpen] = useState(false);
  const [picked, setPicked] = useState<CatalogDimension | null>(null);
  const rootRef = useRef<HTMLSpanElement>(null);
  const menuRef = useRef<HTMLDivElement>(null);
  const buttonRef = useRef<HTMLButtonElement>(null);
  useFilterPopover(menuRef, buttonRef, open && !picked);

  useEffect(() => {
    if (!open) return;
    function onKey(event: KeyboardEvent) {
      if (event.key === "Escape") {
        close();
        buttonRef.current?.focus({ preventScroll: true });
      }
    }
    function onPointer(event: MouseEvent) {
      // The editor's own outside-click handler covers the picked state; only the menu needs this.
      if (picked) return;
      if (!rootRef.current?.contains(event.target as Node) && !menuRef.current?.contains(event.target as Node)) close();
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
        ref={buttonRef}
        type="button"
        aria-haspopup="dialog"
        aria-expanded={open}
        onClick={() => (open ? close() : setOpen(true))}
        className="inline-flex h-7 shrink-0 items-center rounded-full border border-dashed border-line px-3 text-xs text-muted hover:border-faint hover:text-ink"
      >
        + Filter
      </button>
      {open && !picked ? createPortal(
        <div
          ref={menuRef}
          role="menu"
          aria-label="Add a filter for a dimension"
          className="fixed z-30 max-h-[min(24rem,var(--filter-popover-height))] w-80 max-w-[calc(100vw-1rem)] overflow-y-auto rounded-xl border border-line bg-surface p-1.5 text-2xs shadow-[var(--shadow)]"
        >
          {dims.map((dim) => (
            <button
              key={dim.ref}
              type="button"
              role="menuitem"
              onClick={() => setPicked(dim)}
              className="block w-full break-words px-1.5 py-1 text-left text-muted hover:bg-surface-soft hover:text-ink"
            >
              {dim.label}
            </button>
          ))}
        </div>,
        document.body,
      ) : null}
      {open && picked ? <FilterEditor dim={picked} model={model} anchorRef={buttonRef} onClose={close} /> : null}
    </span>
  );
}
