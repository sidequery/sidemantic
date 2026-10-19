import { useLayoutEffect, type RefObject } from "react";

/** Position a portalled filter popup below its chip, outside the strip's scroll clip. */
export function useFilterPopover(ref: RefObject<HTMLDivElement>, anchorRef: RefObject<HTMLButtonElement>, open = true) {
  // Run after each render: adding a pill can move this anchor without resizing it.
  useLayoutEffect(() => {
    const panel = ref.current;
    const anchor = anchorRef.current;
    if (!open || !panel || !anchor) return;

    function position() {
      if (!panel || !anchor) return;
      const bounds = anchor.getBoundingClientRect();
      const strip = anchor.closest("[data-filter-strip]")?.getBoundingClientRect();
      const gutter = 8;
      const left = Math.max(gutter, Math.min(bounds.left, window.innerWidth - panel.offsetWidth - gutter));
      // Keep an open editor attached to the visible strip edge when its chip scrolls out of view.
      const bottom = strip ? Math.max(strip.top, Math.min(bounds.bottom, strip.bottom)) : bounds.bottom;
      const top = Math.max(gutter, Math.min(bottom + 4, window.innerHeight - gutter));
      panel.style.left = `${left}px`;
      panel.style.top = `${top}px`;
      panel.style.setProperty("--filter-popover-height", `${Math.max(0, window.innerHeight - top - gutter)}px`);
    }

    position();
    const observer = new ResizeObserver(position);
    observer.observe(panel);
    observer.observe(anchor);
    // A sibling pill can wrap without changing this anchor's own size.
    const strip = anchor.closest("[data-filter-strip]");
    if (strip) observer.observe(strip);
    window.addEventListener("resize", position);
    window.addEventListener("scroll", position, true);
    return () => {
      observer.disconnect();
      window.removeEventListener("resize", position);
      window.removeEventListener("scroll", position, true);
    };
  });
}
