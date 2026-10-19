import { useLayoutEffect, type RefObject } from "react";

/** Keep a filter popup below its chip and inside the viewport, including after pills wrap. */
export function useFilterPopover(ref: RefObject<HTMLDivElement>, open = true) {
  // Run after each render: adding a pill can move this anchor without resizing it.
  useLayoutEffect(() => {
    const panel = ref.current;
    const anchor = panel?.parentElement;
    if (!open || !panel || !anchor) return;

    function position() {
      if (!panel || !anchor) return;
      const bounds = anchor.getBoundingClientRect();
      const gutter = 8;
      const left = Math.max(gutter, Math.min(bounds.left, window.innerWidth - panel.offsetWidth - gutter));
      panel.style.left = `${left - bounds.left}px`;
      panel.style.setProperty("--filter-popover-height", `${Math.max(0, window.innerHeight - bounds.bottom - gutter - 4)}px`);
    }

    position();
    const observer = new ResizeObserver(position);
    observer.observe(panel);
    observer.observe(anchor);
    // A sibling pill can wrap without changing this anchor's own size.
    if (anchor.parentElement) observer.observe(anchor.parentElement);
    window.addEventListener("resize", position);
    window.addEventListener("scroll", position, true);
    return () => {
      observer.disconnect();
      window.removeEventListener("resize", position);
      window.removeEventListener("scroll", position, true);
    };
  });
}
