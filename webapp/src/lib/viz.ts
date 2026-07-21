// Shared helpers for the categorical chart components. Colors resolve through the theme's
// --viz-N custom properties so embedders can rebrand charts via applyThemeTokens.

export const VIZ_COLOR_COUNT = 7;

/** CSS color for the Nth series (0-based), cycling through the theme's categorical palette. */
export function vizColor(index: number): string {
  const slot = ((index % VIZ_COLOR_COUNT) + VIZ_COLOR_COUNT) % VIZ_COLOR_COUNT;
  return `var(--viz-${slot + 1})`;
}

/** Evenly spaced axis ticks including both ends. Collapses to [min] on a degenerate domain. */
export function axisTicks(min: number, max: number, count = 4): number[] {
  if (!(max > min)) return [min];
  const step = (max - min) / (count - 1);
  return Array.from({ length: count }, (_, index) => min + step * index);
}

/** Observe an element's content width. Components use this instead of aspect-distorting viewBox scaling. */
export function observeWidth(node: Element | null, minWidth: number, onWidth: (width: number) => void): () => void {
  if (!node || typeof ResizeObserver === "undefined") return () => {};
  const observer = new ResizeObserver((entries) => {
    for (const entry of entries) onWidth(Math.max(minWidth, entry.contentRect.width));
  });
  observer.observe(node);
  return () => observer.disconnect();
}
