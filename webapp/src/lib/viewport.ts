// Lazy-mount helper for panels that own their own query. Deferring until the panel is near the
// viewport does two things: an offscreen panel costs nothing, and an onscreen one starts its query
// from an IntersectionObserver callback — which is delivered after the first paint, so it lands
// behind the queries its parent view issued during mount effects. React flushes child effects
// before parent effects, so without this a panel's query would otherwise queue ahead of the primary
// chart's.

/** How far outside the visible area a panel starts loading: roughly one scroll nudge, so a panel
 *  just below the fold is already populated by the time it arrives. */
export const NEAR_VIEWPORT_MARGIN = "600px";

/** The scrolling ancestor the element actually moves inside, or null for the viewport. rootMargin
 *  only expands the observer's root, never an intermediate clip: the app scrolls inside a container
 *  rather than the document, so observing against the viewport would prefetch nothing at all. */
function scrollParent(element: Element): Element | null {
  for (let node = element.parentElement; node; node = node.parentElement) {
    const overflowY = getComputedStyle(node).overflowY;
    if (overflowY === "auto" || overflowY === "scroll") return node;
  }
  return null;
}

/**
 * Call `onEnter` once `element` comes within `rootMargin` of the visible area, then stop observing.
 * Returns a disposer.
 *
 * Where IntersectionObserver is unavailable (older browsers, non-DOM test runners) no visibility
 * callback can ever arrive, so `onEnter` fires immediately — content is never withheld from a
 * client that cannot report visibility.
 */
export function whenNearViewport(
  element: Element,
  onEnter: () => void,
  rootMargin: string = NEAR_VIEWPORT_MARGIN,
): () => void {
  if (typeof IntersectionObserver === "undefined") {
    onEnter();
    return () => {};
  }
  const observer = new IntersectionObserver(
    (entries) => {
      if (!entries.some((entry) => entry.isIntersecting)) return;
      observer.disconnect();
      onEnter();
    },
    { root: scrollParent(element), rootMargin },
  );
  observer.observe(element);
  return () => observer.disconnect();
}
