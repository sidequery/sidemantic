import { createContext, useContext, useEffect, useMemo, useReducer, useRef, type ReactNode } from "react";
import type { SidemanticBackend } from "../data/backend";
import type { Catalog, DashboardSpec } from "../data/types";
import { dashboardTabConfig } from "../lib/dashboard";
import {
  applyDashboardConfig,
  explorerReducer,
  initialStateFromCatalog,
  type ExplorerAction,
  type ExplorerState,
} from "./explorerState";
import { decodeState, encodeState } from "./url";

type ExplorerContextValue = {
  state: ExplorerState;
  dispatch: React.Dispatch<ExplorerAction>;
  catalog: Catalog;
  backend: SidemanticBackend;
  initial: ExplorerState;
  dashboard?: DashboardSpec | null;
};

const ExplorerContext = createContext<ExplorerContextValue | null>(null);

/** The query string (`"?view=explore"`, or `""` when state is fully default) that mirrors a given
 *  explorer state — the same shape as `window.location.search`, so it round-trips through
 *  `initialSearch`. */
export function searchForState(state: ExplorerState): string {
  const query = encodeState(state);
  return query ? `?${query}` : "";
}

/** Decode `search` (with or without the leading "?") into a full explorer state, applying any
 *  dashboard tab config it selects. Shared by the mount-time initializer and post-navigation
 *  re-hydration so both land on identical state. */
export function stateForSearch(
  search: string,
  catalog: Catalog,
  dashboard: DashboardSpec | null | undefined,
  initial: ExplorerState,
): ExplorerState {
  const decoded = decodeState(search, initial);
  const configured = dashboardTabConfig(catalog, dashboard, decoded.dashboardTab);
  if (!configured) return decoded;
  return applyDashboardConfig(decoded, configured, search);
}

const withoutQuestionMark = (search: string) => (search.startsWith("?") ? search.slice(1) : search);

export function ExplorerProvider({
  catalog,
  backend,
  dashboard,
  initialSearch,
  onSearchChange,
  children,
}: {
  catalog: Catalog;
  backend: SidemanticBackend;
  dashboard?: DashboardSpec | null;
  /** Query string to hydrate the initial state from (with or without the leading "?"). Defaults to
   *  the browser's `window.location.search`; a host that server-renders can pass the request's
   *  query string so the first render never touches `window`. */
  initialSearch?: string;
  /** Called with the mirrored query string ("?..." or "") on every state change, instead of
   *  rewriting the browser URL. Lets a host router own history. */
  onSearchChange?: (search: string) => void;
  children: ReactNode;
}) {
  const initial = useMemo(() => initialStateFromCatalog(catalog, dashboard), [catalog, dashboard]);
  const [state, dispatch] = useReducer(
    explorerReducer,
    undefined as never,
    () => {
      // Without a host-supplied search, read the browser URL — and fall back to an empty query when
      // there is no window at all (a bare server render), so hydration lands on the default state.
      const search = initialSearch ?? (typeof window === "undefined" ? "" : window.location.search);
      return stateForSearch(search, catalog, dashboard, initial);
    },
  );

  // A host router that keeps this provider mounted can change `initialSearch` after mount (deep
  // links, Back/Forward). Re-hydrate from it — unless it is our own `onSearchChange` echo arriving
  // back through the host's URL, which already mirrors the current state.
  const appliedSearchRef = useRef(initialSearch);
  useEffect(() => {
    if (initialSearch === undefined || initialSearch === appliedSearchRef.current) return;
    appliedSearchRef.current = initialSearch;
    if (withoutQuestionMark(initialSearch) === withoutQuestionMark(searchForState(state))) return;
    dispatch({ type: "hydrate", state: stateForSearch(initialSearch, catalog, dashboard, initial) });
  }, [initialSearch, state, catalog, dashboard, initial]);

  // Held in a ref so an inline host callback doesn't re-run the sync on every render.
  const onSearchChangeRef = useRef(onSearchChange);
  onSearchChangeRef.current = onSearchChange;

  // Sync selections + filters to the URL for deep-linkable, shareable views.
  useEffect(() => {
    const search = searchForState(state);
    const notify = onSearchChangeRef.current;
    // A host that owns the URL gets the query string and writes it however its router requires.
    if (notify) {
      notify(search);
      return;
    }
    if (typeof window === "undefined") return;
    window.history.replaceState(null, "", `${window.location.pathname}${search}`);
  }, [state]);

  const value = useMemo(
    () => ({ state, dispatch, catalog, backend, initial, dashboard }),
    [state, catalog, backend, initial, dashboard],
  );

  return <ExplorerContext.Provider value={value}>{children}</ExplorerContext.Provider>;
}

export function useExplorer(): ExplorerContextValue {
  const value = useContext(ExplorerContext);
  if (!value) throw new Error("useExplorer must be used within ExplorerProvider");
  return value;
}
