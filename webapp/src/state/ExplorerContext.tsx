import { createContext, useContext, useEffect, useMemo, useReducer, type ReactNode } from "react";
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

export function ExplorerProvider({
  catalog,
  backend,
  dashboard,
  children,
}: {
  catalog: Catalog;
  backend: SidemanticBackend;
  dashboard?: DashboardSpec | null;
  children: ReactNode;
}) {
  const initial = useMemo(() => initialStateFromCatalog(catalog, dashboard), [catalog, dashboard]);
  const [state, dispatch] = useReducer(
    explorerReducer,
    undefined as never,
    () => {
      const decoded = decodeState(window.location.search, initial);
      const configured = dashboardTabConfig(catalog, dashboard, decoded.dashboardTab);
      if (!configured) return decoded;
      return applyDashboardConfig(decoded, configured, window.location.search);
    },
  );

  // Sync selections + filters to the URL for deep-linkable, shareable views.
  useEffect(() => {
    const query = encodeState(state);
    const next = `${window.location.pathname}${query ? `?${query}` : ""}`;
    window.history.replaceState(null, "", next);
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
