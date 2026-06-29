import { createContext, useContext, useEffect, useMemo, useReducer, type ReactNode } from "react";
import type { SidemanticBackend } from "../data/backend";
import type { Catalog } from "../data/types";
import {
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
};

const ExplorerContext = createContext<ExplorerContextValue | null>(null);

export function ExplorerProvider({
  catalog,
  backend,
  children,
}: {
  catalog: Catalog;
  backend: SidemanticBackend;
  children: ReactNode;
}) {
  const initial = useMemo(() => initialStateFromCatalog(catalog), [catalog]);
  const [state, dispatch] = useReducer(
    explorerReducer,
    undefined as never,
    () => decodeState(window.location.search, initial),
  );

  // Sync selections + filters to the URL for deep-linkable, shareable views.
  useEffect(() => {
    const query = encodeState(state);
    const next = `${window.location.pathname}${query ? `?${query}` : ""}`;
    window.history.replaceState(null, "", next);
  }, [state]);

  const value = useMemo(
    () => ({ state, dispatch, catalog, backend, initial }),
    [state, catalog, backend, initial],
  );

  return <ExplorerContext.Provider value={value}>{children}</ExplorerContext.Provider>;
}

export function useExplorer(): ExplorerContextValue {
  const value = useContext(ExplorerContext);
  if (!value) throw new Error("useExplorer must be used within ExplorerProvider");
  return value;
}
