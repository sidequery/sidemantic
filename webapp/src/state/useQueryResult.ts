import { useEffect, useRef, useState } from "react";
import type { SidemanticBackend } from "../data/backend";
import type { QueryResult, StructuredQuery } from "../data/types";
import { beginQuery, endQuery } from "./queryActivity";

export type QueryResultState = {
  result?: QueryResult;
  loading: boolean;
  error?: string;
};

// Coalesce bursts (rapid filter toggles) — a new state lands well within this window, so only the
// final query in a burst is actually issued. Small enough to feel instant on a single action.
const DEBOUNCE_MS = 80;

/**
 * Run a structured query whenever it changes, with stale-response guarding so out-of-order
 * responses never overwrite a newer one. The previous result is kept visible while the next
 * one loads (skeleton-over-data), avoiding layout flicker on every crossfilter toggle. Firing is
 * debounced so a burst of changes issues a single query.
 *
 * Pass `null` to skip (e.g. no metric selected) — yields an idle, non-loading state.
 */
export function useQueryResult(backend: SidemanticBackend, query: StructuredQuery | null): QueryResultState {
  const [state, setState] = useState<QueryResultState>({ loading: false });
  const token = useRef(0);
  const key = query ? JSON.stringify(query) : null;

  useEffect(() => {
    if (!query) {
      setState({ loading: false });
      return;
    }
    const current = ++token.current;
    setState((prev) => ({ result: prev.result, loading: true }));
    const timer = setTimeout(() => {
      beginQuery();
      backend
        .runQuery(query)
        .then((result) => {
          if (current === token.current) setState({ result, loading: false });
        })
        .catch((err: unknown) => {
          if (current === token.current) {
            setState({ loading: false, error: err instanceof Error ? err.message : String(err) });
          }
        })
        .finally(() => endQuery());
    }, DEBOUNCE_MS);
    // Superseded queries are cancelled before they ever fire (and before begin/endQuery).
    return () => clearTimeout(timer);
    // key captures the full query; backend is stable for the app's lifetime.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key, backend]);

  return state;
}
