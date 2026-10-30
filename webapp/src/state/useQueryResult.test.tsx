import { describe, expect, test } from "bun:test";
import { act, create, type ReactTestRenderer } from "react-test-renderer";
import type { SidemanticBackend } from "../data/backend";
import type { QueryResult, StructuredQuery } from "../data/types";
import { useQueryResult, type QueryResultState } from "./useQueryResult";

const query: StructuredQuery = { metrics: ["orders.revenue"] };
const result: QueryResult = { columns: ["revenue"], rows: [{ revenue: 123 }], rowCount: 1, sql: "SELECT revenue" };

function deferred() {
  let resolve!: (result: QueryResult) => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<QueryResult>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}

async function start(backend: SidemanticBackend) {
  let state!: QueryResultState;
  function Probe({ query }: { query: StructuredQuery | null }) {
    state = useQueryResult(backend, query);
    return null;
  }
  let tree!: ReactTestRenderer;
  await act(async () => { tree = create(<Probe query={query} />); });
  await act(async () => { await new Promise((resolve) => setTimeout(resolve, 100)); });
  return { tree, state: () => state, update: (query: StructuredQuery | null) => tree.update(<Probe query={query} />) };
}

describe("useQueryResult cancellation", () => {
  test.each(["resolve", "reject"] as const)("ignores a late %s after disabling an in-flight query", async (completion) => {
    const pending = deferred();
    let called = false;
    const probe = await start({ runQuery: () => { called = true; return pending.promise; } } as SidemanticBackend);
    try {
      expect(called).toBe(true);
      await act(async () => probe.update(null));
      expect(probe.state()).toEqual({ loading: false, queryKey: undefined });
      await act(async () => {
        if (completion === "resolve") pending.resolve(result);
        else pending.reject(new Error("stale failure"));
      });
      expect(probe.state()).toEqual({ loading: false, queryKey: undefined });
    } finally {
      await act(async () => probe.tree.unmount());
    }
  });

  test("a disabled then re-enabled query ignores its old response", async () => {
    const old = deferred();
    const current = deferred();
    let calls = 0;
    const probe = await start({ runQuery: () => ++calls === 1 ? old.promise : current.promise } as SidemanticBackend);
    try {
      await act(async () => probe.update(null));
      await act(async () => probe.update(query));
      await act(async () => { await new Promise((resolve) => setTimeout(resolve, 100)); });
      expect(calls).toBe(2);
      await act(async () => current.resolve(result));
      await act(async () => old.resolve({ ...result, rows: [{ revenue: 999 }] }));
      expect(probe.state().result).toEqual(result);
    } finally {
      await act(async () => probe.tree.unmount());
    }
  });
});
