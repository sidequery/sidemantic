import { expect, test } from "bun:test";
import { act, create, type ReactTestRenderer } from "react-test-renderer";
import { ExplorerProvider, useExplorer } from "../state/ExplorerContext";
import type { SidemanticBackend } from "../data/backend";
import type { Catalog, QueryResult } from "../data/types";
import { ExplorerView } from "./ExplorerView";
import { MetricTimeSeries } from "../components/MetricTimeSeries";
import { MetricCard } from "../components/MetricCard";

test("sparse comparison keeps its window alignment and stays off after a pending total resolves", async () => {
  let dispatch!: ReturnType<typeof useExplorer>["dispatch"];
  function Capture() {
    dispatch = useExplorer().dispatch;
    return <ExplorerView />;
  }
  const time = { ref: "orders.created_at", name: "created_at", model: "orders", label: "Created", type: "time" as const };
  const catalog: Catalog = { models: [{ name: "orders", label: "Orders", dimensions: [time], timeDimension: time,
    metrics: [{ ref: "orders.revenue", name: "revenue", model: "orders", label: "Revenue" }] }], graphMetrics: [] };
  let resolvePrevious!: (result: QueryResult) => void;
  const backend = {
    runQuery: async (query) => {
      const previous = query.filters?.some((filter) => filter.includes("2024-01-01"));
      if (query.dimensions?.length) {
        const rows = previous
          ? [{ created_at__day: "2024-01-02", revenue: 200 }, { created_at__day: "2024-01-03", revenue: 300 }]
          : [{ created_at__day: "2024-01-08", revenue: 10 }, { created_at__day: "2024-01-09", revenue: 20 }];
        return { columns: ["created_at__day", "revenue"], rows, rowCount: rows.length, sql: "series" };
      }
      if (previous) return new Promise<QueryResult>((resolve) => { resolvePrevious = resolve; });
      return { columns: ["revenue"], rows: [{ revenue: 30 }], rowCount: 1, sql: "total" };
    },
  } as SidemanticBackend;
  let tree!: ReactTestRenderer;
  try {
    await act(async () => {
      tree = create(<ExplorerProvider catalog={catalog} backend={backend}
        initialSearch="?view=explore&model=orders&metric=orders.revenue&from=2024-01-08&to=2024-01-14&grain=day"
        onSearchChange={() => {}}><Capture /></ExplorerProvider>);
    });
    await act(async () => { await new Promise((resolve) => setTimeout(resolve, 120)); });
    expect(tree.root.findByType(MetricTimeSeries).props.comparisonPoints)
      .toEqual([{ x: "2024-01-08", y: NaN }, { x: "2024-01-09", y: 200 }]);
    expect(resolvePrevious).toBeFunction();
    await act(async () => dispatch({ type: "setComparison", comparison: "off" }));
    expect(tree.root.findByType(MetricCard).props.delta).toBeNull();
    await act(async () => resolvePrevious({ columns: ["revenue"], rows: [{ revenue: 500 }], rowCount: 1, sql: "previous" }));
    expect(tree.root.findByType(MetricCard).props.delta).toBeNull();
    expect(tree.root.findByType(MetricTimeSeries).props.comparisonPoints).toBeUndefined();
  } finally {
    await act(async () => tree?.unmount());
  }
});
