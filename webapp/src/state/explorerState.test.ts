import { describe, expect, test } from "bun:test";
import { explorerReducer, type ExplorerState } from "./explorerState";

const initial: ExplorerState = {
  view: "home",
  model: "customers",
  selectedMetric: "customers.customer_count",
  filters: {},
  grain: "day",
  dateRange: undefined,
  pivotDims: [],
  pivotMetrics: [],
};

describe("explorerReducer reset", () => {
  test("clears scoped controls but stays on the current view", () => {
    const dirty: ExplorerState = {
      ...initial,
      view: "explore",
      model: "orders",
      selectedMetric: "orders.revenue",
      filters: { "orders.status": ["shipped"] },
      dateRange: { from: "2024-01-01", to: "2024-03-01" },
    };
    const next = explorerReducer(dirty, { type: "reset", initial });
    // Scoped state returns to baseline...
    expect(next.filters).toEqual({});
    expect(next.dateRange).toBeUndefined();
    expect(next.model).toBe(initial.model);
    // ...but Reset must not eject the user to the home index.
    expect(next.view).toBe("explore");
  });

  test("preserves the pivot view too", () => {
    const dirty: ExplorerState = { ...initial, view: "pivot", filters: { "orders.status": ["shipped"] } };
    expect(explorerReducer(dirty, { type: "reset", initial }).view).toBe("pivot");
  });
});
