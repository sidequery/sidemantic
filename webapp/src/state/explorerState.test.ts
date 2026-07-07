import { describe, expect, test } from "bun:test";
import { explorerReducer, type ExplorerState } from "./explorerState";

const base: ExplorerState = {
  view: "explore",
  model: "orders",
  selectedMetric: "orders.revenue",
  filters: {},
  grain: "month",
  dateRange: undefined,
  contextColumn: "none",
  comparison: "previous",
  comparisonRange: undefined,
  pivotDims: [],
  pivotMetrics: [],
};

describe("setContextColumn", () => {
  test("sets the leaderboard context column", () => {
    expect(explorerReducer(base, { type: "setContextColumn", column: "pctTotal" }).contextColumn).toBe("pctTotal");
  });
});

describe("setComparison", () => {
  test("switching to a preset mode drops any stale custom range", () => {
    const withCustom: ExplorerState = { ...base, comparison: "custom", comparisonRange: { from: "2023-01-01", to: "2023-01-31" } };
    const next = explorerReducer(withCustom, { type: "setComparison", comparison: "year" });
    expect(next.comparison).toBe("year");
    expect(next.comparisonRange).toBeUndefined();
  });

  test("custom mode keeps the provided range", () => {
    const range = { from: "2022-06-01", to: "2022-06-30" };
    const next = explorerReducer(base, { type: "setComparison", comparison: "custom", range });
    expect(next.comparison).toBe("custom");
    expect(next.comparisonRange).toEqual(range);
  });

  test("custom mode without a range clears the range (deferred until bounds are entered)", () => {
    const next = explorerReducer(base, { type: "setComparison", comparison: "custom" });
    expect(next.comparison).toBe("custom");
    expect(next.comparisonRange).toBeUndefined();
  });
});

describe("setModel", () => {
  // A model switch shouldn't disturb the display-only context/comparison settings.
  test("preserves contextColumn and comparison", () => {
    const state: ExplorerState = { ...base, contextColumn: "deltaPct", comparison: "year" };
    const next = explorerReducer(state, { type: "setModel", model: "sessions", metric: "sessions.count", grain: "day" });
    expect(next.contextColumn).toBe("deltaPct");
    expect(next.comparison).toBe("year");
  });
});
