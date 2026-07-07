import { describe, expect, test } from "bun:test";
import { NULL_TOKEN } from "../data/types";
import { includeFilter, type FilterState } from "../lib/queries";
import { explorerReducer, type ExplorerAction, type ExplorerState } from "./explorerState";

const base: ExplorerState = {
  view: "explore",
  model: "orders",
  selectedMetric: "orders.revenue",
  filters: {},
  grain: "month",
  timezone: "UTC",
  dateRange: undefined,
  contextColumn: "none",
  comparison: "previous",
  comparisonRange: undefined,
  pivotDims: [],
  pivotMetrics: [],
};

const withFilters = (filters: FilterState): ExplorerState => ({ ...base, filters });
const run = (state: ExplorerState, action: ExplorerAction) => explorerReducer(state, action).filters;

describe("explorerReducer — filter actions", () => {
  test("toggleFilter adds an include filter, then toggles the value off (dropping the key)", () => {
    const added = run(base, { type: "toggleFilter", dim: "orders.status", value: "a" });
    expect(added).toEqual({ "orders.status": { mode: "include", values: ["a"] } });
    const removed = explorerReducer(withFilters(added), { type: "toggleFilter", dim: "orders.status", value: "a" });
    expect(removed.filters).toEqual({});
  });

  test("toggleFilter preserves an exclude mode when adding a value (leaderboard-into-exclude)", () => {
    const start = withFilters({ "orders.status": { mode: "exclude", values: ["a"] } });
    expect(run(start, { type: "toggleFilter", dim: "orders.status", value: "b" })).toEqual({
      "orders.status": { mode: "exclude", values: ["a", "b"] },
    });
  });

  test("toggleFilter on a contains-mode dimension falls back to include", () => {
    const start = withFilters({ "orders.status": { mode: "contains", values: [], pattern: "x" } });
    expect(run(start, { type: "toggleFilter", dim: "orders.status", value: "a" })).toEqual({
      "orders.status": { mode: "include", values: ["a"] },
    });
  });

  test("toggleFilter with an explicit mode creates a filter in that mode (editor path)", () => {
    // The editor forces exclude even though no filter exists yet (empty exclude can't be committed).
    expect(run(base, { type: "toggleFilter", dim: "orders.status", value: "US", mode: "exclude" })).toEqual({
      "orders.status": { mode: "exclude", values: ["US"] },
    });
  });

  test("toggleFilter with a forced mode does not inherit a contains filter's inert values", () => {
    const start = withFilters({ "orders.status": { mode: "contains", values: ["stale"], pattern: "x" } });
    expect(run(start, { type: "toggleFilter", dim: "orders.status", value: "a", mode: "exclude" })).toEqual({
      "orders.status": { mode: "exclude", values: ["a"] },
    });
  });

  test("setFilterMode flips include<->exclude while keeping the value list", () => {
    const start = withFilters({ "orders.status": includeFilter(["a", "b"]) });
    expect(run(start, { type: "setFilterMode", dim: "orders.status", mode: "exclude" })).toEqual({
      "orders.status": { mode: "exclude", values: ["a", "b"] },
    });
  });

  test("setFilterMode to a fresh dimension with no values yields no filter (empty is dropped)", () => {
    expect(run(base, { type: "setFilterMode", dim: "orders.status", mode: "exclude" })).toEqual({});
  });

  test("setFilterMode to contains keeps values inert but does not emit until a pattern is set", () => {
    const start = withFilters({ "orders.status": includeFilter(["a"]) });
    // include(["a"]) -> contains with no pattern is empty, so the dimension drops out.
    expect(run(start, { type: "setFilterMode", dim: "orders.status", mode: "contains" })).toEqual({});
  });

  test("setFilterPattern creates/updates a contains filter; blanking it clears the dimension", () => {
    const set = run(base, { type: "setFilterPattern", dim: "customers.name", pattern: "acme" });
    expect(set).toEqual({ "customers.name": { mode: "contains", values: [], pattern: "acme" } });
    const cleared = explorerReducer(withFilters(set), { type: "setFilterPattern", dim: "customers.name", pattern: "" });
    expect(cleared.filters).toEqual({});
  });

  test("removeFilterValue drops a single value and the dimension when it empties", () => {
    const start = withFilters({ "orders.status": includeFilter(["a", NULL_TOKEN]) });
    const one = run(start, { type: "removeFilterValue", dim: "orders.status", value: "a" });
    expect(one).toEqual({ "orders.status": { mode: "include", values: [NULL_TOKEN] } });
    const gone = explorerReducer(withFilters(one), { type: "removeFilterValue", dim: "orders.status", value: NULL_TOKEN });
    expect(gone.filters).toEqual({});
  });

  test("removeFilterDim and clearFilters remove filters wholesale", () => {
    const start = withFilters({ "orders.status": includeFilter(["a"]), "orders.country": includeFilter(["US"]) });
    expect(run(start, { type: "removeFilterDim", dim: "orders.status" })).toEqual({
      "orders.country": { mode: "include", values: ["US"] },
    });
    expect(run(start, { type: "clearFilters" })).toEqual({});
  });
});

describe("setContextColumn", () => {
  test("sets the leaderboard context column", () => {
    expect(explorerReducer(base, { type: "setContextColumn", column: "pctTotal" }).contextColumn).toBe("pctTotal");
  });
});

describe("setTimezone", () => {
  test("sets the timezone and leaves everything else intact", () => {
    const next = explorerReducer(base, { type: "setTimezone", timezone: "America/New_York" });
    expect(next.timezone).toBe("America/New_York");
    expect(next).toEqual({ ...base, timezone: "America/New_York" });
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
