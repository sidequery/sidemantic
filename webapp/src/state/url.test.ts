import { describe, expect, test } from "bun:test";
import { includeFilter } from "../lib/queries";
import type { ExplorerState } from "./explorerState";
import { decodeState, encodeState } from "./url";

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

const filtersParam = (value: unknown) => `filters=${encodeURIComponent(JSON.stringify(value))}`;

describe("decodeState", () => {
  test("an empty search returns the base state", () => {
    expect(decodeState("", base)).toEqual(base);
  });

  test("reads known params", () => {
    const next = decodeState("view=pivot&grain=day&metric=orders.x", base);
    expect(next.view).toBe("pivot");
    expect(next.grain).toBe("day");
    expect(next.selectedMetric).toBe("orders.x");
  });

  test("ignores an unknown view or grain", () => {
    const next = decodeState("view=galaxy&grain=decade", base);
    expect(next.view).toBe("explore");
    expect(next.grain).toBe("month");
  });

  test("old-format bare value arrays deserialize as include filters (backward compatible)", () => {
    const next = decodeState(filtersParam({ "orders.status": ["a", "b"] }), base);
    expect(next.filters).toEqual({ "orders.status": { mode: "include", values: ["a", "b"] } });
  });

  test("new-format include filter round-trips through a bare array in the URL", () => {
    const state = { ...base, filters: { "orders.status": includeFilter(["a"]) } };
    // Include filters serialize to a bare array (short links); decode restores the object form.
    expect(encodeState(state)).toContain(encodeURIComponent(JSON.stringify({ "orders.status": ["a"] })));
    expect(decodeState(encodeState(state), base).filters).toEqual(state.filters);
  });

  test("new-format exclude filter round-trips as an object", () => {
    const next = decodeState(filtersParam({ "orders.status": { mode: "exclude", values: ["US"] } }), base);
    expect(next.filters).toEqual({ "orders.status": { mode: "exclude", values: ["US"] } });
  });

  test("new-format contains filter round-trips its pattern", () => {
    const next = decodeState(filtersParam({ "customers.name": { mode: "contains", values: [], pattern: "acme" } }), base);
    expect(next.filters).toEqual({ "customers.name": { mode: "contains", values: [], pattern: "acme" } });
  });

  test("drops filters with an unknown mode but keeps valid siblings", () => {
    const next = decodeState(
      filtersParam({ "orders.a": { mode: "wat", values: ["x"] }, "orders.b": ["y"] }),
      base,
    );
    expect(next.filters).toEqual({ "orders.b": { mode: "include", values: ["y"] } });
  });

  test("drops a filter that would emit no SQL (empty include list)", () => {
    expect(decodeState(filtersParam({ "orders.status": { mode: "include", values: [] } }), base).filters).toEqual({});
    expect(decodeState(filtersParam({ "orders.status": { mode: "contains", values: [], pattern: "" } }), base).filters).toEqual({});
  });

  test("rejects a wholly malformed filter map (not an object)", () => {
    expect(decodeState(filtersParam(["not", "a", "map"]), base).filters).toEqual({});
    expect(decodeState(filtersParam("scalar"), base).filters).toEqual({});
  });

  test("rejects a malformed dimension entry (scalar value) but keeps the map otherwise valid", () => {
    const next = decodeState(filtersParam({ "orders.status": 42, "orders.country": ["US"] }), base);
    expect(next.filters).toEqual({ "orders.country": { mode: "include", values: ["US"] } });
  });

  test("rejects non-JSON filters without throwing", () => {
    expect(decodeState("filters=not-json", base).filters).toEqual({});
  });

  test("accepts an ordered ISO date range", () => {
    expect(decodeState("from=2024-01-01&to=2024-01-31", base).dateRange).toEqual({
      from: "2024-01-01",
      to: "2024-01-31",
    });
  });

  test("rejects a reversed or non-ISO date range", () => {
    expect(decodeState("from=2024-02-01&to=2024-01-01", base).dateRange).toBeUndefined();
    expect(decodeState("from=2024-1-1&to=2024-01-31", base).dateRange).toBeUndefined();
  });

  test("a pre-E2/E3 URL (no ctx/cmp params) decodes to the defaults", () => {
    const next = decodeState("view=explore&grain=month&metric=orders.revenue", base);
    expect(next.contextColumn).toBe("none");
    expect(next.comparison).toBe("previous");
    expect(next.comparisonRange).toBeUndefined();
  });

  test("reads the context column and comparison mode", () => {
    const next = decodeState("ctx=deltaPct&cmp=year", base);
    expect(next.contextColumn).toBe("deltaPct");
    expect(next.comparison).toBe("year");
  });

  test("ignores an unknown context column or comparison mode", () => {
    const next = decodeState("ctx=bogus&cmp=galaxy", base);
    expect(next.contextColumn).toBe("none");
    expect(next.comparison).toBe("previous");
  });

  test("reads a custom comparison range only when comparison is custom", () => {
    const custom = decodeState("cmp=custom&cfrom=2023-01-01&cto=2023-01-31", base);
    expect(custom.comparison).toBe("custom");
    expect(custom.comparisonRange).toEqual({ from: "2023-01-01", to: "2023-01-31" });
    // A custom range attached to a non-custom mode is ignored.
    const ignored = decodeState("cmp=year&cfrom=2023-01-01&cto=2023-01-31", base);
    expect(ignored.comparisonRange).toBeUndefined();
  });

  test("encodeState omits default context/comparison so links stay short and back-compatible", () => {
    const params = new URLSearchParams(encodeState(base));
    expect(params.has("ctx")).toBe(false);
    expect(params.has("cmp")).toBe(false);
  });

  test("round-trips a populated state through encodeState", () => {
    const populated: ExplorerState = {
      ...base,
      view: "pivot",
      grain: "week",
      dateRange: { from: "2024-01-01", to: "2024-03-31" },
      contextColumn: "pctTotal",
      comparison: "custom",
      comparisonRange: { from: "2023-01-01", to: "2023-03-31" },
      filters: {
        "orders.status": includeFilter(["completed"]),
        "orders.country": { mode: "exclude", values: ["US"] },
        "customers.name": { mode: "contains", values: [], pattern: "acme" },
      },
      pivotDims: ["orders.country"],
      pivotMetrics: ["orders.revenue"],
    };
    expect(decodeState(encodeState(populated), base)).toEqual(populated);
  });
});
