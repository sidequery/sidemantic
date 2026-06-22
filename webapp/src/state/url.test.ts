import { describe, expect, test } from "bun:test";
import type { ExplorerState } from "./explorerState";
import { decodeState, encodeState } from "./url";

const base: ExplorerState = {
  view: "explore",
  model: "orders",
  selectedMetric: "orders.revenue",
  filters: {},
  grain: "month",
  dateRange: undefined,
  pivotDims: [],
  pivotMetrics: [],
};

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

  test("accepts a well-formed filter map", () => {
    const filters = { "orders.status": ["a", "b"] };
    const next = decodeState(`filters=${encodeURIComponent(JSON.stringify(filters))}`, base);
    expect(next.filters).toEqual(filters);
  });

  test("rejects a malformed filter map (values must be string arrays)", () => {
    const next = decodeState(`filters=${encodeURIComponent(JSON.stringify({ "orders.status": "CA" }))}`, base);
    expect(next.filters).toEqual({});
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

  test("round-trips a populated state through encodeState", () => {
    const populated: ExplorerState = {
      ...base,
      view: "pivot",
      grain: "week",
      dateRange: { from: "2024-01-01", to: "2024-03-31" },
      filters: { "orders.status": ["completed"] },
      pivotDims: ["orders.country"],
      pivotMetrics: ["orders.revenue"],
    };
    expect(decodeState(encodeState(populated), base)).toEqual(populated);
  });
});
