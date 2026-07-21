import { describe, expect, test } from "bun:test";
import { binValues } from "./HistogramChart";

describe("binValues", () => {
  test("spreads values across equal-width bins and closes the last bin", () => {
    const bins = binValues([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 5);
    expect(bins).toHaveLength(5);
    expect(bins[0].x0).toBe(0);
    expect(bins[4].x1).toBe(10);
    // The max value must land inside the final bin, not fall off the edge.
    expect(bins[4].count).toBeGreaterThan(0);
    expect(bins.reduce((sum, bin) => sum + bin.count, 0)).toBe(11);
  });

  test("ignores non-finite values and collapses a constant series to one bin", () => {
    expect(binValues([Number.NaN, Number.POSITIVE_INFINITY])).toEqual([]);
    expect(binValues([7, 7, 7])).toEqual([{ x0: 7, x1: 7, count: 3 }]);
  });

  test("defaults the bin count via Sturges' rule", () => {
    const bins = binValues(Array.from({ length: 128 }, (_, index) => index));
    expect(bins).toHaveLength(8);
  });
});
