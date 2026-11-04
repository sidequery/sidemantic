import { describe, expect, test } from "bun:test";
import {
  alignComparisonSeries,
  brushDateRange,
  canBatchMetric,
  chronologicalSeriesRows,
  isCurrentQueryResult,
  resolveExpandedLeaderboard,
} from "./ExplorerView";

describe("canBatchMetric", () => {
  test.each(["cohort", "conversion", "retention"])("keeps %s metrics out of aggregate strip queries", (type) => {
    expect(canBatchMetric({ type })).toBe(false);
  });

  test.each([undefined, "ratio", "derived", "cumulative", "time_comparison"])(
    "allows composable metric type %s in aggregate strip queries",
    (type) => {
      expect(canBatchMetric({ type })).toBe(true);
    },
  );
});

describe("isCurrentQueryResult", () => {
  test("rejects rows produced by the previously selected dedicated metric", () => {
    expect(isCurrentQueryResult('{"metrics":["events.retention"]}', '{"metrics":["events.conversion"]}')).toBe(false);
  });

  test("accepts rows produced by the active dedicated metric", () => {
    const key = '{"metrics":["events.retention"]}';
    expect(isCurrentQueryResult(key, key)).toBe(true);
  });
});

describe("resolveExpandedLeaderboard", () => {
  test("clears an expansion that is not available on the active dashboard tab", () => {
    expect(resolveExpandedLeaderboard("orders.status", [{ ref: "customers.country" }])).toBeNull();
  });

  test("keeps an expansion that belongs to the active dashboard tab", () => {
    expect(resolveExpandedLeaderboard("customers.country", [{ ref: "customers.country" }])).toBe(
      "customers.country",
    );
  });
});

test("chronologicalSeriesRows restores display order after a latest-first capped query", () => {
  const rows = [
    { bucket: "2024-01-01T00:02:00", revenue: 2 },
    { bucket: "2024-01-01T00:01:00", revenue: 1 },
  ];
  expect(chronologicalSeriesRows(rows, "bucket").map((row) => row.revenue)).toEqual([1, 2]);
});

test("brushDateRange preserves fine-grain timestamp bounds", () => {
  expect(brushDateRange({ from: "2024-01-01T00:01:00", to: "2024-01-01T00:02:00" }, "minute")).toEqual({
    from: "2024-01-01T00:01:00",
    to: "2024-01-01T00:03:00",
  });
});

describe("alignComparisonSeries", () => {
  const currentRange = { from: "2024-01-08", to: "2024-01-14" };
  const previousRange = { from: "2024-01-01", to: "2024-01-07" };

  test("preserves a missing leading comparison bucket and interior gaps", () => {
    expect(alignComparisonSeries(
      [{ x: "2024-01-08", y: 10 }, { x: "2024-01-09", y: 20 }, { x: "2024-01-10", y: 30 }],
      [{ x: "2024-01-02", y: 200 }], currentRange, previousRange, "day",
    )).toEqual([{ x: "2024-01-08", y: NaN }, { x: "2024-01-09", y: 200 }, { x: "2024-01-10", y: NaN }]);
  });

  test("a missing leading current bucket does not shift the previous series", () => {
    expect(alignComparisonSeries(
      [{ x: "2024-01-09", y: 20 }], [{ x: "2024-01-01", y: 100 }, { x: "2024-01-02", y: 200 }],
      currentRange, previousRange, "day",
    )).toEqual([{ x: "2024-01-09", y: 200 }]);
  });

  test.each([
    ["week", "2024-02-04", "2024-01-29", "2024-01-01", "2024-01-01"],
    ["quarter", "2024-08-31", "2024-07-01", "2024-01-01", "2024-01-01"],
    ["hour", "2024-01-02T01:45:00", "2024-01-02T01:00:00", "2024-01-01T01:00:00", "2024-01-01T01:00:00"],
  ] as const)("anchors partial %s windows to containing bucket boundaries", (grain, from, current, prevFrom, previous) => {
    expect(alignComparisonSeries(
      [{ x: current, y: 10 }], [{ x: previous, y: 100 }],
      { from, to: from }, { from: prevFrom, to: prevFrom }, grain,
    )).toEqual([{ x: current, y: 100 }]);
  });
});
