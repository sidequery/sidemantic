import { describe, expect, test } from "bun:test";
import { brushDateRange, chronologicalSeriesRows, resolveExpandedLeaderboard } from "./ExplorerView";

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
