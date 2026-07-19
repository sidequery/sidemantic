import { describe, expect, test } from "bun:test";
import { addDays, bucketOffset, endOfBucket, formatBucketLabel, previousRange, previousYearRange } from "./time";

describe("bucketOffset", () => {
  test("counts second and minute timestamp buckets", () => {
    expect(bucketOffset("2024-01-01T00:00:00", "2024-01-01T00:00:42", "second")).toBe(42);
    expect(bucketOffset("2024-01-01T00:00:00", "2024-01-01T00:42:00", "minute")).toBe(42);
  });
  test("counts whole grain units between two bucket starts", () => {
    expect(bucketOffset("2024-01-01", "2024-01-01", "day")).toBe(0);
    expect(bucketOffset("2024-01-01", "2024-01-05", "day")).toBe(4);
    expect(bucketOffset("2024-01-01", "2024-01-15", "week")).toBe(2);
    expect(bucketOffset("2024-01-01", "2024-03-01", "month")).toBe(2);
    expect(bucketOffset("2024-01-01", "2024-07-01", "quarter")).toBe(2);
    expect(bucketOffset("2024-01-01", "2026-01-01", "year")).toBe(2);
    expect(bucketOffset("2024-01-01T00:00:00", "2024-01-01T05:00:00", "hour")).toBe(5);
  });

  test("is signed (a label before the anchor is negative)", () => {
    expect(bucketOffset("2024-03-01", "2024-01-01", "month")).toBe(-2);
  });

  test("tolerates timestamp bucket labels on date grains", () => {
    expect(bucketOffset("2024-01-01T00:00:00", "2024-01-04T13:00:00", "day")).toBe(3);
  });
});

describe("previousRange", () => {
  test("returns the equal-length window immediately before the range", () => {
    expect(previousRange({ from: "2024-01-08", to: "2024-01-14" })).toEqual({
      from: "2024-01-01",
      to: "2024-01-07",
    });
  });
});

describe("previousYearRange", () => {
  test("shifts both endpoints back one calendar year, same month/day", () => {
    expect(previousYearRange({ from: "2024-03-01", to: "2024-03-31" })).toEqual({
      from: "2023-03-01",
      to: "2023-03-31",
    });
  });

  test("clamps Feb 29 to Feb 28 when the prior year isn't a leap year", () => {
    expect(previousYearRange({ from: "2024-02-01", to: "2024-02-29" })).toEqual({
      from: "2023-02-01",
      to: "2023-02-28",
    });
  });

  test("keeps Feb 29 when the prior year is a leap year", () => {
    expect(previousYearRange({ from: "2025-02-01", to: "2025-02-28" })).toEqual({
      from: "2024-02-01",
      to: "2024-02-28",
    });
  });

  // Weekly-grain YoY is the alignment edge case: a year isn't a whole number of weeks, so the
  // previous-year window's first bucket sits at a fractional-week offset. bucketOffset must still
  // map each current week onto the nearest previous-year week (rounding), keeping the overlay aligned.
  test("weekly bucketOffset aligns a previous-year series to the current one", () => {
    const range = { from: "2024-01-01", to: "2024-01-28" };
    const prev = previousYearRange(range); // { from: "2023-01-01", to: "2023-01-28" }
    expect(prev).toEqual({ from: "2023-01-01", to: "2023-01-28" });

    // Current weekly buckets and the previous-year weekly buckets, offset from each series' first.
    const curWeeks = ["2024-01-01", "2024-01-08", "2024-01-15", "2024-01-22"];
    const prevWeeks = ["2023-01-02", "2023-01-09", "2023-01-16", "2023-01-23"];
    const curOffsets = curWeeks.map((w) => bucketOffset(curWeeks[0], w, "week"));
    const prevOffsets = prevWeeks.map((w) => bucketOffset(prevWeeks[0], w, "week"));
    // Each period's buckets are 0..3, so aligning by offset lines them up one-to-one.
    expect(curOffsets).toEqual([0, 1, 2, 3]);
    expect(prevOffsets).toEqual([0, 1, 2, 3]);
  });
});

describe("endOfBucket", () => {
  test("returns the inclusive last day of the bucket for each grain", () => {
    expect(endOfBucket("2024-01-01", "day")).toBe("2024-01-01");
    expect(endOfBucket("2024-01-01", "week")).toBe("2024-01-07");
    expect(endOfBucket("2024-02-01", "month")).toBe("2024-02-29"); // leap year
    expect(endOfBucket("2024-01-01", "quarter")).toBe("2024-03-31");
    expect(endOfBucket("2024-01-01", "year")).toBe("2024-12-31");
  });
});

describe("addDays", () => {
  test("crosses month and leap-day boundaries in UTC", () => {
    expect(addDays("2024-02-28", 1)).toBe("2024-02-29");
    expect(addDays("2024-03-01", -1)).toBe("2024-02-29");
  });
});

describe("formatBucketLabel", () => {
  test("retains clock precision for second and minute grains", () => {
    expect(formatBucketLabel("2024-01-02T03:04:05", "second")).toBe("2024-01-02 03:04:05");
    expect(formatBucketLabel("2024-01-02T03:04:00", "minute")).toBe("2024-01-02 03:04");
  });
  // The backend already truncates in the selected timezone and returns local wall-clock
  // bucket labels, so formatBucketLabel must NOT re-zone them (that double-shifts the date).
  test("presents day-grain labels as their local calendar date, without shifting", () => {
    expect(formatBucketLabel("2024-01-02", "day")).toBe("2024-01-02");
    // A local wall-clock day bucket keeps its date regardless of any offset.
    expect(formatBucketLabel("2026-01-15T00:00:00", "day")).toBe("2026-01-15");
    expect(formatBucketLabel("2026-01-15 00:00:00", "day")).toBe("2026-01-15");
  });

  test("formats hour-grain labels as local YYYY-MM-DD HH:MM without re-zoning", () => {
    expect(formatBucketLabel("2026-01-15T12:00:00", "hour")).toBe("2026-01-15 12:00");
    expect(formatBucketLabel("2026-01-15 07:30:00", "hour")).toBe("2026-01-15 07:30");
    // A bare date at hour grain has no clock component to show.
    expect(formatBucketLabel("2026-01-15", "hour")).toBe("2026-01-15");
  });

  test("passes empty / unparseable labels through", () => {
    expect(formatBucketLabel("", "day")).toBe("");
    expect(formatBucketLabel("not-a-date", "day")).toBe("not-a-date");
  });
});
