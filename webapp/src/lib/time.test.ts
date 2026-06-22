import { describe, expect, test } from "bun:test";
import { addDays, bucketOffset, endOfBucket, previousRange } from "./time";

describe("bucketOffset", () => {
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
