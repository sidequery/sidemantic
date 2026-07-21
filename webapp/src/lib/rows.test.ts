import { describe, expect, test } from "bun:test";
import { parseTemporal, rowsToBarLine, rowsToCategories, rowsToCells, rowsToPoints, rowsToSeries, rowsToTimeSeries } from "./rows";

const ROWS = [
  { month: "2026-01", region: "NA", revenue: 100, orders: 10 },
  { month: "2026-01", region: "EMEA", revenue: 60, orders: 8 },
  { month: "2026-02", region: "NA", revenue: 120, orders: 11 },
];

describe("parseTemporal", () => {
  test("parses ISO date-only strings in UTC so they never shift a day", () => {
    expect(parseTemporal("2026-01-15")?.toISOString()).toBe("2026-01-15T00:00:00.000Z");
    expect(parseTemporal("2026-01")?.toISOString()).toBe("2026-01-01T00:00:00.000Z");
  });

  test("distinguishes epoch seconds from milliseconds, as numbers or strings", () => {
    expect(parseTemporal(1700000000)?.getUTCFullYear()).toBe(2023);
    expect(parseTemporal(1700000000000)?.getUTCFullYear()).toBe(2023);
    expect(parseTemporal("1700000000")?.getUTCFullYear()).toBe(2023);
  });

  test("passes through Date instances and rejects junk", () => {
    const date = new Date(Date.UTC(2024, 0, 1));
    expect(parseTemporal(date)).toBe(date);
    expect(parseTemporal("not a date")).toBeNull();
    expect(parseTemporal(null)).toBeNull();
    expect(parseTemporal({})).toBeNull();
  });

  test("keeps full precision for timestamp strings", () => {
    expect(parseTemporal("2026-01-15T10:30:00Z")?.toISOString()).toBe("2026-01-15T10:30:00.000Z");
  });
});

describe("row adapters", () => {
  test("rowsToCategories maps field names and coerces numerics", () => {
    expect(rowsToCategories(ROWS.slice(0, 2), { x: "region", y: "revenue" })).toEqual([
      { label: "NA", value: 100 },
      { label: "EMEA", value: 60 },
    ]);
  });

  test("rowsToBarLine and rowsToTimeSeries keep input order", () => {
    expect(rowsToBarLine(ROWS.slice(0, 1), { x: "region", bar: "revenue", line: "orders" })).toEqual([
      { label: "NA", bar: 100, line: 10 },
    ]);
    expect(rowsToTimeSeries(ROWS, { x: "month", y: "revenue" })[2]).toEqual({ x: "2026-02", y: 120 });
  });

  test("rowsToPoints includes label/series only when the field is named", () => {
    const bare = rowsToPoints(ROWS.slice(0, 1), { x: "revenue", y: "orders" });
    expect(bare[0]).toEqual({ x: 100, y: 10 });
    const tagged = rowsToPoints(ROWS.slice(0, 1), { x: "revenue", y: "orders", series: "region" });
    expect(tagged[0]).toEqual({ x: 100, y: 10, series: "NA" });
  });

  test("rowsToCells stringifies axes and null labels become the null token", () => {
    const cells = rowsToCells([{ month: "2026-01", region: null, revenue: 5 }], { x: "month", y: "region", value: "revenue" });
    expect(cells[0]).toEqual({ x: "2026-01", y: "∅", value: 5 });
  });

  test("rowsToSeries pivots long rows into aligned series with zero fill", () => {
    const { labels, series } = rowsToSeries(ROWS, { x: "month", y: "revenue", series: "region" });
    expect(labels).toEqual(["2026-01", "2026-02"]);
    expect(series).toEqual([
      { name: "NA", values: [100, 120] },
      { name: "EMEA", values: [60, 0] },
    ]);
  });
});
