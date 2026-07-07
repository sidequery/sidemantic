import { describe, expect, test } from "bun:test";
import { formatDeltaAbs, formatDeltaPct, formatPercentOfTotal } from "./format";

describe("formatPercentOfTotal", () => {
  test("renders a value as a percent of the total", () => {
    expect(formatPercentOfTotal(25, 100)).toEqual({ label: "25%", tone: "neutral" });
    expect(formatPercentOfTotal(1, 3).label).toBe("33.3%");
  });

  test("a non-positive or non-finite total has no meaningful share (em dash)", () => {
    expect(formatPercentOfTotal(5, 0)).toEqual({ label: "—", tone: "neutral" });
    expect(formatPercentOfTotal(5, -10)).toEqual({ label: "—", tone: "neutral" });
    expect(formatPercentOfTotal(5, Number.NaN)).toEqual({ label: "—", tone: "neutral" });
  });

  test("a non-finite value has no share", () => {
    expect(formatPercentOfTotal(Number.NaN, 100).label).toBe("—");
  });
});

describe("formatDeltaAbs", () => {
  test("signed, compact absolute change with tone", () => {
    expect(formatDeltaAbs(120, 100)).toEqual({ label: "+20", tone: "positive" });
    expect(formatDeltaAbs(80, 100)).toEqual({ label: "−20", tone: "negative" });
    expect(formatDeltaAbs(100, 100)).toEqual({ label: "0", tone: "neutral" });
  });

  test("a missing previous value renders an em dash, never 0", () => {
    expect(formatDeltaAbs(100, undefined)).toEqual({ label: "—", tone: "neutral" });
    expect(formatDeltaAbs(100, null)).toEqual({ label: "—", tone: "neutral" });
    expect(formatDeltaAbs(100, Number.NaN)).toEqual({ label: "—", tone: "neutral" });
  });

  test("honors compact currency formatting", () => {
    expect(formatDeltaAbs(3000, 1000, { format: "usd" }).label).toBe("+$2K");
  });
});

describe("formatDeltaPct", () => {
  test("signed percent change with tone", () => {
    expect(formatDeltaPct(120, 100)).toEqual({ label: "+20%", tone: "positive" });
    expect(formatDeltaPct(50, 100)).toEqual({ label: "-50%", tone: "negative" });
  });

  test("a missing or zero previous value has no defined percent (em dash)", () => {
    expect(formatDeltaPct(100, 0)).toEqual({ label: "—", tone: "neutral" });
    expect(formatDeltaPct(100, undefined)).toEqual({ label: "—", tone: "neutral" });
    expect(formatDeltaPct(100, null)).toEqual({ label: "—", tone: "neutral" });
  });
});
