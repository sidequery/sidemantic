import { describe, expect, test } from "bun:test";
import { formatCompact, formatValue } from "./format";
import { formatUiValue } from "./uiCore.js";

describe("ratio formatting", () => {
  test("escaped LookML percent markers preserve required and optional precision", () => {
    for (const display of [formatValue, formatCompact]) {
      expect(display(0.1234, { format: String.raw`0.00\%` })).toBe("12.34%");
      expect(display(0.1, { format: String.raw`0.00\%`, type: "ratio" })).toBe("10.00%");
      expect(display(0.1234, { format: String.raw`0.0#\%` })).toBe("12.34%");
      expect(display(0.1, { format: String.raw`0.0#\%` })).toBe("10.0%");
      expect(display(0.1234, { format: String.raw`0\%` })).toBe("12%");
    }
  });

  test("explicit numeric formats preserve ARPU, ARPPU, and AOV values", () => {
    const hint = { type: "ratio", format: "#,##0.00" };
    expect(formatValue(160907.2, hint)).toBe("160,907.20");
    expect(formatValue(0.59, hint)).toBe("0.59");
    expect(formatValue(-0.59, hint)).toBe("-0.59");
    expect(formatValue(0, hint)).toBe("0.00");
    expect(formatValue(160907.2, { ...hint, format: "#,##0" })).toBe("160,907");
    expect(formatValue(0.59, { ...hint, format: "decimal_2" })).toBe("0.59");
    expect(formatCompact(160907.2, hint)).toBe("160.91K");
    expect(formatCompact(0.59, hint)).toBe("0.59");
  });

  test("explicit currency formats override ratio inference", () => {
    for (const format of ["usd", "$#,##0.00", "usd_2"]) {
      expect(formatValue(160907.2, { type: "ratio", format })).toBe("$160,907.20");
      expect(formatValue(0.59, { type: "ratio", format })).toBe("$0.59");
    }
    expect(formatCompact(160907.2, { type: "ratio", format: "usd" })).toBe("$160.9K");
    expect(formatCompact(0.59, { type: "ratio", format: "$0.00" })).toBe("$0.59");
    expect(formatUiValue(0.59, { type: "ratio", style: "currency", currency: "USD" })).toBe("$0.59");
    expect(formatUiValue(0.59, { type: "ratio", style: "decimal" })).toBe("0.59");
  });

  test("explicit percentages and unformatted ratios retain percent behavior", () => {
    for (const format of [undefined, "", "percent", "pct"]) {
      expect(formatValue(0.59, { type: "ratio", format })).toBe("59%");
      expect(formatCompact(0.59, { type: "ratio", format })).toBe("59%");
    }
    expect(formatValue(0.59, { type: "ratio", format: "0.00%" })).toBe("59.00%");
    expect(formatCompact(0.59, { type: "ratio", format: "percent_2" })).toBe("59.00%");
    expect(formatValue(160907.2, { type: "ratio" })).toBe("160,907.2%");
    expect(formatUiValue(0.59, { style: "percent" })).toBe("59%");
  });
});
