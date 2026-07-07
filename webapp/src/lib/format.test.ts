import { describe, expect, test } from "bun:test";
import { NULL_TOKEN } from "../data/types";
import { filterSummary } from "./format";

describe("filterSummary", () => {
  test("include: one value, few values, then a count", () => {
    expect(filterSummary({ mode: "include", values: ["US"] })).toBe("is US");
    expect(filterSummary({ mode: "include", values: ["US", "CA"] })).toBe("is US, CA");
    expect(filterSummary({ mode: "include", values: ["a", "b", "c"] })).toBe("is 3 values");
  });

  test("exclude uses 'is not'", () => {
    expect(filterSummary({ mode: "exclude", values: ["US"] })).toBe("is not US");
    expect(filterSummary({ mode: "exclude", values: ["a", "b", "c", "d"] })).toBe("is not 4 values");
  });

  test("contains quotes the pattern", () => {
    expect(filterSummary({ mode: "contains", values: [], pattern: "acme" })).toBe("contains 'acme'");
  });

  test("the NULL token renders as an em dash", () => {
    expect(filterSummary({ mode: "include", values: [NULL_TOKEN] })).toBe("is —");
  });
});
