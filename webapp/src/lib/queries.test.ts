import { describe, expect, test } from "bun:test";
import { NULL_TOKEN } from "../data/types";
import {
  dimensionLeaderboard,
  filterExprs,
  includeFilter,
  isEmptyFilter,
  likeEscape,
  metricSeries,
  metricTotals,
  type DimFilter,
} from "./queries";

// Shorthands so the existing include-mode cases read the same as before the mode refactor.
const inc = (values: string[]): DimFilter => includeFilter(values);
const exc = (values: string[]): DimFilter => ({ mode: "exclude", values });
const has = (pattern: string): DimFilter => ({ mode: "contains", values: [], pattern });

describe("filterExprs (include mode — preserved behavior)", () => {
  test("no filters (or empty value lists) yields no expressions", () => {
    expect(filterExprs({})).toEqual([]);
    expect(filterExprs({ "orders.status": inc([]) })).toEqual([]);
  });

  test("a single string value is a quoted equality", () => {
    expect(filterExprs({ "orders.status": inc(["completed"]) })).toEqual(["orders.status = 'completed'"]);
  });

  test("multiple string values become an IN list", () => {
    expect(filterExprs({ "orders.status": inc(["a", "b"]) })).toEqual(["orders.status IN ('a', 'b')"]);
  });

  test("string literals are single-quote escaped", () => {
    expect(filterExprs({ "customers.name": inc(["O'Brien"]) })).toEqual(["customers.name = 'O''Brien'"]);
  });

  test("numeric and boolean dimensions are not quoted", () => {
    const types = { "orders.amount": "numeric", "orders.is_paid": "boolean" };
    expect(filterExprs({ "orders.amount": inc(["5"]) }, { types })).toEqual(["orders.amount = 5"]);
    expect(filterExprs({ "orders.is_paid": inc(["true"]) }, { types })).toEqual(["orders.is_paid = true"]);
  });

  test("a non-numeric value on a numeric dimension still quotes (no silent bad SQL)", () => {
    expect(filterExprs({ "orders.amount": inc(["n/a"]) }, { types: { "orders.amount": "numeric" } })).toEqual([
      "orders.amount = 'n/a'",
    ]);
  });

  test("the NULL token becomes IS NULL", () => {
    expect(filterExprs({ "orders.status": inc([NULL_TOKEN]) })).toEqual(["orders.status IS NULL"]);
  });

  test("present values OR with a null selection in the same dimension", () => {
    expect(filterExprs({ "orders.status": inc(["a", NULL_TOKEN]) })).toEqual([
      "(orders.status = 'a' OR orders.status IS NULL)",
    ]);
  });

  test("dimensions are emitted independently (ANDed by the caller)", () => {
    expect(filterExprs({ "orders.status": inc(["a"]), "orders.country": inc(["US"]) })).toEqual([
      "orders.status = 'a'",
      "orders.country = 'US'",
    ]);
  });

  test("excludeDim drops only that dimension's own selection", () => {
    const filters = { "orders.status": inc(["a"]), "orders.country": inc(["US"]) };
    expect(filterExprs(filters, { excludeDim: "orders.status" })).toEqual(["orders.country = 'US'"]);
  });
});

describe("filterExprs (exclude mode)", () => {
  // `!=` / `NOT IN` are UNKNOWN for NULL rows, which would silently drop them. When the null
  // token is NOT explicitly excluded, keep NULL rows with an `OR ... IS NULL` branch.
  test("a single value negates with != and preserves NULLs", () => {
    expect(filterExprs({ "orders.status": exc(["US"]) })).toEqual(["(orders.status != 'US' OR orders.status IS NULL)"]);
  });

  test("multiple values become a NOT IN list and preserve NULLs", () => {
    expect(filterExprs({ "orders.status": exc(["a", "b"]) })).toEqual([
      "(orders.status NOT IN ('a', 'b') OR orders.status IS NULL)",
    ]);
  });

  test("the NULL token alone becomes IS NOT NULL", () => {
    expect(filterExprs({ "orders.status": exc([NULL_TOKEN]) })).toEqual(["orders.status IS NOT NULL"]);
  });

  test("excluding a value AND the null token drops both (no OR IS NULL branch)", () => {
    expect(filterExprs({ "orders.status": exc(["a", NULL_TOKEN]) })).toEqual([
      "(orders.status != 'a' AND orders.status IS NOT NULL)",
    ]);
  });

  test("numeric exclusions are not quoted and still preserve NULLs", () => {
    expect(filterExprs({ "orders.amount": exc(["5", "6"]) }, { types: { "orders.amount": "numeric" } })).toEqual([
      "(orders.amount NOT IN (5, 6) OR orders.amount IS NULL)",
    ]);
  });
});

describe("filterExprs (contains mode)", () => {
  // The dimension is cast to text so contains works on numeric/boolean dimensions too
  // (DuckDB/Postgres reject ILIKE on non-text operands).
  test("emits a case-insensitive substring ILIKE with an ESCAPE clause, cast to text", () => {
    expect(filterExprs({ "customers.name": has("acme") })).toEqual([
      "CAST(customers.name AS VARCHAR) ILIKE '%acme%' ESCAPE '\\'",
    ]);
  });

  test("LIKE metacharacters in the pattern are neutralized", () => {
    expect(filterExprs({ "customers.name": has("50%_off") })).toEqual([
      "CAST(customers.name AS VARCHAR) ILIKE '%50\\%\\_off%' ESCAPE '\\'",
    ]);
  });

  test("single quotes in the pattern are still SQL-escaped", () => {
    expect(filterExprs({ "customers.name": has("O'Brien") })).toEqual([
      "CAST(customers.name AS VARCHAR) ILIKE '%O''Brien%' ESCAPE '\\'",
    ]);
  });

  test("an empty pattern emits nothing", () => {
    expect(filterExprs({ "customers.name": has("") })).toEqual([]);
  });
});

describe("likeEscape", () => {
  test("escapes backslash first, then LIKE metacharacters", () => {
    expect(likeEscape("a")).toBe("a");
    expect(likeEscape("100%")).toBe("100\\%");
    expect(likeEscape("a_b")).toBe("a\\_b");
    expect(likeEscape("c:\\path")).toBe("c:\\\\path");
    expect(likeEscape("50%_\\")).toBe("50\\%\\_\\\\");
  });
});

describe("isEmptyFilter", () => {
  test("include/exclude with no values are empty; with values are not", () => {
    expect(isEmptyFilter(inc([]))).toBe(true);
    expect(isEmptyFilter(exc([]))).toBe(true);
    expect(isEmptyFilter(inc(["a"]))).toBe(false);
  });

  test("contains is empty only with a blank pattern", () => {
    expect(isEmptyFilter(has(""))).toBe(true);
    expect(isEmptyFilter(has("x"))).toBe(false);
    // A contains filter ignores its (inert) values for emptiness.
    expect(isEmptyFilter({ mode: "contains", values: ["a"], pattern: "" })).toBe(true);
  });
});

describe("dashboard query configuration", () => {
  test("threads segments and pre-aggregation opt-outs through every Explore query", () => {
    const segments = ["orders.completed"];
    const queries = [
      metricTotals(["orders.revenue"], [], segments, false),
      metricSeries(["orders.revenue"], "orders.created_at", "month", [], segments, false),
      dimensionLeaderboard("orders.revenue", "orders.region", [], 6, segments, false),
    ];
    for (const query of queries) {
      expect(query.segments).toEqual(segments);
      expect(query.usePreaggregations).toBe(false);
    }
  });
});
