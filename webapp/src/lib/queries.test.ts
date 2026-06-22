import { describe, expect, test } from "bun:test";
import { NULL_TOKEN } from "../data/types";
import { filterExprs } from "./queries";

describe("filterExprs", () => {
  test("no filters (or empty value lists) yields no expressions", () => {
    expect(filterExprs({})).toEqual([]);
    expect(filterExprs({ "orders.status": [] })).toEqual([]);
  });

  test("a single string value is a quoted equality", () => {
    expect(filterExprs({ "orders.status": ["completed"] })).toEqual(["orders.status = 'completed'"]);
  });

  test("multiple string values become an IN list", () => {
    expect(filterExprs({ "orders.status": ["a", "b"] })).toEqual(["orders.status IN ('a', 'b')"]);
  });

  test("string literals are single-quote escaped", () => {
    expect(filterExprs({ "customers.name": ["O'Brien"] })).toEqual(["customers.name = 'O''Brien'"]);
  });

  test("numeric and boolean dimensions are not quoted", () => {
    const types = { "orders.amount": "numeric", "orders.is_paid": "boolean" };
    expect(filterExprs({ "orders.amount": ["5"] }, { types })).toEqual(["orders.amount = 5"]);
    expect(filterExprs({ "orders.is_paid": ["true"] }, { types })).toEqual(["orders.is_paid = true"]);
  });

  test("a non-numeric value on a numeric dimension still quotes (no silent bad SQL)", () => {
    expect(filterExprs({ "orders.amount": ["n/a"] }, { types: { "orders.amount": "numeric" } })).toEqual([
      "orders.amount = 'n/a'",
    ]);
  });

  test("the NULL token becomes IS NULL", () => {
    expect(filterExprs({ "orders.status": [NULL_TOKEN] })).toEqual(["orders.status IS NULL"]);
  });

  test("present values OR with a null selection in the same dimension", () => {
    expect(filterExprs({ "orders.status": ["a", NULL_TOKEN] })).toEqual([
      "(orders.status = 'a' OR orders.status IS NULL)",
    ]);
  });

  test("dimensions are emitted independently (ANDed by the caller)", () => {
    expect(filterExprs({ "orders.status": ["a"], "orders.country": ["US"] })).toEqual([
      "orders.status = 'a'",
      "orders.country = 'US'",
    ]);
  });

  test("excludeDim drops only that dimension's own selection", () => {
    const filters = { "orders.status": ["a"], "orders.country": ["US"] };
    expect(filterExprs(filters, { excludeDim: "orders.status" })).toEqual(["orders.country = 'US'"]);
  });
});
