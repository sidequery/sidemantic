import { describe, expect, test } from "bun:test";
import { queryAlias } from "./types";

describe("queryAlias", () => {
  test("keeps the short alias when fields do not collide", () => {
    expect(queryAlias("orders.revenue", ["orders.revenue", "customers.count"])).toBe("revenue");
  });

  test("matches model-prefixed backend aliases for cross-model collisions", () => {
    const fields = ["orders.count", "customers.count"];
    expect(queryAlias("orders.count", fields)).toBe("orders_count");
    expect(queryAlias("customers.count", fields)).toBe("customers_count");
  });
});
