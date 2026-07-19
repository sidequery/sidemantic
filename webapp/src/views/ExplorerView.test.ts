import { describe, expect, test } from "bun:test";
import { resolveExpandedLeaderboard } from "./ExplorerView";

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
