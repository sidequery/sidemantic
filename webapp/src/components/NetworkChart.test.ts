import { describe, expect, test } from "bun:test";
import { layoutNetwork } from "./NetworkChart";

const NODES = [{ id: "orders" }, { id: "customers" }, { id: "products" }, { id: "regions" }];
const LINKS = [
  { source: "orders", target: "customers" },
  { source: "orders", target: "products" },
];

describe("layoutNetwork", () => {
  test("is deterministic for the same graph", () => {
    const first = layoutNetwork(NODES, LINKS);
    const second = layoutNetwork(NODES, LINKS);
    expect(first).toEqual(second);
  });

  test("keeps every node inside the padded viewport and counts degrees", () => {
    const positioned = layoutNetwork(NODES, LINKS, { width: 400, height: 200 });
    for (const node of positioned) {
      expect(node.x).toBeGreaterThanOrEqual(0);
      expect(node.x).toBeLessThanOrEqual(400);
      expect(node.y).toBeGreaterThanOrEqual(0);
      expect(node.y).toBeLessThanOrEqual(200);
    }
    const orders = positioned.find((node) => node.id === "orders");
    const regions = positioned.find((node) => node.id === "regions");
    expect(orders?.degree).toBe(2);
    expect(regions?.degree).toBe(0);
  });

  test("ignores links that reference unknown nodes or self-loops", () => {
    const positioned = layoutNetwork(NODES, [
      { source: "orders", target: "orders" },
      { source: "orders", target: "missing" },
    ]);
    expect(positioned.every((node) => node.degree === 0)).toBe(true);
  });
});
