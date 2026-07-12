import { describe, expect, test } from "bun:test";
import { renderToStaticMarkup } from "react-dom/server";
import { QueryDebugPanel, tokenizeSql } from "./QueryDebugPanel";

describe("tokenizeSql", () => {
  test("classifies comments, keywords, numbers, and strings", () => {
    const tokens = tokenizeSql("-- totals\nSELECT sum(amount), 12 FROM orders WHERE region = 'West'");
    expect(tokens.some((token) => token.kind === "comment" && token.value === "-- totals")).toBe(true);
    expect(tokens.some((token) => token.kind === "keyword" && token.value === "SELECT")).toBe(true);
    expect(tokens.some((token) => token.kind === "number" && token.value === "12")).toBe(true);
    expect(tokens.some((token) => token.kind === "string" && token.value === "'West'")).toBe(true);
  });
});

test("renders structured query inputs alongside highlighted SQL", () => {
  const html = renderToStaticMarkup(
    QueryDebugPanel({
      queries: { Totals: "select sum(revenue) from orders" },
      inputs: { Totals: { metrics: ["orders.revenue"], filters: ["orders.status = 'complete'"] } },
    }),
  );
  expect(html).toContain('data-testid="query-inputs"');
  expect(html).toContain("orders.revenue");
  expect(html).toContain("orders.status = &#x27;complete&#x27;");
});
