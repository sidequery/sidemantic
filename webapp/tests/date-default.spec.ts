import { expect, test } from "@playwright/test";

test("Explore bounds its initial requests and preserves explicit All time across reload", async ({ page }) => {
  await page.clock.setFixedTime(new Date("2026-10-19T12:00:00Z"));
  await page.route("**/describe", (route) => route.fulfill({ json: {
    models: [{
      name: "orders", default_time_dimension: "created_at",
      dimensions: [
        { name: "created_at", type: "time" },
        ...["country", "status", "channel", "region", "category", "source"].map((name) => ({ name, type: "categorical" })),
      ],
      metrics: [{ name: "revenue", agg: "sum" }],
    }],
  } }));
  await page.route("**/graph", (route) => route.fulfill({ json: { models: [], joinable_pairs: [] } }));
  const requests: { filters?: string[] }[] = [];
  await page.route("**/query", (route) => {
    requests.push(route.request().postDataJSON());
    return route.fulfill({ json: { data: [], row_count: 0 } });
  });

  await page.goto("/?view=explore&model=orders&metric=orders.revenue&cmp=off");
  await expect.poll(() => requests.length).toBe(8);
  for (const request of requests) {
    expect(request.filters).toEqual([
      "orders.created_at >= cast('2026-09-22' as date)",
      "orders.created_at < cast('2026-10-20' as date)",
    ]);
  }
  expect(new URL(page.url()).searchParams.has("from")).toBe(false);
  await expect(page.getByRole("button", { name: "Reset", exact: true })).toHaveCount(0);

  await page.locator("summary").filter({ hasText: "Range" }).click();
  await page.getByRole("button", { name: "All time", exact: true }).click();
  await expect.poll(() => new URL(page.url()).searchParams.get("range")).toBe("all");
  await expect.poll(() => requests.length).toBe(16);
  expect(requests.slice(8).every((request) => !request.filters)).toBe(true);

  requests.length = 0;
  await page.reload();
  await expect.poll(() => requests.length).toBe(8);
  expect(requests.every((request) => !request.filters)).toBe(true);
  await expect(page.locator("summary").filter({ hasText: "Range" })).toContainText("All time");
  await page.getByRole("button", { name: "Reset", exact: true }).click();
  await expect.poll(() => new URL(page.url()).searchParams.has("range")).toBe(false);
  await expect(page.locator("summary").filter({ hasText: "Range" })).toContainText("2026-09-22");
});
