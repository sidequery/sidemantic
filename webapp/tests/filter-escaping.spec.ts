import { expect, test } from "@playwright/test";

for (const dialect of ["clickhouse", "duckdb", "postgres"]) {
  test(`${dialect} catalog controls search, membership, and Contains quoting`, async ({ page }) => {
    await page.route("**/describe", (route) => route.fulfill({ json: {
      dialect,
      models: [{
        name: "orders",
        dimensions: [{ name: "status", type: "categorical" }],
        metrics: [{ name: "revenue", agg: "sum" }],
      }],
    } }));
    await page.route("**/graph", (route) => route.fulfill({ json: { models: [], joinable_pairs: [] } }));
    await page.route("**/dashboard", (route) => route.fulfill({ status: 404 }));
    const text = "50%_\\'";
    const requests: { filters?: string[]; metrics?: string[] }[] = [];
    await page.route("**/query", (route) => {
      requests.push(route.request().postDataJSON());
      return route.fulfill({ json: { rows: [{ status: text, revenue: 1 }], row_count: 1 } });
    });
    const contains = dialect === "clickhouse"
      ? String.raw`CAST(orders.status AS VARCHAR) ILIKE '%50\\%\\_\\\\''%'`
      : String.raw`CAST(orders.status AS VARCHAR) ILIKE '%50\%\_\\''%' ESCAPE '\'`;
    const membership = dialect === "clickhouse"
      ? String.raw`orders.status = '50%_\\'''`
      : String.raw`orders.status = '50%_\'''`;

    await page.goto("/?view=explore&model=orders&metric=orders.revenue&cmp=off");
    await page.getByRole("button", { name: "+ Filter" }).click();
    await page.getByRole("menuitem", { name: "Status", exact: true }).click();
    const editor = page.getByRole("dialog");
    await editor.getByRole("textbox").fill(text);
    await expect.poll(() => requests.some((request) => !request.metrics && request.filters?.includes(contains))).toBe(true);
    await editor.getByRole("checkbox").check();
    await expect.poll(() => requests.some((request) => request.metrics?.length && request.filters?.includes(membership))).toBe(true);
    await editor.getByRole("button", { name: "Contains", exact: true }).click();
    await editor.getByRole("textbox").fill(text);
    await expect.poll(() => requests.some((request) => request.metrics?.length && request.filters?.includes(contains))).toBe(true);
  });
}
