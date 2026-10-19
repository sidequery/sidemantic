import { expect, test, type Page } from "@playwright/test";

type Query = { dimensions?: string[]; metrics?: string[]; filters?: string[] };

async function mockCatalog(page: Page, withTime = false) {
  const queries: Query[] = [];
  await page.route("**/health", (route) => route.fulfill({ json: { status: "ok" } }));
  await page.route("**/dashboard", (route) => route.fulfill({ status: 404 }));
  await page.route("**/describe", (route) => route.fulfill({ json: { models: [
    { name: "activity", dimensions: [{ name: "arm" }, ...(withTime ? [{ name: "created_at", type: "time" }] : [])], metrics: [{ name: "revenue", agg: "sum" }] },
    { name: "assignment", dimensions: [{ name: "arm", type: "numeric" }, { name: "active", type: "boolean" }, { name: "secret", public: false }, ...(withTime ? [{ name: "assigned_at", type: "time" }] : [])] },
    { name: "unrelated", dimensions: [{ name: "arm" }, ...(withTime ? [{ name: "created_at", type: "time" }] : [])], metrics: [{ name: "count", agg: "count" }] },
  ] } }));
  await page.route("**/graph", (route) => route.fulfill({ json: { joinable_pairs: [{ from: "activity", to: "assignment" }] } }));
  await page.route("**/query", (route) => {
    const query: Query = route.request().postDataJSON();
    queries.push(query);
    const both = query.dimensions?.includes("activity.arm") && query.dimensions.includes("assignment.arm");
    const row: Record<string, unknown> = {};
    if (query.metrics?.length) row.revenue = 120;
    for (const dim of query.dimensions ?? []) {
      if (dim === "activity.arm") row[both ? "activity_arm" : "arm"] = "local";
      if (dim === "assignment.arm") row[both ? "assignment_arm" : "arm"] = 2;
      if (dim === "assignment.active") row.active = true;
    }
    return route.fulfill({ json: { rows: [row], row_count: 1, sql: "select 1" } });
  });
  return queries;
}

test("joined leaderboards are opt-in, persist on reload, and clear on model change", async ({ page }) => {
  const queries = await mockCatalog(page);
  await page.goto("/?view=explore&model=activity&metric=activity.revenue");
  await expect(page.getByRole("button", { name: "Arm · Assignment", exact: true })).toBeVisible();
  await expect(page.locator('[data-catalog-dimension="assignment.secret"]')).toHaveCount(0);
  await expect(page.locator('[data-catalog-dimension="unrelated.arm"]')).toHaveCount(0);
  await expect(page.locator('button[data-dimension="activity.arm"][data-value="local"]')).toBeVisible();
  expect(queries.some((query) => query.dimensions?.some((dim) => dim.startsWith("assignment.")))).toBe(false);
  await page.getByRole("button", { name: "Arm · Assignment", exact: true }).click();
  await expect(page.locator('button[data-dimension="assignment.arm"][data-value="2"]')).toBeVisible();
  await expect.poll(() => new URL(page.url()).searchParams.get("jdims")).toBe('["assignment.arm"]');
  await page.reload();
  await expect(page.locator('button[data-dimension="assignment.arm"][data-value="2"]')).toBeVisible();
  await page.getByLabel("Model", { exact: true }).selectOption("unrelated");
  await expect.poll(() => new URL(page.url()).searchParams.get("jdims")).toBeNull();
  await expect(page.locator('button[data-dimension="assignment.arm"]')).toHaveCount(0);
});

test("Pivot retains joined deep links, rejects unreachable dimensions and renders same-name columns", async ({ page }) => {
  const queries = await mockCatalog(page);
  const params = new URLSearchParams({ view: "pivot", model: "activity", metric: "activity.revenue", pdims: JSON.stringify(["activity.arm", "assignment.arm", "unrelated.arm", "assignment.secret"]) });
  await page.goto(`/?${params}`);
  const table = page.getByTestId("pivot-table");
  await expect(table.locator("tbody td")).toHaveText(["local", "2", "120"]);
  await expect(table.locator("thead")).toContainText("Arm · Assignment");
  expect(queries.at(-1)?.dimensions).toEqual(["activity.arm", "assignment.arm"]);
  await page.locator('[data-catalog-dimension="assignment.active"]').click();
  await expect(table.locator("tbody td")).toHaveText(["local", "2", "true", "120"]);
});

test("joined numeric and boolean filters send typed values in Explore and Pivot", async ({ page }) => {
  const queries = await mockCatalog(page);
  await page.goto("/?view=explore&model=activity&metric=activity.revenue");
  for (const [label, value] of [["Arm · Assignment", "2"], ["Active · Assignment", "true"]]) {
    await page.getByRole("button", { name: "+ Filter", exact: true }).click();
    await page.getByRole("menuitem", { name: label, exact: true }).click();
    await page.getByRole("dialog").getByLabel(value, { exact: true }).check();
    await page.getByRole("dialog").getByRole("button", { name: "Done", exact: true }).click();
  }
  await expect.poll(() => queries.some((q) => q.metrics?.includes("activity.revenue") && q.filters?.includes("assignment.arm = 2") && q.filters.includes("assignment.active = true"))).toBe(true);
  queries.length = 0;
  await page.getByRole("tab", { name: "Pivot", exact: true }).click();
  await expect.poll(() => queries.some((q) => q.metrics?.includes("activity.revenue") && q.filters?.includes("assignment.arm = 2") && q.filters.includes("assignment.active = true"))).toBe(true);
});

test("reopening a joined filter pill keeps the primary model date range", async ({ page }) => {
  const queries = await mockCatalog(page, true);
  const params = new URLSearchParams({
    view: "explore", model: "activity", metric: "activity.revenue",
    from: "2024-01-01", to: "2024-02-01",
    filters: JSON.stringify({ "assignment.arm": ["2"] }),
  });
  await page.goto(`/?${params}`);
  await page.locator('span[data-dimension="assignment.arm"][data-mode] button').first().click();
  await expect(page.getByRole("dialog").getByRole("checkbox")).toBeVisible();
  const distinct = queries.findLast((q) => !q.metrics?.length && q.dimensions?.includes("assignment.arm"));
  expect(distinct?.filters?.some((filter) => filter.includes("activity.created_at"))).toBe(true);
  expect(distinct?.filters?.some((filter) => filter.includes("assignment.assigned_at"))).toBe(false);
});
