import { expect, test } from "@playwright/test";

// Proves the explorer drives real query changes (not fake status text). Requires a running backend
// + the Vite dev server against the bundled ecommerce model (default model: customers).

function number(text: string | null): number {
  return Number((text ?? "").replace(/[^0-9.-]/g, ""));
}

test("crossfilter, reset, and metric re-rank change rendered data", async ({ page }) => {
  await page.goto("/");

  const kpi = page.locator('button[data-metric="customers.customer_count"]');
  await expect(kpi).toBeVisible();
  // Wait for the totals query to resolve (card shows a skeleton until then).
  await expect.poll(async () => number(await kpi.textContent())).toBeGreaterThan(0);
  const baseline = number(await kpi.textContent());

  // Click a country leaderboard row -> crossfilter. KPI must recompute downward.
  await page.locator('button[data-dimension="customers.country"][data-value="CA"]').click();
  await expect(page.locator('span[data-dimension="customers.country"][data-value="CA"]')).toBeVisible();
  await expect.poll(async () => number(await kpi.textContent())).toBeLessThan(baseline);

  // The country leaderboard still lists every country (its own filter is excluded).
  await expect(page.locator('button[data-dimension="customers.country"][data-value="JP"]')).toBeVisible();

  // Remove the filter -> KPI restores.
  await page.getByRole("button", { name: "Clear" }).click();
  await expect(page.locator('span[data-dimension="customers.country"][data-value="CA"]')).toHaveCount(0);
  await expect.poll(async () => number(await kpi.textContent())).toBe(baseline);

  // Re-rank leaderboards by a different metric.
  await page.locator('button[data-metric="customers.active_customer_count"]').click();
  await expect(page.locator('[data-testid="dimension-leaderboard"]').first()).toContainText("Active Customer Count");
});

test("brush-to-zoom on the chart sets a date range and shows the comparison overlay", async ({ page }) => {
  await page.goto("/?view=explore&model=orders&metric=orders.revenue&grain=month");
  const chart = page.locator('svg[height="280"]');
  await expect(chart).toBeVisible();
  const box = await chart.boundingBox();
  if (!box) throw new Error("chart has no bounding box");

  const y = box.y + box.height / 2;
  await page.mouse.move(box.x + box.width * 0.3, y);
  await page.mouse.down();
  await page.mouse.move(box.x + box.width * 0.45, y);
  await page.mouse.move(box.x + box.width * 0.6, y);
  await page.mouse.up();

  await expect.poll(() => new URL(page.url()).searchParams.get("from")).not.toBeNull();
  await expect(page.getByText("Prev period")).toBeVisible();
});

test("pivot view renders a grouped table", async ({ page }) => {
  await page.goto("/?view=pivot&model=orders&pdims=%5B%22orders.status%22%5D&pmetrics=%5B%22orders.revenue%22%5D");
  const table = page.locator('[data-testid="pivot-table"]');
  await expect(table).toBeVisible();
  await expect(table.locator("tbody tr")).not.toHaveCount(0);
  await expect(table).toContainText("Revenue");
});
