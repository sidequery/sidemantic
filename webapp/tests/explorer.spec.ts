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
  // A per-dimension filter pill now summarizes the include selection ("Country is CA").
  const countryPill = page.locator('span[data-dimension="customers.country"][data-mode]');
  await expect(countryPill).toBeVisible();
  await expect(countryPill).toContainText("CA");
  await expect.poll(async () => number(await kpi.textContent())).toBeLessThan(baseline);

  // The country leaderboard still lists every country (its own filter is excluded).
  await expect(page.locator('button[data-dimension="customers.country"][data-value="JP"]')).toBeVisible();

  // Remove the filter -> KPI restores.
  await page.getByRole("button", { name: "Clear" }).click();
  await expect(countryPill).toHaveCount(0);
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

test("filter editor: open, search, exclude a value, persist to URL, and restore on reload", async ({ page }) => {
  await page.goto("/?model=customers&metric=customers.customer_count");

  const kpi = page.locator('button[data-metric="customers.customer_count"]');
  await expect.poll(async () => number(await kpi.textContent())).toBeGreaterThan(0);
  const baseline = number(await kpi.textContent());

  // Open the editor from the "+ Filter" affordance and pick the country dimension.
  await page.getByRole("button", { name: "+ Filter" }).click();
  await page.getByRole("menuitem", { name: "Country" }).click();
  const editor = page.getByRole("dialog");
  await expect(editor).toBeVisible();

  // Switch to Exclude mode, search to narrow the distinct list (server-side ILIKE), then check
  // the CA value off. The list is debounced, so wait for it to narrow to a single row.
  await editor.getByRole("button", { name: "Exclude" }).click();
  await editor.getByRole("textbox").fill("CA");
  await expect(editor.getByRole("checkbox")).toHaveCount(1);
  const caRow = editor.locator("label", { hasText: "CA" }).getByRole("checkbox");
  await caRow.check();

  // The exclude filter drops those rows, so the KPI recomputes downward.
  await expect.poll(async () => number(await kpi.textContent())).toBeLessThan(baseline);

  // The filter is serialized to the URL in the new object form (mode=exclude).
  await expect
    .poll(() => decodeURIComponent(new URL(page.url()).searchParams.get("filters") ?? ""))
    .toContain('"mode":"exclude"');

  // Escape closes the popover; the pill summarizes the exclude filter.
  await page.keyboard.press("Escape");
  await expect(editor).toHaveCount(0);
  const pill = page.locator('span[data-dimension="customers.country"][data-mode="exclude"]');
  await expect(pill).toContainText("is not");

  // A full reload restores the exclude filter from the URL (deep-linkable state contract).
  const url = page.url();
  await page.goto(url);
  await expect(page.locator('span[data-dimension="customers.country"][data-mode="exclude"]')).toContainText("is not");
  await expect.poll(async () => number(await kpi.textContent())).toBeLessThan(baseline);
});

test("pivot view renders a grouped table", async ({ page }) => {
  await page.goto("/?view=pivot&model=orders&pdims=%5B%22orders.status%22%5D&pmetrics=%5B%22orders.revenue%22%5D");
  const table = page.locator('[data-testid="pivot-table"]');
  await expect(table).toBeVisible();
  await expect(table.locator("tbody tr")).not.toHaveCount(0);
  await expect(table).toContainText("Revenue");
});
