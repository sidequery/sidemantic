import { expect, test } from "@playwright/test";

// Proves the explorer drives real query changes (not fake status text). Requires a running backend
// + the Vite dev server against the bundled ecommerce model (default model: customers).

function number(text: string | null): number {
  return Number((text ?? "").replace(/[^0-9.-]/g, ""));
}

test("the home index lists models and opens one into Explore", async ({ page }) => {
  await page.goto("/");
  const cards = page.locator('[data-testid="explore-card"]');
  await expect(cards.first()).toBeVisible();
  await expect(cards).not.toHaveCount(0);

  // Opening a model card enters its Explore view.
  await page.locator('[data-testid="explore-card"][data-model="customers"]').click();
  await expect.poll(() => new URL(page.url()).searchParams.get("view")).toBe("explore");
  await expect(page.locator('button[data-metric="customers.customer_count"]')).toBeVisible();
});

test("crossfilter, reset, and metric re-rank change rendered data", async ({ page }) => {
  const params = new URLSearchParams({
    view: "explore",
    model: "customers",
    pmetrics: JSON.stringify(["customers.customer_count", "repeat_customer_rate", "cancellation_rate"]),
  });
  await page.goto(`/?${params}`);

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

  // The shared bottom preview uses the same active filters as Explore/Pivot.
  await page.getByRole("button", { name: "Rows preview" }).click();
  const preview = page.locator('[data-testid="row-preview-drawer"]');
  await expect(preview.getByRole("alert")).toHaveCount(0);
  const previewTable = preview.locator('[data-testid="pivot-table"]');
  await expect(previewTable.locator("tbody tr").first()).toBeVisible();
  const headers = await previewTable.locator("thead th").allTextContents();
  const countryColumn = headers.findIndex((header) => header.includes("Country"));
  expect(countryColumn).toBeGreaterThanOrEqual(0);
  await expect(previewTable.locator(`tbody td:nth-child(${countryColumn + 1})`).first()).toHaveText("CA");

  // The country leaderboard still lists every country (its own filter is excluded).
  await expect(page.locator('button[data-dimension="customers.country"][data-value="JP"]')).toBeVisible();

  // Remove the filter -> KPI restores.
  await page.getByRole("button", { name: "Clear" }).click();
  await expect(countryPill).toHaveCount(0);
  await expect.poll(async () => number(await kpi.textContent())).toBe(baseline);
  await expect.poll(async () => new Set(await previewTable.locator(`tbody td:nth-child(${countryColumn + 1})`).allTextContents()).size).toBeGreaterThan(1);

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
  const resetZoom = page.getByRole("button", { name: "Reset zoom" });
  await expect(resetZoom).toBeVisible();
  expect((await resetZoom.boundingBox())?.height).toBeLessThanOrEqual(26);
  await resetZoom.click();
  await expect.poll(() => new URL(page.url()).searchParams.get("from")).toBeNull();
  await expect(resetZoom).toHaveCount(0);

  const rangeControl = page.locator("details").filter({ hasText: "Range" });
  await rangeControl.locator("summary").click();
  await expect(rangeControl.getByRole("button", { name: "Last 28 days" })).toBeVisible();
  await rangeControl.getByRole("button", { name: "Last 28 days" }).click();
  await expect.poll(() => new URL(page.url()).searchParams.get("from")).not.toBeNull();
  await rangeControl.locator("summary").click();
  await rangeControl.getByRole("button", { name: "All time" }).click();
  await expect.poll(() => new URL(page.url()).searchParams.get("from")).toBeNull();
});

test("filter editor: open, search, exclude a value, persist to URL, and restore on reload", async ({ page }) => {
  await page.goto("/?view=explore&model=customers&metric=customers.customer_count");

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
  await page.goto("/?view=explore&model=orders&metric=orders.revenue");
  await page.getByRole("button", { name: "Collapse sidebar" }).click();
  await expect(page.locator("aside")).toBeHidden();
  await page.getByRole("tab", { name: "Pivot" }).click();
  await expect(page.locator("aside")).toBeVisible();

  await page.goto("/?view=pivot&model=orders&pdims=%5B%22orders.status%22%5D&pmetrics=%5B%22orders.revenue%22%5D");
  await expect(page.locator('[data-catalog-dimension="orders.status"]')).toHaveAttribute("data-selected", "true");
  await expect(page.locator('[data-catalog-metric="orders.revenue"]')).toHaveAttribute("data-selected", "true");
  await expect(page.getByText("Raw rows (ungrouped, first 50)")).toHaveCount(0);
  await expect(page.getByRole("button", { name: "Rows preview" })).toBeVisible();
  await page.locator('[data-catalog-dimension="orders.payment_method"]').click();
  await page.locator('[data-catalog-metric="orders.order_count"]').click();
  const table = page.locator('[data-testid="pivot-table"]');
  await expect(table).toBeVisible();
  await expect(table.locator("tbody tr")).not.toHaveCount(0);
  await expect(table).toContainText("Revenue");
  await expect(table).toContainText("Order Count");
  await expect(table).toContainText("Payment Method");
});

test("pivot table paginates large grouped results and SQL is highlighted", async ({ page }) => {
  const params = new URLSearchParams({
    view: "pivot",
    model: "orders",
    pdims: JSON.stringify(["orders.created_at"]),
    pmetrics: JSON.stringify(["orders.revenue"]),
  });
  await page.goto(`/?${params}`);

  const pager = page.locator('[data-testid="pivot-table-pager"]');
  await expect(pager).toBeVisible();
  await expect(pager).toContainText("of");
  const firstCell = page.locator('[data-testid="pivot-table"] tbody tr').first().locator("td").first();
  const firstPageValue = await firstCell.textContent();
  await pager.getByRole("button", { name: "Next" }).click();
  await expect(firstCell).not.toHaveText(firstPageValue ?? "");

  await page.getByText("Generated SQL", { exact: true }).click();
  await expect(page.locator('[data-testid="query-debug"] [data-token="keyword"]').first()).toBeVisible();
});

test("component gallery exposes the WASM-style leaderboard and full-width expanded table", async ({ page }) => {
  await page.goto("/components");
  const analyticalCells = page.locator('[data-testid="pivot-table"] tbody tr').first().locator("td");
  await expect(analyticalCells).toHaveCount(3);
  await expect(analyticalCells.last()).toHaveCSS("border-bottom-style", "solid");
  const leaderboards = page.locator('[data-testid="dimension-leaderboard"]');
  await expect(leaderboards).toHaveCount(4);
  await expect(leaderboards.first()).toHaveCSS("border-radius", "0px");
  await expect(page.locator('[data-testid="gallery-filter-pills"] [data-dimension]')).toHaveCount(3);
  await expect(page.getByRole("button", { name: "Remove filter East" })).toBeVisible();
  await expect(page.getByRole("img", { name: "Eight month revenue trend" })).toBeVisible();
  await expect(page.getByRole("img", { name: "Eight month revenue trend" })).toHaveAttribute("preserveAspectRatio", "none");
  await expect(page.getByText("Column chart", { exact: true })).toBeVisible();
  await expect(page.getByText("Dashboard shell", { exact: true })).toBeVisible();

  await page.getByRole("button", { name: "Expand table (8)" }).click();
  await expect(leaderboards).toHaveCount(1);
  const expandedRows = page.locator('[data-testid="dimension-leaderboard"][data-expanded="true"] .leaderboard-row');
  await expect(expandedRows).toHaveCount(8);
  await expect(expandedRows.first()).toHaveCSS("display", "grid");
  await page.getByRole("button", { name: "← All dimensions" }).click();
  await expect(leaderboards).toHaveCount(4);
});
