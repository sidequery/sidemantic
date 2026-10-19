import { expect, test, type Locator, type Page } from "@playwright/test";

const dimensions = ["status", "country", "device", "platform", "campaign", "channel"];
const prefix = "A shared campaign prefix with enough detail to span several lines ".repeat(3);
const values = [`${prefix}first ending`, `${prefix}second ending`, "x".repeat(180)];

async function mockCatalog(page: Page, catalogDimensions = dimensions) {
  await page.route("**/*", async (route) => {
    const path = new URL(route.request().url()).pathname;
    const json = (body: unknown, status = 200) => route.fulfill({ status, json: body });
    if (path === "/health") return json({ status: "ok" });
    if (path === "/describe") return json({
      models: [{
        name: "activity",
        dimensions: catalogDimensions.map((name) => ({ name, type: "categorical" })),
        metrics: [{ name: "revenue", agg: "sum" }],
      }],
    });
    if (path === "/graph") return json({ joinable_pairs: [] });
    if (path === "/dashboard") return json({ detail: "No dashboard configured" }, 404);
    if (path === "/query") {
      const query = route.request().postDataJSON();
      const rows = values.map((value) => Object.fromEntries([
        ...(query.dimensions ?? []).map((ref: string) => [ref.split(".").at(-1), value]),
        ...(query.metrics ?? []).map((ref: string) => [ref.split(".").at(-1), 100]),
      ]));
      return json({ rows, row_count: rows.length, sql: "select 1" });
    }
    return route.continue();
  });
}

async function expectInsideViewport(panel: Locator) {
  await expect.poll(() => panel.evaluate((element) => {
    const box = element.getBoundingClientRect();
    return { left: box.left >= 0, right: box.right <= innerWidth, top: box.top >= 0, bottom: box.bottom <= innerHeight };
  })).toEqual({ left: true, right: true, top: true, bottom: true });
  await expect.poll(() => panel.evaluate((element) => element.scrollWidth <= element.clientWidth)).toBe(true);
}

async function expectHit(target: Locator) {
  await target.scrollIntoViewIfNeeded();
  await expect.poll(() => target.evaluate((element) => {
    const box = element.getBoundingClientRect();
    return element.contains(document.elementFromPoint(box.x + box.width / 2, box.y + box.height / 2));
  })).toBe(true);
}

for (const width of [1280, 390, 320]) {
  test(`filter menu and editor remain usable at ${width}px`, async ({ page }) => {
    await page.setViewportSize({ width, height: 760 });
    await mockCatalog(page);
    await page.goto("/?view=explore&model=activity&metric=activity.revenue");
    await page.getByRole("button", { name: "+ Filter" }).click();
    const menu = page.getByRole("menu");
    await expectInsideViewport(menu);
    const channel = menu.getByRole("menuitem", { name: "Channel", exact: true });
    await expectHit(channel);
    await channel.click();

    const editor = page.getByRole("dialog", { name: /^Channel/ });
    await expectInsideViewport(editor);
    await editor.getByRole("button", { name: "Exclude", exact: true }).click();
    const first = editor.getByRole("checkbox", { name: values[0], exact: true });
    await first.check();
    await expect(first).toBeChecked();
    await expectInsideViewport(editor);
    const second = editor.getByRole("checkbox", { name: values[1], exact: true });
    await second.check();
    await expect(second).toBeChecked();
    // Both suffixes are rendered as wrapped text, not hidden by ellipsis.
    for (const value of values) {
      const text = editor.locator("label span").filter({ hasText: value });
      await expect(text).toHaveCSS("white-space", "normal");
      expect(await text.evaluate((element) => element.scrollWidth <= element.clientWidth)).toBe(true);
    }
    await expectHit(editor.getByRole("button", { name: "Done", exact: true }));
    await editor.getByRole("button", { name: "Done", exact: true }).click();
    const pill = page.getByRole("button", { name: "Edit filter Channel", exact: true });
    await pill.click();
    await expectInsideViewport(editor);
    await editor.getByRole("button", { name: "Clear", exact: true }).click();
    await expect(pill).toHaveCount(0);
  });
}

test("crowded pills wrap and right-edge popovers follow viewport resizing", async ({ page }) => {
  await page.setViewportSize({ width: 900, height: 640 });
  await mockCatalog(page);
  const filters = Object.fromEntries(dimensions.map((dim) => [`activity.${dim}`, { mode: "include", values: ["Selected campaign value"] }]));
  const params = new URLSearchParams({ view: "explore", model: "activity", metric: "activity.revenue", filters: JSON.stringify(filters) });
  await page.goto(`/?${params}`);
  const pills = page.getByRole("button", { name: /^Edit filter / });
  await expect(pills).toHaveCount(dimensions.length);
  const positions = await pills.evaluateAll((elements) => elements.map((element) => element.getBoundingClientRect().y));
  expect(new Set(positions).size).toBeGreaterThan(1);
  const rightmost = await pills.evaluateAll((elements) => elements.reduce((index, element, next) => element.getBoundingClientRect().right > elements[index].getBoundingClientRect().right ? next : index, 0));
  await pills.nth(rightmost).click();
  const editor = page.getByRole("dialog");
  await expectInsideViewport(editor);
  await page.setViewportSize({ width: 320, height: 640 });
  await expectInsideViewport(editor);
  await expectHit(editor.getByRole("button", { name: "Done", exact: true }));
  await editor.getByRole("button", { name: "Done", exact: true }).click();
  await page.setViewportSize({ width: 320, height: 480 });
  await page.getByRole("button", { name: "+ Filter" }).click();
  await expectInsideViewport(page.getByRole("menu"));
  await page.getByRole("menuitem", { name: "Channel", exact: true }).click();
  await expectInsideViewport(editor);
  await editor.getByRole("button", { name: "Done", exact: true }).click();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
});

test("many active filters scroll without displacing the stage or clipping popovers", async ({ page }) => {
  await page.setViewportSize({ width: 320, height: 480 });
  const manyDimensions = Array.from({ length: 24 }, (_, index) => `dimension_${index}`);
  await mockCatalog(page, manyDimensions);
  const filters = Object.fromEntries(manyDimensions.map((dim) => [`activity.${dim}`, { mode: "include", values: ["Selected campaign value"] }]));
  const params = new URLSearchParams({ view: "explore", model: "activity", metric: "activity.revenue", filters: JSON.stringify(filters) });
  await page.goto(`/?${params}`);
  const pills = page.getByRole("button", { name: /^Edit filter / });
  await expect(pills).toHaveCount(24);
  expect((await page.locator("main").boundingBox())!.height).toBeGreaterThan(150);
  const lastPill = pills.last();
  await lastPill.click();
  const editor = page.getByRole("dialog");
  await expectInsideViewport(editor);
  await editor.getByRole("checkbox", { name: values[1], exact: true }).check();
  const strip = page.getByRole("region", { name: "Active filters" });
  expect(await strip.evaluate((element) => element.scrollTop)).toBeGreaterThan(0);
  // Scrolling the chip out of view keeps the editor accessible at the visible strip edge.
  await strip.evaluate((element) => { element.scrollTop = 0; });
  await expectInsideViewport(editor);
  await expect.poll(async () => (await editor.boundingBox())!.y).toBeLessThanOrEqual((await strip.boundingBox())!.y + (await strip.boundingBox())!.height + 4);
  await editor.getByRole("button", { name: "Done", exact: true }).focus();
  await page.keyboard.press("Tab");
  await expect(editor.getByRole("button", { name: "Close filter editor", exact: true })).toBeFocused();
  await expectHit(editor.getByRole("button", { name: "Done", exact: true }));
  await editor.getByRole("button", { name: "Done", exact: true }).click();
  await expect(lastPill).toBeFocused();
  expect(await strip.evaluate((element) => element.scrollTop)).toBe(0);
  const add = page.getByRole("button", { name: "+ Filter" });
  await add.click();
  const menu = page.getByRole("menu");
  await expectInsideViewport(menu);
  await menu.getByRole("menuitem").last().click();
  await expectInsideViewport(editor);
  await editor.getByRole("button", { name: "Exclude", exact: true }).click();
  await page.keyboard.press("Escape");
  await expect(editor).toHaveCount(0);
  await expect(add).toBeFocused();
  await add.click();
  await page.getByRole("tab", { name: "Explore", exact: true }).click();
  await expect(menu).toHaveCount(0);
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
});
