import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { copyFileSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { expect, test } from "@playwright/test";

const PORT = 4455;
const DASHBOARD_URL = `http://127.0.0.1:${PORT}`;
let server: ChildProcessWithoutNullStreams;
let serverOutput = "";
let dashboardDatabaseDirectory = "";

test.beforeAll(async () => {
  const repository = path.resolve("..");
  dashboardDatabaseDirectory = mkdtempSync(path.join(tmpdir(), "sidemantic-dashboard-e2e-"));
  const dashboardDatabase = path.join(dashboardDatabaseDirectory, "ecommerce.db");
  copyFileSync(path.join(repository, "examples/ecommerce/data/ecommerce.db"), dashboardDatabase);
  server = spawn(
    "uv",
    [
      "run",
      "sidemantic",
      "dashboard",
      "serve",
      "webapp/tests/fixtures/dashboard.yml",
      "--models",
      "examples/ecommerce/models",
      "--db",
      dashboardDatabase,
      "--host",
      "127.0.0.1",
      "--port",
      String(PORT),
    ],
    { cwd: repository, stdio: "pipe" },
  );
  server.stdout.on("data", (chunk) => { serverOutput += chunk.toString(); });
  server.stderr.on("data", (chunk) => { serverOutput += chunk.toString(); });

  await expect.poll(async () => {
    if (server.exitCode != null) throw new Error(`dashboard serve exited ${server.exitCode}:\n${serverOutput}`);
    try {
      return (await fetch(`${DASHBOARD_URL}/readyz`)).ok;
    } catch {
      return false;
    }
  }, { timeout: 30_000, message: `dashboard serve did not become ready:\n${serverOutput}` }).toBe(true);
});

test.afterAll(() => {
  if (server?.exitCode == null) server.kill("SIGTERM");
  if (dashboardDatabaseDirectory) rmSync(dashboardDatabaseDirectory, { recursive: true, force: true });
});

test("dashboard serve renders the document title, tabs, and every chart", async ({ page }) => {
  await page.goto(DASHBOARD_URL);

  await expect(page).toHaveTitle("Revenue Operations");
  await expect(page.getByTestId("dashboard-document")).toBeVisible();
  await expect(page.getByRole("heading", { name: "Revenue Operations" })).toBeVisible();
  await expect(page.getByRole("tab", { name: "Overview" })).toHaveAttribute("aria-selected", "true");
  await expect(page.getByRole("tab", { name: "Customers" })).toBeVisible();

  const charts = page.getByTestId("dashboard-chart");
  await expect(charts).toHaveCount(2);
  await expect(page.locator('[data-chart-id="revenue_trend"]')).toContainText("Monthly Revenue");
  await expect(page.locator('[data-chart-id="orders_by_status"]')).toContainText("Orders by Status");

  const grid = page.getByTestId("dashboard-chart-grid");
  const desktopColumns = await grid.evaluate((node) => getComputedStyle(node).gridTemplateColumns.split(" ").length);
  expect(desktopColumns).toBe(2);

  await page.getByRole("tab", { name: "Customers" }).click();
  await expect(page).toHaveURL(/tab=customers/);
  await expect(page.locator('[data-chart-id="customers_by_country"]')).toBeVisible();
  await expect(page.locator('[data-chart-id="customers_by_tier"]')).toBeVisible();

  await page.setViewportSize({ width: 600, height: 900 });
  const mobileColumns = await grid.evaluate((node) => getComputedStyle(node).gridTemplateColumns.split(" ").length);
  expect(mobileColumns).toBe(1);
});

test("drill, explore, local saved views, CSV, and share URL behavior are explicit", async ({ page }) => {
  await page.goto(DASHBOARD_URL);
  const statusChart = page.locator('[data-chart-id="orders_by_status"]');
  const firstBar = statusChart.getByRole("button", { name: /Filter to/ }).first();
  await expect(firstBar).toBeVisible();
  await firstBar.click();

  await expect(page.getByTestId("chart-details-orders_by_status")).toBeVisible();
  await expect(page).toHaveURL(/dashboard_filters=/);
  await expect(page.getByRole("button", { name: /Remove filter Orders Status/ })).toBeVisible();

  const explore = statusChart.getByRole("link", { name: "Explore from here" });
  await expect(explore).toHaveAttribute("href", /\/explore\?view=explore&model=orders&metric=orders.revenue|\/explore\?view=explore&model=orders&metric=orders.order_count/);
  await expect(statusChart.getByRole("link", { name: "Export CSV" })).toHaveAttribute("download", "orders_by_status.csv");
  await expect(page.getByRole("button", { name: "Copy share URL" })).toHaveAttribute(
    "title",
    /active tab and filters.*saved views are not shared/i,
  );

  await page.getByText("Saved views", { exact: true }).click();
  await page.getByRole("textbox", { name: "Saved view name" }).fill("My status slice");
  await page.getByRole("button", { name: "Save", exact: true }).click();
  await expect(page.getByRole("button", { name: "My status slice", exact: true })).toBeVisible();
  expect(await page.evaluate(() => Object.keys(localStorage).some((key) => key.startsWith("sidemantic-dashboard-views:")))).toBe(true);
});
