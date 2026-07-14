import path from "node:path";
import { readFileSync } from "node:fs";
import { expect, test } from "@playwright/test";

test("AnyWidget renders its canonical loading state and a clearable brush pill", async ({ page }) => {
  await page.goto("/components");
  await page.addStyleTag({ path: path.resolve("../sidemantic/widget/static/widget.css") });
  const widgetModuleUrl = `data:text/javascript;base64,${readFileSync(path.resolve("../sidemantic/widget/static/widget.js")).toString("base64")}`;

  await page.evaluate(async (moduleUrl) => {
    const { default: widget } = await import(moduleUrl);
    const values: Record<string, unknown> = {
      filters: {},
      metrics_config: [{ key: "orders.revenue", label: "Revenue", format: "currency" }],
      dimensions_config: [],
      selected_metric: "orders.revenue",
      date_range: ["2024-01-01", "2024-12-31"],
      brush_selection: ["2024-03-01", "2024-03-31"],
      metric_series_data: "",
      metric_totals: {},
      transport: "base64",
      status: "loading",
      time_grain_options: ["day"],
      time_grain: "day",
    };
    const listeners = new Map<string, Set<() => void>>();
    const model = {
      get: (key: string) => values[key],
      set: (key: string, value: unknown) => {
        values[key] = value;
        for (const listener of listeners.get(`change:${key}`) ?? []) listener();
      },
      save_changes: () => undefined,
      on: (event: string, listener: () => void) => {
        const set = listeners.get(event) ?? new Set();
        set.add(listener);
        listeners.set(event, set);
      },
      off: (event: string, listener?: () => void) => {
        if (listener) listeners.get(event)?.delete(listener);
        else listeners.delete(event);
      },
    };
    const host = document.createElement("div");
    host.id = "widget-test-host";
    document.body.replaceChildren(host);
    widget.render({ model, el: host });
  }, widgetModuleUrl);

  await expect(page.getByText("Loading metrics…", { exact: true })).toBeVisible();
  await expect(page.locator(".metrics-col [data-state=loading]")).toHaveCSS("display", "grid");
  await expect(page.getByText("Date:", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: /Remove filter/ }).click();
  await expect(page.getByText("Date:", { exact: true })).toHaveCount(0);
  await expect(page.getByText("No filters", { exact: true })).toBeVisible();
});
