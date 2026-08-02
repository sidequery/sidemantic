import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./tests",
  // The dev backend is single-threaded (serialized by a lock), so parallel workers just contend
  // and starve each other. One worker keeps the data-change assertions deterministic.
  workers: 1,
  fullyParallel: false,
  reporter: "list",
  expect: { timeout: 10_000 },
  use: {
    baseURL: process.env.BASE_URL ?? "http://127.0.0.1:4327",
    trace: "on-first-retry",
  },
  webServer: [
    {
      command: "uv run --project .. sidemantic api-serve ../examples/ecommerce/models --db ../examples/ecommerce/data/ecommerce.db --port 4460",
      url: "http://127.0.0.1:4460/readyz",
      reuseExistingServer: false,
      timeout: 30_000,
    },
    {
      command: "SIDEMANTIC_API=http://127.0.0.1:4460 bun run dev --host 127.0.0.1 --port 4327",
      url: "http://127.0.0.1:4327",
      reuseExistingServer: false,
      timeout: 30_000,
    },
  ],
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
});
