import { defineConfig, devices } from "@playwright/test";

// Controlled HTTP responses exercise real browser layout without a native backend build.
export default defineConfig({
  testDir: "./tests",
  testMatch: "filter-popovers.spec.ts",
  workers: 1,
  reporter: "list",
  use: { baseURL: "http://127.0.0.1:4548", trace: "retain-on-failure" },
  webServer: {
    command: "bun run dev --host 127.0.0.1 --port 4548",
    url: "http://127.0.0.1:4548",
    reuseExistingServer: false,
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
});
