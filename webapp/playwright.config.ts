import { defineConfig, devices } from "@playwright/test";

// Smoke tests assume a backend (sidemantic api-serve / sidemantic-server) AND the Vite dev server
// are already running. Start them first:
//   uv run --extra dev sidemantic api-serve examples/ecommerce/models \
//     --db examples/ecommerce/data/ecommerce.db --port 4400
//   bun run dev
// Override the UI URL with BASE_URL=... when needed.
export default defineConfig({
  testDir: "./tests",
  // The dev backend is single-threaded (serialized by a lock), so parallel workers just contend
  // and starve each other. One worker keeps the data-change assertions deterministic.
  workers: 1,
  fullyParallel: false,
  reporter: "list",
  expect: { timeout: 10_000 },
  use: {
    baseURL: process.env.BASE_URL ?? "http://localhost:4321",
    trace: "on-first-retry",
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
});
