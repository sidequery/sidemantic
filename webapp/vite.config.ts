import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// The semantic-layer HTTP endpoints both backends expose identically.
// In dev we proxy them to a running backend so the SPA is same-origin (no CORS) with HMR.
const API_PATHS = ["/query", "/compile", "/models", "/graph", "/health", "/readyz", "/sql", "/raw"];

// Target a locally running `sidemantic api-serve` (Python, default :4400) or the Rust
// `sidemantic-server`. Override with SIDEMANTIC_API=http://host:port.
const apiTarget = process.env.SIDEMANTIC_API ?? "http://127.0.0.1:4400";

export default defineConfig({
  // Relative base is REQUIRED so the built bundle works when embedded under any sub-path
  // (e.g. served at "/" or "/ui" by either backend).
  base: "./",
  plugins: [react()],
  server: {
    port: 4321,
    strictPort: true,
    proxy: Object.fromEntries(
      API_PATHS.map((path) => [path, { target: apiTarget, changeOrigin: true }]),
    ),
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
    sourcemap: false,
  },
});
