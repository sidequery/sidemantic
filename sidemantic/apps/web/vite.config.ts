import { defineConfig } from "vite";
import { viteSingleFile } from "vite-plugin-singlefile";

export default defineConfig({
  plugins: [viteSingleFile()],
  build: {
    rollupOptions: { input: "chart.html" },
    outDir: "../",
    emptyOutDir: false,
  },
  define: {
    // Replace new Function calls with a safe fallback at build time
    // This prevents CSP violations in MCP Apps sandboxes
  },
  resolve: {
    alias: {
      // Use CSP-safe expression interpreter
      "vega-functions/codegenExpression": "vega-interpreter",
    },
  },
});
