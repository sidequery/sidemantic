import { defineConfig } from "vite";
import { viteSingleFile } from "vite-plugin-singlefile";

export default defineConfig({
  plugins: [viteSingleFile()],
  build: {
    rollupOptions: { input: "chart.html" },
    outDir: "../",
    emptyOutDir: false,
  },
  resolve: {
    alias: {
      // Use the CSP-safe expression interpreter instead of generated functions.
      "vega-functions/codegenExpression": "vega-interpreter",
    },
  },
});
