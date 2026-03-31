import { defineConfig } from "vite";
import { viteSingleFile } from "vite-plugin-singlefile";
import path from "path";

const entry = process.env.ENTRY || "chart";

export default defineConfig({
  plugins: [viteSingleFile()],
  build: {
    rollupOptions: { input: `${entry}.html` },
    outDir: "../",
    emptyOutDir: false,
  },
  resolve: {
    alias: {
      // Use CSP-safe expression interpreter (chart widget)
      "vega-functions/codegenExpression": "vega-interpreter",
    },
  },
});
