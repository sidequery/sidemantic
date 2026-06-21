// Example Vite config: regenerate Sidemantic types automatically.
//
// With this plugin the "build step" is invisible — `sidemantic.client.generated.ts` and
// `sidemantic.queries.generated.ts` are rewritten on dev-server start and whenever
// `models.yml` (or a scanned source) changes. You import the real generated files, so the
// editor / `tsc` get full `as const` autocomplete with no manual `gen types` command.
//
//   import { schema } from "./sidemantic.client.generated";
//   import type { GeneratedQueries } from "./sidemantic.queries.generated";
//   import { createClient, createSqlClient } from "sidemantic-wasm/client";

import { defineConfig } from "vite";
import { sidemantic } from "sidemantic-wasm/vite";

export default defineConfig({
  plugins: [
    sidemantic({
      models: "models.yml",
      output: "sidemantic.client.generated.ts",
      sql: {
        sources: ["queries.ts"],
        output: "sidemantic.queries.generated.ts",
      },
    }),
  ],
});
