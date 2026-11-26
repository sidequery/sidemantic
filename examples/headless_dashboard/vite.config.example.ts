// Example Vite config: regenerate sqlctx types automatically.
//
// With this plugin the "build step" is invisible — `sqlctx.client.generated.ts` and
// `sqlctx.queries.generated.ts` are rewritten on dev-server start and whenever
// `models.yml` (or a scanned source) changes. You import the real generated files, so the
// editor / `tsc` get full `as const` autocomplete with no manual `gen types` command.
//
//   import { schema } from "./sqlctx.client.generated";
//   import type { GeneratedQueries } from "./sqlctx.queries.generated";
//   import { createClient, createSqlClient } from "sqlctx-wasm/client";

import { defineConfig } from "vite";
import { sqlctx } from "sqlctx-wasm/vite";

export default defineConfig({
  plugins: [
    sqlctx({
      models: "models.yml",
      output: "sqlctx.client.generated.ts",
      sql: {
        sources: ["queries.ts"],
        output: "sqlctx.queries.generated.ts",
      },
    }),
  ],
});
