// Vite plugin: regenerate the typed-client schema (and optional sqlx query types) from a
// semantic-layer YAML whenever it changes, so the "build step" is invisible — types stay
// fresh on dev-server start and on edits, with no manual `gen types` command.
//
// It writes a REAL .ts file (not a virtual module) on purpose: TypeScript's checker does not
// run Vite's resolver, so only an on-disk file gives the editor/tsc the `as const` literal
// types. The plugin just automates writing it.
//
//   // vite.config.ts
//   import { sidemantic } from "sidemantic-wasm/vite";
//   export default { plugins: [sidemantic({ models: "models.yml", output: "src/sidemantic.generated.ts" })] };
//
//   // app.ts
//   import { schema } from "./sidemantic.generated";        // full autocomplete, always fresh
//   import { createClient } from "sidemantic-wasm/client";

import { existsSync, readFileSync, readdirSync, statSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { extractSqlLiterals, generateClientSchema, generateSqlTypes } from "./codegen.js";

function listTsSources(sources, exclude) {
  const files = [];
  const visit = (entry) => {
    const path = resolve(entry);
    if (!existsSync(path)) return;
    if (statSync(path).isDirectory()) {
      for (const child of readdirSync(path)) visit(join(path, child));
    } else if ((path.endsWith(".ts") || path.endsWith(".tsx")) && !path.endsWith(".d.ts") && !exclude.has(path)) {
      files.push(path);
    }
  };
  for (const source of sources) visit(source);
  return files;
}

/**
 * @param {object} options
 * @param {string} options.models  Path to the semantic-layer YAML (watched).
 * @param {string} [options.output="src/sidemantic.client.generated.ts"]  Where to write the schema module.
 * @param {boolean} [options.includeYaml=true]  Embed `SCHEMA_YAML` (for the wasm transport).
 * @param {{ sources: string[], output: string, call?: string }} [options.sql]  Optional sqlx query-type codegen.
 * @param {string|URL} [options.wasmUrl]  Override the wasm binary location.
 * @returns {import("vite").Plugin}
 */
export function sidemantic(options = {}) {
  const { models, output = "src/sidemantic.client.generated.ts", includeYaml = true, sql, wasmUrl } = options;
  if (!models) throw new Error("vite-plugin-sidemantic: `models` (path to the semantic-layer YAML) is required");

  const outputs = new Set([resolve(output)]);
  if (sql) outputs.add(resolve(sql.output));
  let wasmBytes;

  function loadWasm() {
    if (!wasmBytes) {
      wasmBytes = readFileSync(wasmUrl ?? fileURLToPath(new URL("./wasm/sidemantic_bg.wasm", import.meta.url)));
    }
    return wasmBytes;
  }

  function writeIfChanged(path, content) {
    if (!existsSync(path) || readFileSync(path, "utf8") !== content) writeFileSync(path, content);
  }

  async function generate() {
    const modelsYaml = readFileSync(models, "utf8");
    const bytes = loadWasm();
    writeIfChanged(resolve(output), await generateClientSchema(modelsYaml, { includeYaml, wasmUrl: bytes }));
    if (sql) {
      const sources = listTsSources(sql.sources, outputs).map((file) => readFileSync(file, "utf8"));
      const literals = extractSqlLiterals(sources, { call: sql.call });
      writeIfChanged(resolve(sql.output), await generateSqlTypes(modelsYaml, literals, { wasmUrl: bytes }));
    }
  }

  return {
    name: "vite-plugin-sidemantic",
    async buildStart() {
      await generate();
    },
    configureServer(server) {
      server.watcher.add(resolve(models));
      const onChange = async (file) => {
        const changed = resolve(file);
        if (outputs.has(changed)) return; // never react to our own generated files
        const isModel = changed === resolve(models);
        const isSqlSource = Boolean(sql) && (changed.endsWith(".ts") || changed.endsWith(".tsx"));
        if (!isModel && !isSqlSource) return;
        try {
          await generate();
        } catch (error) {
          server.config.logger.error(`[sidemantic] codegen failed: ${error.message}`);
        }
      };
      server.watcher.on("change", onChange);
      server.watcher.on("add", onChange);
    },
  };
}

export default sidemantic;
