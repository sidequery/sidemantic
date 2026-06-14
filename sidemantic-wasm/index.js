// Ergonomic wrapper around the wasm-bindgen Sidemantic bundle.
//
// The package generates SQL from a semantic layer; it does not execute queries.
// Pair the compiled SQL with a browser query engine such as DuckDB-WASM.

import initWasm, {
  wasm_compile_with_yaml_query,
  wasm_generate_catalog_metadata_with_yaml,
  wasm_load_graph_with_yaml,
  wasm_rewrite_with_yaml,
  wasm_validate_query_with_yaml,
} from "./wasm/sidemantic.js";

let initialized;

function defaultWasmUrl() {
  return new URL("./wasm/sidemantic_bg.wasm", import.meta.url);
}

function toQueryYaml(query) {
  // The engine parses the query payload as YAML, and JSON is valid YAML, so a
  // structured object can be passed straight through after serialization.
  return typeof query === "string" ? query : JSON.stringify(query ?? {});
}

/**
 * Initialize the underlying wasm module. Idempotent: repeated calls reuse the
 * first initialization. Pass `wasmUrl` to override where the binary loads from.
 */
export function initSidemantic(options = {}) {
  if (!initialized) {
    initialized = initWasm({ module_or_path: options.wasmUrl ?? defaultWasmUrl() });
  }
  return initialized;
}

/**
 * Initialize wasm and return a small typed runtime around the core operations.
 * `models` is a Sidemantic YAML (or native SQL) definition string.
 */
export async function createSidemanticRuntime(options = {}) {
  await initSidemantic(options);
  return {
    compile(models, query) {
      return wasm_compile_with_yaml_query(models, toQueryYaml(query));
    },
    validate(models, query) {
      return JSON.parse(wasm_validate_query_with_yaml(models, toQueryYaml(query)));
    },
    rewrite(models, sql) {
      return wasm_rewrite_with_yaml(models, sql);
    },
    loadGraph(models) {
      return JSON.parse(wasm_load_graph_with_yaml(models));
    },
    generateCatalogMetadata(models, schema = "main") {
      return wasm_generate_catalog_metadata_with_yaml(models, schema);
    },
  };
}

// Re-export the full raw wasm-bindgen surface for advanced use.
export * from "./wasm/sidemantic.js";
