import initSqlctx, {
  wasm_compile_with_yaml_query,
  wasm_generate_catalog_metadata_with_yaml,
  wasm_load_graph_with_yaml,
  wasm_rewrite_with_yaml,
  wasm_validate_query_with_yaml,
} from "../../vendor/sqlctx/sqlctx.js";

export async function createSqlctxRuntime() {
  await initSqlctx({
    module_or_path: new URL("../../vendor/sqlctx/sqlctx_bg.wasm", import.meta.url),
  });

  return {
    compile(modelYaml, queryYaml) {
      return wasm_compile_with_yaml_query(modelYaml, queryYaml);
    },
    generateCatalogMetadata(modelYaml, catalogName) {
      return wasm_generate_catalog_metadata_with_yaml(modelYaml, catalogName);
    },
    loadGraph(modelYaml) {
      return JSON.parse(wasm_load_graph_with_yaml(modelYaml));
    },
    rewrite(modelYaml, sql) {
      return wasm_rewrite_with_yaml(modelYaml, sql);
    },
    validate(modelYaml, queryYaml) {
      return JSON.parse(wasm_validate_query_with_yaml(modelYaml, queryYaml));
    },
  };
}
