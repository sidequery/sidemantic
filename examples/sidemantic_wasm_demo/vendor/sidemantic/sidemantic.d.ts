/* tslint:disable */
/* eslint-disable */

export function wasm_analyze_migrator_query(sql_query: string): string;

export function wasm_build_preaggregation_refresh_statements(mode: string, table_name: string, source_sql: string, watermark_column?: string | null, from_watermark?: string | null, lookback?: string | null, dialect?: string | null, refresh_every?: string | null): string;

export function wasm_build_symmetric_aggregate_sql(measure_expr: string, primary_key: string, agg_type: string, model_alias: string | null | undefined, dialect: string): string;

export function wasm_calculate_preaggregation_benefit_score(pattern_json: string, count: number): number;

export function wasm_chart_auto_detect_columns(columns_json: string, numeric_flags_json: string): string;

export function wasm_chart_encoding_type(column: string): string;

export function wasm_chart_format_label(column: string): string;

export function wasm_chart_select_type(x: string, x_value_kind: string, y_count: number): string;

export function wasm_compile_with_yaml_query(yaml: string, query_yaml: string): string;

export function wasm_detect_adapter_kind(path: string, content: string): string;

export function wasm_dimension_sql_expr_with_yaml(dimension_yaml: string): string;

export function wasm_dimension_with_granularity_with_yaml(dimension_yaml: string, granularity: string): string;

export function wasm_evaluate_table_calculation_expression(expr: string): number;

export function wasm_extract_column_references(sql_expr: string): string;

export function wasm_extract_metric_dependencies_from_yaml(metric_yaml: string, models_yaml?: string | null, model_context?: string | null): string;

export function wasm_extract_preaggregation_patterns(queries_json: string): string;

export function wasm_find_models_for_query(dimensions_json: string, metrics_json: string): string;

export function wasm_find_relationship_path_with_yaml(yaml: string, from_model: string, to_model: string): string;

export function wasm_format_parameter_value_with_yaml(parameter_yaml: string, value_yaml: string): string;

export function wasm_generate_catalog_metadata_with_yaml(yaml: string, schema: string): string;

export function wasm_generate_preaggregation_definition(recommendation_json: string): string;

export function wasm_generate_preaggregation_materialization_sql_with_yaml(yaml: string, model_name: string, preagg_name: string): string;

export function wasm_generate_preaggregation_name(pattern_json: string): string;

export function wasm_generate_time_comparison_sql(comparison_type: string, calculation: string, current_metric_sql: string, time_dimension: string, offset?: bigint | null, offset_unit?: string | null): string;

export function wasm_interpolate_sql_with_parameters_with_yaml(sql_template: string, parameters_yaml: string, values_yaml: string): string;

export function wasm_is_relative_date(expr: string): boolean;

export function wasm_is_sql_template(sql: string): boolean;

export function wasm_load_graph_with_sql(sql_content: string): string;

export function wasm_load_graph_with_yaml(yaml: string): string;

export function wasm_metric_is_simple_aggregation(metric_yaml: string): boolean;

export function wasm_metric_sql_expr(metric_yaml: string): string;

export function wasm_metric_to_sql(metric_yaml: string): string;

export function wasm_model_find_dimension_index_with_yaml(model_yaml: string, name: string): string;

export function wasm_model_find_metric_index_with_yaml(model_yaml: string, name: string): string;

export function wasm_model_find_pre_aggregation_index_with_yaml(model_yaml: string, name: string): string;

export function wasm_model_find_segment_index_with_yaml(model_yaml: string, name: string): string;

export function wasm_model_get_drill_down_with_yaml(model_yaml: string, dimension_name: string): string;

export function wasm_model_get_drill_up_with_yaml(model_yaml: string, dimension_name: string): string;

export function wasm_model_get_hierarchy_path_with_yaml(model_yaml: string, dimension_name: string): string;

export function wasm_needs_symmetric_aggregate(relationship_type: string, is_base_model: boolean): boolean;

export function wasm_parse_reference_with_yaml(yaml: string, reference: string): string;

export function wasm_parse_relative_date(expr: string, dialect: string): string;

export function wasm_parse_simple_metric_aggregation(sql_expr: string): string;

export function wasm_parse_sql_definitions_payload(sql: string): string;

export function wasm_parse_sql_graph_definitions_payload(sql: string): string;

export function wasm_parse_sql_model_payload(sql: string): string;

export function wasm_parse_sql_statement_blocks_payload(sql: string): string;

export function wasm_recommend_preaggregation_patterns(patterns_json: string, min_count: number, min_benefit_score: number, max_recommendations?: number | null): string;

export function wasm_relationship_foreign_key_columns_with_yaml(relationship_yaml: string): string;

export function wasm_relationship_primary_key_columns_with_yaml(relationship_yaml: string): string;

export function wasm_relationship_related_key_with_yaml(relationship_yaml: string): string;

export function wasm_relationship_sql_expr_with_yaml(relationship_yaml: string): string;

export function wasm_relative_date_to_range(expr: string, column: string, dialect: string): string;

export function wasm_render_sql_template(template: string, context_yaml: string): string;

export function wasm_resolve_metric_inheritance(metrics_yaml: string): string;

export function wasm_resolve_model_inheritance_with_yaml(yaml: string): string;

export function wasm_rewrite_with_yaml(yaml: string, sql: string): string;

export function wasm_segment_get_sql_with_yaml(segment_yaml: string, model_alias: string): string;

export function wasm_summarize_preaggregation_patterns(patterns_json: string, min_count: number): string;

export function wasm_time_comparison_offset_interval(comparison_type: string, offset?: bigint | null, offset_unit?: string | null): string;

export function wasm_time_comparison_sql_offset(comparison_type: string, offset?: bigint | null, offset_unit?: string | null): string;

export function wasm_trailing_period_sql_interval(amount: bigint, unit: string): string;

export function wasm_validate_engine_refresh_sql_compatibility(source_sql: string, dialect: string): string;

export function wasm_validate_metric_payload(metric_yaml: string): boolean;

export function wasm_validate_model_payload(model_yaml: string): boolean;

export function wasm_validate_models_yaml(yaml: string): boolean;

export function wasm_validate_parameter_payload(parameter_yaml: string): boolean;

export function wasm_validate_query_references(yaml: string, metrics_json: string, dimensions_json: string): string;

export function wasm_validate_query_references_with_yaml(yaml: string, metrics_json: string, dimensions_json: string): string;

export function wasm_validate_query_with_yaml(yaml: string, query_yaml: string): string;

export function wasm_validate_table_calculation_payload(calculation_yaml: string): boolean;

export function wasm_validate_table_formula_expression(expression: string): boolean;

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
    readonly memory: WebAssembly.Memory;
    readonly wasm_analyze_migrator_query: (a: number, b: number) => [number, number, number, number];
    readonly wasm_build_preaggregation_refresh_statements: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: number, i: number, j: number, k: number, l: number, m: number, n: number, o: number, p: number) => [number, number, number, number];
    readonly wasm_build_symmetric_aggregate_sql: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: number, i: number, j: number) => [number, number, number, number];
    readonly wasm_calculate_preaggregation_benefit_score: (a: number, b: number, c: number) => [number, number, number];
    readonly wasm_chart_auto_detect_columns: (a: number, b: number, c: number, d: number) => [number, number, number, number];
    readonly wasm_chart_encoding_type: (a: number, b: number) => [number, number];
    readonly wasm_chart_format_label: (a: number, b: number) => [number, number];
    readonly wasm_chart_select_type: (a: number, b: number, c: number, d: number, e: number) => [number, number];
    readonly wasm_compile_with_yaml_query: (a: number, b: number, c: number, d: number) => [number, number, number, number];
    readonly wasm_detect_adapter_kind: (a: number, b: number, c: number, d: number) => [number, number, number, number];
    readonly wasm_dimension_sql_expr_with_yaml: (a: number, b: number) => [number, number, number, number];
    readonly wasm_dimension_with_granularity_with_yaml: (a: number, b: number, c: number, d: number) => [number, number, number, number];
    readonly wasm_evaluate_table_calculation_expression: (a: number, b: number) => [number, number, number];
    readonly wasm_extract_column_references: (a: number, b: number) => [number, number];
    readonly wasm_extract_metric_dependencies_from_yaml: (a: number, b: number, c: number, d: number, e: number, f: number) => [number, number, number, number];
    readonly wasm_extract_preaggregation_patterns: (a: number, b: number) => [number, number, number, number];
    readonly wasm_find_models_for_query: (a: number, b: number, c: number, d: number) => [number, number, number, number];
    readonly wasm_find_relationship_path_with_yaml: (a: number, b: number, c: number, d: number, e: number, f: number) => [number, number, number, number];
    readonly wasm_format_parameter_value_with_yaml: (a: number, b: number, c: number, d: number) => [number, number, number, number];
    readonly wasm_generate_catalog_metadata_with_yaml: (a: number, b: number, c: number, d: number) => [number, number, number, number];
    readonly wasm_generate_preaggregation_definition: (a: number, b: number) => [number, number, number, number];
    readonly wasm_generate_preaggregation_materialization_sql_with_yaml: (a: number, b: number, c: number, d: number, e: number, f: number) => [number, number, number, number];
    readonly wasm_generate_preaggregation_name: (a: number, b: number) => [number, number, number, number];
    readonly wasm_generate_time_comparison_sql: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: number, i: number, j: bigint, k: number, l: number) => [number, number, number, number];
    readonly wasm_interpolate_sql_with_parameters_with_yaml: (a: number, b: number, c: number, d: number, e: number, f: number) => [number, number, number, number];
    readonly wasm_is_relative_date: (a: number, b: number) => number;
    readonly wasm_is_sql_template: (a: number, b: number) => number;
    readonly wasm_load_graph_with_sql: (a: number, b: number) => [number, number, number, number];
    readonly wasm_load_graph_with_yaml: (a: number, b: number) => [number, number, number, number];
    readonly wasm_metric_is_simple_aggregation: (a: number, b: number) => [number, number, number];
    readonly wasm_metric_sql_expr: (a: number, b: number) => [number, number, number, number];
    readonly wasm_metric_to_sql: (a: number, b: number) => [number, number, number, number];
    readonly wasm_model_find_dimension_index_with_yaml: (a: number, b: number, c: number, d: number) => [number, number, number, number];
    readonly wasm_model_find_metric_index_with_yaml: (a: number, b: number, c: number, d: number) => [number, number, number, number];
    readonly wasm_model_find_pre_aggregation_index_with_yaml: (a: number, b: number, c: number, d: number) => [number, number, number, number];
    readonly wasm_model_find_segment_index_with_yaml: (a: number, b: number, c: number, d: number) => [number, number, number, number];
    readonly wasm_model_get_drill_down_with_yaml: (a: number, b: number, c: number, d: number) => [number, number, number, number];
    readonly wasm_model_get_drill_up_with_yaml: (a: number, b: number, c: number, d: number) => [number, number, number, number];
    readonly wasm_model_get_hierarchy_path_with_yaml: (a: number, b: number, c: number, d: number) => [number, number, number, number];
    readonly wasm_needs_symmetric_aggregate: (a: number, b: number, c: number) => number;
    readonly wasm_parse_reference_with_yaml: (a: number, b: number, c: number, d: number) => [number, number, number, number];
    readonly wasm_parse_relative_date: (a: number, b: number, c: number, d: number) => [number, number, number, number];
    readonly wasm_parse_simple_metric_aggregation: (a: number, b: number) => [number, number, number, number];
    readonly wasm_parse_sql_definitions_payload: (a: number, b: number) => [number, number, number, number];
    readonly wasm_parse_sql_graph_definitions_payload: (a: number, b: number) => [number, number, number, number];
    readonly wasm_parse_sql_model_payload: (a: number, b: number) => [number, number, number, number];
    readonly wasm_parse_sql_statement_blocks_payload: (a: number, b: number) => [number, number, number, number];
    readonly wasm_recommend_preaggregation_patterns: (a: number, b: number, c: number, d: number, e: number) => [number, number, number, number];
    readonly wasm_relationship_foreign_key_columns_with_yaml: (a: number, b: number) => [number, number, number, number];
    readonly wasm_relationship_primary_key_columns_with_yaml: (a: number, b: number) => [number, number, number, number];
    readonly wasm_relationship_related_key_with_yaml: (a: number, b: number) => [number, number, number, number];
    readonly wasm_relationship_sql_expr_with_yaml: (a: number, b: number) => [number, number, number, number];
    readonly wasm_relative_date_to_range: (a: number, b: number, c: number, d: number, e: number, f: number) => [number, number, number, number];
    readonly wasm_render_sql_template: (a: number, b: number, c: number, d: number) => [number, number, number, number];
    readonly wasm_resolve_metric_inheritance: (a: number, b: number) => [number, number, number, number];
    readonly wasm_resolve_model_inheritance_with_yaml: (a: number, b: number) => [number, number, number, number];
    readonly wasm_rewrite_with_yaml: (a: number, b: number, c: number, d: number) => [number, number, number, number];
    readonly wasm_segment_get_sql_with_yaml: (a: number, b: number, c: number, d: number) => [number, number, number, number];
    readonly wasm_summarize_preaggregation_patterns: (a: number, b: number, c: number) => [number, number, number, number];
    readonly wasm_time_comparison_offset_interval: (a: number, b: number, c: number, d: bigint, e: number, f: number) => [number, number, number, number];
    readonly wasm_time_comparison_sql_offset: (a: number, b: number, c: number, d: bigint, e: number, f: number) => [number, number, number, number];
    readonly wasm_trailing_period_sql_interval: (a: bigint, b: number, c: number) => [number, number, number, number];
    readonly wasm_validate_engine_refresh_sql_compatibility: (a: number, b: number, c: number, d: number) => [number, number, number, number];
    readonly wasm_validate_metric_payload: (a: number, b: number) => [number, number, number];
    readonly wasm_validate_model_payload: (a: number, b: number) => [number, number, number];
    readonly wasm_validate_models_yaml: (a: number, b: number) => [number, number, number];
    readonly wasm_validate_parameter_payload: (a: number, b: number) => [number, number, number];
    readonly wasm_validate_query_references: (a: number, b: number, c: number, d: number, e: number, f: number) => [number, number, number, number];
    readonly wasm_validate_query_references_with_yaml: (a: number, b: number, c: number, d: number, e: number, f: number) => [number, number, number, number];
    readonly wasm_validate_query_with_yaml: (a: number, b: number, c: number, d: number) => [number, number, number, number];
    readonly wasm_validate_table_calculation_payload: (a: number, b: number) => [number, number, number];
    readonly wasm_validate_table_formula_expression: (a: number, b: number) => [number, number, number];
    readonly sidemantic_add_definition: (a: number, b: number, c: number) => number;
    readonly sidemantic_add_definition_for_context: (a: number, b: number, c: number, d: number) => number;
    readonly sidemantic_autoload: (a: number) => number;
    readonly sidemantic_autoload_for_context: (a: number, b: number) => number;
    readonly sidemantic_clear: () => void;
    readonly sidemantic_clear_for_context: (a: number) => void;
    readonly sidemantic_define: (a: number, b: number, c: number) => number;
    readonly sidemantic_define_for_context: (a: number, b: number, c: number, d: number) => number;
    readonly sidemantic_free: (a: number) => void;
    readonly sidemantic_free_result: (a: number) => void;
    readonly sidemantic_is_model: (a: number) => number;
    readonly sidemantic_is_model_for_context: (a: number, b: number) => number;
    readonly sidemantic_list_models: () => number;
    readonly sidemantic_list_models_for_context: (a: number) => number;
    readonly sidemantic_load_file: (a: number) => number;
    readonly sidemantic_load_file_for_context: (a: number, b: number) => number;
    readonly sidemantic_load_yaml: (a: number) => number;
    readonly sidemantic_load_yaml_for_context: (a: number, b: number) => number;
    readonly sidemantic_rewrite: (a: number, b: number) => void;
    readonly sidemantic_rewrite_for_context: (a: number, b: number, c: number) => void;
    readonly sidemantic_use: (a: number) => number;
    readonly sidemantic_use_for_context: (a: number, b: number) => number;
    readonly __wbindgen_externrefs: WebAssembly.Table;
    readonly __wbindgen_malloc: (a: number, b: number) => number;
    readonly __wbindgen_realloc: (a: number, b: number, c: number, d: number) => number;
    readonly __externref_table_dealloc: (a: number) => void;
    readonly __wbindgen_free: (a: number, b: number, c: number) => void;
    readonly __wbindgen_start: () => void;
}

export type SyncInitInput = BufferSource | WebAssembly.Module;

/**
 * Instantiates the given `module`, which can either be bytes or
 * a precompiled `WebAssembly.Module`.
 *
 * @param {{ module: SyncInitInput }} module - Passing `SyncInitInput` directly is deprecated.
 *
 * @returns {InitOutput}
 */
export function initSync(module: { module: SyncInitInput } | SyncInitInput): InitOutput;

/**
 * If `module_or_path` is {RequestInfo} or {URL}, makes a request and
 * for everything else, calls `WebAssembly.instantiate` directly.
 *
 * @param {{ module_or_path: InitInput | Promise<InitInput> }} module_or_path - Passing `InitInput` directly is deprecated.
 *
 * @returns {Promise<InitOutput>}
 */
export default function __wbg_init (module_or_path?: { module_or_path: InitInput | Promise<InitInput> } | InitInput | Promise<InitInput>): Promise<InitOutput>;
