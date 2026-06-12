//! wasm-bindgen exports for pure Rust runtime orchestration.

use wasm_bindgen::prelude::*;

use crate::runtime::{
    analyze_migrator_query, build_preaggregation_refresh_statements,
    build_symmetric_aggregate_sql as build_symmetric_aggregate_sql_runtime,
    calculate_preaggregation_benefit_score, chart_auto_detect_columns, chart_encoding_type,
    chart_format_label, chart_select_type, compile_with_yaml_query, detect_adapter_kind,
    dimension_sql_expr_with_yaml, dimension_with_granularity_with_yaml,
    evaluate_table_calculation_expression, extract_column_references,
    extract_metric_dependencies_from_yaml, extract_preaggregation_patterns, find_models_for_query,
    find_relationship_path_with_yaml, format_parameter_value_with_yaml,
    generate_catalog_metadata_with_yaml, generate_preaggregation_definition,
    generate_preaggregation_materialization_sql_with_yaml, generate_preaggregation_name,
    generate_time_comparison_sql, interpolate_sql_with_parameters_with_yaml, is_relative_date,
    is_sql_template, load_graph_with_sql, load_graph_with_yaml, metric_is_simple_aggregation,
    metric_sql_expr, metric_to_sql, model_find_dimension_index_with_yaml,
    model_find_metric_index_with_yaml, model_find_pre_aggregation_index_with_yaml,
    model_find_segment_index_with_yaml, model_get_drill_down_with_yaml,
    model_get_drill_up_with_yaml, model_get_hierarchy_path_with_yaml,
    needs_symmetric_aggregate as needs_symmetric_aggregate_runtime, parse_reference_with_yaml,
    parse_relative_date, parse_simple_metric_aggregation, parse_sql_definitions_payload,
    parse_sql_graph_definitions_payload, parse_sql_model_payload,
    parse_sql_statement_blocks_payload, recommend_preaggregation_patterns,
    relationship_foreign_key_columns_with_yaml, relationship_primary_key_columns_with_yaml,
    relationship_related_key_with_yaml, relationship_sql_expr_with_yaml, relative_date_to_range,
    render_sql_template, resolve_metric_inheritance, resolve_model_inheritance_with_yaml,
    rewrite_with_yaml, segment_get_sql_with_yaml, summarize_preaggregation_patterns,
    time_comparison_offset_interval, time_comparison_sql_offset, trailing_period_sql_interval,
    validate_engine_refresh_sql_compatibility, validate_metric_payload, validate_model_payload,
    validate_models_yaml, validate_parameter_payload, validate_query_references_with_yaml,
    validate_query_with_yaml, validate_table_calculation_payload,
    validate_table_formula_expression,
};

fn wasm_error(err: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&err.to_string())
}

#[wasm_bindgen]
pub fn wasm_compile_with_yaml_query(yaml: &str, query_yaml: &str) -> Result<String, JsValue> {
    compile_with_yaml_query(yaml, query_yaml).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_build_symmetric_aggregate_sql(
    measure_expr: &str,
    primary_key: &str,
    agg_type: &str,
    model_alias: Option<String>,
    dialect: &str,
) -> Result<String, JsValue> {
    build_symmetric_aggregate_sql_runtime(
        measure_expr,
        primary_key,
        agg_type,
        model_alias.as_deref(),
        dialect,
    )
    .map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_needs_symmetric_aggregate(relationship_type: &str, is_base_model: bool) -> bool {
    needs_symmetric_aggregate_runtime(relationship_type, is_base_model)
}

#[wasm_bindgen]
pub fn wasm_rewrite_with_yaml(yaml: &str, sql: &str) -> Result<String, JsValue> {
    rewrite_with_yaml(yaml, sql).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_validate_query_with_yaml(yaml: &str, query_yaml: &str) -> Result<String, JsValue> {
    let errors = validate_query_with_yaml(yaml, query_yaml).map_err(wasm_error)?;
    serde_json::to_string(&errors).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_load_graph_with_yaml(yaml: &str) -> Result<String, JsValue> {
    load_graph_with_yaml(yaml).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_load_graph_with_sql(sql_content: &str) -> Result<String, JsValue> {
    load_graph_with_sql(sql_content).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_validate_query_references_with_yaml(
    yaml: &str,
    metrics_json: &str,
    dimensions_json: &str,
) -> Result<String, JsValue> {
    let metrics: Vec<String> = serde_json::from_str(metrics_json).map_err(wasm_error)?;
    let dimensions: Vec<String> = serde_json::from_str(dimensions_json).map_err(wasm_error)?;
    let errors =
        validate_query_references_with_yaml(yaml, &metrics, &dimensions).map_err(wasm_error)?;
    serde_json::to_string(&errors).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_validate_query_references(
    yaml: &str,
    metrics_json: &str,
    dimensions_json: &str,
) -> Result<String, JsValue> {
    wasm_validate_query_references_with_yaml(yaml, metrics_json, dimensions_json)
}

#[wasm_bindgen]
pub fn wasm_generate_catalog_metadata_with_yaml(
    yaml: &str,
    schema: &str,
) -> Result<String, JsValue> {
    generate_catalog_metadata_with_yaml(yaml, schema).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_generate_preaggregation_materialization_sql_with_yaml(
    yaml: &str,
    model_name: &str,
    preagg_name: &str,
) -> Result<String, JsValue> {
    generate_preaggregation_materialization_sql_with_yaml(yaml, model_name, preagg_name)
        .map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_extract_column_references(sql_expr: &str) -> String {
    serde_json::to_string(&extract_column_references(sql_expr)).unwrap_or_else(|_| "[]".to_string())
}

#[wasm_bindgen]
pub fn wasm_find_models_for_query(
    dimensions_json: &str,
    metrics_json: &str,
) -> Result<String, JsValue> {
    let dimensions: Vec<String> = serde_json::from_str(dimensions_json).map_err(wasm_error)?;
    let metrics: Vec<String> = serde_json::from_str(metrics_json).map_err(wasm_error)?;
    let models = find_models_for_query(&dimensions, &metrics);
    serde_json::to_string(&models).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_find_relationship_path_with_yaml(
    yaml: &str,
    from_model: &str,
    to_model: &str,
) -> Result<String, JsValue> {
    let path = find_relationship_path_with_yaml(yaml, from_model, to_model).map_err(wasm_error)?;
    serde_json::to_string(&path).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_render_sql_template(template: &str, context_yaml: &str) -> Result<String, JsValue> {
    render_sql_template(template, context_yaml).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_parse_sql_definitions_payload(sql: &str) -> Result<String, JsValue> {
    parse_sql_definitions_payload(sql).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_parse_sql_graph_definitions_payload(sql: &str) -> Result<String, JsValue> {
    parse_sql_graph_definitions_payload(sql).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_parse_sql_model_payload(sql: &str) -> Result<String, JsValue> {
    parse_sql_model_payload(sql).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_parse_sql_statement_blocks_payload(sql: &str) -> Result<String, JsValue> {
    parse_sql_statement_blocks_payload(sql).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_analyze_migrator_query(sql_query: &str) -> Result<String, JsValue> {
    analyze_migrator_query(sql_query).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_chart_auto_detect_columns(
    columns_json: &str,
    numeric_flags_json: &str,
) -> Result<String, JsValue> {
    let columns: Vec<String> = serde_json::from_str(columns_json).map_err(wasm_error)?;
    let numeric_flags: Vec<bool> = serde_json::from_str(numeric_flags_json).map_err(wasm_error)?;
    let (x, y) = chart_auto_detect_columns(&columns, &numeric_flags).map_err(wasm_error)?;
    serde_json::to_string(&serde_json::json!({
        "x": x,
        "y": y,
    }))
    .map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_chart_select_type(x: &str, x_value_kind: &str, y_count: usize) -> String {
    chart_select_type(x, x_value_kind, y_count)
}

#[wasm_bindgen]
pub fn wasm_chart_encoding_type(column: &str) -> String {
    chart_encoding_type(column)
}

#[wasm_bindgen]
pub fn wasm_chart_format_label(column: &str) -> String {
    chart_format_label(column)
}

#[wasm_bindgen]
pub fn wasm_validate_model_payload(model_yaml: &str) -> Result<bool, JsValue> {
    validate_model_payload(model_yaml).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_validate_metric_payload(metric_yaml: &str) -> Result<bool, JsValue> {
    validate_metric_payload(metric_yaml).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_validate_parameter_payload(parameter_yaml: &str) -> Result<bool, JsValue> {
    validate_parameter_payload(parameter_yaml).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_validate_table_calculation_payload(calculation_yaml: &str) -> Result<bool, JsValue> {
    validate_table_calculation_payload(calculation_yaml).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_parse_relative_date(expr: &str, dialect: &str) -> Result<String, JsValue> {
    serde_json::to_string(&parse_relative_date(expr, dialect)).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_relative_date_to_range(
    expr: &str,
    column: &str,
    dialect: &str,
) -> Result<String, JsValue> {
    serde_json::to_string(&relative_date_to_range(expr, column, dialect)).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_is_relative_date(expr: &str) -> bool {
    is_relative_date(expr)
}

#[wasm_bindgen]
pub fn wasm_time_comparison_offset_interval(
    comparison_type: &str,
    offset: Option<i64>,
    offset_unit: Option<String>,
) -> Result<String, JsValue> {
    let (amount, unit) =
        time_comparison_offset_interval(comparison_type, offset, offset_unit.as_deref())
            .map_err(wasm_error)?;
    serde_json::to_string(&serde_json::json!({
        "amount": amount,
        "unit": unit,
    }))
    .map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_time_comparison_sql_offset(
    comparison_type: &str,
    offset: Option<i64>,
    offset_unit: Option<String>,
) -> Result<String, JsValue> {
    time_comparison_sql_offset(comparison_type, offset, offset_unit.as_deref()).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_trailing_period_sql_interval(amount: i64, unit: &str) -> Result<String, JsValue> {
    trailing_period_sql_interval(amount, unit).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_generate_time_comparison_sql(
    comparison_type: &str,
    calculation: &str,
    current_metric_sql: &str,
    time_dimension: &str,
    offset: Option<i64>,
    offset_unit: Option<String>,
) -> Result<String, JsValue> {
    generate_time_comparison_sql(
        comparison_type,
        calculation,
        current_metric_sql,
        time_dimension,
        offset,
        offset_unit.as_deref(),
    )
    .map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_is_sql_template(sql: &str) -> bool {
    is_sql_template(sql)
}

#[wasm_bindgen]
pub fn wasm_format_parameter_value_with_yaml(
    parameter_yaml: &str,
    value_yaml: &str,
) -> Result<String, JsValue> {
    format_parameter_value_with_yaml(parameter_yaml, value_yaml).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_interpolate_sql_with_parameters_with_yaml(
    sql_template: &str,
    parameters_yaml: &str,
    values_yaml: &str,
) -> Result<String, JsValue> {
    interpolate_sql_with_parameters_with_yaml(sql_template, parameters_yaml, values_yaml)
        .map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_detect_adapter_kind(path: &str, content: &str) -> Result<String, JsValue> {
    serde_json::to_string(&detect_adapter_kind(path, content)).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_parse_simple_metric_aggregation(sql_expr: &str) -> Result<String, JsValue> {
    serde_json::to_string(&parse_simple_metric_aggregation(sql_expr)).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_metric_to_sql(metric_yaml: &str) -> Result<String, JsValue> {
    metric_to_sql(metric_yaml).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_metric_sql_expr(metric_yaml: &str) -> Result<String, JsValue> {
    metric_sql_expr(metric_yaml).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_metric_is_simple_aggregation(metric_yaml: &str) -> Result<bool, JsValue> {
    metric_is_simple_aggregation(metric_yaml).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_extract_preaggregation_patterns(queries_json: &str) -> Result<String, JsValue> {
    let queries: Vec<String> = serde_json::from_str(queries_json).map_err(wasm_error)?;
    extract_preaggregation_patterns(queries).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_recommend_preaggregation_patterns(
    patterns_json: &str,
    min_count: usize,
    min_benefit_score: f64,
    max_recommendations: Option<usize>,
) -> Result<String, JsValue> {
    recommend_preaggregation_patterns(
        patterns_json,
        min_count,
        min_benefit_score,
        max_recommendations,
    )
    .map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_calculate_preaggregation_benefit_score(
    pattern_json: &str,
    count: usize,
) -> Result<f64, JsValue> {
    calculate_preaggregation_benefit_score(pattern_json, count).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_generate_preaggregation_name(pattern_json: &str) -> Result<String, JsValue> {
    generate_preaggregation_name(pattern_json).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_generate_preaggregation_definition(
    recommendation_json: &str,
) -> Result<String, JsValue> {
    generate_preaggregation_definition(recommendation_json).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_summarize_preaggregation_patterns(
    patterns_json: &str,
    min_count: usize,
) -> Result<String, JsValue> {
    summarize_preaggregation_patterns(patterns_json, min_count).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_validate_models_yaml(yaml: &str) -> Result<bool, JsValue> {
    validate_models_yaml(yaml).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_parse_reference_with_yaml(yaml: &str, reference: &str) -> Result<String, JsValue> {
    let parsed = parse_reference_with_yaml(yaml, reference).map_err(wasm_error)?;
    serde_json::to_string(&parsed).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_resolve_model_inheritance_with_yaml(yaml: &str) -> Result<String, JsValue> {
    resolve_model_inheritance_with_yaml(yaml).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_resolve_metric_inheritance(metrics_yaml: &str) -> Result<String, JsValue> {
    resolve_metric_inheritance(metrics_yaml).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_dimension_sql_expr_with_yaml(dimension_yaml: &str) -> Result<String, JsValue> {
    dimension_sql_expr_with_yaml(dimension_yaml).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_dimension_with_granularity_with_yaml(
    dimension_yaml: &str,
    granularity: &str,
) -> Result<String, JsValue> {
    dimension_with_granularity_with_yaml(dimension_yaml, granularity).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_model_get_hierarchy_path_with_yaml(
    model_yaml: &str,
    dimension_name: &str,
) -> Result<String, JsValue> {
    let path =
        model_get_hierarchy_path_with_yaml(model_yaml, dimension_name).map_err(wasm_error)?;
    serde_json::to_string(&path).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_model_get_drill_down_with_yaml(
    model_yaml: &str,
    dimension_name: &str,
) -> Result<String, JsValue> {
    let drill = model_get_drill_down_with_yaml(model_yaml, dimension_name).map_err(wasm_error)?;
    serde_json::to_string(&drill).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_model_get_drill_up_with_yaml(
    model_yaml: &str,
    dimension_name: &str,
) -> Result<String, JsValue> {
    let drill = model_get_drill_up_with_yaml(model_yaml, dimension_name).map_err(wasm_error)?;
    serde_json::to_string(&drill).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_model_find_dimension_index_with_yaml(
    model_yaml: &str,
    name: &str,
) -> Result<String, JsValue> {
    let index = model_find_dimension_index_with_yaml(model_yaml, name).map_err(wasm_error)?;
    serde_json::to_string(&index).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_model_find_metric_index_with_yaml(
    model_yaml: &str,
    name: &str,
) -> Result<String, JsValue> {
    let index = model_find_metric_index_with_yaml(model_yaml, name).map_err(wasm_error)?;
    serde_json::to_string(&index).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_model_find_segment_index_with_yaml(
    model_yaml: &str,
    name: &str,
) -> Result<String, JsValue> {
    let index = model_find_segment_index_with_yaml(model_yaml, name).map_err(wasm_error)?;
    serde_json::to_string(&index).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_model_find_pre_aggregation_index_with_yaml(
    model_yaml: &str,
    name: &str,
) -> Result<String, JsValue> {
    let index = model_find_pre_aggregation_index_with_yaml(model_yaml, name).map_err(wasm_error)?;
    serde_json::to_string(&index).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_relationship_sql_expr_with_yaml(relationship_yaml: &str) -> Result<String, JsValue> {
    relationship_sql_expr_with_yaml(relationship_yaml).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_relationship_related_key_with_yaml(relationship_yaml: &str) -> Result<String, JsValue> {
    relationship_related_key_with_yaml(relationship_yaml).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_relationship_foreign_key_columns_with_yaml(
    relationship_yaml: &str,
) -> Result<String, JsValue> {
    let cols = relationship_foreign_key_columns_with_yaml(relationship_yaml).map_err(wasm_error)?;
    serde_json::to_string(&cols).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_relationship_primary_key_columns_with_yaml(
    relationship_yaml: &str,
) -> Result<String, JsValue> {
    let cols = relationship_primary_key_columns_with_yaml(relationship_yaml).map_err(wasm_error)?;
    serde_json::to_string(&cols).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_segment_get_sql_with_yaml(
    segment_yaml: &str,
    model_alias: &str,
) -> Result<String, JsValue> {
    segment_get_sql_with_yaml(segment_yaml, model_alias).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_extract_metric_dependencies_from_yaml(
    metric_yaml: &str,
    models_yaml: Option<String>,
    model_context: Option<String>,
) -> Result<String, JsValue> {
    let deps = extract_metric_dependencies_from_yaml(
        metric_yaml,
        models_yaml.as_deref(),
        model_context.as_deref(),
    )
    .map_err(wasm_error)?;
    serde_json::to_string(&deps).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_evaluate_table_calculation_expression(expr: &str) -> Result<f64, JsValue> {
    evaluate_table_calculation_expression(expr).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_validate_table_formula_expression(expression: &str) -> Result<bool, JsValue> {
    validate_table_formula_expression(expression).map_err(wasm_error)
}

#[wasm_bindgen]
pub fn wasm_validate_engine_refresh_sql_compatibility(
    source_sql: &str,
    dialect: &str,
) -> Result<String, JsValue> {
    let (is_valid, error) = validate_engine_refresh_sql_compatibility(source_sql, dialect);
    serde_json::to_string(&serde_json::json!({
        "is_valid": is_valid,
        "error": error,
    }))
    .map_err(wasm_error)
}

#[wasm_bindgen]
#[allow(clippy::too_many_arguments)]
pub fn wasm_build_preaggregation_refresh_statements(
    mode: &str,
    table_name: &str,
    source_sql: &str,
    watermark_column: Option<String>,
    from_watermark: Option<String>,
    lookback: Option<String>,
    dialect: Option<String>,
    refresh_every: Option<String>,
) -> Result<String, JsValue> {
    let statements = build_preaggregation_refresh_statements(
        mode,
        table_name,
        source_sql,
        watermark_column.as_deref(),
        from_watermark.as_deref(),
        lookback.as_deref(),
        dialect.as_deref(),
        refresh_every.as_deref(),
    )
    .map_err(wasm_error)?;
    serde_json::to_string(&statements).map_err(wasm_error)
}
