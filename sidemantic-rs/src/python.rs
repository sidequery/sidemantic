//! Python bindings for sidemantic-rs via PyO3.

#[cfg(feature = "python-adbc")]
use crate::db::{execute_with_adbc as execute_with_adbc_native, AdbcExecutionRequest, AdbcValue};
use crate::error::SidemanticError;
use crate::runtime::{
    analyze_migrator_query as analyze_migrator_query_native,
    build_preaggregation_refresh_statements as build_preaggregation_refresh_statements_native,
    build_symmetric_aggregate_sql as build_symmetric_aggregate_sql_native,
    calculate_preaggregation_benefit_score as calculate_preaggregation_benefit_score_native,
    chart_auto_detect_columns as chart_auto_detect_columns_native,
    chart_encoding_type as chart_encoding_type_native,
    chart_format_label as chart_format_label_native, chart_select_type as chart_select_type_native,
    compile_with_yaml_query as compile_with_yaml_query_native,
    detect_adapter_kind as detect_adapter_kind_native,
    dimension_sql_expr_with_yaml as dimension_sql_expr_with_yaml_native,
    dimension_with_granularity_with_yaml as dimension_with_granularity_with_yaml_native,
    evaluate_table_calculation_expression as evaluate_table_calculation_expression_native,
    extract_column_references as extract_column_references_native,
    extract_metric_dependencies_from_yaml as extract_metric_dependencies_from_yaml_native,
    extract_preaggregation_patterns as extract_preaggregation_patterns_native,
    find_models_for_query as find_models_for_query_native,
    find_models_for_query_with_yaml as find_models_for_query_with_yaml_native,
    find_relationship_path_with_yaml as find_relationship_path_with_yaml_native,
    format_parameter_value_with_yaml as format_parameter_value_with_yaml_native,
    generate_catalog_metadata_with_yaml as generate_catalog_metadata_with_yaml_native,
    generate_preaggregation_definition as generate_preaggregation_definition_native,
    generate_preaggregation_materialization_sql_with_yaml as generate_preaggregation_materialization_sql_with_yaml_native,
    generate_preaggregation_name as generate_preaggregation_name_native,
    generate_time_comparison_sql as generate_time_comparison_sql_native,
    interpolate_sql_with_parameters_with_yaml as interpolate_sql_with_parameters_with_yaml_native,
    is_relative_date as is_relative_date_native, is_sql_template as is_sql_template_native,
    load_graph_from_directory as load_graph_from_directory_native,
    load_graph_with_sql as load_graph_with_sql_native,
    load_graph_with_yaml as load_graph_with_yaml_native,
    metric_is_simple_aggregation as metric_is_simple_aggregation_native,
    metric_sql_expr as metric_sql_expr_native, metric_to_sql as metric_to_sql_native,
    model_find_dimension_index_with_yaml as model_find_dimension_index_with_yaml_native,
    model_find_metric_index_with_yaml as model_find_metric_index_with_yaml_native,
    model_find_pre_aggregation_index_with_yaml as model_find_pre_aggregation_index_with_yaml_native,
    model_find_segment_index_with_yaml as model_find_segment_index_with_yaml_native,
    model_get_drill_down_with_yaml as model_get_drill_down_with_yaml_native,
    model_get_drill_up_with_yaml as model_get_drill_up_with_yaml_native,
    model_get_hierarchy_path_with_yaml as model_get_hierarchy_path_with_yaml_native,
    needs_symmetric_aggregate as needs_symmetric_aggregate_native,
    parse_reference_with_yaml as parse_reference_with_yaml_native,
    parse_relative_date as parse_relative_date_native,
    parse_simple_metric_aggregation as parse_simple_metric_aggregation_native,
    parse_sql_definitions_payload as parse_sql_definitions_payload_native,
    parse_sql_graph_definitions_payload as parse_sql_graph_definitions_payload_native,
    parse_sql_model_payload as parse_sql_model_payload_native,
    parse_sql_statement_blocks_payload as parse_sql_statement_blocks_payload_native,
    plan_preaggregation_refresh_execution as plan_preaggregation_refresh_execution_native,
    recommend_preaggregation_patterns as recommend_preaggregation_patterns_native,
    relationship_foreign_key_columns_with_yaml as relationship_foreign_key_columns_with_yaml_native,
    relationship_primary_key_columns_with_yaml as relationship_primary_key_columns_with_yaml_native,
    relationship_related_key_with_yaml as relationship_related_key_with_yaml_native,
    relationship_sql_expr_with_yaml as relationship_sql_expr_with_yaml_native,
    relative_date_to_range as relative_date_to_range_native,
    render_sql_template as render_sql_template_native,
    resolve_metric_inheritance as resolve_metric_inheritance_native,
    resolve_model_inheritance_with_yaml as resolve_model_inheritance_with_yaml_native,
    resolve_preaggregation_refresh_mode as resolve_preaggregation_refresh_mode_native,
    rewrite_with_yaml as rewrite_with_yaml_native,
    segment_get_sql_with_yaml as segment_get_sql_with_yaml_native,
    shape_preaggregation_refresh_result as shape_preaggregation_refresh_result_native,
    summarize_preaggregation_patterns as summarize_preaggregation_patterns_native,
    time_comparison_offset_interval as time_comparison_offset_interval_native,
    time_comparison_sql_offset as time_comparison_sql_offset_native,
    trailing_period_sql_interval as trailing_period_sql_interval_native,
    validate_engine_refresh_sql_compatibility as validate_engine_refresh_sql_compatibility_native,
    validate_metric_payload as validate_metric_payload_native,
    validate_model_payload as validate_model_payload_native,
    validate_models_yaml as validate_models_yaml_native,
    validate_parameter_payload as validate_parameter_payload_native,
    validate_preaggregation_refresh_request as validate_preaggregation_refresh_request_native,
    validate_query_references_with_yaml as validate_query_references_with_yaml_native,
    validate_query_with_yaml as validate_query_with_yaml_native,
    validate_table_calculation_payload as validate_table_calculation_payload_native,
    validate_table_formula_expression as validate_table_formula_expression_native,
    RelationshipPathError,
};
#[cfg(feature = "python-adbc")]
use adbc_core::{
    constants,
    options::{OptionConnection, OptionDatabase, OptionValue},
};
use pyo3::exceptions::PyKeyError;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::sync::GILOnceCell;
#[cfg(feature = "python-adbc")]
use pyo3::types::{PyBool, PyBytes, PyString};
use pyo3::types::{PyDict, PyList, PyTuple};

type PyRelationshipPath = Vec<(String, String, Vec<String>, Vec<String>, String)>;

static REGISTRY_CONTEXTVAR: GILOnceCell<Py<PyAny>> = GILOnceCell::new();

fn registry_contextvar(py: Python<'_>) -> PyResult<&Py<PyAny>> {
    REGISTRY_CONTEXTVAR.get_or_try_init(py, || {
        let contextvars = py.import("contextvars")?;
        let contextvar_type = contextvars.getattr("ContextVar")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("default", py.None())?;
        let contextvar = contextvar_type.call(("sidemantic_rs_current_layer",), Some(&kwargs))?;
        Ok(contextvar.unbind())
    })
}

/// Rewrite SQL using semantic models provided as YAML.
///
/// This is stateless on purpose so Python tests can call it safely across runs.
#[pyfunction]
fn rewrite_with_yaml(yaml: &str, sql: &str) -> PyResult<String> {
    rewrite_with_yaml_native(yaml, sql).map_err(|e| match e {
        SidemanticError::Validation(_)
        | SidemanticError::YamlParse(_)
        | SidemanticError::InvalidConfig(_)
        | SidemanticError::FileNotFound(_)
        | SidemanticError::Io(_) => PyValueError::new_err(e.to_string()),
        _ => PyRuntimeError::new_err(e.to_string()),
    })
}

#[pyfunction]
fn is_sql_template(sql: &str) -> bool {
    is_sql_template_native(sql)
}

#[pyfunction]
fn render_sql_template(template_str: &str, context_yaml: &str) -> PyResult<String> {
    render_sql_template_native(template_str, context_yaml)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

#[pyfunction]
fn format_parameter_value_with_yaml(parameter_yaml: &str, value_yaml: &str) -> PyResult<String> {
    format_parameter_value_with_yaml_native(parameter_yaml, value_yaml)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

#[pyfunction]
fn interpolate_sql_with_parameters(
    sql: &str,
    parameters_yaml: &str,
    values_yaml: &str,
) -> PyResult<String> {
    interpolate_sql_with_parameters_with_yaml_native(sql, parameters_yaml, values_yaml)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

#[pyfunction]
fn evaluate_table_calculation_expression(expr: &str) -> PyResult<f64> {
    evaluate_table_calculation_expression_native(expr)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

#[pyfunction]
fn validate_table_formula_expression(expression: &str) -> PyResult<bool> {
    validate_table_formula_expression_native(expression)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

#[pyfunction]
#[pyo3(signature = (measure_expr, primary_key, agg_type, model_alias = None, dialect = "duckdb"))]
fn build_symmetric_aggregate_sql(
    measure_expr: &str,
    primary_key: &str,
    agg_type: &str,
    model_alias: Option<&str>,
    dialect: &str,
) -> PyResult<String> {
    build_symmetric_aggregate_sql_native(measure_expr, primary_key, agg_type, model_alias, dialect)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

#[pyfunction]
fn needs_symmetric_aggregate(relationship: &str, is_base_model: bool) -> bool {
    needs_symmetric_aggregate_native(relationship, is_base_model)
}

#[pyfunction]
#[pyo3(signature = (expr, dialect = "duckdb"))]
fn parse_relative_date(expr: &str, dialect: &str) -> Option<String> {
    parse_relative_date_native(expr, dialect)
}

#[pyfunction]
#[pyo3(signature = (expr, column = "date_col", dialect = "duckdb"))]
fn relative_date_to_range(expr: &str, column: &str, dialect: &str) -> Option<String> {
    relative_date_to_range_native(expr, column, dialect)
}

#[pyfunction]
fn is_relative_date(expr: &str) -> bool {
    is_relative_date_native(expr)
}

#[pyfunction]
#[pyo3(signature = (comparison_type, offset = None, offset_unit = None))]
fn time_comparison_offset_interval(
    comparison_type: &str,
    offset: Option<i64>,
    offset_unit: Option<&str>,
) -> PyResult<(i64, String)> {
    time_comparison_offset_interval_native(comparison_type, offset, offset_unit)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

#[pyfunction]
#[pyo3(signature = (comparison_type, offset = None, offset_unit = None))]
fn time_comparison_sql_offset(
    comparison_type: &str,
    offset: Option<i64>,
    offset_unit: Option<&str>,
) -> PyResult<String> {
    time_comparison_sql_offset_native(comparison_type, offset, offset_unit)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

#[pyfunction]
fn trailing_period_sql_interval(amount: i64, unit: &str) -> PyResult<String> {
    trailing_period_sql_interval_native(amount, unit)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

#[pyfunction]
#[pyo3(signature = (comparison_type, calculation, current_metric_sql, time_dimension, offset = None, offset_unit = None))]
fn generate_time_comparison_sql(
    comparison_type: &str,
    calculation: &str,
    current_metric_sql: &str,
    time_dimension: &str,
    offset: Option<i64>,
    offset_unit: Option<&str>,
) -> PyResult<String> {
    generate_time_comparison_sql_native(
        comparison_type,
        calculation,
        current_metric_sql,
        time_dimension,
        offset,
        offset_unit,
    )
    .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Compile a semantic query using sidemantic-rs SQL generator.
///
/// `query_yaml` payload supports:
/// - metrics: [str]
/// - dimensions: [str]
/// - filters: [str]
/// - segments: [str]
/// - order_by: [str]
/// - limit: int | null
#[pyfunction]
fn compile_with_yaml(yaml: &str, query_yaml: &str) -> PyResult<String> {
    compile_with_yaml_query_native(yaml, query_yaml).map_err(|e| match e {
        SidemanticError::Validation(_)
        | SidemanticError::YamlParse(_)
        | SidemanticError::InvalidConfig(_)
        | SidemanticError::FileNotFound(_)
        | SidemanticError::Io(_) => PyValueError::new_err(e.to_string()),
        _ => PyRuntimeError::new_err(e.to_string()),
    })
}

/// Parse native YAML definitions and return a serialized graph payload.
#[pyfunction]
fn load_graph_with_yaml(yaml: &str) -> PyResult<String> {
    load_graph_with_yaml_native(yaml).map_err(|e| match e {
        SidemanticError::Validation(_)
        | SidemanticError::YamlParse(_)
        | SidemanticError::InvalidConfig(_)
        | SidemanticError::FileNotFound(_)
        | SidemanticError::Io(_) => PyValueError::new_err(e.to_string()),
        _ => PyRuntimeError::new_err(e.to_string()),
    })
}

/// Parse SQL file content definitions and return a serialized graph payload.
#[pyfunction]
fn load_graph_with_sql(sql_content: &str) -> PyResult<String> {
    load_graph_with_sql_native(sql_content).map_err(|e| match e {
        SidemanticError::Validation(_)
        | SidemanticError::YamlParse(_)
        | SidemanticError::InvalidConfig(_)
        | SidemanticError::FileNotFound(_)
        | SidemanticError::Io(_) => PyValueError::new_err(e.to_string()),
        _ => PyRuntimeError::new_err(e.to_string()),
    })
}

/// Parse supported graph definitions from a directory and return a serialized graph payload.
#[pyfunction]
fn load_graph_from_directory(path: &str) -> PyResult<String> {
    load_graph_from_directory_native(path).map_err(|e| match e {
        SidemanticError::Validation(_)
        | SidemanticError::YamlParse(_)
        | SidemanticError::InvalidConfig(_)
        | SidemanticError::FileNotFound(_)
        | SidemanticError::Io(_) => PyValueError::new_err(e.to_string()),
        _ => PyRuntimeError::new_err(e.to_string()),
    })
}

/// Parse SQL metric/segment definitions and return serialized payload.
#[pyfunction]
fn parse_sql_definitions_payload(sql: &str) -> PyResult<String> {
    parse_sql_definitions_payload_native(sql).map_err(|e| match e {
        SidemanticError::Validation(msg) => PyValueError::new_err(msg),
        SidemanticError::SqlGeneration(msg) => PyRuntimeError::new_err(msg),
        _ => PyRuntimeError::new_err(e.to_string()),
    })
}

/// Parse SQL graph definitions and return serialized payload.
#[pyfunction]
fn parse_sql_graph_definitions_payload(sql: &str) -> PyResult<String> {
    parse_sql_graph_definitions_payload_native(sql).map_err(|e| match e {
        SidemanticError::Validation(msg) => PyValueError::new_err(msg),
        SidemanticError::SqlGeneration(msg) => PyRuntimeError::new_err(msg),
        _ => PyRuntimeError::new_err(e.to_string()),
    })
}

/// Parse SQL model definition and return serialized model payload.
#[pyfunction]
fn parse_sql_model_payload(sql: &str) -> PyResult<String> {
    parse_sql_model_payload_native(sql).map_err(|e| match e {
        SidemanticError::Validation(msg) => PyValueError::new_err(msg),
        SidemanticError::SqlGeneration(msg) => PyRuntimeError::new_err(msg),
        _ => PyRuntimeError::new_err(e.to_string()),
    })
}

/// Parse raw SQL statement blocks and return serialized payload.
#[pyfunction]
fn parse_sql_statement_blocks_payload(sql: &str) -> PyResult<String> {
    parse_sql_statement_blocks_payload_native(sql).map_err(|e| match e {
        SidemanticError::Validation(msg) => PyValueError::new_err(msg),
        SidemanticError::SqlGeneration(msg) => PyRuntimeError::new_err(msg),
        _ => PyRuntimeError::new_err(e.to_string()),
    })
}

/// Set current layer in a Rust-owned ContextVar.
#[pyfunction]
#[pyo3(signature = (layer = None))]
fn registry_set_current_layer(py: Python<'_>, layer: Option<Py<PyAny>>) -> PyResult<()> {
    let contextvar = registry_contextvar(py)?.bind(py);
    match layer {
        Some(value) => {
            contextvar.call_method1("set", (value.bind(py),))?;
        }
        None => {
            contextvar.call_method1("set", (py.None(),))?;
        }
    }
    Ok(())
}

/// Get current layer from a Rust-owned ContextVar.
#[pyfunction]
fn registry_get_current_layer(py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
    let contextvar = registry_contextvar(py)?.bind(py);
    let value = contextvar.call_method0("get")?;
    if value.is_none() {
        Ok(None)
    } else {
        Ok(Some(value.unbind()))
    }
}

/// Validate a semantic query using sidemantic-rs graph semantics.
///
/// Returns a Python-compatible list of validation error strings.
#[pyfunction]
fn validate_query_with_yaml(yaml: &str, query_yaml: &str) -> PyResult<Vec<String>> {
    validate_query_with_yaml_native(yaml, query_yaml).map_err(|e| match e {
        SidemanticError::Validation(_)
        | SidemanticError::YamlParse(_)
        | SidemanticError::InvalidConfig(_)
        | SidemanticError::FileNotFound(_)
        | SidemanticError::Io(_) => PyValueError::new_err(e.to_string()),
        _ => PyRuntimeError::new_err(e.to_string()),
    })
}

/// Validate query metric/dimension references against model YAML.
#[pyfunction]
fn validate_query_references(
    yaml: &str,
    metrics: Vec<String>,
    dimensions: Vec<String>,
) -> PyResult<Vec<String>> {
    validate_query_references_with_yaml_native(yaml, &metrics, &dimensions).map_err(|e| match e {
        SidemanticError::Validation(_)
        | SidemanticError::YamlParse(_)
        | SidemanticError::InvalidConfig(_)
        | SidemanticError::FileNotFound(_)
        | SidemanticError::Io(_) => PyValueError::new_err(e.to_string()),
        _ => PyRuntimeError::new_err(e.to_string()),
    })
}

/// Generate materialization SQL for a model pre-aggregation using sidemantic-rs schema.
#[pyfunction]
fn generate_preaggregation_materialization_sql(
    yaml: &str,
    model_name: &str,
    preagg_name: &str,
) -> PyResult<String> {
    generate_preaggregation_materialization_sql_with_yaml_native(yaml, model_name, preagg_name)
        .map_err(|e| match e {
            SidemanticError::Validation(_)
            | SidemanticError::YamlParse(_)
            | SidemanticError::InvalidConfig(_)
            | SidemanticError::FileNotFound(_)
            | SidemanticError::Io(_) => PyValueError::new_err(e.to_string()),
            _ => PyRuntimeError::new_err(e.to_string()),
        })
}

/// Validate engine refresh SQL compatibility with materialized-view restrictions.
#[pyfunction]
fn validate_engine_refresh_sql_compatibility(
    source_sql: &str,
    dialect: &str,
) -> (bool, Option<String>) {
    validate_engine_refresh_sql_compatibility_native(source_sql, dialect)
}

/// Build SQL statements for a pre-aggregation refresh operation.
#[pyfunction]
#[pyo3(signature = (
    mode,
    table_name,
    source_sql,
    watermark_column = None,
    from_watermark = None,
    lookback = None,
    dialect = None,
    refresh_every = None
))]
#[allow(clippy::too_many_arguments)]
fn build_preaggregation_refresh_statements(
    mode: &str,
    table_name: &str,
    source_sql: &str,
    watermark_column: Option<&str>,
    from_watermark: Option<&str>,
    lookback: Option<&str>,
    dialect: Option<&str>,
    refresh_every: Option<&str>,
) -> PyResult<Vec<String>> {
    build_preaggregation_refresh_statements_native(
        mode,
        table_name,
        source_sql,
        watermark_column,
        from_watermark,
        lookback,
        dialect,
        refresh_every,
    )
    .map_err(map_refresh_planner_error)
}

/// Resolve refresh mode from explicit value or incremental default flag.
#[pyfunction]
#[pyo3(signature = (mode = None, refresh_incremental = false))]
fn resolve_preaggregation_refresh_mode(
    mode: Option<&str>,
    refresh_incremental: bool,
) -> PyResult<String> {
    resolve_preaggregation_refresh_mode_native(mode, refresh_incremental)
        .map_err(map_refresh_planner_error)
}

/// Validate pre-aggregation refresh mode requirements.
#[pyfunction]
#[pyo3(signature = (mode, watermark_column = None, dialect = None))]
fn validate_preaggregation_refresh_request(
    mode: &str,
    watermark_column: Option<&str>,
    dialect: Option<&str>,
) -> PyResult<bool> {
    validate_preaggregation_refresh_request_native(mode, watermark_column, dialect)
        .map_err(map_refresh_planner_error)?;
    Ok(true)
}

/// Plan pre-aggregation refresh execution mode and branch requirements.
#[pyfunction]
#[pyo3(signature = (mode = None, refresh_incremental = false, watermark_column = None, dialect = None))]
fn plan_preaggregation_refresh_execution(
    py: Python<'_>,
    mode: Option<&str>,
    refresh_incremental: bool,
    watermark_column: Option<&str>,
    dialect: Option<&str>,
) -> PyResult<Py<PyAny>> {
    let plan = plan_preaggregation_refresh_execution_native(
        mode,
        refresh_incremental,
        watermark_column,
        dialect,
    )
    .map_err(map_refresh_planner_error)?;

    let payload = PyDict::new(py);
    payload.set_item("mode", &plan.mode)?;
    payload.set_item("requires_prior_watermark", plan.requires_prior_watermark)?;
    payload.set_item(
        "requires_merge_table_existence_check",
        plan.requires_merge_table_existence_check,
    )?;
    payload.set_item("include_new_watermark", plan.include_new_watermark)?;
    Ok(payload.into_any().unbind())
}

/// Compute recommender benefit score for a serialized pattern payload.
#[pyfunction]
#[pyo3(signature = (pattern_json, count))]
fn calculate_preaggregation_benefit_score(pattern_json: &str, count: usize) -> PyResult<f64> {
    calculate_preaggregation_benefit_score_native(pattern_json, count)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Generate recommender name for a serialized pattern payload.
#[pyfunction]
fn generate_preaggregation_name(pattern_json: &str) -> PyResult<String> {
    generate_preaggregation_name_native(pattern_json)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Parse instrumented query comments into pre-aggregation pattern counts.
#[pyfunction]
fn extract_preaggregation_patterns(queries: Vec<String>) -> PyResult<String> {
    extract_preaggregation_patterns_native(queries)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Build pre-aggregation recommendations from known pattern counts.
#[pyfunction]
#[pyo3(signature = (patterns_json, min_query_count, min_benefit_score, top_n = None))]
fn recommend_preaggregation_patterns(
    patterns_json: &str,
    min_query_count: usize,
    min_benefit_score: f64,
    top_n: Option<usize>,
) -> PyResult<String> {
    recommend_preaggregation_patterns_native(
        patterns_json,
        min_query_count,
        min_benefit_score,
        top_n,
    )
    .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Summarize pattern counts for recommendation reporting.
#[pyfunction]
fn summarize_preaggregation_patterns(
    patterns_json: &str,
    min_query_count: usize,
) -> PyResult<String> {
    summarize_preaggregation_patterns_native(patterns_json, min_query_count)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Convert a recommendation payload into a pre-aggregation definition payload.
#[pyfunction]
fn generate_preaggregation_definition(recommendation_json: &str) -> PyResult<String> {
    generate_preaggregation_definition_native(recommendation_json)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

fn execute_sql<'py>(connection: &Bound<'py, PyAny>, sql: &str) -> PyResult<Bound<'py, PyAny>> {
    connection.call_method1("execute", (sql,))
}

fn extract_first_row_value(row: &Bound<'_, PyAny>) -> PyResult<Option<Py<PyAny>>> {
    if row.is_none() {
        return Ok(None);
    }

    if let Ok(tuple) = row.downcast::<PyTuple>() {
        if tuple.is_empty() {
            return Ok(None);
        }
        let item = tuple.get_item(0)?;
        if item.is_none() {
            Ok(None)
        } else {
            Ok(Some(item.unbind()))
        }
    } else if let Ok(list) = row.downcast::<PyList>() {
        if list.is_empty() {
            return Ok(None);
        }
        let item = list.get_item(0)?;
        if item.is_none() {
            Ok(None)
        } else {
            Ok(Some(item.unbind()))
        }
    } else {
        let item = row.get_item(0)?;
        if item.is_none() {
            Ok(None)
        } else {
            Ok(Some(item.unbind()))
        }
    }
}

fn get_current_watermark(
    connection: &Bound<'_, PyAny>,
    table_name: &str,
    watermark_column: &str,
) -> Option<Py<PyAny>> {
    let sql = format!("SELECT MAX({watermark_column}) as max_watermark FROM {table_name}");
    let cursor = execute_sql(connection, &sql).ok()?;
    let row = cursor.call_method0("fetchone").ok()?;
    extract_first_row_value(&row).ok()?
}

fn py_value_to_i64(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    if let Ok(v) = value.extract::<i64>() {
        return Ok(v);
    }
    if let Ok(v) = value.extract::<u64>() {
        return i64::try_from(v)
            .map_err(|_| PyRuntimeError::new_err("count value exceeded i64 range"));
    }
    if let Ok(v) = value.extract::<f64>() {
        return Ok(v as i64);
    }
    Err(PyRuntimeError::new_err(
        "failed to parse numeric result from database cursor",
    ))
}

fn watermark_to_refresh_value(value: &Bound<'_, PyAny>) -> PyResult<String> {
    let rendered = value.str()?.to_str()?.to_string();
    if rendered.len() >= 2 && rendered.starts_with('\'') && rendered.ends_with('\'') {
        return Ok(rendered[1..rendered.len() - 1].to_string());
    }
    Ok(rendered)
}

fn build_refresh_result_dict(
    py: Python<'_>,
    mode: &str,
    rows_inserted: i64,
    rows_updated: i64,
    new_watermark: Option<Py<PyAny>>,
) -> PyResult<Py<PyAny>> {
    let payload = PyDict::new(py);
    payload.set_item("mode", mode)?;
    payload.set_item("rows_inserted", rows_inserted)?;
    payload.set_item("rows_updated", rows_updated)?;
    if let Some(value) = new_watermark {
        payload.set_item("new_watermark", value.bind(py))?;
    } else {
        payload.set_item("new_watermark", py.None())?;
    }
    Ok(payload.into_any().unbind())
}

fn map_refresh_planner_error(err: SidemanticError) -> PyErr {
    match err {
        SidemanticError::Validation(message) => {
            if let Some(rest) =
                message.strip_prefix("unsupported dialect for engine refresh mode: ")
            {
                let dialect = rest.split('.').next().unwrap_or(rest).trim();
                return PyValueError::new_err(format!(
                    "Unsupported dialect for engine mode: {dialect}"
                ));
            }
            PyValueError::new_err(message)
        }
        other => PyRuntimeError::new_err(other.to_string()),
    }
}

fn table_exists(connection: &Bound<'_, PyAny>, table_name: &str) -> bool {
    execute_sql(connection, &format!("SELECT 1 FROM {table_name} LIMIT 1")).is_ok()
}

fn execute_refresh_statements(
    connection: &Bound<'_, PyAny>,
    statements: &[String],
) -> PyResult<()> {
    for statement in statements {
        execute_sql(connection, statement)?;
    }
    Ok(())
}

/// Execute a pre-aggregation refresh strategy using a Python DB connection.
#[pyfunction]
#[pyo3(signature = (
    connection,
    source_sql,
    table_name,
    mode = None,
    watermark_column = None,
    lookback = None,
    from_watermark = None,
    to_watermark = None,
    dialect = None,
    refresh_incremental = false,
    refresh_every = None
))]
#[allow(clippy::too_many_arguments)]
fn refresh_preaggregation(
    py: Python<'_>,
    connection: &Bound<'_, PyAny>,
    source_sql: &str,
    table_name: &str,
    mode: Option<&str>,
    watermark_column: Option<&str>,
    lookback: Option<&str>,
    from_watermark: Option<Py<PyAny>>,
    to_watermark: Option<Py<PyAny>>,
    dialect: Option<&str>,
    refresh_incremental: bool,
    refresh_every: Option<&str>,
) -> PyResult<Py<PyAny>> {
    let _ = to_watermark;
    let refresh_plan = plan_preaggregation_refresh_execution_native(
        mode,
        refresh_incremental,
        watermark_column,
        dialect,
    )
    .map_err(map_refresh_planner_error)?;

    let mut normalized_watermark: Option<String> = None;
    let mut merge_target_table_existed = false;
    if refresh_plan.requires_prior_watermark {
        let watermark_column = watermark_column.unwrap_or_default();
        let mut current_watermark = from_watermark;
        if current_watermark.is_none() {
            current_watermark = get_current_watermark(connection, table_name, watermark_column);
        }
        if let Some(value) = current_watermark {
            normalized_watermark = Some(watermark_to_refresh_value(value.bind(py))?);
        }
        if refresh_plan.requires_merge_table_existence_check {
            merge_target_table_existed = table_exists(connection, table_name);
        }
    }

    let statements = build_preaggregation_refresh_statements_native(
        &refresh_plan.mode,
        table_name,
        source_sql,
        watermark_column,
        normalized_watermark.as_deref(),
        lookback,
        dialect,
        refresh_every,
    )
    .map_err(map_refresh_planner_error)?;
    execute_refresh_statements(connection, &statements)?;

    let full_rows_inserted = if refresh_plan.mode == "full" {
        let count_cursor = execute_sql(connection, &format!("SELECT COUNT(*) FROM {table_name}"))?;
        let count_row = count_cursor.call_method0("fetchone")?;
        if let Some(value) = extract_first_row_value(&count_row)? {
            py_value_to_i64(value.bind(py))?
        } else {
            0
        }
    } else {
        0
    };

    let result_shape = shape_preaggregation_refresh_result_native(
        &refresh_plan.mode,
        merge_target_table_existed,
        full_rows_inserted,
    )
    .map_err(map_refresh_planner_error)?;
    let new_watermark = if refresh_plan.include_new_watermark {
        let watermark_column = watermark_column.unwrap_or_default();
        get_current_watermark(connection, table_name, watermark_column)
    } else {
        None
    };
    build_refresh_result_dict(
        py,
        &result_shape.mode,
        result_shape.rows_inserted,
        result_shape.rows_updated,
        new_watermark,
    )
}

/// Validate models YAML by loading it into a Rust semantic graph.
#[pyfunction]
fn validate_models_yaml(yaml: &str) -> PyResult<bool> {
    validate_models_yaml_native(yaml).map_err(|e| match e {
        SidemanticError::Validation(_)
        | SidemanticError::YamlParse(_)
        | SidemanticError::InvalidConfig(_)
        | SidemanticError::FileNotFound(_)
        | SidemanticError::Io(_) => PyValueError::new_err(e.to_string()),
        _ => PyRuntimeError::new_err(e.to_string()),
    })
}

/// Validate model payload shape in Rust.
#[pyfunction]
fn validate_model_payload(model_yaml: &str) -> PyResult<bool> {
    validate_model_payload_native(model_yaml).map_err(|e| match e {
        SidemanticError::Validation(msg) => PyValueError::new_err(msg),
        _ => PyRuntimeError::new_err(e.to_string()),
    })
}

/// Resolve model inheritance in Rust and return resolved models as YAML.
#[pyfunction]
fn resolve_model_inheritance(yaml: &str) -> PyResult<String> {
    resolve_model_inheritance_with_yaml_native(yaml).map_err(|e| match e {
        SidemanticError::Validation(msg) => PyValueError::new_err(msg),
        SidemanticError::SqlGeneration(msg) => PyRuntimeError::new_err(msg),
        _ => PyRuntimeError::new_err(e.to_string()),
    })
}

/// Resolve metric inheritance in Rust and return resolved metrics as YAML.
#[pyfunction]
fn resolve_metric_inheritance(metrics_yaml: &str) -> PyResult<String> {
    resolve_metric_inheritance_native(metrics_yaml).map_err(|e| match e {
        SidemanticError::Validation(msg) => PyValueError::new_err(msg),
        SidemanticError::SqlGeneration(msg) => PyRuntimeError::new_err(msg),
        _ => PyRuntimeError::new_err(e.to_string()),
    })
}

/// Validate metric payload shape in Rust.
#[pyfunction]
fn validate_metric_payload(metric_yaml: &str) -> PyResult<bool> {
    validate_metric_payload_native(metric_yaml).map_err(|e| match e {
        SidemanticError::Validation(msg) => PyValueError::new_err(msg),
        _ => PyRuntimeError::new_err(e.to_string()),
    })
}

/// Validate parameter payload shape in Rust.
#[pyfunction]
fn validate_parameter_payload(parameter_yaml: &str) -> PyResult<bool> {
    validate_parameter_payload_native(parameter_yaml).map_err(|e| match e {
        SidemanticError::Validation(msg) => PyValueError::new_err(msg),
        _ => PyRuntimeError::new_err(e.to_string()),
    })
}

/// Validate table-calculation payload shape in Rust.
#[pyfunction]
fn validate_table_calculation_payload(calculation_yaml: &str) -> PyResult<bool> {
    validate_table_calculation_payload_native(calculation_yaml).map_err(|e| match e {
        SidemanticError::Validation(msg) => PyValueError::new_err(msg),
        _ => PyRuntimeError::new_err(e.to_string()),
    })
}

/// Export semantic graph catalog metadata in Postgres-compatible format.
#[pyfunction]
#[pyo3(signature = (yaml, schema = "public"))]
fn generate_catalog_metadata(yaml: &str, schema: &str) -> PyResult<String> {
    generate_catalog_metadata_with_yaml_native(yaml, schema).map_err(|e| match e {
        SidemanticError::Validation(_)
        | SidemanticError::YamlParse(_)
        | SidemanticError::InvalidConfig(_)
        | SidemanticError::FileNotFound(_)
        | SidemanticError::Io(_) => PyValueError::new_err(e.to_string()),
        _ => PyRuntimeError::new_err(e.to_string()),
    })
}

#[cfg(feature = "python-adbc")]
fn adbc_error(context: &str, err: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(format!("{context}: {err}"))
}

#[cfg(feature = "python-adbc")]
fn py_value_to_option_value(value: &Bound<'_, PyAny>) -> PyResult<OptionValue> {
    if value.is_instance_of::<PyBool>() {
        let flag = value.extract::<bool>()?;
        let text = if flag { "true" } else { "false" };
        return Ok(OptionValue::String(text.into()));
    }

    if value.is_instance_of::<PyString>() {
        return Ok(OptionValue::String(value.extract::<String>()?));
    }

    if value.is_instance_of::<PyBytes>() {
        return Ok(OptionValue::Bytes(value.extract::<Vec<u8>>()?));
    }

    if let Ok(number) = value.extract::<i64>() {
        return Ok(OptionValue::Int(number));
    }

    if let Ok(number) = value.extract::<f64>() {
        return Ok(OptionValue::Double(number));
    }

    let text = value.str()?.to_str()?.to_owned();
    Ok(OptionValue::String(text))
}

#[cfg(feature = "python-adbc")]
fn merge_database_options(
    uri: Option<&str>,
    db_kwargs: Option<&Bound<'_, PyDict>>,
) -> PyResult<Vec<(OptionDatabase, OptionValue)>> {
    let mut options: Vec<(OptionDatabase, OptionValue)> = Vec::new();

    if let Some(kwargs) = db_kwargs {
        for (key, value) in kwargs.iter() {
            if value.is_none() {
                continue;
            }
            let key = key.extract::<String>()?;
            if uri.is_some() && key == constants::ADBC_OPTION_URI {
                continue;
            }
            options.push((
                OptionDatabase::from(key.as_str()),
                py_value_to_option_value(&value)?,
            ));
        }
    }

    Ok(options)
}

#[cfg(feature = "python-adbc")]
fn merge_connection_options(
    conn_kwargs: Option<&Bound<'_, PyDict>>,
    autocommit: bool,
) -> PyResult<Vec<(OptionConnection, OptionValue)>> {
    let mut options: Vec<(OptionConnection, OptionValue)> = Vec::new();

    if let Some(kwargs) = conn_kwargs {
        for (key, value) in kwargs.iter() {
            if value.is_none() {
                continue;
            }
            let key = key.extract::<String>()?;
            options.push((
                OptionConnection::from(key.as_str()),
                py_value_to_option_value(&value)?,
            ));
        }
    }

    options.push((
        OptionConnection::AutoCommit,
        OptionValue::String(if autocommit { "true" } else { "false" }.to_owned()),
    ));
    Ok(options)
}

#[cfg(feature = "python-adbc")]
fn adbc_value_to_py(py: Python<'_>, value: &AdbcValue) -> PyResult<Py<PyAny>> {
    match value {
        AdbcValue::Null => Ok(py.None()),
        AdbcValue::Bool(v) => Ok(PyBool::new(py, *v).to_owned().into_any().unbind()),
        AdbcValue::I64(v) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        AdbcValue::U64(v) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        AdbcValue::F64(v) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        AdbcValue::String(v) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        AdbcValue::Bytes(v) => Ok(PyBytes::new(py, v).into_any().unbind()),
    }
}

/// Execute SQL via the Rust ADBC driver manager and return rows/columns.
#[cfg(feature = "python-adbc")]
#[pyfunction]
#[pyo3(signature = (driver, sql, uri=None, entrypoint=None, db_kwargs=None, conn_kwargs=None, autocommit=true))]
#[allow(clippy::too_many_arguments)]
fn execute_with_adbc(
    py: Python<'_>,
    driver: &str,
    sql: &str,
    uri: Option<&str>,
    entrypoint: Option<&str>,
    db_kwargs: Option<&Bound<'_, PyDict>>,
    conn_kwargs: Option<&Bound<'_, PyDict>>,
    autocommit: bool,
) -> PyResult<Py<PyAny>> {
    let database_options = merge_database_options(uri, db_kwargs)?;
    let connection_options = merge_connection_options(conn_kwargs, autocommit)?;
    let payload = execute_with_adbc_native(AdbcExecutionRequest {
        driver: driver.to_string(),
        sql: sql.to_string(),
        uri: uri.map(str::to_owned),
        entrypoint: entrypoint.map(str::to_owned),
        database_options,
        connection_options,
    })
    .map_err(|e| adbc_error("rust ADBC execution failed", e))?;

    let mut rows: Vec<Py<PyAny>> = Vec::with_capacity(payload.rows.len());
    for row in &payload.rows {
        let mut values: Vec<Py<PyAny>> = Vec::with_capacity(row.len());
        for value in row {
            values.push(adbc_value_to_py(py, value)?);
        }
        let tuple = PyTuple::new(py, values)?;
        rows.push(tuple.into_any().unbind());
    }

    let result = PyDict::new(py);
    result.set_item("columns", payload.columns)?;
    result.set_item("rows", rows)?;
    Ok(result.into_any().unbind())
}

/// Report disabled ADBC execution in lightweight Python builds.
#[cfg(not(feature = "python-adbc"))]
#[pyfunction]
#[pyo3(signature = (driver, sql, uri=None, entrypoint=None, db_kwargs=None, conn_kwargs=None, autocommit=true))]
#[allow(unused_variables, clippy::too_many_arguments)]
fn execute_with_adbc(
    driver: &str,
    sql: &str,
    uri: Option<&str>,
    entrypoint: Option<&str>,
    db_kwargs: Option<&Bound<'_, PyDict>>,
    conn_kwargs: Option<&Bound<'_, PyDict>>,
    autocommit: bool,
) -> PyResult<()> {
    Err(PyRuntimeError::new_err(
        "ADBC execution support is not enabled. Rebuild sidemantic-rs with feature 'python-adbc' to use execute_with_adbc.",
    ))
}

/// Detect adapter kind from file path and content.
#[pyfunction]
fn detect_adapter_kind(path: &str, content: &str) -> Option<String> {
    detect_adapter_kind_native(path, content)
}

/// Extract column references from a SQL expression.
#[pyfunction]
fn extract_column_references(sql_expr: &str) -> Vec<String> {
    extract_column_references_native(sql_expr)
}

/// Analyze query components for migrator helper extraction.
#[pyfunction]
fn analyze_migrator_query(sql_query: &str) -> PyResult<String> {
    analyze_migrator_query_native(sql_query).map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Auto-detect chart x/y columns from ordered columns + numeric flags.
#[pyfunction]
fn chart_auto_detect_columns(
    columns: Vec<String>,
    numeric_flags: Vec<bool>,
) -> PyResult<(String, Vec<String>)> {
    chart_auto_detect_columns_native(&columns, &numeric_flags)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Select chart type from x-column semantics and y-count.
#[pyfunction]
fn chart_select_type(x: &str, x_value_kind: &str, y_count: usize) -> String {
    chart_select_type_native(x, x_value_kind, y_count)
}

/// Format chart labels using Python-compatible naming rules.
#[pyfunction]
fn chart_format_label(column: &str) -> String {
    chart_format_label_native(column)
}

/// Determine chart encoding type from column name.
#[pyfunction]
fn chart_encoding_type(column: &str) -> String {
    chart_encoding_type_native(column)
}

/// Extract metric dependencies from a metric payload, with optional graph/context resolution.
#[pyfunction]
#[pyo3(signature = (metric_yaml, models_yaml = None, model_context = None))]
fn extract_metric_dependencies(
    metric_yaml: &str,
    models_yaml: Option<&str>,
    model_context: Option<&str>,
) -> PyResult<Vec<String>> {
    extract_metric_dependencies_from_yaml_native(metric_yaml, models_yaml, model_context)
        .map_err(|e| PyValueError::new_err(format!("failed to extract metric dependencies: {e}")))
}

/// Parse a top-level simple metric aggregation expression.
///
/// Returns `(agg, inner_sql)` for simple aggregations like:
/// - `SUM(amount)` -> `("sum", Some("amount"))`
/// - `COUNT(*)` -> `("count", None)`
/// - `COUNT(DISTINCT user_id)` -> `("count_distinct", Some("user_id"))`
///
/// Returns `None` for non-simple/complex expressions.
#[pyfunction]
fn parse_simple_metric_aggregation(sql_expr: &str) -> Option<(String, Option<String>)> {
    parse_simple_metric_aggregation_native(sql_expr)
}

/// Convert a metric payload to SQL aggregation expression.
#[pyfunction]
fn metric_to_sql(metric_yaml: &str) -> PyResult<String> {
    metric_to_sql_native(metric_yaml).map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Resolve metric SQL expression with Python-compatible count fallback.
#[pyfunction]
fn metric_sql_expr(metric_yaml: &str) -> PyResult<String> {
    metric_sql_expr_native(metric_yaml).map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Determine whether metric payload represents a simple aggregation.
#[pyfunction]
fn metric_is_simple_aggregation(metric_yaml: &str) -> PyResult<bool> {
    metric_is_simple_aggregation_native(metric_yaml)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Resolve dimension SQL expression.
#[pyfunction]
fn dimension_sql_expr(dimension_yaml: &str) -> PyResult<String> {
    dimension_sql_expr_with_yaml_native(dimension_yaml)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Apply time granularity to a dimension SQL expression.
#[pyfunction]
fn dimension_with_granularity(dimension_yaml: &str, granularity: &str) -> PyResult<String> {
    dimension_with_granularity_with_yaml_native(dimension_yaml, granularity)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Get full hierarchy path from root to a given dimension.
#[pyfunction]
fn model_get_hierarchy_path(model_yaml: &str, dimension_name: &str) -> PyResult<Vec<String>> {
    model_get_hierarchy_path_with_yaml_native(model_yaml, dimension_name)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Get first child dimension in hierarchy order for a given dimension.
#[pyfunction]
fn model_get_drill_down(model_yaml: &str, dimension_name: &str) -> PyResult<Option<String>> {
    model_get_drill_down_with_yaml_native(model_yaml, dimension_name)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Get parent dimension for a given dimension.
#[pyfunction]
fn model_get_drill_up(model_yaml: &str, dimension_name: &str) -> PyResult<Option<String>> {
    model_get_drill_up_with_yaml_native(model_yaml, dimension_name)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Get dimension index by name.
#[pyfunction]
fn model_find_dimension_index(model_yaml: &str, name: &str) -> PyResult<Option<usize>> {
    model_find_dimension_index_with_yaml_native(model_yaml, name)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Get metric index by name.
#[pyfunction]
fn model_find_metric_index(model_yaml: &str, name: &str) -> PyResult<Option<usize>> {
    model_find_metric_index_with_yaml_native(model_yaml, name)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Get segment index by name.
#[pyfunction]
fn model_find_segment_index(model_yaml: &str, name: &str) -> PyResult<Option<usize>> {
    model_find_segment_index_with_yaml_native(model_yaml, name)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Get pre-aggregation index by name.
#[pyfunction]
fn model_find_pre_aggregation_index(model_yaml: &str, name: &str) -> PyResult<Option<usize>> {
    model_find_pre_aggregation_index_with_yaml_native(model_yaml, name)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Resolve Relationship.sql_expr via sidemantic-rs.
#[pyfunction]
fn relationship_sql_expr(relationship_yaml: &str) -> PyResult<String> {
    relationship_sql_expr_with_yaml_native(relationship_yaml)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Resolve Relationship.related_key via sidemantic-rs.
#[pyfunction]
fn relationship_related_key(relationship_yaml: &str) -> PyResult<String> {
    relationship_related_key_with_yaml_native(relationship_yaml)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Resolve Relationship.foreign_key_columns via sidemantic-rs.
#[pyfunction]
fn relationship_foreign_key_columns(relationship_yaml: &str) -> PyResult<Vec<String>> {
    relationship_foreign_key_columns_with_yaml_native(relationship_yaml)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Resolve Relationship.primary_key_columns via sidemantic-rs.
#[pyfunction]
fn relationship_primary_key_columns(relationship_yaml: &str) -> PyResult<Vec<String>> {
    relationship_primary_key_columns_with_yaml_native(relationship_yaml)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Resolve Segment.get_sql via sidemantic-rs.
#[pyfunction]
fn segment_get_sql(segment_yaml: &str, model_alias: &str) -> PyResult<String> {
    segment_get_sql_with_yaml_native(segment_yaml, model_alias)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Find join path between two models using Python-compatible graph semantics.
#[pyfunction]
fn find_relationship_path_with_yaml(
    graph_yaml: &str,
    from_model: &str,
    to_model: &str,
) -> PyResult<PyRelationshipPath> {
    find_relationship_path_with_yaml_native(graph_yaml, from_model, to_model).map_err(|e| match e {
        RelationshipPathError::ModelNotFound(model_name) => {
            PyKeyError::new_err(format!("Model {model_name} not found"))
        }
        RelationshipPathError::NoJoinPath {
            from_model,
            to_model,
        } => PyValueError::new_err(format!(
            "No join path found between {from_model} and {to_model}"
        )),
        RelationshipPathError::InvalidPayload(err) => {
            PyValueError::new_err(format!("failed to parse graph payload: {err}"))
        }
    })
}

/// Parse a qualified semantic reference using Rust graph semantics.
#[pyfunction]
fn parse_reference_with_yaml(
    yaml: &str,
    reference: &str,
) -> PyResult<(String, String, Option<String>)> {
    parse_reference_with_yaml_native(yaml, reference).map_err(|e| match e {
        SidemanticError::Validation(_)
        | SidemanticError::YamlParse(_)
        | SidemanticError::InvalidConfig(_)
        | SidemanticError::FileNotFound(_)
        | SidemanticError::Io(_) => PyValueError::new_err(e.to_string()),
        _ => PyRuntimeError::new_err(e.to_string()),
    })
}

/// Discover model names referenced by dimensions/measures.
#[pyfunction]
fn find_models_for_query(dimensions: Vec<String>, measures: Vec<String>) -> Vec<String> {
    find_models_for_query_native(&dimensions, &measures)
        .into_iter()
        .collect()
}

/// Discover model names referenced by dimensions/measures using graph payload context.
#[pyfunction]
fn find_models_for_query_with_yaml(
    yaml: &str,
    dimensions: Vec<String>,
    measures: Vec<String>,
) -> PyResult<Vec<String>> {
    find_models_for_query_with_yaml_native(yaml, &dimensions, &measures)
        .map(|models| models.into_iter().collect())
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Python module entrypoint.
#[pymodule]
fn sidemantic_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(rewrite_with_yaml, m)?)?;
    m.add_function(wrap_pyfunction!(compile_with_yaml, m)?)?;
    m.add_function(wrap_pyfunction!(load_graph_with_yaml, m)?)?;
    m.add_function(wrap_pyfunction!(load_graph_with_sql, m)?)?;
    m.add_function(wrap_pyfunction!(load_graph_from_directory, m)?)?;
    m.add_function(wrap_pyfunction!(parse_sql_definitions_payload, m)?)?;
    m.add_function(wrap_pyfunction!(parse_sql_graph_definitions_payload, m)?)?;
    m.add_function(wrap_pyfunction!(parse_sql_model_payload, m)?)?;
    m.add_function(wrap_pyfunction!(parse_sql_statement_blocks_payload, m)?)?;
    m.add_function(wrap_pyfunction!(registry_set_current_layer, m)?)?;
    m.add_function(wrap_pyfunction!(registry_get_current_layer, m)?)?;
    m.add_function(wrap_pyfunction!(validate_query_with_yaml, m)?)?;
    m.add_function(wrap_pyfunction!(validate_query_references, m)?)?;
    m.add_function(wrap_pyfunction!(
        generate_preaggregation_materialization_sql,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        validate_engine_refresh_sql_compatibility,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        build_preaggregation_refresh_statements,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(resolve_preaggregation_refresh_mode, m)?)?;
    m.add_function(wrap_pyfunction!(
        validate_preaggregation_refresh_request,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(plan_preaggregation_refresh_execution, m)?)?;
    m.add_function(wrap_pyfunction!(refresh_preaggregation, m)?)?;
    m.add_function(wrap_pyfunction!(validate_models_yaml, m)?)?;
    m.add_function(wrap_pyfunction!(validate_model_payload, m)?)?;
    m.add_function(wrap_pyfunction!(resolve_model_inheritance, m)?)?;
    m.add_function(wrap_pyfunction!(resolve_metric_inheritance, m)?)?;
    m.add_function(wrap_pyfunction!(validate_metric_payload, m)?)?;
    m.add_function(wrap_pyfunction!(validate_parameter_payload, m)?)?;
    m.add_function(wrap_pyfunction!(validate_table_calculation_payload, m)?)?;
    m.add_function(wrap_pyfunction!(extract_preaggregation_patterns, m)?)?;
    m.add_function(wrap_pyfunction!(recommend_preaggregation_patterns, m)?)?;
    m.add_function(wrap_pyfunction!(summarize_preaggregation_patterns, m)?)?;
    m.add_function(wrap_pyfunction!(calculate_preaggregation_benefit_score, m)?)?;
    m.add_function(wrap_pyfunction!(generate_preaggregation_name, m)?)?;
    m.add_function(wrap_pyfunction!(generate_preaggregation_definition, m)?)?;
    m.add_function(wrap_pyfunction!(is_sql_template, m)?)?;
    m.add_function(wrap_pyfunction!(render_sql_template, m)?)?;
    m.add_function(wrap_pyfunction!(format_parameter_value_with_yaml, m)?)?;
    m.add_function(wrap_pyfunction!(interpolate_sql_with_parameters, m)?)?;
    m.add_function(wrap_pyfunction!(evaluate_table_calculation_expression, m)?)?;
    m.add_function(wrap_pyfunction!(validate_table_formula_expression, m)?)?;
    m.add_function(wrap_pyfunction!(build_symmetric_aggregate_sql, m)?)?;
    m.add_function(wrap_pyfunction!(needs_symmetric_aggregate, m)?)?;
    m.add_function(wrap_pyfunction!(parse_relative_date, m)?)?;
    m.add_function(wrap_pyfunction!(relative_date_to_range, m)?)?;
    m.add_function(wrap_pyfunction!(is_relative_date, m)?)?;
    m.add_function(wrap_pyfunction!(time_comparison_offset_interval, m)?)?;
    m.add_function(wrap_pyfunction!(time_comparison_sql_offset, m)?)?;
    m.add_function(wrap_pyfunction!(trailing_period_sql_interval, m)?)?;
    m.add_function(wrap_pyfunction!(generate_time_comparison_sql, m)?)?;
    m.add_function(wrap_pyfunction!(execute_with_adbc, m)?)?;
    m.add_function(wrap_pyfunction!(detect_adapter_kind, m)?)?;
    m.add_function(wrap_pyfunction!(extract_column_references, m)?)?;
    m.add_function(wrap_pyfunction!(analyze_migrator_query, m)?)?;
    m.add_function(wrap_pyfunction!(chart_auto_detect_columns, m)?)?;
    m.add_function(wrap_pyfunction!(chart_select_type, m)?)?;
    m.add_function(wrap_pyfunction!(chart_format_label, m)?)?;
    m.add_function(wrap_pyfunction!(chart_encoding_type, m)?)?;
    m.add_function(wrap_pyfunction!(extract_metric_dependencies, m)?)?;
    m.add_function(wrap_pyfunction!(parse_simple_metric_aggregation, m)?)?;
    m.add_function(wrap_pyfunction!(metric_to_sql, m)?)?;
    m.add_function(wrap_pyfunction!(metric_sql_expr, m)?)?;
    m.add_function(wrap_pyfunction!(metric_is_simple_aggregation, m)?)?;
    m.add_function(wrap_pyfunction!(dimension_sql_expr, m)?)?;
    m.add_function(wrap_pyfunction!(dimension_with_granularity, m)?)?;
    m.add_function(wrap_pyfunction!(model_get_hierarchy_path, m)?)?;
    m.add_function(wrap_pyfunction!(model_get_drill_down, m)?)?;
    m.add_function(wrap_pyfunction!(model_get_drill_up, m)?)?;
    m.add_function(wrap_pyfunction!(model_find_dimension_index, m)?)?;
    m.add_function(wrap_pyfunction!(model_find_metric_index, m)?)?;
    m.add_function(wrap_pyfunction!(model_find_segment_index, m)?)?;
    m.add_function(wrap_pyfunction!(model_find_pre_aggregation_index, m)?)?;
    m.add_function(wrap_pyfunction!(relationship_sql_expr, m)?)?;
    m.add_function(wrap_pyfunction!(relationship_related_key, m)?)?;
    m.add_function(wrap_pyfunction!(relationship_foreign_key_columns, m)?)?;
    m.add_function(wrap_pyfunction!(relationship_primary_key_columns, m)?)?;
    m.add_function(wrap_pyfunction!(segment_get_sql, m)?)?;
    m.add_function(wrap_pyfunction!(find_relationship_path_with_yaml, m)?)?;
    m.add_function(wrap_pyfunction!(parse_reference_with_yaml, m)?)?;
    m.add_function(wrap_pyfunction!(find_models_for_query, m)?)?;
    m.add_function(wrap_pyfunction!(find_models_for_query_with_yaml, m)?)?;
    m.add_function(wrap_pyfunction!(generate_catalog_metadata, m)?)?;
    Ok(())
}
