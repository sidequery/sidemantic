//! Sidemantic: A SQL-first semantic layer in Rust
//!
//! This library provides a way to define semantic models (dimensions and metrics)
//! on top of database tables, and automatically generates SQL queries that respect
//! those definitions.
//!
//! # Example
//!
//! ```
//! use sidemantic::{SemanticGraph, Model, Dimension, Metric, Relationship};
//! use sidemantic::sql::{SqlGenerator, SemanticQuery};
//!
//! // Create a semantic graph
//! let mut graph = SemanticGraph::new();
//!
//! // Define an orders model
//! let orders = Model::new("orders", "order_id")
//!     .with_table("orders")
//!     .with_dimension(Dimension::categorical("status"))
//!     .with_dimension(Dimension::time("order_date"))
//!     .with_metric(Metric::sum("revenue", "amount"))
//!     .with_metric(Metric::count("order_count"))
//!     .with_relationship(Relationship::many_to_one("customers"));
//!
//! graph.add_model(orders).unwrap();
//!
//! // Generate SQL from a semantic query
//! let generator = SqlGenerator::new(&graph);
//! let query = SemanticQuery::new()
//!     .with_metrics(vec!["orders.revenue".into()])
//!     .with_dimensions(vec!["orders.status".into()]);
//!
//! let sql = generator.generate(&query).unwrap();
//! println!("{}", sql);
//! ```

pub mod config;
pub mod core;
pub mod db;
pub mod error;
pub mod ffi;
#[cfg(feature = "python")]
mod python;
pub mod runtime;
pub mod sql;
#[cfg(feature = "wasm")]
pub mod wasm;

// Re-export commonly used types
pub use config::{
    load_from_directory, load_from_directory_with_metadata, load_from_file, load_from_string,
};
pub use core::{
    build_symmetric_aggregate_sql, merge_model, resolve_model_inheritance, Aggregation, Dimension,
    DimensionType, JoinPath, JoinStep, Metric, MetricType, Model, Parameter, ParameterType,
    Relationship, RelationshipType, RelativeDate, Segment, SemanticGraph, SqlDialect,
    SymmetricAggType, TableCalcType, TableCalculation,
};
pub use error::{Result, SidemanticError};
pub use runtime::{
    analyze_migrator_query, build_preaggregation_refresh_statements,
    calculate_preaggregation_benefit_score, chart_auto_detect_columns, chart_encoding_type,
    chart_format_label, chart_select_type, compile_with_yaml_query, detect_adapter_kind,
    dimension_sql_expr_with_yaml, dimension_with_granularity_with_yaml,
    evaluate_table_calculation_expression, extract_column_references,
    extract_metric_dependencies_from_yaml, extract_preaggregation_patterns, find_models_for_query,
    find_relationship_path_with_yaml, format_parameter_value_with_yaml,
    generate_catalog_metadata_with_yaml, generate_preaggregation_definition,
    generate_preaggregation_materialization_sql_with_yaml, generate_preaggregation_name,
    generate_time_comparison_sql, interpolate_sql_with_parameters_with_yaml, is_relative_date,
    is_sql_template, load_graph_from_directory, load_graph_with_yaml, metric_is_simple_aggregation,
    metric_sql_expr, metric_to_sql, model_find_dimension_index_with_yaml,
    model_find_metric_index_with_yaml, model_find_pre_aggregation_index_with_yaml,
    model_find_segment_index_with_yaml, model_get_drill_down_with_yaml,
    model_get_drill_up_with_yaml, model_get_hierarchy_path_with_yaml, parse_reference_with_yaml,
    parse_relative_date, parse_simple_metric_aggregation, parse_sql_definitions_payload,
    parse_sql_graph_definitions_payload, parse_sql_model_payload,
    plan_preaggregation_refresh_execution, recommend_preaggregation_patterns,
    relationship_foreign_key_columns_with_yaml, relationship_primary_key_columns_with_yaml,
    relationship_related_key_with_yaml, relationship_sql_expr_with_yaml, relative_date_to_range,
    render_sql_template, resolve_metric_inheritance, resolve_model_inheritance_with_yaml,
    resolve_preaggregation_refresh_mode, rewrite_with_yaml, segment_get_sql_with_yaml,
    shape_preaggregation_refresh_result, summarize_preaggregation_patterns,
    time_comparison_offset_interval, time_comparison_sql_offset, trailing_period_sql_interval,
    validate_engine_refresh_sql_compatibility, validate_metric_payload, validate_model_payload,
    validate_models_yaml, validate_parameter_payload, validate_preaggregation_refresh_request,
    validate_query_references, validate_query_with_yaml, validate_table_calculation_payload,
    validate_table_formula_expression, LoadedGraphPayload, PreaggregationRefreshExecutionPlan,
    PreaggregationRefreshResultShape, QueryValidationContext, RelationshipPathError,
    RelationshipPathStep, SidemanticRuntime,
};
pub use sql::{QueryRewriter, SemanticQuery, SqlGenerator};
#[cfg(feature = "wasm")]
pub use wasm::{
    wasm_analyze_migrator_query, wasm_build_preaggregation_refresh_statements,
    wasm_build_symmetric_aggregate_sql, wasm_calculate_preaggregation_benefit_score,
    wasm_chart_auto_detect_columns, wasm_chart_encoding_type, wasm_chart_format_label,
    wasm_chart_select_type, wasm_compile_with_yaml_query, wasm_detect_adapter_kind,
    wasm_dimension_sql_expr_with_yaml, wasm_dimension_with_granularity_with_yaml,
    wasm_evaluate_table_calculation_expression, wasm_extract_column_references,
    wasm_extract_metric_dependencies_from_yaml, wasm_extract_preaggregation_patterns,
    wasm_find_models_for_query, wasm_find_relationship_path_with_yaml,
    wasm_format_parameter_value_with_yaml, wasm_generate_catalog_metadata_with_yaml,
    wasm_generate_preaggregation_definition,
    wasm_generate_preaggregation_materialization_sql_with_yaml, wasm_generate_preaggregation_name,
    wasm_generate_time_comparison_sql, wasm_interpolate_sql_with_parameters_with_yaml,
    wasm_is_relative_date, wasm_is_sql_template, wasm_load_graph_with_sql,
    wasm_load_graph_with_yaml, wasm_metric_is_simple_aggregation, wasm_metric_sql_expr,
    wasm_metric_to_sql, wasm_model_find_dimension_index_with_yaml,
    wasm_model_find_metric_index_with_yaml, wasm_model_find_pre_aggregation_index_with_yaml,
    wasm_model_find_segment_index_with_yaml, wasm_model_get_drill_down_with_yaml,
    wasm_model_get_drill_up_with_yaml, wasm_model_get_hierarchy_path_with_yaml,
    wasm_needs_symmetric_aggregate, wasm_parse_reference_with_yaml, wasm_parse_relative_date,
    wasm_parse_simple_metric_aggregation, wasm_parse_sql_definitions_payload,
    wasm_parse_sql_graph_definitions_payload, wasm_parse_sql_model_payload,
    wasm_parse_sql_statement_blocks_payload, wasm_recommend_preaggregation_patterns,
    wasm_relationship_foreign_key_columns_with_yaml,
    wasm_relationship_primary_key_columns_with_yaml, wasm_relationship_related_key_with_yaml,
    wasm_relationship_sql_expr_with_yaml, wasm_relative_date_to_range, wasm_render_sql_template,
    wasm_resolve_metric_inheritance, wasm_resolve_model_inheritance_with_yaml,
    wasm_rewrite_with_yaml, wasm_segment_get_sql_with_yaml, wasm_summarize_preaggregation_patterns,
    wasm_time_comparison_offset_interval, wasm_time_comparison_sql_offset,
    wasm_trailing_period_sql_interval, wasm_validate_engine_refresh_sql_compatibility,
    wasm_validate_metric_payload, wasm_validate_model_payload, wasm_validate_models_yaml,
    wasm_validate_parameter_payload, wasm_validate_query_references,
    wasm_validate_query_references_with_yaml, wasm_validate_query_with_yaml,
    wasm_validate_table_calculation_payload, wasm_validate_table_formula_expression,
};

#[cfg(feature = "adbc-exec")]
pub use db::{execute_with_adbc, AdbcExecutionRequest, AdbcExecutionResult, AdbcValue};
