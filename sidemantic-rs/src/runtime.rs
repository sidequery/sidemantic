//! Runtime orchestration helpers for pure Rust consumers.
//!
//! This module exposes a high-level API for loading models, compiling queries,
//! rewriting SQL, and validating query references without requiring the Python
//! bridge.

use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::Path;

use chrono::Utc;
use minijinja::Environment;
use polyglot_sql::{parse_one as polyglot_parse_one, DialectType};
use regex::Regex;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::config::{
    load_from_directory_with_metadata, load_from_file_with_metadata,
    load_from_sql_string_with_metadata, load_from_string_with_metadata,
    parse_sql_definitions as parse_sql_definitions_config,
    parse_sql_graph_definitions_extended as parse_sql_graph_definitions_extended_config,
    parse_sql_model as parse_sql_model_config,
    parse_sql_statement_blocks as parse_sql_statement_blocks_config, LoadedModelSource,
};
use crate::core::symmetric_agg::needs_symmetric_aggregate as needs_symmetric_aggregate_core;
use crate::core::{
    build_symmetric_aggregate_sql as build_symmetric_aggregate_sql_core,
    extract_column_references_from_expr, extract_dependencies_with_context,
    resolve_model_inheritance as resolve_models_inheritance, Aggregation, Dimension, DimensionType,
    JoinPath, Metric, MetricType, Model, Parameter, ParameterType, Relationship, RelationshipType,
    SemanticGraph, SqlDialect, SymmetricAggType,
};
#[cfg(any(target_arch = "wasm32", test))]
use crate::core::{TableCalcType, TableCalculation};
use crate::error::{Result, SidemanticError};
use crate::sql::{QueryRewriter, SemanticQuery, SqlGenerator};

/// Query-validation context for unqualified metric semantics.
#[derive(Debug, Clone, Default)]
pub struct QueryValidationContext {
    /// Top-level metric names defined outside `models`.
    pub top_level_metric_names: HashSet<String>,
    /// Optional `sql` references for top-level metrics.
    pub top_level_metric_sql_refs: HashMap<String, String>,
}

impl QueryValidationContext {
    pub fn new(
        top_level_metric_names: HashSet<String>,
        top_level_metric_sql_refs: HashMap<String, String>,
    ) -> Self {
        Self {
            top_level_metric_names,
            top_level_metric_sql_refs,
        }
    }

    pub fn from_top_level_metrics(metrics: &[Metric]) -> Self {
        let mut names = HashSet::new();
        let mut sql_refs = HashMap::new();
        for metric in metrics {
            names.insert(metric.name.clone());
            if let Some(sql) = metric.sql.as_ref() {
                if !sql.is_empty() {
                    sql_refs.insert(metric.name.clone(), sql.clone());
                }
            }
        }
        Self::new(names, sql_refs)
    }
}

/// High-level runtime wrapper for pure Rust orchestration.
#[derive(Debug)]
pub struct SidemanticRuntime {
    graph: SemanticGraph,
    query_validation: QueryValidationContext,
    top_level_metrics: Vec<Metric>,
    model_order: Vec<String>,
    original_model_metrics: HashMap<String, Vec<String>>,
    model_sources: HashMap<String, LoadedModelSource>,
}

/// Serialized graph payload compatible with Python bridge metadata consumers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadedGraphPayload {
    pub models: Vec<Model>,
    pub parameters: Vec<Parameter>,
    pub top_level_metrics: Vec<Metric>,
    pub model_order: Vec<String>,
    pub original_model_metrics: HashMap<String, Vec<String>>,
    pub model_sources: HashMap<String, LoadedModelSource>,
}

/// Tuple shape returned to Python bridge for graph path steps.
pub type RelationshipPathStep = (String, String, Vec<String>, Vec<String>, String);

/// Relationship path discovery errors that preserve Python-compatible exception semantics.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RelationshipPathError {
    #[error("Model {0} not found")]
    ModelNotFound(String),
    #[error("No join path found between {from_model} and {to_model}")]
    NoJoinPath {
        from_model: String,
        to_model: String,
    },
    #[error("{0}")]
    InvalidPayload(String),
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum GraphPathKeyPayload {
    Single(String),
    Multiple(Vec<String>),
}

#[derive(Debug, Deserialize)]
struct GraphPathPayload {
    #[serde(default)]
    models: Vec<GraphPathModelPayload>,
}

#[derive(Debug, Deserialize)]
struct GraphPathModelPayload {
    name: String,
    #[serde(default)]
    primary_key_columns: Vec<String>,
    #[serde(default)]
    primary_key: Option<GraphPathKeyPayload>,
    #[serde(default)]
    relationships: Vec<GraphPathRelationshipPayload>,
}

#[derive(Debug, Deserialize)]
struct GraphPathRelationshipPayload {
    name: String,
    #[serde(default, rename = "type")]
    relationship_type: Option<String>,
    #[serde(default)]
    foreign_key: Option<GraphPathKeyPayload>,
    #[serde(default)]
    primary_key: Option<GraphPathKeyPayload>,
    #[serde(default)]
    foreign_key_columns: Vec<String>,
    #[serde(default)]
    primary_key_columns: Vec<String>,
    #[serde(default)]
    has_foreign_key: bool,
    #[serde(default)]
    has_primary_key: bool,
    through: Option<String>,
    through_foreign_key: Option<String>,
    related_foreign_key: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MetricDependencyPayload {
    name: String,
    #[serde(default, rename = "type")]
    metric_type: Option<String>,
    #[serde(default)]
    agg: Option<String>,
    #[serde(default)]
    sql: Option<String>,
    #[serde(default)]
    numerator: Option<String>,
    #[serde(default)]
    denominator: Option<String>,
    #[serde(default)]
    base_metric: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RuntimeQueryPayload {
    #[serde(default)]
    metrics: Vec<String>,
    #[serde(default)]
    dimensions: Vec<String>,
    #[serde(default)]
    filters: Vec<String>,
    #[serde(default)]
    segments: Vec<String>,
    #[serde(default)]
    order_by: Vec<String>,
    limit: Option<usize>,
    offset: Option<usize>,
    #[serde(default)]
    ungrouped: bool,
    #[serde(default)]
    use_preaggregations: bool,
    #[serde(default)]
    skip_default_time_dimensions: bool,
    preagg_database: Option<String>,
    preagg_schema: Option<String>,
    #[serde(default)]
    parameter_values: HashMap<String, serde_yaml::Value>,
}

#[derive(Debug, Deserialize)]
struct RuntimeQueryValidationPayload {
    #[serde(default)]
    metrics: Vec<String>,
    #[serde(default)]
    dimensions: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ParsedSqlDefinitionsPayload {
    metrics: Vec<Metric>,
    segments: Vec<crate::core::Segment>,
}

#[derive(Debug, Serialize)]
struct ParsedSqlGraphDefinitionsPayload {
    metrics: Vec<Metric>,
    segments: Vec<crate::core::Segment>,
    parameters: Vec<Parameter>,
    pre_aggregations: Vec<crate::core::PreAggregation>,
}

#[derive(Debug, Deserialize)]
struct DimensionHelperPayload {
    name: String,
    #[serde(default, rename = "type")]
    dimension_type: Option<String>,
    #[serde(default)]
    sql: Option<String>,
    #[serde(default)]
    supported_granularities: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct ModelHierarchyPayload {
    #[serde(default)]
    dimensions: Vec<ModelHierarchyDimensionPayload>,
}

#[derive(Debug, Deserialize)]
struct ModelHierarchyDimensionPayload {
    name: String,
    #[serde(default)]
    parent: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ModelLookupPayload {
    #[serde(default)]
    dimensions: Vec<ModelLookupItemPayload>,
    #[serde(default)]
    metrics: Vec<ModelLookupItemPayload>,
    #[serde(default)]
    segments: Vec<ModelLookupItemPayload>,
    #[serde(default)]
    pre_aggregations: Vec<ModelLookupItemPayload>,
}

#[derive(Debug, Deserialize)]
struct ModelLookupItemPayload {
    name: String,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RelationshipKeyPayload {
    Single(String),
    Multiple(Vec<String>),
}

#[derive(Debug, Deserialize)]
struct RelationshipHelperPayload {
    name: String,
    #[serde(default, rename = "type")]
    relationship_type: Option<String>,
    #[serde(default)]
    foreign_key: Option<RelationshipKeyPayload>,
    #[serde(default)]
    primary_key: Option<RelationshipKeyPayload>,
}

#[derive(Debug, Deserialize)]
struct SegmentHelperPayload {
    sql: String,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
struct PreaggPatternRecord {
    model: String,
    metrics: Vec<String>,
    dimensions: Vec<String>,
    granularities: Vec<String>,
    count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PreaggRecommendationRecord {
    pattern: PreaggPatternRecord,
    suggested_name: String,
    query_count: usize,
    estimated_benefit_score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct MigratorAnalysisPayload {
    #[serde(default)]
    column_references: Vec<String>,
    #[serde(default)]
    group_by_columns: Vec<(String, String)>,
    #[serde(default)]
    derived_metrics: Vec<MigratorDerivedMetricRecord>,
    #[serde(default)]
    cumulative_metrics: Vec<MigratorCumulativeMetricRecord>,
    #[serde(default)]
    aggregations_in_derived: Vec<(String, String, String)>,
    #[serde(default)]
    aggregations_in_cumulative: Vec<(String, String, String)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MigratorDerivedMetricRecord {
    name: String,
    sql_expression: String,
    table: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MigratorCumulativeMetricRecord {
    name: String,
    base_metric: String,
    table: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    window: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    grain_to_date: Option<String>,
    agg_type: String,
    agg_column: String,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct PatternKey {
    model: String,
    metrics: Vec<String>,
    dimensions: Vec<String>,
    granularities: Vec<String>,
}

fn parse_metric_helper_payload(metric_yaml: &str) -> Result<Metric> {
    serde_yaml::from_str(metric_yaml)
        .map_err(|e| SidemanticError::Validation(format!("failed to parse metric payload: {e}")))
}

fn normalize_set(values: Vec<String>) -> Vec<String> {
    let set: BTreeSet<String> = values
        .into_iter()
        .filter(|value| !value.is_empty())
        .collect::<BTreeSet<_>>();
    set.into_iter().collect()
}

fn extract_pattern_key(query: &str, instrumentation_re: &Regex) -> Option<PatternKey> {
    let captures = instrumentation_re.captures(query)?;
    let metadata = captures.get(1)?.as_str();

    let mut parts: HashMap<String, String> = HashMap::new();
    for part in metadata.split_whitespace() {
        if let Some((key, value)) = part.split_once('=') {
            parts.insert(key.to_string(), value.to_string());
        }
    }

    let models = normalize_set(
        parts
            .get("models")
            .map(|value| {
                value
                    .split(',')
                    .map(str::trim)
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
    );
    let metrics = normalize_set(
        parts
            .get("metrics")
            .map(|value| {
                value
                    .split(',')
                    .map(str::trim)
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
    );
    let dimensions = normalize_set(
        parts
            .get("dimensions")
            .map(|value| {
                value
                    .split(',')
                    .map(str::trim)
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
    );
    let granularities = normalize_set(
        parts
            .get("granularities")
            .map(|value| {
                value
                    .split(',')
                    .map(str::trim)
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
    );

    if models.len() != 1 || metrics.is_empty() {
        return None;
    }

    Some(PatternKey {
        model: models[0].clone(),
        metrics,
        dimensions,
        granularities,
    })
}

fn benefit_score(pattern: &PreaggPatternRecord, count: usize) -> f64 {
    let query_score = (count as f64 + 1.0).log10() / 6.0;
    let dim_count = pattern.dimensions.len() as f64;
    let dim_score = (1.0 - (dim_count * 0.1)).max(0.0);
    let metric_count = pattern.metrics.len() as f64;
    let metric_score = (0.25 + (metric_count * 0.25)).min(1.0);
    ((query_score * 0.5) + (dim_score * 0.25) + (metric_score * 0.25)).min(1.0)
}

fn granularity_rank(granularity: &str) -> usize {
    match granularity {
        "hour" => 0,
        "day" => 1,
        "week" => 2,
        "month" => 3,
        "quarter" => 4,
        "year" => 5,
        _ => 99,
    }
}

fn pattern_name(pattern: &PreaggPatternRecord) -> String {
    let mut parts: Vec<String> = Vec::new();

    if !pattern.granularities.is_empty() {
        let mut granularities = pattern.granularities.clone();
        granularities.sort_by_key(|value| granularity_rank(value));
        if let Some(first) = granularities.first() {
            parts.push(first.clone());
        }
    }

    if !pattern.dimensions.is_empty() {
        let mut dimensions = pattern.dimensions.clone();
        dimensions.sort();
        if dimensions.len() <= 2 {
            parts.extend(
                dimensions
                    .into_iter()
                    .map(|value| value.rsplit('.').next().unwrap_or(&value).to_string()),
            );
        } else {
            parts.push(format!("{}dims", dimensions.len()));
        }
    }

    if pattern.metrics.len() == 1 {
        if let Some(metric) = pattern.metrics.first() {
            parts.push(metric.rsplit('.').next().unwrap_or(metric).to_string());
        }
    } else if !pattern.metrics.is_empty() {
        parts.push(format!("{}metrics", pattern.metrics.len()));
    }

    if parts.is_empty() {
        "rollup".to_string()
    } else {
        parts.join("_")
    }
}

fn parse_pattern_payload(pattern_json: &str, context: &str) -> Result<PreaggPatternRecord> {
    serde_json::from_str(pattern_json).map_err(|e| {
        SidemanticError::Validation(format!(
            "failed to parse pattern payload for {context}: {e}"
        ))
    })
}

fn parse_patterns_payload(patterns_json: &str, context: &str) -> Result<Vec<PreaggPatternRecord>> {
    serde_json::from_str(patterns_json).map_err(|e| {
        SidemanticError::Validation(format!(
            "failed to parse pattern payload for {context}: {e}"
        ))
    })
}

fn serialize_json_payload(value: &serde_json::Value, context: &str) -> Result<String> {
    serde_json::to_string(value).map_err(|e| {
        SidemanticError::SqlGeneration(format!("failed to serialize {context} payload: {e}"))
    })
}

fn strip_model_prefix(value: &str) -> String {
    value.rsplit('.').next().unwrap_or(value).to_string()
}

fn graph_path_key_to_columns(key: &GraphPathKeyPayload) -> Vec<String> {
    match key {
        GraphPathKeyPayload::Single(value) => vec![value.clone()],
        GraphPathKeyPayload::Multiple(values) => values.clone(),
    }
}

fn graph_model_primary_keys(model: &GraphPathModelPayload) -> Vec<String> {
    if !model.primary_key_columns.is_empty() {
        model.primary_key_columns.clone()
    } else if let Some(primary_key) = model.primary_key.as_ref() {
        graph_path_key_to_columns(primary_key)
    } else {
        vec!["id".to_string()]
    }
}

fn relationship_type_name(relationship: &GraphPathRelationshipPayload) -> &str {
    relationship
        .relationship_type
        .as_deref()
        .unwrap_or("many_to_one")
}

fn parse_relationship_type_label(relationship_type: &str) -> RelationshipType {
    match relationship_type {
        "one_to_one" => RelationshipType::OneToOne,
        "one_to_many" => RelationshipType::OneToMany,
        "many_to_many" => RelationshipType::ManyToMany,
        _ => RelationshipType::ManyToOne,
    }
}

fn relationship_foreign_keys(relationship: &GraphPathRelationshipPayload) -> Vec<String> {
    if !relationship.foreign_key_columns.is_empty() {
        return relationship.foreign_key_columns.clone();
    }
    if let Some(foreign_key) = relationship.foreign_key.as_ref() {
        return graph_path_key_to_columns(foreign_key);
    }

    if relationship_type_name(relationship) == "many_to_one" {
        return vec![format!("{}_id", relationship.name)];
    }

    vec!["id".to_string()]
}

fn relationship_primary_keys(relationship: &GraphPathRelationshipPayload) -> Vec<String> {
    if !relationship.primary_key_columns.is_empty() {
        return relationship.primary_key_columns.clone();
    }
    if let Some(primary_key) = relationship.primary_key.as_ref() {
        return graph_path_key_to_columns(primary_key);
    }
    vec!["id".to_string()]
}

fn relationship_has_foreign_key(relationship: &GraphPathRelationshipPayload) -> bool {
    relationship.has_foreign_key
        || !relationship.foreign_key_columns.is_empty()
        || relationship.foreign_key.is_some()
}

fn relationship_has_primary_key(relationship: &GraphPathRelationshipPayload) -> bool {
    relationship.has_primary_key
        || !relationship.primary_key_columns.is_empty()
        || relationship.primary_key.is_some()
}

fn relationship_first_foreign_key(relationship: &GraphPathRelationshipPayload) -> Option<String> {
    relationship_foreign_keys(relationship).first().cloned()
}

fn parse_metric_type_for_dependencies(payload: &MetricDependencyPayload) -> MetricType {
    match payload.metric_type.as_deref() {
        Some("derived") => MetricType::Derived,
        Some("ratio") => MetricType::Ratio,
        Some("cumulative") => MetricType::Cumulative,
        Some("time_comparison" | "timecomparison") => MetricType::TimeComparison,
        Some("conversion") => MetricType::Conversion,
        _ => {
            if payload.agg.is_none() && payload.sql.is_some() {
                MetricType::Derived
            } else {
                MetricType::Simple
            }
        }
    }
}

fn parse_metric_agg_for_dependencies(agg: Option<&str>) -> Option<Aggregation> {
    match agg {
        Some("sum") => Some(Aggregation::Sum),
        Some("count") => Some(Aggregation::Count),
        Some("count_distinct") => Some(Aggregation::CountDistinct),
        Some("avg") => Some(Aggregation::Avg),
        Some("min") => Some(Aggregation::Min),
        Some("max") => Some(Aggregation::Max),
        Some("median") => Some(Aggregation::Median),
        Some("expression") => Some(Aggregation::Expression),
        _ => None,
    }
}

fn format_python_string_list(values: &[String]) -> String {
    let rendered = values
        .iter()
        .map(|value| format!("'{}'", value.replace('\'', "\\'")))
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{rendered}]")
}

fn has_inline_aggregation(sql: &str) -> bool {
    Regex::new(r"(?i)\b(sum|avg|count|min|max|median)\s*\(")
        .ok()
        .is_some_and(|re| re.is_match(sql))
}

fn yaml_value_to_python_str(value: &serde_yaml::Value) -> String {
    match value {
        serde_yaml::Value::Null => "None".to_string(),
        serde_yaml::Value::Bool(v) => {
            if *v {
                "True".to_string()
            } else {
                "False".to_string()
            }
        }
        serde_yaml::Value::Number(v) => v.to_string(),
        serde_yaml::Value::String(v) => v.clone(),
        serde_yaml::Value::Sequence(v) => {
            serde_json::to_string(v).unwrap_or_else(|_| "[]".to_string())
        }
        serde_yaml::Value::Mapping(v) => {
            serde_json::to_string(v).unwrap_or_else(|_| "{}".to_string())
        }
        serde_yaml::Value::Tagged(v) => yaml_value_to_python_str(&v.value),
    }
}

fn yaml_value_type_name(value: &serde_yaml::Value) -> &'static str {
    match value {
        serde_yaml::Value::Null => "NoneType",
        serde_yaml::Value::Bool(_) => "bool",
        serde_yaml::Value::Number(_) => "number",
        serde_yaml::Value::String(_) => "str",
        serde_yaml::Value::Sequence(_) => "list",
        serde_yaml::Value::Mapping(_) => "dict",
        serde_yaml::Value::Tagged(v) => yaml_value_type_name(&v.value),
    }
}

fn value_is_truthy(value: &serde_yaml::Value) -> bool {
    match value {
        serde_yaml::Value::Null => false,
        serde_yaml::Value::Bool(v) => *v,
        serde_yaml::Value::Number(v) => {
            if let Some(i) = v.as_i64() {
                return i != 0;
            }
            if let Some(u) = v.as_u64() {
                return u != 0;
            }
            if let Some(f) = v.as_f64() {
                return f != 0.0;
            }
            true
        }
        serde_yaml::Value::String(v) => !v.is_empty(),
        serde_yaml::Value::Sequence(v) => !v.is_empty(),
        serde_yaml::Value::Mapping(v) => !v.is_empty(),
        serde_yaml::Value::Tagged(v) => value_is_truthy(&v.value),
    }
}

fn parameter_runtime_value(
    parameter: &Parameter,
    parameter_values: &HashMap<String, serde_yaml::Value>,
) -> serde_yaml::Value {
    if let Some(value) = parameter_values.get(&parameter.name) {
        return value.clone();
    }

    if parameter.default_to_today && parameter.parameter_type == ParameterType::Date {
        return serde_yaml::Value::String(Utc::now().date_naive().to_string());
    }

    if let Some(default_value) = &parameter.default_value {
        return serde_yaml::to_value(default_value).unwrap_or(serde_yaml::Value::Null);
    }

    serde_yaml::Value::Null
}

fn format_float_like_python(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{value:.1}")
    } else {
        value.to_string()
    }
}

fn format_parameter_value(
    parameter: &Parameter,
    value: &serde_yaml::Value,
) -> std::result::Result<String, String> {
    match parameter.parameter_type {
        ParameterType::String => {
            let escaped = yaml_value_to_python_str(value).replace('\'', "''");
            Ok(format!("'{escaped}'"))
        }
        ParameterType::Date => Ok(format!("'{}'", yaml_value_to_python_str(value))),
        ParameterType::Number => match value {
            serde_yaml::Value::Number(v) => Ok(v.to_string()),
            serde_yaml::Value::String(v) => {
                let parsed = v
                    .parse::<f64>()
                    .map_err(|_| format!("Invalid numeric parameter value: {v}"))?;
                if !parsed.is_finite() {
                    return Err(format!("Invalid numeric parameter value: {v}"));
                }
                Ok(format_float_like_python(parsed))
            }
            serde_yaml::Value::Tagged(v) => format_parameter_value(parameter, &v.value),
            other => Err(format!(
                "Numeric parameter must be int, float, or numeric string, got {}",
                yaml_value_type_name(other)
            )),
        },
        ParameterType::Unquoted => {
            let rendered = yaml_value_to_python_str(value);
            let is_safe = rendered
                .chars()
                .filter(|c| *c != '_' && *c != '.')
                .all(char::is_alphanumeric);
            if !is_safe {
                return Err(format!(
                    "Unquoted parameter must be alphanumeric with underscores/dots only: {}",
                    yaml_value_to_python_str(value)
                ));
            }
            Ok(rendered)
        }
        ParameterType::Yesno => {
            if value_is_truthy(value) {
                Ok("TRUE".to_string())
            } else {
                Ok("FALSE".to_string())
            }
        }
    }
}

pub fn is_sql_template(sql: &str) -> bool {
    sql.contains("{{") || sql.contains("{%") || sql.contains("{#")
}

fn has_jinja_control_markers(sql: &str) -> bool {
    sql.contains("{%") || sql.contains("{#")
}

fn yaml_to_json_value(value: &serde_yaml::Value) -> serde_json::Value {
    match value {
        serde_yaml::Value::Null => serde_json::Value::Null,
        serde_yaml::Value::Bool(v) => serde_json::Value::Bool(*v),
        serde_yaml::Value::Number(v) => {
            if let Some(i) = v.as_i64() {
                return serde_json::json!(i);
            }
            if let Some(u) = v.as_u64() {
                return serde_json::json!(u);
            }
            if let Some(f) = v.as_f64() {
                return serde_json::json!(f);
            }
            serde_json::Value::Null
        }
        serde_yaml::Value::String(v) => serde_json::Value::String(v.clone()),
        serde_yaml::Value::Sequence(values) => {
            serde_json::Value::Array(values.iter().map(yaml_to_json_value).collect())
        }
        serde_yaml::Value::Mapping(values) => {
            let mut object = serde_json::Map::new();
            for (key, value) in values {
                let key = match key {
                    serde_yaml::Value::String(v) => v.clone(),
                    other => yaml_value_to_python_str(other),
                };
                object.insert(key, yaml_to_json_value(value));
            }
            serde_json::Value::Object(object)
        }
        serde_yaml::Value::Tagged(v) => yaml_to_json_value(&v.value),
    }
}

fn parse_string_keyed_yaml_mapping(
    payload_yaml: &str,
    error_context: &str,
) -> std::result::Result<HashMap<String, serde_yaml::Value>, String> {
    let parsed: serde_yaml::Value = serde_yaml::from_str(payload_yaml)
        .map_err(|e| format!("failed to parse {error_context} payload: {e}"))?;

    let mut result = HashMap::new();
    let Some(mapping) = parsed.as_mapping() else {
        if parsed.is_null() {
            return Ok(result);
        }
        return Err(format!("{error_context} payload must be a YAML mapping"));
    };

    for (key, value) in mapping {
        let key = match key {
            serde_yaml::Value::String(v) => v.clone(),
            other => yaml_value_to_python_str(other),
        };
        result.insert(key, value.clone());
    }

    Ok(result)
}

fn build_runtime_context(
    parameters_by_name: &HashMap<String, &Parameter>,
    parameter_values: &HashMap<String, serde_yaml::Value>,
) -> HashMap<String, serde_yaml::Value> {
    let mut context = HashMap::new();
    for (name, parameter) in parameters_by_name {
        context.insert(
            name.clone(),
            parameter_runtime_value(parameter, parameter_values),
        );
    }
    context
}

fn render_template_with_context(
    template_str: &str,
    context: &HashMap<String, serde_yaml::Value>,
) -> std::result::Result<String, String> {
    let env = Environment::new();
    let render_context = context
        .iter()
        .map(|(key, value)| (key.clone(), yaml_to_json_value(value)))
        .collect::<HashMap<_, _>>();
    let render_once = |candidate: &str| -> std::result::Result<String, String> {
        let template = env
            .template_from_str(candidate)
            .map_err(|e| format!("Template syntax error: {e}"))?;
        template
            .render(render_context.clone())
            .map_err(|e| format!("Template rendering error: {e}"))
    };

    match render_once(template_str) {
        Ok(rendered) => Ok(rendered),
        Err(err) if err.contains("method named items") => {
            let re = Regex::new(r"([A-Za-z_][A-Za-z0-9_\.]*)\.items\(\)")
                .expect("valid template items() compatibility regex");
            let rewritten = re.replace_all(template_str, "$1|items").into_owned();
            if rewritten == template_str {
                return Err(err);
            }
            render_once(&rewritten)
        }
        Err(err) => Err(err),
    }
}

fn interpolate_simple_filter(
    filter: &str,
    parameters_by_name: &HashMap<String, &Parameter>,
    parameter_values: &HashMap<String, serde_yaml::Value>,
) -> std::result::Result<String, String> {
    let pattern = Regex::new(r"\{\{\s*(\w+)\s*\}\}").expect("valid parameter regex");
    let mut interpolation_error: Option<String> = None;

    let rendered = pattern
        .replace_all(filter, |captures: &regex::Captures<'_>| {
            let Some(param_name_match) = captures.get(1) else {
                return Cow::Owned(
                    captures
                        .get(0)
                        .map(|m| m.as_str())
                        .unwrap_or("")
                        .to_string(),
                );
            };
            let param_name = param_name_match.as_str();
            let Some(parameter) = parameters_by_name.get(param_name) else {
                return Cow::Owned(
                    captures
                        .get(0)
                        .map(|m| m.as_str())
                        .unwrap_or("")
                        .to_string(),
                );
            };

            let value = parameter_runtime_value(parameter, parameter_values);
            match format_parameter_value(parameter, &value) {
                Ok(formatted) => Cow::Owned(formatted),
                Err(err) => {
                    interpolation_error = Some(err);
                    Cow::Owned(
                        captures
                            .get(0)
                            .map(|m| m.as_str())
                            .unwrap_or("")
                            .to_string(),
                    )
                }
            }
        })
        .into_owned();

    if let Some(err) = interpolation_error {
        return Err(err);
    }

    Ok(rendered)
}

fn interpolate_sql_with_parameters_impl(
    sql: &str,
    parameters_by_name: &HashMap<String, &Parameter>,
    parameter_values: &HashMap<String, serde_yaml::Value>,
) -> std::result::Result<String, String> {
    if is_sql_template(sql) && has_jinja_control_markers(sql) {
        let context = build_runtime_context(parameters_by_name, parameter_values);
        return render_template_with_context(sql, &context);
    }
    interpolate_simple_filter(sql, parameters_by_name, parameter_values)
}

pub fn interpolate_query_filters(
    graph: &SemanticGraph,
    filters: Vec<String>,
    parameter_values: &HashMap<String, serde_yaml::Value>,
) -> std::result::Result<Vec<String>, String> {
    let parameters_by_name: HashMap<String, &Parameter> = graph
        .parameters()
        .map(|parameter| (parameter.name.clone(), parameter))
        .collect();

    filters
        .into_iter()
        .map(|filter| {
            interpolate_sql_with_parameters_impl(&filter, &parameters_by_name, parameter_values)
        })
        .collect()
}

/// Render SQL template using YAML context payload.
pub fn render_sql_template(template_str: &str, context_yaml: &str) -> Result<String> {
    let context = parse_string_keyed_yaml_mapping(context_yaml, "template context")
        .map_err(SidemanticError::Validation)?;
    render_template_with_context(template_str, &context).map_err(SidemanticError::Validation)
}

/// Format a parameter value from YAML payloads.
pub fn format_parameter_value_with_yaml(parameter_yaml: &str, value_yaml: &str) -> Result<String> {
    let parameter: Parameter = serde_yaml::from_str(parameter_yaml).map_err(|e| {
        SidemanticError::Validation(format!("failed to parse parameter payload: {e}"))
    })?;
    let value: serde_yaml::Value = serde_yaml::from_str(value_yaml).map_err(|e| {
        SidemanticError::Validation(format!("failed to parse parameter value: {e}"))
    })?;
    format_parameter_value(&parameter, &value).map_err(SidemanticError::Validation)
}

/// Interpolate SQL with parameter definitions and values from YAML payloads.
pub fn interpolate_sql_with_parameters_with_yaml(
    sql: &str,
    parameters_yaml: &str,
    values_yaml: &str,
) -> Result<String> {
    let parameters: Vec<Parameter> = serde_yaml::from_str(parameters_yaml).map_err(|e| {
        SidemanticError::Validation(format!("failed to parse parameter definitions: {e}"))
    })?;
    let values = parse_string_keyed_yaml_mapping(values_yaml, "parameter values")
        .map_err(SidemanticError::Validation)?;

    let parameters_by_name: HashMap<String, &Parameter> = parameters
        .iter()
        .map(|parameter| (parameter.name.clone(), parameter))
        .collect();

    interpolate_sql_with_parameters_impl(sql, &parameters_by_name, &values)
        .map_err(SidemanticError::Validation)
}

/// Compile a semantic query by loading graph YAML and parsing query YAML payload.
pub fn compile_with_yaml_query(yaml: &str, query_yaml: &str) -> Result<String> {
    let runtime = SidemanticRuntime::from_yaml(yaml)
        .map_err(|e| SidemanticError::Validation(format!("failed to load YAML models: {e}")))?;
    let payload: RuntimeQueryPayload = serde_yaml::from_str(query_yaml)
        .map_err(|e| SidemanticError::Validation(format!("failed to parse query payload: {e}")))?;
    let filters =
        interpolate_query_filters(runtime.graph(), payload.filters, &payload.parameter_values)
            .map_err(|e| {
                SidemanticError::Validation(format!("failed to interpolate query parameters: {e}"))
            })?;

    let mut query = SemanticQuery::new()
        .with_metrics(payload.metrics)
        .with_dimensions(payload.dimensions)
        .with_filters(filters)
        .with_segments(payload.segments)
        .with_order_by(payload.order_by)
        .with_ungrouped(payload.ungrouped)
        .with_use_preaggregations(payload.use_preaggregations)
        .with_skip_default_time_dimensions(payload.skip_default_time_dimensions)
        .with_preaggregation_qualifiers(payload.preagg_database, payload.preagg_schema);

    if let Some(limit) = payload.limit {
        query = query.with_limit(limit);
    }
    if let Some(offset) = payload.offset {
        query = query.with_offset(offset);
    }

    runtime
        .compile(&query)
        .map_err(|e| SidemanticError::SqlGeneration(format!("failed to compile SQL: {e}")))
}

/// Validate query references using graph and query YAML payloads.
pub fn validate_query_references_with_yaml(
    yaml: &str,
    metrics: &[String],
    dimensions: &[String],
) -> Result<Vec<String>> {
    let runtime = SidemanticRuntime::from_yaml(yaml)
        .map_err(|e| SidemanticError::Validation(format!("failed to load YAML models: {e}")))?;
    Ok(runtime.validate_query_references(metrics, dimensions))
}

/// Validate query references using graph and query YAML payloads.
pub fn validate_query_with_yaml(yaml: &str, query_yaml: &str) -> Result<Vec<String>> {
    let payload: RuntimeQueryValidationPayload = serde_yaml::from_str(query_yaml)
        .map_err(|e| SidemanticError::Validation(format!("failed to parse query payload: {e}")))?;
    validate_query_references_with_yaml(yaml, &payload.metrics, &payload.dimensions)
}

/// Load graph YAML and serialize runtime payload.
pub fn load_graph_with_yaml(yaml: &str) -> Result<String> {
    let runtime = SidemanticRuntime::from_yaml(yaml)
        .map_err(|e| SidemanticError::Validation(format!("failed to load YAML models: {e}")))?;
    let payload = runtime.loaded_graph_payload();
    serde_json::to_string(&payload)
        .map_err(|e| SidemanticError::Validation(format!("failed to serialize graph payload: {e}")))
}

/// Load graph definitions from a directory and serialize runtime payload.
pub fn load_graph_from_directory(path: &str) -> Result<String> {
    let runtime = SidemanticRuntime::from_directory(path).map_err(|e| {
        SidemanticError::Validation(format!("failed to load directory models: {e}"))
    })?;
    serde_json::to_string(&runtime.loaded_graph_payload())
        .map_err(|e| SidemanticError::Validation(format!("failed to serialize graph payload: {e}")))
}

fn build_runtime_with_metadata(
    graph: SemanticGraph,
    top_level_metrics: Vec<Metric>,
    model_order: Vec<String>,
    original_model_metrics: HashMap<String, Vec<String>>,
    model_sources: HashMap<String, LoadedModelSource>,
) -> SidemanticRuntime {
    let query_validation = QueryValidationContext::from_top_level_metrics(&top_level_metrics);
    SidemanticRuntime {
        graph,
        query_validation,
        top_level_metrics,
        model_order,
        original_model_metrics,
        model_sources,
    }
}

/// Load graph SQL content (.sql definitions with optional YAML frontmatter) and serialize runtime payload.
pub fn load_graph_with_sql(sql_content: &str) -> Result<String> {
    let loaded = load_from_sql_string_with_metadata(sql_content)?;
    let runtime = build_runtime_with_metadata(
        loaded.graph,
        loaded.top_level_metrics,
        loaded.model_order,
        loaded.original_model_metrics,
        loaded.model_sources,
    );
    serde_json::to_string(&runtime.loaded_graph_payload())
        .map_err(|e| SidemanticError::Validation(format!("failed to serialize graph payload: {e}")))
}

/// Parse SQL metric/segment definitions and return serialized payload.
pub fn parse_sql_definitions_payload(sql: &str) -> Result<String> {
    let (metrics, segments) = parse_sql_definitions_config(sql).map_err(|e| {
        SidemanticError::Validation(format!("failed to parse SQL definitions: {e}"))
    })?;

    let payload = ParsedSqlDefinitionsPayload { metrics, segments };
    serde_json::to_string(&payload).map_err(|e| {
        SidemanticError::SqlGeneration(format!("failed to serialize SQL definitions payload: {e}"))
    })
}

/// Parse SQL graph definitions and return serialized payload.
pub fn parse_sql_graph_definitions_payload(sql: &str) -> Result<String> {
    let (metrics, segments, parameters, pre_aggregations) =
        parse_sql_graph_definitions_extended_config(sql).map_err(|e| {
            SidemanticError::Validation(format!("failed to parse SQL graph definitions: {e}"))
        })?;

    let payload = ParsedSqlGraphDefinitionsPayload {
        metrics,
        segments,
        parameters,
        pre_aggregations,
    };
    serde_json::to_string(&payload).map_err(|e| {
        SidemanticError::SqlGeneration(format!(
            "failed to serialize SQL graph definitions payload: {e}"
        ))
    })
}

/// Parse SQL model definition and return serialized model payload.
pub fn parse_sql_model_payload(sql: &str) -> Result<String> {
    let model = parse_sql_model_config(sql)
        .map_err(|e| SidemanticError::Validation(format!("failed to parse SQL model: {e}")))?;
    serde_json::to_string(&model).map_err(|e| {
        SidemanticError::SqlGeneration(format!("failed to serialize SQL model payload: {e}"))
    })
}

/// Parse raw SQL statement blocks and return serialized payload.
pub fn parse_sql_statement_blocks_payload(sql: &str) -> Result<String> {
    let blocks = parse_sql_statement_blocks_config(sql).map_err(|e| {
        SidemanticError::Validation(format!("failed to parse SQL statement blocks: {e}"))
    })?;
    serde_json::to_string(&blocks).map_err(|e| {
        SidemanticError::SqlGeneration(format!(
            "failed to serialize SQL statement blocks payload: {e}"
        ))
    })
}

/// Validate model graph payload parses and composes into a runtime graph.
pub fn validate_models_yaml(yaml: &str) -> Result<bool> {
    SidemanticRuntime::from_yaml(yaml)
        .map(|_| true)
        .map_err(|e| SidemanticError::Validation(format!("failed to load YAML models: {e}")))
}

/// Parse reference from graph and reference payloads.
pub fn parse_reference_with_yaml(
    yaml: &str,
    reference: &str,
) -> Result<(String, String, Option<String>)> {
    let runtime = SidemanticRuntime::from_yaml(yaml)
        .map_err(|e| SidemanticError::Validation(format!("failed to load YAML models: {e}")))?;
    runtime
        .parse_reference(reference)
        .map_err(|e| SidemanticError::Validation(format!("failed to parse reference: {e}")))
}

#[cfg(any(target_arch = "wasm32", test))]
fn split_top_level_csv(input: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut depth: i32 = 0;
    let mut in_quote: Option<char> = None;
    let mut escape = false;

    for ch in input.chars() {
        if let Some(quote) = in_quote {
            current.push(ch);
            if escape {
                escape = false;
                continue;
            }
            if ch == '\\' {
                escape = true;
                continue;
            }
            if ch == quote {
                in_quote = None;
            }
            continue;
        }

        match ch {
            '\'' | '"' => {
                in_quote = Some(ch);
                current.push(ch);
            }
            '(' => {
                depth += 1;
                current.push(ch);
            }
            ')' => {
                depth = (depth - 1).max(0);
                current.push(ch);
            }
            ',' if depth == 0 => {
                let trimmed = current.trim();
                if !trimmed.is_empty() {
                    parts.push(trimmed.to_string());
                }
                current.clear();
            }
            _ => current.push(ch),
        }
    }

    let trimmed = current.trim();
    if !trimmed.is_empty() {
        parts.push(trimmed.to_string());
    }
    parts
}

#[cfg(any(target_arch = "wasm32", test))]
fn normalize_ident(ident: &str) -> String {
    ident
        .trim()
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']')
        .to_string()
}

#[cfg(any(target_arch = "wasm32", test))]
fn split_alias_suffix(expr: &str) -> (String, Option<String>) {
    let trimmed = expr.trim();
    let with_as = Regex::new(r"(?is)^(?P<body>.+?)\s+as\s+(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*$")
        .expect("valid alias regex");
    if let Some(captures) = with_as.captures(trimmed) {
        let body = captures
            .name("body")
            .map(|m| m.as_str().trim().to_string())
            .unwrap_or_else(|| trimmed.to_string());
        let alias = captures.name("alias").map(|m| normalize_ident(m.as_str()));
        return (body, alias);
    }
    let without_as = Regex::new(r"(?is)^(?P<body>.+?)\s+(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*$")
        .expect("valid bare alias regex");
    if let Some(captures) = without_as.captures(trimmed) {
        let body = captures
            .name("body")
            .map(|m| m.as_str().trim().to_string())
            .unwrap_or_else(|| trimmed.to_string());
        let alias = captures.name("alias").map(|m| normalize_ident(m.as_str()));
        return (body, alias);
    }
    (trimmed.to_string(), None)
}

#[cfg(any(target_arch = "wasm32", test))]
fn normalize_alias_key(value: &str) -> String {
    normalize_ident(value).to_ascii_lowercase()
}

#[cfg(any(target_arch = "wasm32", test))]
fn split_order_suffix(order_item: &str) -> (String, String) {
    let trimmed = order_item.trim();
    let order_re = Regex::new(
        r"(?is)^(?P<expr>.+?)\s+(?P<dir>asc|desc)(?:\s+nulls\s+(?P<nulls>first|last))?\s*$",
    )
    .expect("valid order suffix regex");
    if let Some(captures) = order_re.captures(trimmed) {
        let expr = captures
            .name("expr")
            .map(|m| m.as_str().trim().to_string())
            .unwrap_or_else(|| trimmed.to_string());
        let mut suffix = captures
            .name("dir")
            .map(|m| m.as_str().to_ascii_uppercase())
            .unwrap_or_default();
        if let Some(nulls) = captures.name("nulls") {
            if !suffix.is_empty() {
                suffix.push(' ');
            }
            suffix.push_str("NULLS ");
            suffix.push_str(&nulls.as_str().to_ascii_uppercase());
        }
        return (expr, suffix);
    }
    (trimmed.to_string(), String::new())
}

#[cfg(any(target_arch = "wasm32", test))]
fn resolve_wasm_semantic_ref(
    runtime: &SidemanticRuntime,
    aliases: &HashMap<String, String>,
    default_model: &str,
    reference: &str,
) -> Result<(String, bool)> {
    let (model_ref, field_ref) = if let Some((model_part, field_part)) = reference.split_once('.') {
        (normalize_ident(model_part), normalize_ident(field_part))
    } else {
        (default_model.to_string(), normalize_ident(reference))
    };

    let resolved_model = aliases
        .get(&model_ref)
        .cloned()
        .unwrap_or(model_ref.clone());
    let model = runtime.graph.get_model(&resolved_model).ok_or_else(|| {
        SidemanticError::Validation(format!(
            "wasm rewrite fallback model '{resolved_model}' not found"
        ))
    })?;

    if model.get_metric(&field_ref).is_some() {
        Ok((format!("{resolved_model}.{field_ref}"), true))
    } else if model.get_dimension(&field_ref).is_some() {
        Ok((format!("{resolved_model}.{field_ref}"), false))
    } else {
        Err(SidemanticError::Validation(format!(
            "wasm rewrite fallback field '{resolved_model}.{field_ref}' not found"
        )))
    }
}

#[cfg(any(target_arch = "wasm32", test))]
fn normalize_wasm_match_key(value: &str) -> String {
    value
        .trim()
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']')
        .to_ascii_lowercase()
}

#[cfg(any(target_arch = "wasm32", test))]
fn normalize_wasm_projection_expression(expr: &str, aliases: &HashMap<String, String>) -> String {
    let mut normalized = expr.to_string();
    for (alias, model) in aliases {
        if alias == model {
            continue;
        }
        let pattern = format!(r"(?i)\b{}\s*\.\s*", regex::escape(alias));
        if let Ok(re) = Regex::new(&pattern) {
            normalized = re.replace_all(&normalized, format!("{model}.")).to_string();
        }
    }
    normalized = normalized.replace('`', "");
    normalized = normalized.replace('"', "");
    normalized = normalized.replace('[', "");
    normalized = normalized.replace(']', "");
    if let Ok(re) = Regex::new(r"\s+") {
        normalized = re.replace_all(&normalized, "").to_string();
    }
    normalized.to_ascii_lowercase()
}

#[cfg(any(target_arch = "wasm32", test))]
fn strip_wasm_model_qualifier(expr: &str, model_name: &str) -> String {
    let pattern = format!(r"(?i)\b{}\s*\.\s*", regex::escape(model_name));
    if let Ok(re) = Regex::new(&pattern) {
        re.replace_all(expr, "").to_string()
    } else {
        expr.to_string()
    }
}

#[cfg(any(target_arch = "wasm32", test))]
fn resolve_wasm_expression_projection_metric(
    runtime: &SidemanticRuntime,
    aliases: &HashMap<String, String>,
    projection: &str,
) -> Result<String> {
    let projection_norm = normalize_wasm_projection_expression(projection, aliases);
    if projection_norm.is_empty() {
        return Err(SidemanticError::Validation(
            "wasm rewrite fallback received empty projection expression".into(),
        ));
    }

    let mut matches: Vec<(usize, String)> = Vec::new();

    for model in runtime.graph.models() {
        let model_name = model.name.clone();
        let projection_without_model = strip_wasm_model_qualifier(&projection_norm, &model_name);

        for metric in &model.metrics {
            if metric.sql.is_none() {
                continue;
            }

            let metric_sql_norm = normalize_wasm_projection_expression(metric.sql_expr(), aliases);
            let metric_sql_qualified_norm = normalize_wasm_projection_expression(
                &format!("{}.{}", model.name, metric.sql_expr()),
                aliases,
            );
            let metric_sql_without_model =
                strip_wasm_model_qualifier(&metric_sql_norm, &model_name);

            let rank = if projection_norm == metric_sql_qualified_norm {
                Some(0usize)
            } else if projection_norm == metric_sql_norm {
                Some(1usize)
            } else if projection_without_model == metric_sql_without_model {
                Some(2usize)
            } else {
                None
            };

            if let Some(rank) = rank {
                matches.push((rank, format!("{}.{}", model.name, metric.name)));
            }
        }
    }

    if matches.is_empty() {
        return Err(SidemanticError::Validation(format!(
            "wasm rewrite fallback projection '{projection}' requires a matching metric expression"
        )));
    }

    let best_rank = matches
        .iter()
        .map(|(rank, _)| *rank)
        .min()
        .unwrap_or(usize::MAX);
    let mut best_matches: Vec<String> = matches
        .into_iter()
        .filter_map(|(rank, reference)| (rank == best_rank).then_some(reference))
        .collect();
    best_matches.sort();
    best_matches.dedup();

    if best_matches.len() == 1 {
        return Ok(best_matches[0].clone());
    }

    Err(SidemanticError::Validation(format!(
        "wasm rewrite fallback projection '{projection}' is ambiguous; matches: {}",
        best_matches.join(", ")
    )))
}

#[cfg(any(target_arch = "wasm32", test))]
fn build_wasm_formula_projection_from_aggregates(
    runtime: &SidemanticRuntime,
    aliases: &HashMap<String, String>,
    default_model: &str,
    projection: &str,
) -> Result<(Vec<String>, String)> {
    let aggregate_pattern = Regex::new(
        r"(?is)(?:count\s*\(\s*distinct\s+[^()]+\s*\)|(?:sum|avg|min|max|median|count)\s*\(\s*(?:[^()]+|\*)\s*\))",
    )
    .expect("valid aggregate token regex");

    let mut replacements: Vec<(usize, usize, String)> = Vec::new();
    let mut metric_refs: Vec<String> = Vec::new();

    for capture in aggregate_pattern.find_iter(projection) {
        let aggregate_sql = capture.as_str();
        let semantic_ref =
            resolve_wasm_aggregate_projection(runtime, aliases, default_model, aggregate_sql)?;
        let metric_alias = semantic_ref
            .split('.')
            .next_back()
            .map(str::to_string)
            .unwrap_or_else(|| semantic_ref.clone());
        metric_refs.push(semantic_ref);
        replacements.push((capture.start(), capture.end(), metric_alias));
    }

    if replacements.is_empty() {
        return Err(SidemanticError::Validation(format!(
            "wasm rewrite fallback projection '{projection}' requires a matching metric expression"
        )));
    }

    metric_refs.sort();
    metric_refs.dedup();

    let mut rewritten = projection.to_string();
    for (start, end, replacement) in replacements.into_iter().rev() {
        rewritten.replace_range(start..end, &replacement);
    }

    Ok((metric_refs, rewritten))
}

#[cfg(any(target_arch = "wasm32", test))]
fn parse_wasm_field_reference(
    aliases: &HashMap<String, String>,
    default_model: &str,
    reference: &str,
) -> Result<(String, String)> {
    let trimmed = reference.trim();
    if trimmed.is_empty() {
        return Err(SidemanticError::Validation(
            "wasm rewrite fallback received empty field reference".into(),
        ));
    }

    let identifier_re =
        Regex::new(r"(?i)^[A-Za-z_][A-Za-z0-9_]*$").expect("valid wasm identifier regex");

    if let Some((model_part, field_part)) = trimmed.split_once('.') {
        let model_key = normalize_wasm_match_key(model_part);
        let field_key = normalize_wasm_match_key(field_part);
        if !identifier_re.is_match(&field_key) {
            return Err(SidemanticError::Validation(format!(
                "wasm rewrite fallback only supports simple field references in aggregation inputs: {trimmed}"
            )));
        }
        let resolved_model = aliases
            .get(&normalize_ident(model_part))
            .cloned()
            .unwrap_or(model_key);
        return Ok((resolved_model, field_key));
    }

    let field_key = normalize_wasm_match_key(trimmed);
    if !identifier_re.is_match(&field_key) {
        return Err(SidemanticError::Validation(format!(
            "wasm rewrite fallback only supports simple field references in aggregation inputs: {trimmed}"
        )));
    }
    Ok((default_model.to_string(), field_key))
}

#[cfg(any(target_arch = "wasm32", test))]
fn parse_wasm_count_input(
    aliases: &HashMap<String, String>,
    default_model: &str,
    inner_sql: &str,
) -> Result<(String, bool, Option<String>)> {
    let trimmed = inner_sql.trim();
    if trimmed == "*" || trimmed == "1" {
        return Ok((default_model.to_string(), true, None));
    }

    if let Some((model_part, field_part)) = trimmed.split_once('.') {
        let model_key = normalize_ident(model_part);
        let field_key = normalize_wasm_match_key(field_part);
        if field_key == "*" || field_key == "1" {
            let resolved_model = aliases
                .get(&model_key)
                .cloned()
                .unwrap_or_else(|| normalize_wasm_match_key(model_part));
            return Ok((resolved_model, true, None));
        }
    }

    let (model_name, field_name) = parse_wasm_field_reference(aliases, default_model, trimmed)?;
    Ok((model_name, false, Some(field_name)))
}

#[cfg(any(target_arch = "wasm32", test))]
fn resolve_wasm_aggregate_projection(
    runtime: &SidemanticRuntime,
    aliases: &HashMap<String, String>,
    default_model: &str,
    projection: &str,
) -> Result<String> {
    let Some((agg_name, inner_expr)) = parse_simple_metric_aggregation(projection) else {
        return resolve_wasm_expression_projection_metric(runtime, aliases, projection);
    };

    let target_agg = match agg_name.as_str() {
        "sum" => Aggregation::Sum,
        "avg" => Aggregation::Avg,
        "min" => Aggregation::Min,
        "max" => Aggregation::Max,
        "median" => Aggregation::Median,
        "count" => Aggregation::Count,
        "count_distinct" => Aggregation::CountDistinct,
        _ => {
            return Err(SidemanticError::Validation(format!(
                "wasm rewrite fallback does not support function/aggregate projection '{projection}'"
            )));
        }
    };

    let inner_expr = inner_expr.unwrap_or_default();
    let (model_name, is_count_star, field_name) = if target_agg == Aggregation::Count {
        parse_wasm_count_input(aliases, default_model, &inner_expr)?
    } else {
        let (model_name, field_name) =
            parse_wasm_field_reference(aliases, default_model, &inner_expr)?;
        (model_name, false, Some(field_name))
    };

    let model = runtime.graph.get_model(&model_name).ok_or_else(|| {
        SidemanticError::Validation(format!(
            "wasm rewrite fallback model '{model_name}' not found"
        ))
    })?;

    for metric in &model.metrics {
        if metric.r#type != MetricType::Simple {
            continue;
        }

        let metric_agg = metric.agg.as_ref().unwrap_or(&Aggregation::Sum);
        if metric_agg != &target_agg {
            continue;
        }

        if is_count_star {
            let metric_sql = normalize_wasm_match_key(metric.sql_expr());
            if metric_sql == "*"
                || metric_sql.is_empty()
                || metric.name.eq_ignore_ascii_case("count")
            {
                return Ok(format!("{}.{}", model_name, metric.name));
            }
            continue;
        }

        let Some(field_name) = field_name.as_ref() else {
            continue;
        };
        let metric_sql = normalize_wasm_match_key(metric.sql_expr());
        let metric_name = normalize_wasm_match_key(&metric.name);
        let qualified_field = format!("{}.{}", model_name, field_name);

        if metric_sql == *field_name || metric_sql == qualified_field || metric_name == *field_name
        {
            return Ok(format!("{}.{}", model_name, metric.name));
        }
    }

    if is_count_star && target_agg == Aggregation::Count {
        return Err(SidemanticError::Validation(format!(
            "wasm rewrite fallback count(*) requires '{model_name}.count' metric"
        )));
    }

    Err(SidemanticError::Validation(format!(
        "wasm rewrite fallback projection '{projection}' requires a matching metric on model '{model_name}'"
    )))
}

#[cfg(any(target_arch = "wasm32", test))]
fn parse_simple_from_aliases(from_clause: &str) -> Result<(String, HashMap<String, String>)> {
    let lower = from_clause.to_ascii_lowercase();
    for unsupported in [
        " join ", " left ", " right ", " inner ", " outer ", " cross ", " full ", " union ",
        " group ", " order ", " having ", " limit ", " offset ", " with ",
    ] {
        if lower.contains(unsupported) {
            return Err(SidemanticError::Validation(format!(
                "wasm rewrite fallback only supports SELECT ... FROM ... [WHERE ...] [ORDER BY ...] [LIMIT ...]; unsupported clause in FROM: {from_clause}"
            )));
        }
    }
    if from_clause.contains(',') || from_clause.contains('(') || from_clause.contains(')') {
        return Err(SidemanticError::Validation(format!(
            "wasm rewrite fallback only supports a single table in FROM: {from_clause}"
        )));
    }

    let tokens: Vec<&str> = from_clause.split_whitespace().collect();
    let (model, alias) = match tokens.as_slice() {
        [model] => (normalize_ident(model), None),
        [model, alias] => (normalize_ident(model), Some(normalize_ident(alias))),
        [model, as_kw, alias] if as_kw.eq_ignore_ascii_case("as") => {
            (normalize_ident(model), Some(normalize_ident(alias)))
        }
        _ => {
            return Err(SidemanticError::Validation(format!(
                "wasm rewrite fallback could not parse FROM clause: {from_clause}"
            )));
        }
    };

    let mut aliases = HashMap::new();
    aliases.insert(model.clone(), model.clone());
    if let Some(alias) = alias {
        aliases.insert(alias, model.clone());
    }
    Ok((model, aliases))
}

#[cfg(any(target_arch = "wasm32", test))]
fn rewrite_where_aliases(where_sql: &str, aliases: &HashMap<String, String>) -> String {
    let mut output = where_sql.to_string();
    for (alias, model) in aliases {
        if alias == model {
            continue;
        }
        let pattern = format!(r"\b{}\s*\.", regex::escape(alias));
        if let Ok(re) = Regex::new(&pattern) {
            output = re.replace_all(&output, format!("{model}.")).to_string();
        }
    }
    output
}

#[cfg(any(target_arch = "wasm32", test))]
fn rewrite_with_yaml_wasm_fallback(runtime: &SidemanticRuntime, sql: &str) -> Result<String> {
    let select_re = Regex::new(
        r"(?is)^\s*select\s+(?P<select>.+?)\s+from\s+(?P<from>.+?)(?:\s+where\s+(?P<where>.+?))?(?:\s+order\s+by\s+(?P<order_by>.+?))?(?:\s+limit\s+(?P<limit>\d+))?\s*;?\s*$",
    )
    .expect("valid SELECT regex");
    let captures = select_re.captures(sql).ok_or_else(|| {
        SidemanticError::Validation(
            "wasm rewrite fallback only supports SELECT ... FROM ... [WHERE ...] [ORDER BY ...] [LIMIT ...]".into(),
        )
    })?;

    let select_sql = captures.name("select").map(|m| m.as_str()).ok_or_else(|| {
        SidemanticError::Validation("wasm rewrite fallback missing SELECT projection".into())
    })?;
    let from_sql = captures.name("from").map(|m| m.as_str()).ok_or_else(|| {
        SidemanticError::Validation("wasm rewrite fallback missing FROM clause".into())
    })?;

    let (default_model, aliases) = parse_simple_from_aliases(from_sql)?;
    let projection_items = split_top_level_csv(select_sql);
    if projection_items.is_empty() {
        return Err(SidemanticError::Validation(
            "wasm rewrite fallback requires at least one projection".into(),
        ));
    }
    let mut metrics = Vec::new();
    let mut metric_set = HashSet::new();
    let mut dimensions = Vec::new();
    let mut table_calculations: Vec<TableCalculation> = Vec::new();
    let mut projection_formula_counter = 0usize;
    let mut projection_aliases = HashMap::new();
    let mut projection_expression_refs = HashMap::new();
    let mut projection_order_refs: Vec<String> = Vec::new();
    for item in projection_items {
        let (projection, projection_alias) = split_alias_suffix(&item);
        if projection == "*" {
            return Err(SidemanticError::Validation(
                "wasm rewrite fallback does not support SELECT *".into(),
            ));
        }
        if projection.contains('(') || projection.contains(')') {
            match resolve_wasm_aggregate_projection(runtime, &aliases, &default_model, &projection)
            {
                Ok(semantic_ref) => {
                    if metric_set.insert(semantic_ref.clone()) {
                        metrics.push(semantic_ref.clone());
                    }
                    projection_expression_refs.insert(
                        normalize_wasm_projection_expression(&projection, &aliases),
                        semantic_ref.clone(),
                    );
                    projection_order_refs.push(semantic_ref.clone());
                    if let Some(alias) = projection_alias {
                        projection_aliases.insert(normalize_alias_key(&alias), semantic_ref);
                    }
                }
                Err(primary_err) => {
                    let (formula_metric_refs, formula_expr) =
                        build_wasm_formula_projection_from_aggregates(
                            runtime,
                            &aliases,
                            &default_model,
                            &projection,
                        )
                        .map_err(|_| primary_err)?;
                    for metric_ref in formula_metric_refs {
                        if metric_set.insert(metric_ref.clone()) {
                            metrics.push(metric_ref);
                        }
                    }
                    let calc_name = if let Some(alias) = projection_alias {
                        alias
                    } else {
                        projection_formula_counter += 1;
                        format!("expr_{}", projection_formula_counter)
                    };
                    let calculation =
                        TableCalculation::new(calc_name.clone(), TableCalcType::Formula)
                            .with_expression(formula_expr);
                    table_calculations.push(calculation);
                    projection_expression_refs.insert(
                        normalize_wasm_projection_expression(&projection, &aliases),
                        calc_name.clone(),
                    );
                    projection_aliases.insert(normalize_alias_key(&calc_name), calc_name.clone());
                    projection_order_refs.push(calc_name);
                }
            }
            continue;
        }

        let (semantic_ref, is_metric) =
            resolve_wasm_semantic_ref(runtime, &aliases, &default_model, &projection)?;
        if is_metric {
            if metric_set.insert(semantic_ref.clone()) {
                metrics.push(semantic_ref.clone());
            }
        } else {
            dimensions.push(semantic_ref.clone());
        }
        projection_expression_refs.insert(
            normalize_wasm_projection_expression(&projection, &aliases),
            semantic_ref.clone(),
        );
        projection_order_refs.push(semantic_ref.clone());
        if let Some(alias) = projection_alias {
            projection_aliases.insert(normalize_alias_key(&alias), semantic_ref);
        }
    }

    if metrics.is_empty() && dimensions.is_empty() && table_calculations.is_empty() {
        return Err(SidemanticError::Validation(
            "wasm rewrite fallback could not resolve projection fields".into(),
        ));
    }

    let mut filters = Vec::new();
    if let Some(where_match) = captures.name("where") {
        let where_sql = where_match.as_str().trim();
        if !where_sql.is_empty() {
            filters.push(rewrite_where_aliases(where_sql, &aliases));
        }
    }

    let mut order_by = Vec::new();
    if let Some(order_match) = captures.name("order_by") {
        let order_items = split_top_level_csv(order_match.as_str());
        for item in order_items {
            let (order_expr, order_suffix) = split_order_suffix(&item);
            let order_expr_trimmed = order_expr.trim();
            let semantic_ref = if let Ok(position) = order_expr_trimmed.parse::<usize>() {
                if position == 0 || position > projection_order_refs.len() {
                    return Err(SidemanticError::Validation(format!(
                        "wasm rewrite fallback ORDER BY position {position} is out of range for {} selected columns",
                        projection_order_refs.len()
                    )));
                }
                projection_order_refs[position - 1].clone()
            } else if let Some(alias_ref) =
                projection_aliases.get(&normalize_alias_key(order_expr_trimmed))
            {
                alias_ref.clone()
            } else if let Some(expression_ref) = projection_expression_refs.get(
                &normalize_wasm_projection_expression(order_expr_trimmed, &aliases),
            ) {
                expression_ref.clone()
            } else if order_expr_trimmed.contains('(') || order_expr_trimmed.contains(')') {
                resolve_wasm_aggregate_projection(
                    runtime,
                    &aliases,
                    &default_model,
                    order_expr_trimmed,
                )?
            } else {
                resolve_wasm_semantic_ref(runtime, &aliases, &default_model, order_expr_trimmed)?.0
            };

            if order_suffix.is_empty() {
                order_by.push(semantic_ref);
            } else {
                order_by.push(format!("{semantic_ref} {order_suffix}"));
            }
        }
    }

    let limit = captures
        .name("limit")
        .map(|m| {
            m.as_str().trim().parse::<usize>().map_err(|e| {
                SidemanticError::Validation(format!(
                    "wasm rewrite fallback could not parse LIMIT value '{}': {e}",
                    m.as_str().trim()
                ))
            })
        })
        .transpose()?;

    let mut query = SemanticQuery::new()
        .with_metrics(metrics)
        .with_dimensions(dimensions)
        .with_filters(filters)
        .with_table_calculations(table_calculations);
    if !order_by.is_empty() {
        query = query.with_order_by(order_by);
    }
    if let Some(limit) = limit {
        query = query.with_limit(limit);
    }
    runtime
        .compile(&query)
        .map_err(|e| SidemanticError::Validation(format!("wasm rewrite fallback failed: {e}")))
}

/// Rewrite SQL using graph YAML payload.
pub fn rewrite_with_yaml(yaml: &str, sql: &str) -> Result<String> {
    let runtime = SidemanticRuntime::from_yaml(yaml)
        .map_err(|e| SidemanticError::Validation(format!("failed to load YAML models: {e}")))?;
    match runtime.rewrite(sql) {
        Ok(rewritten) => Ok(rewritten),
        Err(rewrite_error) => {
            #[cfg(target_arch = "wasm32")]
            {
                if rewrite_error
                    .to_string()
                    .contains("operation not supported on this platform")
                {
                    return rewrite_with_yaml_wasm_fallback(&runtime, sql);
                }
            }
            Err(SidemanticError::Validation(format!(
                "failed to rewrite SQL: {rewrite_error}"
            )))
        }
    }
}

/// Generate pre-aggregation materialization SQL from graph YAML payload.
pub fn generate_preaggregation_materialization_sql_with_yaml(
    yaml: &str,
    model_name: &str,
    preagg_name: &str,
) -> Result<String> {
    let runtime = SidemanticRuntime::from_yaml(yaml)
        .map_err(|e| SidemanticError::Validation(format!("failed to load YAML models: {e}")))?;
    runtime.generate_preaggregation_materialization_sql(model_name, preagg_name)
}

/// Generate catalog metadata from graph YAML payload.
pub fn generate_catalog_metadata_with_yaml(yaml: &str, schema: &str) -> Result<String> {
    let runtime = SidemanticRuntime::from_yaml(yaml)
        .map_err(|e| SidemanticError::Validation(format!("failed to load YAML models: {e}")))?;
    runtime.generate_catalog_metadata(schema)
}

/// Resolve model inheritance and return resolved models as YAML.
pub fn resolve_model_inheritance_with_yaml(yaml: &str) -> Result<String> {
    let config: crate::config::SidemanticConfig = serde_yaml::from_str(yaml)
        .map_err(|e| SidemanticError::Validation(format!("failed to parse models payload: {e}")))?;
    let extends_map: HashMap<String, String> = config
        .models
        .iter()
        .filter_map(|model| {
            model
                .extends
                .as_ref()
                .map(|extends| (model.name.clone(), extends.clone()))
        })
        .collect();
    let (models, _, _) = config.into_parts();

    let mut models_map: HashMap<String, crate::core::Model> = HashMap::new();
    for model in models {
        if models_map.contains_key(&model.name) {
            return Err(SidemanticError::Validation(format!(
                "Duplicate model '{}' in inheritance payload",
                model.name
            )));
        }
        models_map.insert(model.name.clone(), model);
    }

    let resolved = resolve_models_inheritance(models_map, &extends_map).map_err(|e| {
        SidemanticError::Validation(format!("failed to resolve model inheritance: {e}"))
    })?;
    let mut resolved_models: Vec<_> = resolved.into_values().collect();
    resolved_models.sort_by(|left, right| left.name.cmp(&right.name));

    serde_yaml::to_string(&resolved_models).map_err(|e| {
        SidemanticError::SqlGeneration(format!("failed to serialize resolved models: {e}"))
    })
}

fn metric_inheritance_mapping_get_string(
    mapping: &serde_yaml::Mapping,
    key: &str,
) -> Option<String> {
    let key_value = serde_yaml::Value::String(key.to_owned());
    mapping
        .get(&key_value)
        .and_then(serde_yaml::Value::as_str)
        .map(str::to_owned)
}

fn metric_inheritance_mapping_get_sequence(
    mapping: &serde_yaml::Mapping,
    key: &str,
) -> Option<Vec<serde_yaml::Value>> {
    let key_value = serde_yaml::Value::String(key.to_owned());
    mapping
        .get(&key_value)
        .and_then(serde_yaml::Value::as_sequence)
        .cloned()
}

fn metric_inheritance_mapping_set(
    mapping: &mut serde_yaml::Mapping,
    key: &str,
    value: serde_yaml::Value,
) {
    mapping.insert(serde_yaml::Value::String(key.to_owned()), value);
}

fn merge_metric_inheritance_mappings(
    child: &serde_yaml::Mapping,
    parent: &serde_yaml::Mapping,
) -> serde_yaml::Mapping {
    let mut merged = parent.clone();
    merged.remove(serde_yaml::Value::String("name".to_string()));

    for field in ["filters", "drill_fields"] {
        let parent_items =
            metric_inheritance_mapping_get_sequence(&merged, field).unwrap_or_default();
        let child_items = metric_inheritance_mapping_get_sequence(child, field);

        if let Some(items) = child_items {
            if !items.is_empty() {
                let mut combined = parent_items.clone();
                combined.extend(items);
                metric_inheritance_mapping_set(
                    &mut merged,
                    field,
                    serde_yaml::Value::Sequence(combined),
                );
                continue;
            }
        }

        if parent_items.is_empty() {
            metric_inheritance_mapping_set(&mut merged, field, serde_yaml::Value::Null);
        } else {
            metric_inheritance_mapping_set(
                &mut merged,
                field,
                serde_yaml::Value::Sequence(parent_items),
            );
        }
    }

    for (key, value) in child {
        let Some(field_name) = key.as_str() else {
            continue;
        };
        if matches!(field_name, "filters" | "drill_fields" | "extends" | "name") {
            continue;
        }
        merged.insert(key.clone(), value.clone());
    }

    if let Some(name) = metric_inheritance_mapping_get_string(child, "name") {
        metric_inheritance_mapping_set(&mut merged, "name", serde_yaml::Value::String(name));
    }

    merged
}

fn resolve_metric_inheritance_mapping(
    name: &str,
    metrics: &HashMap<String, serde_yaml::Mapping>,
    resolved: &mut HashMap<String, serde_yaml::Mapping>,
    in_progress: &mut HashSet<String>,
) -> std::result::Result<serde_yaml::Mapping, String> {
    if let Some(metric) = resolved.get(name) {
        return Ok(metric.clone());
    }

    if in_progress.contains(name) {
        return Err(format!("Circular inheritance detected for metric '{name}'"));
    }

    let metric = metrics
        .get(name)
        .ok_or_else(|| format!("Metric '{name}' not found"))?;

    let Some(parent_name) = metric_inheritance_mapping_get_string(metric, "extends") else {
        resolved.insert(name.to_string(), metric.clone());
        return Ok(metric.clone());
    };

    in_progress.insert(name.to_string());
    let parent_metric =
        resolve_metric_inheritance_mapping(&parent_name, metrics, resolved, in_progress)?;
    in_progress.remove(name);

    let merged = merge_metric_inheritance_mappings(metric, &parent_metric);
    resolved.insert(name.to_string(), merged.clone());
    Ok(merged)
}

/// Resolve metric inheritance and return resolved metrics as YAML.
pub fn resolve_metric_inheritance(metrics_yaml: &str) -> Result<String> {
    let payload: serde_yaml::Value = serde_yaml::from_str(metrics_yaml).map_err(|e| {
        SidemanticError::Validation(format!("failed to parse metrics payload: {e}"))
    })?;
    let metric_items = payload.as_sequence().ok_or_else(|| {
        SidemanticError::Validation("metrics payload must be a YAML sequence".to_string())
    })?;

    let mut metrics_by_name: HashMap<String, serde_yaml::Mapping> = HashMap::new();
    for item in metric_items {
        let mapping = item.as_mapping().ok_or_else(|| {
            SidemanticError::Validation("each metric payload item must be a mapping".to_string())
        })?;
        let metric_name =
            metric_inheritance_mapping_get_string(mapping, "name").ok_or_else(|| {
                SidemanticError::Validation("metric payload item missing 'name'".to_string())
            })?;
        if metrics_by_name.contains_key(&metric_name) {
            return Err(SidemanticError::Validation(format!(
                "Duplicate metric '{}' in inheritance payload",
                metric_name
            )));
        }
        metrics_by_name.insert(metric_name, mapping.clone());
    }

    let mut resolved: HashMap<String, serde_yaml::Mapping> = HashMap::new();
    let mut in_progress: HashSet<String> = HashSet::new();
    let mut metric_names: Vec<String> = metrics_by_name.keys().cloned().collect();
    metric_names.sort();
    for metric_name in metric_names {
        resolve_metric_inheritance_mapping(
            &metric_name,
            &metrics_by_name,
            &mut resolved,
            &mut in_progress,
        )
        .map_err(|e| {
            SidemanticError::Validation(format!("failed to resolve metric inheritance: {e}"))
        })?;
    }

    let mut resolved_metrics: Vec<serde_yaml::Value> = resolved
        .into_values()
        .map(serde_yaml::Value::Mapping)
        .collect();
    resolved_metrics.sort_by(|left, right| {
        let left_name = left
            .as_mapping()
            .and_then(|mapping| metric_inheritance_mapping_get_string(mapping, "name"))
            .unwrap_or_default();
        let right_name = right
            .as_mapping()
            .and_then(|mapping| metric_inheritance_mapping_get_string(mapping, "name"))
            .unwrap_or_default();
        left_name.cmp(&right_name)
    });

    serde_yaml::to_string(&resolved_metrics).map_err(|e| {
        SidemanticError::SqlGeneration(format!("failed to serialize resolved metrics: {e}"))
    })
}

fn model_dimension_by_name<'a>(
    dimensions: &'a [ModelHierarchyDimensionPayload],
    name: &str,
) -> Option<&'a ModelHierarchyDimensionPayload> {
    dimensions.iter().find(|dimension| dimension.name == name)
}

fn model_item_index_by_name(items: &[ModelLookupItemPayload], name: &str) -> Option<usize> {
    items.iter().position(|item| item.name == name)
}

fn payload_has_non_empty_string(mapping: &serde_yaml::Mapping, key: &str) -> bool {
    let key_value = serde_yaml::Value::String(key.to_owned());
    mapping
        .get(&key_value)
        .and_then(serde_yaml::Value::as_str)
        .map(|v| !v.trim().is_empty())
        .unwrap_or(false)
}

fn payload_optional_string(mapping: &serde_yaml::Mapping, key: &str) -> Option<String> {
    let key_value = serde_yaml::Value::String(key.to_owned());
    mapping
        .get(&key_value)
        .and_then(serde_yaml::Value::as_str)
        .map(str::to_owned)
}

fn relationship_helper_type_name(payload: &RelationshipHelperPayload) -> &str {
    payload
        .relationship_type
        .as_deref()
        .unwrap_or("many_to_one")
}

fn relationship_key_is_truthy(value: &Option<RelationshipKeyPayload>) -> bool {
    match value {
        Some(RelationshipKeyPayload::Single(item)) => !item.is_empty(),
        Some(RelationshipKeyPayload::Multiple(items)) => !items.is_empty(),
        None => false,
    }
}

fn relationship_key_first(value: &RelationshipKeyPayload) -> Option<&str> {
    match value {
        RelationshipKeyPayload::Single(item) => Some(item.as_str()),
        RelationshipKeyPayload::Multiple(items) => items.first().map(String::as_str),
    }
}

fn relationship_key_to_columns(value: &RelationshipKeyPayload) -> Vec<String> {
    match value {
        RelationshipKeyPayload::Single(item) => vec![item.clone()],
        RelationshipKeyPayload::Multiple(items) => items.clone(),
    }
}

/// Resolve dimension SQL expression from a YAML payload.
pub fn dimension_sql_expr_with_yaml(dimension_yaml: &str) -> Result<String> {
    let dimension: DimensionHelperPayload = serde_yaml::from_str(dimension_yaml).map_err(|e| {
        SidemanticError::Validation(format!("failed to parse dimension payload: {e}"))
    })?;
    Ok(dimension.sql.unwrap_or(dimension.name))
}

/// Apply time granularity to a dimension SQL expression from YAML payload.
pub fn dimension_with_granularity_with_yaml(
    dimension_yaml: &str,
    granularity: &str,
) -> Result<String> {
    let dimension: DimensionHelperPayload = serde_yaml::from_str(dimension_yaml).map_err(|e| {
        SidemanticError::Validation(format!("failed to parse dimension payload: {e}"))
    })?;

    if dimension.dimension_type.as_deref() != Some("time") {
        return Err(SidemanticError::Validation(format!(
            "Cannot apply granularity to non-time dimension {}",
            dimension.name
        )));
    }

    let supported = dimension.supported_granularities.unwrap_or_else(|| {
        vec![
            "second".to_string(),
            "minute".to_string(),
            "hour".to_string(),
            "day".to_string(),
            "week".to_string(),
            "month".to_string(),
            "quarter".to_string(),
            "year".to_string(),
        ]
    });

    if !supported.iter().any(|value| value == granularity) {
        return Err(SidemanticError::Validation(format!(
            "Granularity {granularity} not supported for {}. Supported: {}",
            dimension.name,
            format_python_string_list(&supported)
        )));
    }

    let sql_expr = dimension.sql.unwrap_or(dimension.name);
    Ok(format!("DATE_TRUNC('{granularity}', {sql_expr})"))
}

/// Get full hierarchy path from root to a given dimension from YAML payload.
pub fn model_get_hierarchy_path_with_yaml(
    model_yaml: &str,
    dimension_name: &str,
) -> Result<Vec<String>> {
    let model: ModelHierarchyPayload = serde_yaml::from_str(model_yaml)
        .map_err(|e| SidemanticError::Validation(format!("failed to parse model payload: {e}")))?;

    let Some(mut current) = model_dimension_by_name(&model.dimensions, dimension_name) else {
        return Ok(Vec::new());
    };

    let mut path = vec![dimension_name.to_string()];
    while let Some(parent) = current.parent.as_deref() {
        path.insert(0, parent.to_string());
        let Some(next) = model_dimension_by_name(&model.dimensions, parent) else {
            break;
        };
        current = next;
    }

    Ok(path)
}

/// Get first child dimension in hierarchy order from YAML payload.
pub fn model_get_drill_down_with_yaml(
    model_yaml: &str,
    dimension_name: &str,
) -> Result<Option<String>> {
    let model: ModelHierarchyPayload = serde_yaml::from_str(model_yaml)
        .map_err(|e| SidemanticError::Validation(format!("failed to parse model payload: {e}")))?;

    for dimension in &model.dimensions {
        if dimension.parent.as_deref() == Some(dimension_name) {
            return Ok(Some(dimension.name.clone()));
        }
    }
    Ok(None)
}

/// Get parent dimension from YAML payload.
pub fn model_get_drill_up_with_yaml(
    model_yaml: &str,
    dimension_name: &str,
) -> Result<Option<String>> {
    let model: ModelHierarchyPayload = serde_yaml::from_str(model_yaml)
        .map_err(|e| SidemanticError::Validation(format!("failed to parse model payload: {e}")))?;
    Ok(model_dimension_by_name(&model.dimensions, dimension_name)
        .and_then(|dim| dim.parent.clone()))
}

/// Get dimension index by name from YAML payload.
pub fn model_find_dimension_index_with_yaml(model_yaml: &str, name: &str) -> Result<Option<usize>> {
    let model: ModelLookupPayload = serde_yaml::from_str(model_yaml)
        .map_err(|e| SidemanticError::Validation(format!("failed to parse model payload: {e}")))?;
    Ok(model_item_index_by_name(&model.dimensions, name))
}

/// Get metric index by name from YAML payload.
pub fn model_find_metric_index_with_yaml(model_yaml: &str, name: &str) -> Result<Option<usize>> {
    let model: ModelLookupPayload = serde_yaml::from_str(model_yaml)
        .map_err(|e| SidemanticError::Validation(format!("failed to parse model payload: {e}")))?;
    Ok(model_item_index_by_name(&model.metrics, name))
}

/// Get segment index by name from YAML payload.
pub fn model_find_segment_index_with_yaml(model_yaml: &str, name: &str) -> Result<Option<usize>> {
    let model: ModelLookupPayload = serde_yaml::from_str(model_yaml)
        .map_err(|e| SidemanticError::Validation(format!("failed to parse model payload: {e}")))?;
    Ok(model_item_index_by_name(&model.segments, name))
}

/// Get pre-aggregation index by name from YAML payload.
pub fn model_find_pre_aggregation_index_with_yaml(
    model_yaml: &str,
    name: &str,
) -> Result<Option<usize>> {
    let model: ModelLookupPayload = serde_yaml::from_str(model_yaml)
        .map_err(|e| SidemanticError::Validation(format!("failed to parse model payload: {e}")))?;
    Ok(model_item_index_by_name(&model.pre_aggregations, name))
}

/// Resolve relationship SQL expression from YAML payload.
pub fn relationship_sql_expr_with_yaml(relationship_yaml: &str) -> Result<String> {
    let relationship: RelationshipHelperPayload =
        serde_yaml::from_str(relationship_yaml).map_err(|e| {
            SidemanticError::Validation(format!("failed to parse relationship payload: {e}"))
        })?;

    if relationship_key_is_truthy(&relationship.foreign_key) {
        if let Some(foreign_key) = relationship.foreign_key.as_ref() {
            if let Some(first_key) = relationship_key_first(foreign_key) {
                return Ok(first_key.to_string());
            }
        }
        return Ok(format!("{}_id", relationship.name));
    }

    if relationship_helper_type_name(&relationship) == "many_to_one" {
        return Ok(format!("{}_id", relationship.name));
    }
    Ok("id".to_string())
}

/// Resolve relationship related key from YAML payload.
pub fn relationship_related_key_with_yaml(relationship_yaml: &str) -> Result<String> {
    let relationship: RelationshipHelperPayload =
        serde_yaml::from_str(relationship_yaml).map_err(|e| {
            SidemanticError::Validation(format!("failed to parse relationship payload: {e}"))
        })?;

    if relationship_key_is_truthy(&relationship.primary_key) {
        if let Some(primary_key) = relationship.primary_key.as_ref() {
            if let Some(first_key) = relationship_key_first(primary_key) {
                return Ok(first_key.to_string());
            }
        }
    }
    Ok("id".to_string())
}

/// Resolve relationship foreign-key columns from YAML payload.
pub fn relationship_foreign_key_columns_with_yaml(relationship_yaml: &str) -> Result<Vec<String>> {
    let relationship: RelationshipHelperPayload =
        serde_yaml::from_str(relationship_yaml).map_err(|e| {
            SidemanticError::Validation(format!("failed to parse relationship payload: {e}"))
        })?;

    match relationship.foreign_key {
        None => {
            if relationship_helper_type_name(&relationship) == "many_to_one" {
                Ok(vec![format!("{}_id", relationship.name)])
            } else {
                Ok(vec!["id".to_string()])
            }
        }
        Some(foreign_key) => Ok(relationship_key_to_columns(&foreign_key)),
    }
}

/// Resolve relationship primary-key columns from YAML payload.
pub fn relationship_primary_key_columns_with_yaml(relationship_yaml: &str) -> Result<Vec<String>> {
    let relationship: RelationshipHelperPayload =
        serde_yaml::from_str(relationship_yaml).map_err(|e| {
            SidemanticError::Validation(format!("failed to parse relationship payload: {e}"))
        })?;

    match relationship.primary_key {
        None => Ok(vec!["id".to_string()]),
        Some(primary_key) => Ok(relationship_key_to_columns(&primary_key)),
    }
}

/// Resolve segment SQL from YAML payload.
pub fn segment_get_sql_with_yaml(segment_yaml: &str, model_alias: &str) -> Result<String> {
    let segment: SegmentHelperPayload = serde_yaml::from_str(segment_yaml).map_err(|e| {
        SidemanticError::Validation(format!("failed to parse segment payload: {e}"))
    })?;
    Ok(segment.sql.replace("{model}", model_alias))
}

fn detect_adapter_kind_impl(path: &str, content: &str) -> Option<&'static str> {
    let lower_path = path.to_ascii_lowercase();
    if lower_path.ends_with(".lkml") {
        return Some("lookml");
    }
    if lower_path.ends_with(".malloy") {
        return Some("malloy");
    }
    if lower_path.ends_with(".sql") {
        return Some("sidemantic");
    }
    if !(lower_path.ends_with(".yml") || lower_path.ends_with(".yaml")) {
        return None;
    }

    if content.contains("models:") {
        return Some("sidemantic");
    }
    if content.contains("cubes:") || (content.contains("views:") && content.contains("measures:")) {
        return Some("cube");
    }
    if content.contains("semantic_models:")
        || (content.contains("metrics:") && content.contains("type: "))
    {
        return Some("metricflow");
    }
    if content.contains("base_sql_table:") && content.contains("measures:") {
        return Some("hex");
    }
    if content.contains("tables:") && content.contains("base_table:") {
        return Some("snowflake");
    }
    if content.contains("_.") && (content.contains("dimensions:") || content.contains("measures:"))
    {
        return Some("bsl");
    }
    if content.contains("type: metrics_view") {
        return Some("rill");
    }
    if content.contains("table_name:")
        && content.contains("columns:")
        && content.contains("metrics:")
    {
        return Some("superset");
    }
    if content.contains("measures:")
        && content.contains("dimensions:")
        && (content.contains("table_name:")
            || content.contains("table:")
            || content.contains("schema:"))
    {
        return Some("omni");
    }

    None
}

/// Detect adapter kind from file path and content.
pub fn detect_adapter_kind(path: &str, content: &str) -> Option<String> {
    detect_adapter_kind_impl(path, content).map(str::to_string)
}

/// Extract column references from a SQL expression.
pub fn extract_column_references(sql_expr: &str) -> Vec<String> {
    let mut refs: Vec<String> = extract_column_references_from_expr(sql_expr)
        .into_iter()
        .collect();
    refs.sort();
    refs
}

fn is_identifier_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

fn find_top_level_keyword(haystack: &str, keyword: &str) -> Option<usize> {
    let mut depth = 0i32;
    let mut in_quote: Option<char> = None;
    let mut escaped = false;

    for (idx, ch) in haystack.char_indices() {
        if let Some(quote) = in_quote {
            if escaped {
                escaped = false;
                continue;
            }
            if ch == '\\' {
                escaped = true;
                continue;
            }
            if ch == quote {
                in_quote = None;
            }
            continue;
        }

        if ch == '\'' || ch == '"' {
            in_quote = Some(ch);
            continue;
        }

        match ch {
            '(' => {
                depth += 1;
                continue;
            }
            ')' => {
                depth = (depth - 1).max(0);
                continue;
            }
            _ => {}
        }

        if depth != 0 {
            continue;
        }

        let end = idx.saturating_add(keyword.len());
        let Some(candidate) = haystack.get(idx..end) else {
            continue;
        };
        if !candidate.eq_ignore_ascii_case(keyword) {
            continue;
        }

        let before = haystack[..idx].chars().next_back();
        if before.is_some_and(is_identifier_char) {
            continue;
        }
        let after = haystack[end..].chars().next();
        if after.is_some_and(is_identifier_char) {
            continue;
        }
        return Some(idx);
    }

    None
}

fn split_top_level_csv_migrator(input: &str) -> Vec<String> {
    let mut values = Vec::new();
    let mut current = String::new();
    let mut depth = 0i32;
    let mut in_quote: Option<char> = None;
    let mut escaped = false;

    for ch in input.chars() {
        if let Some(quote) = in_quote {
            current.push(ch);
            if escaped {
                escaped = false;
                continue;
            }
            if ch == '\\' {
                escaped = true;
                continue;
            }
            if ch == quote {
                in_quote = None;
            }
            continue;
        }

        if ch == '\'' || ch == '"' {
            in_quote = Some(ch);
            current.push(ch);
            continue;
        }

        match ch {
            '(' => depth += 1,
            ')' => depth = (depth - 1).max(0),
            ',' if depth == 0 => {
                let trimmed = current.trim();
                if !trimmed.is_empty() {
                    values.push(trimmed.to_string());
                }
                current.clear();
                continue;
            }
            _ => {}
        }
        current.push(ch);
    }

    let trailing = current.trim();
    if !trailing.is_empty() {
        values.push(trailing.to_string());
    }
    values
}

fn split_expression_alias(expr: &str) -> (String, Option<String>) {
    let mut depth = 0i32;
    let mut in_quote: Option<char> = None;
    let mut escaped = false;
    let mut last_as_idx: Option<usize> = None;
    let bytes = expr.as_bytes();

    for (idx, ch) in expr.char_indices() {
        if let Some(quote) = in_quote {
            if escaped {
                escaped = false;
                continue;
            }
            if ch == '\\' {
                escaped = true;
                continue;
            }
            if ch == quote {
                in_quote = None;
            }
            continue;
        }

        if ch == '\'' || ch == '"' {
            in_quote = Some(ch);
            continue;
        }

        match ch {
            '(' => {
                depth += 1;
                continue;
            }
            ')' => {
                depth = (depth - 1).max(0);
                continue;
            }
            _ => {}
        }

        if depth != 0 {
            continue;
        }

        let Some(next) = expr.get(idx..) else {
            continue;
        };
        if !next.starts_with('a') && !next.starts_with('A') {
            continue;
        }
        let end = idx.saturating_add(2);
        let Some(token) = expr.get(idx..end) else {
            continue;
        };
        if !token.eq_ignore_ascii_case("as") {
            continue;
        }

        let prev_whitespace = if idx == 0 {
            false
        } else {
            bytes[idx - 1].is_ascii_whitespace()
        };
        let next_whitespace = if end >= bytes.len() {
            false
        } else {
            bytes[end].is_ascii_whitespace()
        };
        if prev_whitespace && next_whitespace {
            last_as_idx = Some(idx);
        }
    }

    if let Some(idx) = last_as_idx {
        let expr_body = expr[..idx].trim().to_string();
        let alias = expr[idx + 2..].trim().to_string();
        if !expr_body.is_empty() && !alias.is_empty() {
            return (expr_body, Some(alias));
        }
    }

    (expr.trim().to_string(), None)
}

fn split_reference(reference: &str) -> (String, String) {
    let token = reference.trim().trim_matches('`').trim_matches('"');
    if let Some((table, column)) = token.rsplit_once('.') {
        let table_name = table
            .trim()
            .trim_matches('`')
            .trim_matches('"')
            .trim_matches('[')
            .trim_matches(']')
            .to_string();
        let column_name = column
            .trim()
            .trim_matches('`')
            .trim_matches('"')
            .trim_matches('[')
            .trim_matches(']')
            .to_string();
        return (table_name, column_name);
    }
    (
        String::new(),
        token
            .trim_matches('[')
            .trim_matches(']')
            .trim_matches('`')
            .trim_matches('"')
            .to_string(),
    )
}

fn is_keyword_token(token: &str) -> bool {
    [
        "select",
        "from",
        "where",
        "and",
        "or",
        "not",
        "null",
        "case",
        "when",
        "then",
        "else",
        "end",
        "as",
        "distinct",
        "over",
        "partition",
        "order",
        "by",
        "rows",
        "range",
        "between",
        "preceding",
        "current",
        "row",
    ]
    .iter()
    .any(|keyword| keyword.eq_ignore_ascii_case(token))
}

fn is_cast_type_token(token: &str) -> bool {
    [
        "float",
        "double",
        "decimal",
        "numeric",
        "integer",
        "int",
        "bigint",
        "smallint",
        "real",
        "boolean",
        "bool",
        "date",
        "time",
        "timestamp",
        "varchar",
        "text",
    ]
    .iter()
    .any(|cast_type| cast_type.eq_ignore_ascii_case(token))
}

fn sanitize_reference_token(raw: &str) -> Option<String> {
    let mut token = raw.trim();
    while let Some(stripped) = token.strip_prefix('.') {
        token = stripped;
    }
    if token.is_empty()
        || is_keyword_token(token)
        || is_cast_type_token(token)
        || token.eq_ignore_ascii_case("cube")
        || token.parse::<f64>().is_ok()
    {
        return None;
    }
    Some(token.to_string())
}

fn extract_column_references_migrator(sql_expr: &str) -> Vec<String> {
    let mut refs = BTreeSet::new();
    let normalized_sql = sql_expr.replace("${CUBE}.", "").replace("${CUBE}", "");

    let mut current = String::new();
    let mut in_quote = false;
    let mut previous_char = ' ';

    for ch in normalized_sql.chars() {
        if ch == '\'' && previous_char != '\\' {
            in_quote = !in_quote;
        }

        if !in_quote {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '.' {
                current.push(ch);
            } else {
                let is_function_call = ch == '(';
                if !is_function_call {
                    if let Some(cleaned) = sanitize_reference_token(&current) {
                        refs.insert(cleaned);
                    }
                }
                current.clear();
            }
        }
        previous_char = ch;
    }

    if let Some(cleaned) = sanitize_reference_token(&current) {
        refs.insert(cleaned);
    }

    refs.into_iter().collect()
}

fn find_select_expressions(sql_query: &str) -> Vec<String> {
    let Some(select_idx) = find_top_level_keyword(sql_query, "select") else {
        return Vec::new();
    };
    let select_end = select_idx + "select".len();
    let Some(from_offset) = find_top_level_keyword(&sql_query[select_end..], "from") else {
        return Vec::new();
    };
    let from_idx = select_end + from_offset;
    split_top_level_csv_migrator(&sql_query[select_end..from_idx])
}

fn parse_group_by_clause(sql_query: &str) -> Option<String> {
    let group_by_idx = find_top_level_keyword(sql_query, "group by")?;
    let group_by_end = group_by_idx + "group by".len();
    let tail = &sql_query[group_by_end..];

    let mut end = tail.len();
    for keyword in ["having", "order by", "limit", "union", "qualify"] {
        if let Some(offset) = find_top_level_keyword(tail, keyword) {
            end = end.min(offset);
        }
    }
    Some(tail[..end].trim().to_string())
}

fn parse_group_by_columns(sql_query: &str, select_expressions: &[String]) -> Vec<(String, String)> {
    let Some(group_by_clause) = parse_group_by_clause(sql_query) else {
        return Vec::new();
    };

    let mut values: BTreeSet<(String, String)> = BTreeSet::new();
    for item in split_top_level_csv_migrator(&group_by_clause) {
        let target_expr = item.trim();
        if target_expr.is_empty() {
            continue;
        }

        let mut expr_to_extract = target_expr.to_string();
        if let Ok(ordinal) = target_expr.parse::<usize>() {
            if ordinal > 0 && ordinal <= select_expressions.len() {
                let (expr_body, _) = split_expression_alias(&select_expressions[ordinal - 1]);
                expr_to_extract = expr_body;
            }
        }

        for reference in extract_column_references_migrator(&expr_to_extract) {
            let (table_name, column_name) = split_reference(&reference);
            if !column_name.is_empty() {
                values.insert((table_name, column_name));
            }
        }
    }

    values.into_iter().collect()
}

fn extract_aggregation_calls(expr: &str) -> Vec<(String, String)> {
    let Ok(re) = Regex::new(r"(?i)\b(sum|avg|count|min|max|median)\s*\(([^)]*)\)") else {
        return Vec::new();
    };

    re.captures_iter(expr)
        .filter_map(|capture| {
            let agg = capture.get(1)?.as_str().to_ascii_lowercase();
            let arg = capture.get(2)?.as_str().trim().to_string();
            Some((agg, arg))
        })
        .collect()
}

fn contains_top_level_arithmetic_operator(expr: &str) -> bool {
    let mut depth = 0i32;
    let mut in_quote: Option<char> = None;
    let mut escaped = false;
    for ch in expr.chars() {
        if let Some(quote) = in_quote {
            if escaped {
                escaped = false;
                continue;
            }
            if ch == '\\' {
                escaped = true;
                continue;
            }
            if ch == quote {
                in_quote = None;
            }
            continue;
        }

        if ch == '\'' || ch == '"' {
            in_quote = Some(ch);
            continue;
        }

        match ch {
            '(' => depth += 1,
            ')' => depth = (depth - 1).max(0),
            '+' | '-' | '*' | '/' if depth == 0 => return true,
            _ => {}
        }
    }
    false
}

fn infer_table_from_expression(expr: &str) -> String {
    for reference in extract_column_references_migrator(expr) {
        let (table_name, _column_name) = split_reference(&reference);
        if !table_name.is_empty() {
            return table_name;
        }
    }
    String::new()
}

fn normalize_aggregation_tuple(agg_type: &str, agg_arg: &str) -> (String, String, String) {
    let mut normalized_agg = agg_type.to_ascii_lowercase();
    let mut target_arg = agg_arg.trim().to_string();
    if normalized_agg == "count" && target_arg.to_ascii_lowercase().starts_with("distinct ") {
        normalized_agg = "count_distinct".to_string();
        target_arg = target_arg[8..].trim().to_string();
    }

    if normalized_agg == "count" && target_arg.is_empty() {
        target_arg = "*".to_string();
    }

    let references = extract_column_references_migrator(&target_arg);
    if let Some(reference) = references.first() {
        let (table_name, column_name) = split_reference(reference);
        if !column_name.is_empty() {
            return (normalized_agg, column_name, table_name);
        }
    }

    let (table_name, column_name) = split_reference(&target_arg);
    let normalized_column = if column_name.is_empty() {
        target_arg
    } else {
        column_name
    };
    (normalized_agg, normalized_column, table_name)
}

fn parse_window_parameters(expr: &str) -> (Option<String>, Option<String>) {
    let mut window: Option<String> = None;
    let mut grain_to_date: Option<String> = None;

    if let Ok(re) = Regex::new(r"(?i)(\d+)\s+preceding") {
        if let Some(capture) = re.captures(expr) {
            if let Some(size) = capture.get(1) {
                window = Some(format!("{} days", size.as_str()));
            }
        }
    }

    if let Ok(re) =
        Regex::new(r"(?i)partition\s+by\s+date_trunc\s*\(\s*'?(year|quarter|month|week|day)'?")
    {
        if let Some(capture) = re.captures(expr) {
            if let Some(grain) = capture.get(1) {
                grain_to_date = Some(grain.as_str().to_ascii_lowercase());
            }
        }
    }

    if grain_to_date.is_none() {
        if let Ok(re) =
            Regex::new(r"(?i)partition\s+by\s+extract\s*\(\s*(year|quarter|month|week|day)\s+from")
        {
            if let Some(capture) = re.captures(expr) {
                if let Some(grain) = capture.get(1) {
                    grain_to_date = Some(grain.as_str().to_ascii_lowercase());
                }
            }
        }
    }

    (window, grain_to_date)
}

/// Analyze migrator query components (columns/group-by/derived/window) for Python migrator helpers.
pub fn analyze_migrator_query(sql_query: &str) -> Result<String> {
    let mut payload = MigratorAnalysisPayload {
        column_references: extract_column_references_migrator(sql_query),
        ..Default::default()
    };

    let select_expressions = find_select_expressions(sql_query);
    payload.group_by_columns = parse_group_by_columns(sql_query, &select_expressions);

    for select_expr in &select_expressions {
        let (expr_body, alias) = split_expression_alias(select_expr);
        if expr_body.is_empty() {
            continue;
        }

        let agg_calls = extract_aggregation_calls(&expr_body);
        if agg_calls.is_empty() {
            continue;
        }

        let expr_lower = expr_body.to_ascii_lowercase();
        let is_window_expr = expr_lower.contains(" over (") || expr_lower.contains(" over(");

        if is_window_expr {
            let (agg_type, agg_column, mut table_name) =
                normalize_aggregation_tuple(&agg_calls[0].0, &agg_calls[0].1);
            if table_name.is_empty() {
                table_name = infer_table_from_expression(&expr_body);
            }

            let base_metric_name = if agg_type == "count" && agg_column == "*" {
                "count".to_string()
            } else if agg_type == "count" || agg_type == "count_distinct" {
                format!("{agg_column}_count")
            } else {
                format!("{agg_type}_{agg_column}")
            };
            let base_metric = if table_name.is_empty() {
                base_metric_name.clone()
            } else {
                format!("{table_name}.{base_metric_name}")
            };

            let (window, grain_to_date) = parse_window_parameters(&expr_body);
            let metric_name = alias.clone().unwrap_or_else(|| {
                if window.is_some() {
                    format!("rolling_{base_metric_name}")
                } else if let Some(grain) = grain_to_date.as_ref() {
                    format!("{grain}_to_date_{base_metric_name}")
                } else {
                    format!("running_{base_metric_name}")
                }
            });

            payload
                .cumulative_metrics
                .push(MigratorCumulativeMetricRecord {
                    name: metric_name,
                    base_metric,
                    table: table_name.clone(),
                    window,
                    grain_to_date,
                    agg_type: agg_type.clone(),
                    agg_column: agg_column.clone(),
                });
            payload
                .aggregations_in_cumulative
                .push((agg_type, agg_column, table_name));
            continue;
        }

        if !contains_top_level_arithmetic_operator(&expr_body) {
            continue;
        }

        let metric_name = alias
            .clone()
            .unwrap_or_else(|| format!("derived_metric_{}", payload.derived_metrics.len() + 1));
        let table_name = infer_table_from_expression(&expr_body);
        payload.derived_metrics.push(MigratorDerivedMetricRecord {
            name: metric_name,
            sql_expression: expr_body.clone(),
            table: table_name,
        });

        for (agg_type, agg_arg) in agg_calls {
            payload
                .aggregations_in_derived
                .push(normalize_aggregation_tuple(&agg_type, &agg_arg));
        }
    }

    payload.group_by_columns = payload
        .group_by_columns
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    payload.aggregations_in_derived = payload
        .aggregations_in_derived
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    payload.aggregations_in_cumulative = payload
        .aggregations_in_cumulative
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    serialize_json_payload(
        &serde_json::to_value(payload).unwrap_or_else(|_| serde_json::json!({})),
        "migrator analysis",
    )
}

/// Auto-detect chart x/y columns from ordered column names and numeric flags.
pub fn chart_auto_detect_columns(
    columns: &[String],
    numeric_flags: &[bool],
) -> Result<(String, Vec<String>)> {
    if columns.is_empty() {
        return Err(SidemanticError::Validation(
            "Cannot auto-detect columns from empty data".to_string(),
        ));
    }

    if columns.len() == 1 {
        return Ok(("index".to_string(), vec![columns[0].clone()]));
    }

    if numeric_flags.len() != columns.len() - 1 {
        return Err(SidemanticError::Validation(format!(
            "chart numeric flag count mismatch: expected {}, got {}",
            columns.len() - 1,
            numeric_flags.len()
        )));
    }

    let mut y_cols: Vec<String> = columns
        .iter()
        .skip(1)
        .zip(numeric_flags.iter())
        .filter_map(|(column, is_numeric)| {
            if *is_numeric {
                Some(column.clone())
            } else {
                None
            }
        })
        .collect();

    if y_cols.is_empty() {
        y_cols = columns.iter().skip(1).cloned().collect();
    }

    Ok((columns[0].clone(), y_cols))
}

/// Select chart type using Python-compatible chart heuristics.
pub fn chart_select_type(x: &str, x_value_kind: &str, y_count: usize) -> String {
    let kind = x_value_kind.to_ascii_lowercase();
    let x_lower = x.to_ascii_lowercase();
    let time_indicators = [
        "date", "time", "month", "year", "day", "week", "quarter", "created", "updated",
    ];

    if kind == "string"
        && time_indicators
            .iter()
            .any(|indicator| x_lower.contains(indicator))
    {
        return if y_count > 1 { "line" } else { "area" }.to_string();
    }

    if kind == "number" {
        return "scatter".to_string();
    }

    "bar".to_string()
}

/// Determine chart encoding type from column name.
pub fn chart_encoding_type(column: &str) -> String {
    let column_lower = column.to_ascii_lowercase();
    let time_indicators = [
        "date",
        "time",
        "month",
        "year",
        "day",
        "week",
        "quarter",
        "created",
        "updated",
        "timestamp",
    ];
    if time_indicators
        .iter()
        .any(|indicator| column_lower.contains(indicator))
    {
        return "temporal".to_string();
    }
    "nominal".to_string()
}

fn chart_capitalize_word(word: &str) -> String {
    let mut chars = word.chars();
    let Some(first) = chars.next() else {
        return String::new();
    };
    let mut result = String::new();
    result.push_str(&first.to_uppercase().collect::<String>());
    result.push_str(chars.as_str().to_lowercase().as_str());
    result
}

fn parse_symmetric_agg_type(agg_type: &str) -> Result<SymmetricAggType> {
    match agg_type.to_ascii_lowercase().as_str() {
        "sum" => Ok(SymmetricAggType::Sum),
        "avg" => Ok(SymmetricAggType::Avg),
        "count" => Ok(SymmetricAggType::Count),
        "count_distinct" => Ok(SymmetricAggType::CountDistinct),
        "min" => Ok(SymmetricAggType::Min),
        "max" => Ok(SymmetricAggType::Max),
        "median" => Err(SidemanticError::Validation(
            "Symmetric aggregates do not support MEDIAN. Use pre-aggregation or restructure the query to avoid fan-out joins.".to_string(),
        )),
        _ => Err(SidemanticError::Validation(format!(
            "unsupported aggregation type for symmetric aggregates: {agg_type}"
        ))),
    }
}

fn parse_symmetric_sql_dialect(dialect: &str) -> Result<SqlDialect> {
    SqlDialect::parse(dialect)
        .ok_or_else(|| SidemanticError::Validation(format!("unsupported SQL dialect: {dialect}")))
}

/// Build SQL for symmetric aggregation with Python-compatible argument semantics.
pub fn build_symmetric_aggregate_sql(
    measure_expr: &str,
    primary_key: &str,
    agg_type: &str,
    model_alias: Option<&str>,
    dialect: &str,
) -> Result<String> {
    let agg_type = parse_symmetric_agg_type(agg_type)?;
    let dialect = parse_symmetric_sql_dialect(dialect)?;
    Ok(build_symmetric_aggregate_sql_core(
        measure_expr,
        primary_key,
        agg_type,
        model_alias,
        dialect,
    ))
}

/// Determine whether symmetric aggregate semantics are required.
pub fn needs_symmetric_aggregate(relationship: &str, is_base_model: bool) -> bool {
    needs_symmetric_aggregate_core(relationship, is_base_model)
}

/// Format chart axis/legend labels using Python-compatible naming rules.
pub fn chart_format_label(column: &str) -> String {
    if let Some((base, granularity)) = column.rsplit_once("__") {
        return format!(
            "{} ({})",
            chart_format_label(base),
            chart_capitalize_word(granularity)
        );
    }

    let field = column.rsplit('.').next().unwrap_or(column);
    field
        .replace('_', " ")
        .split_whitespace()
        .map(chart_capitalize_word)
        .collect::<Vec<_>>()
        .join(" ")
}

/// Validate model payload shape.
pub fn validate_model_payload(model_yaml: &str) -> Result<bool> {
    let _: crate::config::ModelConfig = serde_yaml::from_str(model_yaml)
        .map_err(|e| SidemanticError::Validation(format!("failed to parse model payload: {e}")))?;
    Ok(true)
}

/// Validate metric payload shape.
pub fn validate_metric_payload(metric_yaml: &str) -> Result<bool> {
    let value: serde_yaml::Value = serde_yaml::from_str(metric_yaml)
        .map_err(|e| SidemanticError::Validation(format!("failed to parse metric payload: {e}")))?;
    let mapping = value.as_mapping().ok_or_else(|| {
        SidemanticError::Validation("metric payload must be a YAML mapping".to_string())
    })?;

    let metric_type = payload_optional_string(mapping, "type");
    if let Some(metric_type) = metric_type.as_deref() {
        match metric_type {
            "ratio" => {
                if !payload_has_non_empty_string(mapping, "numerator") {
                    return Err(SidemanticError::Validation(
                        "ratio metric requires 'numerator' field".to_string(),
                    ));
                }
                if !payload_has_non_empty_string(mapping, "denominator") {
                    return Err(SidemanticError::Validation(
                        "ratio metric requires 'denominator' field".to_string(),
                    ));
                }
            }
            "derived" => {
                if !payload_has_non_empty_string(mapping, "sql") {
                    return Err(SidemanticError::Validation(
                        "derived metric requires 'sql' field".to_string(),
                    ));
                }
            }
            "cumulative" => {
                let has_sql = payload_has_non_empty_string(mapping, "sql");
                let has_window_expression =
                    payload_has_non_empty_string(mapping, "window_expression");
                if !has_sql && !has_window_expression {
                    return Err(SidemanticError::Validation(
                        "cumulative metric requires 'sql' or 'window_expression' field".to_string(),
                    ));
                }
            }
            "time_comparison" => {
                if !payload_has_non_empty_string(mapping, "base_metric") {
                    return Err(SidemanticError::Validation(
                        "time_comparison metric requires 'base_metric' field".to_string(),
                    ));
                }
            }
            "conversion" => {
                if !payload_has_non_empty_string(mapping, "entity") {
                    return Err(SidemanticError::Validation(
                        "conversion metric requires 'entity' field".to_string(),
                    ));
                }
                if !payload_has_non_empty_string(mapping, "base_event") {
                    return Err(SidemanticError::Validation(
                        "conversion metric requires 'base_event' field".to_string(),
                    ));
                }
                if !payload_has_non_empty_string(mapping, "conversion_event") {
                    return Err(SidemanticError::Validation(
                        "conversion metric requires 'conversion_event' field".to_string(),
                    ));
                }
            }
            _ => {
                return Err(SidemanticError::Validation(format!(
                    "unsupported metric type in rust validator: {metric_type}"
                )));
            }
        }
    }

    if let Some(agg) = payload_optional_string(mapping, "agg") {
        let valid = matches!(
            agg.as_str(),
            "sum" | "count" | "count_distinct" | "avg" | "min" | "max" | "median"
        );
        if !valid {
            return Err(SidemanticError::Validation(format!(
                "invalid metric aggregation '{agg}'"
            )));
        }
    }

    Ok(true)
}

/// Validate parameter payload shape.
pub fn validate_parameter_payload(parameter_yaml: &str) -> Result<bool> {
    let _: crate::core::Parameter = serde_yaml::from_str(parameter_yaml).map_err(|e| {
        SidemanticError::Validation(format!("failed to parse parameter payload: {e}"))
    })?;
    Ok(true)
}

/// Validate table-calculation payload shape.
pub fn validate_table_calculation_payload(calculation_yaml: &str) -> Result<bool> {
    let value: serde_yaml::Value = serde_yaml::from_str(calculation_yaml).map_err(|e| {
        SidemanticError::Validation(format!("failed to parse table calculation payload: {e}"))
    })?;
    let mapping = value.as_mapping().ok_or_else(|| {
        SidemanticError::Validation("table calculation payload must be a YAML mapping".to_string())
    })?;

    if !payload_has_non_empty_string(mapping, "name") {
        return Err(SidemanticError::Validation(
            "table calculation requires non-empty 'name' field".to_string(),
        ));
    }

    let calc_type = payload_optional_string(mapping, "type").ok_or_else(|| {
        SidemanticError::Validation("table calculation requires 'type' field".to_string())
    })?;
    let valid = matches!(
        calc_type.as_str(),
        "formula"
            | "percent_of_total"
            | "percent_of_previous"
            | "percent_of_column_total"
            | "running_total"
            | "rank"
            | "row_number"
            | "percentile"
            | "moving_average"
    );
    if !valid {
        return Err(SidemanticError::Validation(format!(
            "unsupported table calculation type '{calc_type}'"
        )));
    }

    Ok(true)
}

/// Build SQL statements for a pre-aggregation refresh operation.
fn contains_scalar_subquery_in_select_projection(sql: &str) -> bool {
    let Some(select_idx) = find_top_level_keyword(sql, "select") else {
        return false;
    };
    let projection_start = select_idx + "select".len();
    let remaining = &sql[projection_start..];
    let from_idx = find_top_level_keyword(remaining, "from")
        .map(|offset| projection_start + offset)
        .unwrap_or(sql.len());
    let projection = &sql[projection_start..from_idx];
    Regex::new(r"(?is)\(\s*select\b")
        .expect("valid scalar-subquery detection regex")
        .is_match(projection)
}

fn first_unsupported_bigquery_join(sql: &str) -> Option<String> {
    let regex = Regex::new(r"(?is)\b(left\s+semi|left\s+anti|right\s+semi|right\s+anti|cross\s+apply|outer\s+apply|natural\s+left|natural\s+right|natural\s+full|left\s+lateral|asof\s+left|asof\s+right|left\s+array|cross|natural|semi|anti|lateral|asof|straight|array|outer)\s+join\b")
        .expect("valid unsupported-bigquery-join regex");
    regex.captures(sql).and_then(|caps| {
        caps.get(1).map(|join_type| {
            join_type
                .as_str()
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
                .to_ascii_uppercase()
        })
    })
}

fn parse_engine_sql_with_large_stack(
    source_sql: &str,
    dialect: DialectType,
) -> std::result::Result<(), String> {
    #[cfg(target_arch = "wasm32")]
    {
        return polyglot_parse_one(source_sql, dialect)
            .map(|_| ())
            .map_err(|err| err.to_string());
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        let sql = source_sql.to_string();
        let parser_thread = std::thread::Builder::new()
            .stack_size(16 * 1024 * 1024)
            .spawn(move || {
                polyglot_parse_one(&sql, dialect)
                    .map(|_| ())
                    .map_err(|err| err.to_string())
            })
            .map_err(|err| err.to_string())?;

        parser_thread
            .join()
            .map_err(|_| "failed to join SQL parser thread".to_string())?
    }
}

/// Validate SQL compatibility with engine refresh materialized-view restrictions.
///
/// Mirrors Python-side pre-aggregation validation behavior so the Rust path can run
/// independently of Python helpers.
pub fn validate_engine_refresh_sql_compatibility(
    source_sql: &str,
    dialect: &str,
) -> (bool, Option<String>) {
    let dialect_name = dialect.trim().to_ascii_lowercase();
    let dialect_type = match dialect_name.parse::<DialectType>() {
        Ok(kind) => kind,
        Err(err) => {
            return (false, Some(format!("Failed to parse SQL: {err}")));
        }
    };

    if let Err(err) = parse_engine_sql_with_large_stack(source_sql, dialect_type) {
        return (false, Some(format!("Failed to parse SQL: {err}")));
    }

    if Regex::new(r"(?is)\bover\s*\(")
        .expect("valid window-function detection regex")
        .is_match(source_sql)
    {
        return (
            false,
            Some("Window functions not supported in materialized views".to_string()),
        );
    }

    if dialect_name == "snowflake" && contains_scalar_subquery_in_select_projection(source_sql) {
        return (
            false,
            Some("Scalar subqueries not fully supported in Snowflake DYNAMIC TABLES".to_string()),
        );
    }

    if dialect_name == "bigquery" {
        if let Some(join_type) = first_unsupported_bigquery_join(source_sql) {
            return (
                false,
                Some(format!(
                    "BigQuery materialized views don't support {join_type} joins"
                )),
            );
        }
    }

    (true, None)
}

/// Resolve refresh mode using Python-compatible defaults and validation.
pub fn resolve_preaggregation_refresh_mode(
    mode: Option<&str>,
    refresh_incremental: bool,
) -> Result<String> {
    let resolved_mode = mode.unwrap_or(if refresh_incremental {
        "incremental"
    } else {
        "full"
    });
    if !matches!(resolved_mode, "full" | "incremental" | "merge" | "engine") {
        return Err(SidemanticError::Validation(format!(
            "Invalid refresh mode: {resolved_mode}"
        )));
    }
    Ok(resolved_mode.to_string())
}

/// Validate mode-specific refresh requirements using Python-compatible errors.
pub fn validate_preaggregation_refresh_request(
    mode: &str,
    watermark_column: Option<&str>,
    dialect: Option<&str>,
) -> Result<()> {
    match mode {
        "full" => Ok(()),
        "incremental" => {
            if watermark_column.is_none() {
                return Err(SidemanticError::Validation(
                    "watermark_column required for incremental refresh".to_string(),
                ));
            }
            Ok(())
        }
        "merge" => {
            if watermark_column.is_none() {
                return Err(SidemanticError::Validation(
                    "watermark_column required for merge refresh".to_string(),
                ));
            }
            Ok(())
        }
        "engine" => {
            if dialect.is_none() {
                return Err(SidemanticError::Validation(
                    "dialect required for engine refresh mode".to_string(),
                ));
            }
            Ok(())
        }
        _ => Err(SidemanticError::Validation(format!(
            "Invalid refresh mode: {mode}"
        ))),
    }
}

/// Runtime-owned refresh execution planning for Python/CLI orchestration layers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreaggregationRefreshExecutionPlan {
    pub mode: String,
    pub requires_prior_watermark: bool,
    pub requires_merge_table_existence_check: bool,
    pub include_new_watermark: bool,
}

/// Plan pre-aggregation refresh execution mode and branch requirements.
pub fn plan_preaggregation_refresh_execution(
    mode: Option<&str>,
    refresh_incremental: bool,
    watermark_column: Option<&str>,
    dialect: Option<&str>,
) -> Result<PreaggregationRefreshExecutionPlan> {
    let resolved_mode = resolve_preaggregation_refresh_mode(mode, refresh_incremental)?;
    validate_preaggregation_refresh_request(&resolved_mode, watermark_column, dialect)?;
    let (requires_prior_watermark, requires_merge_table_existence_check, include_new_watermark) =
        match resolved_mode.as_str() {
            "full" => (false, false, false),
            "incremental" => (true, false, true),
            "merge" => (true, true, true),
            "engine" => (false, false, false),
            _ => unreachable!(),
        };

    Ok(PreaggregationRefreshExecutionPlan {
        mode: resolved_mode,
        requires_prior_watermark,
        requires_merge_table_existence_check,
        include_new_watermark,
    })
}

/// Runtime-owned refresh result shape for Python/CLI orchestration layers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreaggregationRefreshResultShape {
    pub mode: String,
    pub rows_inserted: i64,
    pub rows_updated: i64,
    pub include_new_watermark: bool,
}

/// Resolve pre-aggregation refresh result semantics for all refresh modes.
pub fn shape_preaggregation_refresh_result(
    mode: &str,
    merge_target_table_existed: bool,
    full_rows_inserted: i64,
) -> Result<PreaggregationRefreshResultShape> {
    match mode {
        "full" => Ok(PreaggregationRefreshResultShape {
            mode: "full".to_string(),
            rows_inserted: full_rows_inserted,
            rows_updated: 0,
            include_new_watermark: false,
        }),
        "incremental" => Ok(PreaggregationRefreshResultShape {
            mode: "incremental".to_string(),
            rows_inserted: -1,
            rows_updated: 0,
            include_new_watermark: true,
        }),
        "merge" => Ok(PreaggregationRefreshResultShape {
            mode: "merge".to_string(),
            rows_inserted: -1,
            rows_updated: if merge_target_table_existed { -1 } else { 0 },
            include_new_watermark: true,
        }),
        "engine" => Ok(PreaggregationRefreshResultShape {
            mode: "engine".to_string(),
            rows_inserted: -1,
            rows_updated: -1,
            include_new_watermark: false,
        }),
        _ => Err(SidemanticError::Validation(format!(
            "Invalid refresh mode: {mode}"
        ))),
    }
}

#[allow(clippy::too_many_arguments)]
pub fn build_preaggregation_refresh_statements(
    mode: &str,
    table_name: &str,
    source_sql: &str,
    watermark_column: Option<&str>,
    from_watermark: Option<&str>,
    lookback: Option<&str>,
    dialect: Option<&str>,
    refresh_every: Option<&str>,
) -> Result<Vec<String>> {
    match mode {
        "full" => {
            let create_if_missing = format!(
                "CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM ({source_sql}) AS sidemantic_preagg_seed WHERE 1 = 0"
            );
            Ok(vec![
                create_if_missing,
                format!("DELETE FROM {table_name}"),
                format!("INSERT INTO {table_name} {source_sql}"),
            ])
        }
        "incremental" => {
            let watermark_column = watermark_column.ok_or_else(|| {
                SidemanticError::Validation(
                    "incremental refresh requires --watermark-column or pre-aggregation time_dimension+granularity"
                        .to_string(),
                )
            })?;
            let watermark = watermark_threshold_expression(from_watermark, lookback);
            let refresh_sql =
                resolve_incremental_refresh_source_sql(source_sql, watermark_column, &watermark);
            let create_if_missing = format!(
                "CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM ({refresh_sql}) AS sidemantic_preagg_seed WHERE 1 = 0"
            );
            Ok(vec![
                create_if_missing,
                format!("INSERT INTO {table_name} {refresh_sql}"),
            ])
        }
        "merge" => {
            let watermark_column = watermark_column.ok_or_else(|| {
                SidemanticError::Validation(
                    "merge refresh requires --watermark-column or pre-aggregation time_dimension+granularity"
                        .to_string(),
                )
            })?;
            let watermark = watermark_threshold_expression(from_watermark, lookback);
            let refresh_sql =
                resolve_incremental_refresh_source_sql(source_sql, watermark_column, &watermark);
            let create_if_missing = format!(
                "CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM ({refresh_sql}) AS sidemantic_preagg_seed WHERE 1 = 0"
            );
            let temp_table = format!("{table_name}__refresh_tmp");
            Ok(vec![
                create_if_missing,
                format!("DROP TABLE IF EXISTS {temp_table}"),
                format!("CREATE TABLE {temp_table} AS {refresh_sql}"),
                format!("DELETE FROM {table_name} WHERE {watermark_column} >= {watermark}"),
                format!("INSERT INTO {table_name} SELECT * FROM {temp_table}"),
                format!("DROP TABLE IF EXISTS {temp_table}"),
            ])
        }
        "engine" => {
            let dialect = dialect.ok_or_else(|| {
                SidemanticError::Validation(
                    "engine refresh mode requires --dialect (snowflake|clickhouse|bigquery)"
                        .to_string(),
                )
            })?;
            let (is_valid, error_message) =
                validate_engine_refresh_sql_compatibility(source_sql, dialect);
            if !is_valid {
                let details =
                    error_message.unwrap_or_else(|| "unknown validation failure".to_string());
                return Err(SidemanticError::Validation(format!(
                    "SQL not compatible with {dialect} materialized views: {details}"
                )));
            }
            let ddl = build_engine_refresh_ddl(table_name, source_sql, dialect, refresh_every)?;
            Ok(vec![ddl])
        }
        _ => Err(SidemanticError::Validation(format!(
            "unsupported refresh mode '{mode}'"
        ))),
    }
}

fn resolve_incremental_refresh_source_sql(
    source_sql: &str,
    watermark_column: &str,
    watermark: &str,
) -> String {
    if source_sql.contains("{WATERMARK}") {
        source_sql.replace("{WATERMARK}", watermark)
    } else {
        format!(
            "SELECT * FROM ({source_sql}) AS sidemantic_preagg_source WHERE {watermark_column} >= {watermark}"
        )
    }
}

fn build_engine_refresh_ddl(
    table_name: &str,
    source_sql: &str,
    dialect: &str,
    refresh_every: Option<&str>,
) -> Result<String> {
    match dialect {
        "snowflake" => {
            let refresh_interval = refresh_every
                .unwrap_or("1 HOUR")
                .to_uppercase()
                .replace('\'', "''");
            Ok(format!(
                "CREATE OR REPLACE DYNAMIC TABLE {table_name}\nTARGET_LAG = '{refresh_interval}'\nWAREHOUSE = 'COMPUTE_WH'\nAS\n{source_sql}"
            ))
        }
        "clickhouse" => {
            let target_table = format!("{table_name}_data");
            Ok(format!(
                "CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name}\nTO {target_table}\nAS\n{source_sql}"
            ))
        }
        "bigquery" => {
            let refresh_interval_minutes = parse_refresh_interval_minutes(refresh_every)?;
            Ok(format!(
                "CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name}\nOPTIONS(\n  enable_refresh = true,\n  refresh_interval_minutes = {refresh_interval_minutes}\n)\nAS\n{source_sql}"
            ))
        }
        other => Err(SidemanticError::Validation(format!(
            "unsupported dialect for engine refresh mode: {other}. Supported dialects: snowflake, clickhouse, bigquery"
        ))),
    }
}

fn parse_refresh_interval_minutes(refresh_every: Option<&str>) -> Result<i64> {
    let Some(value) = refresh_every else {
        return Ok(60);
    };
    let parts: Vec<&str> = value.split_whitespace().collect();
    if parts.len() != 2 {
        return Err(SidemanticError::Validation(format!(
            "invalid --refresh-every value '{value}'. Expected '<number> <unit>', e.g. '1 hour'"
        )));
    }

    let amount = parts[0].parse::<i64>().map_err(|_| {
        SidemanticError::Validation(format!("invalid refresh interval value: {}", parts[0]))
    })?;
    if amount < 0 {
        return Err(SidemanticError::Validation(
            "refresh interval value cannot be negative".to_string(),
        ));
    }

    let unit = parts[1].to_ascii_lowercase();
    if unit.contains("minute") {
        Ok(amount)
    } else if unit.contains("hour") {
        Ok(amount * 60)
    } else if unit.contains("day") {
        Ok(amount * 60 * 24)
    } else {
        Err(SidemanticError::Validation(format!(
            "unsupported refresh interval unit '{unit}'. Use minute(s), hour(s), or day(s)"
        )))
    }
}

fn watermark_threshold_expression(from_watermark: Option<&str>, lookback: Option<&str>) -> String {
    let base = from_watermark
        .map(quote_sql_literal)
        .unwrap_or_else(|| "'1970-01-01'".to_string());
    match lookback {
        Some(lookback_interval) => format!(
            "(CAST({base} AS TIMESTAMP) - INTERVAL '{}')",
            lookback_interval.replace('\'', "''")
        ),
        None => base,
    }
}

fn quote_sql_literal(value: &str) -> String {
    let escaped = value.replace('\'', "''");
    format!("'{escaped}'")
}

/// Compute recommender benefit score for a serialized pattern payload.
pub fn calculate_preaggregation_benefit_score(pattern_json: &str, count: usize) -> Result<f64> {
    let pattern = parse_pattern_payload(pattern_json, "benefit score calculation")?;
    Ok(benefit_score(&pattern, count))
}

/// Generate recommender name for a serialized pattern payload.
pub fn generate_preaggregation_name(pattern_json: &str) -> Result<String> {
    let pattern = parse_pattern_payload(pattern_json, "name generation")?;
    Ok(pattern_name(&pattern))
}

/// Parse instrumented query comments into pre-aggregation pattern counts.
pub fn extract_preaggregation_patterns(queries: Vec<String>) -> Result<String> {
    let instrumentation_re = Regex::new(r"--\s*sidemantic:\s*(.+)").map_err(|e| {
        SidemanticError::SqlGeneration(format!("invalid instrumentation regex: {e}"))
    })?;
    let mut counts: HashMap<PatternKey, usize> = HashMap::new();

    for query in &queries {
        if let Some(pattern) = extract_pattern_key(query, &instrumentation_re) {
            *counts.entry(pattern).or_insert(0) += 1;
        }
    }

    let mut records: Vec<PreaggPatternRecord> = counts
        .into_iter()
        .map(|(pattern, count)| PreaggPatternRecord {
            model: pattern.model,
            metrics: pattern.metrics,
            dimensions: pattern.dimensions,
            granularities: pattern.granularities,
            count,
        })
        .collect();
    records.sort_by(|left, right| {
        left.model
            .cmp(&right.model)
            .then_with(|| left.metrics.cmp(&right.metrics))
            .then_with(|| left.dimensions.cmp(&right.dimensions))
            .then_with(|| left.granularities.cmp(&right.granularities))
    });

    serde_json::to_string(&records).map_err(|e| {
        SidemanticError::SqlGeneration(format!("failed to serialize pattern records: {e}"))
    })
}

/// Build pre-aggregation recommendations from known pattern counts.
pub fn recommend_preaggregation_patterns(
    patterns_json: &str,
    min_query_count: usize,
    min_benefit_score: f64,
    top_n: Option<usize>,
) -> Result<String> {
    let patterns = parse_patterns_payload(patterns_json, "recommendations")?;

    let mut recommendations: Vec<PreaggRecommendationRecord> = patterns
        .into_iter()
        .filter(|pattern| pattern.count >= min_query_count)
        .filter_map(|pattern| {
            let score = benefit_score(&pattern, pattern.count);
            if score < min_benefit_score {
                return None;
            }

            Some(PreaggRecommendationRecord {
                suggested_name: pattern_name(&pattern),
                query_count: pattern.count,
                estimated_benefit_score: score,
                pattern,
            })
        })
        .collect();

    recommendations.sort_by(|left, right| {
        right
            .estimated_benefit_score
            .partial_cmp(&left.estimated_benefit_score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| right.query_count.cmp(&left.query_count))
            .then_with(|| left.suggested_name.cmp(&right.suggested_name))
    });

    if let Some(limit) = top_n {
        recommendations.truncate(limit);
    }

    serde_json::to_string(&recommendations).map_err(|e| {
        SidemanticError::SqlGeneration(format!(
            "failed to serialize recommendation payload from patterns: {e}"
        ))
    })
}

/// Summarize pattern counts for recommendation reporting.
pub fn summarize_preaggregation_patterns(
    patterns_json: &str,
    min_query_count: usize,
) -> Result<String> {
    let patterns = parse_patterns_payload(patterns_json, "summary")?;

    let total_queries = patterns.iter().map(|pattern| pattern.count).sum::<usize>();
    let unique_patterns = patterns.len();
    let patterns_above_threshold = patterns
        .iter()
        .filter(|pattern| pattern.count >= min_query_count)
        .count();

    let mut model_counts: HashMap<String, usize> = HashMap::new();
    for pattern in patterns {
        *model_counts.entry(pattern.model).or_insert(0) += pattern.count;
    }

    let payload = serde_json::json!({
        "total_queries": total_queries,
        "unique_patterns": unique_patterns,
        "models": model_counts,
        "patterns_above_threshold": patterns_above_threshold,
    });

    serialize_json_payload(&payload, "summary")
}

/// Convert a recommendation payload into a pre-aggregation definition payload.
pub fn generate_preaggregation_definition(recommendation_json: &str) -> Result<String> {
    let recommendation: PreaggRecommendationRecord = serde_json::from_str(recommendation_json)
        .map_err(|e| {
            SidemanticError::Validation(format!(
                "failed to parse recommendation payload for definition generation: {e}"
            ))
        })?;
    let pattern = recommendation.pattern;

    let measures: Vec<String> = pattern
        .metrics
        .iter()
        .map(|metric| strip_model_prefix(metric))
        .collect();
    let dimensions: Vec<String> = pattern
        .dimensions
        .iter()
        .map(|dimension| strip_model_prefix(dimension))
        .collect();

    let mut time_dimension: Option<String> = None;
    let mut granularity: Option<String> = None;

    if !pattern.granularities.is_empty() {
        let time_candidates = [
            "created_at",
            "updated_at",
            "date",
            "timestamp",
            "time",
            "datetime",
        ];
        for dimension in &dimensions {
            if time_candidates
                .iter()
                .any(|candidate| dimension.contains(candidate))
            {
                time_dimension = Some(dimension.clone());
                break;
            }
        }

        if time_dimension.is_none() && !dimensions.is_empty() {
            time_dimension = dimensions.first().cloned();
        }

        if time_dimension.is_some() {
            let mut granularities = pattern.granularities.clone();
            granularities.sort_by_key(|value| granularity_rank(value));
            granularity = granularities.first().cloned();
        }
    }

    let definition_dimensions: Vec<String> = if let Some(time_dimension_name) = &time_dimension {
        dimensions
            .into_iter()
            .filter(|dimension| dimension != time_dimension_name)
            .collect()
    } else {
        dimensions
    };

    let payload = serde_json::json!({
        "name": recommendation.suggested_name,
        "type": "rollup",
        "measures": measures,
        "dimensions": definition_dimensions,
        "time_dimension": time_dimension,
        "granularity": granularity,
    });

    serialize_json_payload(&payload, "pre-aggregation definition")
}

/// Parse a top-level simple metric aggregation expression.
///
/// Returns `(agg, inner_sql)` for simple aggregations like:
/// - `SUM(amount)` -> `("sum", Some("amount"))`
/// - `COUNT(*)` -> `("count", Some("*"))`
/// - `COUNT(DISTINCT user_id)` -> `("count_distinct", Some("user_id"))`
///
/// Returns `None` for non-simple/complex expressions.
pub fn parse_simple_metric_aggregation(sql_expr: &str) -> Option<(String, Option<String>)> {
    let trimmed = sql_expr.trim();
    if trimmed.is_empty() {
        return None;
    }

    let open_paren = trimmed.find('(')?;
    let func = trimmed[..open_paren].trim().to_lowercase();
    if !matches!(
        func.as_str(),
        "sum" | "avg" | "min" | "max" | "median" | "count"
    ) {
        return None;
    }

    let mut depth = 0i32;
    let mut close_paren: Option<usize> = None;
    for (idx, ch) in trimmed.char_indices().skip(open_paren) {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    close_paren = Some(idx);
                    break;
                }
                if depth < 0 {
                    return None;
                }
            }
            _ => {}
        }
    }

    let close_paren = close_paren?;
    if depth != 0 {
        return None;
    }

    if !trimmed[close_paren + 1..].trim().is_empty() {
        return None;
    }

    let inner = trimmed[open_paren + 1..close_paren].trim();

    match func.as_str() {
        "sum" | "avg" | "min" | "max" | "median" => {
            if inner.is_empty() {
                None
            } else {
                Some((func, Some(inner.to_string())))
            }
        }
        "count" => {
            if inner == "*" {
                return Some(("count".to_string(), Some("*".to_string())));
            }

            let distinct_pattern = Regex::new(r"(?is)^distinct\s+(.+)$").ok()?;
            if let Some(distinct) = distinct_pattern.captures(inner) {
                let distinct_expr = distinct.get(1)?.as_str().trim();
                if distinct_expr.is_empty() {
                    None
                } else {
                    Some((
                        "count_distinct".to_string(),
                        Some(distinct_expr.to_string()),
                    ))
                }
            } else if inner.is_empty() {
                None
            } else {
                Some(("count".to_string(), Some(inner.to_string())))
            }
        }
        _ => None,
    }
}

/// Convert a metric payload to SQL aggregation expression.
pub fn metric_to_sql(metric_yaml: &str) -> Result<String> {
    let metric = parse_metric_helper_payload(metric_yaml)?;

    if metric.agg.is_none() {
        return Err(SidemanticError::Validation(format!(
            "Cannot convert complex metric '{}' to SQL - use type-specific logic",
            metric.name
        )));
    }

    Ok(metric.to_sql(None))
}

/// Resolve metric SQL expression with Python-compatible count fallback.
pub fn metric_sql_expr(metric_yaml: &str) -> Result<String> {
    let metric = parse_metric_helper_payload(metric_yaml)?;

    if metric.agg == Some(Aggregation::Count) && metric.sql.is_none() {
        return Ok("*".to_string());
    }

    Ok(metric.sql.unwrap_or(metric.name))
}

/// Determine whether metric payload represents a simple aggregation.
pub fn metric_is_simple_aggregation(metric_yaml: &str) -> Result<bool> {
    let metric = parse_metric_helper_payload(metric_yaml)?;
    Ok(metric.is_simple_aggregation())
}

struct ArithmeticParser<'a> {
    chars: Vec<char>,
    pos: usize,
    _marker: std::marker::PhantomData<&'a str>,
}

impl<'a> ArithmeticParser<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            chars: input.chars().collect(),
            pos: 0,
            _marker: std::marker::PhantomData,
        }
    }

    fn parse(mut self) -> std::result::Result<f64, String> {
        let value = self.parse_additive()?;
        self.skip_whitespace();
        if self.pos != self.chars.len() {
            return Err(format!(
                "unexpected token '{}'",
                self.chars.get(self.pos).copied().unwrap_or('?')
            ));
        }
        Ok(value)
    }

    fn parse_additive(&mut self) -> std::result::Result<f64, String> {
        let mut value = self.parse_multiplicative()?;
        loop {
            self.skip_whitespace();
            if self.consume("+") {
                let rhs = self.parse_multiplicative()?;
                value += rhs;
            } else if self.consume("-") {
                let rhs = self.parse_multiplicative()?;
                value -= rhs;
            } else {
                break;
            }
        }
        Ok(value)
    }

    fn parse_multiplicative(&mut self) -> std::result::Result<f64, String> {
        let mut value = self.parse_power()?;
        loop {
            self.skip_whitespace();
            if self.consume("//") {
                let rhs = self.parse_power()?;
                if rhs == 0.0 {
                    return Err("division by zero".to_string());
                }
                value = (value / rhs).floor();
            } else if self.consume("*") {
                let rhs = self.parse_power()?;
                value *= rhs;
            } else if self.consume("/") {
                let rhs = self.parse_power()?;
                if rhs == 0.0 {
                    return Err("division by zero".to_string());
                }
                value /= rhs;
            } else if self.consume("%") {
                let rhs = self.parse_power()?;
                if rhs == 0.0 {
                    return Err("modulo by zero".to_string());
                }
                value %= rhs;
            } else {
                break;
            }
        }
        Ok(value)
    }

    fn parse_power(&mut self) -> std::result::Result<f64, String> {
        let value = self.parse_unary()?;
        self.skip_whitespace();
        if self.consume("**") {
            let rhs = self.parse_power()?;
            Ok(value.powf(rhs))
        } else {
            Ok(value)
        }
    }

    fn parse_unary(&mut self) -> std::result::Result<f64, String> {
        self.skip_whitespace();
        if self.consume("+") {
            self.parse_unary()
        } else if self.consume("-") {
            Ok(-self.parse_unary()?)
        } else {
            self.parse_primary()
        }
    }

    fn parse_primary(&mut self) -> std::result::Result<f64, String> {
        self.skip_whitespace();
        if self.consume("(") {
            let value = self.parse_additive()?;
            self.skip_whitespace();
            if !self.consume(")") {
                return Err("missing ')'".to_string());
            }
            return Ok(value);
        }

        self.parse_number()
    }

    fn parse_number(&mut self) -> std::result::Result<f64, String> {
        self.skip_whitespace();
        let start = self.pos;
        let mut saw_digit = false;
        let mut saw_dot = false;

        while let Some(ch) = self.peek_char() {
            if ch.is_ascii_digit() {
                saw_digit = true;
                self.pos += 1;
            } else if ch == '.' && !saw_dot {
                saw_dot = true;
                self.pos += 1;
            } else {
                break;
            }
        }

        if !saw_digit {
            return Err(if self.pos < self.chars.len() {
                format!("unexpected token '{}'", self.chars[self.pos])
            } else {
                "unexpected end of expression".to_string()
            });
        }

        let token: String = self.chars[start..self.pos].iter().collect();
        token
            .parse::<f64>()
            .map_err(|_| format!("invalid number '{token}'"))
    }

    fn skip_whitespace(&mut self) {
        while matches!(self.peek_char(), Some(c) if c.is_whitespace()) {
            self.pos += 1;
        }
    }

    fn consume(&mut self, token: &str) -> bool {
        let token_chars: Vec<char> = token.chars().collect();
        if self.pos + token_chars.len() > self.chars.len() {
            return false;
        }

        if self.chars[self.pos..self.pos + token_chars.len()] == token_chars[..] {
            self.pos += token_chars.len();
            true
        } else {
            false
        }
    }

    fn peek_char(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }
}

fn evaluate_arithmetic_expression(expr: &str) -> std::result::Result<f64, String> {
    ArithmeticParser::new(expr).parse()
}

/// Evaluate a table-calculation arithmetic expression.
pub fn evaluate_table_calculation_expression(expr: &str) -> Result<f64> {
    evaluate_arithmetic_expression(expr)
        .map_err(|_| SidemanticError::Validation(format!("Invalid expression: {expr}")))
}

/// Validate table-calculation formula syntax.
pub fn validate_table_formula_expression(expression: &str) -> Result<bool> {
    let placeholder_re = Regex::new(r"\$\{[^}]+\}").expect("valid formula placeholder regex");
    let test_expr = placeholder_re.replace_all(expression, "1").into_owned();
    evaluate_arithmetic_expression(&test_expr)
        .map(|_| true)
        .map_err(|_| {
            SidemanticError::Validation(format!(
                "Invalid formula expression syntax: {expression:?}"
            ))
        })
}

fn relative_date_trunc(granularity: &str, dialect: &str) -> String {
    if dialect.eq_ignore_ascii_case("bigquery") {
        format!("DATE_TRUNC(CURRENT_DATE, {})", granularity.to_uppercase())
    } else {
        format!("DATE_TRUNC('{granularity}', CURRENT_DATE)")
    }
}

fn parse_relative_date_expr(expr: &str, dialect: &str) -> Option<String> {
    let normalized = expr.to_lowercase();
    let normalized = normalized.trim();

    match normalized {
        "today" => return Some("CURRENT_DATE".to_string()),
        "yesterday" => return Some("CURRENT_DATE - 1".to_string()),
        "tomorrow" => return Some("CURRENT_DATE + 1".to_string()),
        "this week" => return Some(relative_date_trunc("week", dialect)),
        "last week" => {
            return Some(format!(
                "{} - INTERVAL '1 week'",
                relative_date_trunc("week", dialect)
            ));
        }
        "next week" => {
            return Some(format!(
                "{} + INTERVAL '1 week'",
                relative_date_trunc("week", dialect)
            ));
        }
        "this month" => return Some(relative_date_trunc("month", dialect)),
        "last month" => {
            return Some(format!(
                "{} - INTERVAL '1 month'",
                relative_date_trunc("month", dialect)
            ));
        }
        "next month" => {
            return Some(format!(
                "{} + INTERVAL '1 month'",
                relative_date_trunc("month", dialect)
            ));
        }
        "this quarter" => return Some(relative_date_trunc("quarter", dialect)),
        "last quarter" => {
            return Some(format!(
                "{} - INTERVAL '3 months'",
                relative_date_trunc("quarter", dialect)
            ));
        }
        "next quarter" => {
            return Some(format!(
                "{} + INTERVAL '3 months'",
                relative_date_trunc("quarter", dialect)
            ));
        }
        "this year" => return Some(relative_date_trunc("year", dialect)),
        "last year" => {
            return Some(format!(
                "{} - INTERVAL '1 year'",
                relative_date_trunc("year", dialect)
            ));
        }
        "next year" => {
            return Some(format!(
                "{} + INTERVAL '1 year'",
                relative_date_trunc("year", dialect)
            ));
        }
        _ => {}
    }

    let last_days_re = Regex::new(r"^last (\d+) day(?:s)?$").expect("valid relative date regex");
    if let Some(captures) = last_days_re.captures(normalized) {
        if let Ok(days) = captures[1].parse::<i64>() {
            return Some(format!("CURRENT_DATE - {days}"));
        }
    }

    let last_weeks_re = Regex::new(r"^last (\d+) week(?:s)?$").expect("valid relative date regex");
    if let Some(captures) = last_weeks_re.captures(normalized) {
        if let Ok(weeks) = captures[1].parse::<i64>() {
            return Some(format!("CURRENT_DATE - {}", weeks * 7));
        }
    }

    let last_months_re =
        Regex::new(r"^last (\d+) month(?:s)?$").expect("valid relative date regex");
    if let Some(captures) = last_months_re.captures(normalized) {
        if let Ok(months) = captures[1].parse::<i64>() {
            return Some(format!(
                "{} - INTERVAL '{months} months'",
                relative_date_trunc("month", dialect)
            ));
        }
    }

    let last_years_re = Regex::new(r"^last (\d+) year(?:s)?$").expect("valid relative date regex");
    if let Some(captures) = last_years_re.captures(normalized) {
        if let Ok(years) = captures[1].parse::<i64>() {
            return Some(format!(
                "{} - INTERVAL '{years} years'",
                relative_date_trunc("year", dialect)
            ));
        }
    }

    None
}

/// Parse a relative-date expression to SQL.
pub fn parse_relative_date(expr: &str, dialect: &str) -> Option<String> {
    parse_relative_date_expr(expr, dialect)
}

/// Convert a relative-date expression to a SQL range filter.
pub fn relative_date_to_range(expr: &str, column: &str, dialect: &str) -> Option<String> {
    let normalized = expr.to_lowercase();
    let normalized = normalized.trim();

    if normalized.starts_with("last ")
        && (normalized.contains("day") || normalized.contains("week"))
    {
        if let Some(sql_expr) = parse_relative_date_expr(normalized, dialect) {
            return Some(format!("{column} >= {sql_expr}"));
        }
    }

    if (normalized.contains("month")
        || normalized.contains("quarter")
        || normalized.contains("year"))
        && (normalized.starts_with("this ")
            || normalized.starts_with("last ")
            || normalized.starts_with("next "))
    {
        if let Some(start_sql) = parse_relative_date_expr(normalized, dialect) {
            let interval = if normalized.contains("month") {
                "1 month"
            } else if normalized.contains("quarter") {
                "3 months"
            } else if normalized.contains("year") {
                "1 year"
            } else if normalized.contains("week") {
                "1 week"
            } else {
                "1 day"
            };

            return Some(format!(
                "{column} >= {start_sql} AND {column} < {start_sql} + INTERVAL '{interval}'"
            ));
        }
    }

    if ["today", "yesterday", "tomorrow"].contains(&normalized) {
        if let Some(sql_expr) = parse_relative_date_expr(normalized, dialect) {
            return Some(format!("{column} = {sql_expr}"));
        }
    }

    None
}

/// Check whether an expression is a recognized relative date.
pub fn is_relative_date(expr: &str) -> bool {
    parse_relative_date_expr(expr, "duckdb").is_some()
}

fn parse_time_offset_unit(unit: &str) -> std::result::Result<&'static str, String> {
    match unit {
        "day" => Ok("day"),
        "week" => Ok("week"),
        "month" => Ok("month"),
        "quarter" => Ok("quarter"),
        "year" => Ok("year"),
        _ => Err(format!("invalid time offset unit '{unit}'")),
    }
}

fn default_time_comparison_offset(
    comparison_type: &str,
) -> std::result::Result<(i64, &'static str), String> {
    match comparison_type {
        "dod" => Ok((1, "day")),
        "wow" => Ok((1, "week")),
        "mom" => Ok((1, "month")),
        "qoq" => Ok((1, "quarter")),
        "yoy" => Ok((1, "year")),
        "prior_period" => Ok((1, "day")),
        _ => Err(format!("invalid time comparison type '{comparison_type}'")),
    }
}

fn resolve_time_comparison_offset(
    comparison_type: &str,
    offset: Option<i64>,
    offset_unit: Option<&str>,
) -> std::result::Result<(i64, String), String> {
    if let (Some(amount), Some(unit)) = (offset, offset_unit) {
        let unit = parse_time_offset_unit(unit)?;
        return Ok((amount, unit.to_string()));
    }

    let (amount, unit) = default_time_comparison_offset(comparison_type)?;
    Ok((amount, unit.to_string()))
}

/// Resolve time-comparison offset interval.
pub fn time_comparison_offset_interval(
    comparison_type: &str,
    offset: Option<i64>,
    offset_unit: Option<&str>,
) -> Result<(i64, String)> {
    resolve_time_comparison_offset(comparison_type, offset, offset_unit)
        .map_err(SidemanticError::Validation)
}

/// Render SQL INTERVAL expression for time-comparison offset.
pub fn time_comparison_sql_offset(
    comparison_type: &str,
    offset: Option<i64>,
    offset_unit: Option<&str>,
) -> Result<String> {
    let (amount, unit) = resolve_time_comparison_offset(comparison_type, offset, offset_unit)
        .map_err(SidemanticError::Validation)?;
    Ok(format!("INTERVAL '{amount} {unit}'"))
}

/// Render SQL INTERVAL expression for trailing period.
pub fn trailing_period_sql_interval(amount: i64, unit: &str) -> Result<String> {
    let unit = parse_time_offset_unit(unit).map_err(SidemanticError::Validation)?;
    Ok(format!("INTERVAL '{amount} {unit}'"))
}

/// Generate SQL expression for time-comparison calculations.
pub fn generate_time_comparison_sql(
    comparison_type: &str,
    calculation: &str,
    current_metric_sql: &str,
    time_dimension: &str,
    offset: Option<i64>,
    offset_unit: Option<&str>,
) -> Result<String> {
    let _ = resolve_time_comparison_offset(comparison_type, offset, offset_unit)
        .map_err(SidemanticError::Validation)?;

    let offset_metric_sql = format!(
        "LAG({current_metric_sql}) OVER (\n            ORDER BY {time_dimension}\n        )"
    );

    match calculation {
        "difference" => Ok(format!("({current_metric_sql} - {offset_metric_sql})")),
        "percent_change" => Ok(format!(
            "(({current_metric_sql} - {offset_metric_sql}) / NULLIF({offset_metric_sql}, 0) * 100)"
        )),
        "ratio" => Ok(format!(
            "({current_metric_sql} / NULLIF({offset_metric_sql}, 0))"
        )),
        _ => Err(SidemanticError::Validation(format!(
            "Unknown calculation type: {calculation}"
        ))),
    }
}

fn extract_top_level_metric_names(models_yaml: Option<&str>) -> HashSet<String> {
    let Some(models_yaml) = models_yaml else {
        return HashSet::new();
    };

    let Ok(value) = serde_yaml::from_str::<serde_yaml::Value>(models_yaml) else {
        return HashSet::new();
    };

    let Some(root) = value.as_mapping() else {
        return HashSet::new();
    };

    let metrics_key = serde_yaml::Value::String("metrics".to_string());
    let Some(metrics) = root
        .get(&metrics_key)
        .and_then(serde_yaml::Value::as_sequence)
    else {
        return HashSet::new();
    };

    let mut names = HashSet::new();
    for metric in metrics {
        let Some(mapping) = metric.as_mapping() else {
            continue;
        };
        let name_key = serde_yaml::Value::String("name".to_string());
        let Some(name) = mapping.get(&name_key).and_then(serde_yaml::Value::as_str) else {
            continue;
        };
        if !name.is_empty() {
            names.insert(name.to_string());
        }
    }

    names
}

fn json_object(value: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
    match value {
        serde_json::Value::Object(map) => map,
        _ => serde_json::Map::new(),
    }
}

fn catalog_dimension_data_type(dimension: &Dimension) -> &'static str {
    match dimension.r#type {
        DimensionType::Categorical => "VARCHAR",
        DimensionType::Numeric => "NUMERIC",
        DimensionType::Time => match dimension.granularity.as_deref() {
            Some("day" | "week" | "month" | "quarter" | "year") => "DATE",
            Some("hour") => "TIMESTAMP",
            _ => "TIMESTAMP",
        },
        DimensionType::Boolean => "BOOLEAN",
    }
}

fn catalog_aggregation_name(aggregation: Option<&Aggregation>) -> Option<&'static str> {
    match aggregation {
        Some(Aggregation::Count) => Some("count"),
        Some(Aggregation::CountDistinct) => Some("count_distinct"),
        Some(Aggregation::Sum) => Some("sum"),
        Some(Aggregation::Avg) => Some("avg"),
        Some(Aggregation::Min) => Some("min"),
        Some(Aggregation::Max) => Some("max"),
        Some(Aggregation::Median) => Some("median"),
        Some(Aggregation::Expression) => Some("expression"),
        None => None,
    }
}

fn catalog_metric_data_type(aggregation: Option<&str>) -> &'static str {
    match aggregation {
        Some("count" | "count_distinct") => "BIGINT",
        Some("sum" | "avg" | "min" | "max" | "median" | "percentile") => "NUMERIC",
        _ => "NUMERIC",
    }
}

impl SidemanticRuntime {
    pub fn from_graph(graph: SemanticGraph) -> Self {
        Self {
            graph,
            query_validation: QueryValidationContext::default(),
            top_level_metrics: Vec::new(),
            model_order: Vec::new(),
            original_model_metrics: HashMap::new(),
            model_sources: HashMap::new(),
        }
    }

    pub fn from_yaml(yaml: &str) -> Result<Self> {
        let loaded = load_from_string_with_metadata(yaml)?;
        let query_validation =
            QueryValidationContext::from_top_level_metrics(&loaded.top_level_metrics);
        Ok(Self {
            query_validation,
            top_level_metrics: loaded.top_level_metrics,
            model_order: loaded.model_order,
            original_model_metrics: loaded.original_model_metrics,
            model_sources: loaded.model_sources,
            graph: loaded.graph,
        })
    }

    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let loaded = load_from_file_with_metadata(path)?;
        let query_validation =
            QueryValidationContext::from_top_level_metrics(&loaded.top_level_metrics);
        Ok(Self {
            query_validation,
            top_level_metrics: loaded.top_level_metrics,
            model_order: loaded.model_order,
            original_model_metrics: loaded.original_model_metrics,
            model_sources: loaded.model_sources,
            graph: loaded.graph,
        })
    }

    pub fn from_directory(path: impl AsRef<Path>) -> Result<Self> {
        let loaded = load_from_directory_with_metadata(path)?;
        let query_validation =
            QueryValidationContext::from_top_level_metrics(&loaded.top_level_metrics);
        Ok(Self {
            query_validation,
            top_level_metrics: loaded.top_level_metrics,
            model_order: loaded.model_order,
            original_model_metrics: loaded.original_model_metrics,
            model_sources: loaded.model_sources,
            graph: loaded.graph,
        })
    }

    pub fn graph(&self) -> &SemanticGraph {
        &self.graph
    }

    pub fn graph_mut(&mut self) -> &mut SemanticGraph {
        &mut self.graph
    }

    pub fn top_level_metrics(&self) -> &[Metric] {
        &self.top_level_metrics
    }

    pub fn loaded_graph_payload(&self) -> LoadedGraphPayload {
        let mut model_lookup: HashMap<String, Model> = HashMap::new();
        for model in self.graph.models() {
            model_lookup.insert(model.name.clone(), model.clone());
        }

        let mut models: Vec<Model> = Vec::new();
        for name in &self.model_order {
            if let Some(model) = model_lookup.remove(name) {
                models.push(model);
            }
        }
        let mut remaining_models: Vec<Model> = model_lookup.into_values().collect();
        remaining_models.sort_by(|left, right| left.name.cmp(&right.name));
        models.extend(remaining_models);

        let mut parameters: Vec<Parameter> = self.graph.parameters().cloned().collect();
        parameters.sort_by(|left, right| left.name.cmp(&right.name));

        LoadedGraphPayload {
            models,
            parameters,
            top_level_metrics: self.top_level_metrics.clone(),
            model_order: self.model_order.clone(),
            original_model_metrics: self.original_model_metrics.clone(),
            model_sources: self.model_sources.clone(),
        }
    }

    pub fn set_query_validation_context(&mut self, context: QueryValidationContext) {
        self.query_validation = context;
    }

    pub fn compile(&self, query: &SemanticQuery) -> Result<String> {
        SqlGenerator::new(&self.graph).generate(query)
    }

    pub fn rewrite(&self, sql: &str) -> Result<String> {
        QueryRewriter::new(&self.graph).rewrite(sql)
    }

    pub fn validate_query_references(
        &self,
        metrics: &[String],
        dimensions: &[String],
    ) -> Vec<String> {
        validate_query_references(&self.graph, metrics, dimensions, &self.query_validation)
    }

    pub fn parse_reference(&self, reference: &str) -> Result<(String, String, Option<String>)> {
        self.graph.parse_reference(reference)
    }

    pub fn find_join_path(&self, from_model: &str, to_model: &str) -> Result<JoinPath> {
        self.graph.find_join_path(from_model, to_model)
    }

    pub fn find_models_for_query(
        &self,
        dimensions: &[String],
        measures: &[String],
    ) -> BTreeSet<String> {
        find_models_for_query_with_context(
            &self.graph,
            &self.query_validation,
            dimensions,
            measures,
        )
    }

    pub fn generate_preaggregation_materialization_sql(
        &self,
        model_name: &str,
        preagg_name: &str,
    ) -> Result<String> {
        let model = self.graph.get_model(model_name).ok_or_else(|| {
            SidemanticError::Validation(format!(
                "model '{model_name}' not found in materialization payload"
            ))
        })?;
        let preagg = model.get_pre_aggregation(preagg_name).ok_or_else(|| {
            SidemanticError::Validation(format!(
                "pre-aggregation '{preagg_name}' not found in model '{model_name}'"
            ))
        })?;

        let mut select_exprs: Vec<String> = Vec::new();
        let mut group_by_positions: Vec<String> = Vec::new();
        let mut position = 1usize;

        if let (Some(time_dimension), Some(granularity)) =
            (preagg.time_dimension.as_ref(), preagg.granularity.as_ref())
        {
            if let Some(time_dim) = model.get_dimension(time_dimension) {
                let col_name = format!("{time_dimension}_{granularity}");
                select_exprs.push(format!(
                    "DATE_TRUNC('{granularity}', {}) as {col_name}",
                    time_dim.sql_expr()
                ));
                group_by_positions.push(position.to_string());
                position += 1;
            }
        }

        if let Some(dimensions) = preagg.dimensions.as_ref() {
            for dim_name in dimensions {
                if let Some(dim) = model.get_dimension(dim_name) {
                    select_exprs.push(format!("{} as {dim_name}", dim.sql_expr()));
                    group_by_positions.push(position.to_string());
                    position += 1;
                }
            }
        }

        if let Some(measures) = preagg.measures.as_ref() {
            for measure_name in measures {
                if let Some(measure) = model.get_metric(measure_name) {
                    let sql_expr = measure.sql_expr();
                    match measure.agg.as_ref() {
                        Some(Aggregation::Count)
                            if measure.sql.as_deref().is_none_or(str::is_empty) =>
                        {
                            select_exprs.push(format!("COUNT(*) as {measure_name}_raw"));
                        }
                        Some(Aggregation::Count) => {
                            select_exprs.push(format!("COUNT({sql_expr}) as {measure_name}_raw"));
                        }
                        Some(Aggregation::CountDistinct) => {
                            select_exprs
                                .push(format!("COUNT(DISTINCT {sql_expr}) as {measure_name}_raw"));
                        }
                        Some(Aggregation::Sum) => {
                            select_exprs.push(format!("SUM({sql_expr}) as {measure_name}_raw"));
                        }
                        Some(Aggregation::Avg) => {
                            select_exprs.push(format!("AVG({sql_expr}) as {measure_name}_raw"));
                        }
                        Some(Aggregation::Min) => {
                            select_exprs.push(format!("MIN({sql_expr}) as {measure_name}_raw"));
                        }
                        Some(Aggregation::Max) => {
                            select_exprs.push(format!("MAX({sql_expr}) as {measure_name}_raw"));
                        }
                        Some(Aggregation::Median) => {
                            select_exprs.push(format!("MEDIAN({sql_expr}) as {measure_name}_raw"));
                        }
                        Some(Aggregation::Expression) | None => {
                            select_exprs.push(format!("SUM({sql_expr}) as {measure_name}_raw"));
                        }
                    }
                }
            }
        }

        let from_clause = if let Some(model_sql) = model.sql.as_ref() {
            format!("({model_sql}) AS t")
        } else {
            model.table.clone().unwrap_or_else(|| "None".to_string())
        };
        let select_str = select_exprs.join(",\n  ");
        let group_by_str = group_by_positions.join(", ");

        Ok(format!(
            "SELECT\n  {select_str}\nFROM {from_clause}\nGROUP BY {group_by_str}"
        ))
    }

    /// Export semantic graph catalog metadata in Postgres-compatible format.
    pub fn generate_catalog_metadata(&self, schema: &str) -> Result<String> {
        let mut tables: Vec<serde_json::Map<String, serde_json::Value>> = Vec::new();
        let mut columns: Vec<serde_json::Map<String, serde_json::Value>> = Vec::new();
        let mut constraints: Vec<serde_json::Map<String, serde_json::Value>> = Vec::new();
        let mut key_column_usage: Vec<serde_json::Map<String, serde_json::Value>> = Vec::new();

        for model in self.graph.models() {
            let model_primary_keys = model.primary_keys();
            tables.push(json_object(serde_json::json!({
                "table_catalog": "sidemantic",
                "table_schema": schema,
                "table_name": model.name,
                "table_type": "BASE TABLE",
                "is_insertable_into": "NO",
                "is_typed": "NO",
            })));

            let mut ordinal_position = 1_i64;

            if !model_primary_keys.is_empty() {
                constraints.push(json_object(serde_json::json!({
                    "constraint_catalog": "sidemantic",
                    "constraint_schema": schema,
                    "constraint_name": format!("{}_pkey", model.name),
                    "table_catalog": "sidemantic",
                    "table_schema": schema,
                    "table_name": model.name,
                    "constraint_type": "PRIMARY KEY",
                    "is_deferrable": "NO",
                    "initially_deferred": "NO",
                })));

                for (index, primary_key) in model_primary_keys.iter().enumerate() {
                    columns.push(json_object(serde_json::json!({
                        "table_catalog": "sidemantic",
                        "table_schema": schema,
                        "table_name": model.name,
                        "column_name": primary_key,
                        "ordinal_position": ordinal_position,
                        "column_default": serde_json::Value::Null,
                        "is_nullable": "NO",
                        "data_type": "BIGINT",
                        "character_maximum_length": serde_json::Value::Null,
                        "numeric_precision": 64,
                        "numeric_scale": 0,
                        "is_primary_key": true,
                        "is_foreign_key": false,
                        "is_metric": false,
                    })));
                    ordinal_position += 1;

                    key_column_usage.push(json_object(serde_json::json!({
                        "constraint_catalog": "sidemantic",
                        "constraint_schema": schema,
                        "constraint_name": format!("{}_pkey", model.name),
                        "table_catalog": "sidemantic",
                        "table_schema": schema,
                        "table_name": model.name,
                        "column_name": primary_key,
                        "ordinal_position": index + 1,
                    })));
                }
            }

            for dimension in &model.dimensions {
                if model_primary_keys.iter().any(|key| key == &dimension.name) {
                    continue;
                }

                let data_type = catalog_dimension_data_type(dimension);
                let mut column = json_object(serde_json::json!({
                    "table_catalog": "sidemantic",
                    "table_schema": schema,
                    "table_name": model.name,
                    "column_name": dimension.name,
                    "ordinal_position": ordinal_position,
                    "column_default": serde_json::Value::Null,
                    "is_nullable": "YES",
                    "data_type": data_type,
                    "character_maximum_length": if data_type == "VARCHAR" { serde_json::Value::from(255) } else { serde_json::Value::Null },
                    "numeric_precision": if data_type == "NUMERIC" { serde_json::Value::from(38) } else { serde_json::Value::Null },
                    "numeric_scale": if data_type == "NUMERIC" { serde_json::Value::from(10) } else { serde_json::Value::Null },
                    "is_primary_key": false,
                    "is_foreign_key": false,
                    "is_metric": false,
                }));

                if let Some(description) = &dimension.description {
                    column.insert(
                        "description".to_string(),
                        serde_json::Value::String(description.clone()),
                    );
                }
                if let Some(label) = &dimension.label {
                    column.insert(
                        "label".to_string(),
                        serde_json::Value::String(label.clone()),
                    );
                }

                columns.push(column);
                ordinal_position += 1;
            }

            for metric in &model.metrics {
                let aggregation = catalog_aggregation_name(metric.agg.as_ref());
                let data_type = catalog_metric_data_type(aggregation);
                let mut column = json_object(serde_json::json!({
                    "table_catalog": "sidemantic",
                    "table_schema": schema,
                    "table_name": model.name,
                    "column_name": metric.name,
                    "ordinal_position": ordinal_position,
                    "column_default": serde_json::Value::Null,
                    "is_nullable": "YES",
                    "data_type": data_type,
                    "character_maximum_length": serde_json::Value::Null,
                    "numeric_precision": if data_type == "NUMERIC" { serde_json::Value::from(38) } else { serde_json::Value::from(64) },
                    "numeric_scale": if data_type == "NUMERIC" { serde_json::Value::from(10) } else { serde_json::Value::from(0) },
                    "is_primary_key": false,
                    "is_foreign_key": false,
                    "is_metric": true,
                    "aggregation": aggregation,
                }));

                if let Some(description) = &metric.description {
                    column.insert(
                        "description".to_string(),
                        serde_json::Value::String(description.clone()),
                    );
                }
                if let Some(label) = &metric.label {
                    column.insert(
                        "label".to_string(),
                        serde_json::Value::String(label.clone()),
                    );
                }

                columns.push(column);
                ordinal_position += 1;
            }

            for relationship in &model.relationships {
                if !matches!(
                    relationship.r#type,
                    RelationshipType::ManyToOne | RelationshipType::OneToOne
                ) {
                    continue;
                }

                let foreign_key_columns = relationship.foreign_key_columns();
                let referenced_table = relationship.name.clone();
                let referenced_model =
                    self.graph.get_model(&referenced_table).ok_or_else(|| {
                        SidemanticError::Validation(format!(
                            "referenced model '{}' not found while generating catalog metadata",
                            referenced_table
                        ))
                    })?;
                let referenced_columns = if relationship.primary_key.is_some()
                    || relationship.primary_key_columns.is_some()
                {
                    relationship.primary_key_columns()
                } else {
                    referenced_model.primary_keys()
                };
                let constraint_name =
                    format!("{}_{}_fkey", model.name, foreign_key_columns.join("_"));

                constraints.push(json_object(serde_json::json!({
                    "constraint_catalog": "sidemantic",
                    "constraint_schema": schema,
                    "constraint_name": constraint_name,
                    "table_catalog": "sidemantic",
                    "table_schema": schema,
                    "table_name": model.name,
                    "constraint_type": "FOREIGN KEY",
                    "is_deferrable": "NO",
                    "initially_deferred": "NO",
                })));

                for (index, fk_column) in foreign_key_columns.iter().enumerate() {
                    let referenced_column = referenced_columns
                        .get(index)
                        .cloned()
                        .or_else(|| referenced_columns.first().cloned())
                        .unwrap_or_else(|| referenced_model.primary_key.clone());

                    key_column_usage.push(json_object(serde_json::json!({
                        "constraint_catalog": "sidemantic",
                        "constraint_schema": schema,
                        "constraint_name": constraint_name,
                        "table_catalog": "sidemantic",
                        "table_schema": schema,
                        "table_name": model.name,
                        "column_name": fk_column,
                        "ordinal_position": index + 1,
                        "position_in_unique_constraint": index + 1,
                        "referenced_table_schema": schema,
                        "referenced_table_name": referenced_table,
                        "referenced_column_name": referenced_column,
                    })));

                    for column in &mut columns {
                        let table_matches = column
                            .get("table_name")
                            .and_then(serde_json::Value::as_str)
                            .is_some_and(|table_name| table_name == model.name);
                        let column_matches = column
                            .get("column_name")
                            .and_then(serde_json::Value::as_str)
                            .is_some_and(|column_name| column_name == fk_column);

                        if table_matches && column_matches {
                            column.insert(
                                "is_foreign_key".to_string(),
                                serde_json::Value::Bool(true),
                            );
                            break;
                        }
                    }
                }
            }
        }

        serde_json::to_string(&serde_json::json!({
            "tables": tables,
            "columns": columns,
            "constraints": constraints,
            "key_column_usage": key_column_usage,
        }))
        .map_err(|e| {
            SidemanticError::Validation(format!("failed to serialize catalog metadata: {e}"))
        })
    }
}

/// Discover model names referenced by dimensions/measures.
pub fn find_models_for_query(dimensions: &[String], measures: &[String]) -> BTreeSet<String> {
    let mut models = BTreeSet::new();

    for dimension in dimensions {
        if let Some((model_name, _)) = dimension.split_once('.') {
            if !model_name.is_empty() {
                models.insert(model_name.to_string());
            }
        }
    }

    for measure in measures {
        if let Some((model_name, _)) = measure.split_once('.') {
            if !model_name.is_empty() {
                models.insert(model_name.to_string());
            }
        }
    }

    models
}

fn strip_dimension_granularity_suffix(reference: &str) -> &str {
    if let Some((base_ref, granularity)) = reference.rsplit_once("__") {
        if matches!(
            granularity,
            "hour" | "day" | "week" | "month" | "quarter" | "year"
        ) {
            return base_ref;
        }
    }
    reference
}

/// Discover model names referenced by dimensions/measures using graph context.
pub fn find_models_for_query_with_context(
    graph: &SemanticGraph,
    context: &QueryValidationContext,
    dimensions: &[String],
    measures: &[String],
) -> BTreeSet<String> {
    let mut models = find_models_for_query(dimensions, measures);

    for dimension in dimensions {
        let dimension_ref = strip_dimension_granularity_suffix(dimension);
        if dimension_ref.contains('.') {
            continue;
        }
        for model in graph.models() {
            if model.get_dimension(dimension_ref).is_some() {
                models.insert(model.name.clone());
            }
        }
    }

    for measure in measures {
        if measure.contains('.') {
            continue;
        }

        if let Some(sql_ref) = context.top_level_metric_sql_refs.get(measure) {
            if let Some((model_name, _)) = sql_ref.split_once('.') {
                if !model_name.is_empty() {
                    models.insert(model_name.to_string());
                }
            }
        }

        for model in graph.models() {
            if model.get_metric(measure).is_some() {
                models.insert(model.name.clone());
            }
        }
    }

    models
}

/// Discover model names referenced by dimensions/measures from serialized graph YAML.
pub fn find_models_for_query_with_yaml(
    models_yaml: &str,
    dimensions: &[String],
    measures: &[String],
) -> Result<BTreeSet<String>> {
    let runtime = SidemanticRuntime::from_yaml(models_yaml)?;
    Ok(runtime.find_models_for_query(dimensions, measures))
}

/// Find join path between two models from a serialized graph relationship payload.
pub fn find_relationship_path_with_yaml(
    graph_yaml: &str,
    from_model: &str,
    to_model: &str,
) -> std::result::Result<Vec<RelationshipPathStep>, RelationshipPathError> {
    if from_model == to_model {
        return Ok(Vec::new());
    }

    if let Ok(runtime) = SidemanticRuntime::from_yaml(graph_yaml) {
        return relationship_path_with_runtime(&runtime, from_model, to_model);
    }

    let payload: GraphPathPayload = serde_yaml::from_str(graph_yaml)
        .map_err(|e| RelationshipPathError::InvalidPayload(e.to_string()))?;

    let graph = semantic_graph_from_graph_path_payload(&payload)?;
    relationship_path_with_graph(&graph, from_model, to_model)
}

fn relationship_path_with_runtime(
    runtime: &SidemanticRuntime,
    from_model: &str,
    to_model: &str,
) -> std::result::Result<Vec<RelationshipPathStep>, RelationshipPathError> {
    let join_path = runtime
        .find_join_path(from_model, to_model)
        .map_err(|err| match err {
            SidemanticError::ModelNotFound(model_name, _) => {
                RelationshipPathError::ModelNotFound(model_name)
            }
            SidemanticError::NoJoinPath { from, to } => RelationshipPathError::NoJoinPath {
                from_model: from,
                to_model: to,
            },
            other => RelationshipPathError::InvalidPayload(other.to_string()),
        })?;

    Ok(join_path
        .steps
        .into_iter()
        .map(|step| {
            (
                step.from_model,
                step.to_model,
                step.from_keys,
                step.to_keys,
                relationship_type_label(&step.relationship_type).to_string(),
            )
        })
        .collect())
}

fn relationship_type_label(relationship_type: &RelationshipType) -> &'static str {
    match relationship_type {
        RelationshipType::ManyToOne => "many_to_one",
        RelationshipType::OneToOne => "one_to_one",
        RelationshipType::OneToMany => "one_to_many",
        RelationshipType::ManyToMany => "many_to_many",
    }
}

fn relationship_path_with_graph(
    graph: &SemanticGraph,
    from_model: &str,
    to_model: &str,
) -> std::result::Result<Vec<RelationshipPathStep>, RelationshipPathError> {
    let join_path = graph
        .find_join_path(from_model, to_model)
        .map_err(|err| match err {
            SidemanticError::ModelNotFound(model_name, _) => {
                RelationshipPathError::ModelNotFound(model_name)
            }
            SidemanticError::NoJoinPath { from, to } => RelationshipPathError::NoJoinPath {
                from_model: from,
                to_model: to,
            },
            other => RelationshipPathError::InvalidPayload(other.to_string()),
        })?;

    Ok(join_path
        .steps
        .into_iter()
        .map(|step| {
            (
                step.from_model,
                step.to_model,
                step.from_keys,
                step.to_keys,
                relationship_type_label(&step.relationship_type).to_string(),
            )
        })
        .collect())
}

fn semantic_graph_from_graph_path_payload(
    payload: &GraphPathPayload,
) -> std::result::Result<SemanticGraph, RelationshipPathError> {
    let mut graph = SemanticGraph::new();

    for model_payload in &payload.models {
        let primary_key_columns = graph_model_primary_keys(model_payload);
        let primary_key = primary_key_columns
            .first()
            .cloned()
            .unwrap_or_else(|| "id".to_string());

        let mut model = Model::new(model_payload.name.clone(), primary_key)
            .with_primary_key_columns(primary_key_columns)
            .with_table(model_payload.name.clone());

        for relationship_payload in &model_payload.relationships {
            let relationship_type = relationship_type_name(relationship_payload);
            let normalized_type = if relationship_type == "many_to_many"
                && relationship_payload.through.is_none()
                && relationship_has_foreign_key(relationship_payload)
            {
                RelationshipType::OneToMany
            } else {
                parse_relationship_type_label(relationship_type)
            };

            let foreign_key_columns = if relationship_has_foreign_key(relationship_payload) {
                Some(relationship_foreign_keys(relationship_payload))
            } else {
                None
            };
            let primary_key_columns = if relationship_has_primary_key(relationship_payload) {
                Some(relationship_primary_keys(relationship_payload))
            } else {
                None
            };

            model.relationships.push(Relationship {
                name: relationship_payload.name.clone(),
                r#type: normalized_type,
                foreign_key: foreign_key_columns
                    .as_ref()
                    .and_then(|columns| columns.first().cloned()),
                foreign_key_columns,
                primary_key: primary_key_columns
                    .as_ref()
                    .and_then(|columns| columns.first().cloned()),
                primary_key_columns,
                through: relationship_payload.through.clone(),
                through_foreign_key: relationship_payload.through_foreign_key.clone().or_else(
                    || {
                        if relationship_type == "many_to_many"
                            && relationship_payload.through.is_some()
                        {
                            relationship_first_foreign_key(relationship_payload)
                        } else {
                            None
                        }
                    },
                ),
                related_foreign_key: relationship_payload.related_foreign_key.clone(),
                sql: None,
            });
        }

        graph
            .add_model(model)
            .map_err(|err| RelationshipPathError::InvalidPayload(err.to_string()))?;
    }

    Ok(graph)
}

/// Extract metric dependencies with optional graph/context resolution from YAML payloads.
pub fn extract_metric_dependencies_from_yaml(
    metric_yaml: &str,
    models_yaml: Option<&str>,
    model_context: Option<&str>,
) -> Result<Vec<String>> {
    let payload: MetricDependencyPayload = serde_yaml::from_str(metric_yaml)
        .map_err(|e| SidemanticError::Validation(format!("failed to parse metric payload: {e}")))?;
    let metric_type = parse_metric_type_for_dependencies(&payload);
    let is_implicit_derived = payload.metric_type.is_none()
        && metric_type == MetricType::Derived
        && payload.sql.is_some();
    if is_implicit_derived && payload.sql.as_deref().is_some_and(has_inline_aggregation) {
        return Ok(Vec::new());
    }

    let mut metric = Metric::new(payload.name);
    metric.r#type = metric_type;
    metric.agg = parse_metric_agg_for_dependencies(payload.agg.as_deref());
    metric.sql = payload.sql;
    metric.numerator = payload.numerator;
    metric.denominator = payload.denominator;
    metric.base_metric = payload.base_metric;

    let runtime = match models_yaml {
        Some(models_yaml) => Some(SidemanticRuntime::from_yaml(models_yaml).map_err(|e| {
            SidemanticError::Validation(format!("failed to load models YAML: {e}"))
        })?),
        None => None,
    };
    let graph = runtime.as_ref().map(SidemanticRuntime::graph);
    let top_level_metric_names = extract_top_level_metric_names(models_yaml);

    let mut deps: Vec<String> = extract_dependencies_with_context(&metric, graph, model_context)
        .into_iter()
        .map(|dep| {
            if let Some((_, name)) = dep.split_once('.') {
                if top_level_metric_names.contains(name) {
                    return name.to_string();
                }
            }
            dep
        })
        .collect();
    deps.sort();
    Ok(deps)
}

/// Validate query references with Python-compatible error messages.
pub fn validate_query_references(
    graph: &SemanticGraph,
    metrics: &[String],
    dimensions: &[String],
    context: &QueryValidationContext,
) -> Vec<String> {
    let mut errors = Vec::new();

    for metric_ref in metrics {
        if let Some((model_name, metric_name)) = metric_ref.split_once('.') {
            if graph.get_model(model_name).is_none() {
                errors.push(format!(
                    "Model '{model_name}' not found (referenced in '{metric_ref}')"
                ));
                continue;
            }
            if graph
                .get_model(model_name)
                .and_then(|model| model.get_metric(metric_name))
                .is_none()
            {
                errors.push(format!(
                    "Metric '{metric_name}' not found in model '{model_name}' (referenced in '{metric_ref}')"
                ));
            }
        } else if !context.top_level_metric_names.contains(metric_ref) {
            let owner_count = graph
                .models()
                .filter(|model| model.get_metric(metric_ref).is_some())
                .count();
            if owner_count != 1 {
                errors.push(format!("Metric '{metric_ref}' not found"));
            }
        }
    }

    for dim_ref in dimensions {
        let mut dim_ref_for_lookup = dim_ref.clone();
        if let Some((base_ref, granularity)) = dim_ref.rsplit_once("__") {
            if !matches!(
                granularity,
                "hour" | "day" | "week" | "month" | "quarter" | "year"
            ) {
                errors.push(format!(
                    "Invalid time granularity '{granularity}' in '{dim_ref}'. Must be one of: hour, day, week, month, quarter, year"
                ));
            }
            dim_ref_for_lookup = base_ref.to_string();
        }

        if let Some((model_name, dim_name)) = dim_ref_for_lookup.split_once('.') {
            if graph.get_model(model_name).is_none() {
                errors.push(format!(
                    "Model '{model_name}' not found (referenced in '{dim_ref_for_lookup}')"
                ));
                continue;
            }
            if graph
                .get_model(model_name)
                .and_then(|model| model.get_dimension(dim_name))
                .is_none()
            {
                errors.push(format!(
                    "Dimension '{dim_name}' not found in model '{model_name}' (referenced in '{dim_ref_for_lookup}')"
                ));
            }
        } else {
            errors.push(format!(
                "Dimension reference '{dim_ref_for_lookup}' must be in 'model.dimension' format"
            ));
        }
    }

    let mut model_names: BTreeSet<String> = BTreeSet::new();
    for metric_ref in metrics {
        if let Some((model_name, _)) = metric_ref.split_once('.') {
            model_names.insert(model_name.to_string());
            continue;
        }
        if let Some(sql_ref) = context.top_level_metric_sql_refs.get(metric_ref) {
            if let Some((model_name, _)) = sql_ref.split_once('.') {
                model_names.insert(model_name.to_string());
            }
        }
    }
    for dim_ref in dimensions {
        let dim_ref_base = dim_ref
            .rsplit_once("__")
            .map(|(value, _)| value)
            .unwrap_or(dim_ref);
        if let Some((model_name, _)) = dim_ref_base.split_once('.') {
            model_names.insert(model_name.to_string());
        }
    }

    let valid_model_names: Vec<String> = model_names
        .into_iter()
        .filter(|model_name| graph.get_model(model_name).is_some())
        .collect();
    for (index, model_a) in valid_model_names.iter().enumerate() {
        for model_b in valid_model_names.iter().skip(index + 1) {
            if graph.find_join_path(model_a, model_b).is_err() {
                errors.push(format!(
                    "No join path found between models '{model_a}' and '{model_b}'. Add relationships to enable joining these models."
                ));
            }
        }
    }

    errors
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn test_runtime_compile_and_rewrite() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: amount
"#;
        let runtime = SidemanticRuntime::from_yaml(yaml).unwrap();

        let query = SemanticQuery::new()
            .with_metrics(vec!["orders.revenue".to_string()])
            .with_dimensions(vec!["orders.status".to_string()]);
        let compiled = runtime.compile(&query).unwrap();
        assert!(compiled.contains("SUM("));
        assert!(compiled.contains("orders_cte"));

        let rewritten = runtime
            .rewrite("SELECT orders.revenue, orders.status FROM orders")
            .unwrap();
        assert!(rewritten.contains("SUM("));
    }

    #[test]
    fn test_runtime_compile_with_yaml_query_interpolates_parameter_filters() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: amount
parameters:
  - name: status
    type: string
    default_value: pending
"#;
        let query_yaml = r#"
metrics: [orders.revenue]
dimensions: [orders.status]
filters:
  - "orders.status = {{ status }}"
parameter_values:
  status: completed
"#;

        let compiled = compile_with_yaml_query(yaml, query_yaml).unwrap();
        assert!(compiled.contains("SUM("));
        assert!(compiled.contains("'completed'"));
    }

    #[test]
    fn test_runtime_compile_with_yaml_query_renders_jinja_control_filters() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: amount
        type: numeric
    metrics:
      - name: revenue
        agg: sum
        sql: amount
parameters:
  - name: include_all
    type: yesno
    default_value: false
  - name: min_amount
    type: number
    default_value: 100
"#;
        let query_yaml = r#"
metrics: [orders.revenue]
filters:
  - "{% if include_all %}1 = 1{% else %}orders.amount >= {{ min_amount }}{% endif %}"
parameter_values:
  include_all: false
  min_amount: 500
"#;

        let compiled = compile_with_yaml_query(yaml, query_yaml).unwrap();
        assert!(compiled.contains("SUM("));
        assert!(compiled.contains("500"));
        assert!(!compiled.contains("{%"));
        assert!(!compiled.contains("{{"));
    }

    #[test]
    fn test_runtime_compile_with_yaml_query_skips_default_time_dimensions_when_requested() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    default_time_dimension: created_at
    dimensions:
      - name: created_at
        type: time
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: amount
"#;
        let query_yaml = r#"
metrics: [orders.revenue]
dimensions: [orders.status]
skip_default_time_dimensions: true
"#;

        let compiled = compile_with_yaml_query(yaml, query_yaml).unwrap();
        assert!(!compiled.contains("created_at"));
        assert!(compiled.contains("status"));
    }

    #[test]
    fn test_runtime_template_parameter_helpers_from_yaml() {
        assert!(is_sql_template("select {{ col }} from orders"));
        assert!(!is_sql_template("select col from orders"));

        let rendered = render_sql_template(
            "select {{ col }} from {{ table }}",
            "col: amount\ntable: orders\n",
        )
        .unwrap();
        assert_eq!(rendered, "select amount from orders");

        let formatted =
            format_parameter_value_with_yaml("name: status\ntype: string\n", "\"complete\"\n")
                .unwrap();
        assert_eq!(formatted, "'complete'");

        let interpolated = interpolate_sql_with_parameters_with_yaml(
            "status = {{ status }} and amount >= {{ min_amount }}",
            "- name: status\n  type: string\n- name: min_amount\n  type: number\n",
            "status: complete\nmin_amount: 100\n",
        )
        .unwrap();
        assert!(interpolated.contains("status = 'complete'"));
        assert!(interpolated.contains("amount >= 100"));
    }

    #[test]
    fn test_runtime_dimension_and_model_helper_payload_entrypoints() {
        let dimension_yaml = r#"
name: created_at
type: time
sql: created_at
supported_granularities: [day, month]
"#;
        assert_eq!(
            dimension_sql_expr_with_yaml(dimension_yaml).unwrap(),
            "created_at"
        );
        assert_eq!(
            dimension_with_granularity_with_yaml(dimension_yaml, "month").unwrap(),
            "DATE_TRUNC('month', created_at)"
        );
        let granularity_err = dimension_with_granularity_with_yaml(dimension_yaml, "year")
            .unwrap_err()
            .to_string();
        assert!(granularity_err.contains("Supported: ['day', 'month']"));

        let model_yaml = r#"
dimensions:
  - name: country
  - name: state
    parent: country
  - name: city
    parent: state
"#;
        assert_eq!(
            model_get_hierarchy_path_with_yaml(model_yaml, "city").unwrap(),
            vec![
                "country".to_string(),
                "state".to_string(),
                "city".to_string()
            ]
        );
        assert_eq!(
            model_get_drill_down_with_yaml(model_yaml, "country").unwrap(),
            Some("state".to_string())
        );
        assert_eq!(
            model_get_drill_up_with_yaml(model_yaml, "city").unwrap(),
            Some("state".to_string())
        );

        let lookup_yaml = r#"
dimensions:
  - name: status
  - name: region
metrics:
  - name: revenue
  - name: count
segments:
  - name: active
  - name: priority
pre_aggregations:
  - name: daily
  - name: monthly
"#;
        assert_eq!(
            model_find_dimension_index_with_yaml(lookup_yaml, "region").unwrap(),
            Some(1)
        );
        assert_eq!(
            model_find_metric_index_with_yaml(lookup_yaml, "count").unwrap(),
            Some(1)
        );
        assert_eq!(
            model_find_segment_index_with_yaml(lookup_yaml, "priority").unwrap(),
            Some(1)
        );
        assert_eq!(
            model_find_pre_aggregation_index_with_yaml(lookup_yaml, "monthly").unwrap(),
            Some(1)
        );
    }

    #[test]
    fn test_runtime_relationship_and_segment_helper_payload_entrypoints() {
        let relationship_yaml = r#"
name: customers
type: many_to_one
foreign_key: customer_id
primary_key: customer_uid
"#;
        assert_eq!(
            relationship_sql_expr_with_yaml(relationship_yaml).unwrap(),
            "customer_id"
        );
        assert_eq!(
            relationship_related_key_with_yaml(relationship_yaml).unwrap(),
            "customer_uid"
        );
        assert_eq!(
            relationship_foreign_key_columns_with_yaml(relationship_yaml).unwrap(),
            vec!["customer_id".to_string()]
        );
        assert_eq!(
            relationship_primary_key_columns_with_yaml(relationship_yaml).unwrap(),
            vec!["customer_uid".to_string()]
        );

        let relationship_defaults_yaml = r#"
name: orders
type: one_to_many
"#;
        assert_eq!(
            relationship_sql_expr_with_yaml(relationship_defaults_yaml).unwrap(),
            "id"
        );
        assert_eq!(
            relationship_foreign_key_columns_with_yaml(relationship_defaults_yaml).unwrap(),
            vec!["id".to_string()]
        );
        assert_eq!(
            relationship_primary_key_columns_with_yaml(relationship_defaults_yaml).unwrap(),
            vec!["id".to_string()]
        );

        let segment_yaml = "sql: \"{model}.status = 'completed'\"\n";
        assert_eq!(
            segment_get_sql_with_yaml(segment_yaml, "orders_cte").unwrap(),
            "orders_cte.status = 'completed'"
        );
    }

    #[test]
    fn test_runtime_preaggregation_recommender_helper_entrypoints() {
        let queries = vec![
            "select * from orders -- sidemantic: models=orders metrics=orders.revenue dimensions=orders.status granularities=day".to_string(),
            "select * from orders -- sidemantic: models=orders metrics=orders.revenue dimensions=orders.status granularities=day".to_string(),
            "select * from orders -- sidemantic: models=orders metrics=orders.count dimensions=orders.region".to_string(),
            "select * from orders".to_string(),
        ];
        let patterns_json = extract_preaggregation_patterns(queries).unwrap();
        assert!(patterns_json.contains("\"model\":\"orders\""));

        let summary_json = summarize_preaggregation_patterns(&patterns_json, 2).unwrap();
        assert!(summary_json.contains("\"total_queries\":3"));
        assert!(summary_json.contains("\"patterns_above_threshold\":1"));

        let recommendations_json =
            recommend_preaggregation_patterns(&patterns_json, 1, 0.0, Some(1)).unwrap();
        assert!(recommendations_json.contains("\"query_count\":2"));

        let score = calculate_preaggregation_benefit_score(
            r#"{"model":"orders","metrics":["orders.revenue"],"dimensions":["orders.status"],"granularities":["day"],"count":2}"#,
            2,
        )
        .unwrap();
        assert!(score > 0.0);

        let generated_name = generate_preaggregation_name(
            r#"{"model":"orders","metrics":["orders.revenue"],"dimensions":["orders.status"],"granularities":["day"],"count":2}"#,
        )
        .unwrap();
        assert_eq!(generated_name, "day_status_revenue");

        let definition_json = generate_preaggregation_definition(
            r#"{"pattern":{"model":"orders","metrics":["orders.revenue"],"dimensions":["orders.created_at","orders.status"],"granularities":["day"],"count":2},"suggested_name":"day_created_at_status_revenue","query_count":2,"estimated_benefit_score":0.5}"#,
        )
        .unwrap();
        assert!(definition_json.contains("\"name\":\"day_created_at_status_revenue\""));
        assert!(definition_json.contains("\"time_dimension\":\"created_at\""));
        assert!(definition_json.contains("\"granularity\":\"day\""));

        let parse_err = calculate_preaggregation_benefit_score("{", 1).unwrap_err();
        assert!(parse_err
            .to_string()
            .contains("failed to parse pattern payload for benefit score calculation"));
    }

    #[test]
    fn test_runtime_metric_helper_payload_entrypoints() {
        assert_eq!(
            parse_simple_metric_aggregation("SUM(amount)"),
            Some(("sum".to_string(), Some("amount".to_string())))
        );
        assert_eq!(
            parse_simple_metric_aggregation("COUNT(DISTINCT customer_id)"),
            Some((
                "count_distinct".to_string(),
                Some("customer_id".to_string())
            ))
        );
        assert_eq!(parse_simple_metric_aggregation("revenue + cost"), None);

        let simple_metric_yaml = r#"
name: revenue
agg: sum
sql: amount
"#;
        assert_eq!(metric_to_sql(simple_metric_yaml).unwrap(), "SUM(amount)");
        assert_eq!(metric_sql_expr(simple_metric_yaml).unwrap(), "amount");
        assert!(metric_is_simple_aggregation(simple_metric_yaml).unwrap());

        let count_metric_yaml = r#"
name: orders
agg: count
"#;
        assert_eq!(metric_sql_expr(count_metric_yaml).unwrap(), "*");

        let derived_metric_yaml = r#"
name: margin
type: derived
sql: revenue - cost
"#;
        let to_sql_err = metric_to_sql(derived_metric_yaml).unwrap_err();
        assert!(to_sql_err
            .to_string()
            .contains("Cannot convert complex metric 'margin' to SQL"));
        assert!(!metric_is_simple_aggregation(derived_metric_yaml).unwrap());

        let parse_err = metric_sql_expr("name: [").unwrap_err();
        assert!(parse_err
            .to_string()
            .contains("failed to parse metric payload"));
    }

    #[test]
    fn test_runtime_metric_inheritance_helper_entrypoint() {
        let metrics_yaml = r#"
- name: base
  agg: sum
  sql: amount
  filters:
    - status = 'complete'
- name: child
  extends: base
  filters:
    - region = 'US'
"#;
        let resolved_yaml = resolve_metric_inheritance(metrics_yaml).unwrap();
        assert!(resolved_yaml.contains("name: base"));
        assert!(resolved_yaml.contains("name: child"));
        assert!(resolved_yaml.contains("agg: sum"));
        assert!(resolved_yaml.contains("sql: amount"));
        assert!(resolved_yaml.contains("- status = 'complete'"));
        assert!(resolved_yaml.contains("- region = 'US'"));

        let circular_err = resolve_metric_inheritance(
            r#"
- name: a
  extends: b
- name: b
  extends: a
"#,
        )
        .unwrap_err();
        assert!(circular_err
            .to_string()
            .contains("failed to resolve metric inheritance"));

        let parse_err = resolve_metric_inheritance("{").unwrap_err();
        assert!(parse_err
            .to_string()
            .contains("failed to parse metrics payload"));
    }

    #[test]
    fn test_runtime_model_inheritance_helper_entrypoint() {
        let models_yaml = r#"
models:
  - name: base
    table: orders
    primary_key: id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: amount
  - name: child
    extends: base
    table: child_orders
    primary_key: id
"#;
        let resolved_yaml = resolve_model_inheritance_with_yaml(models_yaml).unwrap();
        assert!(resolved_yaml.contains("name: base"));
        assert!(resolved_yaml.contains("name: child"));

        let duplicate_err = resolve_model_inheritance_with_yaml(
            r#"
models:
  - name: dup
    table: a
    primary_key: id
  - name: dup
    table: b
    primary_key: id
"#,
        )
        .unwrap_err();
        assert!(duplicate_err
            .to_string()
            .contains("Duplicate model 'dup' in inheritance payload"));

        let parse_err = resolve_model_inheritance_with_yaml("{").unwrap_err();
        assert!(parse_err
            .to_string()
            .contains("failed to parse models payload"));
    }

    #[test]
    fn test_runtime_payload_validation_helper_entrypoints() {
        let model_yaml = r#"
name: orders
table: orders
primary_key: id
"#;
        assert!(validate_model_payload(model_yaml).unwrap());

        let metric_yaml = r#"
name: revenue
type: ratio
numerator: revenue
denominator: cost
agg: sum
"#;
        assert!(validate_metric_payload(metric_yaml).unwrap());
        let metric_err = validate_metric_payload("name: revenue\ntype: derived\n").unwrap_err();
        assert!(metric_err
            .to_string()
            .contains("derived metric requires 'sql' field"));

        let parameter_yaml = r#"
name: region
type: string
default_value: us
"#;
        assert!(validate_parameter_payload(parameter_yaml).unwrap());

        let calc_yaml = r#"
name: pct
type: percent_of_total
"#;
        assert!(validate_table_calculation_payload(calc_yaml).unwrap());
        let calc_err = validate_table_calculation_payload("name: pct\ntype: nope\n").unwrap_err();
        assert!(calc_err
            .to_string()
            .contains("unsupported table calculation type"));
    }

    #[test]
    fn test_runtime_detect_adapter_kind_entrypoint() {
        assert_eq!(
            detect_adapter_kind("test.lkml", ""),
            Some("lookml".to_string())
        );
        assert_eq!(
            detect_adapter_kind("test.malloy", ""),
            Some("malloy".to_string())
        );
        assert_eq!(
            detect_adapter_kind("test.sql", ""),
            Some("sidemantic".to_string())
        );
        assert_eq!(
            detect_adapter_kind("test.yml", "models:\n  - name: orders\n"),
            Some("sidemantic".to_string())
        );
        assert_eq!(
            detect_adapter_kind("test.yml", "cubes:\n  - name: orders\n"),
            Some("cube".to_string())
        );
        assert_eq!(
            detect_adapter_kind("test.yml", "metrics:\n  - name: orders\n    type: ratio\n"),
            Some("metricflow".to_string())
        );
        assert_eq!(
            detect_adapter_kind("test.yml", "dimensions:\n  status: _.status\n"),
            Some("bsl".to_string())
        );
        assert_eq!(
            detect_adapter_kind("test.yml", "table_name: orders\ncolumns: []\nmetrics: []\n"),
            Some("superset".to_string())
        );
        assert_eq!(detect_adapter_kind("test.txt", "models:\n"), None);
        assert_eq!(detect_adapter_kind("test.yml", "foo: bar\n"), None);
    }

    #[test]
    fn test_runtime_table_calculation_expression_helpers() {
        assert_eq!(
            evaluate_table_calculation_expression("1 + 2 * 3").unwrap(),
            7.0
        );
        assert!(validate_table_formula_expression("${revenue} - ${cost}").unwrap());

        let eval_err = evaluate_table_calculation_expression("1 +").unwrap_err();
        assert!(eval_err.to_string().contains("Invalid expression: 1 +"));

        let validation_err = validate_table_formula_expression("bad ++").unwrap_err();
        assert!(validation_err
            .to_string()
            .contains("Invalid formula expression syntax"));
    }

    #[test]
    fn test_runtime_relative_date_helpers() {
        assert_eq!(
            parse_relative_date("today", "duckdb"),
            Some("CURRENT_DATE".to_string())
        );
        assert_eq!(
            parse_relative_date("this month", "bigquery"),
            Some("DATE_TRUNC(CURRENT_DATE, MONTH)".to_string())
        );

        let range = relative_date_to_range("this month", "order_date", "duckdb").unwrap();
        assert!(range.contains("order_date >= DATE_TRUNC('month', CURRENT_DATE)"));
        assert!(
            range.contains("order_date < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'")
        );
        assert_eq!(
            relative_date_to_range("today", "event_date", "duckdb"),
            Some("event_date = CURRENT_DATE".to_string())
        );

        assert!(is_relative_date("last 7 days"));
        assert!(!is_relative_date("2024-01-01"));
    }

    #[test]
    fn test_runtime_time_intelligence_helpers() {
        assert_eq!(
            time_comparison_offset_interval("yoy", None, None).unwrap(),
            (1, "year".to_string())
        );
        assert_eq!(
            time_comparison_offset_interval("prior_period", Some(7), Some("day")).unwrap(),
            (7, "day".to_string())
        );
        assert_eq!(
            time_comparison_sql_offset("mom", None, None).unwrap(),
            "INTERVAL '1 month'"
        );
        assert_eq!(
            trailing_period_sql_interval(3, "month").unwrap(),
            "INTERVAL '3 month'"
        );

        let sql = generate_time_comparison_sql(
            "mom",
            "percent_change",
            "SUM(amount)",
            "order_date",
            None,
            None,
        )
        .unwrap();
        assert!(sql.contains("LAG("));
        assert!(sql.contains("ORDER BY order_date"));
        assert!(sql.contains("/ NULLIF"));

        let calc_err =
            generate_time_comparison_sql("mom", "bad", "SUM(amount)", "order_date", None, None)
                .unwrap_err();
        assert!(calc_err
            .to_string()
            .contains("Unknown calculation type: bad"));

        let offset_err =
            time_comparison_offset_interval("mom", Some(1), Some("fortnight")).unwrap_err();
        assert!(offset_err
            .to_string()
            .contains("invalid time offset unit 'fortnight'"));
    }

    #[test]
    fn test_runtime_validate_query_with_yaml_payload() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: amount
"#;
        let query_yaml = r#"
metrics: [orders.revenue]
dimensions: [orders.status]
"#;

        let errors = validate_query_with_yaml(yaml, query_yaml).unwrap();
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
    }

    #[test]
    fn test_runtime_validate_query_references_with_yaml_payload() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: amount
"#;

        let errors = validate_query_references_with_yaml(
            yaml,
            &["orders.revenue".to_string()],
            &["orders.status".to_string()],
        )
        .unwrap();
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
    }

    #[test]
    fn test_runtime_validate_query_with_yaml_reports_payload_parse_error() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
"#;
        let err = validate_query_with_yaml(yaml, "[invalid").unwrap_err();
        assert!(err.to_string().contains("failed to parse query payload"));
    }

    #[test]
    fn test_runtime_load_graph_with_yaml_payload_helper() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    metrics:
      - name: revenue
        agg: sum
        sql: amount
"#;
        let payload_json = load_graph_with_yaml(yaml).unwrap();
        let payload: serde_json::Value = serde_json::from_str(&payload_json).unwrap();
        assert!(payload["models"]
            .as_array()
            .is_some_and(|models| models.iter().any(|model| model["name"] == "orders")));
    }

    #[test]
    fn test_runtime_load_graph_with_sql_payload_helper_model_path() {
        let sql = r#"
MODEL (
  name orders,
  table orders,
  primary_key order_id
);

METRIC (
  name order_count,
  agg count
);

METRIC (
  name total_orders,
  type derived,
  sql orders.order_count
);

PARAMETER (
  name region,
  type string
);
"#;

        let payload_json = load_graph_with_sql(sql).unwrap();
        let payload: serde_json::Value = serde_json::from_str(&payload_json).unwrap();
        assert!(payload["models"].as_array().is_some_and(|models| {
            models.iter().any(|model| {
                model["name"] == "orders"
                    && model["metrics"].as_array().is_some_and(|metrics| {
                        metrics
                            .iter()
                            .any(|metric| metric["name"] == "total_orders")
                    })
            })
        }));
        assert!(payload["top_level_metrics"]
            .as_array()
            .is_some_and(|metrics| metrics.is_empty()));
        assert!(payload["parameters"]
            .as_array()
            .is_some_and(|params| params.iter().any(|param| param["name"] == "region")));
    }

    #[test]
    fn test_runtime_load_graph_with_sql_payload_helper_frontmatter_path() {
        let sql = r#"
---
name: orders
table: orders
primary_key: order_id
---

METRIC (
  name order_count,
  agg count
);

SEGMENT (
  name completed,
  sql status = 'completed'
);
"#;

        let payload_json = load_graph_with_sql(sql).unwrap();
        let payload: serde_json::Value = serde_json::from_str(&payload_json).unwrap();
        assert!(payload["models"].as_array().is_some_and(|models| {
            models.iter().any(|model| {
                model["name"] == "orders"
                    && model["metrics"].as_array().is_some_and(|metrics| {
                        metrics.iter().any(|metric| metric["name"] == "order_count")
                    })
                    && model["segments"].as_array().is_some_and(|segments| {
                        segments
                            .iter()
                            .any(|segment| segment["name"] == "completed")
                    })
            })
        }));
        assert!(payload["top_level_metrics"]
            .as_array()
            .is_some_and(|metrics| metrics.is_empty()));
    }

    #[test]
    fn test_runtime_load_graph_with_sql_payload_helper_graph_level_path() {
        let sql = r#"
METRIC (
  name total_orders,
  agg count
);

PARAMETER (
  name region,
  type string
);
"#;

        let payload_json = load_graph_with_sql(sql).unwrap();
        let payload: serde_json::Value = serde_json::from_str(&payload_json).unwrap();
        assert!(payload["models"]
            .as_array()
            .is_some_and(|models| models.is_empty()));
        assert!(payload["top_level_metrics"]
            .as_array()
            .is_some_and(|metrics| metrics
                .iter()
                .any(|metric| metric["name"] == "total_orders")));
        assert!(payload["parameters"]
            .as_array()
            .is_some_and(|params| params.iter().any(|param| param["name"] == "region")));
    }

    #[test]
    fn test_runtime_sql_definitions_parser_entrypoints() {
        let definitions_payload = parse_sql_definitions_payload(
            r#"
METRIC (name revenue, agg sum, sql amount);
SEGMENT (name completed, sql status = 'completed');
"#,
        )
        .unwrap();
        assert!(definitions_payload.contains("\"metrics\""));
        assert!(definitions_payload.contains("\"segments\""));
        assert!(definitions_payload.contains("\"revenue\""));

        let graph_payload = parse_sql_graph_definitions_payload(
            r#"
METRIC (name revenue, agg sum, sql amount);
PARAMETER (name region, type string);
PRE_AGGREGATION (name daily_rollup, measures [revenue], dimensions [status]);
"#,
        )
        .unwrap();
        assert!(graph_payload.contains("\"parameters\""));
        assert!(graph_payload.contains("\"pre_aggregations\""));
        assert!(graph_payload.contains("\"daily_rollup\""));

        let model_payload = parse_sql_model_payload(
            r#"
MODEL (name orders, table orders, primary_key order_id);
DIMENSION (name status, type categorical);
"#,
        )
        .unwrap();
        assert!(model_payload.contains("\"name\":\"orders\""));
        assert!(model_payload.contains("\"dimensions\""));

        let statement_blocks_payload = parse_sql_statement_blocks_payload(
            r#"
MODEL (name orders, table orders);
METRIC (name revenue, expression SUM(amount));
"#,
        )
        .unwrap();
        assert!(statement_blocks_payload.contains("\"kind\":\"model\""));
        assert!(statement_blocks_payload.contains("\"kind\":\"metric\""));
        assert!(statement_blocks_payload.contains("\"sql\":\"SUM(amount)\""));

        let parse_err =
            parse_sql_model_payload("METRIC (name revenue, agg sum, sql amount);").unwrap_err();
        assert!(parse_err.to_string().contains("failed to parse SQL model"));
    }

    #[test]
    fn test_runtime_extract_column_references_entrypoint() {
        let refs = extract_column_references("(revenue - cost) / revenue");
        assert_eq!(refs, vec!["cost".to_string(), "revenue".to_string()]);
    }

    #[test]
    fn test_runtime_analyze_migrator_query_entrypoint() {
        let payload = analyze_migrator_query(
            r#"
SELECT
    status,
    SUM(amount) / COUNT(*) AS avg_order_value
FROM orders
GROUP BY status
"#,
        )
        .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&payload).unwrap();

        assert!(parsed["group_by_columns"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry == &serde_json::json!(["", "status"])));
        assert_eq!(parsed["derived_metrics"].as_array().unwrap().len(), 1);
        assert_eq!(parsed["derived_metrics"][0]["name"], "avg_order_value");
    }

    #[test]
    fn test_runtime_analyze_migrator_query_window_entrypoint() {
        let payload = analyze_migrator_query(
            r#"
SELECT
    SUM(amount) OVER (ORDER BY order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7day_total
FROM orders
"#,
        )
        .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(parsed["cumulative_metrics"].as_array().unwrap().len(), 1);
        assert_eq!(
            parsed["cumulative_metrics"][0]["name"],
            "rolling_7day_total"
        );
        assert_eq!(parsed["cumulative_metrics"][0]["window"], "6 days");
    }

    #[test]
    fn test_runtime_chart_auto_detect_columns_entrypoint() {
        let columns = vec![
            "created_at".to_string(),
            "revenue".to_string(),
            "region".to_string(),
        ];
        let numeric_flags = vec![true, false];
        let (x, y_cols) = chart_auto_detect_columns(&columns, &numeric_flags).unwrap();
        assert_eq!(x, "created_at");
        assert_eq!(y_cols, vec!["revenue".to_string()]);
    }

    #[test]
    fn test_runtime_chart_select_type_entrypoint() {
        assert_eq!(chart_select_type("created_at", "string", 1), "area");
        assert_eq!(chart_select_type("created_at", "string", 2), "line");
        assert_eq!(chart_select_type("amount", "number", 1), "scatter");
        assert_eq!(chart_select_type("status", "other", 1), "bar");
    }

    #[test]
    fn test_runtime_chart_encoding_type_entrypoint() {
        assert_eq!(chart_encoding_type("order_date"), "temporal");
        assert_eq!(chart_encoding_type("customer_status"), "nominal");
    }

    #[test]
    fn test_runtime_chart_format_label_entrypoint() {
        assert_eq!(chart_format_label("order_count"), "Order Count");
        assert_eq!(chart_format_label("orders.revenue"), "Revenue");
        assert_eq!(
            chart_format_label("created_at__month"),
            "Created At (Month)"
        );
    }

    #[test]
    fn test_runtime_build_symmetric_aggregate_sql_entrypoint() {
        let sql = build_symmetric_aggregate_sql(
            "amount",
            "order_id",
            "sum",
            Some("orders_cte"),
            "duckdb",
        )
        .unwrap();
        assert!(sql.contains("SUM(DISTINCT"));
        assert!(sql.contains("orders_cte.order_id"));
    }

    #[test]
    fn test_runtime_needs_symmetric_aggregate_entrypoint() {
        assert!(needs_symmetric_aggregate("one_to_many", true));
        assert!(!needs_symmetric_aggregate("many_to_one", true));
    }

    #[test]
    fn test_runtime_validate_models_yaml_helper() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
"#;
        assert!(validate_models_yaml(yaml).unwrap());
    }

    #[test]
    fn test_runtime_parse_reference_with_yaml_helper() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: order_date
        type: time
"#;
        let parsed = parse_reference_with_yaml(yaml, "orders.order_date__month").unwrap();
        assert_eq!(parsed.0, "orders");
        assert_eq!(parsed.1, "order_date");
        assert_eq!(parsed.2.as_deref(), Some("month"));
    }

    #[test]
    fn test_runtime_rewrite_with_yaml_helper() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: amount
"#;
        let rewritten =
            rewrite_with_yaml(yaml, "SELECT orders.revenue, orders.status FROM orders").unwrap();
        assert!(rewritten.contains("SUM("));
    }

    #[test]
    fn test_runtime_rewrite_with_yaml_wasm_fallback_helper() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: amount
"#;
        let runtime = SidemanticRuntime::from_yaml(yaml).unwrap();
        let rewritten = rewrite_with_yaml_wasm_fallback(
            &runtime,
            "SELECT o.revenue, o.status FROM orders AS o WHERE o.status = 'completed'",
        )
        .unwrap();
        assert!(rewritten.contains("SUM("));
        assert!(rewritten.contains("status"));
        assert!(rewritten.contains("'completed'"));
    }

    #[test]
    fn test_runtime_rewrite_with_yaml_wasm_fallback_helper_supports_count_star() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: count
        agg: count
"#;
        let runtime = SidemanticRuntime::from_yaml(yaml).unwrap();
        let rewritten =
            rewrite_with_yaml_wasm_fallback(&runtime, "SELECT COUNT(*) AS row_count FROM orders o")
                .unwrap();
        assert!(rewritten.to_ascii_uppercase().contains("COUNT("));
    }

    #[test]
    fn test_runtime_rewrite_with_yaml_wasm_fallback_helper_count_requires_metric() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: amount
"#;
        let runtime = SidemanticRuntime::from_yaml(yaml).unwrap();
        let err = rewrite_with_yaml_wasm_fallback(
            &runtime,
            "SELECT COUNT(*) AS row_count FROM orders AS o WHERE o.status = 'completed'",
        )
        .unwrap_err();
        assert!(err.to_string().contains("requires 'orders.count' metric"));
    }

    #[test]
    fn test_runtime_rewrite_with_yaml_wasm_fallback_helper_supports_order_by_alias_and_limit() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: amount
"#;
        let runtime = SidemanticRuntime::from_yaml(yaml).unwrap();
        let rewritten = rewrite_with_yaml_wasm_fallback(
            &runtime,
            "SELECT o.revenue AS rev, o.status FROM orders AS o WHERE o.status = 'completed' ORDER BY rev DESC, o.status ASC LIMIT 5",
        )
        .unwrap();
        assert!(rewritten.contains("ORDER BY"));
        assert!(rewritten.contains("DESC"));
        assert!(rewritten.contains("LIMIT 5"));
        assert!(rewritten.contains("'completed'"));
    }

    #[test]
    fn test_runtime_rewrite_with_yaml_wasm_fallback_helper_supports_count_alias_order_by() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    metrics:
      - name: count
        agg: count
"#;
        let runtime = SidemanticRuntime::from_yaml(yaml).unwrap();
        let rewritten = rewrite_with_yaml_wasm_fallback(
            &runtime,
            "SELECT COUNT(*) AS row_count FROM orders ORDER BY row_count DESC LIMIT 2",
        )
        .unwrap();
        assert!(rewritten.to_ascii_uppercase().contains("COUNT("));
        assert!(rewritten.contains("ORDER BY"));
        assert!(rewritten.contains("LIMIT 2"));
    }

    #[test]
    fn test_runtime_rewrite_with_yaml_wasm_fallback_helper_supports_sum_projection_via_metric() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: amount
"#;
        let runtime = SidemanticRuntime::from_yaml(yaml).unwrap();
        let rewritten = rewrite_with_yaml_wasm_fallback(
            &runtime,
            "SELECT SUM(o.amount) AS total_revenue, o.status FROM orders o WHERE o.status = 'completed' ORDER BY total_revenue DESC LIMIT 10",
        )
        .unwrap();
        assert!(rewritten.contains("SUM("));
        assert!(rewritten.contains("ORDER BY"));
        assert!(rewritten.contains("LIMIT 10"));
        assert!(rewritten.contains("'completed'"));
    }

    #[test]
    fn test_runtime_rewrite_with_yaml_wasm_fallback_helper_supports_count_distinct_projection_via_metric(
    ) {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: unique_customers
        agg: count_distinct
        sql: customer_id
"#;
        let runtime = SidemanticRuntime::from_yaml(yaml).unwrap();
        let rewritten = rewrite_with_yaml_wasm_fallback(
            &runtime,
            "SELECT COUNT(DISTINCT o.customer_id) AS uniq FROM orders o ORDER BY uniq DESC LIMIT 1",
        )
        .unwrap();
        assert!(rewritten.to_ascii_uppercase().contains("COUNT(DISTINCT"));
        assert!(rewritten.contains("ORDER BY"));
        assert!(rewritten.contains("LIMIT 1"));
    }

    #[test]
    fn test_runtime_rewrite_with_yaml_wasm_fallback_helper_supports_order_by_aggregate_expression()
    {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: amount
"#;
        let runtime = SidemanticRuntime::from_yaml(yaml).unwrap();
        let rewritten = rewrite_with_yaml_wasm_fallback(
            &runtime,
            "SELECT SUM(amount) AS total_revenue, status FROM orders ORDER BY SUM(amount) DESC LIMIT 3",
        )
        .unwrap();
        assert!(rewritten.contains("SUM("));
        assert!(rewritten.contains("ORDER BY"));
        assert!(rewritten.contains("LIMIT 3"));
    }

    #[test]
    fn test_runtime_rewrite_with_yaml_wasm_fallback_helper_supports_order_by_positional_index() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: amount
"#;
        let runtime = SidemanticRuntime::from_yaml(yaml).unwrap();
        let rewritten = rewrite_with_yaml_wasm_fallback(
            &runtime,
            "SELECT SUM(amount) AS total_revenue, status FROM orders ORDER BY 1 DESC LIMIT 3",
        )
        .unwrap();
        assert!(rewritten.contains("SUM("));
        assert!(rewritten.contains("ORDER BY"));
        assert!(rewritten.contains("LIMIT 3"));
    }

    #[test]
    fn test_runtime_rewrite_with_yaml_wasm_fallback_helper_order_by_positional_index_out_of_range()
    {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
"#;
        let runtime = SidemanticRuntime::from_yaml(yaml).unwrap();
        let err =
            rewrite_with_yaml_wasm_fallback(&runtime, "SELECT status FROM orders ORDER BY 2 DESC")
                .unwrap_err();
        assert!(err
            .to_string()
            .contains("ORDER BY position 2 is out of range"));
    }

    #[test]
    fn test_runtime_rewrite_with_yaml_wasm_fallback_helper_supports_derived_expression_projection()
    {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: count
        agg: count
      - name: avg_order_value
        type: derived
        sql: SUM(amount) / COUNT(*)
"#;
        let runtime = SidemanticRuntime::from_yaml(yaml).unwrap();
        let rewritten = rewrite_with_yaml_wasm_fallback(
            &runtime,
            "SELECT SUM(amount) / COUNT(*) AS aov, status FROM orders ORDER BY aov DESC LIMIT 2",
        )
        .unwrap();
        assert!(rewritten.to_ascii_uppercase().contains("COUNT("));
        assert!(rewritten.contains("ORDER BY"));
        assert!(rewritten.contains("LIMIT 2"));
    }

    #[test]
    fn test_runtime_rewrite_with_yaml_wasm_fallback_helper_supports_expression_projection_via_formula_table_calc(
    ) {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: count
        agg: count
      - name: revenue
        agg: sum
        sql: amount
"#;
        let runtime = SidemanticRuntime::from_yaml(yaml).unwrap();
        let rewritten = rewrite_with_yaml_wasm_fallback(
            &runtime,
            "SELECT SUM(amount) / COUNT(*) AS aov, status FROM orders ORDER BY aov DESC LIMIT 2",
        )
        .unwrap();
        assert!(rewritten.to_ascii_uppercase().contains("COUNT("));
        assert!(rewritten.contains("revenue / count AS aov"));
        assert!(rewritten.contains("ORDER BY"));
        assert!(rewritten.contains("LIMIT 2"));
    }

    #[test]
    fn test_runtime_rewrite_with_yaml_wasm_fallback_helper_expression_requires_matching_metric() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    metrics:
      - name: count
        agg: count
      - name: revenue
        agg: sum
        sql: amount
"#;
        let runtime = SidemanticRuntime::from_yaml(yaml).unwrap();
        let err = rewrite_with_yaml_wasm_fallback(
            &runtime,
            "SELECT SUM(profit) / COUNT(*) AS aov FROM orders ORDER BY aov DESC",
        )
        .unwrap_err();
        assert!(err
            .to_string()
            .contains("requires a matching metric expression"));
    }

    #[test]
    fn test_runtime_rewrite_with_yaml_wasm_fallback_helper_aggregate_requires_matching_metric() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    metrics:
      - name: count
        agg: count
"#;
        let runtime = SidemanticRuntime::from_yaml(yaml).unwrap();
        let err =
            rewrite_with_yaml_wasm_fallback(&runtime, "SELECT SUM(amount) AS total FROM orders")
                .unwrap_err();
        assert!(err.to_string().contains("requires a matching metric"));
    }

    #[test]
    fn test_runtime_generate_preaggregation_materialization_sql_with_yaml_helper() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: order_date
        type: time
    metrics:
      - name: revenue
        agg: sum
        sql: amount
    pre_aggregations:
      - name: daily_revenue
        time_dimension: order_date
        granularity: day
        measures: [revenue]
"#;
        let sql =
            generate_preaggregation_materialization_sql_with_yaml(yaml, "orders", "daily_revenue")
                .unwrap();
        assert!(sql.contains("DATE_TRUNC('day', order_date) as order_date_day"));
        assert!(sql.contains("SUM(amount) as revenue_raw"));
    }

    #[test]
    fn test_runtime_generate_catalog_metadata_with_yaml_helper() {
        let yaml = r#"
models:
  - name: customers
    table: customers
    primary_key: id
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: customer_id
        type: numeric
    relationships:
      - name: customers
        type: many_to_one
        foreign_key: customer_id
"#;
        let json = generate_catalog_metadata_with_yaml(yaml, "analytics").unwrap();
        let payload: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(payload["tables"]
            .as_array()
            .is_some_and(|tables| tables.iter().any(|table| table["table_name"] == "orders")));
    }

    #[test]
    fn test_runtime_validation_accepts_top_level_metric() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: amount
      - name: order_count
        agg: count
metrics:
  - name: revenue_per_order
    type: ratio
    numerator: revenue
    denominator: order_count
"#;
        let runtime = SidemanticRuntime::from_yaml(yaml).unwrap();
        let errors = runtime.validate_query_references(
            &["revenue_per_order".to_string()],
            &["orders.status".to_string()],
        );
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
    }

    #[test]
    fn test_runtime_validation_reports_missing_join_path() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: amount
  - name: customers
    table: customers
    primary_key: customer_id
    dimensions:
      - name: country
        type: categorical
"#;
        let runtime = SidemanticRuntime::from_yaml(yaml).unwrap();
        let errors = runtime.validate_query_references(
            &["orders.revenue".to_string()],
            &["customers.country".to_string()],
        );
        assert!(errors.iter().any(|error| {
            error.contains("No join path found between models 'customers' and 'orders'")
        }));
    }

    #[test]
    fn test_find_models_for_query_collects_unique_models() {
        let models = find_models_for_query(
            &["orders.status".to_string(), "customers.country".to_string()],
            &["orders.revenue".to_string(), "orders.count".to_string()],
        );
        assert_eq!(
            models.into_iter().collect::<Vec<_>>(),
            vec!["customers".to_string(), "orders".to_string()]
        );
    }

    #[test]
    fn test_runtime_find_models_for_query_uses_graph_context_for_unqualified_refs() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
      - name: order_date
        type: time
    metrics:
      - name: revenue
        agg: sum
        sql: amount
metrics:
  - name: total_revenue
    agg: sum
    sql: orders.revenue
"#;
        let runtime = SidemanticRuntime::from_yaml(yaml).unwrap();
        let models = runtime.find_models_for_query(
            &["status".to_string(), "order_date__month".to_string()],
            &["total_revenue".to_string()],
        );
        assert_eq!(
            models.into_iter().collect::<Vec<_>>(),
            vec!["orders".to_string()]
        );
    }

    #[test]
    fn test_find_models_for_query_with_yaml_resolves_context() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: amount
metrics:
  - name: total_revenue
    agg: sum
    sql: orders.revenue
"#;
        let models = find_models_for_query_with_yaml(
            yaml,
            &["status".to_string()],
            &["total_revenue".to_string()],
        )
        .unwrap();
        assert_eq!(
            models.into_iter().collect::<Vec<_>>(),
            vec!["orders".to_string()]
        );
    }

    #[test]
    fn test_runtime_from_directory_preserves_top_level_metric_context() {
        let mut dir = std::env::temp_dir();
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        dir.push(format!("sidemantic_runtime_dir_{suffix}"));
        std::fs::create_dir_all(&dir).unwrap();

        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: amount
      - name: order_count
        agg: count
metrics:
  - name: revenue_per_order
    type: ratio
    numerator: revenue
    denominator: order_count
"#;
        let file_path = dir.join("models.yml");
        std::fs::write(&file_path, yaml).unwrap();

        let runtime = SidemanticRuntime::from_directory(&dir).unwrap();
        let errors = runtime.validate_query_references(
            &["revenue_per_order".to_string()],
            &["orders.status".to_string()],
        );
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_runtime_from_file_supports_sql_model_files() {
        let mut path = std::env::temp_dir();
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        path.push(format!("sidemantic_runtime_sql_{suffix}.sql"));

        let sql_models = r#"
MODEL (
  name orders,
  table orders,
  primary_key order_id
);

DIMENSION (
  name status,
  type categorical
);

METRIC (
  name order_count,
  agg count
);
"#;
        std::fs::write(&path, sql_models).unwrap();

        let runtime = SidemanticRuntime::from_file(&path).unwrap();
        let query = SemanticQuery::new().with_metrics(vec!["orders.order_count".to_string()]);
        let sql = runtime.compile(&query).unwrap();
        assert!(sql.contains("COUNT("), "unexpected SQL: {sql}");

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_runtime_from_directory_supports_sql_and_yaml_files() {
        let mut dir = std::env::temp_dir();
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        dir.push(format!("sidemantic_runtime_sql_dir_{suffix}"));
        std::fs::create_dir_all(&dir).unwrap();

        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: amount
      - name: order_count
        agg: count
"#;
        let sql = r#"
METRIC (
  name revenue_per_order,
  type ratio,
  numerator revenue,
  denominator order_count
);
"#;

        std::fs::write(dir.join("models.yml"), yaml).unwrap();
        std::fs::write(dir.join("metrics.sql"), sql).unwrap();

        let runtime = SidemanticRuntime::from_directory(&dir).unwrap();
        let errors = runtime.validate_query_references(
            &["revenue_per_order".to_string()],
            &["orders.status".to_string()],
        );
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_runtime_load_graph_from_directory_payload_helper_preserves_model_sources() {
        let mut dir = std::env::temp_dir();
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        dir.push(format!("sidemantic_runtime_dir_payload_{suffix}"));
        std::fs::create_dir_all(&dir).unwrap();

        std::fs::write(
            dir.join("orders.yml"),
            r#"
models:
  - name: orders
    table: orders
    primary_key: id
"#,
        )
        .unwrap();

        let payload_json = load_graph_from_directory(dir.to_string_lossy().as_ref()).unwrap();
        let payload: LoadedGraphPayload = serde_json::from_str(&payload_json).unwrap();
        let source = payload.model_sources.get("orders").unwrap();
        assert_eq!(source.source_format, "Sidemantic");
        assert_eq!(source.source_file.as_deref(), Some("orders.yml"));

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_runtime_parse_reference_and_join_path() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: order_date
        type: time
    relationships:
      - name: customers
        type: many_to_one
        foreign_key: customer_id
  - name: customers
    table: customers
    primary_key: id
"#;

        let runtime = SidemanticRuntime::from_yaml(yaml).unwrap();
        let (model, field, granularity) =
            runtime.parse_reference("orders.order_date__month").unwrap();
        assert_eq!(model, "orders");
        assert_eq!(field, "order_date");
        assert_eq!(granularity.as_deref(), Some("month"));

        let path = runtime.find_join_path("orders", "customers").unwrap();
        assert_eq!(path.steps.len(), 1);
        assert_eq!(path.steps[0].from_model, "orders");
        assert_eq!(path.steps[0].to_model, "customers");
    }

    #[test]
    fn test_runtime_loaded_graph_payload_preserves_metadata() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    metrics:
      - name: revenue
        agg: sum
        sql: amount
  - name: customers
    table: customers
    primary_key: customer_id
metrics:
  - name: total_revenue
    type: cumulative
    base_metric: revenue
"#;

        let runtime = SidemanticRuntime::from_yaml(yaml).unwrap();
        let payload = runtime.loaded_graph_payload();
        let names: Vec<String> = payload.models.into_iter().map(|model| model.name).collect();
        assert_eq!(names, vec!["orders".to_string(), "customers".to_string()]);
        assert_eq!(
            payload.model_order,
            vec!["orders".to_string(), "customers".to_string()]
        );
        assert_eq!(payload.top_level_metrics.len(), 1);
        assert_eq!(payload.top_level_metrics[0].name, "total_revenue");
        assert_eq!(
            payload.original_model_metrics.get("orders"),
            Some(&vec!["revenue".to_string()])
        );
    }

    #[test]
    fn test_runtime_generate_preaggregation_materialization_sql() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: order_date
        type: time
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: amount
    pre_aggregations:
      - name: daily_revenue
        time_dimension: order_date
        granularity: day
        dimensions: [status]
        measures: [revenue]
"#;

        let runtime = SidemanticRuntime::from_yaml(yaml).unwrap();
        let sql = runtime
            .generate_preaggregation_materialization_sql("orders", "daily_revenue")
            .unwrap();
        assert!(sql.contains("DATE_TRUNC('day', order_date) as order_date_day"));
        assert!(sql.contains("status as status"));
        assert!(sql.contains("SUM(amount) as revenue_raw"));
        assert!(sql.contains("FROM orders"));
        assert!(sql.contains("GROUP BY 1, 2"));
    }

    #[test]
    fn test_runtime_resolve_preaggregation_refresh_mode_defaults_and_validation() {
        let full = resolve_preaggregation_refresh_mode(None, false).unwrap();
        assert_eq!(full, "full");

        let incremental = resolve_preaggregation_refresh_mode(None, true).unwrap();
        assert_eq!(incremental, "incremental");

        let merge = resolve_preaggregation_refresh_mode(Some("merge"), false).unwrap();
        assert_eq!(merge, "merge");

        let err = resolve_preaggregation_refresh_mode(Some("bad"), false).unwrap_err();
        assert!(matches!(
            err,
            SidemanticError::Validation(message) if message == "Invalid refresh mode: bad"
        ));
    }

    #[test]
    fn test_runtime_validate_preaggregation_refresh_request_requirements() {
        validate_preaggregation_refresh_request("full", None, None).unwrap();
        validate_preaggregation_refresh_request("incremental", Some("order_date"), None).unwrap();
        validate_preaggregation_refresh_request("merge", Some("order_date"), None).unwrap();
        validate_preaggregation_refresh_request("engine", None, Some("snowflake")).unwrap();

        let incremental_err =
            validate_preaggregation_refresh_request("incremental", None, None).unwrap_err();
        assert!(matches!(
            incremental_err,
            SidemanticError::Validation(message)
                if message == "watermark_column required for incremental refresh"
        ));

        let merge_err = validate_preaggregation_refresh_request("merge", None, None).unwrap_err();
        assert!(matches!(
            merge_err,
            SidemanticError::Validation(message)
                if message == "watermark_column required for merge refresh"
        ));

        let engine_err = validate_preaggregation_refresh_request("engine", None, None).unwrap_err();
        assert!(matches!(
            engine_err,
            SidemanticError::Validation(message)
                if message == "dialect required for engine refresh mode"
        ));
    }

    #[test]
    fn test_runtime_plan_preaggregation_refresh_execution_defaults_and_flags() {
        let full = plan_preaggregation_refresh_execution(None, false, None, None).unwrap();
        assert_eq!(full.mode, "full");
        assert!(!full.requires_prior_watermark);
        assert!(!full.requires_merge_table_existence_check);
        assert!(!full.include_new_watermark);

        let incremental =
            plan_preaggregation_refresh_execution(None, true, Some("order_date"), None).unwrap();
        assert_eq!(incremental.mode, "incremental");
        assert!(incremental.requires_prior_watermark);
        assert!(!incremental.requires_merge_table_existence_check);
        assert!(incremental.include_new_watermark);

        let merge =
            plan_preaggregation_refresh_execution(Some("merge"), false, Some("order_date"), None)
                .unwrap();
        assert_eq!(merge.mode, "merge");
        assert!(merge.requires_prior_watermark);
        assert!(merge.requires_merge_table_existence_check);
        assert!(merge.include_new_watermark);

        let engine =
            plan_preaggregation_refresh_execution(Some("engine"), false, None, Some("snowflake"))
                .unwrap();
        assert_eq!(engine.mode, "engine");
        assert!(!engine.requires_prior_watermark);
        assert!(!engine.requires_merge_table_existence_check);
        assert!(!engine.include_new_watermark);
    }

    #[test]
    fn test_runtime_plan_preaggregation_refresh_execution_validation() {
        let invalid_mode =
            plan_preaggregation_refresh_execution(Some("bad"), false, None, None).unwrap_err();
        assert!(matches!(
            invalid_mode,
            SidemanticError::Validation(message) if message == "Invalid refresh mode: bad"
        ));

        let incremental_missing_watermark =
            plan_preaggregation_refresh_execution(Some("incremental"), false, None, None)
                .unwrap_err();
        assert!(matches!(
            incremental_missing_watermark,
            SidemanticError::Validation(message)
                if message == "watermark_column required for incremental refresh"
        ));

        let merge_missing_watermark =
            plan_preaggregation_refresh_execution(Some("merge"), false, None, None).unwrap_err();
        assert!(matches!(
            merge_missing_watermark,
            SidemanticError::Validation(message)
                if message == "watermark_column required for merge refresh"
        ));

        let engine_missing_dialect =
            plan_preaggregation_refresh_execution(Some("engine"), false, None, None).unwrap_err();
        assert!(matches!(
            engine_missing_dialect,
            SidemanticError::Validation(message)
                if message == "dialect required for engine refresh mode"
        ));
    }

    #[test]
    fn test_runtime_shape_preaggregation_refresh_result_semantics() {
        let full = shape_preaggregation_refresh_result("full", false, 42).unwrap();
        assert_eq!(full.mode, "full");
        assert_eq!(full.rows_inserted, 42);
        assert_eq!(full.rows_updated, 0);
        assert!(!full.include_new_watermark);

        let incremental = shape_preaggregation_refresh_result("incremental", false, 0).unwrap();
        assert_eq!(incremental.mode, "incremental");
        assert_eq!(incremental.rows_inserted, -1);
        assert_eq!(incremental.rows_updated, 0);
        assert!(incremental.include_new_watermark);

        let merge_existing = shape_preaggregation_refresh_result("merge", true, 0).unwrap();
        assert_eq!(merge_existing.mode, "merge");
        assert_eq!(merge_existing.rows_inserted, -1);
        assert_eq!(merge_existing.rows_updated, -1);
        assert!(merge_existing.include_new_watermark);

        let merge_new = shape_preaggregation_refresh_result("merge", false, 0).unwrap();
        assert_eq!(merge_new.mode, "merge");
        assert_eq!(merge_new.rows_inserted, -1);
        assert_eq!(merge_new.rows_updated, 0);
        assert!(merge_new.include_new_watermark);

        let engine = shape_preaggregation_refresh_result("engine", false, 0).unwrap();
        assert_eq!(engine.mode, "engine");
        assert_eq!(engine.rows_inserted, -1);
        assert_eq!(engine.rows_updated, -1);
        assert!(!engine.include_new_watermark);
    }

    #[test]
    fn test_runtime_shape_preaggregation_refresh_result_rejects_invalid_mode() {
        let err = shape_preaggregation_refresh_result("bad", false, 0).unwrap_err();
        assert!(matches!(
            err,
            SidemanticError::Validation(message) if message == "Invalid refresh mode: bad"
        ));
    }

    #[test]
    fn test_runtime_build_preaggregation_refresh_incremental_plan() {
        let statements = build_preaggregation_refresh_statements(
            "incremental",
            "orders_preagg_daily_revenue",
            "SELECT order_date, SUM(revenue) AS total_revenue FROM orders GROUP BY order_date",
            Some("order_date"),
            Some("2026-01-01"),
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(statements.len(), 2);
        assert!(statements[0].contains("CREATE TABLE IF NOT EXISTS orders_preagg_daily_revenue"));
        assert!(statements[1].contains("INSERT INTO orders_preagg_daily_revenue"));
        assert!(statements[1].contains("WHERE order_date >= '2026-01-01'"));
    }

    #[test]
    fn test_runtime_build_preaggregation_refresh_merge_plan_with_lookback() {
        let statements = build_preaggregation_refresh_statements(
            "merge",
            "orders_preagg_daily_revenue",
            "SELECT order_date, SUM(revenue) AS total_revenue FROM orders GROUP BY order_date",
            Some("order_date"),
            Some("2026-01-31"),
            Some("7 days"),
            None,
            None,
        )
        .unwrap();
        assert_eq!(statements.len(), 6);
        assert!(statements[2].contains("CREATE TABLE orders_preagg_daily_revenue__refresh_tmp AS"));
        assert!(statements[3]
            .contains("WHERE order_date >= (CAST('2026-01-31' AS TIMESTAMP) - INTERVAL '7 days')"));
    }

    #[test]
    fn test_runtime_build_preaggregation_refresh_incremental_plan_with_watermark_placeholder() {
        let statements = build_preaggregation_refresh_statements(
            "incremental",
            "orders_preagg_daily_revenue",
            "SELECT order_date, SUM(revenue) AS total_revenue FROM orders WHERE order_date > {WATERMARK} GROUP BY order_date",
            Some("order_date"),
            Some("2026-01-01"),
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(statements.len(), 2);
        assert!(!statements[0].contains("{WATERMARK}"));
        assert!(!statements[1].contains("{WATERMARK}"));
        assert!(statements[1].contains("WHERE order_date > '2026-01-01'"));
        assert!(!statements[1].contains("sidemantic_preagg_source WHERE order_date >="));
    }

    #[test]
    fn test_runtime_build_preaggregation_refresh_merge_plan_with_watermark_placeholder_and_lookback(
    ) {
        let statements = build_preaggregation_refresh_statements(
            "merge",
            "orders_preagg_daily_revenue",
            "SELECT order_date, SUM(revenue) AS total_revenue FROM orders WHERE order_date >= {WATERMARK} GROUP BY order_date",
            Some("order_date"),
            Some("2026-01-31"),
            Some("7 days"),
            None,
            None,
        )
        .unwrap();
        assert_eq!(statements.len(), 6);
        assert!(statements[2]
            .contains("WHERE order_date >= (CAST('2026-01-31' AS TIMESTAMP) - INTERVAL '7 days')"));
        assert!(statements[3]
            .contains("WHERE order_date >= (CAST('2026-01-31' AS TIMESTAMP) - INTERVAL '7 days')"));
    }

    #[test]
    fn test_runtime_build_preaggregation_refresh_engine_bigquery_plan() {
        let statements = build_preaggregation_refresh_statements(
            "engine",
            "orders_preagg_daily_revenue",
            "SELECT order_date, SUM(revenue) AS total_revenue FROM orders GROUP BY order_date",
            None,
            None,
            None,
            Some("bigquery"),
            Some("2 hours"),
        )
        .unwrap();
        assert_eq!(statements.len(), 1);
        assert!(statements[0]
            .contains("CREATE MATERIALIZED VIEW IF NOT EXISTS orders_preagg_daily_revenue"));
        assert!(statements[0].contains("refresh_interval_minutes = 120"));
    }

    #[test]
    fn test_runtime_validate_engine_refresh_sql_rejects_window_functions() {
        let (is_valid, error) = validate_engine_refresh_sql_compatibility(
            "SELECT status, ROW_NUMBER() OVER (PARTITION BY status) AS rn FROM orders",
            "snowflake",
        );

        assert!(!is_valid);
        assert!(error
            .as_deref()
            .unwrap_or_default()
            .contains("Window functions not supported in materialized views"));
    }

    #[test]
    fn test_runtime_validate_engine_refresh_sql_rejects_snowflake_scalar_subqueries() {
        let (is_valid, error) = validate_engine_refresh_sql_compatibility(
            "SELECT status, (SELECT MAX(amount) FROM orders) AS max_amount FROM orders",
            "snowflake",
        );

        assert!(!is_valid);
        assert!(error
            .as_deref()
            .unwrap_or_default()
            .contains("Scalar subqueries not fully supported in Snowflake DYNAMIC TABLES"));
    }

    #[test]
    fn test_runtime_validate_engine_refresh_sql_rejects_bigquery_unsupported_join_kind() {
        let (is_valid, error) = validate_engine_refresh_sql_compatibility(
            "SELECT o.status, SUM(o.revenue) FROM orders o CROSS JOIN users u GROUP BY o.status",
            "bigquery",
        );

        assert!(!is_valid);
        assert!(error
            .as_deref()
            .unwrap_or_default()
            .contains("BigQuery materialized views don't support CROSS joins"));
    }

    #[test]
    fn test_runtime_validate_engine_refresh_sql_accepts_simple_aggregation() {
        let (is_valid, error) = validate_engine_refresh_sql_compatibility(
            "SELECT status, SUM(revenue) AS total_revenue FROM orders GROUP BY status",
            "snowflake",
        );

        assert!(is_valid);
        assert!(error.is_none());
    }

    #[test]
    fn test_runtime_build_preaggregation_refresh_engine_rejects_invalid_interval() {
        let err = build_preaggregation_refresh_statements(
            "engine",
            "orders_preagg_daily_revenue",
            "SELECT 1",
            None,
            None,
            None,
            Some("bigquery"),
            Some("abc"),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            SidemanticError::Validation(message)
                if message.contains("invalid --refresh-every value")
        ));
    }

    #[test]
    fn test_runtime_find_relationship_path_with_yaml_handles_composite_keys() {
        let graph_yaml = r#"
models:
  - name: order_items
    primary_key_columns: [order_id, item_id]
    relationships: []
  - name: shipments
    primary_key_columns: [shipment_id]
    relationships:
      - name: order_items
        type: many_to_one
        foreign_key_columns: [order_id, item_id]
        primary_key_columns: [order_id, item_id]
        has_foreign_key: true
        has_primary_key: true
"#;

        let path =
            find_relationship_path_with_yaml(graph_yaml, "shipments", "order_items").unwrap();
        assert_eq!(path.len(), 1);
        assert_eq!(path[0].0, "shipments");
        assert_eq!(path[0].1, "order_items");
        assert_eq!(
            path[0].2,
            vec!["order_id".to_string(), "item_id".to_string()]
        );
        assert_eq!(
            path[0].3,
            vec!["order_id".to_string(), "item_id".to_string()]
        );
        assert_eq!(path[0].4, "many_to_one");
    }

    #[test]
    fn test_runtime_find_relationship_path_with_yaml_prefers_runtime_contract_for_simple_graph() {
        let graph_yaml = r#"
models:
  - name: customers
    table: customers
    primary_key: id
    relationships: []
  - name: orders
    table: orders
    primary_key: order_id
    relationships:
      - name: customers
        type: many_to_one
        foreign_key: customer_id
        primary_key: id
"#;

        let path = find_relationship_path_with_yaml(graph_yaml, "orders", "customers").unwrap();
        assert_eq!(path.len(), 1);
        assert_eq!(path[0].0, "orders");
        assert_eq!(path[0].1, "customers");
        assert_eq!(path[0].2, vec!["customer_id".to_string()]);
        assert_eq!(path[0].3, vec!["id".to_string()]);
        assert_eq!(path[0].4, "many_to_one");
    }

    #[test]
    fn test_runtime_find_relationship_path_with_yaml_falls_back_for_compact_payload_shape() {
        let graph_yaml = r#"
models:
  - name: customers
    primary_key_columns: [id]
    relationships: []
  - name: orders
    primary_key_columns: [order_id]
    relationships:
      - name: customers
        type: many_to_one
        foreign_key_columns: [customer_id]
        primary_key_columns: [id]
        has_foreign_key: true
        has_primary_key: true
"#;

        let path = find_relationship_path_with_yaml(graph_yaml, "orders", "customers").unwrap();
        assert_eq!(path.len(), 1);
        assert_eq!(path[0].0, "orders");
        assert_eq!(path[0].1, "customers");
        assert_eq!(path[0].2, vec!["customer_id".to_string()]);
        assert_eq!(path[0].3, vec!["id".to_string()]);
        assert_eq!(path[0].4, "many_to_one");
    }

    #[test]
    fn test_runtime_find_relationship_path_with_yaml_accepts_core_graph_payload_shape() {
        let graph_yaml = r#"
models:
  - name: order_items
    table: order_items
    primary_key: [order_id, item_id]
    relationships: []
  - name: shipments
    table: shipments
    primary_key: shipment_id
    relationships:
      - name: order_items
        type: many_to_one
        foreign_key: [order_id, item_id]
        primary_key: [order_id, item_id]
"#;

        let path =
            find_relationship_path_with_yaml(graph_yaml, "shipments", "order_items").unwrap();
        assert_eq!(path.len(), 1);
        assert_eq!(path[0].0, "shipments");
        assert_eq!(path[0].1, "order_items");
        assert_eq!(
            path[0].2,
            vec!["order_id".to_string(), "item_id".to_string()]
        );
        assert_eq!(
            path[0].3,
            vec!["order_id".to_string(), "item_id".to_string()]
        );
        assert_eq!(path[0].4, "many_to_one");
    }

    #[test]
    fn test_runtime_find_relationship_path_with_yaml_reports_missing_model() {
        let graph_yaml = r#"
models:
  - name: customers
    primary_key_columns: [id]
    relationships: []
"#;

        let err = find_relationship_path_with_yaml(graph_yaml, "orders", "customers").unwrap_err();
        assert_eq!(
            err,
            RelationshipPathError::ModelNotFound("orders".to_string())
        );
    }

    #[test]
    fn test_runtime_extract_metric_dependencies_from_yaml_with_context() {
        let models_yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    metrics:
      - name: revenue
        agg: sum
        sql: amount
      - name: cost
        agg: sum
        sql: cost
"#;
        let metric_yaml = r#"
name: margin
type: derived
sql: revenue / cost
"#;

        let deps =
            extract_metric_dependencies_from_yaml(metric_yaml, Some(models_yaml), Some("orders"))
                .unwrap();
        assert_eq!(
            deps,
            vec!["orders.cost".to_string(), "orders.revenue".to_string()]
        );
    }

    #[test]
    fn test_runtime_extract_metric_dependencies_from_yaml_skips_inline_aggregation() {
        let metric_yaml = r#"
name: computed_sum
sql: sum(amount)
"#;

        let deps = extract_metric_dependencies_from_yaml(metric_yaml, None, None).unwrap();
        assert!(deps.is_empty());
    }

    #[test]
    fn test_runtime_generate_catalog_metadata_includes_table_columns_and_fk() {
        let yaml = r#"
models:
  - name: customers
    table: customers
    primary_key: id
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: customer_id
        type: numeric
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: amount
    relationships:
      - name: customers
        type: many_to_one
        foreign_key: customer_id
"#;

        let runtime = SidemanticRuntime::from_yaml(yaml).unwrap();
        let json = runtime.generate_catalog_metadata("analytics").unwrap();
        let payload: serde_json::Value = serde_json::from_str(&json).unwrap();

        let tables = payload["tables"].as_array().unwrap();
        assert!(tables.iter().any(|table| table["table_name"] == "orders"));

        let columns = payload["columns"].as_array().unwrap();
        assert!(columns
            .iter()
            .any(|column| column["table_name"] == "orders" && column["column_name"] == "status"));
        assert!(columns
            .iter()
            .any(|column| column["table_name"] == "orders" && column["column_name"] == "revenue"));
        assert!(columns.iter().any(|column| {
            column["table_name"] == "orders"
                && column["column_name"] == "customer_id"
                && column["is_foreign_key"] == true
        }));

        let constraints = payload["constraints"].as_array().unwrap();
        assert!(constraints
            .iter()
            .any(|constraint| constraint["constraint_type"] == "FOREIGN KEY"));
    }

    #[test]
    fn test_runtime_generate_catalog_metadata_errors_on_missing_relationship_model() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: customer_id
        type: numeric
    relationships:
      - name: customers
        type: many_to_one
        foreign_key: customer_id
"#;

        let runtime = SidemanticRuntime::from_yaml(yaml).unwrap();
        let err = runtime.generate_catalog_metadata("public").unwrap_err();
        assert!(err
            .to_string()
            .contains("referenced model 'customers' not found while generating catalog metadata"));
    }

    #[test]
    fn test_runtime_generate_catalog_metadata_includes_composite_keys() {
        let yaml = r#"
models:
  - name: order_items
    table: order_items
    primary_key: [order_id, item_id]
  - name: shipments
    table: shipments
    primary_key: shipment_id
    dimensions:
      - name: order_id
        type: numeric
      - name: item_id
        type: numeric
      - name: status
        type: categorical
    relationships:
      - name: order_items
        type: many_to_one
        foreign_key: [order_id, item_id]
        primary_key: [order_id, item_id]
"#;

        let runtime = SidemanticRuntime::from_yaml(yaml).unwrap();
        let json = runtime.generate_catalog_metadata("analytics").unwrap();
        let payload: serde_json::Value = serde_json::from_str(&json).unwrap();

        let columns = payload["columns"].as_array().unwrap();
        let order_item_pk_columns: Vec<_> = columns
            .iter()
            .filter(|column| {
                column["table_name"] == "order_items" && column["is_primary_key"] == true
            })
            .collect();
        assert_eq!(order_item_pk_columns.len(), 2);

        let shipment_fk_columns: Vec<_> = columns
            .iter()
            .filter(|column| {
                column["table_name"] == "shipments" && column["is_foreign_key"] == true
            })
            .collect();
        assert_eq!(shipment_fk_columns.len(), 2);

        let key_usage = payload["key_column_usage"].as_array().unwrap();
        let shipment_fk_usage: Vec<_> = key_usage
            .iter()
            .filter(|row| {
                row["table_name"] == "shipments"
                    && row["referenced_table_name"] == "order_items"
                    && row["constraint_name"] == "shipments_order_id_item_id_fkey"
            })
            .collect();
        assert_eq!(shipment_fk_usage.len(), 2);
        assert!(shipment_fk_usage.iter().any(|row| {
            row["column_name"] == "order_id"
                && row["referenced_column_name"] == "order_id"
                && row["ordinal_position"] == 1
        }));
        assert!(shipment_fk_usage.iter().any(|row| {
            row["column_name"] == "item_id"
                && row["referenced_column_name"] == "item_id"
                && row["ordinal_position"] == 2
        }));
    }
}
