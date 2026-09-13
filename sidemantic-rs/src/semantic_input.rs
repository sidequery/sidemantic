//! Versioned, inert semantic handoff. This boundary never invokes a source adapter.
//!
//! Python declarations remain available in `source`; the executable graph is a
//! checked projection. Unsupported semantics fail before any SQL is generated.
use std::collections::HashMap;

use polyglot_sql::DialectType;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Map, Value};

use crate::core::{
    parse_semantic_expression, Dimension, Metric, Model, Parameter, PreAggregation, Relationship,
    Segment, SemanticGraph,
};
use crate::error::{Result, SidemanticError};
use crate::runtime::{
    interpolate_query_filters, validate_query_references, QueryValidationContext,
};
use crate::sql::{QueryRewriter, SemanticQuery, SqlGenerator};
mod policies;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    version: u32,
    input_dialect: String,
    models: Vec<Value>,
    #[serde(default)]
    metrics: Vec<Value>,
    #[serde(default)]
    metric_owners: HashMap<String, String>,
    #[serde(default)]
    parameters: Vec<Value>,
    #[serde(default)]
    metadata: Option<Value>,
    #[serde(default)]
    required_capabilities: Vec<String>,
    #[serde(default)]
    table_calculations: Vec<Value>,
    #[serde(default)]
    explores: Vec<Value>,
    #[serde(default)]
    saved_queries: Vec<Value>,
    #[serde(default)]
    import_warnings: Vec<Value>,
}

pub struct SemanticInput {
    pub graph: SemanticGraph,
    /// Original declarations, including metadata which has no executable core slot.
    pub source: Value,
    validation_context: QueryValidationContext,
    policies: HashMap<String, policies::ModelPolicies>,
}

fn invalid(path: &str, message: impl std::fmt::Display) -> SidemanticError {
    SidemanticError::validation_issue(
        "invalid_semantic_input",
        None,
        path,
        None,
        message.to_string(),
    )
}

fn unsupported(capability: impl Into<String>) -> SidemanticError {
    SidemanticError::UnsupportedSemanticFeatures {
        capabilities: vec![capability.into()],
    }
}

// polyglot's parser and recursive AST serialization need more stack than a
// default Rust test/host worker thread for nested expressions such as SUM(CASE).
// Keep the entire boundary on the same 16 MiB stack used by the SQL rewriter.
pub(crate) fn with_semantic_stack<T: Send>(
    operation: impl FnOnce() -> Result<T> + Send,
) -> Result<T> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        std::thread::scope(|scope| {
            std::thread::Builder::new()
                .stack_size(16 * 1024 * 1024)
                .spawn_scoped(scope, operation)
                .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?
                .join()
                .map_err(|_| {
                    SidemanticError::SqlGeneration("semantic compiler thread panicked".into())
                })?
        })
    }
    #[cfg(target_arch = "wasm32")]
    operation()
}

fn object(value: Value, path: &str) -> Result<Map<String, Value>> {
    value
        .as_object()
        .cloned()
        .ok_or_else(|| invalid(path, "expected an object"))
}

fn deserialize<T: DeserializeOwned>(value: Value, path: &str) -> Result<T> {
    serde_json::from_value(value).map_err(|error| invalid(path, error))
}

// These declarations describe presentation/provenance, not execution or authorization.
const DESCRIPTIVE_FIELDS: &[&str] = &[
    "owner",
    "domain",
    "category",
    "tags",
    "status",
    "certification",
    "deprecation",
    "freshness",
    "visibility",
    "synonyms",
    "sample_values",
    "cortex_search_service_name",
    "uri",
];

fn neutral(value: &Value) -> bool {
    value.is_null() || value == &json!(false) || value == &json!([])
}

/// Check fields against the actual serialized core type rather than maintaining
/// another copy of its schema. Python-only fields are handled explicitly first.
fn project<T: Serialize + DeserializeOwned>(
    mut raw: Map<String, Value>,
    exemplar: T,
    path: &str,
) -> Result<T> {
    let fields = serde_json::to_value(exemplar).map_err(|error| invalid(path, error))?;
    let fields = fields.as_object().expect("core objects serialize as maps");
    for field in DESCRIPTIVE_FIELDS {
        raw.remove(*field); // Preserved verbatim in SemanticInput::source.
    }
    for key in raw.keys() {
        if !fields.contains_key(key) {
            return Err(invalid(&format!("{path}.{key}"), "unknown semantic field"));
        }
    }
    deserialize(Value::Object(raw), path)
}

fn expression_language(raw: &mut Map<String, Value>, path: &str) -> Result<()> {
    if let Some(metadata) = raw.get("metadata").and_then(Value::as_object) {
        // A lowering target owns the executable SQL dialect; the source dialect
        // alongside it describes the original expression only.
        let dialect = metadata
            .get("ossie_target_dialect")
            .or_else(|| metadata.get("ossie_expression_dialect"));
        if let Some(dialect) = dialect {
            let dialect = dialect
                .as_str()
                .ok_or_else(|| invalid(path, "expression dialect must be a string"))?;
            if !dialect.eq_ignore_ascii_case("duckdb") {
                return Err(unsupported(format!("input_dialect.{dialect}")));
            }
        }
    }
    if let Some(value) = raw.remove("dax") {
        if !value.is_null() {
            return Err(unsupported(format!("{path}.dax")));
        }
    }
    if let Some(value) = raw.remove("expression_language") {
        if !value.is_null() && value != "sql" {
            return Err(unsupported(format!("{path}.expression_language")));
        }
    }
    Ok(())
}

fn reject_active(raw: &mut Map<String, Value>, field: &str, capability: &str) -> Result<()> {
    if let Some(value) = raw.remove(field) {
        if !neutral(&value) {
            return Err(unsupported(capability));
        }
    }
    Ok(())
}

fn key_columns(value: Value, path: &str) -> Result<Vec<String>> {
    match value {
        Value::Null => Ok(Vec::new()),
        Value::String(key) if !key.is_empty() => Ok(vec![key]),
        Value::Array(keys) => {
            let keys: Vec<String> = deserialize(Value::Array(keys), path)?;
            if keys.iter().any(String::is_empty) {
                return Err(invalid(path, "empty key column"));
            }
            Ok(keys)
        }
        _ => Err(invalid(
            path,
            "expected null, a key name, or an array of key names",
        )),
    }
}

fn decode_metric(value: Value, path: &str) -> Result<Metric> {
    let mut raw = object(value, path)?;
    expression_language(&mut raw, path)?;
    reject_active(&mut raw, "extends", "metric.inheritance")?;
    let complete = raw.remove("sql_is_complete").unwrap_or(json!(false));
    let complete: bool = deserialize(complete, &format!("{path}.sql_is_complete"))?;
    raw.insert("sql_is_complete".into(), json!(complete));
    if complete && raw.get("filters").is_some_and(|value| !neutral(value)) {
        return Err(unsupported("metric.complete_filters"));
    }
    if raw.get("type").is_none_or(Value::is_null) {
        let kind = if raw.get("agg").is_some_and(|value| !value.is_null()) && !complete {
            "simple"
        } else {
            "derived"
        };
        raw.insert("type".into(), json!(kind));
    }
    if complete && raw.get("agg").is_some_and(|value| !value.is_null()) {
        return Err(invalid(
            path,
            "sql_is_complete cannot also declare an aggregation",
        ));
    }
    if raw.get("filters").is_some_and(Value::is_null) {
        raw.remove("filters");
    }
    if let Some(kind) = raw.get("type").and_then(Value::as_str) {
        if ![
            "simple",
            "derived",
            "ratio",
            "cumulative",
            "time_comparison",
        ]
        .contains(&kind)
        {
            return Err(unsupported(format!("metric.{kind}")));
        }
    }
    if raw.get("agg") == Some(&json!("approx_count_distinct")) {
        return Err(unsupported("metric.approx_count_distinct"));
    }
    // These change aggregation grain or temporal semantics and are not promoted yet.
    for field in ["window_expression", "window_frame", "window_order"] {
        if raw.get(field).is_some_and(|value| !neutral(value)) {
            return Err(unsupported(format!("metric.{field}")));
        }
    }
    if let Some(fill) = raw.get("fill_nulls_with").filter(|value| !value.is_null()) {
        if !fill.is_number() && !fill.is_string() {
            return Err(invalid(path, "fill_nulls_with must be a number or string"));
        }
        let kind = raw.get("type").and_then(Value::as_str).unwrap_or("simple");
        if !matches!(kind, "simple" | "derived" | "ratio")
            || raw
                .get("offset_window")
                .is_some_and(|value| !neutral(value))
            || raw.get("window").is_some_and(|value| !neutral(value))
            || raw
                .get("grain_to_date")
                .is_some_and(|value| !neutral(value))
        {
            return Err(unsupported("metric.fill_nulls_shape"));
        }
    }
    // The optional provenance fields are omitted by core serialization when None.
    let mut exemplar = Metric::new("");
    exemplar.logical_data_type = Some(String::new());
    exemplar.sql_is_complete = true;
    let metric = project(raw, exemplar, path)?;
    if metric.non_additive_dimension.is_some() {
        if metric.r#type != crate::core::MetricType::Simple {
            return Err(unsupported("metric.non_additive_metric_shape"));
        }
        if !matches!(
            metric.non_additive_window.as_deref(),
            None | Some("min" | "max")
        ) {
            return Err(invalid(path, "non_additive_window must be min or max"));
        }
    }
    SqlGenerator::validate_temporal_metric(&metric)?;
    Ok(metric)
}

fn decode_relationship(value: Value, path: &str) -> Result<Relationship> {
    let mut raw = object(value, path)?;
    if raw.get("type") == Some(&json!("cross")) {
        return Err(unsupported("relationship.cross"));
    }
    if raw.get("type") == Some(&json!("many_to_many")) {
        return Err(unsupported("relationship.many_to_many"));
    }
    for field in ["foreign_key", "primary_key"] {
        if let Some(keys) = raw.remove(field) {
            let keys = key_columns(keys, &format!("{path}.{field}"))?;
            raw.insert(
                field.into(),
                keys.first().map_or(Value::Null, |key| json!(key)),
            );
            raw.insert(format!("{field}_columns"), json!(keys));
        }
    }
    let mut exemplar = Relationship::new("");
    exemplar.edge_id = Some(String::new());
    exemplar.target_model = Some(String::new());
    project(raw, exemplar, path)
}

fn decode_model(value: Value, path: &str) -> Result<Model> {
    let mut raw = object(value, path)?;
    expression_language(&mut raw, path)?;
    for (field, capability) in [
        ("schema_exposure", "model.schema_exposure"),
        ("auto_dimensions", "model.auto_dimensions"),
        ("extends", "model.inheritance"),
    ] {
        reject_active(&mut raw, field, capability)?;
    }
    // These declarations are retained and checked separately from core models.
    raw.remove("security");
    raw.remove("invariant_filters");
    if let Some(value) = raw.remove("pre_aggregations") {
        let values: Vec<Value> = deserialize(value, &format!("{path}.pre_aggregations"))?;
        let preaggregations = values
            .into_iter()
            .enumerate()
            .map(|(index, value)| {
                let path = format!("{path}.pre_aggregations[{index}]");
                let mut raw = object(value, &path)?;
                // Lambda freshness semantics have no executable core slot yet.
                // Do not discard active behavior, even on an otherwise plain rollup.
                reject_active(&mut raw, "rollups", "preaggregation.lambda")?;
                reject_active(&mut raw, "union_with_source_data", "preaggregation.lambda")?;
                let exemplar: PreAggregation = deserialize(json!({"name":""}), &path)?;
                project(raw, exemplar, &path)
            })
            .collect::<Result<Vec<PreAggregation>>>()?;
        raw.insert("pre_aggregations".into(), json!(preaggregations));
    }
    if raw.contains_key("primary_key_columns") {
        return Err(invalid(path, "use primary_key, not primary_key_columns"));
    }
    let keys = key_columns(
        raw.remove("primary_key").unwrap_or(Value::Null),
        &format!("{path}.primary_key"),
    )?;
    raw.insert(
        "primary_key".into(),
        json!(keys.first().cloned().unwrap_or_default()),
    );
    raw.insert("primary_key_columns".into(), json!(keys));
    if let Some(metrics) = raw.remove("metrics") {
        let metrics: Vec<Value> = deserialize(metrics, path)?;
        let metrics = metrics
            .into_iter()
            .enumerate()
            .map(|(i, value)| decode_metric(value, &format!("{path}.metrics[{i}]")))
            .collect::<Result<Vec<_>>>()?;
        raw.insert("metrics".into(), json!(metrics));
    }
    if let Some(dimensions) = raw.remove("dimensions") {
        let dimensions: Vec<Value> = deserialize(dimensions, path)?;
        let dimensions = dimensions
            .into_iter()
            .enumerate()
            .map(|(i, value)| {
                let path = format!("{path}.dimensions[{i}]");
                let mut raw = object(value, &path)?;
                expression_language(&mut raw, &path)?;
                let mut exemplar = Dimension::new("");
                exemplar.logical_data_type = Some(String::new());
                exemplar.declared_is_time = Some(false);
                project(raw, exemplar, &path)
            })
            .collect::<Result<Vec<_>>>()?;
        raw.insert("dimensions".into(), json!(dimensions));
    }
    if let Some(relationships) = raw.remove("relationships") {
        let relationships: Vec<Value> = deserialize(relationships, path)?;
        let relationships = relationships
            .into_iter()
            .enumerate()
            .map(|(i, value)| decode_relationship(value, &format!("{path}.relationships[{i}]")))
            .collect::<Result<Vec<_>>>()?;
        raw.insert("relationships".into(), json!(relationships));
    }
    if let Some(segments) = raw.remove("segments") {
        let segments: Vec<Value> = deserialize(segments, path)?;
        let segments = segments
            .into_iter()
            .enumerate()
            .map(|(i, value)| {
                let path = format!("{path}.segments[{i}]");
                project(object(value, &path)?, Segment::new("", ""), &path)
            })
            .collect::<Result<Vec<_>>>()?;
        raw.insert("segments".into(), json!(segments));
    }
    let mut model = project(raw, Model::new("", ""), path)?;
    let primary_keys = model.primary_keys();
    for dimension in &model.dimensions {
        if primary_keys.contains(&dimension.name) {
            if let Some(sql) = &dimension.sql {
                let expression = parse_semantic_expression(&sql.replace("{model}", &model.name))?;
                let identity = matches!(expression, polyglot_sql::Expression::Column(column)
                    if column.name.name == dimension.name
                        && column.table.as_ref().is_none_or(|table| table.name == model.name));
                if !identity {
                    // Source keys and computed dimension outputs currently share
                    // a projection name. Do not let key projection erase SQL.
                    return Err(unsupported("dimension.computed_primary_key"));
                }
            }
        }
    }
    for metric in &mut model.metrics {
        if metric.agg == Some(crate::core::Aggregation::CountDistinct)
            && metric
                .sql
                .as_deref()
                .is_none_or(|sql| sql.is_empty() || sql == "*")
        {
            if primary_keys.len() != 1 {
                return Err(unsupported("metric.count_distinct_primary_key"));
            }
            metric.sql = Some(primary_keys[0].clone());
        }
    }
    Ok(model)
}

fn validate_semantic_dependencies(graph: &SemanticGraph, graph_metrics: &[Metric]) -> Result<()> {
    let mut definitions = Vec::new();
    for model in graph.models() {
        for metric in &model.metrics {
            definitions.push((
                format!("{}.{}", model.name, metric.name),
                metric,
                Some(model.name.as_str()),
            ));
        }
    }
    for metric in graph_metrics {
        definitions.push((
            metric.name.clone(),
            metric,
            graph.metric_owner(&metric.name),
        ));
    }
    let mut dependencies: HashMap<String, Vec<String>> = HashMap::new();
    for (name, metric, context) in definitions {
        let mut metric_dependencies = Vec::new();
        for expression in [
            metric.sql.as_deref(),
            metric.numerator.as_deref(),
            metric.denominator.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            for column in crate::core::semantic_column_references(expression)? {
                let model_name = column.model.as_deref().or(context);
                if let Some(model_name) = model_name {
                    let model = graph
                        .get_model(model_name)
                        .ok_or_else(|| invalid(&name, format!("unknown model '{model_name}'")))?;
                    if column.aggregate_input
                        && model
                            .get_dimension(&column.field)
                            .is_some_and(|dimension| dimension.sql_expr() != column.field)
                    {
                        return Err(unsupported("metric.raw_computed_column"));
                    }
                }
                if metric.r#type == crate::core::MetricType::Simple || column.aggregate_input {
                    continue;
                }
                let dependency = if let Some(model_name) = &column.model {
                    let model = graph
                        .get_model(model_name)
                        .ok_or_else(|| invalid(&name, format!("unknown model '{model_name}'")))?;
                    if model.get_metric(&column.field).is_none() {
                        return Err(invalid(
                            &name,
                            format!("unknown metric '{}'", column.name()),
                        ));
                    }
                    column.name()
                } else if context.is_some_and(|model| {
                    graph
                        .get_model(model)
                        .is_some_and(|model| model.get_metric(&column.field).is_some())
                }) {
                    format!("{}.{}", context.unwrap(), column.field)
                } else if graph_metrics
                    .iter()
                    .any(|metric| metric.name == column.field)
                {
                    column.field.clone()
                } else {
                    let candidates: Vec<_> = graph
                        .models()
                        .filter(|model| model.get_metric(&column.field).is_some())
                        .collect();
                    if candidates.len() != 1 {
                        return Err(invalid(
                            &name,
                            format!(
                                "metric '{}' has {} possible definitions",
                                column.field,
                                candidates.len()
                            ),
                        ));
                    }
                    format!("{}.{}", candidates[0].name, column.field)
                };
                metric_dependencies.push(dependency);
            }
        }
        dependencies.insert(name, metric_dependencies);
    }
    fn visit(
        name: &str,
        dependencies: &HashMap<String, Vec<String>>,
        active: &mut std::collections::HashSet<String>,
        complete: &mut std::collections::HashSet<String>,
    ) -> Result<()> {
        if complete.contains(name) {
            return Ok(());
        }
        if !active.insert(name.to_string()) {
            return Err(SidemanticError::CircularDependency(name.to_string()));
        }
        if let Some(children) = dependencies.get(name) {
            for child in children {
                visit(child, dependencies, active, complete)?;
            }
        }
        active.remove(name);
        complete.insert(name.to_string());
        Ok(())
    }
    let mut active = std::collections::HashSet::new();
    let mut complete = std::collections::HashSet::new();
    for name in dependencies.keys() {
        visit(name, &dependencies, &mut active, &mut complete)?;
    }
    Ok(())
}

impl SemanticInput {
    pub fn from_json(input: &str) -> Result<Self> {
        with_semantic_stack(|| Self::decode(input))
    }

    fn decode(input: &str) -> Result<Self> {
        let source: Value = serde_json::from_str(input).map_err(|error| invalid("input", error))?;
        let envelope: Envelope = deserialize(source.clone(), "input")?;
        if envelope.version != 1 {
            return Err(invalid("version", "supported semantic input version is 1"));
        }
        if envelope.input_dialect != "duckdb" {
            return Err(unsupported(format!(
                "input_dialect.{}",
                envelope.input_dialect
            )));
        }
        let unsupported_capabilities: Vec<String> = envelope
            .required_capabilities
            .into_iter()
            .filter(|capability| {
                !matches!(
                    capability.as_str(),
                    "relationship.roles"
                        | "relationship.inactive"
                        | "model.security"
                        | "model.invariant_filters"
                        | "visibility"
                )
            })
            .collect();
        if !unsupported_capabilities.is_empty() {
            return Err(SidemanticError::UnsupportedSemanticFeatures {
                capabilities: unsupported_capabilities,
            });
        }
        for (name, values) in [
            ("table_calculations", &envelope.table_calculations),
            ("explores", &envelope.explores),
            ("saved_queries", &envelope.saved_queries),
        ] {
            if !values.is_empty() {
                return Err(unsupported(name));
            }
        }
        let _ = envelope.import_warnings; // Descriptive state remains in source.
        let policies = policies::decode(&envelope.models)?;
        let mut graph = SemanticGraph::new();
        let mut models = Vec::new();
        for (index, model) in envelope.models.into_iter().enumerate() {
            models.push(decode_model(model, &format!("models[{index}]"))?);
        }
        let keys: HashMap<String, Vec<String>> = models
            .iter()
            .map(|model| (model.name.clone(), model.primary_keys()))
            .collect();
        for mut model in models {
            for relationship in &mut model.relationships {
                if !relationship.active {
                    continue;
                }
                if !keys.contains_key(relationship.related_model()) {
                    return Err(invalid(
                        "relationships",
                        format!("unknown target {}", relationship.related_model()),
                    ));
                }
                if relationship.sql.is_some() {
                    continue;
                }
                let foreign = relationship.foreign_key_columns.clone().unwrap_or_default();
                if foreign.is_empty() {
                    return Err(invalid(
                        "relationships",
                        "keyed relationships require explicit foreign_key",
                    ));
                }
                let mut primary = relationship.primary_key_columns.clone().unwrap_or_default();
                if primary.is_empty() {
                    let primary_model = if relationship.r#type
                        == crate::core::RelationshipType::OneToMany
                        || (relationship.target_model.is_some()
                            && relationship.r#type == crate::core::RelationshipType::OneToOne)
                    {
                        &model.name
                    } else {
                        relationship.related_model()
                    };
                    primary = keys[primary_model].clone();
                }
                if primary.is_empty() {
                    return Err(unsupported("relationship.unknown_primary_key"));
                }
                if primary.len() != foreign.len() {
                    return Err(invalid("relationships", "join key arity mismatch"));
                }
                relationship.primary_key = primary.first().cloned();
                relationship.primary_key_columns = Some(primary);
            }
            graph.add_model(model)?;
        }
        let mut metrics = Vec::new();
        for (index, metric) in envelope.metrics.into_iter().enumerate() {
            let metric = decode_metric(metric, &format!("metrics[{index}]"))?;
            if metric.agg == Some(crate::core::Aggregation::CountDistinct)
                && metric
                    .sql
                    .as_deref()
                    .is_none_or(|sql| sql.is_empty() || sql == "*")
            {
                return Err(unsupported("metric.count_distinct_primary_key"));
            }
            graph.add_metric_unvalidated(metric.clone())?;
            metrics.push(metric);
        }
        graph.set_metric_scopes(envelope.metric_owners)?;
        validate_semantic_dependencies(&graph, &metrics)?;
        for (index, parameter) in envelope.parameters.into_iter().enumerate() {
            let path = format!("parameters[{index}]");
            let exemplar: Parameter = deserialize(json!({"name":"", "type":"string"}), &path)?;
            graph.add_parameter(project(object(parameter, &path)?, exemplar, &path)?)?;
        }
        if let Some(metadata) = envelope.metadata {
            graph.set_metadata(metadata);
        }
        Ok(Self {
            graph,
            source,
            validation_context: QueryValidationContext::from_top_level_metrics(&metrics),
            policies,
        })
    }
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct QueryInput {
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
    dialect: Option<String>,
    user_attributes: Option<Map<String, Value>>,
    #[serde(default)]
    enforce_visibility: bool,
}

fn query_input(query: &str) -> Result<QueryInput> {
    serde_json::from_str(query).map_err(|error| invalid("query", error))
}

pub fn compile_with_semantic_input(input_json: &str, query_json: &str) -> Result<String> {
    with_semantic_stack(|| compile_semantic_input(input_json, query_json))
}

fn compile_semantic_input(input_json: &str, query_json: &str) -> Result<String> {
    let input = SemanticInput::decode(input_json)?;
    let payload = query_input(query_json)?;
    let dialect = payload
        .dialect
        .as_deref()
        .unwrap_or("duckdb")
        .parse::<DialectType>()
        .map_err(|error| invalid("query.dialect", error))?;
    let filters =
        interpolate_query_filters(&input.graph, payload.filters, &payload.parameter_values)
            .map_err(|error| invalid("query.parameter_values", error))?;
    let mut query = SemanticQuery {
        metrics: payload.metrics,
        dimensions: payload.dimensions,
        filters,
        segments: payload.segments,
        order_by: payload.order_by,
        limit: payload.limit,
        offset: payload.offset,
        ungrouped: payload.ungrouped,
        use_preaggregations: payload.use_preaggregations,
        skip_default_time_dimensions: payload.skip_default_time_dimensions,
        preagg_database: payload.preagg_database,
        preagg_schema: payload.preagg_schema,
        ..SemanticQuery::default()
    };
    query.prepared_policies = policies::prepare(
        &input.graph,
        &input.policies,
        &query,
        payload.user_attributes.as_ref(),
        payload.enforce_visibility,
        dialect,
    )?;
    if query.prepared_policies.has_row_filters()
        || query
            .prepared_policies
            .invariant_filters
            .values()
            .any(|filters| !filters.is_empty())
    {
        query.use_preaggregations = false;
    }
    SqlGenerator::new(&input.graph)
        .with_dialect(dialect)
        .generate(&query)
}

pub fn validate_with_semantic_input(input_json: &str, query_json: &str) -> Result<Vec<String>> {
    with_semantic_stack(|| validate_semantic_input(input_json, query_json))
}

fn validate_semantic_input(input_json: &str, query_json: &str) -> Result<Vec<String>> {
    let input = SemanticInput::decode(input_json)?;
    let query = query_input(query_json)?;
    Ok(validate_query_references(
        &input.graph,
        &query.metrics,
        &query.dimensions,
        &input.validation_context,
    ))
}

pub fn rewrite_with_semantic_input(input_json: &str, sql: &str) -> Result<String> {
    rewrite_with_semantic_input_context(input_json, sql, "{}")
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RewriteContext {
    user_attributes: Option<Map<String, Value>>,
    #[serde(default)]
    enforce_visibility: bool,
}

pub fn rewrite_with_semantic_input_context(
    input_json: &str,
    sql: &str,
    context_json: &str,
) -> Result<String> {
    with_semantic_stack(|| {
        let input = SemanticInput::decode(input_json)?;
        let context: RewriteContext = serde_json::from_str(context_json)
            .map_err(|error| invalid("rewrite.context", error))?;
        let requires_policies = context.user_attributes.is_some()
            || context.enforce_visibility
            || input
                .policies
                .values()
                .any(|policy| policy.security.is_some() || !policy.invariant_filters.is_empty());
        let prepare = |query: &mut SemanticQuery| {
            query.prepared_policies = policies::prepare(
                &input.graph,
                &input.policies,
                query,
                context.user_attributes.as_ref(),
                context.enforce_visibility,
                DialectType::DuckDB,
            )?;
            Ok(())
        };
        let mut rewriter = QueryRewriter::new(&input.graph);
        if requires_policies {
            rewriter = rewriter.with_query_preparer(&prepare);
        }
        rewriter.rewrite_with_dialect(sql, DialectType::DuckDB)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input() -> Value {
        json!({
            "version": 1, "input_dialect": "duckdb", "models": [{
                "name": "orders", "table": "orders", "primary_key": null,
                "dimensions": [{"name": "status", "type": "categorical"}],
                "metrics": [{"name": "revenue", "agg": "sum", "sql": "amount"}]
            }], "metrics": [], "metric_owners": {}, "metadata": {"source": "orders.yml"}
        })
    }

    #[test]
    fn handoff_preserves_unknown_keys_and_original_declarations() {
        let source = input();
        let decoded = SemanticInput::from_json(&source.to_string()).unwrap();
        assert_eq!(decoded.source, source);
        assert!(decoded
            .graph
            .get_model("orders")
            .unwrap()
            .primary_keys()
            .is_empty());
        let sql =
            compile_with_semantic_input(&source.to_string(), r#"{"metrics":["orders.revenue"]}"#)
                .unwrap();
        assert!(sql.contains("SUM("));
        assert!(!sql.contains(".id"));
    }

    #[test]
    fn handoff_rejects_unknown_fields_versions_and_restrictions() {
        for (field, value) in [
            ("unexpected", json!(true)),
            ("primary_key_columns", json!(["id"])),
        ] {
            let mut source = input();
            source["models"][0][field] = value;
            assert!(matches!(
                SemanticInput::from_json(&source.to_string()),
                Err(SidemanticError::ValidationIssue { .. })
            ));
        }
        let mut source = input();
        source["models"][0]["schema_exposure"] = json!({});
        assert!(matches!(
            SemanticInput::from_json(&source.to_string()),
            Err(SidemanticError::UnsupportedSemanticFeatures { .. })
        ));
        let mut source = input();
        source["models"][0]["invariant_filters"] = json!(["tenant_id = 1"]);
        assert!(SemanticInput::from_json(&source.to_string()).is_ok());
        source = input();
        source["version"] = json!(2);
        assert!(matches!(
            SemanticInput::from_json(&source.to_string()),
            Err(SidemanticError::ValidationIssue { .. })
        ));
        source = input();
        source["models"][0]["dimensions"][0]["mystery"] = json!(1);
        assert!(SemanticInput::from_json(&source.to_string()).is_err());
    }

    #[test]
    fn handoff_rollups_are_checked_and_mandatory_filters_bypass_them() {
        let mut source = input();
        source["models"][0]["pre_aggregations"] = json!([{"name":"total", "measures":["revenue"], "rollups":null, "union_with_source_data":false}]);
        let query = r#"{"metrics":["orders.revenue"],"use_preaggregations":true}"#;
        let sql = compile_with_semantic_input(&source.to_string(), query).unwrap();
        assert!(sql.contains("orders_preagg_total"), "{sql}");
        source["models"][0]["invariant_filters"] = json!(["not deleted"]);
        let sql = compile_with_semantic_input(&source.to_string(), query).unwrap();
        assert!(!sql.contains("orders_preagg_total"), "{sql}");
        assert!(sql.contains("NOT deleted"), "{sql}");
        source["models"][0]["pre_aggregations"][0]["unexpected"] = json!(true);
        assert!(matches!(
            SemanticInput::from_json(&source.to_string()),
            Err(SidemanticError::ValidationIssue { .. })
        ));
        source["models"][0]["pre_aggregations"][0]
            .as_object_mut()
            .unwrap()
            .remove("unexpected");
        source["models"][0]["pre_aggregations"][0]["union_with_source_data"] = json!(true);
        assert!(matches!(
            SemanticInput::from_json(&source.to_string()),
            Err(SidemanticError::UnsupportedSemanticFeatures { .. })
        ));
    }

    #[test]
    fn handoff_policies_are_enforced_and_cannot_be_forged() {
        let mut source = input();
        source["models"][0]["security"] = json!({"row_filters":["tenant = {{ user.tenant }}"]});
        source["models"][0]["invariant_filters"] = json!(["not deleted"]);
        source["models"][0]["pre_aggregations"] = json!([{"name":"all_orders"}]);
        let input = source.to_string();
        assert!(matches!(
            compile_with_semantic_input(&input, r#"{"metrics":["orders.revenue"]}"#),
            Err(SidemanticError::Security(_))
        ));
        let sql = compile_with_semantic_input(&input, r#"{"metrics":["orders.revenue"],"user_attributes":{"tenant":1},"use_preaggregations":true}"#).unwrap();
        assert!(sql.contains("tenant = 1"), "{sql}");
        assert!(sql.contains("NOT deleted"), "{sql}");
        assert!(!sql.contains("all_orders"), "{sql}");
        assert!(compile_with_semantic_input(
            &input,
            r#"{"metrics":["orders.revenue"],"prepared_policies":{}}"#
        )
        .is_err());
        assert!(matches!(
            rewrite_with_semantic_input(&input, "select revenue from orders"),
            Err(SidemanticError::UnsupportedSemanticFeatures { .. })
        ));
    }

    #[test]
    fn rewrite_context_enforces_security_and_invariants() {
        let mut source = input();
        source["models"][0]["security"] = json!({"row_filters":["tenant = {{ user.tenant }}"]});
        source["models"][0]["invariant_filters"] = json!(["not deleted"]);
        let input = source.to_string();
        let sql = rewrite_with_semantic_input_context(
            &input,
            "select orders.revenue as amount from metrics order by amount desc limit 2 offset 1",
            r#"{"user_attributes":{"tenant":7}}"#,
        )
        .unwrap();
        assert!(sql.contains("tenant = 7"), "{sql}");
        assert!(sql.contains("NOT deleted"), "{sql}");
        assert!(sql.contains("AS amount"), "{sql}");
        assert!(sql.contains("LIMIT 2"), "{sql}");
        assert!(sql.contains("OFFSET 1"), "{sql}");
        assert!(matches!(
            rewrite_with_semantic_input(&input, "select orders.revenue from metrics"),
            Err(SidemanticError::Security(_))
        ));
    }

    #[test]
    fn rewrite_context_rejects_unsupported_shapes_and_forged_controls() {
        let input = input().to_string();
        for sql in [
            "select orders.revenue from orders",
            "select orders.revenue from metrics union all select orders.revenue from metrics",
            "with x as (select orders.revenue from metrics) select * from x",
            "select orders.revenue from metrics where orders.status in (select status from orders)",
            "select orders.revenue from metrics qualify 1 = 1",
            "select orders.revenue + 1 from metrics",
            "delete from orders",
        ] {
            assert!(
                matches!(
                    rewrite_with_semantic_input_context(&input, sql, r#"{"user_attributes":{}}"#),
                    Err(SidemanticError::UnsupportedSemanticFeatures { .. })
                ),
                "{sql}"
            );
        }
        for context in [
            r#"{"prepared_policies":{}}"#,
            r#"{"user_attributes":[]}"#,
            r#"{"enforce_visibility":"false"}"#,
        ] {
            assert!(rewrite_with_semantic_input_context(
                &input,
                "select orders.revenue from metrics",
                context
            )
            .is_err());
        }
    }

    #[test]
    fn rewrite_context_filters_require_declared_semantic_fields() {
        let mut source = input();
        source["models"][0]["invariant_filters"] = json!(["not deleted"]);
        let input = source.to_string();
        for clause in ["where", "having"] {
            for reference in [
                "orders.amount",
                "unknown.status",
                "status",
                "orders.missing",
            ] {
                let sql = format!("select orders.revenue from metrics {clause} {reference} = 1");
                assert!(
                    matches!(
                        rewrite_with_semantic_input_context(
                            &input,
                            &sql,
                            r#"{"user_attributes":{}}"#
                        ),
                        Err(SidemanticError::Validation(_))
                    ),
                    "{sql}"
                );
            }
        }
        // Physical columns from the metric and invariant remain valid model
        // declarations, while the user filter names a declared dimension.
        let sql = rewrite_with_semantic_input_context(
            &input,
            "select orders.revenue from metrics where orders.status = 'paid'",
            r#"{"user_attributes":{}}"#,
        )
        .unwrap();
        assert!(sql.contains("amount"), "{sql}");
        assert!(sql.contains("NOT deleted"), "{sql}");
        source["models"][0]["dimensions"][0]["public"] = json!(false);
        for clause in ["where", "having"] {
            let sql = format!("select orders.revenue from metrics {clause} orders.status = 'paid'");
            assert!(
                matches!(
                    rewrite_with_semantic_input_context(
                        &source.to_string(),
                        &sql,
                        r#"{"enforce_visibility":true}"#
                    ),
                    Err(SidemanticError::Security(_))
                ),
                "{sql}"
            );
        }
    }

    #[test]
    fn rewrite_context_visibility_and_legacy_compatibility() {
        let mut source = input();
        source["models"][0]["metrics"][0]["public"] = json!(false);
        let input = source.to_string();
        assert!(rewrite_with_semantic_input(&input, "select orders.revenue from orders").is_ok());
        assert!(rewrite_with_semantic_input_context(
            &input,
            "select orders.revenue from metrics",
            "{}"
        )
        .is_ok());
        assert!(matches!(
            rewrite_with_semantic_input_context(
                &input,
                "select orders.revenue from metrics",
                r#"{"enforce_visibility":true}"#,
            ),
            Err(SidemanticError::Security(_))
        ));
        assert!(matches!(
            rewrite_with_semantic_input_context(
                &input,
                "select orders.missing from metrics",
                r#"{"enforce_visibility":true}"#,
            ),
            Err(SidemanticError::Validation(_))
        ));
        source["models"][0]["metrics"][0]["public"] = json!(true);
        source["models"][0]["dimensions"][0]["public"] = json!(false);
        assert!(matches!(
            rewrite_with_semantic_input_context(
                &source.to_string(),
                "select orders.revenue from metrics order by orders.status",
                r#"{"enforce_visibility":true}"#,
            ),
            Err(SidemanticError::Security(_))
        ));
    }

    #[test]
    fn handoff_checks_required_capabilities_and_dialects() {
        let mut source = input();
        source["required_capabilities"] = json!(["future.capability"]);
        assert!(
            matches!(SemanticInput::from_json(&source.to_string()), Err(SidemanticError::UnsupportedSemanticFeatures { capabilities }) if capabilities == vec!["future.capability"])
        );
        source = input();
        source["input_dialect"] = json!("snowflake");
        assert!(matches!(
            SemanticInput::from_json(&source.to_string()),
            Err(SidemanticError::UnsupportedSemanticFeatures { .. })
        ));
    }

    #[test]
    fn handoff_keeps_graph_scope_without_inventing_an_owner() {
        let mut source = input();
        source["metrics"] = json!([{"name":"total", "agg":"sum", "sql":"amount"}]);
        let decoded = SemanticInput::from_json(&source.to_string()).unwrap();
        assert_eq!(decoded.graph.metric_owner("total"), None);
        assert!(matches!(
            compile_with_semantic_input(&source.to_string(), r#"{"metrics":["total"]}"#),
            Err(SidemanticError::UnsupportedSemanticFeatures { .. })
        ));
        source["metric_owners"] = json!({"total":"orders"});
        let sql =
            compile_with_semantic_input(&source.to_string(), r#"{"metrics":["total"]}"#).unwrap();
        assert!(sql.contains("SUM("));
        source["metric_owners"] = json!({"total":"missing"});
        assert!(SemanticInput::from_json(&source.to_string()).is_err());
    }

    #[test]
    fn handoff_binds_qualified_graph_dependencies_and_reports_invalid_queries() {
        let mut source = input();
        source["metrics"] =
            json!([{"name":"double_revenue", "type":"derived", "sql":"orders.revenue * 2"}]);
        let sql =
            compile_with_semantic_input(&source.to_string(), r#"{"metrics":["double_revenue"]}"#)
                .unwrap();
        assert!(sql.contains("* 2"));
        assert!(validate_with_semantic_input(
            &source.to_string(),
            r#"{"metrics":["double_revenue"]}"#
        )
        .unwrap()
        .is_empty());
        assert!(
            !validate_with_semantic_input(&source.to_string(), r#"{"metrics":["missing"]}"#)
                .unwrap()
                .is_empty()
        );
        assert!(compile_with_semantic_input(
            &source.to_string(),
            r#"{"metrics":["orders.missing"]}"#
        )
        .is_err());
    }

    #[test]
    fn handoff_preserves_complete_aggregate_expression() {
        let mut source = input();
        source["models"][0]["metrics"] = json!([{"name":"spread", "sql":"SUM(amount) / NULLIF(COUNT(*), 0)", "sql_is_complete":true}]);
        let decoded = SemanticInput::from_json(&source.to_string()).unwrap();
        let metric = decoded
            .graph
            .get_model("orders")
            .unwrap()
            .get_metric("spread")
            .unwrap();
        assert_eq!(
            metric.sql.as_deref(),
            Some("SUM(amount) / NULLIF(COUNT(*), 0)")
        );
        assert_eq!(metric.agg, None);
        let sql =
            compile_with_semantic_input(&source.to_string(), r#"{"metrics":["orders.spread"]}"#)
                .unwrap();
        assert!(sql.contains("NULLIF"));
    }

    #[test]
    fn handoff_relationships_preserve_composite_keys_and_roles() {
        let mut source = input();
        source["models"][0]["primary_key"] = json!(["tenant_id", "order_id"]);
        source["models"][0]["relationships"] = json!([{"name":"customers", "type":"many_to_one", "foreign_key":["tenant_id", "customer_id"]}]);
        source["models"].as_array_mut().unwrap().push(json!({"name":"customers", "table":"customers", "primary_key":["tenant_id", "customer_id"]}));
        let decoded = SemanticInput::from_json(&source.to_string()).unwrap();
        let relationship = &decoded.graph.get_model("orders").unwrap().relationships[0];
        assert_eq!(
            relationship.primary_key_columns(),
            vec!["tenant_id", "customer_id"]
        );
        source["models"][0]["relationships"][0]["name"] = json!("buyer");
        source["models"][0]["relationships"][0]["target_model"] = json!("customers");
        source["required_capabilities"] = json!(["relationship.roles"]);
        let decoded = SemanticInput::from_json(&source.to_string()).unwrap();
        let path = decoded.graph.find_join_path("orders", "buyer").unwrap();
        assert_eq!(path.steps[0].to_keys, vec!["tenant_id", "customer_id"]);
        assert_eq!(decoded.graph.get_model("buyer").unwrap().name, "customers");
    }

    #[test]
    fn handoff_compiles_two_roles_and_nested_paths_without_conflating_sources() {
        let mut source = input();
        source["models"][0]["primary_key"] = json!(["id"]);
        source["models"][0]["relationships"] = json!([
            {"name":"buyer", "target_model":"customers", "type":"many_to_one", "foreign_key":["buyer_id"]},
            {"name":"recipient", "target_model":"customers", "type":"many_to_one", "foreign_key":["recipient_id"]}
        ]);
        source["models"].as_array_mut().unwrap().extend([
            json!({"name":"customers", "table":"customers", "primary_key":["id"], "dimensions":[{"name":"name", "type":"categorical"}], "relationships":[{"name":"country", "target_model":"countries", "type":"many_to_one", "foreign_key":["country_id"]}]}),
            json!({"name":"countries", "table":"countries", "primary_key":["id"], "dimensions":[{"name":"label", "type":"categorical"}]})
        ]);
        source["required_capabilities"] = json!(["relationship.roles"]);
        let query = r#"{"metrics":["orders.revenue"],"dimensions":["buyer.name","recipient.name","buyer$country.label"],"use_preaggregations":false}"#;
        let sql = compile_with_semantic_input(&source.to_string(), query).unwrap();
        assert!(sql.contains("buyer_cte"), "{sql}");
        assert!(sql.contains("recipient_cte"), "{sql}");
        assert!(sql.contains("buyer$country_cte"), "{sql}");
        assert!(sql.contains("orders_cte.buyer_id"), "{sql}");
        assert!(sql.contains("orders_cte.recipient_id"), "{sql}");
        assert!(sql.contains("FROM orders_cte"), "{sql}");
    }

    #[test]
    fn handoff_preserves_inactive_relationships_without_making_them_queryable() {
        let mut source = input();
        source["models"][0]["relationships"] = json!([
            {"name":"buyer", "target_model":"customers", "type":"many_to_one", "foreign_key":["buyer_id"], "active":false}
        ]);
        source["required_capabilities"] = json!(["relationship.roles", "relationship.inactive"]);
        let decoded = SemanticInput::from_json(&source.to_string()).unwrap();
        assert!(!decoded.graph.get_model("orders").unwrap().relationships[0].active);
        assert!(decoded.graph.get_model("buyer").is_none());
        assert!(compile_with_semantic_input(
            &source.to_string(),
            r#"{"metrics":["orders.revenue"],"dimensions":["buyer.name"]}"#
        )
        .is_err());
    }

    #[test]
    fn handoff_rewrite_uses_declared_duckdb_input() {
        let source = input();
        let sql =
            rewrite_with_semantic_input(&source.to_string(), "select orders.revenue from orders")
                .unwrap();
        assert!(sql.contains("SUM("));
    }

    #[test]
    fn native_missing_null_and_empty_primary_keys_stay_unknown() {
        for declaration in ["", "    primary_key: null\n", "    primary_key: []\n"] {
            let yaml = format!("models:\n  - name: events\n    table: events\n{declaration}");
            let config: crate::config::schema::SidemanticConfig =
                serde_yaml::from_str(&yaml).unwrap();
            let models = config.into_models().unwrap();
            assert!(models[0].primary_keys().is_empty());
        }
    }

    #[test]
    fn count_distinct_uses_only_a_known_single_key() {
        let mut source = input();
        source["models"][0]["metrics"] = json!([{"name":"unique_orders", "agg":"count_distinct"}]);
        for key in [Value::Null, json!(["tenant_id", "order_id"])] {
            source["models"][0]["primary_key"] = key;
            assert!(matches!(
                SemanticInput::from_json(&source.to_string()),
                Err(SidemanticError::UnsupportedSemanticFeatures { .. })
            ));
        }
        source["models"][0]["primary_key"] = json!(["order_id"]);
        let sql = compile_with_semantic_input(
            &source.to_string(),
            r#"{"metrics":["orders.unique_orders"]}"#,
        )
        .unwrap();
        assert!(sql.contains("COUNT(DISTINCT"));
        assert!(sql.contains("order_id"));
        assert!(!sql.contains("CONCAT"));
    }

    #[test]
    fn expression_metadata_cannot_override_input_context() {
        let mut source = input();
        source["models"][0]["metrics"][0]["metadata"] = json!({"ossie_target_dialect":"BIGQUERY"});
        assert!(matches!(
            SemanticInput::from_json(&source.to_string()),
            Err(SidemanticError::UnsupportedSemanticFeatures { .. })
        ));
        source["models"][0]["metrics"][0]["metadata"] =
            json!({"ossie_target_dialect":"DUCKDB", "ossie_expression_dialect":"BIGQUERY"});
        assert!(SemanticInput::from_json(&source.to_string()).is_ok());
    }

    #[test]
    fn output_dialect_is_generated_by_rust() {
        let mut source = input();
        source["models"][0]["metrics"][0]["name"] = json!("order total");
        let sql = compile_with_semantic_input(
            &source.to_string(),
            r#"{"metrics":["orders.order total"],"dialect":"bigquery"}"#,
        )
        .unwrap();
        assert!(sql.contains("`"), "Generated BigQuery SQL:\n{sql}");
        assert!(
            sql.contains("AS `order total`"),
            "Generated BigQuery SQL:\n{sql}"
        );
        source["models"][0]["dimensions"] = json!([{"name":"created_at", "type":"time"}]);
        let sql = compile_with_semantic_input(
            &source.to_string(),
            r#"{"dimensions":["orders.created_at__month"],"dialect":"bigquery"}"#,
        )
        .unwrap();
        assert!(
            sql.contains("DATE_TRUNC(created_at, MONTH)"),
            "Generated BigQuery SQL:\n{sql}"
        );
        for dialect in ["bigquery", "mysql", "databricks", "postgres"] {
            let mut source = input();
            let name = "order `total\"";
            source["models"][0]["metrics"][0]["name"] = json!(name);
            let query = json!({"metrics":[format!("orders.{name}")], "dialect":dialect});
            let sql = compile_with_semantic_input(&source.to_string(), &query.to_string()).unwrap();
            let parsed = polyglot_sql::parse_one(&sql, dialect.parse().unwrap())
                .unwrap_or_else(|error| panic!("{dialect}: {error}\n{sql}"));
            let polyglot_sql::Expression::Select(select) = parsed else {
                panic!("expected SELECT")
            };
            let polyglot_sql::Expression::Alias(alias) = &select.expressions[0] else {
                panic!("expected alias")
            };
            assert_eq!(alias.alias.name, name, "Generated {dialect} SQL:\n{sql}");
        }
    }

    #[test]
    fn cross_model_graph_metric_requires_a_declared_join_path() {
        let mut source = input();
        source["models"].as_array_mut().unwrap().push(json!({"name":"refunds", "table":"refunds", "metrics":[{"name":"amount", "agg":"sum", "sql":"amount"}]}));
        source["metrics"] =
            json!([{"name":"net", "type":"derived", "sql":"orders.revenue - refunds.amount"}]);
        assert!(matches!(
            compile_with_semantic_input(&source.to_string(), r#"{"metrics":["net"]}"#),
            Err(SidemanticError::NoJoinPath { .. })
        ));
        source["metric_owners"] = json!({"net":"orders"});
        assert!(matches!(
            compile_with_semantic_input(&source.to_string(), r#"{"metrics":["net"]}"#),
            Err(SidemanticError::NoJoinPath { .. })
        ));
    }

    #[test]
    fn computed_primary_key_dimensions_cannot_be_silently_erased() {
        let mut source = input();
        source["models"][0]["primary_key"] = json!(["id"]);
        source["models"][0]["dimensions"] =
            json!([{"name":"id", "type":"numeric", "sql":"id * 10"}]);
        assert!(matches!(
            SemanticInput::from_json(&source.to_string()),
            Err(SidemanticError::UnsupportedSemanticFeatures { capabilities })
                if capabilities == vec!["dimension.computed_primary_key"]
        ));
        source["models"][0]["dimensions"][0]["sql"] = json!("\"orders\".\"id\"");
        assert!(SemanticInput::from_json(&source.to_string()).is_ok());
    }

    #[test]
    fn cyclic_metrics_are_invalid_not_unsupported() {
        let mut source = input();
        source["metrics"] = json!([
            {"name":"a", "type":"derived", "sql":"b * 2"},
            {"name":"b", "type":"derived", "sql":"a * 2"}
        ]);
        assert!(matches!(
            SemanticInput::from_json(&source.to_string()),
            Err(SidemanticError::CircularDependency(_))
        ));
    }

    #[test]
    fn sql_literals_and_comments_do_not_supply_graph_owners() {
        for expression in [
            "SUM(amount) + LENGTH('orders.foo')",
            "SUM(amount) /* orders.foo */",
        ] {
            let mut source = input();
            source["metrics"] = json!([{"name":"total", "sql":expression, "sql_is_complete":true}]);
            assert!(SemanticInput::from_json(&source.to_string()).is_ok());
            assert!(matches!(
                compile_with_semantic_input(&source.to_string(), r#"{"metrics":["total"]}"#),
                Err(SidemanticError::UnsupportedSemanticFeatures { .. })
            ));
        }
    }

    #[test]
    fn quoted_graph_references_bind_without_rewriting_literal_names() {
        let mut source = input();
        source["models"]
            .as_array_mut()
            .unwrap()
            .push(json!({"name":"refunds", "table":"refunds"}));
        source["metrics"] = json!([{"name":"total", "type":"derived", "sql":"\"orders\".\"revenue\" + LENGTH('total refunds.amount orders.revenue')"}]);
        let sql =
            compile_with_semantic_input(&source.to_string(), r#"{"metrics":["total"]}"#).unwrap();
        assert!(
            sql.contains("'total refunds.amount orders.revenue'"),
            "{sql}"
        );
        assert!(!sql.contains("JOIN"), "{sql}");
        assert!(sql.contains("SUM("), "{sql}");
    }

    #[test]
    fn complete_self_name_literal_is_not_a_cycle() {
        let mut source = input();
        source["metrics"] = json!([{"name":"total", "sql":"SUM(amount) + LENGTH('total')", "sql_is_complete":true}]);
        source["metric_owners"] = json!({"total":"orders"});
        let decoded = SemanticInput::from_json(&source.to_string()).unwrap();
        assert!(decoded.graph.get_metric("total").unwrap().sql_is_complete);
        let sql =
            compile_with_semantic_input(&source.to_string(), r#"{"metrics":["total"]}"#).unwrap();
        assert!(sql.contains("'total'"), "{sql}");
    }

    #[test]
    fn null_fill_literals_are_checked_and_other_restrictions_remain_explicit() {
        let mut source = input();
        source["models"][0]["metrics"][0]["fill_nulls_with"] = json!(0);
        assert!(SemanticInput::from_json(&source.to_string()).is_ok());
        source["models"][0]["metrics"][0]["fill_nulls_with"] = json!({"sql":"unsafe"});
        assert!(matches!(
            SemanticInput::from_json(&source.to_string()),
            Err(SidemanticError::ValidationIssue { .. })
        ));
        source = input();
        source["models"][0]["metrics"] = json!([{"name":"paid", "sql":"SUM(amount)", "sql_is_complete":true, "filters":["status = 'paid'"]}]);
        assert!(
            matches!(SemanticInput::from_json(&source.to_string()), Err(SidemanticError::UnsupportedSemanticFeatures { capabilities }) if capabilities == vec!["metric.complete_filters"])
        );
        source["models"][0]["metrics"][0]
            .as_object_mut()
            .unwrap()
            .remove("filters");
        source["models"][0]["dimensions"] =
            json!([{"name":"amount", "type":"numeric", "sql":"amount * 10"}]);
        let error = SemanticInput::from_json(&source.to_string()).err();
        assert!(
            matches!(&error, Some(SidemanticError::UnsupportedSemanticFeatures { capabilities }) if capabilities == &vec!["metric.raw_computed_column"]),
            "{error:?}"
        );
    }

    #[test]
    fn local_metric_dependencies_win_over_other_models_same_named_metrics() {
        let mut source = input();
        source["models"][0]["metrics"].as_array_mut().unwrap().extend([
            json!({"name":"count", "agg":"count"}),
            json!({"name":"average", "type":"ratio", "numerator":"revenue", "denominator":"count"}),
        ]);
        source["models"].as_array_mut().unwrap().push(json!({"name":"refunds", "table":"refunds", "metrics":[{"name":"count", "agg":"count"}]}));
        let sql =
            compile_with_semantic_input(&source.to_string(), r#"{"metrics":["orders.average"]}"#)
                .unwrap();
        assert!(!sql.contains("refunds"), "{sql}");
        assert!(sql.contains("NULLIF"), "{sql}");
    }

    #[test]
    fn typed_aggregate_and_scalar_ast_children_supply_real_columns() {
        let columns = crate::core::semantic_column_references(
            "SUM(orders.amount) + LENGTH(orders.label) + LENGTH('other.fake')",
        )
        .unwrap();
        assert_eq!(columns.len(), 2, "{columns:?}");
        assert!(columns
            .iter()
            .any(|column| column.model.as_deref() == Some("orders")
                && column.field == "amount"
                && column.aggregate_input));
        assert!(columns
            .iter()
            .any(|column| column.model.as_deref() == Some("orders")
                && column.field == "label"
                && !column.aggregate_input));
    }

    #[test]
    fn qualified_complete_aggregate_rewrites_all_typed_children() {
        let mut source = input();
        source["metrics"] = json!([{"name":"large_revenue", "sql":"SUM(CASE WHEN orders.amount >= 100 THEN orders.amount ELSE 0 END)", "sql_is_complete":true}]);
        let sql =
            compile_with_semantic_input(&source.to_string(), r#"{"metrics":["large_revenue"]}"#)
                .unwrap();
        assert!(sql.contains("orders_cte.amount"), "{sql}");
        assert!(!sql.contains("orders.amount"), "{sql}");
    }
}
