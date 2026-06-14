//! OSI (Open Semantic Interchange) adapter.
//!
//! OSI is a vendor-agnostic semantic model specification. This adapter imports
//! and exports OSI YAML at parity with the Python `OSIAdapter`:
//!
//! - OSI `semantic_model` / `ontology_mappings[].semantic_model` → graph
//! - OSI `datasets` → models
//! - OSI `fields` → dimensions
//! - OSI `metrics` → graph-level metrics
//! - OSI `relationships` → many-to-one relationships
//!
//! Semantic-model-level metadata (name, description, ai_context,
//! custom_extensions, ontology) round-trips through the graph metadata payload.
//!
//! Spec: <https://github.com/open-semantic-interchange/OSI>

use std::collections::HashSet;

use polyglot_sql::{DialectType, Expression};
use serde_json::{Map as JsonMap, Value as Json};
use serde_yaml::{Mapping as YamlMap, Value as Yaml};

use crate::config::schema::metric_from_sql_expression;
use crate::core::{
    Aggregation, Dimension, DimensionType, Metric, MetricType, Model, Relationship,
    RelationshipType, SemanticGraph,
};
use crate::error::{Result, SidemanticError};

use super::{Adapter, ParsedDocument};

/// OSI spec version emitted on export.
pub const OSI_VERSION: &str = "0.2.0.dev0";

/// Dialect preference order for extracting SQL expressions on import.
const DIALECT_PREFERENCE: &[&str] = &[
    "ANSI_SQL",
    "SNOWFLAKE",
    "DATABRICKS",
    "MAQL",
    "TABLEAU",
    "MDX",
];

/// Dialects we can safely emit (and transpile to) on export.
const SUPPORTED_EXPORT_DIALECTS: &[&str] = &["ANSI_SQL", "SNOWFLAKE", "DATABRICKS"];

/// Adapter for importing/exporting OSI (Open Semantic Interchange) YAML.
#[derive(Debug, Clone)]
pub struct OsiAdapter {
    dialects: Vec<String>,
}

impl Default for OsiAdapter {
    fn default() -> Self {
        Self {
            dialects: vec!["ANSI_SQL".to_string()],
        }
    }
}

impl OsiAdapter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the SQL dialects emitted on export (default `["ANSI_SQL"]`).
    pub fn with_dialects(mut self, dialects: Vec<String>) -> Self {
        self.dialects = dialects;
        self
    }

    /// Parse a single OSI document into a complete semantic graph.
    ///
    /// Convenience wrapper used by tests and the OSI adapter API; the config
    /// loader uses [`Adapter::parse_document`] so it can merge multiple files.
    pub fn parse_str(&self, content: &str) -> Result<SemanticGraph> {
        let doc = self.parse_document(content)?;
        let mut graph = SemanticGraph::new();
        for model in doc.models {
            graph.add_model(model)?;
        }
        // Register all graph-level metrics before validating, so a derived/ratio
        // metric declared before the metrics it references still loads.
        for metric in &doc.graph_metrics {
            if graph.get_metric(&metric.name).is_none() {
                graph.add_metric_unvalidated(metric.clone())?;
            }
        }
        for metric in &doc.graph_metrics {
            graph.validate_metric_dependencies(metric)?;
        }
        if let Some(metadata) = doc.metadata {
            graph.set_metadata(metadata);
        }
        Ok(graph)
    }
}

impl Adapter for OsiAdapter {
    fn parse_document(&self, content: &str) -> Result<ParsedDocument> {
        let mut doc = ParsedDocument {
            explicit_relationships: true,
            ..Default::default()
        };

        if content.trim().is_empty() {
            return Ok(doc);
        }

        let data: Json = serde_yaml::from_str(content)
            .map_err(|e| SidemanticError::Validation(format!("OSI YAML parse error: {e}")))?;
        let Some(root) = data.as_object() else {
            return Ok(doc);
        };

        let mut osi_meta = JsonMap::new();
        osi_meta.insert("semantic_models".to_string(), Json::Array(Vec::new()));
        if let Some(version) = root.get("version") {
            if !version.is_null() {
                osi_meta.insert("version".to_string(), version.clone());
            }
        }
        if let Some(ontology) = root.get("ontology") {
            if !ontology.is_null() {
                osi_meta.insert("ontology".to_string(), ontology.clone());
            }
        }

        for (sm_def, source, mapping_def) in iter_semantic_models(root) {
            remember_semantic_model_metadata(&mut osi_meta, sm_def, source, mapping_def);
            parse_semantic_model(sm_def, &mut doc)?;
        }

        let mut graph_metadata = JsonMap::new();
        graph_metadata.insert("osi".to_string(), Json::Object(osi_meta));
        doc.metadata = Some(Json::Object(graph_metadata));

        Ok(doc)
    }

    fn export_string(&self, graph: &SemanticGraph) -> Result<String> {
        let dialects = if self.dialects.is_empty() {
            vec!["ANSI_SQL".to_string()]
        } else {
            self.dialects.clone()
        };
        let unsupported: Vec<&str> = dialects
            .iter()
            .map(String::as_str)
            .filter(|d| !SUPPORTED_EXPORT_DIALECTS.contains(d))
            .collect();
        if !unsupported.is_empty() {
            return Err(SidemanticError::Validation(format!(
                "Unsupported OSI export dialect(s): {}. Supported: {}",
                unsupported.join(", "),
                SUPPORTED_EXPORT_DIALECTS.join(", ")
            )));
        }

        // Deterministic model order for stable output.
        let mut models: Vec<&Model> = graph.models().collect();
        models.sort_by(|a, b| a.name.cmp(&b.name));

        let semantic_model = export_semantic_model(&models, graph, &dialects);

        let mut root = YamlMap::new();
        put(&mut root, "version", Yaml::String(export_version(graph)));
        put(
            &mut root,
            "semantic_model",
            Yaml::Sequence(vec![semantic_model]),
        );

        serde_yaml::to_string(&Yaml::Mapping(root))
            .map_err(|e| SidemanticError::Validation(format!("OSI YAML serialize error: {e}")))
    }
}

// =============================================================================
// Import helpers
// =============================================================================

/// Top-level and ontology-mapped semantic model definitions.
fn iter_semantic_models(root: &JsonMap<String, Json>) -> Vec<(&Json, String, Option<&Json>)> {
    let mut result: Vec<(&Json, String, Option<&Json>)> = Vec::new();

    match root.get("semantic_model") {
        Some(Json::Array(items)) => {
            for item in items {
                if item.is_object() {
                    result.push((item, "semantic_model".to_string(), None));
                }
            }
        }
        Some(sm @ Json::Object(_)) => {
            result.push((sm, "semantic_model".to_string(), None));
        }
        _ => {}
    }

    if let Some(Json::Array(mappings)) = root.get("ontology_mappings") {
        for (index, mapping_def) in mappings.iter().enumerate() {
            let Some(mapping) = mapping_def.as_object() else {
                continue;
            };
            if let Some(sm_def) = mapping.get("semantic_model") {
                if sm_def.is_object() {
                    result.push((
                        sm_def,
                        format!("ontology_mappings[{index}].semantic_model"),
                        Some(mapping_def),
                    ));
                }
            }
        }
    }

    result
}

/// Preserve semantic-model-level OSI fields that do not map to models.
fn remember_semantic_model_metadata(
    osi_meta: &mut JsonMap<String, Json>,
    sm_def: &Json,
    source: String,
    mapping_def: Option<&Json>,
) {
    let Some(sm) = sm_def.as_object() else {
        return;
    };

    let mut sm_meta = JsonMap::new();
    sm_meta.insert("source".to_string(), Json::String(source));
    for key in ["name", "description", "ai_context", "custom_extensions"] {
        if let Some(value) = sm.get(key) {
            sm_meta.insert(key.to_string(), value.clone());
        }
    }

    if let Some(mapping) = mapping_def.and_then(Json::as_object) {
        let mut mapping_meta = JsonMap::new();
        for key in ["name", "description", "concept_mappings"] {
            if let Some(value) = mapping.get(key) {
                mapping_meta.insert(key.to_string(), value.clone());
            }
        }
        if !mapping_meta.is_empty() {
            sm_meta.insert("ontology_mapping".to_string(), Json::Object(mapping_meta));
        }
    }

    if let Some(Json::Array(list)) = osi_meta.get_mut("semantic_models") {
        list.push(Json::Object(sm_meta));
    }
}

fn parse_semantic_model(sm_def: &Json, doc: &mut ParsedDocument) -> Result<()> {
    let Some(sm) = sm_def.as_object() else {
        return Ok(());
    };

    if let Some(Json::Array(datasets)) = sm.get("datasets") {
        for dataset_def in datasets {
            if let Some(model) = parse_dataset(dataset_def) {
                doc.models.push(model);
            }
        }
    }

    if let Some(Json::Array(relationships)) = sm.get("relationships") {
        for rel_def in relationships {
            add_relationship_to_model(rel_def, &mut doc.models);
        }
    }

    if let Some(Json::Array(metrics)) = sm.get("metrics") {
        for metric_def in metrics {
            if let Some(metric) = parse_metric(metric_def) {
                doc.graph_metrics.push(metric);
            }
        }
    }

    Ok(())
}

fn parse_dataset(dataset_def: &Json) -> Option<Model> {
    let dataset = dataset_def.as_object()?;
    let name = dataset.get("name").and_then(Json::as_str)?;
    if name.is_empty() {
        return None;
    }

    let source = dataset
        .get("source")
        .and_then(Json::as_str)
        .map(String::from);

    let primary_key_columns = string_list(dataset.get("primary_key"));
    let (primary_key, primary_key_columns) = if primary_key_columns.is_empty() {
        ("id".to_string(), vec!["id".to_string()])
    } else {
        (primary_key_columns[0].clone(), primary_key_columns)
    };

    let unique_keys = dataset
        .get("unique_keys")
        .and_then(Json::as_array)
        .map(|rows| rows.iter().map(|row| string_list(Some(row))).collect());

    let mut dimensions = Vec::new();
    if let Some(Json::Array(fields)) = dataset.get("fields") {
        for field_def in fields {
            if let Some(dim) = parse_field(field_def) {
                dimensions.push(dim);
            }
        }
    }

    let default_time_dimension = dimensions
        .iter()
        .find(|dim| dim.r#type == DimensionType::Time)
        .map(|dim| dim.name.clone());

    let mut model = Model::new(name, primary_key);
    model.table = source;
    model.description = dataset
        .get("description")
        .and_then(Json::as_str)
        .map(String::from);
    model.primary_key_columns = primary_key_columns;
    model.unique_keys = unique_keys;
    model.dimensions = dimensions;
    model.default_time_dimension = default_time_dimension;
    model.meta = build_meta(dataset);
    Some(model)
}

fn parse_field(field_def: &Json) -> Option<Dimension> {
    let field = field_def.as_object()?;
    let name = field.get("name").and_then(Json::as_str)?;
    if name.is_empty() {
        return None;
    }

    let sql = extract_expression(field.get("expression"));

    let is_time = field
        .get("dimension")
        .and_then(Json::as_object)
        .and_then(|dim| dim.get("is_time"))
        .and_then(Json::as_bool)
        .unwrap_or(false);

    Some(Dimension {
        name: name.to_string(),
        r#type: if is_time {
            DimensionType::Time
        } else {
            DimensionType::Categorical
        },
        sql,
        granularity: if is_time {
            Some("day".to_string())
        } else {
            None
        },
        supported_granularities: None,
        label: field.get("label").and_then(Json::as_str).map(String::from),
        description: field
            .get("description")
            .and_then(Json::as_str)
            .map(String::from),
        metadata: None,
        meta: build_meta(field),
        format: None,
        value_format_name: None,
        parent: None,
        window: None,
        public: true,
    })
}

fn parse_metric(metric_def: &Json) -> Option<Metric> {
    let metric = metric_def.as_object()?;
    let name = metric.get("name").and_then(Json::as_str)?;
    if name.is_empty() {
        return None;
    }

    let expression = extract_expression(metric.get("expression"))?;
    if expression.is_empty() {
        return None;
    }

    let description = metric
        .get("description")
        .and_then(Json::as_str)
        .map(String::from);
    Some(metric_from_sql_expression(
        name.to_string(),
        Some(expression),
        description,
        build_meta(metric),
    ))
}

/// Extract a SQL expression from an OSI `expression` definition, preferring
/// ANSI_SQL and falling back through the dialect preference order.
fn extract_expression(expression_def: Option<&Json>) -> Option<String> {
    let dialects = expression_def?.as_object()?.get("dialects")?.as_array()?;

    let mut dialect_map: Vec<(String, String)> = Vec::new();
    for entry in dialects {
        if let Some(obj) = entry.as_object() {
            let name = obj.get("dialect").and_then(Json::as_str);
            let expr = obj.get("expression").and_then(Json::as_str);
            if let (Some(name), Some(expr)) = (name, expr) {
                dialect_map.push((name.to_string(), expr.to_string()));
            }
        }
    }

    for preferred in DIALECT_PREFERENCE {
        if let Some((_, expr)) = dialect_map.iter().find(|(name, _)| name == preferred) {
            return Some(expr.clone());
        }
    }

    dialects
        .first()
        .and_then(Json::as_object)
        .and_then(|obj| obj.get("expression"))
        .and_then(Json::as_str)
        .map(String::from)
}

fn add_relationship_to_model(rel_def: &Json, models: &mut [Model]) {
    let Some(rel) = rel_def.as_object() else {
        return;
    };
    let Some(from_model) = rel.get("from").and_then(Json::as_str) else {
        return;
    };
    let Some(to_model) = rel.get("to").and_then(Json::as_str) else {
        return;
    };
    let Some(model) = models.iter_mut().find(|m| m.name == from_model) else {
        return;
    };

    let from_columns = string_list(rel.get("from_columns"));
    let to_columns = string_list(rel.get("to_columns"));

    let foreign_key_columns = if from_columns.is_empty() {
        vec![format!("{to_model}_id")]
    } else {
        from_columns
    };
    let primary_key_columns = if to_columns.is_empty() {
        vec!["id".to_string()]
    } else {
        to_columns
    };

    let mut metadata = JsonMap::new();
    if let Some(name) = rel.get("name").and_then(Json::as_str) {
        metadata.insert("osi_name".to_string(), Json::String(name.to_string()));
    }
    if rel.contains_key("ai_context") {
        metadata.insert(
            "ai_context".to_string(),
            rel.get("ai_context").cloned().unwrap_or(Json::Null),
        );
    }
    if let Some(custom) = decode_custom_extensions(rel.get("custom_extensions")) {
        metadata.insert("custom_extensions".to_string(), custom);
    }

    let relationship = Relationship {
        name: to_model.to_string(),
        r#type: RelationshipType::ManyToOne,
        foreign_key: foreign_key_columns.first().cloned(),
        foreign_key_columns: Some(foreign_key_columns),
        primary_key: primary_key_columns.first().cloned(),
        primary_key_columns: Some(primary_key_columns),
        through: None,
        through_foreign_key: None,
        through_foreign_key_columns: None,
        related_foreign_key: None,
        related_foreign_key_columns: None,
        sql: None,
        metadata: if metadata.is_empty() {
            None
        } else {
            Some(Json::Object(metadata))
        },
    };

    model.relationships.push(relationship);
}

/// Build the `meta` payload from `ai_context` and `custom_extensions` keys.
fn build_meta(obj: &JsonMap<String, Json>) -> Option<Json> {
    let has_ai_context = obj.contains_key("ai_context");
    let custom = decode_custom_extensions(obj.get("custom_extensions"));
    if !has_ai_context && custom.is_none() {
        return None;
    }

    let mut meta = JsonMap::new();
    if has_ai_context {
        meta.insert(
            "ai_context".to_string(),
            obj.get("ai_context").cloned().unwrap_or(Json::Null),
        );
    }
    if let Some(custom) = custom {
        meta.insert("custom_extensions".to_string(), custom);
    }
    Some(Json::Object(meta))
}

/// Decode a Sidemantic-owned extension wrapper while preserving standard lists.
fn decode_custom_extensions(value: Option<&Json>) -> Option<Json> {
    let value = value?;
    if value.is_null() {
        return None;
    }

    if let Json::Array(items) = value {
        if items.len() == 1 {
            if let Some(obj) = items[0].as_object() {
                if obj.get("vendor_name").and_then(Json::as_str) == Some("SIDEMANTIC") {
                    return match obj.get("data") {
                        Some(Json::String(data)) => Some(
                            serde_json::from_str::<Json>(data)
                                .unwrap_or_else(|_| Json::String(data.clone())),
                        ),
                        other => other.cloned(),
                    };
                }
            }
        }
    }

    Some(value.clone())
}

fn string_list(value: Option<&Json>) -> Vec<String> {
    value
        .and_then(Json::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

// =============================================================================
// Export helpers
// =============================================================================

fn export_version(graph: &SemanticGraph) -> String {
    let stored = graph
        .metadata()
        .and_then(|m| m.get("osi"))
        .and_then(|osi| osi.get("version"))
        .and_then(Json::as_str);
    match stored {
        Some(version) if version == OSI_VERSION => version.to_string(),
        _ => OSI_VERSION.to_string(),
    }
}

fn export_semantic_model(models: &[&Model], graph: &SemanticGraph, dialects: &[String]) -> Yaml {
    let mut sm = YamlMap::new();
    put(&mut sm, "name", Yaml::String("semantic_model".to_string()));
    put(
        &mut sm,
        "description",
        Yaml::String("Semantic model exported from Sidemantic".to_string()),
    );

    // Override with preserved semantic-model-level metadata, if any.
    let sm_meta = graph
        .metadata()
        .and_then(|m| m.get("osi"))
        .and_then(|osi| osi.get("semantic_models"))
        .and_then(Json::as_array)
        .and_then(|list| list.first());
    if let Some(sm_meta) = sm_meta {
        for key in ["name", "description", "ai_context"] {
            if let Some(value) = sm_meta.get(key) {
                if !value.is_null() {
                    put(&mut sm, key, json_to_yaml(value));
                }
            }
        }
        if let Some(custom) =
            normalize_custom_extensions_for_export(sm_meta.get("custom_extensions"))
        {
            put(
                &mut sm,
                "custom_extensions",
                json_to_yaml(&Json::Array(custom)),
            );
        }
    }

    let datasets: Vec<Yaml> = models
        .iter()
        .map(|model| export_dataset(model, dialects))
        .collect();
    put(&mut sm, "datasets", Yaml::Sequence(datasets));

    let mut relationships = Vec::new();
    for model in models {
        for rel in &model.relationships {
            if let Some(rel_def) = export_relationship(&model.name, rel, models) {
                relationships.push(rel_def);
            }
        }
    }
    if !relationships.is_empty() {
        put(&mut sm, "relationships", Yaml::Sequence(relationships));
    }

    // Metrics owned by a model are exported (qualified) in the model loop
    // below. `graph.metrics()` can surface the same metric at graph scope
    // (model-level time_comparison/conversion metrics are indexed there, and
    // top-level metrics get assigned to an owner model), so exclude those names
    // from the graph-level loop to avoid emitting duplicate OSI definitions.
    let model_owned: HashSet<&str> = models
        .iter()
        .flat_map(|model| model.metrics.iter().map(|metric| metric.name.as_str()))
        .collect();

    let mut metrics = Vec::new();
    let mut graph_metrics: Vec<&Metric> = graph.metrics().collect();
    graph_metrics.sort_by(|a, b| a.name.cmp(&b.name));
    for metric in graph_metrics {
        if model_owned.contains(metric.name.as_str()) {
            continue;
        }
        if let Some(metric_def) = export_metric(metric, None, dialects) {
            metrics.push(metric_def);
        }
    }
    for model in models {
        for metric in &model.metrics {
            if let Some(metric_def) = export_metric(metric, Some(&model.name), dialects) {
                metrics.push(metric_def);
            }
        }
    }
    if !metrics.is_empty() {
        put(&mut sm, "metrics", Yaml::Sequence(metrics));
    }

    Yaml::Mapping(sm)
}

fn export_dataset(model: &Model, dialects: &[String]) -> Yaml {
    let mut dataset = YamlMap::new();
    put(&mut dataset, "name", Yaml::String(model.name.clone()));

    if let Some(sql) = &model.sql {
        put(&mut dataset, "source", Yaml::String(format!("({sql})")));
    } else if let Some(table) = &model.table {
        put(&mut dataset, "source", Yaml::String(table.clone()));
    }

    put(
        &mut dataset,
        "primary_key",
        yaml_string_seq(&model.primary_key_columns),
    );

    if let Some(unique_keys) = &model.unique_keys {
        let rows: Vec<Yaml> = unique_keys.iter().map(|row| yaml_string_seq(row)).collect();
        put(&mut dataset, "unique_keys", Yaml::Sequence(rows));
    }

    if let Some(description) = &model.description {
        put(
            &mut dataset,
            "description",
            Yaml::String(description.clone()),
        );
    }

    if !model.dimensions.is_empty() {
        let fields: Vec<Yaml> = model
            .dimensions
            .iter()
            .map(|dim| export_field(dim, dialects))
            .collect();
        put(&mut dataset, "fields", Yaml::Sequence(fields));
    }

    apply_meta_export(&mut dataset, model.meta.as_ref());

    Yaml::Mapping(dataset)
}

fn export_field(dim: &Dimension, dialects: &[String]) -> Yaml {
    let mut field = YamlMap::new();
    put(&mut field, "name", Yaml::String(dim.name.clone()));

    let sql_expr = dim.sql.clone().unwrap_or_else(|| dim.name.clone());
    put(
        &mut field,
        "expression",
        expression_value(&sql_expr, dialects),
    );

    if dim.r#type == DimensionType::Time {
        let mut dimension = YamlMap::new();
        put(&mut dimension, "is_time", Yaml::Bool(true));
        put(&mut field, "dimension", Yaml::Mapping(dimension));
    }

    if let Some(description) = &dim.description {
        put(&mut field, "description", Yaml::String(description.clone()));
    }
    if let Some(label) = &dim.label {
        put(&mut field, "label", Yaml::String(label.clone()));
    }

    apply_meta_export(&mut field, dim.meta.as_ref());

    Yaml::Mapping(field)
}

fn export_relationship(from_model: &str, rel: &Relationship, models: &[&Model]) -> Option<Yaml> {
    if rel.r#type != RelationshipType::ManyToOne {
        return None;
    }

    let to_columns = if rel.primary_key.is_none() {
        models
            .iter()
            .find(|m| m.name == rel.name)
            .map(|m| m.primary_key_columns.clone())
            .unwrap_or_else(|| vec!["id".to_string()])
    } else {
        rel.primary_key_columns()
    };

    let name = rel
        .metadata
        .as_ref()
        .and_then(|m| m.get("osi_name"))
        .and_then(Json::as_str)
        .map(String::from)
        .unwrap_or_else(|| format!("{from_model}_to_{}", rel.name));

    let mut result = YamlMap::new();
    put(&mut result, "name", Yaml::String(name));
    put(&mut result, "from", Yaml::String(from_model.to_string()));
    put(&mut result, "to", Yaml::String(rel.name.clone()));
    put(
        &mut result,
        "from_columns",
        yaml_string_seq(&rel.foreign_key_columns()),
    );
    put(&mut result, "to_columns", yaml_string_seq(&to_columns));

    apply_meta_export(&mut result, rel.metadata.as_ref());

    Some(Yaml::Mapping(result))
}

fn export_metric(metric: &Metric, model_name: Option<&str>, dialects: &[String]) -> Option<Yaml> {
    let expression = build_metric_expression(metric, model_name)?;
    if expression.is_empty() {
        return None;
    }

    let mut result = YamlMap::new();
    put(&mut result, "name", Yaml::String(metric.name.clone()));
    put(
        &mut result,
        "expression",
        expression_value(&expression, dialects),
    );
    if let Some(description) = &metric.description {
        put(
            &mut result,
            "description",
            Yaml::String(description.clone()),
        );
    }

    apply_meta_export(&mut result, metric.meta.as_ref());

    Some(Yaml::Mapping(result))
}

fn build_metric_expression(metric: &Metric, model_name: Option<&str>) -> Option<String> {
    match metric.r#type {
        MetricType::Ratio => {
            let num = metric.numerator.clone().unwrap_or_default();
            let denom = metric.denominator.clone().unwrap_or_default();
            Some(format!("{num} / NULLIF({denom}, 0)"))
        }
        MetricType::Derived => metric.sql.clone(),
        _ => {
            let Some(agg) = &metric.agg else {
                return metric.sql.clone();
            };
            let mut inner = metric.sql.clone().unwrap_or_else(|| "*".to_string());
            if let Some(model_name) = model_name {
                if inner != "*" && !inner.contains('.') {
                    inner = format!("{model_name}.{inner}");
                }
            }
            if *agg == Aggregation::CountDistinct {
                Some(format!("COUNT(DISTINCT {inner})"))
            } else {
                Some(format!("{}({inner})", agg.as_sql()))
            }
        }
    }
}

/// Build an OSI `expression` value (`{ dialects: [...] }`) for a SQL string,
/// transpiling to non-ANSI dialects via polyglot-sql.
fn expression_value(sql_expr: &str, dialects: &[String]) -> Yaml {
    let mut entries = Vec::new();
    for dialect in dialects {
        let expression = if dialect == "ANSI_SQL" {
            sql_expr.to_string()
        } else {
            transpile(sql_expr, dialect).unwrap_or_else(|| sql_expr.to_string())
        };
        let mut entry = YamlMap::new();
        put(&mut entry, "dialect", Yaml::String(dialect.clone()));
        put(&mut entry, "expression", Yaml::String(expression));
        entries.push(Yaml::Mapping(entry));
    }

    let mut wrapper = YamlMap::new();
    put(&mut wrapper, "dialects", Yaml::Sequence(entries));
    Yaml::Mapping(wrapper)
}

/// Transpile a bare SQL expression from DuckDB/ANSI to a target dialect.
fn transpile(sql: &str, dialect: &str) -> Option<String> {
    let target = match dialect {
        "SNOWFLAKE" => DialectType::Snowflake,
        "DATABRICKS" => DialectType::Databricks,
        _ => return None,
    };

    // Wrap so polyglot-sql parses the bare expression, then emit just the
    // projection in the target dialect.
    let wrapped = format!("SELECT {sql}");
    let statement = polyglot_sql::parse_one(&wrapped, DialectType::DuckDB).ok()?;
    if let Expression::Select(select) = statement {
        let projection = select.expressions.into_iter().next()?;
        return polyglot_sql::generate(&projection, target).ok();
    }
    None
}

/// Normalize permissive local extension metadata to the OSI schema shape:
/// a list of `{ vendor_name, data }` where `data` is a string.
fn normalize_custom_extensions_for_export(custom: Option<&Json>) -> Option<Vec<Json>> {
    let custom = custom?;
    if custom.is_null() {
        return None;
    }

    if let Json::Array(items) = custom {
        let mut normalized = Vec::new();
        for item in items {
            let Some(obj) = item.as_object() else {
                normalized.push(extension_entry(
                    "SIDEMANTIC",
                    extension_data_to_string(item),
                ));
                continue;
            };
            let vendor_name = obj
                .get("vendor_name")
                .or_else(|| obj.get("vendor"))
                .and_then(Json::as_str)
                .unwrap_or("SIDEMANTIC")
                .to_string();
            let data = match obj.get("data") {
                Some(data) => data.clone(),
                None => {
                    let mut rest = JsonMap::new();
                    for (key, value) in obj {
                        if key != "vendor_name" && key != "vendor" {
                            rest.insert(key.clone(), value.clone());
                        }
                    }
                    Json::Object(rest)
                }
            };
            normalized.push(extension_entry(
                &vendor_name,
                extension_data_to_string(&data),
            ));
        }
        return Some(normalized);
    }

    if let Json::Object(obj) = custom {
        if obj.contains_key("vendor_name") && obj.contains_key("data") {
            let vendor_name = obj
                .get("vendor_name")
                .and_then(Json::as_str)
                .unwrap_or("SIDEMANTIC");
            let data = extension_data_to_string(obj.get("data").unwrap_or(&Json::Null));
            return Some(vec![extension_entry(vendor_name, data)]);
        }
    }

    Some(vec![extension_entry(
        "SIDEMANTIC",
        extension_data_to_string(custom),
    )])
}

fn extension_entry(vendor_name: &str, data: String) -> Json {
    let mut entry = JsonMap::new();
    entry.insert(
        "vendor_name".to_string(),
        Json::String(vendor_name.to_string()),
    );
    entry.insert("data".to_string(), Json::String(data));
    Json::Object(entry)
}

fn extension_data_to_string(data: &Json) -> String {
    match data {
        Json::String(s) => s.clone(),
        // Mirror Python's json.dumps(data, sort_keys=True): serde_json's default
        // Map is sorted, and PythonJsonFormatter reproduces the ", "/": " spacing.
        other => to_python_json(other),
    }
}

/// Serialize a JSON value with Python `json.dumps` default separators (`, `/`: `).
fn to_python_json(value: &Json) -> String {
    use serde::Serialize;
    let mut buf = Vec::new();
    let mut serializer = serde_json::Serializer::with_formatter(&mut buf, PythonJsonFormatter);
    if value.serialize(&mut serializer).is_err() {
        return serde_json::to_string(value).unwrap_or_default();
    }
    String::from_utf8(buf).unwrap_or_default()
}

/// `serde_json` formatter matching Python's default `json.dumps` separators.
struct PythonJsonFormatter;

impl serde_json::ser::Formatter for PythonJsonFormatter {
    fn begin_array_value<W: ?Sized + std::io::Write>(
        &mut self,
        writer: &mut W,
        first: bool,
    ) -> std::io::Result<()> {
        if first {
            Ok(())
        } else {
            writer.write_all(b", ")
        }
    }

    fn begin_object_key<W: ?Sized + std::io::Write>(
        &mut self,
        writer: &mut W,
        first: bool,
    ) -> std::io::Result<()> {
        if first {
            Ok(())
        } else {
            writer.write_all(b", ")
        }
    }

    fn begin_object_value<W: ?Sized + std::io::Write>(
        &mut self,
        writer: &mut W,
    ) -> std::io::Result<()> {
        writer.write_all(b": ")
    }
}

/// Apply `ai_context`/`custom_extensions` from a `meta`-style payload to a YAML map.
fn apply_meta_export(target: &mut YamlMap, meta: Option<&Json>) {
    let Some(meta) = meta.and_then(Json::as_object) else {
        return;
    };
    if let Some(ai_context) = meta.get("ai_context") {
        put(target, "ai_context", json_to_yaml(ai_context));
    }
    if let Some(custom) = meta.get("custom_extensions") {
        if let Some(normalized) = normalize_custom_extensions_for_export(Some(custom)) {
            put(
                target,
                "custom_extensions",
                json_to_yaml(&Json::Array(normalized)),
            );
        }
    }
}

// =============================================================================
// YAML construction helpers
// =============================================================================

fn put(map: &mut YamlMap, key: &str, value: Yaml) {
    map.insert(Yaml::String(key.to_string()), value);
}

fn yaml_string_seq(items: &[String]) -> Yaml {
    Yaml::Sequence(items.iter().map(|s| Yaml::String(s.clone())).collect())
}

fn json_to_yaml(value: &Json) -> Yaml {
    serde_yaml::to_value(value).unwrap_or(Yaml::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::Aggregation;

    fn export_to_json(graph: &SemanticGraph, dialects: Vec<String>) -> Json {
        let yaml = OsiAdapter::new()
            .with_dialects(dialects)
            .export_string(graph)
            .unwrap();
        serde_yaml::from_str(&yaml).unwrap()
    }

    // ----- import -----

    #[test]
    fn test_import_simple_dataset_and_fields() {
        let yaml = r#"
semantic_model:
  - name: analytics
    datasets:
      - name: events
        source: analytics.events
        primary_key: [event_id]
        fields:
          - name: event_id
            expression:
              dialects:
                - dialect: ANSI_SQL
                  expression: event_id
          - name: event_time
            expression:
              dialects:
                - dialect: ANSI_SQL
                  expression: event_time
            dimension:
              is_time: true
"#;
        let graph = OsiAdapter::new().parse_str(yaml).unwrap();
        let events = graph.get_model("events").unwrap();
        assert_eq!(events.table.as_deref(), Some("analytics.events"));
        assert_eq!(events.primary_key, "event_id");
        assert!(events.get_dimension("event_id").is_some());
        let event_time = events.get_dimension("event_time").unwrap();
        assert_eq!(event_time.r#type, DimensionType::Time);
        assert_eq!(event_time.granularity.as_deref(), Some("day"));
        assert_eq!(events.default_time_dimension.as_deref(), Some("event_time"));
    }

    #[test]
    fn test_import_prefers_ansi_dialect() {
        let yaml = r#"
semantic_model:
  - name: m
    datasets:
      - name: data
        source: test.data
        fields:
          - name: full_name
            expression:
              dialects:
                - dialect: SNOWFLAKE
                  expression: "CONCAT(first, ' ', last)"
                - dialect: ANSI_SQL
                  expression: "first || ' ' || last"
"#;
        let graph = OsiAdapter::new().parse_str(yaml).unwrap();
        let dim = graph
            .get_model("data")
            .unwrap()
            .get_dimension("full_name")
            .unwrap();
        assert_eq!(dim.sql.as_deref(), Some("first || ' ' || last"));
    }

    #[test]
    fn test_import_relationship_and_metric_aggregations() {
        let yaml = r#"
semantic_model:
  - name: shop
    datasets:
      - name: orders
        source: public.orders
        primary_key: [order_id]
        fields:
          - name: customer_id
            expression:
              dialects:
                - dialect: ANSI_SQL
                  expression: customer_id
      - name: customers
        source: public.customers
        primary_key: [customer_id]
        fields:
          - name: customer_id
            expression:
              dialects:
                - dialect: ANSI_SQL
                  expression: customer_id
    relationships:
      - name: orders_to_customers
        from: orders
        to: customers
        from_columns: [customer_id]
        to_columns: [customer_id]
    metrics:
      - name: total_revenue
        expression:
          dialects:
            - dialect: ANSI_SQL
              expression: SUM(orders.amount)
      - name: order_count
        expression:
          dialects:
            - dialect: ANSI_SQL
              expression: COUNT(*)
      - name: avg_order_value
        expression:
          dialects:
            - dialect: ANSI_SQL
              expression: AVG(orders.amount)
      - name: customer_count
        expression:
          dialects:
            - dialect: ANSI_SQL
              expression: COUNT(DISTINCT customers.customer_id)
"#;
        let graph = OsiAdapter::new().parse_str(yaml).unwrap();

        let orders = graph.get_model("orders").unwrap();
        assert_eq!(orders.relationships.len(), 1);
        let rel = &orders.relationships[0];
        assert_eq!(rel.name, "customers");
        assert_eq!(rel.r#type, RelationshipType::ManyToOne);
        assert_eq!(rel.foreign_key.as_deref(), Some("customer_id"));
        assert_eq!(rel.primary_key.as_deref(), Some("customer_id"));
        assert_eq!(
            rel.metadata.as_ref().unwrap()["osi_name"],
            Json::String("orders_to_customers".to_string())
        );

        assert_eq!(
            graph.get_metric("total_revenue").unwrap().agg,
            Some(Aggregation::Sum)
        );
        assert_eq!(
            graph.get_metric("order_count").unwrap().agg,
            Some(Aggregation::Count)
        );
        assert_eq!(
            graph.get_metric("avg_order_value").unwrap().agg,
            Some(Aggregation::Avg)
        );
        assert_eq!(
            graph.get_metric("customer_count").unwrap().agg,
            Some(Aggregation::CountDistinct)
        );
    }

    #[test]
    fn test_import_composite_key_unique_keys_and_meta() {
        let yaml = r#"
semantic_model:
  - name: m
    datasets:
      - name: order_items
        source: public.order_items
        primary_key: [order_id, item_id]
        unique_keys: [[sku], [tenant_id, slot]]
        ai_context:
          synonyms: [line_items]
        custom_extensions:
          vendor:
            name: acme
          tags: [important]
        fields:
          - name: order_id
            expression:
              dialects:
                - dialect: ANSI_SQL
                  expression: order_id
"#;
        let graph = OsiAdapter::new().parse_str(yaml).unwrap();
        let model = graph.get_model("order_items").unwrap();
        assert_eq!(model.primary_key, "order_id");
        assert_eq!(
            model.primary_key_columns,
            vec!["order_id".to_string(), "item_id".to_string()]
        );
        assert_eq!(
            model.unique_keys.as_ref().unwrap(),
            &vec![
                vec!["sku".to_string()],
                vec!["tenant_id".to_string(), "slot".to_string()]
            ]
        );
        let meta = model.meta.as_ref().unwrap();
        assert_eq!(meta["ai_context"]["synonyms"][0], "line_items");
        assert_eq!(meta["custom_extensions"]["vendor"]["name"], "acme");
        assert_eq!(meta["custom_extensions"]["tags"][0], "important");
    }

    #[test]
    fn test_import_graph_metadata_and_ontology_mapping() {
        let yaml = r#"
version: 0.2.0.dev0
ontology:
  - concept:
      name: Flight
ontology_mappings:
  - name: flights_map
    description: Flight mapping
    semantic_model:
      name: logical_flights
      datasets:
        - name: flights
          source: analytics.flights
          fields:
            - name: flight_id
              expression:
                dialects:
                  - dialect: ANSI_SQL
                    expression: flight_id
    concept_mappings:
      - concept: Flight
"#;
        let graph = OsiAdapter::new().parse_str(yaml).unwrap();
        assert!(graph.get_model("flights").is_some());
        let osi = &graph.metadata().unwrap()["osi"];
        assert_eq!(osi["version"], "0.2.0.dev0");
        assert_eq!(osi["ontology"][0]["concept"]["name"], "Flight");
        let sm = &osi["semantic_models"][0];
        assert_eq!(sm["source"], "ontology_mappings[0].semantic_model");
        assert_eq!(sm["name"], "logical_flights");
        assert_eq!(sm["ontology_mapping"]["name"], "flights_map");
    }

    #[test]
    fn test_import_skips_invalid_entries_and_empty() {
        assert_eq!(OsiAdapter::new().parse_str("").unwrap().models().count(), 0);

        let yaml = r#"
semantic_model:
  - name: m
    datasets:
      - source: nameless.dataset
      - name: data
        source: public.data
        fields:
          - expression:
              dialects:
                - dialect: ANSI_SQL
                  expression: id
    metrics:
      - name: bad_metric
"#;
        let graph = OsiAdapter::new().parse_str(yaml).unwrap();
        assert_eq!(graph.models().count(), 1);
        assert_eq!(graph.get_model("data").unwrap().dimensions.len(), 0);
        assert_eq!(graph.metrics().count(), 0);
    }

    #[test]
    fn test_decode_sidemantic_custom_extension_wrapper() {
        let yaml = r#"
semantic_model:
  - name: m
    datasets:
      - name: data
        source: public.data
        custom_extensions:
          - vendor_name: SIDEMANTIC
            data: '{"catalog_version": "2.0"}'
        fields:
          - name: id
            expression:
              dialects:
                - dialect: ANSI_SQL
                  expression: id
"#;
        let graph = OsiAdapter::new().parse_str(yaml).unwrap();
        let meta = graph.get_model("data").unwrap().meta.as_ref().unwrap();
        assert_eq!(meta["custom_extensions"]["catalog_version"], "2.0");
    }

    // ----- export -----

    fn sample_graph() -> SemanticGraph {
        let yaml = r#"
semantic_model:
  - name: m
    datasets:
      - name: orders
        source: public.orders
        primary_key: [order_id]
        description: Customer orders
        fields:
          - name: order_id
            expression:
              dialects:
                - dialect: ANSI_SQL
                  expression: order_id
          - name: order_date
            expression:
              dialects:
                - dialect: ANSI_SQL
                  expression: order_date
            dimension:
              is_time: true
    metrics:
      - name: total_revenue
        expression:
          dialects:
            - dialect: ANSI_SQL
              expression: SUM(orders.amount)
"#;
        OsiAdapter::new().parse_str(yaml).unwrap()
    }

    #[test]
    fn test_export_structure() {
        let graph = sample_graph();
        let data = export_to_json(&graph, vec!["ANSI_SQL".to_string()]);
        assert_eq!(data["version"], OSI_VERSION);
        let sm = &data["semantic_model"][0];
        let dataset = &sm["datasets"][0];
        assert_eq!(dataset["name"], "orders");
        assert_eq!(dataset["source"], "public.orders");
        assert_eq!(dataset["primary_key"][0], "order_id");
        let fields = dataset["fields"].as_array().unwrap();
        let order_date = fields.iter().find(|f| f["name"] == "order_date").unwrap();
        assert_eq!(order_date["dimension"]["is_time"], true);
        assert_eq!(sm["metrics"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn test_export_multi_dialect_includes_all() {
        let graph = sample_graph();
        let data = export_to_json(
            &graph,
            vec!["ANSI_SQL".to_string(), "SNOWFLAKE".to_string()],
        );
        let field = &data["semantic_model"][0]["datasets"][0]["fields"][0];
        let names: Vec<&str> = field["expression"]["dialects"]
            .as_array()
            .unwrap()
            .iter()
            .map(|d| d["dialect"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"ANSI_SQL"));
        assert!(names.contains(&"SNOWFLAKE"));
    }

    #[test]
    fn test_export_unknown_dialect_errors() {
        let graph = sample_graph();
        let err = OsiAdapter::new()
            .with_dialects(vec!["UNKNOWN".to_string()])
            .export_string(&graph)
            .unwrap_err();
        assert!(err.to_string().contains("Unsupported OSI export dialect"));
    }

    #[test]
    fn test_export_empty_dialects_defaults_to_ansi() {
        let graph = sample_graph();
        let data = export_to_json(&graph, vec![]);
        let dialects = data["semantic_model"][0]["datasets"][0]["fields"][0]["expression"]
            ["dialects"]
            .as_array()
            .unwrap();
        assert_eq!(dialects.len(), 1);
        assert_eq!(dialects[0]["dialect"], "ANSI_SQL");
    }

    #[test]
    fn test_export_relationship_and_meta() {
        let yaml = r#"
semantic_model:
  - name: m
    datasets:
      - name: orders
        source: public.orders
        primary_key: [order_id]
        ai_context:
          synonyms: [purchases]
        custom_extensions:
          catalog: v2
        fields:
          - name: customer_id
            expression:
              dialects:
                - dialect: ANSI_SQL
                  expression: customer_id
      - name: customers
        source: public.customers
        primary_key: [id]
        fields:
          - name: id
            expression:
              dialects:
                - dialect: ANSI_SQL
                  expression: id
    relationships:
      - name: orders_to_customers
        from: orders
        to: customers
        from_columns: [customer_id]
        to_columns: [id]
"#;
        let graph = OsiAdapter::new().parse_str(yaml).unwrap();
        let data = export_to_json(&graph, vec!["ANSI_SQL".to_string()]);
        let sm = &data["semantic_model"][0];
        let rels = sm["relationships"].as_array().unwrap();
        assert_eq!(rels.len(), 1);
        assert_eq!(rels[0]["from"], "orders");
        assert_eq!(rels[0]["to"], "customers");
        assert_eq!(rels[0]["from_columns"][0], "customer_id");
        assert_eq!(rels[0]["to_columns"][0], "id");

        let orders_ds = sm["datasets"]
            .as_array()
            .unwrap()
            .iter()
            .find(|d| d["name"] == "orders")
            .unwrap();
        assert_eq!(orders_ds["ai_context"]["synonyms"][0], "purchases");
        // permissive dict meta normalized into the OSI custom_extensions schema
        assert_eq!(
            orders_ds["custom_extensions"][0]["vendor_name"],
            "SIDEMANTIC"
        );
        assert!(orders_ds["custom_extensions"][0]["data"].is_string());
    }

    // ----- round trip -----

    #[test]
    fn test_roundtrip_preserves_models_and_metadata() {
        let graph = sample_graph();
        let yaml = OsiAdapter::new().export_string(&graph).unwrap();
        let graph2 = OsiAdapter::new().parse_str(&yaml).unwrap();

        let orders = graph2.get_model("orders").unwrap();
        assert_eq!(orders.table.as_deref(), Some("public.orders"));
        assert_eq!(orders.primary_key, "order_id");
        assert_eq!(
            orders.get_dimension("order_date").unwrap().r#type,
            DimensionType::Time
        );
        assert!(graph2.get_metric("total_revenue").is_some());

        // Semantic-model-level metadata survives the round trip.
        let yaml2 = OsiAdapter::new().export_string(&graph2).unwrap();
        let data: Json = serde_yaml::from_str(&yaml2).unwrap();
        assert_eq!(data["version"], OSI_VERSION);
    }

    #[test]
    fn test_graph_metadata_roundtrip_preserves_semantic_model_fields() {
        let yaml = r#"
version: 0.2.0.dev0
semantic_model:
  - name: current_model
    description: Current OSI model
    ai_context:
      instructions: Use for tests
    datasets:
      - name: events
        source: analytics.events
        fields:
          - name: event_id
            expression:
              dialects:
                - dialect: ANSI_SQL
                  expression: event_id
"#;
        let graph = OsiAdapter::new().parse_str(yaml).unwrap();
        let data = export_to_json(&graph, vec!["ANSI_SQL".to_string()]);
        let sm = &data["semantic_model"][0];
        assert_eq!(sm["name"], "current_model");
        assert_eq!(sm["description"], "Current OSI model");
        assert_eq!(sm["ai_context"]["instructions"], "Use for tests");
    }

    #[test]
    fn test_export_does_not_duplicate_owner_assigned_metrics() {
        // A top-level metric assigned to an owner model lives in both
        // `graph.metrics()` and `model.metrics`; it must be exported once.
        let yaml = r#"
version: 1
models:
  - name: orders
    table: orders
    primary_key: order_id
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
        let graph = crate::config::load_from_string(yaml).unwrap();
        let data = export_to_json(&graph, vec!["ANSI_SQL".to_string()]);
        let names: Vec<&str> = data["semantic_model"][0]["metrics"]
            .as_array()
            .unwrap()
            .iter()
            .map(|metric| metric["name"].as_str().unwrap())
            .collect();

        let mut unique = names.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(
            unique.len(),
            names.len(),
            "duplicate metric names: {names:?}"
        );
        assert!(names.contains(&"revenue"));
        assert!(names.contains(&"order_count"));
        assert!(names.contains(&"revenue_per_order"));
    }
}
