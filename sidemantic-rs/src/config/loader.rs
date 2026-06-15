//! Config loader: loads semantic layer definitions from YAML/SQL files

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;

use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::adapters::{Adapter, CubeAdapter, OsiAdapter};
use crate::core::{
    extract_dependencies, resolve_model_inheritance, Metric, Model, Parameter, Relationship,
    RelationshipType, SemanticGraph,
};
use crate::error::{Result, SidemanticError};

use super::schema::{SidemanticConfig, NATIVE_FORMAT_VERSION};
use super::sql_parser::{
    parse_sql_definitions, parse_sql_graph_definitions_extended, parse_sql_models,
};

#[derive(Debug, Default)]
struct ParsedConfig {
    models: Vec<Model>,
    extends_map: HashMap<String, String>,
    /// Top-level metrics assigned to an owning model (native format).
    top_level_metrics: Vec<Metric>,
    top_level_parameters: Vec<Parameter>,
    /// Graph-level metrics added directly to the graph (OSI), never reassigned.
    graph_metrics: Vec<Metric>,
    /// Graph-level metadata payload (OSI import state).
    graph_metadata: Option<serde_json::Value>,
    /// When true, relationships are declared explicitly; skip FK inference.
    explicit_relationships: bool,
}

#[derive(Debug)]
pub struct LoadedGraphMetadata {
    pub graph: SemanticGraph,
    pub model_order: Vec<String>,
    pub top_level_metrics: Vec<Metric>,
    pub original_model_metrics: HashMap<String, Vec<String>>,
    pub model_sources: HashMap<String, LoadedModelSource>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadedModelSource {
    pub source_format: String,
    pub source_file: Option<String>,
}

/// Detected config format
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigFormat {
    /// Native Sidemantic format (models: key)
    Sidemantic,
    /// Cube.js format (cubes: key)
    Cube,
    /// OSI (Open Semantic Interchange) format (semantic_model: / ontology_mappings: key)
    Osi,
}

impl ConfigFormat {
    fn source_label(self) -> &'static str {
        match self {
            ConfigFormat::Sidemantic => "Sidemantic",
            ConfigFormat::Cube => "Cube",
            ConfigFormat::Osi => "OSI",
        }
    }
}

/// Load a semantic graph from a single YAML file
pub fn load_from_file(path: impl AsRef<Path>) -> Result<SemanticGraph> {
    Ok(load_from_file_with_metadata(path)?.graph)
}

/// Load a semantic graph from a single file with metadata.
///
/// Supported file extensions:
/// - `.yml` / `.yaml` (native/cube YAML)
/// - `.sql` (MODEL statements or SQL + YAML frontmatter)
pub fn load_from_file_with_metadata(path: impl AsRef<Path>) -> Result<LoadedGraphMetadata> {
    let path = path.as_ref();
    let content = fs::read_to_string(path)
        .map_err(|e| SidemanticError::Validation(format!("Failed to read file: {e}")))?;

    let ext = path
        .extension()
        .and_then(|value| value.to_str())
        .map(str::to_ascii_lowercase);
    match ext.as_deref() {
        Some("sql") => load_from_sql_string_with_metadata(&content),
        _ => load_from_string_with_metadata(&content),
    }
}

/// Load a semantic graph from a YAML string
pub fn load_from_string(content: &str) -> Result<SemanticGraph> {
    Ok(load_from_string_with_metadata(content)?.graph)
}

/// Load a semantic graph from YAML with parsing metadata used by Python bridge.
pub fn load_from_string_with_metadata(content: &str) -> Result<LoadedGraphMetadata> {
    let format = detect_format(content);
    let parsed = parse_content_with_extends(content, format)?;
    let ParsedConfig {
        models,
        extends_map,
        top_level_metrics,
        top_level_parameters,
        graph_metrics,
        graph_metadata,
        ..
    } = parsed;
    let model_order: Vec<String> = models.iter().map(|model| model.name.clone()).collect();
    let original_model_metrics: HashMap<String, Vec<String>> = models
        .iter()
        .map(|model| {
            (
                model.name.clone(),
                model
                    .metrics
                    .iter()
                    .map(|metric| metric.name.clone())
                    .collect::<Vec<_>>(),
            )
        })
        .collect();

    // Resolve inheritance
    let models_map = collect_unique_models(models)?;
    let mut resolved_models = resolve_model_inheritance(models_map, &extends_map)?;
    if !resolved_models.is_empty() && !top_level_metrics.is_empty() {
        assign_top_level_metrics(&mut resolved_models, top_level_metrics.clone())?;
    }

    let mut graph = SemanticGraph::new();
    add_resolved_models_in_order(&mut graph, resolved_models, &model_order)?;
    for metric in top_level_metrics.iter().cloned() {
        if graph.get_metric(&metric.name).is_none() {
            graph.add_metric(metric)?;
        }
    }
    register_graph_metrics(&mut graph, &graph_metrics)?;
    for parameter in top_level_parameters {
        graph.add_parameter(parameter)?;
    }
    if let Some(metadata) = graph_metadata {
        graph.set_metadata(metadata);
    }

    let mut reported_metrics = top_level_metrics;
    reported_metrics.extend(graph_metrics);

    let model_sources = model_order
        .iter()
        .map(|model_name| {
            (
                model_name.clone(),
                LoadedModelSource {
                    source_format: format.source_label().to_string(),
                    source_file: None,
                },
            )
        })
        .collect();

    Ok(LoadedGraphMetadata {
        graph,
        model_order,
        top_level_metrics: reported_metrics,
        original_model_metrics,
        model_sources,
    })
}

fn parse_sql_frontmatter_and_body(content: &str) -> Result<(Option<serde_yaml::Mapping>, String)> {
    if !content.trim().starts_with("---") {
        return Ok((None, content.to_string()));
    }

    let parts: Vec<&str> = content.splitn(3, "---").collect();
    if parts.len() < 3 {
        return Ok((None, content.to_string()));
    }

    let frontmatter_text = parts[1].trim();
    let sql_body = parts[2].trim().to_string();
    if frontmatter_text.is_empty() {
        return Ok((None, sql_body));
    }

    let frontmatter_value: serde_yaml::Value =
        serde_yaml::from_str(frontmatter_text).map_err(|e| {
            SidemanticError::Validation(format!("failed to parse SQL frontmatter: {e}"))
        })?;

    match frontmatter_value {
        serde_yaml::Value::Null => Ok((None, sql_body)),
        serde_yaml::Value::Mapping(mut mapping) => {
            validate_sql_frontmatter_version(&mut mapping)?;
            if mapping.is_empty() {
                Ok((None, sql_body))
            } else {
                Ok((Some(mapping), sql_body))
            }
        }
        _ => Err(SidemanticError::Validation(
            "failed to parse SQL frontmatter: frontmatter must be a YAML mapping".to_string(),
        )),
    }
}

fn validate_sql_frontmatter_version(mapping: &mut serde_yaml::Mapping) -> Result<()> {
    let version_key = serde_yaml::Value::String("version".to_string());
    let Some(version_value) = mapping.remove(&version_key) else {
        return Ok(());
    };

    if let Some(version) = version_value.as_u64() {
        if version == u64::from(NATIVE_FORMAT_VERSION) {
            return Ok(());
        }
        return Err(SidemanticError::Validation(format!(
            "Unsupported native Sidemantic format version {version}; supported version is {NATIVE_FORMAT_VERSION}"
        )));
    }

    if let Some(version) = version_value.as_i64() {
        return Err(SidemanticError::Validation(format!(
            "Unsupported native Sidemantic format version {version}; supported version is {NATIVE_FORMAT_VERSION}"
        )));
    }

    Err(SidemanticError::Validation(
        "failed to parse SQL frontmatter: version must be an integer".to_string(),
    ))
}

fn model_from_sql_frontmatter(frontmatter: serde_yaml::Mapping) -> Result<Model> {
    let mut wrapper = serde_yaml::Mapping::new();
    wrapper.insert(
        serde_yaml::Value::String("models".to_string()),
        serde_yaml::Value::Sequence(vec![serde_yaml::Value::Mapping(frontmatter)]),
    );

    let config: SidemanticConfig = serde_yaml::from_value(serde_yaml::Value::Mapping(wrapper))
        .map_err(|e| {
            SidemanticError::Validation(format!("failed to parse SQL frontmatter model: {e}"))
        })?;
    let (models, _, _) = config.into_parts()?;
    models.into_iter().next().ok_or_else(|| {
        SidemanticError::Validation(
            "failed to parse SQL frontmatter: missing model definition".to_string(),
        )
    })
}

fn parse_sql_content(content: &str) -> Result<ParsedConfig> {
    let mut models: Vec<Model> = Vec::new();
    let mut top_level_metrics: Vec<Metric> = Vec::new();
    let mut top_level_parameters: Vec<Parameter> = Vec::new();

    match parse_sql_models(content) {
        Ok(parsed_models) => {
            let model_metric_names: HashSet<String> = parsed_models
                .iter()
                .flat_map(|model| model.metrics.iter().map(|metric| metric.name.clone()))
                .collect();
            models.extend(parsed_models);

            let graph_definitions = parse_sql_graph_definitions_extended(content);
            let (sql_metrics, _, sql_parameters, _) = match graph_definitions {
                Ok(definitions) => definitions,
                Err(_)
                    if content
                        .trim_start()
                        .to_ascii_lowercase()
                        .starts_with("model ")
                        && content.to_ascii_lowercase().contains(" from ") =>
                {
                    (Vec::new(), Vec::new(), Vec::new(), Vec::new())
                }
                Err(err) => {
                    return Err(SidemanticError::Validation(format!(
                        "failed to parse SQL graph definitions: {err}"
                    )));
                }
            };
            for metric in sql_metrics {
                if !model_metric_names.contains(&metric.name) {
                    top_level_metrics.push(metric);
                }
            }
            top_level_parameters.extend(sql_parameters);
        }
        Err(model_err)
            if content
                .trim_start()
                .to_ascii_lowercase()
                .starts_with("model ") =>
        {
            return Err(SidemanticError::Validation(format!(
                "failed to parse SQL model statement: {model_err}"
            )));
        }
        Err(_) => {
            let (frontmatter, sql_body) = parse_sql_frontmatter_and_body(content)?;
            let (sql_metrics, sql_segments, sql_parameters, sql_preaggs) =
                parse_sql_graph_definitions_extended(&sql_body).map_err(|e| {
                    SidemanticError::Validation(format!(
                        "failed to parse SQL graph definitions: {e}"
                    ))
                })?;
            top_level_parameters.extend(sql_parameters);

            if let Some(frontmatter) = frontmatter {
                let mut model = model_from_sql_frontmatter(frontmatter)?;
                model.metrics.extend(sql_metrics);
                model.segments.extend(sql_segments);
                model.pre_aggregations.extend(sql_preaggs);
                models.push(model);
            } else {
                top_level_metrics.extend(sql_metrics);
            }
        }
    }

    Ok(ParsedConfig {
        models,
        extends_map: HashMap::new(),
        top_level_metrics,
        top_level_parameters,
        ..Default::default()
    })
}

/// Load a semantic graph from SQL content with metadata.
pub fn load_from_sql_string_with_metadata(content: &str) -> Result<LoadedGraphMetadata> {
    let parsed = parse_sql_content(content)?;
    let ParsedConfig {
        models,
        extends_map,
        top_level_metrics,
        top_level_parameters,
        ..
    } = parsed;
    let model_order: Vec<String> = models.iter().map(|model| model.name.clone()).collect();
    let original_model_metrics: HashMap<String, Vec<String>> = models
        .iter()
        .map(|model| {
            (
                model.name.clone(),
                model
                    .metrics
                    .iter()
                    .map(|metric| metric.name.clone())
                    .collect::<Vec<_>>(),
            )
        })
        .collect();

    let models_map = collect_unique_models(models)?;
    let mut resolved_models = resolve_model_inheritance(models_map, &extends_map)?;
    if !resolved_models.is_empty() && !top_level_metrics.is_empty() {
        assign_top_level_metrics(&mut resolved_models, top_level_metrics.clone())?;
    }

    let mut graph = SemanticGraph::new();
    add_resolved_models_in_order(&mut graph, resolved_models, &model_order)?;
    for metric in top_level_metrics.iter().cloned() {
        if graph.get_metric(&metric.name).is_none() {
            graph.add_metric(metric)?;
        }
    }
    for parameter in top_level_parameters {
        graph.add_parameter(parameter)?;
    }

    let model_sources = model_order
        .iter()
        .map(|model_name| {
            (
                model_name.clone(),
                LoadedModelSource {
                    source_format: "Sidemantic".to_string(),
                    source_file: None,
                },
            )
        })
        .collect();

    Ok(LoadedGraphMetadata {
        graph,
        model_order,
        top_level_metrics,
        original_model_metrics,
        model_sources,
    })
}

/// Load all semantic model files from a directory into a semantic graph.
///
/// This function:
/// 1. Recursively finds all `.yml`/`.yaml`/`.sql` files
/// 2. Auto-detects only native Sidemantic YAML vs Cube YAML for YAML files
/// 3. Parses and collects all models
/// 4. Infers relationships from FK naming conventions
/// 5. Returns a unified SemanticGraph
///
/// External formats supported by the Python package (LookML, MetricFlow, Hex,
/// Rill, Malloy, and similar) must be converted to native YAML/SQL before using
/// the Rust runtime loader.
pub fn load_from_directory(dir: impl AsRef<Path>) -> Result<SemanticGraph> {
    Ok(load_from_directory_with_metadata(dir)?.graph)
}

/// Load all YAML files from a directory into a semantic graph with metadata.
pub fn load_from_directory_with_metadata(dir: impl AsRef<Path>) -> Result<LoadedGraphMetadata> {
    let dir = dir.as_ref();

    if !dir.is_dir() {
        return Err(SidemanticError::Validation(format!(
            "Path is not a directory: {}",
            dir.display()
        )));
    }

    let mut all_models: HashMap<String, Model> = HashMap::new();
    let mut all_extends_map: HashMap<String, String> = HashMap::new();
    let mut all_top_level_metrics: Vec<Metric> = Vec::new();
    let mut all_top_level_parameters: Vec<Parameter> = Vec::new();
    let mut all_graph_metrics: Vec<Metric> = Vec::new();
    let mut model_order: Vec<String> = Vec::new();
    let mut model_sources: HashMap<String, LoadedModelSource> = HashMap::new();
    // Models whose format declares relationships explicitly (e.g. OSI); these
    // are excluded from foreign-key relationship inference.
    let mut explicit_rel_models: HashSet<String> = HashSet::new();
    let mut merged_graph_metadata: Option<serde_json::Value> = None;

    // Recursively find and parse model files.
    for entry in walkdir(dir)? {
        let path = entry;
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .map(str::to_ascii_lowercase);

        match ext.as_deref() {
            Some("yml") | Some("yaml") => {
                let content = fs::read_to_string(&path).map_err(|e| {
                    SidemanticError::Validation(format!("Failed to read {}: {}", path.display(), e))
                })?;

                let format = detect_format(&content);
                let parsed = parse_content(&content, format)?;
                let source_format = format.source_label();
                let source_file = path
                    .strip_prefix(dir)
                    .ok()
                    .map(|value| value.to_string_lossy().to_string());
                let ParsedConfig {
                    models,
                    extends_map,
                    top_level_metrics,
                    top_level_parameters,
                    graph_metrics,
                    graph_metadata,
                    explicit_relationships,
                } = parsed;

                for model in models {
                    if all_models.contains_key(&model.name) {
                        return Err(SidemanticError::Validation(format!(
                            "Duplicate model '{}' found while loading directory",
                            model.name
                        )));
                    }
                    if explicit_relationships {
                        explicit_rel_models.insert(model.name.clone());
                    }
                    model_order.push(model.name.clone());
                    model_sources.insert(
                        model.name.clone(),
                        LoadedModelSource {
                            source_format: source_format.to_string(),
                            source_file: source_file.clone(),
                        },
                    );
                    all_models.insert(model.name.clone(), model);
                }
                all_extends_map.extend(extends_map);
                all_top_level_metrics.extend(top_level_metrics);
                all_top_level_parameters.extend(top_level_parameters);
                all_graph_metrics.extend(graph_metrics);
                merge_graph_metadata(&mut merged_graph_metadata, graph_metadata);
            }
            Some("sql") => {
                let content = fs::read_to_string(&path).map_err(|e| {
                    SidemanticError::Validation(format!("Failed to read {}: {}", path.display(), e))
                })?;
                let parsed = parse_sql_content(&content)?;
                let source_file = path
                    .strip_prefix(dir)
                    .ok()
                    .map(|value| value.to_string_lossy().to_string());
                let ParsedConfig {
                    models,
                    extends_map,
                    top_level_metrics,
                    top_level_parameters,
                    ..
                } = parsed;

                for model in models {
                    if all_models.contains_key(&model.name) {
                        return Err(SidemanticError::Validation(format!(
                            "Duplicate model '{}' found while loading directory",
                            model.name
                        )));
                    }
                    model_order.push(model.name.clone());
                    model_sources.insert(
                        model.name.clone(),
                        LoadedModelSource {
                            source_format: "Sidemantic".to_string(),
                            source_file: source_file.clone(),
                        },
                    );
                    all_models.insert(model.name.clone(), model);
                }
                all_extends_map.extend(extends_map);
                all_top_level_metrics.extend(top_level_metrics);
                all_top_level_parameters.extend(top_level_parameters);
            }
            _ => {}
        }
    }

    let original_model_metrics: HashMap<String, Vec<String>> = all_models
        .iter()
        .map(|(model_name, model)| {
            (
                model_name.clone(),
                model
                    .metrics
                    .iter()
                    .map(|metric| metric.name.clone())
                    .collect::<Vec<_>>(),
            )
        })
        .collect();

    // Infer relationships from FK naming conventions (skip formats that
    // declare relationships explicitly, e.g. OSI).
    infer_relationships(&mut all_models, &explicit_rel_models);
    let mut resolved_models = resolve_model_inheritance(all_models, &all_extends_map)?;
    if !resolved_models.is_empty() && !all_top_level_metrics.is_empty() {
        assign_top_level_metrics(&mut resolved_models, all_top_level_metrics.clone())?;
    }

    // Build the graph
    let mut graph = SemanticGraph::new();
    add_resolved_models_in_order(&mut graph, resolved_models, &model_order)?;
    for metric in all_top_level_metrics.iter().cloned() {
        if graph.get_metric(&metric.name).is_none() {
            graph.add_metric(metric)?;
        }
    }
    register_graph_metrics(&mut graph, &all_graph_metrics)?;
    for parameter in all_top_level_parameters {
        graph.add_parameter(parameter)?;
    }
    if let Some(metadata) = merged_graph_metadata {
        graph.set_metadata(metadata);
    }

    let mut reported_metrics = all_top_level_metrics;
    reported_metrics.extend(all_graph_metrics);

    Ok(LoadedGraphMetadata {
        graph,
        model_order,
        top_level_metrics: reported_metrics,
        original_model_metrics,
        model_sources,
    })
}

/// Merge an OSI `{ "osi": { ... } }` metadata payload into the accumulator,
/// concatenating `semantic_models` and keeping the first `version`/`ontology`.
fn merge_graph_metadata(acc: &mut Option<serde_json::Value>, incoming: Option<serde_json::Value>) {
    let Some(incoming) = incoming else {
        return;
    };
    match acc {
        Some(existing) => deep_merge_json(existing, incoming),
        None => *acc = Some(incoming),
    }
}

/// Recursively merge `incoming` into `target`: objects merge, arrays append, and
/// scalars keep the existing (first-wins) value. This preserves OSI accumulation
/// (semantic_models arrays append, version/ontology keep first) while also merging
/// non-OSI payloads such as `metadata.snowflake` from Python `export-native` files.
fn deep_merge_json(target: &mut serde_json::Value, incoming: serde_json::Value) {
    match (target, incoming) {
        (serde_json::Value::Object(target_map), serde_json::Value::Object(incoming_map)) => {
            for (key, value) in incoming_map {
                match target_map.get_mut(&key) {
                    Some(existing) => deep_merge_json(existing, value),
                    None => {
                        target_map.insert(key, value);
                    }
                }
            }
        }
        (serde_json::Value::Array(target_arr), serde_json::Value::Array(incoming_arr)) => {
            target_arr.extend(incoming_arr);
        }
        // Scalars (or type mismatches): keep the existing value.
        _ => {}
    }
}

/// Detect the config format from content
fn detect_format(content: &str) -> ConfigFormat {
    // Check for Cube.js format markers
    if content.contains("cubes:") {
        return ConfigFormat::Cube;
    }

    // Check for OSI format markers
    if content.contains("semantic_model:") || content.contains("ontology_mappings:") {
        return ConfigFormat::Osi;
    }

    // Default to Sidemantic format
    ConfigFormat::Sidemantic
}

/// Parse content based on detected format
fn parse_content(content: &str, format: ConfigFormat) -> Result<ParsedConfig> {
    parse_content_with_extends(content, format)
}

fn yaml_mapping_get_str<'a>(mapping: &'a serde_yaml::Mapping, key: &str) -> Option<&'a str> {
    mapping
        .get(serde_yaml::Value::String(key.to_string()))
        .and_then(serde_yaml::Value::as_str)
}

fn apply_embedded_sql_definitions(
    content: &str,
    models: &mut [Model],
    top_level_metrics: &mut Vec<Metric>,
) -> Result<()> {
    let root: serde_yaml::Value = serde_yaml::from_str(content)
        .map_err(|e| SidemanticError::Validation(format!("YAML parse error: {e}")))?;
    let Some(root_mapping) = root.as_mapping() else {
        return Ok(());
    };

    if let Some(sql_metrics) = yaml_mapping_get_str(root_mapping, "sql_metrics") {
        let (metrics, _) = parse_sql_definitions(sql_metrics).map_err(|e| {
            SidemanticError::Validation(format!(
                "Failed to parse top-level sql_metrics definitions: {e}"
            ))
        })?;
        top_level_metrics.extend(metrics);
    }

    if let Some(sql_segments) = yaml_mapping_get_str(root_mapping, "sql_segments") {
        parse_sql_definitions(sql_segments).map_err(|e| {
            SidemanticError::Validation(format!(
                "Failed to parse top-level sql_segments definitions: {e}"
            ))
        })?;
    }

    let model_lookup: HashMap<String, usize> = models
        .iter()
        .enumerate()
        .map(|(index, model)| (model.name.clone(), index))
        .collect();

    let Some(model_entries) = root_mapping
        .get(serde_yaml::Value::String("models".to_string()))
        .and_then(serde_yaml::Value::as_sequence)
    else {
        return Ok(());
    };

    for model_entry in model_entries {
        let Some(model_mapping) = model_entry.as_mapping() else {
            continue;
        };
        let Some(model_name) = yaml_mapping_get_str(model_mapping, "name") else {
            continue;
        };
        let Some(model_index) = model_lookup.get(model_name) else {
            continue;
        };

        if let Some(sql_metrics) = yaml_mapping_get_str(model_mapping, "sql_metrics") {
            let (metrics, _) = parse_sql_definitions(sql_metrics).map_err(|e| {
                SidemanticError::Validation(format!(
                    "Failed to parse sql_metrics definitions for model '{model_name}': {e}"
                ))
            })?;
            models[*model_index].metrics.extend(metrics);
        }

        if let Some(sql_segments) = yaml_mapping_get_str(model_mapping, "sql_segments") {
            let (_, segments) = parse_sql_definitions(sql_segments).map_err(|e| {
                SidemanticError::Validation(format!(
                    "Failed to parse sql_segments definitions for model '{model_name}': {e}"
                ))
            })?;
            models[*model_index].segments.extend(segments);
        }
    }

    Ok(())
}

fn substitute_env_vars(content: &str) -> String {
    let brace_pattern = Regex::new(r"\$\{([^}]+)\}").expect("valid regex");
    let substituted = brace_pattern.replace_all(content, |caps: &regex::Captures<'_>| {
        let var_expr = &caps[1];
        if let Some((var_name, default_value)) = var_expr.split_once(":-") {
            std::env::var(var_name).unwrap_or_else(|_| default_value.to_string())
        } else {
            std::env::var(var_expr).unwrap_or_else(|_| caps[0].to_string())
        }
    });

    let simple_pattern = Regex::new(r"\$([A-Z_][A-Z0-9_]*)").expect("valid regex");
    simple_pattern
        .replace_all(&substituted, |caps: &regex::Captures<'_>| {
            let var_name = &caps[1];
            std::env::var(var_name).unwrap_or_else(|_| caps[0].to_string())
        })
        .into_owned()
}

/// Parse content and return extends map for inheritance resolution
fn parse_content_with_extends(content: &str, format: ConfigFormat) -> Result<ParsedConfig> {
    let content = substitute_env_vars(content);

    match format {
        ConfigFormat::Sidemantic => {
            let config: SidemanticConfig = serde_yaml::from_str(&content)
                .map_err(|e| SidemanticError::Validation(format!("YAML parse error: {e}")))?;
            config.validate_contract()?;
            let extends_map: HashMap<String, String> = config
                .models
                .iter()
                .filter_map(|m| m.extends.as_ref().map(|e| (m.name.clone(), e.clone())))
                .collect();
            let graph_metadata = config.metadata.clone();
            let (mut models, mut top_level_metrics, top_level_parameters) = config.into_parts()?;
            apply_embedded_sql_definitions(&content, &mut models, &mut top_level_metrics)?;

            Ok(ParsedConfig {
                models,
                extends_map,
                top_level_metrics,
                top_level_parameters,
                graph_metadata,
                ..Default::default()
            })
        }
        ConfigFormat::Cube => {
            // Cube.js doesn't support extends in the same way
            Ok(ParsedConfig {
                models: CubeAdapter::new().parse_models(&content)?,
                ..Default::default()
            })
        }
        ConfigFormat::Osi => {
            let doc = OsiAdapter::new().parse_document(&content)?;
            Ok(ParsedConfig {
                models: doc.models,
                top_level_parameters: doc.parameters,
                graph_metrics: doc.graph_metrics,
                graph_metadata: doc.metadata,
                explicit_relationships: doc.explicit_relationships,
                ..Default::default()
            })
        }
    }
}

/// Register graph-level metrics, then validate dependencies once all are present.
///
/// OSI documents may declare a derived/ratio metric before the graph metrics it
/// references; registering before validating keeps loading order-independent
/// while still rejecting genuinely missing references.
fn register_graph_metrics(graph: &mut SemanticGraph, metrics: &[Metric]) -> Result<()> {
    for metric in metrics.iter().cloned() {
        if graph.get_metric(&metric.name).is_none() {
            graph.add_metric_unvalidated(metric)?;
        }
    }
    for metric in metrics {
        graph.validate_metric_dependencies(metric)?;
    }
    Ok(())
}

fn collect_unique_models(models: Vec<Model>) -> Result<HashMap<String, Model>> {
    let mut map = HashMap::new();
    for model in models {
        if map.contains_key(&model.name) {
            return Err(SidemanticError::Validation(format!(
                "Duplicate model '{}' in config",
                model.name
            )));
        }
        map.insert(model.name.clone(), model);
    }
    Ok(map)
}

fn add_resolved_models_in_order(
    graph: &mut SemanticGraph,
    mut resolved_models: HashMap<String, Model>,
    model_order: &[String],
) -> Result<()> {
    for model_name in model_order {
        if let Some(model) = resolved_models.remove(model_name) {
            graph.add_model(model)?;
        }
    }

    let mut remaining_models: Vec<(String, Model)> = resolved_models.into_iter().collect();
    remaining_models.sort_by(|left, right| left.0.cmp(&right.0));
    for (_, model) in remaining_models {
        graph.add_model(model)?;
    }

    Ok(())
}

fn owners_from_dotted_reference(
    reference: &str,
    models: &HashMap<String, Model>,
) -> Option<String> {
    let (model_name, _) = reference.split_once('.')?;
    models
        .contains_key(model_name)
        .then(|| model_name.to_string())
}

fn owners_from_sql_fragment(
    fragment: &str,
    models: &HashMap<String, Model>,
) -> Result<HashSet<String>> {
    let model_ref_regex = Regex::new(r"\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b")
        .map_err(|e| SidemanticError::Validation(format!("Invalid ownership regex: {e}")))?;
    Ok(model_ref_regex
        .captures_iter(fragment)
        .filter_map(|captures| captures.get(1).map(|model_name| model_name.as_str()))
        .filter(|model_name| models.contains_key(*model_name))
        .map(ToString::to_string)
        .collect())
}

fn model_owners_for_entity(
    entity: Option<&str>,
    models: &HashMap<String, Model>,
) -> HashSet<String> {
    let Some(entity) = entity else {
        return HashSet::new();
    };
    if let Some(owner) = owners_from_dotted_reference(entity, models) {
        return HashSet::from([owner]);
    }
    models
        .iter()
        .filter(|(_, model)| {
            model
                .dimensions
                .iter()
                .any(|dimension| dimension.name == entity)
        })
        .map(|(model_name, _)| model_name.clone())
        .collect()
}

fn metric_reference_strings(metric: &Metric) -> Vec<&str> {
    let mut references = vec![
        metric.sql.as_deref(),
        metric.base_metric.as_deref(),
        metric.numerator.as_deref(),
        metric.denominator.as_deref(),
        metric.entity.as_deref(),
        metric.base_event.as_deref(),
        metric.conversion_event.as_deref(),
        metric.cohort_event.as_deref(),
        metric.activity_event.as_deref(),
        metric.having.as_deref(),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();

    if let Some(steps) = metric.steps.as_ref() {
        references.extend(steps.iter().map(String::as_str));
    }
    if let Some(inner_metrics) = metric.inner_metrics.as_ref() {
        references.extend(
            inner_metrics
                .iter()
                .filter_map(|inner| inner.sql.as_deref()),
        );
    }
    if let Some(entity_dimensions) = metric.entity_dimensions.as_ref() {
        references.extend(entity_dimensions.iter().map(String::as_str));
    }

    references
}

fn resolve_metric_owners(
    metric_name: &str,
    top_level_metrics: &HashMap<String, Metric>,
    models: &HashMap<String, Model>,
    cache: &mut HashMap<String, HashSet<String>>,
    visiting: &mut HashSet<String>,
) -> Result<HashSet<String>> {
    if let Some(cached) = cache.get(metric_name) {
        return Ok(cached.clone());
    }
    if !visiting.insert(metric_name.to_string()) {
        return Ok(HashSet::new());
    }

    let metric = top_level_metrics.get(metric_name).ok_or_else(|| {
        SidemanticError::Validation(format!(
            "Top-level metric '{}' not found while resolving ownership",
            metric_name
        ))
    })?;

    let existing_model_owners: HashSet<String> = models
        .iter()
        .filter_map(|(model_name, model)| {
            if model.get_metric(metric_name).is_some() {
                Some(model_name.clone())
            } else {
                None
            }
        })
        .collect();
    if existing_model_owners.len() == 1 {
        visiting.remove(metric_name);
        cache.insert(metric_name.to_string(), existing_model_owners.clone());
        return Ok(existing_model_owners);
    }

    let deps = extract_dependencies(metric, None);
    let mut owners = HashSet::new();
    for dep in deps {
        if let Some(owner) = owners_from_dotted_reference(&dep, models) {
            owners.insert(owner);
            continue;
        }

        if top_level_metrics.contains_key(&dep) {
            owners.extend(resolve_metric_owners(
                &dep,
                top_level_metrics,
                models,
                cache,
                visiting,
            )?);
            continue;
        }

        for (model_name, model) in models {
            if model.get_metric(&dep).is_some() {
                owners.insert(model_name.clone());
            }
        }
    }

    if owners.is_empty() {
        for reference in metric_reference_strings(metric) {
            if let Some(owner) = owners_from_dotted_reference(reference, models) {
                owners.insert(owner);
            }
            owners.extend(owners_from_sql_fragment(reference, models)?);
        }
    }

    if owners.is_empty() {
        owners.extend(model_owners_for_entity(metric.entity.as_deref(), models));
    }

    if owners.is_empty() && models.len() == 1 {
        if let Some(single_model) = models.keys().next() {
            owners.insert(single_model.clone());
        }
    }

    visiting.remove(metric_name);
    cache.insert(metric_name.to_string(), owners.clone());
    Ok(owners)
}

fn assign_top_level_metrics(
    models: &mut HashMap<String, Model>,
    top_level_metrics: Vec<Metric>,
) -> Result<()> {
    if top_level_metrics.is_empty() {
        return Ok(());
    }

    let mut metric_by_name = HashMap::new();
    for metric in &top_level_metrics {
        if metric_by_name
            .insert(metric.name.clone(), metric.clone())
            .is_some()
        {
            return Err(SidemanticError::Validation(format!(
                "Duplicate top-level metric '{}'",
                metric.name
            )));
        }
    }

    let mut owner_cache: HashMap<String, HashSet<String>> = HashMap::new();
    let mut ownership: HashMap<String, String> = HashMap::new();
    for metric in &top_level_metrics {
        let owners = resolve_metric_owners(
            &metric.name,
            &metric_by_name,
            models,
            &mut owner_cache,
            &mut HashSet::new(),
        )?;

        if owners.len() != 1 {
            let mut owner_list: Vec<String> = owners.into_iter().collect();
            owner_list.sort();
            return Err(SidemanticError::Validation(format!(
                "Cannot determine single owning model for top-level metric '{}'; owners={:?}",
                metric.name, owner_list
            )));
        }

        let owner = owners
            .into_iter()
            .next()
            .expect("owner set length checked to be exactly one");
        ownership.insert(metric.name.clone(), owner);
    }

    for metric in top_level_metrics {
        let owner = ownership.get(&metric.name).ok_or_else(|| {
            SidemanticError::Validation(format!(
                "Missing owner assignment for top-level metric '{}'",
                metric.name
            ))
        })?;
        let owner_model = models.get_mut(owner).ok_or_else(|| {
            SidemanticError::Validation(format!(
                "Owner model '{}' not found for top-level metric '{}'",
                owner, metric.name
            ))
        })?;
        if owner_model.get_metric(&metric.name).is_none() {
            owner_model.metrics.push(metric);
        }
    }

    Ok(())
}

/// Infer relationships between models based on FK naming conventions
///
/// Looks for columns ending with `_id` and tries to match them to existing models.
/// For example: `customer_id` -> `customer` or `customers` model
fn infer_relationships(models: &mut HashMap<String, Model>, skip: &HashSet<String>) {
    // Collect model names for lookup
    let mut model_names: Vec<String> = models.keys().cloned().collect();
    model_names.sort();

    // Collect relationships to add (to avoid borrow issues)
    let mut relationships_to_add: Vec<(String, Relationship)> = Vec::new();

    for model_name in &model_names {
        // Skip models whose format declares relationships explicitly.
        if skip.contains(model_name) {
            continue;
        }
        let Some(model) = models.get(model_name) else {
            continue;
        };
        for dim in &model.dimensions {
            let dim_name = dim.name.to_lowercase();

            // Check if dimension looks like a foreign key (ends with _id)
            if !dim_name.ends_with("_id") {
                continue;
            }

            // Extract referenced table name (e.g., customer_id -> customer)
            let referenced = &dim_name[..dim_name.len() - 3];

            // Try to find matching model (singular or plural)
            let potential_targets = vec![
                referenced.to_string(),
                format!("{}s", referenced),  // customer -> customers
                format!("{}es", referenced), // box -> boxes
            ];

            // Check if relationship already exists, including singular/plural names.
            if model.relationships.iter().any(|relationship| {
                let relationship_name = relationship.name.to_lowercase();
                relationship_name == referenced
                    || potential_targets
                        .iter()
                        .any(|target| target == &relationship_name)
            }) {
                continue;
            }

            for target in potential_targets {
                if model_names.iter().any(|n| n.to_lowercase() == target)
                    && target != model_name.to_lowercase()
                {
                    // Find the actual model name with correct casing
                    let actual_target = model_names
                        .iter()
                        .find(|n| n.to_lowercase() == target)
                        .unwrap()
                        .clone();
                    let target_primary_keys = models
                        .get(&actual_target)
                        .map(Model::primary_keys)
                        .unwrap_or_else(|| vec!["id".to_string()]);
                    let target_primary_key = target_primary_keys
                        .first()
                        .cloned()
                        .unwrap_or_else(|| "id".to_string());

                    // Add many_to_one relationship from current model
                    relationships_to_add.push((
                        model_name.clone(),
                        Relationship {
                            name: actual_target.clone(),
                            r#type: RelationshipType::ManyToOne,
                            foreign_key: Some(dim.name.clone()),
                            foreign_key_columns: Some(vec![dim.name.clone()]),
                            primary_key: Some(target_primary_key.clone()),
                            primary_key_columns: Some(target_primary_keys.clone()),
                            through: None,
                            through_foreign_key: None,
                            through_foreign_key_columns: None,
                            related_foreign_key: None,
                            related_foreign_key_columns: None,
                            sql: None,
                            metadata: None,
                        },
                    ));

                    // Add reverse one_to_many relationship
                    relationships_to_add.push((
                        actual_target,
                        Relationship {
                            name: model_name.clone(),
                            r#type: RelationshipType::OneToMany,
                            foreign_key: Some(dim.name.clone()),
                            foreign_key_columns: Some(vec![dim.name.clone()]),
                            primary_key: Some(target_primary_key),
                            primary_key_columns: Some(target_primary_keys),
                            through: None,
                            through_foreign_key: None,
                            through_foreign_key_columns: None,
                            related_foreign_key: None,
                            related_foreign_key_columns: None,
                            sql: None,
                            metadata: None,
                        },
                    ));

                    break;
                }
            }
        }
    }

    relationships_to_add.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.name.cmp(&right.1.name))
            .then_with(|| format!("{:?}", left.1.r#type).cmp(&format!("{:?}", right.1.r#type)))
    });

    // Apply collected relationships
    for (model_name, rel) in relationships_to_add {
        if let Some(model) = models.get_mut(&model_name) {
            // Check if relationship already exists before adding
            if !model.relationships.iter().any(|r| r.name == rel.name) {
                model.relationships.push(rel);
            }
        }
    }
}

/// Simple recursive directory walker
fn walkdir(dir: &Path) -> Result<Vec<std::path::PathBuf>> {
    let mut files = Vec::new();

    let entries = fs::read_dir(dir)
        .map_err(|e| SidemanticError::Validation(format!("Failed to read directory: {e}")))?;

    for entry in entries {
        let entry =
            entry.map_err(|e| SidemanticError::Validation(format!("Failed to read entry: {e}")))?;
        let path = entry.path();

        if path.is_dir() {
            files.extend(walkdir(&path)?);
        } else {
            files.push(path);
        }
    }

    files.sort();
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_format_sidemantic() {
        let content = "models:\n  - name: orders";
        assert_eq!(detect_format(content), ConfigFormat::Sidemantic);
    }

    #[test]
    fn test_detect_format_cube() {
        let content = "cubes:\n  - name: orders";
        assert_eq!(detect_format(content), ConfigFormat::Cube);
    }

    #[test]
    fn test_detect_format_osi() {
        assert_eq!(
            detect_format("semantic_model:\n  - name: m"),
            ConfigFormat::Osi
        );
        assert_eq!(
            detect_format("ontology_mappings:\n  - name: m"),
            ConfigFormat::Osi
        );
    }

    #[test]
    fn test_load_from_string_auto_detects_osi() {
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
"#;

        let loaded = load_from_string_with_metadata(yaml).unwrap();
        let graph = &loaded.graph;
        assert!(graph.get_model("orders").is_some());
        assert!(graph.get_model("customers").is_some());
        // OSI relationship is explicit; FK inference must not add a reverse edge.
        let orders = graph.get_model("orders").unwrap();
        assert_eq!(orders.relationships.len(), 1);
        let customers = graph.get_model("customers").unwrap();
        assert!(customers.get_relationship("orders").is_none());
        // Graph-level metric and metadata preserved.
        assert!(graph.get_metric("total_revenue").is_some());
        assert!(graph.metadata().is_some());
        assert_eq!(
            loaded.model_sources["orders"].source_format,
            "OSI".to_string()
        );
    }

    #[test]
    fn test_load_osi_graph_metrics_independent_of_declaration_order() {
        // `revenue_per_order` references `total_revenue`/`order_count` but is
        // declared before them; loading must not fail on declaration order.
        let yaml = r#"
semantic_model:
  - name: m
    datasets:
      - name: orders
        source: public.orders
        primary_key: [order_id]
        fields:
          - name: amount
            expression:
              dialects:
                - dialect: ANSI_SQL
                  expression: amount
    metrics:
      - name: revenue_per_order
        expression:
          dialects:
            - dialect: ANSI_SQL
              expression: total_revenue / order_count
      - name: total_revenue
        expression:
          dialects:
            - dialect: ANSI_SQL
              expression: SUM(amount)
      - name: order_count
        expression:
          dialects:
            - dialect: ANSI_SQL
              expression: COUNT(*)
"#;

        let graph = load_from_string(yaml).unwrap();
        assert!(graph.get_metric("revenue_per_order").is_some());
        assert!(graph.get_metric("total_revenue").is_some());
        assert!(graph.get_metric("order_count").is_some());
    }

    #[test]
    fn test_directory_infers_native_fk_into_osi_target() {
        // A native source model should still infer its many_to_one even when
        // the matched target model came from OSI (which is excluded only as an
        // inference *source*, not as a target).
        let dir = std::env::temp_dir().join(format!(
            "sidemantic-rs-loader-mixed-osi-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("orders.yml"),
            r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: customer_id
        type: categorical
"#,
        )
        .unwrap();
        fs::write(
            dir.join("customers.yaml"),
            r#"
semantic_model:
  - name: crm
    datasets:
      - name: customers
        source: public.customers
        primary_key: [id]
        fields:
          - name: id
            expression:
              dialects:
                - dialect: ANSI_SQL
                  expression: id
"#,
        )
        .unwrap();

        let graph = load_from_directory(&dir).unwrap();
        fs::remove_dir_all(&dir).unwrap();

        let orders = graph.get_model("orders").unwrap();
        let rel = orders.get_relationship("customers").unwrap();
        assert_eq!(rel.r#type, RelationshipType::ManyToOne);
        assert_eq!(rel.foreign_key_columns(), vec!["customer_id".to_string()]);
        assert_eq!(rel.primary_key_columns(), vec!["id".to_string()]);
    }

    #[test]
    fn test_substitute_env_vars_with_default() {
        let content = "table: ${SIDEMANTIC_RS_MISSING_FOR_TEST:-orders_table}";
        let substituted = substitute_env_vars(content);
        assert_eq!(substituted, "table: orders_table");
    }

    #[test]
    fn test_substitute_env_vars_for_existing_var() {
        let Some(home) = std::env::var("HOME").ok() else {
            return;
        };
        let content = "root: $HOME";
        let substituted = substitute_env_vars(content);
        assert_eq!(substituted, format!("root: {home}"));
    }

    #[test]
    fn test_load_from_string_sidemantic() {
        let yaml = r#"
version: 1
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

        let graph = load_from_string(yaml).unwrap();
        assert!(graph.get_model("orders").is_some());
    }

    #[test]
    fn test_load_from_string_accepts_missing_native_version_as_version_one() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
"#;

        let graph = load_from_string(yaml).unwrap();
        assert!(graph.get_model("orders").is_some());
    }

    #[test]
    fn test_load_from_string_rejects_unsupported_native_version() {
        let yaml = r#"
version: 2
models:
  - name: orders
    table: orders
    primary_key: order_id
"#;

        let err = load_from_string(yaml).unwrap_err();
        assert!(err
            .to_string()
            .contains("Unsupported native Sidemantic format version 2; supported version is 1"));
    }

    #[test]
    fn test_load_from_sql_string_accepts_missing_frontmatter_version() {
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
"#;

        let loaded = load_from_sql_string_with_metadata(sql).unwrap();
        let orders = loaded.graph.get_model("orders").unwrap();
        assert!(orders.get_metric("order_count").is_some());
    }

    #[test]
    fn test_load_from_sql_string_accepts_frontmatter_version_one() {
        let sql = r#"
---
version: 1
name: orders
table: orders
primary_key: order_id
---

METRIC (
  name order_count,
  agg count
);
"#;

        let loaded = load_from_sql_string_with_metadata(sql).unwrap();
        let orders = loaded.graph.get_model("orders").unwrap();
        assert!(orders.get_metric("order_count").is_some());
    }

    #[test]
    fn test_load_from_sql_string_rejects_unsupported_frontmatter_version() {
        let sql = r#"
---
version: 2
name: orders
table: orders
primary_key: order_id
---

METRIC (
  name order_count,
  agg count
);
"#;

        let err = load_from_sql_string_with_metadata(sql).unwrap_err();
        assert!(err
            .to_string()
            .contains("Unsupported native Sidemantic format version 2; supported version is 1"));
    }

    #[test]
    fn test_load_from_sql_string_supports_compact_model_syntax() {
        let sql = r#"
model orders from orders (
  primary key (order_id)
  status
  sum(amount) as revenue
)
"#;

        let loaded = load_from_sql_string_with_metadata(sql).unwrap();
        let orders = loaded.graph.get_model("orders").unwrap();
        assert!(orders.get_dimension("status").is_some());
        assert!(orders.get_metric("revenue").is_some());
    }

    #[test]
    fn test_load_from_sql_string_collects_graph_definitions_after_compact_model() {
        let sql = r#"
model orders from orders (
  primary key (order_id)
  sum(amount) as revenue
)

METRIC (
  name total_revenue,
  sql orders.revenue
);

PARAMETER (
  name region,
  type string,
  allowed_values [us, eu]
);
"#;

        let loaded = load_from_sql_string_with_metadata(sql).unwrap();

        let orders = loaded.graph.get_model("orders").unwrap();
        assert!(orders.get_metric("revenue").is_some());
        assert!(loaded.graph.get_metric("total_revenue").is_some());
        assert!(loaded.graph.get_parameter("region").is_some());
    }

    #[test]
    fn test_load_from_sql_string_keeps_multiple_legacy_models_separate() {
        let sql = r#"
MODEL (name orders, table orders, primary_key order_id);
METRIC order_count AS COUNT(*);

MODEL (name customers, table customers, primary_key customer_id);
METRIC customer_count AS COUNT(*);
"#;

        let loaded = load_from_sql_string_with_metadata(sql).unwrap();

        let orders = loaded.graph.get_model("orders").unwrap();
        assert!(orders.get_metric("order_count").is_some());
        assert!(orders.get_metric("customer_count").is_none());

        let customers = loaded.graph.get_model("customers").unwrap();
        assert!(customers.get_metric("customer_count").is_some());
        assert!(customers.get_metric("order_count").is_none());
        assert_eq!(
            loaded.model_order,
            vec!["orders".to_string(), "customers".to_string()]
        );
    }

    #[test]
    fn test_sql_frontmatter_version_is_not_model_metadata() {
        let sql = r#"
---
version: 1
---

METRIC (
  name order_count,
  agg count
);
"#;

        let loaded = load_from_sql_string_with_metadata(sql).unwrap();
        assert_eq!(loaded.graph.models().count(), 0);
        assert_eq!(loaded.top_level_metrics.len(), 1);
        assert_eq!(loaded.top_level_metrics[0].name, "order_count");
    }

    #[test]
    fn test_load_from_string_cube() {
        let yaml = r#"
cubes:
  - name: orders
    sql_table: orders

    dimensions:
      - name: status
        sql: "${CUBE}.status"
        type: string

    measures:
      - name: revenue
        sql: "${CUBE}.amount"
        type: sum
"#;

        let graph = load_from_string(yaml).unwrap();
        let model = graph.get_model("orders").unwrap();
        assert_eq!(model.dimensions[0].sql, Some("status".to_string()));
    }

    #[test]
    fn test_load_from_string_assigns_top_level_metrics() {
        let yaml = r#"
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
  - name: double_revenue_per_order
    type: derived
    sql: revenue_per_order * 2
"#;

        let graph = load_from_string(yaml).unwrap();
        let orders = graph.get_model("orders").unwrap();
        assert!(orders.get_metric("revenue_per_order").is_some());
        assert!(orders.get_metric("double_revenue_per_order").is_some());
    }

    #[test]
    fn test_load_from_string_rejects_ambiguous_top_level_metrics() {
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
      - name: revenue
        agg: sum
        sql: lifetime_value

metrics:
  - name: blended_revenue
    type: derived
    sql: revenue
"#;

        let err = load_from_string(yaml).unwrap_err();
        assert!(err.to_string().contains(
            "Cannot determine single owning model for top-level metric 'blended_revenue'"
        ));
    }

    #[test]
    fn test_load_from_string_assigns_top_level_metric_by_existing_model_metric_name() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    metrics:
      - name: revenue
        agg: sum
        sql: amount
      - name: revenue_yoy
        type: time_comparison
        base_metric: revenue
        comparison_type: yoy

  - name: customers
    table: customers
    primary_key: customer_id
    metrics:
      - name: revenue
        agg: sum
        sql: lifetime_value

metrics:
  - name: revenue_yoy
    type: time_comparison
    base_metric: revenue
    comparison_type: yoy
"#;

        let graph = load_from_string(yaml).unwrap();
        let orders = graph.get_model("orders").unwrap();
        assert_eq!(
            orders
                .metrics
                .iter()
                .filter(|metric| metric.name == "revenue_yoy")
                .count(),
            1
        );

        let customers = graph.get_model("customers").unwrap();
        assert!(customers.get_metric("revenue_yoy").is_none());
    }

    #[test]
    fn test_load_from_string_assigns_complex_top_level_metrics_by_entity_dimension() {
        let yaml = r#"
models:
  - name: events
    table: events
    primary_key: event_id
    dimensions:
      - name: user_id
        type: categorical
      - name: platform
        type: categorical
      - name: event_type
        type: categorical

  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: order_id
        type: categorical

metrics:
  - name: signup_conversion
    type: conversion
    entity: user_id
    base_event: event_type = 'signup'
    conversion_event: event_type = 'purchase'
    conversion_window: 7 days
  - name: signup_retention
    type: retention
    entity: user_id
    cohort_event: event_type = 'signup'
  - name: multi_platform_users
    type: cohort
    entity: user_id
    inner_metrics:
      - name: platform_count
        agg: count_distinct
        sql: platform
    having: platform_count >= 2
    agg: count
"#;

        let graph = load_from_string(yaml).unwrap();
        let events = graph.get_model("events").unwrap();
        assert!(events.get_metric("signup_conversion").is_some());
        assert!(events.get_metric("signup_retention").is_some());
        assert!(events.get_metric("multi_platform_users").is_some());

        let orders = graph.get_model("orders").unwrap();
        assert!(orders.get_metric("signup_conversion").is_none());
        assert!(orders.get_metric("signup_retention").is_none());
        assert!(orders.get_metric("multi_platform_users").is_none());
    }

    #[test]
    fn test_load_from_directory_resolves_cross_file_inheritance() {
        let dir = std::env::temp_dir().join(format!(
            "sidemantic-rs-loader-inheritance-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("base.yml"),
            r#"
models:
  - name: base_orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: amount
"#,
        )
        .unwrap();
        fs::write(
            dir.join("orders.yml"),
            r#"
models:
  - name: orders
    extends: base_orders
    metrics:
      - name: net_revenue
        agg: sum
        sql: amount - discount
"#,
        )
        .unwrap();

        let loaded = load_from_directory_with_metadata(&dir).unwrap();
        fs::remove_dir_all(&dir).unwrap();

        let orders = loaded.graph.get_model("orders").unwrap();
        assert_eq!(orders.table, Some("orders".to_string()));
        assert!(orders.get_dimension("status").is_some());
        assert!(orders.get_metric("revenue").is_some());
        assert!(orders.get_metric("net_revenue").is_some());
    }

    #[test]
    fn test_load_from_directory_merges_non_osi_root_metadata() {
        let dir = std::env::temp_dir().join(format!(
            "sidemantic-rs-loader-metadata-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        // Python `export-native` writes root `metadata.snowflake` (no `osi` key).
        fs::write(
            dir.join("a.yml"),
            r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
metadata:
  snowflake:
    custom_instructions: Prefer revenue.
    verified_queries:
      - name: q1
"#,
        )
        .unwrap();
        fs::write(
            dir.join("b.yml"),
            r#"
models:
  - name: customers
    table: customers
    primary_key: id
metadata:
  snowflake:
    verified_queries:
      - name: q2
"#,
        )
        .unwrap();

        let loaded = load_from_directory_with_metadata(&dir).unwrap();
        fs::remove_dir_all(&dir).unwrap();

        let metadata = loaded.graph.metadata().expect("graph metadata preserved");
        let snowflake = &metadata["snowflake"];
        assert_eq!(snowflake["custom_instructions"], "Prefer revenue.");
        // verified_queries from both files accumulate.
        let names: Vec<&str> = snowflake["verified_queries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry["name"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"q1"));
        assert!(names.contains(&"q2"));
    }

    #[test]
    fn test_walkdir_returns_deterministic_lexical_order() {
        let dir = std::env::temp_dir().join(format!(
            "sidemantic-rs-loader-walkdir-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(dir.join("b")).unwrap();
        fs::create_dir_all(dir.join("a")).unwrap();
        fs::write(dir.join("z.yml"), "models: []").unwrap();
        fs::write(
            dir.join("b").join("a.sql"),
            "METRIC (name b_metric, agg count);",
        )
        .unwrap();
        fs::write(dir.join("a").join("m.yml"), "models: []").unwrap();

        let files = walkdir(&dir).unwrap();
        let relative_files = files
            .iter()
            .map(|path| {
                path.strip_prefix(&dir)
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/")
            })
            .collect::<Vec<_>>();
        fs::remove_dir_all(&dir).unwrap();

        assert_eq!(relative_files, vec!["a/m.yml", "b/a.sql", "z.yml"]);
    }

    #[test]
    fn test_load_from_string_parses_model_embedded_sql_definitions() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    metrics:
      - name: yaml_metric
        agg: sum
        sql: amount
    sql_metrics: |
      METRIC (
        name sql_metric,
        agg count
      );
    sql_segments: |
      SEGMENT (
        name completed,
        sql status = 'completed'
      );
"#;

        let graph = load_from_string(yaml).unwrap();
        let orders = graph.get_model("orders").unwrap();
        assert!(orders.get_metric("yaml_metric").is_some());
        assert!(orders.get_metric("sql_metric").is_some());
        assert!(orders.get_segment("completed").is_some());
    }

    #[test]
    fn test_load_from_string_with_metadata_parses_graph_level_sql_metrics() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    metrics:
      - name: revenue
        agg: sum
        sql: amount
sql_metrics: |
  METRIC (
    name total_revenue,
    type derived,
    sql orders.revenue
  );
"#;

        let loaded = load_from_string_with_metadata(yaml).unwrap();
        assert_eq!(loaded.top_level_metrics.len(), 1);
        assert_eq!(loaded.top_level_metrics[0].name, "total_revenue");
        let orders = loaded.graph.get_model("orders").unwrap();
        assert!(orders.get_metric("total_revenue").is_some());
    }

    #[test]
    fn test_infer_relationships() {
        let mut models = HashMap::new();

        // Orders model with customer_id dimension
        let orders = Model::new("orders", "order_id")
            .with_table("orders")
            .with_dimension(crate::core::Dimension::categorical("customer_id"));

        // Customers model
        let customers = Model::new("customers", "id").with_table("customers");

        models.insert("orders".to_string(), orders);
        models.insert("customers".to_string(), customers);

        infer_relationships(&mut models, &HashSet::new());

        // Check orders now has relationship to customers
        let orders = models.get("orders").unwrap();
        assert!(orders.get_relationship("customers").is_some());

        // Check customers has reverse relationship
        let customers = models.get("customers").unwrap();
        assert!(customers.get_relationship("orders").is_some());
    }

    #[test]
    fn test_infer_relationships_preserves_explicit_plural_relationship() {
        let mut models = HashMap::new();

        let orders = Model::new("orders", "order_id")
            .with_table("orders")
            .with_dimension(crate::core::Dimension::categorical("customer_id"))
            .with_relationship(
                Relationship::many_to_one("customers").with_keys("customer_id", "customer_id"),
            );
        let customers = Model::new("customers", "customer_id").with_table("customers");

        models.insert("orders".to_string(), orders);
        models.insert("customers".to_string(), customers);

        infer_relationships(&mut models, &HashSet::new());

        let orders = models.get("orders").unwrap();
        assert_eq!(orders.relationships.len(), 1);
        let relationship = orders.get_relationship("customers").unwrap();
        assert_eq!(relationship.foreign_key_columns(), vec!["customer_id"]);
        assert_eq!(relationship.primary_key_columns(), vec!["customer_id"]);

        let customers = models.get("customers").unwrap();
        assert!(customers.get_relationship("orders").is_none());
    }

    #[test]
    fn test_infer_relationships_uses_target_model_primary_key() {
        let mut models = HashMap::new();

        let orders = Model::new("orders", "order_id")
            .with_table("orders")
            .with_dimension(crate::core::Dimension::categorical("customer_id"));
        let customers = Model::new("customers", "customer_id").with_table("customers");

        models.insert("orders".to_string(), orders);
        models.insert("customers".to_string(), customers);

        infer_relationships(&mut models, &HashSet::new());

        let orders = models.get("orders").unwrap();
        let relationship = orders.get_relationship("customers").unwrap();
        assert_eq!(relationship.foreign_key_columns(), vec!["customer_id"]);
        assert_eq!(relationship.primary_key_columns(), vec!["customer_id"]);

        let customers = models.get("customers").unwrap();
        let reverse = customers.get_relationship("orders").unwrap();
        assert_eq!(reverse.foreign_key_columns(), vec!["customer_id"]);
        assert_eq!(reverse.primary_key_columns(), vec!["customer_id"]);
    }

    #[test]
    fn test_model_inheritance() {
        let yaml = r#"
models:
  - name: base_orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: amount

  - name: us_orders
    extends: base_orders
    metrics:
      - name: order_count
        agg: count
"#;

        let graph = load_from_string(yaml).unwrap();

        // us_orders should inherit from base_orders
        let us_orders = graph.get_model("us_orders").unwrap();
        assert_eq!(us_orders.table, Some("orders".to_string())); // inherited
        assert!(us_orders.get_dimension("status").is_some()); // inherited
        assert!(us_orders.get_metric("revenue").is_some()); // inherited
        assert!(us_orders.get_metric("order_count").is_some()); // own
    }

    #[test]
    fn test_load_from_string_parses_top_level_parameters() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
parameters:
  - name: status
    type: string
    default_value: pending
  - name: start_date
    type: date
    default_to_today: true
"#;

        let graph = load_from_string(yaml).unwrap();
        assert!(graph.get_parameter("status").is_some());
        assert!(graph.get_parameter("start_date").is_some());
    }

    #[test]
    fn test_load_from_string_rejects_duplicate_parameters() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
parameters:
  - name: status
    type: string
  - name: status
    type: string
"#;

        let err = load_from_string(yaml).unwrap_err();
        assert!(err
            .to_string()
            .contains("Parameter 'status' already exists"));
    }

    #[test]
    fn test_load_from_string_rejects_duplicate_models() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
  - name: orders
    table: orders_v2
    primary_key: id
"#;

        let err = load_from_string(yaml).unwrap_err();
        assert!(err
            .to_string()
            .contains("Duplicate model 'orders' in config"));
    }

    #[test]
    fn test_load_from_string_rejects_invalid_parameter_type() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
parameters:
  - name: status
    type: enum
"#;

        let err = load_from_string(yaml).unwrap_err();
        assert!(err.to_string().contains("YAML parse error"));
    }

    #[test]
    fn test_load_from_string_parses_many_to_many_through_relationship() {
        let yaml = r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    relationships:
      - name: products
        type: many_to_many
        through: order_items
        through_foreign_key: order_id
        related_foreign_key: product_id
        primary_key: product_id
  - name: order_items
    table: order_items
  - name: products
    table: products
    primary_key: product_id
"#;

        let graph = load_from_string(yaml).unwrap();
        let path = graph.find_join_path("orders", "products").unwrap();
        assert_eq!(path.steps.len(), 2);
        assert_eq!(path.steps[0].to_model, "order_items");
        assert_eq!(path.steps[1].to_model, "products");
    }
}
