//! Config loader: loads semantic layer definitions from YAML/SQL files

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;

use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::core::{
    extract_dependencies, resolve_model_inheritance, Metric, Model, Parameter, Relationship,
    RelationshipType, SemanticGraph,
};
use crate::error::{Result, SidemanticError};

use super::schema::{CubeConfig, SidemanticConfig};
use super::sql_parser::{
    parse_sql_definitions, parse_sql_graph_definitions_extended, parse_sql_model,
};

#[derive(Debug)]
struct ParsedConfig {
    models: Vec<Model>,
    extends_map: HashMap<String, String>,
    top_level_metrics: Vec<Metric>,
    top_level_parameters: Vec<Parameter>,
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
    for model in resolved_models.into_values() {
        graph.add_model(model)?;
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
                    source_format: match format {
                        ConfigFormat::Sidemantic => "Sidemantic".to_string(),
                        ConfigFormat::Cube => "Cube".to_string(),
                    },
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
        serde_yaml::Value::Mapping(mapping) => {
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
    let (models, _, _) = config.into_parts();
    models.into_iter().next().ok_or_else(|| {
        SidemanticError::Validation(
            "failed to parse SQL frontmatter: missing model definition".to_string(),
        )
    })
}

fn parse_sql_content(content: &str) -> Result<ParsedConfig> {
    let has_model_statement = {
        let upper = content.to_ascii_uppercase();
        upper.contains("MODEL") && upper.contains("MODEL (")
    };

    let mut models: Vec<Model> = Vec::new();
    let mut top_level_metrics: Vec<Metric> = Vec::new();
    let mut top_level_parameters: Vec<Parameter> = Vec::new();

    if has_model_statement {
        let model = parse_sql_model(content).map_err(|e| {
            SidemanticError::Validation(format!("failed to parse SQL model statement: {e}"))
        })?;
        let model_metric_names: HashSet<String> = model
            .metrics
            .iter()
            .map(|metric| metric.name.clone())
            .collect();
        models.push(model);

        let (sql_metrics, _, sql_parameters, _) = parse_sql_graph_definitions_extended(content)
            .map_err(|e| {
                SidemanticError::Validation(format!("failed to parse SQL graph definitions: {e}"))
            })?;
        for metric in sql_metrics {
            if !model_metric_names.contains(&metric.name) {
                top_level_metrics.push(metric);
            }
        }
        top_level_parameters.extend(sql_parameters);
    } else {
        let (frontmatter, sql_body) = parse_sql_frontmatter_and_body(content)?;
        let (sql_metrics, sql_segments, sql_parameters, sql_preaggs) =
            parse_sql_graph_definitions_extended(&sql_body).map_err(|e| {
                SidemanticError::Validation(format!("failed to parse SQL graph definitions: {e}"))
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

    Ok(ParsedConfig {
        models,
        extends_map: HashMap::new(),
        top_level_metrics,
        top_level_parameters,
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
    for model in resolved_models.into_values() {
        graph.add_model(model)?;
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
/// 2. Auto-detects format (Sidemantic vs Cube.js)
/// 3. Parses and collects all models
/// 4. Infers relationships from FK naming conventions
/// 5. Returns a unified SemanticGraph
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
    let mut all_top_level_metrics: Vec<Metric> = Vec::new();
    let mut all_top_level_parameters: Vec<Parameter> = Vec::new();
    let mut model_order: Vec<String> = Vec::new();
    let mut model_sources: HashMap<String, LoadedModelSource> = HashMap::new();

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
                let source_format = match format {
                    ConfigFormat::Sidemantic => "Sidemantic",
                    ConfigFormat::Cube => "Cube",
                };
                let source_file = path
                    .strip_prefix(dir)
                    .ok()
                    .map(|value| value.to_string_lossy().to_string());

                for model in parsed.models {
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
                            source_format: source_format.to_string(),
                            source_file: source_file.clone(),
                        },
                    );
                    all_models.insert(model.name.clone(), model);
                }
                all_top_level_metrics.extend(parsed.top_level_metrics);
                all_top_level_parameters.extend(parsed.top_level_parameters);
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

                for model in parsed.models {
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
                all_top_level_metrics.extend(parsed.top_level_metrics);
                all_top_level_parameters.extend(parsed.top_level_parameters);
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

    // Infer relationships from FK naming conventions
    infer_relationships(&mut all_models);
    if !all_models.is_empty() && !all_top_level_metrics.is_empty() {
        assign_top_level_metrics(&mut all_models, all_top_level_metrics.clone())?;
    }

    // Build the graph
    let mut graph = SemanticGraph::new();
    for (_, model) in all_models {
        graph.add_model(model)?;
    }
    for parameter in all_top_level_parameters {
        graph.add_parameter(parameter)?;
    }

    Ok(LoadedGraphMetadata {
        graph,
        model_order,
        top_level_metrics: all_top_level_metrics,
        original_model_metrics,
        model_sources,
    })
}

/// Detect the config format from content
fn detect_format(content: &str) -> ConfigFormat {
    // Check for Cube.js format markers
    if content.contains("cubes:") {
        return ConfigFormat::Cube;
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
            let extends_map: HashMap<String, String> = config
                .models
                .iter()
                .filter_map(|m| m.extends.as_ref().map(|e| (m.name.clone(), e.clone())))
                .collect();
            let (mut models, mut top_level_metrics, top_level_parameters) = config.into_parts();
            apply_embedded_sql_definitions(&content, &mut models, &mut top_level_metrics)?;

            Ok(ParsedConfig {
                models,
                extends_map,
                top_level_metrics,
                top_level_parameters,
            })
        }
        ConfigFormat::Cube => {
            let config: CubeConfig = serde_yaml::from_str(&content)
                .map_err(|e| SidemanticError::Validation(format!("YAML parse error: {e}")))?;
            // Cube.js doesn't support extends in the same way
            Ok(ParsedConfig {
                models: config.into_models(),
                extends_map: HashMap::new(),
                top_level_metrics: Vec::new(),
                top_level_parameters: Vec::new(),
            })
        }
    }
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
        if let Some((model_name, _)) = dep.split_once('.') {
            owners.insert(model_name.to_string());
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
        for reference in [
            metric.sql.as_deref(),
            metric.base_metric.as_deref(),
            metric.numerator.as_deref(),
            metric.denominator.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            if let Some((model_name, _)) = reference.split_once('.') {
                owners.insert(model_name.to_string());
            }
        }
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
fn infer_relationships(models: &mut HashMap<String, Model>) {
    // Collect model names for lookup
    let model_names: Vec<String> = models.keys().cloned().collect();

    // Collect relationships to add (to avoid borrow issues)
    let mut relationships_to_add: Vec<(String, Relationship)> = Vec::new();

    for (model_name, model) in models.iter() {
        for dim in &model.dimensions {
            let dim_name = dim.name.to_lowercase();

            // Check if dimension looks like a foreign key (ends with _id)
            if !dim_name.ends_with("_id") {
                continue;
            }

            // Extract referenced table name (e.g., customer_id -> customer)
            let referenced = &dim_name[..dim_name.len() - 3];

            // Check if relationship already exists
            if model
                .relationships
                .iter()
                .any(|r| r.name.to_lowercase() == referenced)
            {
                continue;
            }

            // Try to find matching model (singular or plural)
            let potential_targets = vec![
                referenced.to_string(),
                format!("{}s", referenced),  // customer -> customers
                format!("{}es", referenced), // box -> boxes
            ];

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

                    // Add many_to_one relationship from current model
                    relationships_to_add.push((
                        model_name.clone(),
                        Relationship {
                            name: actual_target.clone(),
                            r#type: RelationshipType::ManyToOne,
                            foreign_key: Some(dim.name.clone()),
                            foreign_key_columns: Some(vec![dim.name.clone()]),
                            primary_key: Some("id".to_string()),
                            primary_key_columns: Some(vec!["id".to_string()]),
                            through: None,
                            through_foreign_key: None,
                            related_foreign_key: None,
                            sql: None,
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
                            primary_key: Some("id".to_string()),
                            primary_key_columns: Some(vec!["id".to_string()]),
                            through: None,
                            through_foreign_key: None,
                            related_foreign_key: None,
                            sql: None,
                        },
                    ));

                    break;
                }
            }
        }
    }

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

        infer_relationships(&mut models);

        // Check orders now has relationship to customers
        let orders = models.get("orders").unwrap();
        assert!(orders.get_relationship("customers").is_some());

        // Check customers has reverse relationship
        let customers = models.get("customers").unwrap();
        assert!(customers.get_relationship("orders").is_some());
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
