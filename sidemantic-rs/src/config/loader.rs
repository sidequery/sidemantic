//! Config loader: loads semantic layer definitions from YAML files

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use crate::core::{
    resolve_model_inheritance, Model, Relationship, RelationshipType, SemanticGraph,
};
use crate::error::{Result, SidemanticError};

use super::schema::{CubeConfig, SidemanticConfig};

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
    let path = path.as_ref();
    let content = fs::read_to_string(path)
        .map_err(|e| SidemanticError::Validation(format!("Failed to read file: {e}")))?;

    load_from_string(&content)
}

/// Load a semantic graph from a YAML string
pub fn load_from_string(content: &str) -> Result<SemanticGraph> {
    let format = detect_format(content);
    let (models, extends_map) = parse_content_with_extends(content, format)?;

    // Resolve inheritance
    let models_map: HashMap<String, Model> =
        models.into_iter().map(|m| (m.name.clone(), m)).collect();
    let resolved_models = resolve_model_inheritance(models_map, &extends_map)?;

    let mut graph = SemanticGraph::new();
    for model in resolved_models.into_values() {
        graph.add_model(model)?;
    }

    Ok(graph)
}

/// Load all YAML files from a directory into a semantic graph
///
/// This function:
/// 1. Recursively finds all .yml/.yaml files
/// 2. Auto-detects format (Sidemantic vs Cube.js)
/// 3. Parses and collects all models
/// 4. Infers relationships from FK naming conventions
/// 5. Returns a unified SemanticGraph
pub fn load_from_directory(dir: impl AsRef<Path>) -> Result<SemanticGraph> {
    let dir = dir.as_ref();

    if !dir.is_dir() {
        return Err(SidemanticError::Validation(format!(
            "Path is not a directory: {}",
            dir.display()
        )));
    }

    let mut all_models: HashMap<String, Model> = HashMap::new();

    // Recursively find and parse all YAML files
    for entry in walkdir(dir)? {
        let path = entry;
        let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");

        if ext == "yml" || ext == "yaml" {
            let content = fs::read_to_string(&path).map_err(|e| {
                SidemanticError::Validation(format!("Failed to read {}: {}", path.display(), e))
            })?;

            let format = detect_format(&content);
            let models = parse_content(&content, format)?;

            for model in models {
                all_models.insert(model.name.clone(), model);
            }
        }
    }

    // Infer relationships from FK naming conventions
    infer_relationships(&mut all_models);

    // Build the graph
    let mut graph = SemanticGraph::new();
    for (_, model) in all_models {
        graph.add_model(model)?;
    }

    Ok(graph)
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
fn parse_content(content: &str, format: ConfigFormat) -> Result<Vec<Model>> {
    let (models, _) = parse_content_with_extends(content, format)?;
    Ok(models)
}

/// Parse content and return extends map for inheritance resolution
fn parse_content_with_extends(
    content: &str,
    format: ConfigFormat,
) -> Result<(Vec<Model>, HashMap<String, String>)> {
    match format {
        ConfigFormat::Sidemantic => {
            let config: SidemanticConfig = serde_yaml::from_str(content)
                .map_err(|e| SidemanticError::Validation(format!("YAML parse error: {e}")))?;

            // Extract extends map before converting to models
            let extends_map: HashMap<String, String> = config
                .models
                .iter()
                .filter_map(|m| m.extends.as_ref().map(|e| (m.name.clone(), e.clone())))
                .collect();

            Ok((config.into_models(), extends_map))
        }
        ConfigFormat::Cube => {
            let config: CubeConfig = serde_yaml::from_str(content)
                .map_err(|e| SidemanticError::Validation(format!("YAML parse error: {e}")))?;
            // Cube.js doesn't support extends in the same way
            Ok((config.into_models(), HashMap::new()))
        }
    }
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
                            primary_key: Some("id".to_string()),
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
                            primary_key: Some("id".to_string()),
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
}
