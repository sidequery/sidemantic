//! Model inheritance support
//!
//! Allows models to extend other models, inheriting dimensions, metrics,
//! relationships, and segments. Child values override parent values.

use std::collections::{HashMap, HashSet};

use crate::core::model::Model;
use crate::error::{Result, SidemanticError};

/// Merge a child model with its parent.
///
/// Child inherits all fields from parent, with child values taking precedence.
/// List fields (dimensions, metrics, relationships, segments) are merged by name,
/// with child items overriding parent items of the same name.
pub fn merge_model(child: &Model, parent: &Model) -> Model {
    // Start with parent's table/sql, override with child if set
    let table = child.table.clone().or_else(|| parent.table.clone());
    let sql = child.sql.clone().or_else(|| parent.sql.clone());
    let primary_key = if child.primary_key != "id" {
        child.primary_key.clone()
    } else {
        parent.primary_key.clone()
    };
    let description = child
        .description
        .clone()
        .or_else(|| parent.description.clone());
    let label = child.label.clone().or_else(|| parent.label.clone());

    // Merge dimensions by name (child overrides parent)
    let mut dimensions_map: HashMap<String, _> = parent
        .dimensions
        .iter()
        .map(|d| (d.name.clone(), d.clone()))
        .collect();
    for dim in &child.dimensions {
        dimensions_map.insert(dim.name.clone(), dim.clone());
    }
    let dimensions: Vec<_> = dimensions_map.into_values().collect();

    // Merge metrics by name
    let mut metrics_map: HashMap<String, _> = parent
        .metrics
        .iter()
        .map(|m| (m.name.clone(), m.clone()))
        .collect();
    for metric in &child.metrics {
        metrics_map.insert(metric.name.clone(), metric.clone());
    }
    let metrics: Vec<_> = metrics_map.into_values().collect();

    // Merge relationships by name
    let mut relationships_map: HashMap<String, _> = parent
        .relationships
        .iter()
        .map(|r| (r.name.clone(), r.clone()))
        .collect();
    for rel in &child.relationships {
        relationships_map.insert(rel.name.clone(), rel.clone());
    }
    let relationships: Vec<_> = relationships_map.into_values().collect();

    // Merge segments by name
    let mut segments_map: HashMap<String, _> = parent
        .segments
        .iter()
        .map(|s| (s.name.clone(), s.clone()))
        .collect();
    for seg in &child.segments {
        segments_map.insert(seg.name.clone(), seg.clone());
    }
    let segments: Vec<_> = segments_map.into_values().collect();

    Model {
        name: child.name.clone(),
        table,
        sql,
        primary_key,
        dimensions,
        metrics,
        relationships,
        segments,
        label,
        description,
    }
}

/// Resolve inheritance for all models.
///
/// Models with `extends` field are merged with their parent models.
/// Handles transitive inheritance (A extends B extends C).
/// Detects circular inheritance.
pub fn resolve_model_inheritance(
    models: HashMap<String, Model>,
    extends_map: &HashMap<String, String>,
) -> Result<HashMap<String, Model>> {
    let mut resolved: HashMap<String, Model> = HashMap::new();
    let mut in_progress: HashSet<String> = HashSet::new();

    fn resolve(
        name: &str,
        models: &HashMap<String, Model>,
        extends_map: &HashMap<String, String>,
        resolved: &mut HashMap<String, Model>,
        in_progress: &mut HashSet<String>,
    ) -> Result<Model> {
        // Already resolved
        if let Some(model) = resolved.get(name) {
            return Ok(model.clone());
        }

        // Check for circular inheritance
        if in_progress.contains(name) {
            return Err(SidemanticError::Validation(format!(
                "Circular inheritance detected for model '{name}'"
            )));
        }

        // Get the model
        let model = models
            .get(name)
            .ok_or_else(|| SidemanticError::Validation(format!("Model '{name}' not found")))?;

        // If no inheritance, just return as-is
        let parent_name = match extends_map.get(name) {
            Some(parent) => parent,
            None => {
                resolved.insert(name.to_string(), model.clone());
                return Ok(model.clone());
            }
        };

        // Resolve parent first
        in_progress.insert(name.to_string());
        let parent = resolve(parent_name, models, extends_map, resolved, in_progress)?;
        in_progress.remove(name);

        // Merge child with parent
        let merged = merge_model(model, &parent);
        resolved.insert(name.to_string(), merged.clone());
        Ok(merged)
    }

    // Resolve all models
    let names: Vec<_> = models.keys().cloned().collect();
    for name in names {
        resolve(&name, &models, extends_map, &mut resolved, &mut in_progress)?;
    }

    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{Dimension, Metric};

    #[test]
    fn test_merge_model_basic() {
        let parent = Model::new("base", "id")
            .with_table("base_table")
            .with_dimension(Dimension::categorical("status"))
            .with_metric(Metric::sum("revenue", "amount"));

        let child = Model::new("child", "id")
            .with_dimension(Dimension::categorical("category"))
            .with_metric(Metric::count("order_count"));

        let merged = merge_model(&child, &parent);

        assert_eq!(merged.name, "child");
        assert_eq!(merged.table, Some("base_table".to_string()));
        assert_eq!(merged.dimensions.len(), 2); // status + category
        assert_eq!(merged.metrics.len(), 2); // revenue + order_count
    }

    #[test]
    fn test_merge_model_override() {
        let parent = Model::new("base", "id")
            .with_table("base_table")
            .with_metric(Metric::sum("revenue", "amount"));

        let child = Model::new("child", "id")
            .with_table("child_table")
            .with_metric(Metric::sum("revenue", "total_amount")); // Override

        let merged = merge_model(&child, &parent);

        assert_eq!(merged.table, Some("child_table".to_string()));
        // Child's metric should override parent's
        let revenue = merged.metrics.iter().find(|m| m.name == "revenue").unwrap();
        assert_eq!(revenue.sql, Some("total_amount".to_string()));
    }

    #[test]
    fn test_resolve_inheritance() {
        let mut models = HashMap::new();
        models.insert(
            "base".to_string(),
            Model::new("base", "id")
                .with_table("base_table")
                .with_dimension(Dimension::categorical("status")),
        );
        models.insert(
            "child".to_string(),
            Model::new("child", "id").with_metric(Metric::count("order_count")),
        );

        let mut extends_map = HashMap::new();
        extends_map.insert("child".to_string(), "base".to_string());

        let resolved = resolve_model_inheritance(models, &extends_map).unwrap();

        let child = resolved.get("child").unwrap();
        assert_eq!(child.table, Some("base_table".to_string()));
        assert_eq!(child.dimensions.len(), 1);
        assert_eq!(child.metrics.len(), 1);
    }

    #[test]
    fn test_circular_inheritance_detected() {
        let mut models = HashMap::new();
        models.insert("a".to_string(), Model::new("a", "id"));
        models.insert("b".to_string(), Model::new("b", "id"));

        let mut extends_map = HashMap::new();
        extends_map.insert("a".to_string(), "b".to_string());
        extends_map.insert("b".to_string(), "a".to_string());

        let result = resolve_model_inheritance(models, &extends_map);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Circular"));
    }
}
