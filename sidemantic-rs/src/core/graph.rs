//! SemanticGraph: stores models and finds join paths

use std::collections::{HashMap, HashSet, VecDeque};

use crate::core::model::{Model, RelationshipType};
use crate::error::{Result, SidemanticError};

/// A step in a join path
#[derive(Debug, Clone)]
pub struct JoinStep {
    pub from_model: String,
    pub to_model: String,
    pub from_key: String,
    pub to_key: String,
    pub relationship_type: RelationshipType,
    /// Custom SQL join condition (overrides FK/PK join)
    pub custom_condition: Option<String>,
}

impl JoinStep {
    /// Check if this join step causes fan-out (row multiplication)
    /// Fan-out occurs when joining from "one" side to "many" side
    pub fn causes_fan_out(&self) -> bool {
        matches!(
            self.relationship_type,
            RelationshipType::OneToMany | RelationshipType::ManyToMany
        )
    }
}

/// A complete join path between two models
#[derive(Debug, Clone)]
pub struct JoinPath {
    pub steps: Vec<JoinStep>,
}

impl JoinPath {
    pub fn is_empty(&self) -> bool {
        self.steps.is_empty()
    }

    /// Check if any step in the path causes fan-out
    pub fn has_fan_out(&self) -> bool {
        self.steps.iter().any(|s| s.causes_fan_out())
    }

    /// Get all models that are on the "many" side of a fan-out join
    /// These models' metrics need symmetric aggregate handling
    pub fn fan_out_models(&self) -> Vec<&str> {
        self.steps
            .iter()
            .filter(|s| s.causes_fan_out())
            .map(|s| s.to_model.as_str())
            .collect()
    }

    /// Get the first model where fan-out occurs (the boundary)
    pub fn fan_out_boundary(&self) -> Option<&str> {
        self.steps
            .iter()
            .find(|s| s.causes_fan_out())
            .map(|s| s.to_model.as_str())
    }
}

/// Edge in the adjacency list: (target_model, fk, pk, relationship_type, custom_sql)
type AdjacencyEdge = (String, String, String, RelationshipType, Option<String>);

/// The semantic graph holds all models and their relationships
#[derive(Debug, Default)]
pub struct SemanticGraph {
    models: HashMap<String, Model>,
    /// Adjacency list: model -> edges
    adjacency: HashMap<String, Vec<AdjacencyEdge>>,
}

impl SemanticGraph {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a model to the graph
    pub fn add_model(&mut self, model: Model) -> Result<()> {
        let name = model.name.clone();

        // Validate model
        if model.table.is_none() && model.sql.is_none() {
            return Err(SidemanticError::Validation(format!(
                "Model '{name}' must have either 'table' or 'sql' defined"
            )));
        }

        self.models.insert(name, model);
        self.rebuild_adjacency();
        Ok(())
    }

    /// Get a model by name
    pub fn get_model(&self, name: &str) -> Option<&Model> {
        self.models.get(name)
    }

    /// Get all models
    pub fn models(&self) -> impl Iterator<Item = &Model> {
        self.models.values()
    }

    /// Rebuild the adjacency list from model relationships
    fn rebuild_adjacency(&mut self) {
        self.adjacency.clear();

        for model in self.models.values() {
            let edges = self.adjacency.entry(model.name.clone()).or_default();

            for rel in &model.relationships {
                edges.push((
                    rel.name.clone(),
                    rel.fk(),
                    rel.pk(),
                    rel.r#type.clone(),
                    rel.sql.clone(),
                ));
            }

            // Add reverse edges for relationships
            for rel in &model.relationships {
                let reverse_type = match rel.r#type {
                    RelationshipType::ManyToOne => RelationshipType::OneToMany,
                    RelationshipType::OneToMany => RelationshipType::ManyToOne,
                    RelationshipType::OneToOne => RelationshipType::OneToOne,
                    RelationshipType::ManyToMany => RelationshipType::ManyToMany,
                };

                // For reverse edges, swap {from} and {to} in custom SQL
                let reverse_sql = rel.sql.as_ref().map(|sql| {
                    sql.replace("{from}", "__TEMP__")
                        .replace("{to}", "{from}")
                        .replace("__TEMP__", "{to}")
                });

                self.adjacency.entry(rel.name.clone()).or_default().push((
                    model.name.clone(),
                    rel.pk(),
                    rel.fk(),
                    reverse_type,
                    reverse_sql,
                ));
            }
        }
    }

    /// Find the shortest join path between two models using BFS
    pub fn find_join_path(&self, from: &str, to: &str) -> Result<JoinPath> {
        if from == to {
            return Ok(JoinPath { steps: Vec::new() });
        }

        if !self.models.contains_key(from) {
            let available: Vec<&str> = self.models.keys().map(|s| s.as_str()).collect();
            return Err(SidemanticError::model_not_found(from, &available));
        }
        if !self.models.contains_key(to) {
            let available: Vec<&str> = self.models.keys().map(|s| s.as_str()).collect();
            return Err(SidemanticError::model_not_found(to, &available));
        }

        // BFS to find shortest path
        let mut visited: HashSet<String> = HashSet::new();
        let mut queue: VecDeque<(String, Vec<JoinStep>)> = VecDeque::new();

        visited.insert(from.to_string());
        queue.push_back((from.to_string(), Vec::new()));

        while let Some((current, path)) = queue.pop_front() {
            if let Some(edges) = self.adjacency.get(&current) {
                for (target, fk, pk, rel_type, custom_sql) in edges {
                    if !visited.contains(target) {
                        let mut new_path = path.clone();
                        new_path.push(JoinStep {
                            from_model: current.clone(),
                            to_model: target.clone(),
                            from_key: fk.clone(),
                            to_key: pk.clone(),
                            relationship_type: rel_type.clone(),
                            custom_condition: custom_sql.clone(),
                        });

                        if target == to {
                            return Ok(JoinPath { steps: new_path });
                        }

                        visited.insert(target.clone());
                        queue.push_back((target.clone(), new_path));
                    }
                }
            }
        }

        Err(SidemanticError::NoJoinPath {
            from: from.to_string(),
            to: to.to_string(),
        })
    }

    /// Parse a qualified reference (model.field) and return (model_name, field_name, granularity)
    pub fn parse_reference(&self, reference: &str) -> Result<(String, String, Option<String>)> {
        let parts: Vec<&str> = reference.split('.').collect();
        if parts.len() != 2 {
            return Err(SidemanticError::InvalidReference {
                reference: reference.to_string(),
            });
        }

        let model_name = parts[0];
        let field_with_granularity = parts[1];

        // Check for granularity suffix (e.g., order_date__month)
        let (field_name, granularity) = if let Some(pos) = field_with_granularity.find("__") {
            let (field, gran) = field_with_granularity.split_at(pos);
            (field.to_string(), Some(gran[2..].to_string()))
        } else {
            (field_with_granularity.to_string(), None)
        };

        // Verify model exists
        if !self.models.contains_key(model_name) {
            let available: Vec<&str> = self.models.keys().map(|s| s.as_str()).collect();
            return Err(SidemanticError::model_not_found(model_name, &available));
        }

        Ok((model_name.to_string(), field_name, granularity))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::model::{Dimension, Metric, Relationship};

    fn create_test_graph() -> SemanticGraph {
        let mut graph = SemanticGraph::new();

        let orders = Model::new("orders", "order_id")
            .with_table("orders")
            .with_dimension(Dimension::categorical("status"))
            .with_dimension(Dimension::time("order_date"))
            .with_metric(Metric::sum("revenue", "amount"))
            .with_relationship(Relationship::many_to_one("customers"));

        let customers = Model::new("customers", "id")
            .with_table("customers")
            .with_dimension(Dimension::categorical("name"))
            .with_dimension(Dimension::categorical("country"));

        graph.add_model(orders).unwrap();
        graph.add_model(customers).unwrap();

        graph
    }

    #[test]
    fn test_add_and_get_model() {
        let graph = create_test_graph();
        assert!(graph.get_model("orders").is_some());
        assert!(graph.get_model("customers").is_some());
        assert!(graph.get_model("nonexistent").is_none());
    }

    #[test]
    fn test_find_join_path() {
        let graph = create_test_graph();

        // Same model - empty path
        let path = graph.find_join_path("orders", "orders").unwrap();
        assert!(path.is_empty());

        // Direct relationship
        let path = graph.find_join_path("orders", "customers").unwrap();
        assert_eq!(path.steps.len(), 1);
        assert_eq!(path.steps[0].from_model, "orders");
        assert_eq!(path.steps[0].to_model, "customers");
        assert_eq!(path.steps[0].from_key, "customers_id");
        assert_eq!(path.steps[0].to_key, "id");

        // Reverse relationship
        let path = graph.find_join_path("customers", "orders").unwrap();
        assert_eq!(path.steps.len(), 1);
    }

    #[test]
    fn test_parse_reference() {
        let graph = create_test_graph();

        let (model, field, gran) = graph.parse_reference("orders.status").unwrap();
        assert_eq!(model, "orders");
        assert_eq!(field, "status");
        assert!(gran.is_none());

        let (model, field, gran) = graph.parse_reference("orders.order_date__month").unwrap();
        assert_eq!(model, "orders");
        assert_eq!(field, "order_date");
        assert_eq!(gran.unwrap(), "month");
    }

    #[test]
    fn test_fan_out_detection() {
        let graph = create_test_graph();

        // orders -> customers is many_to_one (no fan-out)
        let path = graph.find_join_path("orders", "customers").unwrap();
        assert!(!path.has_fan_out());
        assert!(path.fan_out_models().is_empty());
        assert!(path.fan_out_boundary().is_none());

        // customers -> orders is one_to_many (causes fan-out)
        let path = graph.find_join_path("customers", "orders").unwrap();
        assert!(path.has_fan_out());
        assert_eq!(path.fan_out_models(), vec!["orders"]);
        assert_eq!(path.fan_out_boundary(), Some("orders"));
    }

    #[test]
    fn test_custom_join_condition() {
        let mut graph = SemanticGraph::new();

        // Create models with custom join condition
        let orders = Model::new("orders", "order_id")
            .with_table("orders")
            .with_relationship(
                Relationship::many_to_one("customers")
                    .with_condition("{from}.customer_id = {to}.id AND {to}.active = true"),
            );

        let customers = Model::new("customers", "id").with_table("customers");

        graph.add_model(orders).unwrap();
        graph.add_model(customers).unwrap();

        // Verify custom condition is preserved in join path
        let path = graph.find_join_path("orders", "customers").unwrap();
        assert_eq!(path.steps.len(), 1);
        assert!(path.steps[0].custom_condition.is_some());
        assert!(path.steps[0]
            .custom_condition
            .as_ref()
            .unwrap()
            .contains("{from}.customer_id = {to}.id"));
    }
}
