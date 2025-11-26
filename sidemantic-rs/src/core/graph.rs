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
}

/// The semantic graph holds all models and their relationships
#[derive(Debug, Default)]
pub struct SemanticGraph {
    models: HashMap<String, Model>,
    /// Adjacency list: model -> [(target_model, fk, pk, relationship_type)]
    adjacency: HashMap<String, Vec<(String, String, String, RelationshipType)>>,
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
                "Model '{}' must have either 'table' or 'sql' defined",
                name
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

                self.adjacency
                    .entry(rel.name.clone())
                    .or_default()
                    .push((
                        model.name.clone(),
                        rel.pk(),
                        rel.fk(),
                        reverse_type,
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
            return Err(SidemanticError::ModelNotFound(from.to_string()));
        }
        if !self.models.contains_key(to) {
            return Err(SidemanticError::ModelNotFound(to.to_string()));
        }

        // BFS to find shortest path
        let mut visited: HashSet<String> = HashSet::new();
        let mut queue: VecDeque<(String, Vec<JoinStep>)> = VecDeque::new();

        visited.insert(from.to_string());
        queue.push_back((from.to_string(), Vec::new()));

        while let Some((current, path)) = queue.pop_front() {
            if let Some(edges) = self.adjacency.get(&current) {
                for (target, fk, pk, rel_type) in edges {
                    if !visited.contains(target) {
                        let mut new_path = path.clone();
                        new_path.push(JoinStep {
                            from_model: current.clone(),
                            to_model: target.clone(),
                            from_key: fk.clone(),
                            to_key: pk.clone(),
                            relationship_type: rel_type.clone(),
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
            return Err(SidemanticError::InvalidReference(format!(
                "Expected 'model.field' format, got '{}'",
                reference
            )));
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
            return Err(SidemanticError::ModelNotFound(model_name.to_string()));
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
}
