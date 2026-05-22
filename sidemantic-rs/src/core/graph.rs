//! SemanticGraph: stores models and finds join paths

use std::collections::{HashMap, HashSet, VecDeque};

use crate::core::model::{Model, RelationshipType};
use crate::core::Parameter;
use crate::error::{Result, SidemanticError};

/// A step in a join path
#[derive(Debug, Clone)]
pub struct JoinStep {
    pub from_model: String,
    pub to_model: String,
    pub from_key: String,
    pub to_key: String,
    pub from_keys: Vec<String>,
    pub to_keys: Vec<String>,
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

/// Edge in the adjacency list: (target_model, from_keys, to_keys, relationship_type, custom_sql)
type AdjacencyEdge = (
    String,
    Vec<String>,
    Vec<String>,
    RelationshipType,
    Option<String>,
);

/// The semantic graph holds all models and their relationships
#[derive(Debug, Default)]
pub struct SemanticGraph {
    models: HashMap<String, Model>,
    parameters: HashMap<String, Parameter>,
    /// Adjacency list: model -> edges
    adjacency: HashMap<String, Vec<AdjacencyEdge>>,
}

impl SemanticGraph {
    pub fn new() -> Self {
        Self::default()
    }

    fn validate_model(model: &Model) -> Result<()> {
        if model.table.is_none() && model.sql.is_none() {
            return Err(SidemanticError::Validation(format!(
                "Model '{}' must have either 'table' or 'sql' defined",
                model.name
            )));
        }

        Ok(())
    }

    /// Add a model to the graph
    pub fn add_model(&mut self, model: Model) -> Result<()> {
        let name = model.name.clone();

        Self::validate_model(&model)?;
        if self.models.contains_key(&name) {
            return Err(SidemanticError::Validation(format!(
                "Model '{name}' already exists"
            )));
        }

        self.models.insert(name, model);
        self.rebuild_adjacency();
        Ok(())
    }

    /// Add or replace a model in the graph.
    pub fn replace_model(&mut self, model: Model) -> Result<()> {
        let name = model.name.clone();

        Self::validate_model(&model)?;
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

    /// Add a parameter to the graph
    pub fn add_parameter(&mut self, parameter: Parameter) -> Result<()> {
        if self.parameters.contains_key(&parameter.name) {
            return Err(SidemanticError::Validation(format!(
                "Parameter '{}' already exists",
                parameter.name
            )));
        }
        self.parameters.insert(parameter.name.clone(), parameter);
        Ok(())
    }

    /// Get a parameter by name
    pub fn get_parameter(&self, name: &str) -> Option<&Parameter> {
        self.parameters.get(name)
    }

    /// Get all parameters
    pub fn parameters(&self) -> impl Iterator<Item = &Parameter> {
        self.parameters.values()
    }

    /// Rebuild the adjacency list from model relationships
    fn rebuild_adjacency(&mut self) {
        self.adjacency.clear();

        for model in self.models.values() {
            self.adjacency.entry(model.name.clone()).or_default();

            for rel in &model.relationships {
                if rel.r#type == RelationshipType::ManyToMany {
                    if let Some(through_name) = &rel.through {
                        let through_model_exists = self.models.contains_key(through_name);
                        let target_model_exists = self.models.contains_key(&rel.name);
                        if !through_model_exists || !target_model_exists {
                            continue;
                        }

                        let (source_fk_opt, target_fk_opt) = rel.junction_keys();
                        let (Some(source_fk), Some(target_fk)) = (source_fk_opt, target_fk_opt)
                        else {
                            continue;
                        };

                        let source_pk = model.primary_keys();
                        let target_pk =
                            if rel.primary_key.is_some() || rel.primary_key_columns.is_some() {
                                rel.primary_key_columns()
                            } else {
                                self.models
                                    .get(&rel.name)
                                    .map(|target_model| target_model.primary_keys())
                                    .unwrap_or_else(|| vec!["id".to_string()])
                            };
                        let source_pk_first = source_pk
                            .first()
                            .cloned()
                            .unwrap_or_else(|| "id".to_string());
                        let target_pk_first = target_pk
                            .first()
                            .cloned()
                            .unwrap_or_else(|| "id".to_string());

                        // source -> through (one_to_many)
                        self.adjacency.entry(model.name.clone()).or_default().push((
                            through_name.clone(),
                            vec![source_pk_first.clone()],
                            vec![source_fk.clone()],
                            RelationshipType::OneToMany,
                            None,
                        ));
                        // through -> source (many_to_one)
                        self.adjacency
                            .entry(through_name.clone())
                            .or_default()
                            .push((
                                model.name.clone(),
                                vec![source_fk],
                                vec![source_pk_first],
                                RelationshipType::ManyToOne,
                                None,
                            ));

                        // through -> target (many_to_one)
                        self.adjacency
                            .entry(through_name.clone())
                            .or_default()
                            .push((
                                rel.name.clone(),
                                vec![target_fk.clone()],
                                vec![target_pk_first.clone()],
                                RelationshipType::ManyToOne,
                                None,
                            ));
                        // target -> through (one_to_many)
                        self.adjacency.entry(rel.name.clone()).or_default().push((
                            through_name.clone(),
                            vec![target_pk_first],
                            vec![target_fk],
                            RelationshipType::OneToMany,
                            None,
                        ));
                        continue;
                    }
                }

                let fk_keys = rel.foreign_key_columns();
                let pk_keys = if rel.primary_key.is_some() || rel.primary_key_columns.is_some() {
                    rel.primary_key_columns()
                } else {
                    self.models
                        .get(&rel.name)
                        .map(|target_model| target_model.primary_keys())
                        .unwrap_or_else(|| vec!["id".to_string()])
                };

                let (from_keys, to_keys) = match rel.r#type {
                    RelationshipType::ManyToOne | RelationshipType::OneToOne => {
                        (fk_keys.clone(), pk_keys.clone())
                    }
                    RelationshipType::OneToMany | RelationshipType::ManyToMany => {
                        (pk_keys.clone(), fk_keys.clone())
                    }
                };

                self.adjacency.entry(model.name.clone()).or_default().push((
                    rel.name.clone(),
                    from_keys.clone(),
                    to_keys.clone(),
                    rel.r#type.clone(),
                    rel.sql.clone(),
                ));
            }

            // Add reverse edges for relationships
            for rel in &model.relationships {
                if rel.r#type == RelationshipType::ManyToMany && rel.through.is_some() {
                    continue;
                }

                // If the target model already declares an explicit reverse relationship,
                // don't synthesize another reverse edge. This avoids conflicting
                // FK/PK directions when both sides are configured.
                if self
                    .models
                    .get(&rel.name)
                    .and_then(|target| target.get_relationship(&model.name))
                    .is_some()
                {
                    continue;
                }

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

                let fk_keys = rel.foreign_key_columns();
                let pk_keys = if rel.primary_key.is_some() || rel.primary_key_columns.is_some() {
                    rel.primary_key_columns()
                } else {
                    self.models
                        .get(&rel.name)
                        .map(|target_model| target_model.primary_keys())
                        .unwrap_or_else(|| vec!["id".to_string()])
                };

                let (reverse_from_keys, reverse_to_keys) = match rel.r#type {
                    RelationshipType::ManyToOne | RelationshipType::OneToOne => {
                        (pk_keys.clone(), fk_keys.clone())
                    }
                    RelationshipType::OneToMany | RelationshipType::ManyToMany => {
                        (fk_keys.clone(), pk_keys.clone())
                    }
                };

                self.adjacency.entry(rel.name.clone()).or_default().push((
                    model.name.clone(),
                    reverse_from_keys,
                    reverse_to_keys,
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
                for (target, from_keys, to_keys, rel_type, custom_sql) in edges {
                    if !visited.contains(target) {
                        let mut new_path = path.clone();
                        let from_key = from_keys.first().cloned().unwrap_or_default();
                        let to_key = to_keys.first().cloned().unwrap_or_default();
                        new_path.push(JoinStep {
                            from_model: current.clone(),
                            to_model: target.clone(),
                            from_key,
                            to_key,
                            from_keys: from_keys.clone(),
                            to_keys: to_keys.clone(),
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
    use crate::core::parameter::{Parameter, ParameterType};

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
    fn test_replace_model_overwrites_existing_model() {
        let mut graph = SemanticGraph::new();

        graph
            .add_model(
                Model::new("orders", "order_id")
                    .with_table("orders")
                    .with_dimension(Dimension::categorical("status")),
            )
            .unwrap();

        graph
            .replace_model(
                Model::new("orders", "id")
                    .with_table("orders_v2")
                    .with_metric(Metric::count("order_count")),
            )
            .unwrap();

        let model = graph.get_model("orders").unwrap();
        assert_eq!(model.table.as_deref(), Some("orders_v2"));
        assert!(model.get_dimension("status").is_none());
        assert!(model.get_metric("order_count").is_some());
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
        assert_eq!(path.steps[0].from_keys, vec!["customers_id".to_string()]);
        assert_eq!(path.steps[0].to_keys, vec!["id".to_string()]);

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

    #[test]
    fn test_default_relationship_uses_target_primary_key() {
        let mut graph = SemanticGraph::new();

        let orders = Model::new("orders", "order_id")
            .with_table("orders")
            .with_relationship(Relationship {
                name: "customers".to_string(),
                r#type: RelationshipType::ManyToOne,
                foreign_key: Some("customer_id".to_string()),
                foreign_key_columns: None,
                primary_key: None,
                primary_key_columns: None,
                through: None,
                through_foreign_key: None,
                related_foreign_key: None,
                sql: None,
            });

        let customers = Model::new("customers", "customer_id").with_table("customers");

        graph.add_model(orders).unwrap();
        graph.add_model(customers).unwrap();

        let path = graph.find_join_path("orders", "customers").unwrap();
        assert_eq!(path.steps.len(), 1);
        assert_eq!(path.steps[0].from_key, "customer_id");
        assert_eq!(path.steps[0].to_key, "customer_id");
        assert_eq!(path.steps[0].from_keys, vec!["customer_id".to_string()]);
        assert_eq!(path.steps[0].to_keys, vec!["customer_id".to_string()]);
    }

    #[test]
    fn test_many_to_many_with_through_builds_two_hop_path() {
        let mut graph = SemanticGraph::new();

        let orders = Model::new("orders", "order_id")
            .with_table("orders")
            .with_relationship(Relationship {
                name: "products".to_string(),
                r#type: RelationshipType::ManyToMany,
                foreign_key: None,
                foreign_key_columns: None,
                primary_key: Some("product_id".to_string()),
                primary_key_columns: None,
                through: Some("order_items".to_string()),
                through_foreign_key: Some("order_id".to_string()),
                related_foreign_key: Some("product_id".to_string()),
                sql: None,
            });
        let order_items = Model::new("order_items", "id").with_table("order_items");
        let products = Model::new("products", "product_id").with_table("products");

        graph.add_model(orders).unwrap();
        graph.add_model(order_items).unwrap();
        graph.add_model(products).unwrap();

        let path = graph.find_join_path("orders", "products").unwrap();
        assert_eq!(path.steps.len(), 2);

        // orders -> order_items
        assert_eq!(path.steps[0].from_model, "orders");
        assert_eq!(path.steps[0].to_model, "order_items");
        assert_eq!(path.steps[0].from_key, "order_id");
        assert_eq!(path.steps[0].to_key, "order_id");
        assert_eq!(path.steps[0].from_keys, vec!["order_id".to_string()]);
        assert_eq!(path.steps[0].to_keys, vec!["order_id".to_string()]);
        assert_eq!(path.steps[0].relationship_type, RelationshipType::OneToMany);

        // order_items -> products
        assert_eq!(path.steps[1].from_model, "order_items");
        assert_eq!(path.steps[1].to_model, "products");
        assert_eq!(path.steps[1].from_key, "product_id");
        assert_eq!(path.steps[1].to_key, "product_id");
        assert_eq!(path.steps[1].from_keys, vec!["product_id".to_string()]);
        assert_eq!(path.steps[1].to_keys, vec!["product_id".to_string()]);
        assert_eq!(path.steps[1].relationship_type, RelationshipType::ManyToOne);
    }

    #[test]
    fn test_find_join_path_with_composite_keys() {
        let mut graph = SemanticGraph::new();

        let order_items = Model::new("order_items", "order_id")
            .with_primary_key_columns(vec!["order_id".to_string(), "item_id".to_string()])
            .with_table("order_items");
        let shipments = Model::new("shipments", "shipment_id")
            .with_table("shipments")
            .with_relationship(Relationship::many_to_one("order_items").with_key_columns(
                vec!["order_id".to_string(), "item_id".to_string()],
                vec!["order_id".to_string(), "item_id".to_string()],
            ));

        graph.add_model(order_items).unwrap();
        graph.add_model(shipments).unwrap();

        let path = graph.find_join_path("shipments", "order_items").unwrap();
        assert_eq!(path.steps.len(), 1);
        assert_eq!(path.steps[0].from_key, "order_id");
        assert_eq!(path.steps[0].to_key, "order_id");
        assert_eq!(
            path.steps[0].from_keys,
            vec!["order_id".to_string(), "item_id".to_string()]
        );
        assert_eq!(
            path.steps[0].to_keys,
            vec!["order_id".to_string(), "item_id".to_string()]
        );
    }

    #[test]
    fn test_add_model_duplicate_name() {
        let mut graph = SemanticGraph::new();
        let orders_one = Model::new("orders", "order_id").with_table("orders");
        let orders_two = Model::new("orders", "id").with_table("orders_v2");

        graph.add_model(orders_one).unwrap();
        let err = graph.add_model(orders_two).unwrap_err();
        assert!(err.to_string().contains("Model 'orders' already exists"));
    }

    #[test]
    fn test_add_parameter() {
        let mut graph = create_test_graph();
        let parameter = Parameter {
            name: "status".to_string(),
            parameter_type: ParameterType::String,
            description: None,
            label: None,
            default_value: Some(serde_json::Value::String("pending".to_string())),
            allowed_values: None,
            default_to_today: false,
        };
        graph.add_parameter(parameter).unwrap();
        assert!(graph.get_parameter("status").is_some());
    }

    #[test]
    fn test_add_parameter_duplicate() {
        let mut graph = create_test_graph();
        let parameter = Parameter {
            name: "status".to_string(),
            parameter_type: ParameterType::String,
            description: None,
            label: None,
            default_value: None,
            allowed_values: None,
            default_to_today: false,
        };
        graph.add_parameter(parameter.clone()).unwrap();
        let err = graph.add_parameter(parameter).unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }
}
