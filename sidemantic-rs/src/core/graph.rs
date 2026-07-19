//! SemanticGraph: stores models and finds join paths

use std::collections::{HashMap, HashSet, VecDeque};

use crate::core::extract_dependencies;
use crate::core::model::{DimensionType, Metric, MetricType, Model, RelationshipType};
use crate::core::Parameter;
use crate::core::TableCalculation;
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
#[derive(Debug, Default, Clone)]
pub struct SemanticGraph {
    models: HashMap<String, Model>,
    metrics: HashMap<String, Metric>,
    model_metrics: HashMap<String, Metric>,
    table_calculations: HashMap<String, TableCalculation>,
    parameters: HashMap<String, Parameter>,
    /// Adjacency list: model -> edges
    adjacency: HashMap<String, Vec<AdjacencyEdge>>,
    /// Graph-level metadata payload (e.g. format-specific import/export state).
    metadata: Option<serde_json::Value>,
}

impl SemanticGraph {
    pub fn new() -> Self {
        Self::default()
    }

    fn validate_model(model: &Model) -> Result<()> {
        if model.table.is_none() && model.sql.is_none() && model.source_uri.is_none() {
            return Err(SidemanticError::Validation(format!(
                "Model '{}' must have one of 'table', 'sql', or 'source_uri' defined",
                model.name
            )));
        }

        Self::validate_unique_model_names(model)?;
        Self::validate_default_time_dimension(model)?;
        Self::validate_pre_aggregation_references(model)?;

        Ok(())
    }

    fn validate_unique_model_names(model: &Model) -> Result<()> {
        let mut seen = HashSet::new();
        for dimension in &model.dimensions {
            if !seen.insert(dimension.name.as_str()) {
                return Err(SidemanticError::Validation(format!(
                    "Model '{}' has duplicate dimension '{}'",
                    model.name, dimension.name
                )));
            }
        }

        let mut seen = HashSet::new();
        for metric in &model.metrics {
            if !seen.insert(metric.name.as_str()) {
                return Err(SidemanticError::Validation(format!(
                    "Model '{}' has duplicate metric '{}'",
                    model.name, metric.name
                )));
            }
        }

        let mut seen = HashSet::new();
        for segment in &model.segments {
            if !seen.insert(segment.name.as_str()) {
                return Err(SidemanticError::Validation(format!(
                    "Model '{}' has duplicate segment '{}'",
                    model.name, segment.name
                )));
            }
        }

        let mut seen = HashSet::new();
        for preagg in &model.pre_aggregations {
            if !seen.insert(preagg.name.as_str()) {
                return Err(SidemanticError::Validation(format!(
                    "Model '{}' has duplicate pre-aggregation '{}'",
                    model.name, preagg.name
                )));
            }
        }

        Ok(())
    }

    fn validate_default_time_dimension(model: &Model) -> Result<()> {
        let Some(default_time_dimension) = model.default_time_dimension.as_deref() else {
            return Ok(());
        };
        let Some(dimension) = model.get_dimension(default_time_dimension) else {
            return Err(SidemanticError::Validation(format!(
                "Model '{}' default_time_dimension '{}' does not reference a dimension",
                model.name, default_time_dimension
            )));
        };
        if dimension.r#type != DimensionType::Time {
            return Err(SidemanticError::Validation(format!(
                "Model '{}' default_time_dimension '{}' must reference a time dimension",
                model.name, default_time_dimension
            )));
        }
        Ok(())
    }

    fn validate_pre_aggregation_references(model: &Model) -> Result<()> {
        for preagg in &model.pre_aggregations {
            if let Some(measures) = preagg.measures.as_ref() {
                for measure in measures {
                    if model.get_metric(measure).is_none() {
                        return Err(SidemanticError::Validation(format!(
                            "Pre-aggregation '{}.{}' references unknown measure '{}'",
                            model.name, preagg.name, measure
                        )));
                    }
                }
            }

            if let Some(dimensions) = preagg.dimensions.as_ref() {
                for dimension in dimensions {
                    if model.get_dimension(dimension).is_none() {
                        return Err(SidemanticError::Validation(format!(
                            "Pre-aggregation '{}.{}' references unknown dimension '{}'",
                            model.name, preagg.name, dimension
                        )));
                    }
                }
            }

            if let Some(time_dimension) = preagg.time_dimension.as_deref() {
                let Some(dimension) = model.get_dimension(time_dimension) else {
                    return Err(SidemanticError::Validation(format!(
                        "Pre-aggregation '{}.{}' references unknown time_dimension '{}'",
                        model.name, preagg.name, time_dimension
                    )));
                };
                if dimension.r#type != DimensionType::Time {
                    return Err(SidemanticError::Validation(format!(
                        "Pre-aggregation '{}.{}' time_dimension '{}' must reference a time dimension",
                        model.name, preagg.name, time_dimension
                    )));
                }
            }
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

        self.index_model_metrics(&model);

        self.models.insert(name, model);
        self.rebuild_adjacency();
        Ok(())
    }

    /// Add or replace a model in the graph.
    pub fn replace_model(&mut self, model: Model) -> Result<()> {
        let name = model.name.clone();

        Self::validate_model(&model)?;
        self.models.insert(name, model);
        self.rebuild_model_metric_index();
        self.rebuild_adjacency();
        Ok(())
    }

    fn is_indexed_model_metric(metric: &Metric) -> bool {
        matches!(
            metric.r#type,
            MetricType::TimeComparison | MetricType::Conversion
        )
    }

    fn index_model_metrics(&mut self, model: &Model) {
        for metric in &model.metrics {
            if Self::is_indexed_model_metric(metric) && self.get_metric(&metric.name).is_none() {
                self.model_metrics
                    .insert(metric.name.clone(), metric.clone());
            }
        }
    }

    fn rebuild_model_metric_index(&mut self) {
        self.model_metrics.clear();

        let mut model_names: Vec<String> = self.models.keys().cloned().collect();
        model_names.sort();
        for model_name in model_names {
            if let Some(model) = self.models.get(&model_name).cloned() {
                self.index_model_metrics(&model);
            }
        }
    }

    /// Get a model by name
    pub fn get_model(&self, name: &str) -> Option<&Model> {
        self.models.get(name)
    }

    /// Get all models
    pub fn models(&self) -> impl Iterator<Item = &Model> {
        self.models.values()
    }

    /// Add a graph-level metric without validating its dependencies.
    ///
    /// Use when reconstructing a graph whose metrics were already validated
    /// elsewhere (e.g. re-exporting a graph built by the Python layer), where
    /// insertion order may not match dependency order.
    pub fn add_metric_unvalidated(&mut self, metric: Metric) -> Result<()> {
        if self.get_metric(&metric.name).is_some() {
            return Err(SidemanticError::Validation(format!(
                "Measure '{}' already exists",
                metric.name
            )));
        }
        self.metrics.insert(metric.name.clone(), metric);
        Ok(())
    }

    /// Add a graph-level metric.
    pub fn add_metric(&mut self, metric: Metric) -> Result<()> {
        if self.get_metric(&metric.name).is_some() {
            return Err(SidemanticError::Validation(format!(
                "Measure '{}' already exists",
                metric.name
            )));
        }
        self.validate_metric_dependencies(&metric)?;
        self.metrics.insert(metric.name.clone(), metric);
        Ok(())
    }

    /// Validate that a metric's dependencies resolve against the current graph.
    ///
    /// Exposed so callers that register a batch of interdependent metrics out of
    /// dependency order (e.g. OSI import) can insert them all first, then
    /// validate once everything is present.
    pub fn validate_metric_dependencies(&self, metric: &Metric) -> Result<()> {
        for dependency in extract_dependencies(metric, Some(self)) {
            let dependency_name = dependency
                .rsplit_once('.')
                .map(|(_, name)| name)
                .unwrap_or(dependency.as_str());
            if dependency == metric.name || dependency_name == metric.name {
                return Err(SidemanticError::Validation(format!(
                    "Metric '{}' cannot reference itself",
                    metric.name
                )));
            }

            if Self::metric_uses_inline_aggregation(metric)
                && self.inline_aggregate_column_dependency_exists(&dependency)
            {
                continue;
            }

            if self.metric_dependency_exists(&dependency)? {
                continue;
            }

            return Err(SidemanticError::Validation(format!(
                "measure '{}' not found",
                dependency_name
            )));
        }
        Ok(())
    }

    fn metric_dependency_exists(&self, dependency: &str) -> Result<bool> {
        if let Some((model_name, metric_name)) = dependency.rsplit_once('.') {
            let Some(model) = self.models.get(model_name) else {
                let available: Vec<&str> = self.models.keys().map(|s| s.as_str()).collect();
                return Err(SidemanticError::model_not_found(model_name, &available));
            };
            return Ok(model.get_metric(metric_name).is_some());
        }

        if self.get_metric(dependency).is_some() {
            return Ok(true);
        }

        Ok(self
            .models
            .values()
            .any(|model| model.get_metric(dependency).is_some()))
    }

    fn metric_uses_inline_aggregation(metric: &Metric) -> bool {
        metric.r#type == MetricType::Derived
            && metric
                .sql
                .as_deref()
                .is_some_and(Self::sql_has_inline_aggregation)
    }

    fn sql_has_inline_aggregation(sql: &str) -> bool {
        let lower = sql.to_ascii_lowercase();
        let bytes = lower.as_bytes();
        let aggregate_names = [
            "sum",
            "avg",
            "count",
            "min",
            "max",
            "median",
            "stddev",
            "stddev_pop",
            "variance",
            "variance_pop",
        ];

        for name in aggregate_names {
            let mut start = 0;
            while let Some(offset) = lower[start..].find(name) {
                let name_start = start + offset;
                let name_end = name_start + name.len();
                let before_is_ident = name_start > 0
                    && (bytes[name_start - 1].is_ascii_alphanumeric()
                        || bytes[name_start - 1] == b'_');
                let after_is_ident = name_end < bytes.len()
                    && (bytes[name_end].is_ascii_alphanumeric() || bytes[name_end] == b'_');
                if before_is_ident || after_is_ident {
                    start = name_end;
                    continue;
                }

                if lower[name_end..].trim_start().starts_with('(') {
                    return true;
                }
                start = name_end;
            }
        }

        false
    }

    fn inline_aggregate_column_dependency_exists(&self, dependency: &str) -> bool {
        if let Some((model_name, _)) = dependency.rsplit_once('.') {
            return self.models.contains_key(model_name);
        }

        self.models.len() == 1
    }

    /// Get a graph-level metric by name.
    pub fn get_metric(&self, name: &str) -> Option<&Metric> {
        self.metrics
            .get(name)
            .or_else(|| self.model_metrics.get(name))
    }

    /// Get all graph-level metrics.
    pub fn metrics(&self) -> impl Iterator<Item = &Metric> {
        self.metrics.values().chain(self.model_metrics.values())
    }

    /// Add a graph-level table calculation.
    pub fn add_table_calculation(&mut self, calc: TableCalculation) -> Result<()> {
        if self.table_calculations.contains_key(&calc.name) {
            return Err(SidemanticError::Validation(format!(
                "Table calculation '{}' already exists",
                calc.name
            )));
        }
        self.table_calculations.insert(calc.name.clone(), calc);
        Ok(())
    }

    /// Get a graph-level table calculation by name.
    pub fn get_table_calculation(&self, name: &str) -> Option<&TableCalculation> {
        self.table_calculations.get(name)
    }

    /// Get all graph-level table calculations.
    pub fn table_calculations(&self) -> impl Iterator<Item = &TableCalculation> {
        self.table_calculations.values()
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

    /// Get the graph-level metadata payload, if any.
    pub fn metadata(&self) -> Option<&serde_json::Value> {
        self.metadata.as_ref()
    }

    /// Replace the graph-level metadata payload.
    pub fn set_metadata(&mut self, metadata: serde_json::Value) {
        self.metadata = Some(metadata);
    }

    /// Mutable access to the graph-level metadata payload.
    pub fn metadata_mut(&mut self) -> &mut Option<serde_json::Value> {
        &mut self.metadata
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

                        let (source_fks, target_fks) = rel.junction_key_columns();
                        if source_fks.is_empty() || target_fks.is_empty() {
                            continue;
                        }

                        let source_pk = model.primary_keys();
                        let target_pk =
                            if rel.primary_key.is_some() || rel.primary_key_columns.is_some() {
                                rel.primary_key_columns()
                            } else {
                                self.models
                                    .get(&rel.name)
                                    .map(|target_model| target_model.primary_keys())
                                    .unwrap_or_default()
                            };
                        if source_pk.is_empty()
                            || target_pk.is_empty()
                            || source_pk.len() != source_fks.len()
                            || target_pk.len() != target_fks.len()
                        {
                            continue;
                        }

                        // source -> through (one_to_many)
                        self.adjacency.entry(model.name.clone()).or_default().push((
                            through_name.clone(),
                            source_pk.clone(),
                            source_fks.clone(),
                            RelationshipType::OneToMany,
                            None,
                        ));
                        // through -> source (many_to_one)
                        self.adjacency
                            .entry(through_name.clone())
                            .or_default()
                            .push((
                                model.name.clone(),
                                source_fks,
                                source_pk,
                                RelationshipType::ManyToOne,
                                None,
                            ));

                        // through -> target (many_to_one)
                        self.adjacency
                            .entry(through_name.clone())
                            .or_default()
                            .push((
                                rel.name.clone(),
                                target_fks.clone(),
                                target_pk.clone(),
                                RelationshipType::ManyToOne,
                                None,
                            ));
                        // target -> through (one_to_many)
                        self.adjacency.entry(rel.name.clone()).or_default().push((
                            through_name.clone(),
                            target_pk,
                            target_fks,
                            RelationshipType::OneToMany,
                            None,
                        ));
                        continue;
                    }
                }

                let fk_keys = rel.declared_foreign_key_columns();
                let pk_keys = if rel.primary_key.is_some() || rel.primary_key_columns.is_some() {
                    rel.declared_primary_key_columns()
                } else {
                    self.models
                        .get(&rel.name)
                        .map(|target_model| target_model.primary_keys())
                        .unwrap_or_default()
                };

                let (from_keys, to_keys) = match rel.r#type {
                    RelationshipType::ManyToOne | RelationshipType::OneToOne => {
                        (fk_keys.clone(), pk_keys.clone())
                    }
                    RelationshipType::OneToMany | RelationshipType::ManyToMany => {
                        (pk_keys.clone(), fk_keys.clone())
                    }
                };

                if rel.sql.is_none()
                    && (from_keys.is_empty()
                        || to_keys.is_empty()
                        || from_keys.len() != to_keys.len())
                {
                    continue;
                }

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

                let fk_keys = rel.declared_foreign_key_columns();
                let pk_keys = if rel.primary_key.is_some() || rel.primary_key_columns.is_some() {
                    rel.declared_primary_key_columns()
                } else {
                    self.models
                        .get(&rel.name)
                        .map(|target_model| target_model.primary_keys())
                        .unwrap_or_default()
                };

                let (reverse_from_keys, reverse_to_keys) = match rel.r#type {
                    RelationshipType::ManyToOne | RelationshipType::OneToOne => {
                        (pk_keys.clone(), fk_keys.clone())
                    }
                    RelationshipType::OneToMany | RelationshipType::ManyToMany => {
                        (fk_keys.clone(), pk_keys.clone())
                    }
                };

                if rel.sql.is_none()
                    && (reverse_from_keys.is_empty()
                        || reverse_to_keys.is_empty()
                        || reverse_from_keys.len() != reverse_to_keys.len())
                {
                    continue;
                }

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
        let (field_name, granularity) =
            if let Some((field, gran)) = field_with_granularity.rsplit_once("__") {
                if field.is_empty() || gran.is_empty() {
                    return Err(SidemanticError::InvalidReference {
                        reference: reference.to_string(),
                    });
                }
                (field.to_string(), Some(gran.to_string()))
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
    use crate::core::model::{
        ComparisonType, Dimension, Metric, PreAggregation, PreAggregationType, Relationship,
    };
    use crate::core::parameter::{Parameter, ParameterType};

    fn create_test_graph() -> SemanticGraph {
        let mut graph = SemanticGraph::new();

        let orders = Model::new("orders", "order_id")
            .with_table("orders")
            .with_dimension(Dimension::categorical("status"))
            .with_dimension(Dimension::time("order_date"))
            .with_metric(Metric::sum("revenue", "amount"))
            .with_relationship(
                Relationship::many_to_one("customers").with_keys("customers_id", "id"),
            );

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
    fn test_replace_model_updates_indexed_model_metrics() {
        let mut graph = SemanticGraph::new();

        graph
            .add_model(
                Model::new("orders", "order_id")
                    .with_table("orders")
                    .with_metric(Metric::sum("revenue", "amount"))
                    .with_metric(Metric::time_comparison(
                        "revenue_yoy",
                        "revenue",
                        ComparisonType::Yoy,
                    )),
            )
            .unwrap();
        assert!(graph.get_metric("revenue_yoy").is_some());

        graph
            .replace_model(
                Model::new("orders", "order_id")
                    .with_table("orders")
                    .with_metric(Metric::sum("revenue", "amount"))
                    .with_metric(Metric::time_comparison(
                        "revenue_mom",
                        "revenue",
                        ComparisonType::Mom,
                    )),
            )
            .unwrap();

        assert!(graph.get_metric("revenue_yoy").is_none());
        assert!(graph.get_metric("revenue_mom").is_some());
        graph
            .add_metric(Metric::sum("revenue_yoy", "amount"))
            .unwrap();
    }

    #[test]
    fn test_source_uri_model_is_valid_for_loading() {
        let mut graph = SemanticGraph::new();
        let mut model = Model::new("events", "event_id");
        model.source_uri = Some("s3://warehouse/events.parquet".to_string());

        graph.add_model(model).unwrap();

        assert_eq!(
            graph.get_model("events").unwrap().source_uri.as_deref(),
            Some("s3://warehouse/events.parquet")
        );
    }

    #[test]
    fn test_rejects_duplicate_dimension_names() {
        let mut graph = SemanticGraph::new();
        let model = Model::new("orders", "order_id")
            .with_table("orders")
            .with_dimension(Dimension::categorical("status"))
            .with_dimension(Dimension::categorical("status"));

        let err = graph.add_model(model).unwrap_err();
        assert!(err.to_string().contains("duplicate dimension 'status'"));
    }

    #[test]
    fn test_rejects_invalid_default_time_dimension() {
        let mut graph = SemanticGraph::new();
        let mut model = Model::new("orders", "order_id")
            .with_table("orders")
            .with_dimension(Dimension::categorical("status"));
        model.default_time_dimension = Some("status".to_string());

        let err = graph.add_model(model).unwrap_err();
        assert!(err
            .to_string()
            .contains("default_time_dimension 'status' must reference a time dimension"));
    }

    #[test]
    fn test_rejects_invalid_pre_aggregation_references() {
        let mut graph = SemanticGraph::new();
        let model = Model::new("orders", "order_id")
            .with_table("orders")
            .with_dimension(Dimension::time("created_at"))
            .with_pre_aggregation(PreAggregation {
                name: "monthly".to_string(),
                preagg_type: PreAggregationType::Rollup,
                measures: Some(vec!["missing_revenue".to_string()]),
                dimensions: Some(vec!["created_at".to_string()]),
                time_dimension: Some("created_at".to_string()),
                granularity: Some("month".to_string()),
                partition_granularity: None,
                build_range_start: None,
                build_range_end: None,
                scheduled_refresh: true,
                refresh_key: None,
                indexes: None,
                sql: None,
                meta: None,
            });

        let err = graph.add_model(model).unwrap_err();
        assert!(err
            .to_string()
            .contains("references unknown measure 'missing_revenue'"));
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
    fn test_one_to_many_omitted_key_does_not_create_join_edge() {
        let mut graph = SemanticGraph::new();

        let customers = Model::new("customers", "id")
            .with_table("customers")
            .with_relationship(Relationship::one_to_many("orders"));
        let orders = Model::new("orders", "id").with_table("orders");

        graph.add_model(customers).unwrap();
        graph.add_model(orders).unwrap();

        assert!(graph.find_join_path("customers", "orders").is_err());
        assert!(graph.find_join_path("orders", "customers").is_err());
    }

    #[test]
    fn test_many_to_one_omitted_key_does_not_create_join_edge() {
        let mut graph = SemanticGraph::new();

        let orders = Model::new("orders", "order_id")
            .with_table("orders")
            .with_relationship(Relationship::many_to_one("customers"));
        let customers = Model::new("customers", "customer_uid").with_table("customers");

        graph.add_model(orders).unwrap();
        graph.add_model(customers).unwrap();

        assert!(graph.find_join_path("orders", "customers").is_err());
        assert!(graph.find_join_path("customers", "orders").is_err());
    }

    #[test]
    fn test_keyless_target_does_not_create_incomplete_join_edge() {
        let mut graph = SemanticGraph::new();

        let orders = Model::new("orders", "order_id")
            .with_table("orders")
            .with_relationship(Relationship::many_to_one("customers"));
        let customers = Model::new("customers", "").with_table("customers");

        graph.add_model(orders).unwrap();
        graph.add_model(customers).unwrap();

        assert!(graph.find_join_path("orders", "customers").is_err());
        assert!(graph.find_join_path("customers", "orders").is_err());
    }

    #[test]
    fn test_one_to_one_omitted_key_does_not_create_join_edge() {
        let mut graph = SemanticGraph::new();

        let mut relationship = Relationship::new("profiles");
        relationship.r#type = RelationshipType::OneToOne;

        let users = Model::new("users", "id")
            .with_table("users")
            .with_relationship(relationship);
        let profiles = Model::new("profiles", "id").with_table("profiles");

        graph.add_model(users).unwrap();
        graph.add_model(profiles).unwrap();

        assert!(graph.find_join_path("users", "profiles").is_err());
        assert!(graph.find_join_path("profiles", "users").is_err());
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
                through_foreign_key_columns: None,
                related_foreign_key: None,
                related_foreign_key_columns: None,
                sql: None,
                metadata: None,
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
                through_foreign_key_columns: None,
                related_foreign_key: Some("product_id".to_string()),
                related_foreign_key_columns: None,
                sql: None,
                metadata: None,
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
    fn test_many_to_many_through_preserves_composite_primary_keys() {
        let mut graph = SemanticGraph::new();

        let orders = Model::new("orders", "tenant_id")
            .with_primary_key_columns(vec!["tenant_id".to_string(), "order_id".to_string()])
            .with_table("orders")
            .with_relationship(Relationship {
                name: "products".to_string(),
                r#type: RelationshipType::ManyToMany,
                foreign_key: None,
                foreign_key_columns: None,
                primary_key: None,
                primary_key_columns: None,
                through: Some("order_items".to_string()),
                through_foreign_key: Some("order_id".to_string()),
                through_foreign_key_columns: Some(vec![
                    "tenant_id".to_string(),
                    "order_id".to_string(),
                ]),
                related_foreign_key: Some("product_id".to_string()),
                related_foreign_key_columns: Some(vec![
                    "tenant_id".to_string(),
                    "product_id".to_string(),
                ]),
                sql: None,
                metadata: None,
            });
        let order_items = Model::new("order_items", "id").with_table("order_items");
        let products = Model::new("products", "tenant_id")
            .with_primary_key_columns(vec!["tenant_id".to_string(), "product_id".to_string()])
            .with_table("products");

        graph.add_model(orders).unwrap();
        graph.add_model(order_items).unwrap();
        graph.add_model(products).unwrap();

        let path = graph.find_join_path("orders", "products").unwrap();
        assert_eq!(path.steps.len(), 2);
        assert_eq!(
            path.steps[0].from_keys,
            vec!["tenant_id".to_string(), "order_id".to_string()]
        );
        assert_eq!(
            path.steps[0].to_keys,
            vec!["tenant_id".to_string(), "order_id".to_string()]
        );
        assert_eq!(
            path.steps[1].from_keys,
            vec!["tenant_id".to_string(), "product_id".to_string()]
        );
        assert_eq!(
            path.steps[1].to_keys,
            vec!["tenant_id".to_string(), "product_id".to_string()]
        );
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
