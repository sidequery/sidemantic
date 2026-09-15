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
    /// Stable identity of the declared relationship edge.
    pub edge_id: Option<String>,
    /// Custom SQL join condition (overrides FK/PK join)
    pub custom_condition: Option<String>,
}

impl JoinStep {
    /// Check if this join step causes fan-out (row multiplication)
    /// Fan-out occurs when joining from "one" side to "many" side
    pub fn causes_fan_out(&self) -> bool {
        matches!(
            self.relationship_type,
            RelationshipType::OneToMany | RelationshipType::ManyToMany | RelationshipType::Cross
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

/// Edge in the adjacency list: target, keys, relationship type, custom SQL, edge identity.
type AdjacencyEdge = (
    String,
    Vec<String>,
    Vec<String>,
    RelationshipType,
    Option<String>,
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
    /// Query instances remain separate from their canonical model declarations.
    role_models: HashMap<String, String>,
    role_owners: HashMap<String, String>,
    relationship_instances: HashMap<(String, String), String>,
    /// Graph-level metadata payload (e.g. format-specific import/export state).
    metadata: Option<serde_json::Value>,
    /// Explicit scope supplied by a semantic handoff, independent of SQL references.
    metric_owners: HashMap<String, String>,
    strict_metric_scope: bool,
}

impl SemanticGraph {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_metric_scopes(&mut self, owners: HashMap<String, String>) -> Result<()> {
        for (metric, owner) in &owners {
            if !self.metrics.contains_key(metric) || !self.models.contains_key(owner) {
                return Err(SidemanticError::InvalidConfig(format!(
                    "Invalid metric owner: {metric} -> {owner}"
                )));
            }
        }
        self.metric_owners = owners;
        self.strict_metric_scope = true;
        Ok(())
    }

    pub fn metric_owner(&self, metric: &str) -> Option<&str> {
        self.metric_owners.get(metric).map(String::as_str)
    }

    pub fn has_strict_metric_scope(&self) -> bool {
        self.strict_metric_scope
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

        self.models.insert(name.clone(), model);
        if let Err(error) = self.rebuild_adjacency() {
            self.models.remove(&name);
            self.rebuild_model_metric_index();
            self.rebuild_adjacency()?;
            return Err(error);
        }
        Ok(())
    }

    /// Add or replace a model in the graph.
    pub fn replace_model(&mut self, model: Model) -> Result<()> {
        let name = model.name.clone();

        Self::validate_model(&model)?;
        let previous = self.models.insert(name.clone(), model);
        self.rebuild_model_metric_index();
        if let Err(error) = self.rebuild_adjacency() {
            if let Some(previous) = previous {
                self.models.insert(name, previous);
            } else {
                self.models.remove(&name);
            }
            self.rebuild_model_metric_index();
            self.rebuild_adjacency()?;
            return Err(error);
        }
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
        self.models
            .get(self.role_models.get(name).map_or(name, String::as_str))
    }

    /// Physical key names on resolved join edges for a canonical declaration.
    pub(crate) fn join_key_names(&self, canonical: &str) -> HashSet<String> {
        let mut keys = HashSet::new();
        for (instance, edges) in &self.adjacency {
            for (target, from_keys, to_keys, _, _, _) in edges {
                if self
                    .get_model(instance)
                    .is_some_and(|model| model.name == canonical)
                {
                    keys.extend(from_keys.iter().cloned());
                }
                if self
                    .get_model(target)
                    .is_some_and(|model| model.name == canonical)
                {
                    keys.extend(to_keys.iter().cloned());
                }
            }
        }
        keys
    }

    /// Canonical declarations and independently addressable role instances.
    pub fn model_instances(&self) -> impl Iterator<Item = &str> {
        self.models
            .keys()
            .chain(self.role_models.keys())
            .map(String::as_str)
    }

    pub fn role_root_owner(&self, instance: &str) -> Option<&str> {
        let mut owner = self.role_owners.get(instance)?;
        while let Some(parent) = self.role_owners.get(owner) {
            owner = parent;
        }
        Some(owner)
    }

    /// Whether this SQL instance is a declared many-to-many junction population.
    pub(crate) fn is_bridge_instance(&self, instance: &str) -> bool {
        self.relationship_instances
            .iter()
            .any(|((source, name), target)| {
                let Some(model) = self.get_model(source) else {
                    return false;
                };
                model.relationships.iter().any(|relationship| {
                    if !relationship.active
                        || relationship.name != *name
                        || relationship.r#type != RelationshipType::ManyToMany
                    {
                        return false;
                    }
                    let Some(through) = &relationship.through else {
                        return false;
                    };
                    if source != &model.name || relationship.target_model.is_some() {
                        instance == format!("{target}$through")
                    } else {
                        instance == through
                    }
                })
            })
    }

    pub fn relationship_target_instance(
        &self,
        source: &str,
        relationship: &crate::core::Relationship,
    ) -> Option<&str> {
        if !relationship.active {
            return None;
        }
        self.relationship_instances
            .get(&(source.to_string(), relationship.name.clone()))
            .map(String::as_str)
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

    /// Register separate SQL instances for role relationships and nested paths.
    fn rebuild_role_instances(&mut self) -> Result<Vec<(String, String)>> {
        let mut names: Vec<String> = self.models.keys().cloned().collect();
        names.sort();
        let mut counts = HashMap::<String, usize>::new();
        for model in self.models.values() {
            for rel in &model.relationships {
                if rel.active
                    && rel.target_model.is_some()
                    && self.models.contains_key(rel.related_model())
                {
                    *counts.entry(rel.name.clone()).or_default() += 1;
                }
            }
        }
        if !counts.is_empty() && names.iter().any(|name| name.contains('$')) {
            return Err(SidemanticError::Validation(
                "Model names cannot contain '$' when relationship roles are used".into(),
            ));
        }
        let mut roles = HashMap::new();
        let mut owners = HashMap::new();
        let mut relationships = HashMap::new();
        let mut instances: Vec<(String, String)> = names
            .iter()
            .map(|name| (name.clone(), name.clone()))
            .collect();
        let mut pending: VecDeque<(String, String, usize)> = names
            .into_iter()
            .map(|name| (name.clone(), name, 0))
            .collect();
        while let Some((instance, canonical, depth)) = pending.pop_front() {
            if depth >= self.models.len() {
                continue;
            }
            let model = &self.models[&canonical];
            for rel in &model.relationships {
                if !rel.active || !self.models.contains_key(rel.related_model()) {
                    continue;
                }
                let is_role = depth > 0 || rel.target_model.is_some();
                let target = if depth > 0 || (rel.target_model.is_some() && counts[&rel.name] > 1) {
                    format!("{instance}${}", rel.name)
                } else if rel.target_model.is_some() {
                    rel.name.clone()
                } else {
                    rel.related_model().to_string()
                };
                if is_role {
                    if self.models.contains_key(&target) {
                        return Err(SidemanticError::Validation(format!(
                            "Relationship role instance '{target}' collides with canonical model '{target}'"
                        )));
                    }
                    if relationships.contains_key(&(instance.clone(), rel.name.clone()))
                        || roles.contains_key(&target)
                    {
                        return Err(SidemanticError::Validation(format!(
                            "Model '{instance}' declares relationship role '{}' more than once",
                            rel.name
                        )));
                    }
                    roles.insert(target.clone(), rel.related_model().to_string());
                    owners.insert(target.clone(), instance.clone());
                    if let Some(through) = &rel.through {
                        if rel.r#type == RelationshipType::ManyToMany {
                            // A bridge belongs to this relationship instance. Sharing
                            // its canonical alias mixes keys from alternate roles.
                            let bridge = format!("{target}$through");
                            if self.models.contains_key(&bridge) || roles.contains_key(&bridge) {
                                return Err(SidemanticError::Validation(format!(
                                    "Relationship bridge instance '{bridge}' collides with another model"
                                )));
                            }
                            roles.insert(bridge.clone(), through.clone());
                            owners.insert(bridge, instance.clone());
                        }
                    }
                    instances.push((target.clone(), rel.related_model().to_string()));
                    pending.push_back((target.clone(), rel.related_model().to_string(), depth + 1));
                }
                relationships.insert((instance.clone(), rel.name.clone()), target);
            }
        }
        self.role_models = roles;
        self.role_owners = owners;
        self.relationship_instances = relationships;
        Ok(instances)
    }

    /// Rebuild the adjacency list from model relationships.
    fn rebuild_adjacency(&mut self) -> Result<()> {
        let instances = self.rebuild_role_instances()?;
        self.adjacency.clear();

        for (instance, canonical) in instances {
            let model = &self.models[&canonical];
            self.adjacency.entry(instance.clone()).or_default();

            for rel in &model.relationships {
                if !rel.active {
                    continue;
                }
                let Some(related_instance) = self
                    .relationship_instances
                    .get(&(instance.clone(), rel.name.clone()))
                else {
                    continue;
                };
                if rel.r#type == RelationshipType::ManyToMany {
                    if let Some(through_name) = &rel.through {
                        let through_model_exists = self.models.contains_key(through_name);
                        let target_model_exists = self.models.contains_key(rel.related_model());
                        if !through_model_exists || !target_model_exists {
                            continue;
                        }

                        let through_instance =
                            if instance != canonical || rel.target_model.is_some() {
                                format!("{related_instance}$through")
                            } else {
                                through_name.clone()
                            };

                        let (source_fks, target_fks) = rel.junction_key_columns();
                        if source_fks.is_empty() || target_fks.is_empty() {
                            continue;
                        }

                        let source_pk = {
                            let keys = model.primary_keys();
                            if keys.is_empty() {
                                vec!["id".to_string()]
                            } else {
                                keys
                            }
                        };
                        let target_pk =
                            if rel.primary_key.is_some() || rel.primary_key_columns.is_some() {
                                rel.primary_key_columns()
                            } else {
                                self.models
                                    .get(rel.related_model())
                                    .map(|target_model| target_model.primary_keys())
                                    .unwrap_or_else(|| vec!["id".to_string()])
                            };
                        let target_pk = if target_pk.is_empty() {
                            vec!["id".to_string()]
                        } else {
                            target_pk
                        };

                        // source -> through (one_to_many)
                        self.adjacency.entry(instance.clone()).or_default().push((
                            through_instance.clone(),
                            source_pk.clone(),
                            source_fks.clone(),
                            RelationshipType::OneToMany,
                            None,
                            rel.edge_id.clone(),
                        ));
                        // through -> source (many_to_one)
                        self.adjacency
                            .entry(through_instance.clone())
                            .or_default()
                            .push((
                                instance.clone(),
                                source_fks,
                                source_pk,
                                RelationshipType::ManyToOne,
                                None,
                                rel.edge_id.clone(),
                            ));

                        // through -> target (many_to_one)
                        self.adjacency
                            .entry(through_instance.clone())
                            .or_default()
                            .push((
                                related_instance.clone(),
                                target_fks.clone(),
                                target_pk.clone(),
                                RelationshipType::ManyToOne,
                                None,
                                rel.edge_id.clone(),
                            ));
                        // target -> through (one_to_many)
                        self.adjacency
                            .entry(related_instance.clone())
                            .or_default()
                            .push((
                                through_instance,
                                target_pk,
                                target_fks,
                                RelationshipType::OneToMany,
                                None,
                                rel.edge_id.clone(),
                            ));
                        continue;
                    }
                }

                let fk_keys = rel.foreign_key_columns();
                let pk_keys = if rel.primary_key.is_some() || rel.primary_key_columns.is_some() {
                    rel.primary_key_columns()
                } else if matches!(
                    rel.r#type,
                    RelationshipType::OneToMany | RelationshipType::OneToOne
                ) {
                    model.primary_keys()
                } else {
                    self.models
                        .get(rel.related_model())
                        .map(|target_model| target_model.primary_keys())
                        .unwrap_or_else(|| vec!["id".to_string()])
                };

                let (from_keys, to_keys) = match rel.r#type {
                    RelationshipType::Cross => (Vec::new(), Vec::new()),
                    RelationshipType::ManyToMany => {
                        if rel.primary_key.is_some() || rel.primary_key_columns.is_some() {
                            (fk_keys.clone(), pk_keys.clone())
                        } else {
                            (model.primary_keys(), fk_keys.clone())
                        }
                    }
                    RelationshipType::ManyToOne => (fk_keys.clone(), pk_keys.clone()),
                    RelationshipType::OneToMany | RelationshipType::OneToOne => {
                        (pk_keys.clone(), fk_keys.clone())
                    }
                };

                self.adjacency.entry(instance.clone()).or_default().push((
                    related_instance.clone(),
                    from_keys.clone(),
                    to_keys.clone(),
                    if rel.r#type == RelationshipType::ManyToMany {
                        RelationshipType::OneToMany
                    } else {
                        rel.r#type.clone()
                    },
                    if rel.r#type == RelationshipType::Cross {
                        None
                    } else {
                        rel.sql.clone()
                    },
                    rel.edge_id.clone(),
                ));
            }

            // Add reverse edges for relationships
            for rel in &model.relationships {
                if !rel.active {
                    continue;
                }
                let Some(related_instance) = self
                    .relationship_instances
                    .get(&(instance.clone(), rel.name.clone()))
                else {
                    continue;
                };
                if rel.r#type == RelationshipType::ManyToMany && rel.through.is_some() {
                    continue;
                }

                // If the target model already declares an explicit reverse relationship,
                // don't synthesize another reverse edge. This avoids conflicting
                // FK/PK directions when both sides are configured.
                if instance == canonical
                    && rel.target_model.is_none()
                    && self.models.get(rel.related_model()).is_some_and(|target| {
                        target.relationships.iter().any(|reverse| {
                            reverse.active
                                && reverse.target_model.is_none()
                                && reverse.related_model() == canonical
                        })
                    })
                {
                    continue;
                }

                let reverse_type = match rel.r#type {
                    RelationshipType::ManyToOne => RelationshipType::OneToMany,
                    RelationshipType::OneToMany => RelationshipType::ManyToOne,
                    RelationshipType::OneToOne => RelationshipType::OneToOne,
                    RelationshipType::ManyToMany => RelationshipType::ManyToOne,
                    RelationshipType::Cross => RelationshipType::Cross,
                };

                // For reverse edges, swap {from} and {to} in custom SQL
                let reverse_sql = rel
                    .sql
                    .as_ref()
                    .filter(|_| rel.r#type != RelationshipType::Cross)
                    .map(|sql| {
                        sql.replace("{from}", "__TEMP__")
                            .replace("{to}", "{from}")
                            .replace("__TEMP__", "{to}")
                    });

                let fk_keys = rel.foreign_key_columns();
                let pk_keys = if rel.primary_key.is_some() || rel.primary_key_columns.is_some() {
                    rel.primary_key_columns()
                } else if matches!(
                    rel.r#type,
                    RelationshipType::OneToMany | RelationshipType::OneToOne
                ) {
                    model.primary_keys()
                } else {
                    self.models
                        .get(rel.related_model())
                        .map(|target_model| target_model.primary_keys())
                        .unwrap_or_else(|| vec!["id".to_string()])
                };

                let (reverse_from_keys, reverse_to_keys) = match rel.r#type {
                    RelationshipType::Cross => (Vec::new(), Vec::new()),
                    RelationshipType::ManyToMany => {
                        if rel.primary_key.is_some() || rel.primary_key_columns.is_some() {
                            (pk_keys.clone(), fk_keys.clone())
                        } else {
                            (fk_keys.clone(), model.primary_keys())
                        }
                    }
                    RelationshipType::ManyToOne => (pk_keys.clone(), fk_keys.clone()),
                    RelationshipType::OneToMany | RelationshipType::OneToOne => {
                        (fk_keys.clone(), pk_keys.clone())
                    }
                };

                self.adjacency
                    .entry(related_instance.clone())
                    .or_default()
                    .push((
                        instance.clone(),
                        reverse_from_keys,
                        reverse_to_keys,
                        reverse_type,
                        reverse_sql,
                        rel.edge_id.clone(),
                    ));
            }
        }
        Ok(())
    }

    /// Find a unique shortest join path, rejecting distinct equal-length routes.
    pub fn find_join_path(&self, from: &str, to: &str) -> Result<JoinPath> {
        self.find_join_path_with_context(from, to, None)
    }

    pub fn find_join_path_with_context(
        &self,
        from: &str,
        to: &str,
        query_models: Option<&HashSet<String>>,
    ) -> Result<JoinPath> {
        for name in [from, to] {
            if self.get_model(name).is_none() {
                let available: Vec<&str> = self.models.keys().map(String::as_str).collect();
                return Err(SidemanticError::model_not_found(name, &available));
            }
        }
        if from == to {
            return Ok(JoinPath { steps: Vec::new() });
        }

        let mut queue = VecDeque::from([(
            from.to_string(),
            Vec::<JoinStep>::new(),
            HashSet::from([from.to_string()]),
        )]);
        let mut shortest = None;
        let mut candidates = Vec::<Vec<JoinStep>>::new();
        while let Some((current, path, visited)) = queue.pop_front() {
            if shortest.is_some_and(|length| path.len() >= length) {
                continue;
            }
            for (target, from_keys, to_keys, rel_type, custom_sql, edge_id) in
                self.adjacency.get(&current).into_iter().flatten()
            {
                if visited.contains(target) {
                    continue;
                }
                let mut next_path = path.clone();
                next_path.push(JoinStep {
                    from_model: current.clone(),
                    to_model: target.clone(),
                    from_key: from_keys.first().cloned().unwrap_or_default(),
                    to_key: to_keys.first().cloned().unwrap_or_default(),
                    from_keys: from_keys.clone(),
                    to_keys: to_keys.clone(),
                    relationship_type: rel_type.clone(),
                    edge_id: edge_id.clone(),
                    custom_condition: custom_sql.clone(),
                });
                if target == to {
                    shortest = Some(next_path.len());
                    let equivalent = |candidate: &Vec<JoinStep>| {
                        candidate.len() == next_path.len()
                            && candidate.iter().zip(&next_path).all(|(left, right)| {
                                left.from_model == right.from_model
                                    && left.to_model == right.to_model
                                    && left.relationship_type == right.relationship_type
                                    && left.edge_id == right.edge_id
                                    && left.custom_condition.as_deref().map(str::trim)
                                        == right.custom_condition.as_deref().map(str::trim)
                                    && (left.custom_condition.is_some()
                                        || (left.from_keys == right.from_keys
                                            && left.to_keys == right.to_keys))
                            })
                    };
                    if !candidates.iter().any(equivalent) {
                        candidates.push(next_path);
                    }
                } else {
                    let mut next_visited = visited.clone();
                    next_visited.insert(target.clone());
                    queue.push_back((target.clone(), next_path, next_visited));
                }
            }
        }
        if let Some(context) = query_models {
            let score = |path: &Vec<JoinStep>| {
                path.iter()
                    .take(path.len().saturating_sub(1))
                    .filter(|step| !context.contains(&step.to_model))
                    .count()
            };
            if let Some(best) = candidates.iter().map(score).min() {
                candidates.retain(|path| score(path) == best);
            }
        }
        match candidates.len() {
            0 => Err(SidemanticError::NoJoinPath {
                from: from.into(),
                to: to.into(),
            }),
            1 => Ok(JoinPath {
                steps: candidates.remove(0),
            }),
            _ => Err(SidemanticError::AmbiguousJoinPath {
                from: from.into(),
                to: to.into(),
            }),
        }
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

        let model = self.get_model(model_name).ok_or_else(|| {
            let available: Vec<&str> = self.models.keys().map(|s| s.as_str()).collect();
            SidemanticError::model_not_found(model_name, &available)
        })?;

        // Exact public fields win over the granularity syntax, including names
        // resembling internal helpers such as __fanout_rank_0.
        let (field_name, granularity) = if model.get_metric(field_with_granularity).is_some()
            || model.get_dimension(field_with_granularity).is_some()
        {
            (field_with_granularity.to_string(), None)
        } else if let Some((field, gran)) = field_with_granularity.rsplit_once("__") {
            if field.is_empty() || gran.is_empty() {
                return Err(SidemanticError::InvalidReference {
                    reference: reference.to_string(),
                });
            }
            (field.to_string(), Some(gran.to_string()))
        } else {
            (field_with_granularity.to_string(), None)
        };

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

    #[test]
    fn exact_field_names_take_precedence_over_granularity_suffixes() {
        let mut graph = SemanticGraph::new();
        graph
            .add_model(
                Model::new("orders", "id")
                    .with_table("orders")
                    .with_metric(Metric::count("__fanout_rank_0"))
                    .with_metric(Metric::count("__sidemantic_filtered_2"))
                    .with_dimension(Dimension::categorical("__sidemantic_filtered_1_raw"))
                    .with_dimension(Dimension::categorical("literal__month"))
                    .with_dimension(Dimension::time("day")),
            )
            .unwrap();
        for name in [
            "__fanout_rank_0",
            "__sidemantic_filtered_2",
            "__sidemantic_filtered_1_raw",
            "literal__month",
        ] {
            assert_eq!(
                graph.parse_reference(&format!("orders.{name}")).unwrap(),
                ("orders".into(), name.into(), None)
            );
        }
        assert_eq!(
            graph.parse_reference("orders.day__month").unwrap(),
            ("orders".into(), "day".into(), Some("month".into()))
        );
    }

    fn role(name: &str, target: &str, key: &str) -> Relationship {
        let mut relationship = Relationship::many_to_one(name).with_keys(key, "id");
        relationship.target_model = Some(target.into());
        relationship.edge_id = Some(format!("edge_{name}"));
        relationship
    }

    #[test]
    fn role_instances_resolve_canonical_models_and_preserve_keys() {
        let mut graph = SemanticGraph::new();
        graph
            .add_model(
                Model::new("flights", "id")
                    .with_table("flights")
                    .with_relationship(role("origin", "airports", "origin_id"))
                    .with_relationship(role("destination", "airports", "destination_id")),
            )
            .unwrap();
        graph
            .add_model(
                Model::new("airports", "id")
                    .with_table("airports")
                    .with_dimension(Dimension::categorical("city")),
            )
            .unwrap();
        assert_eq!(graph.models().count(), 2);
        assert_eq!(graph.get_model("origin").unwrap().name, "airports");
        assert_eq!(
            graph.parse_reference("destination.city").unwrap().0,
            "destination"
        );
        for (alias, key) in [("origin", "origin_id"), ("destination", "destination_id")] {
            let path = graph.find_join_path("flights", alias).unwrap();
            assert_eq!(path.steps[0].to_model, alias);
            assert_eq!(path.steps[0].from_key, key);
            assert_eq!(path.steps[0].edge_id, Some(format!("edge_{alias}")));
            assert_eq!(graph.role_root_owner(alias), Some("flights"));
            let reverse = graph.find_join_path(alias, "flights").unwrap();
            assert_eq!(reverse.steps[0].to_key, key);
            assert!(reverse.has_fan_out());
        }
        assert!(matches!(
            graph.find_join_path("flights", "airports"),
            Err(SidemanticError::NoJoinPath { .. })
        ));
    }

    #[test]
    fn nested_and_repeated_roles_use_scoped_instances() {
        let mut graph = SemanticGraph::new();
        for owner in ["flights", "bookings"] {
            graph
                .add_model(
                    Model::new(owner, "id")
                        .with_table(owner)
                        .with_relationship(role("airport", "airports", "airport_id")),
                )
                .unwrap();
        }
        graph
            .add_model(
                Model::new("airports", "id")
                    .with_table("airports")
                    .with_relationship(role("country", "countries", "country_id")),
            )
            .unwrap();
        graph
            .add_model(
                Model::new("countries", "id")
                    .with_table("countries")
                    .with_dimension(Dimension::categorical("label")),
            )
            .unwrap();
        assert!(graph.get_model("airport").is_none());
        for owner in ["flights", "bookings"] {
            let nested = format!("{owner}$airport$country");
            assert_eq!(graph.get_model(&nested).unwrap().name, "countries");
            assert_eq!(graph.role_root_owner(&nested), Some(owner));
            assert_eq!(graph.find_join_path(owner, &nested).unwrap().steps.len(), 2);
        }
    }

    #[test]
    fn inactive_and_unresolved_roles_do_not_change_active_alias() {
        let mut graph = SemanticGraph::new();
        let mut inactive = role("airport", "airports", "archived_id");
        inactive.active = false;
        graph
            .add_model(
                Model::new("flights", "id")
                    .with_table("flights")
                    .with_relationship(role("airport", "airports", "airport_id"))
                    .with_relationship(inactive),
            )
            .unwrap();
        graph
            .add_model(
                Model::new("archives", "id")
                    .with_table("archives")
                    .with_relationship(role("airport", "missing", "airport_id")),
            )
            .unwrap();
        graph
            .add_model(Model::new("airports", "id").with_table("airports"))
            .unwrap();
        assert_eq!(
            graph.find_join_path("flights", "airport").unwrap().steps[0].from_key,
            "airport_id"
        );
        assert!(graph.get_model("flights$airport").is_none());
        let mut inactive = Relationship::many_to_one("airports");
        inactive.active = false;
        graph
            .add_model(
                Model::new("inactive", "id")
                    .with_table("inactive")
                    .with_relationship(inactive),
            )
            .unwrap();
        assert!(matches!(
            graph.find_join_path("inactive", "airports"),
            Err(SidemanticError::NoJoinPath { .. })
        ));
    }

    #[test]
    fn many_to_many_roles_have_independent_bridge_instances() {
        let mut graph = SemanticGraph::new();
        graph
            .add_model(Model::new("tags", "id").with_table("tags"))
            .unwrap();
        graph
            .add_model(Model::new("links", "id").with_table("links"))
            .unwrap();
        let mut orders = Model::new("orders", "id").with_table("orders");
        for (name, key) in [
            ("primary_tags", "primary_tag"),
            ("secondary_tags", "secondary_tag"),
        ] {
            let mut relationship = role(name, "tags", "order_id");
            relationship.r#type = RelationshipType::ManyToMany;
            relationship.through = Some("links".into());
            relationship.through_foreign_key = Some("order_id".into());
            relationship.related_foreign_key = Some(key.into());
            orders.relationships.push(relationship);
        }
        graph.add_model(orders).unwrap();
        for (name, key) in [
            ("primary_tags", "primary_tag"),
            ("secondary_tags", "secondary_tag"),
        ] {
            let bridge = format!("{name}$through");
            let path = graph.find_join_path("orders", name).unwrap();
            assert_eq!(path.steps.len(), 2);
            assert_eq!(path.steps[0].to_model, bridge);
            assert_eq!(path.steps[1].from_keys, vec![key.to_string()]);
            assert_eq!(graph.get_model(&bridge).unwrap().name, "links");
            assert_eq!(graph.role_root_owner(&bridge), Some("orders"));
            assert!(graph.is_bridge_instance(&bridge));
            assert!(!graph.is_bridge_instance(name));
        }
        assert!(!graph.is_bridge_instance("links"));
        assert!(!graph.is_bridge_instance("orders"));
    }

    #[test]
    fn direct_many_to_many_roles_keep_recorded_key_direction() {
        let mut graph = SemanticGraph::new();
        graph
            .add_model(Model::new("airports", "id").with_table("airports"))
            .unwrap();
        let mut relationship = role("airport", "airports", "airport_id");
        relationship.r#type = RelationshipType::ManyToMany;
        graph
            .add_model(
                Model::new("flights", "id")
                    .with_table("flights")
                    .with_relationship(relationship),
            )
            .unwrap();
        let path = graph.find_join_path("flights", "airport").unwrap();
        assert_eq!(path.steps[0].relationship_type, RelationshipType::OneToMany);
        assert_eq!(path.steps[0].from_keys, vec!["airport_id".to_string()]);
        assert_eq!(path.steps[0].to_keys, vec!["id".to_string()]);
    }

    #[test]
    fn cross_edges_have_no_keys_or_predicate_in_either_direction() {
        let mut graph = SemanticGraph::new();
        let mut relationship = Relationship::new("calendar");
        relationship.r#type = RelationshipType::Cross;
        relationship.sql = Some("{from}.unused = {to}.unused".into());
        graph
            .add_model(Model::new("calendar", "id").with_table("calendar"))
            .unwrap();
        graph
            .add_model(
                Model::new("orders", "id")
                    .with_table("orders")
                    .with_relationship(relationship),
            )
            .unwrap();
        for (from, to) in [("orders", "calendar"), ("calendar", "orders")] {
            let path = graph.find_join_path(from, to).unwrap();
            assert_eq!(path.steps[0].relationship_type, RelationshipType::Cross);
            assert!(path.steps[0].from_keys.is_empty());
            assert!(path.steps[0].to_keys.is_empty());
            assert!(path.steps[0].custom_condition.is_none());
            assert!(path.has_fan_out());
        }
    }

    #[test]
    fn role_one_to_many_and_one_to_one_use_owner_primary_key() {
        for kind in [RelationshipType::OneToMany, RelationshipType::OneToOne] {
            let mut graph = SemanticGraph::new();
            let mut relationship = Relationship::new("child");
            relationship.target_model = Some("children".into());
            relationship.r#type = kind;
            relationship.foreign_key_columns = Some(vec!["owner_tenant".into(), "owner_id".into()]);
            graph
                .add_model(
                    Model::new("parents", "tenant_id")
                        .with_table("parents")
                        .with_primary_key_columns(vec!["tenant_id".into(), "parent_id".into()])
                        .with_relationship(relationship),
                )
                .unwrap();
            graph
                .add_model(Model::new("children", "child_id").with_table("children"))
                .unwrap();
            let path = graph.find_join_path("parents", "child").unwrap();
            assert_eq!(path.steps[0].from_keys, vec!["tenant_id", "parent_id"]);
            assert_eq!(path.steps[0].to_keys, vec!["owner_tenant", "owner_id"]);
            let reverse = graph.find_join_path("child", "parents").unwrap();
            assert_eq!(reverse.steps[0].to_keys, vec!["tenant_id", "parent_id"]);
        }
    }

    #[test]
    fn invalid_role_declarations_fail_without_corrupting_graph() {
        let mut graph = SemanticGraph::new();
        graph
            .add_model(Model::new("airports", "id").with_table("airports"))
            .unwrap();
        for relationships in [
            vec![role("airports", "airports", "airport_id")],
            vec![
                role("airport", "airports", "origin_id"),
                role("airport", "airports", "destination_id"),
            ],
        ] {
            let mut model = Model::new("flights", "id").with_table("flights");
            model.relationships = relationships;
            assert!(graph.add_model(model).is_err());
            assert!(graph.get_model("flights").is_none());
            assert_eq!(graph.models().count(), 1);
        }
        graph
            .add_model(Model::new("reserved$name", "id").with_table("flights"))
            .unwrap();
        assert!(graph
            .add_model(
                Model::new("flights", "id")
                    .with_table("flights")
                    .with_relationship(role("airport", "airports", "airport_id"))
            )
            .is_err());
    }

    #[test]
    fn shortest_paths_reject_ambiguity_and_use_query_context() {
        let mut graph = SemanticGraph::new();
        graph
            .add_model(
                Model::new("a", "id")
                    .with_table("a")
                    .with_relationship(Relationship::many_to_one("b"))
                    .with_relationship(Relationship::many_to_one("c")),
            )
            .unwrap();
        for name in ["b", "c"] {
            graph
                .add_model(
                    Model::new(name, "id")
                        .with_table(name)
                        .with_relationship(Relationship::many_to_one("d")),
                )
                .unwrap();
        }
        graph
            .add_model(Model::new("d", "id").with_table("d"))
            .unwrap();
        assert!(matches!(
            graph.find_join_path("a", "d"),
            Err(SidemanticError::AmbiguousJoinPath { .. })
        ));
        let context = HashSet::from(["a".into(), "b".into(), "d".into()]);
        let path = graph
            .find_join_path_with_context("a", "d", Some(&context))
            .unwrap();
        assert_eq!(path.steps[0].to_model, "b");
    }

    #[test]
    fn duplicate_edges_deduplicate_but_distinct_edge_ids_remain_ambiguous() {
        let mut graph = SemanticGraph::new();
        let first = Relationship::many_to_one("b")
            .with_condition("{from}.x = {to}.x AND {from}.y = {to}.y");
        let second = first.clone();
        graph
            .add_model(
                Model::new("a", "id")
                    .with_table("a")
                    .with_relationship(first)
                    .with_relationship(second),
            )
            .unwrap();
        graph
            .add_model(Model::new("b", "id").with_table("b"))
            .unwrap();
        assert_eq!(graph.find_join_path("a", "b").unwrap().steps.len(), 1);
        let mut model = graph.get_model("a").unwrap().clone();
        model.relationships[1].edge_id = Some("different".into());
        graph.replace_model(model).unwrap();
        assert!(matches!(
            graph.find_join_path("a", "b"),
            Err(SidemanticError::AmbiguousJoinPath { .. })
        ));
    }

    #[test]
    fn custom_join_literals_are_never_normalized_into_equivalent_paths() {
        for (left, right) in [
            ("{from}.label = 'a b'", "{from}.label = 'ab'"),
            ("{from}.label = 'x AND y'", "{from}.label = 'y AND x'"),
        ] {
            let mut graph = SemanticGraph::new();
            graph
                .add_model(
                    Model::new("a", "id")
                        .with_table("a")
                        .with_relationship(Relationship::many_to_one("b").with_condition(left))
                        .with_relationship(Relationship::many_to_one("b").with_condition(right)),
                )
                .unwrap();
            graph
                .add_model(Model::new("b", "id").with_table("b"))
                .unwrap();
            assert!(matches!(
                graph.find_join_path("a", "b"),
                Err(SidemanticError::AmbiguousJoinPath { .. })
            ));
        }
    }

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
                rollups: None,
                union_with_source_data: false,
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
    fn test_edge_id_survives_direct_and_reverse_paths() {
        let mut graph = SemanticGraph::new();
        let mut relationship = Relationship::many_to_one("customers");
        relationship.edge_id = Some("orders_customer".to_string());
        let orders = Model::new("orders", "order_id")
            .with_table("orders")
            .with_relationship(relationship);
        let customers = Model::new("customers", "id").with_table("customers");

        graph.add_model(orders).unwrap();
        graph.add_model(customers).unwrap();

        let forward = graph.find_join_path("orders", "customers").unwrap();
        let reverse = graph.find_join_path("customers", "orders").unwrap();
        assert_eq!(forward.steps[0].edge_id.as_deref(), Some("orders_customer"));
        assert_eq!(reverse.steps[0].edge_id.as_deref(), Some("orders_customer"));
    }

    #[test]
    fn test_one_to_many_omitted_key_defaults_to_id() {
        let mut graph = SemanticGraph::new();

        let customers = Model::new("customers", "id")
            .with_table("customers")
            .with_relationship(Relationship::one_to_many("orders"));
        let orders = Model::new("orders", "id").with_table("orders");

        graph.add_model(customers).unwrap();
        graph.add_model(orders).unwrap();

        let path = graph.find_join_path("customers", "orders").unwrap();
        assert_eq!(path.steps.len(), 1);
        assert_eq!(path.steps[0].from_keys, vec!["id".to_string()]);
        assert_eq!(path.steps[0].to_keys, vec!["id".to_string()]);
    }

    #[test]
    fn test_many_to_one_omitted_keys_use_name_id_and_target_primary_key() {
        let mut graph = SemanticGraph::new();

        let orders = Model::new("orders", "order_id")
            .with_table("orders")
            .with_relationship(Relationship::many_to_one("customers"));
        let customers = Model::new("customers", "customer_uid").with_table("customers");

        graph.add_model(orders).unwrap();
        graph.add_model(customers).unwrap();

        let path = graph.find_join_path("orders", "customers").unwrap();
        assert_eq!(path.steps.len(), 1);
        assert_eq!(path.steps[0].from_keys, vec!["customers_id".to_string()]);
        assert_eq!(path.steps[0].to_keys, vec!["customer_uid".to_string()]);
    }

    #[test]
    fn one_to_one_uses_local_key_and_remote_foreign_key_in_both_directions() {
        for explicit_key in [false, true] {
            let mut graph = SemanticGraph::new();
            let mut relationship = Relationship::new("regions");
            relationship.r#type = RelationshipType::OneToOne;
            relationship.foreign_key = Some("region_record_id".into());
            if explicit_key {
                relationship.primary_key = Some("region_id".into());
            }
            graph
                .add_model(
                    Model::new("sales", "region_id")
                        .with_table("sales")
                        .with_relationship(relationship),
                )
                .unwrap();
            graph
                .add_model(Model::new("regions", "region_record_id").with_table("regions"))
                .unwrap();
            let forward = graph.find_join_path("sales", "regions").unwrap();
            assert_eq!(forward.steps[0].from_keys, vec!["region_id"]);
            assert_eq!(forward.steps[0].to_keys, vec!["region_record_id"]);
            let reverse = graph.find_join_path("regions", "sales").unwrap();
            assert_eq!(reverse.steps[0].from_keys, vec!["region_record_id"]);
            assert_eq!(reverse.steps[0].to_keys, vec!["region_id"]);
        }
    }

    #[test]
    fn test_one_to_one_omitted_key_defaults_to_id() {
        let mut graph = SemanticGraph::new();

        let mut relationship = Relationship::new("profiles");
        relationship.r#type = RelationshipType::OneToOne;

        let users = Model::new("users", "id")
            .with_table("users")
            .with_relationship(relationship);
        let profiles = Model::new("profiles", "id").with_table("profiles");

        graph.add_model(users).unwrap();
        graph.add_model(profiles).unwrap();

        let path = graph.find_join_path("users", "profiles").unwrap();
        assert_eq!(path.steps.len(), 1);
        assert_eq!(path.steps[0].from_keys, vec!["id".to_string()]);
        assert_eq!(path.steps[0].to_keys, vec!["id".to_string()]);
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
                target_model: None,
                active: true,
                edge_id: None,
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
                target_model: None,
                active: true,
                edge_id: Some("orders_products".to_string()),
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
        assert!(graph.is_bridge_instance("order_items"));
        assert!(!graph.is_bridge_instance("products"));

        // orders -> order_items
        assert_eq!(path.steps[0].from_model, "orders");
        assert_eq!(path.steps[0].to_model, "order_items");
        assert_eq!(path.steps[0].from_key, "order_id");
        assert_eq!(path.steps[0].to_key, "order_id");
        assert_eq!(path.steps[0].from_keys, vec!["order_id".to_string()]);
        assert_eq!(path.steps[0].to_keys, vec!["order_id".to_string()]);
        assert_eq!(path.steps[0].relationship_type, RelationshipType::OneToMany);
        assert_eq!(path.steps[0].edge_id.as_deref(), Some("orders_products"));

        // order_items -> products
        assert_eq!(path.steps[1].from_model, "order_items");
        assert_eq!(path.steps[1].to_model, "products");
        assert_eq!(path.steps[1].from_key, "product_id");
        assert_eq!(path.steps[1].to_key, "product_id");
        assert_eq!(path.steps[1].from_keys, vec!["product_id".to_string()]);
        assert_eq!(path.steps[1].to_keys, vec!["product_id".to_string()]);
        assert_eq!(path.steps[1].relationship_type, RelationshipType::ManyToOne);
        assert_eq!(path.steps[1].edge_id.as_deref(), Some("orders_products"));

        let reverse_path = graph.find_join_path("products", "orders").unwrap();
        assert_eq!(reverse_path.steps.len(), 2);
        assert!(reverse_path
            .steps
            .iter()
            .all(|step| step.edge_id.as_deref() == Some("orders_products")));
    }

    #[test]
    fn test_many_to_many_through_preserves_composite_primary_keys() {
        let mut graph = SemanticGraph::new();

        let orders = Model::new("orders", "tenant_id")
            .with_primary_key_columns(vec!["tenant_id".to_string(), "order_id".to_string()])
            .with_table("orders")
            .with_relationship(Relationship {
                name: "products".to_string(),
                target_model: None,
                active: true,
                edge_id: None,
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
