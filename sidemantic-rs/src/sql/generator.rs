//! SQL generator: compiles semantic queries to SQL

use std::collections::{HashMap, HashSet};

use crate::core::{MetricType, SemanticGraph};
use crate::error::{Result, SidemanticError};

/// A semantic query definition
#[derive(Debug, Clone, Default)]
pub struct SemanticQuery {
    pub metrics: Vec<String>,
    pub dimensions: Vec<String>,
    pub filters: Vec<String>,
    /// Segment references (e.g., "orders.completed")
    pub segments: Vec<String>,
    pub order_by: Vec<String>,
    pub limit: Option<usize>,
}

impl SemanticQuery {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_metrics(mut self, metrics: Vec<String>) -> Self {
        self.metrics = metrics;
        self
    }

    pub fn with_dimensions(mut self, dimensions: Vec<String>) -> Self {
        self.dimensions = dimensions;
        self
    }

    pub fn with_filters(mut self, filters: Vec<String>) -> Self {
        self.filters = filters;
        self
    }

    pub fn with_segments(mut self, segments: Vec<String>) -> Self {
        self.segments = segments;
        self
    }

    pub fn with_order_by(mut self, order_by: Vec<String>) -> Self {
        self.order_by = order_by;
        self
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
}

/// Parsed dimension reference with optional granularity
#[derive(Debug, Clone)]
struct DimensionRef {
    model: String,
    name: String,
    granularity: Option<String>,
    alias: String,
}

/// Parsed metric reference
#[derive(Debug, Clone)]
struct MetricRef {
    model: String,
    name: String,
    alias: String,
}

/// SQL generator for semantic queries
pub struct SqlGenerator<'a> {
    graph: &'a SemanticGraph,
}

impl<'a> SqlGenerator<'a> {
    pub fn new(graph: &'a SemanticGraph) -> Self {
        Self { graph }
    }

    /// Generate SQL from a semantic query
    pub fn generate(&self, query: &SemanticQuery) -> Result<String> {
        // Parse all references
        let dimension_refs = self.parse_dimension_refs(&query.dimensions)?;
        let metric_refs = self.parse_metric_refs(&query.metrics)?;

        // Find all required models
        let required_models = self.find_required_models(&dimension_refs, &metric_refs)?;

        // Determine base model (first model with metrics, or first model)
        let base_model = metric_refs
            .first()
            .map(|m| m.model.clone())
            .or_else(|| dimension_refs.first().map(|d| d.model.clone()))
            .ok_or_else(|| {
                SidemanticError::Validation("Query must have at least one metric or dimension".into())
            })?;

        // Build join paths from base model to all other required models
        let join_paths = self.build_join_paths(&base_model, &required_models)?;

        // Generate SQL
        let mut sql = String::new();

        // SELECT clause
        sql.push_str("SELECT\n");
        let mut select_parts = Vec::new();

        // Add dimensions to SELECT
        for dim_ref in &dimension_refs {
            let model = self.graph.get_model(&dim_ref.model).ok_or_else(|| {
                let available: Vec<&str> = self.graph.models().map(|m| m.name.as_str()).collect();
                SidemanticError::model_not_found(&dim_ref.model, &available)
            })?;
            let dimension = model.get_dimension(&dim_ref.name).ok_or_else(|| {
                let available: Vec<&str> = model.dimensions.iter().map(|d| d.name.as_str()).collect();
                SidemanticError::dimension_not_found(&dim_ref.model, &dim_ref.name, &available)
            })?;

            let alias = self.model_alias(&dim_ref.model);
            let sql_expr = if dim_ref.granularity.is_some() {
                dimension.sql_with_granularity(dim_ref.granularity.as_deref())
            } else {
                format!("{}.{}", alias, dimension.sql_expr())
            };

            select_parts.push(format!("  {} AS {}", sql_expr, dim_ref.alias));
        }

        // Add metrics to SELECT
        for metric_ref in &metric_refs {
            let model = self.graph.get_model(&metric_ref.model).ok_or_else(|| {
                let available: Vec<&str> = self.graph.models().map(|m| m.name.as_str()).collect();
                SidemanticError::model_not_found(&metric_ref.model, &available)
            })?;
            let metric = model.get_metric(&metric_ref.name).ok_or_else(|| {
                let available: Vec<&str> = model.metrics.iter().map(|m| m.name.as_str()).collect();
                SidemanticError::metric_not_found(&metric_ref.model, &metric_ref.name, &available)
            })?;

            let alias = self.model_alias(&metric_ref.model);
            let sql_expr = match metric.r#type {
                MetricType::Simple => metric.to_sql(Some(&alias)),
                MetricType::Derived => {
                    // For derived metrics, we need to expand referenced metrics
                    self.expand_derived_metric(metric.sql_expr(), &metric_ref.model)?
                }
                MetricType::Ratio => {
                    // For ratio metrics, expand numerator and denominator
                    let num = metric.numerator.as_deref().unwrap_or("1");
                    let denom = metric.denominator.as_deref().unwrap_or("1");
                    let num_sql = self.expand_derived_metric(num, &metric_ref.model)?;
                    let denom_sql = self.expand_derived_metric(denom, &metric_ref.model)?;
                    format!("({}) / NULLIF({}, 0)", num_sql, denom_sql)
                }
                MetricType::Cumulative | MetricType::TimeComparison => {
                    // Complex metric types use to_sql which generates placeholder SQL
                    metric.to_sql(Some(&alias))
                }
            };

            select_parts.push(format!("  {} AS {}", sql_expr, metric_ref.alias));
        }

        sql.push_str(&select_parts.join(",\n"));
        sql.push('\n');

        // FROM clause
        let base_model_obj = self.graph.get_model(&base_model).unwrap();
        sql.push_str(&format!(
            "FROM {} AS {}\n",
            base_model_obj.table_source(),
            self.model_alias(&base_model)
        ));

        // JOIN clauses
        for (model_name, path) in &join_paths {
            if model_name == &base_model {
                continue;
            }

            for step in &path.steps {
                let target_model = self.graph.get_model(&step.to_model).unwrap();
                let from_alias = self.model_alias(&step.from_model);
                let to_alias = self.model_alias(&step.to_model);

                sql.push_str(&format!(
                    "LEFT JOIN {} AS {} ON {}.{} = {}.{}\n",
                    target_model.table_source(),
                    to_alias,
                    from_alias,
                    step.from_key,
                    to_alias,
                    step.to_key
                ));
            }
        }

        // WHERE clause (filters + resolved segments)
        let segment_filters = self.resolve_segments(&query.segments)?;
        let all_filters: Vec<String> = query
            .filters
            .iter()
            .cloned()
            .chain(segment_filters)
            .collect();

        if !all_filters.is_empty() {
            let filter_sql = self.expand_filters(&all_filters)?;
            sql.push_str(&format!("WHERE {}\n", filter_sql.join(" AND ")));
        }

        // GROUP BY clause (if we have aggregations)
        if !dimension_refs.is_empty() && !metric_refs.is_empty() {
            let group_by_indices: Vec<String> = (1..=dimension_refs.len())
                .map(|i| i.to_string())
                .collect();
            sql.push_str(&format!("GROUP BY {}\n", group_by_indices.join(", ")));
        }

        // ORDER BY clause
        if !query.order_by.is_empty() {
            sql.push_str(&format!("ORDER BY {}\n", query.order_by.join(", ")));
        }

        // LIMIT clause
        if let Some(limit) = query.limit {
            sql.push_str(&format!("LIMIT {}\n", limit));
        }

        Ok(sql.trim_end().to_string())
    }

    /// Parse dimension references from query
    fn parse_dimension_refs(&self, dimensions: &[String]) -> Result<Vec<DimensionRef>> {
        let mut refs = Vec::new();

        for dim in dimensions {
            let (model, name, granularity) = self.graph.parse_reference(dim)?;

            // Create alias: model_field or model_field__granularity
            let alias = if let Some(ref g) = granularity {
                format!("{}__{}", name, g)
            } else {
                name.clone()
            };

            refs.push(DimensionRef {
                model,
                name,
                granularity,
                alias,
            });
        }

        Ok(refs)
    }

    /// Parse metric references from query
    fn parse_metric_refs(&self, metrics: &[String]) -> Result<Vec<MetricRef>> {
        let mut refs = Vec::new();

        for metric in metrics {
            let (model, name, _) = self.graph.parse_reference(metric)?;

            refs.push(MetricRef {
                model,
                name: name.clone(),
                alias: name,
            });
        }

        Ok(refs)
    }

    /// Find all models required by the query
    fn find_required_models(
        &self,
        dimension_refs: &[DimensionRef],
        metric_refs: &[MetricRef],
    ) -> Result<HashSet<String>> {
        let mut models = HashSet::new();

        for dim in dimension_refs {
            models.insert(dim.model.clone());
        }

        for metric in metric_refs {
            models.insert(metric.model.clone());
        }

        Ok(models)
    }

    /// Build join paths from base model to all other required models
    fn build_join_paths(
        &self,
        base_model: &str,
        required_models: &HashSet<String>,
    ) -> Result<HashMap<String, crate::core::JoinPath>> {
        let mut paths = HashMap::new();

        for model in required_models {
            let path = self.graph.find_join_path(base_model, model)?;
            paths.insert(model.clone(), path);
        }

        Ok(paths)
    }

    /// Generate alias for a model (first letter lowercase)
    fn model_alias(&self, model_name: &str) -> String {
        model_name.chars().next().unwrap_or('t').to_string()
    }

    /// Expand a derived metric expression, replacing metric references with their SQL
    fn expand_derived_metric(&self, expr: &str, default_model: &str) -> Result<String> {
        // Simple implementation: look for metric names and expand them
        // A more robust implementation would use sqlparser to parse the expression
        let model = self.graph.get_model(default_model).ok_or_else(|| {
            let available: Vec<&str> = self.graph.models().map(|m| m.name.as_str()).collect();
            SidemanticError::model_not_found(default_model, &available)
        })?;

        let alias = self.model_alias(default_model);
        let mut result = expr.to_string();

        // Try to find and expand metric references
        for metric in &model.metrics {
            if result.contains(&metric.name) && metric.r#type == MetricType::Simple {
                let metric_sql = metric.to_sql(Some(&alias));
                result = result.replace(&metric.name, &metric_sql);
            }
        }

        Ok(result)
    }

    /// Expand filter expressions, replacing model.field references
    fn expand_filters(&self, filters: &[String]) -> Result<Vec<String>> {
        let mut expanded = Vec::new();

        for filter in filters {
            // Simple expansion: replace model.field with alias.field
            let mut expanded_filter = filter.clone();

            for model in self.graph.models() {
                let alias = self.model_alias(&model.name);

                // Replace model references with aliases
                for dim in &model.dimensions {
                    let pattern = format!("{}.{}", model.name, dim.name);
                    let replacement = format!("{}.{}", alias, dim.sql_expr());
                    expanded_filter = expanded_filter.replace(&pattern, &replacement);
                }
            }

            expanded.push(expanded_filter);
        }

        Ok(expanded)
    }

    /// Resolve segment references to SQL filter expressions
    fn resolve_segments(&self, segments: &[String]) -> Result<Vec<String>> {
        let mut filters = Vec::new();

        for seg_ref in segments {
            // Parse model.segment format
            let (model_name, segment_name, _) = self.graph.parse_reference(seg_ref)?;

            let model = self.graph.get_model(&model_name).ok_or_else(|| {
                let available: Vec<&str> = self.graph.models().map(|m| m.name.as_str()).collect();
                SidemanticError::model_not_found(&model_name, &available)
            })?;

            let segment = model.get_segment(&segment_name).ok_or_else(|| {
                let available: Vec<&str> = model.segments.iter().map(|s| s.name.as_str()).collect();
                SidemanticError::segment_not_found(&model_name, &segment_name, &available)
            })?;

            // Get SQL with model alias replaced
            let alias = self.model_alias(&model_name);
            let filter_sql = segment.get_sql(&alias);
            filters.push(filter_sql);
        }

        Ok(filters)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{Dimension, Metric, Model, Relationship};

    fn create_test_graph() -> SemanticGraph {
        let mut graph = SemanticGraph::new();

        let orders = Model::new("orders", "order_id")
            .with_table("orders")
            .with_dimension(Dimension::categorical("status"))
            .with_dimension(Dimension::time("order_date").with_sql("created_at"))
            .with_metric(Metric::sum("revenue", "amount"))
            .with_metric(Metric::count("order_count"))
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
    fn test_simple_query() {
        let graph = create_test_graph();
        let generator = SqlGenerator::new(&graph);

        let query = SemanticQuery::new()
            .with_metrics(vec!["orders.revenue".into()])
            .with_dimensions(vec!["orders.status".into()]);

        let sql = generator.generate(&query).unwrap();

        assert!(sql.contains("SELECT"));
        assert!(sql.contains("SUM(o.amount) AS revenue"));
        assert!(sql.contains("o.status AS status"));
        assert!(sql.contains("FROM orders AS o"));
        assert!(sql.contains("GROUP BY 1"));
    }

    #[test]
    fn test_query_with_join() {
        let graph = create_test_graph();
        let generator = SqlGenerator::new(&graph);

        let query = SemanticQuery::new()
            .with_metrics(vec!["orders.revenue".into()])
            .with_dimensions(vec!["customers.country".into()]);

        let sql = generator.generate(&query).unwrap();

        assert!(sql.contains("LEFT JOIN customers AS c"));
        assert!(sql.contains("o.customers_id = c.id"));
    }

    #[test]
    fn test_query_with_filter() {
        let graph = create_test_graph();
        let generator = SqlGenerator::new(&graph);

        let query = SemanticQuery::new()
            .with_metrics(vec!["orders.revenue".into()])
            .with_dimensions(vec!["orders.status".into()])
            .with_filters(vec!["orders.status = 'completed'".into()]);

        let sql = generator.generate(&query).unwrap();

        assert!(sql.contains("WHERE o.status = 'completed'"));
    }
}
