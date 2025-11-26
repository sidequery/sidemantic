//! SQL generator: compiles semantic queries to SQL

use std::collections::{HashMap, HashSet};

use crate::core::{
    build_symmetric_aggregate_sql, JoinPath, MetricType, RelativeDate, SemanticGraph, SqlDialect,
    SymmetricAggType, TableCalculation,
};
use crate::error::{Result, SidemanticError};

/// A semantic query definition
#[derive(Debug, Clone, Default)]
pub struct SemanticQuery {
    pub metrics: Vec<String>,
    pub dimensions: Vec<String>,
    pub filters: Vec<String>,
    /// Segment references (e.g., "orders.completed")
    pub segments: Vec<String>,
    /// Table calculations (window functions)
    pub table_calculations: Vec<TableCalculation>,
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

    pub fn with_table_calculations(mut self, calcs: Vec<TableCalculation>) -> Self {
        self.table_calculations = calcs;
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
                SidemanticError::Validation(
                    "Query must have at least one metric or dimension".into(),
                )
            })?;

        // Build join paths from base model to all other required models
        let join_paths = self.build_join_paths(&base_model, &required_models)?;

        // Detect fan-out risk for symmetric aggregate handling
        let fan_out_at_risk = self.detect_fan_out_risk(&base_model, &join_paths);

        // Generate SQL
        let mut sql = String::new();

        // Note: fan_out_at_risk is used below to apply symmetric aggregates

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
                let available: Vec<&str> =
                    model.dimensions.iter().map(|d| d.name.as_str()).collect();
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
            let use_symmetric = fan_out_at_risk.contains(&metric_ref.model);

            let sql_expr = match metric.r#type {
                MetricType::Simple if use_symmetric => {
                    // Use symmetric aggregate to prevent fan-out inflation
                    use crate::core::Aggregation;
                    match metric.agg {
                        Some(Aggregation::Sum) => build_symmetric_aggregate_sql(
                            metric.sql_expr(),
                            &model.primary_key,
                            SymmetricAggType::Sum,
                            Some(&alias),
                            SqlDialect::DuckDB,
                        ),
                        Some(Aggregation::Avg) => build_symmetric_aggregate_sql(
                            metric.sql_expr(),
                            &model.primary_key,
                            SymmetricAggType::Avg,
                            Some(&alias),
                            SqlDialect::DuckDB,
                        ),
                        Some(Aggregation::Count) => build_symmetric_aggregate_sql(
                            metric.sql_expr(),
                            &model.primary_key,
                            SymmetricAggType::Count,
                            Some(&alias),
                            SqlDialect::DuckDB,
                        ),
                        Some(Aggregation::CountDistinct) => build_symmetric_aggregate_sql(
                            metric.sql_expr(),
                            &model.primary_key,
                            SymmetricAggType::CountDistinct,
                            Some(&alias),
                            SqlDialect::DuckDB,
                        ),
                        // Min/Max/None don't need symmetric aggregates
                        _ => metric.to_sql(Some(&alias)),
                    }
                }
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
                    format!("({num_sql}) / NULLIF({denom_sql}, 0)")
                }
                MetricType::Cumulative | MetricType::TimeComparison => {
                    // Complex metric types use to_sql which generates placeholder SQL
                    metric.to_sql(Some(&alias))
                }
            };

            select_parts.push(format!("  {} AS {}", sql_expr, metric_ref.alias));
        }

        // Add table calculations to SELECT
        for calc in &query.table_calculations {
            let calc_sql = calc.to_sql().map_err(SidemanticError::Validation)?;
            select_parts.push(format!("  {} AS {}", calc_sql, calc.name));
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

                // Use custom condition if available, otherwise default FK/PK join
                let join_condition = if let Some(custom) = &step.custom_condition {
                    // Replace {from} and {to} placeholders with actual aliases
                    custom
                        .replace("{from}", &from_alias)
                        .replace("{to}", &to_alias)
                } else {
                    format!(
                        "{}.{} = {}.{}",
                        from_alias, step.from_key, to_alias, step.to_key
                    )
                };

                sql.push_str(&format!(
                    "LEFT JOIN {} AS {} ON {}\n",
                    target_model.table_source(),
                    to_alias,
                    join_condition
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
            let group_by_indices: Vec<String> =
                (1..=dimension_refs.len()).map(|i| i.to_string()).collect();
            sql.push_str(&format!("GROUP BY {}\n", group_by_indices.join(", ")));
        }

        // ORDER BY clause
        if !query.order_by.is_empty() {
            sql.push_str(&format!("ORDER BY {}\n", query.order_by.join(", ")));
        }

        // LIMIT clause
        if let Some(limit) = query.limit {
            sql.push_str(&format!("LIMIT {limit}\n"));
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
                format!("{name}__{g}")
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

    /// Expand filter expressions, replacing model.field references and relative dates
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

            // Expand relative date expressions in quoted strings
            // e.g., "created_at >= 'last 7 days'" -> "created_at >= CURRENT_DATE - 7"
            expanded_filter = self.expand_relative_dates(&expanded_filter);

            expanded.push(expanded_filter);
        }

        Ok(expanded)
    }

    /// Expand relative date expressions in a filter string
    fn expand_relative_dates(&self, filter: &str) -> String {
        let mut result = filter.to_string();

        // Find quoted strings and try to parse as relative dates
        let re = regex::Regex::new(r"'([^']+)'").unwrap();
        for cap in re.captures_iter(filter) {
            let quoted = &cap[0];
            let inner = &cap[1];

            if let Some(sql_date) = RelativeDate::parse(inner) {
                result = result.replace(quoted, &sql_date);
            }
        }

        result
    }

    /// Check if metrics from a model are at fan-out risk
    /// Returns the set of models whose metrics will be inflated due to fan-out
    fn detect_fan_out_risk(
        &self,
        base_model: &str,
        join_paths: &HashMap<String, JoinPath>,
    ) -> HashSet<String> {
        let mut at_risk = HashSet::new();

        // For each model we join to, check if the path has fan-out
        for (model, path) in join_paths {
            if path.has_fan_out() {
                // All models BEFORE the fan-out boundary are at risk
                // The base model's metrics can be inflated if we join to a "many" side
                if let Some(boundary) = path.fan_out_boundary() {
                    // If we're joining to a model that causes fan-out,
                    // the base model's metrics are at risk
                    if model != boundary {
                        at_risk.insert(base_model.to_string());
                    }
                }
            }
        }

        // Also check reverse: if the base model is a "many" side model
        // and we're pulling metrics from a "one" side model
        for (model, path) in join_paths {
            if model == base_model {
                continue;
            }
            // If the path TO this model has no fan-out, but the REVERSE would,
            // then metrics from this model might be duplicated
            // This is detected by checking if any step is one_to_many
            for step in &path.steps {
                if step.causes_fan_out() {
                    // The TO model of this step's metrics would be duplicated
                    // when viewed from the base model's grain
                    at_risk.insert(step.from_model.clone());
                }
            }
        }

        at_risk
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

    #[test]
    fn test_fan_out_warning() {
        // Create a graph where customers have metrics and we join to orders
        let mut graph = SemanticGraph::new();

        let orders = Model::new("orders", "order_id")
            .with_table("orders")
            .with_dimension(Dimension::categorical("status"))
            .with_metric(Metric::sum("revenue", "amount"))
            .with_relationship(Relationship::many_to_one("customers"));

        let customers = Model::new("customers", "id")
            .with_table("customers")
            .with_dimension(Dimension::categorical("country"))
            .with_metric(Metric::sum("total_credit", "credit_limit"));

        graph.add_model(orders).unwrap();
        graph.add_model(customers).unwrap();

        let generator = SqlGenerator::new(&graph);

        // Query customer metrics grouped by order status
        // This causes fan-out: one customer can have many orders
        let query = SemanticQuery::new()
            .with_metrics(vec!["customers.total_credit".into()])
            .with_dimensions(vec!["orders.status".into()]);

        let sql = generator.generate(&query).unwrap();

        // Should use symmetric aggregates for fan-out prevention
        assert!(
            sql.contains("SUM(DISTINCT"),
            "Expected symmetric aggregate in SQL: {sql}"
        );
        assert!(
            sql.contains("HASH(c.id)"),
            "Expected hash on primary key: {sql}"
        );
    }

    #[test]
    fn test_table_calculations() {
        use crate::core::{TableCalcType, TableCalculation};

        let graph = create_test_graph();
        let generator = SqlGenerator::new(&graph);

        let query = SemanticQuery::new()
            .with_metrics(vec!["orders.revenue".into()])
            .with_dimensions(vec!["orders.order_date".into()])
            .with_table_calculations(vec![
                TableCalculation::new("cumulative_revenue", TableCalcType::RunningTotal)
                    .with_field("revenue")
                    .with_order_by(vec!["order_date".into()]),
                TableCalculation::new("pct_total", TableCalcType::PercentOfTotal)
                    .with_field("revenue"),
            ]);

        let sql = generator.generate(&query).unwrap();

        // Should include running total window function
        assert!(
            sql.contains("SUM(revenue) OVER"),
            "Expected running total: {sql}"
        );
        assert!(
            sql.contains("ROWS UNBOUNDED PRECEDING"),
            "Expected unbounded preceding: {sql}"
        );

        // Should include percent of total
        assert!(
            sql.contains("revenue * 100.0 / NULLIF(SUM(revenue) OVER"),
            "Expected percent of total: {sql}"
        );
    }

    #[test]
    fn test_relative_date_filter() {
        let graph = create_test_graph();
        let generator = SqlGenerator::new(&graph);

        let query = SemanticQuery::new()
            .with_metrics(vec!["orders.revenue".into()])
            .with_dimensions(vec!["orders.status".into()])
            .with_filters(vec!["orders.order_date >= 'last 7 days'".into()]);

        let sql = generator.generate(&query).unwrap();

        // Relative date should be expanded to SQL
        assert!(
            sql.contains("CURRENT_DATE - 7"),
            "Expected relative date expansion: {sql}"
        );
        // Should NOT contain the quoted string anymore
        assert!(
            !sql.contains("'last 7 days'"),
            "Relative date should be expanded: {sql}"
        );
    }
}
