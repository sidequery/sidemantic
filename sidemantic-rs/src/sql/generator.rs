//! SQL generator: compiles semantic queries to SQL

use std::collections::{HashMap, HashSet};

use crate::core::{
    build_symmetric_aggregate_sql_with_key_expr, Aggregation, JoinPath, MetricType,
    RelationshipType, RelativeDate, SemanticGraph, SqlDialect, SymmetricAggType, TableCalculation,
};
use crate::error::{Result, SidemanticError};

type CtePushdownClassification = (HashMap<String, Vec<String>>, Vec<String>);

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
    pub offset: Option<usize>,
    pub ungrouped: bool,
    pub use_preaggregations: bool,
    pub preagg_database: Option<String>,
    pub preagg_schema: Option<String>,
    pub skip_default_time_dimensions: bool,
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

    pub fn with_offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }

    pub fn with_ungrouped(mut self, ungrouped: bool) -> Self {
        self.ungrouped = ungrouped;
        self
    }

    pub fn with_use_preaggregations(mut self, use_preaggregations: bool) -> Self {
        self.use_preaggregations = use_preaggregations;
        self
    }

    pub fn with_preaggregation_qualifiers(
        mut self,
        preagg_database: Option<String>,
        preagg_schema: Option<String>,
    ) -> Self {
        self.preagg_database = preagg_database;
        self.preagg_schema = preagg_schema;
        self
    }

    pub fn with_skip_default_time_dimensions(mut self, skip_default_time_dimensions: bool) -> Self {
        self.skip_default_time_dimensions = skip_default_time_dimensions;
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
        let effective_dimensions = if query.skip_default_time_dimensions {
            query.dimensions.clone()
        } else {
            self.apply_default_time_dimensions(&query.metrics, &query.dimensions)?
        };

        // Parse all references
        let dimension_refs = self.parse_dimension_refs(&effective_dimensions)?;
        let metric_refs = self.parse_metric_refs(&query.metrics)?;
        if self.has_cumulative_metrics(&metric_refs)? {
            return self.generate_with_cumulative(
                query,
                &effective_dimensions,
                &dimension_refs,
                &metric_refs,
            );
        }

        // Find all required models
        let mut required_models = self.find_required_models(&dimension_refs, &metric_refs)?;
        let segment_filters = self.resolve_segments(&query.segments)?;
        let all_filters: Vec<String> = query
            .filters
            .iter()
            .cloned()
            .chain(segment_filters)
            .collect();
        for model_name in self.find_filter_models(&all_filters) {
            required_models.insert(model_name);
        }
        for metric_ref in &metric_refs {
            self.collect_metric_referenced_models(
                metric_ref,
                &mut required_models,
                &mut HashSet::new(),
            )?;
        }

        if self.needs_preaggregation_for_fanout(&metric_refs)? {
            return self.generate_with_preaggregation(
                query,
                &effective_dimensions,
                &dimension_refs,
                &metric_refs,
                &all_filters,
            );
        }

        // Try pre-aggregation routing for single-model aggregate queries.
        if query.use_preaggregations && !query.ungrouped && required_models.len() == 1 {
            if let Some(model_name) = required_models.iter().next() {
                if let Some(preagg_sql) = self.try_use_preaggregation(
                    model_name,
                    &metric_refs,
                    &dimension_refs,
                    &all_filters,
                    &query.order_by,
                    query.limit,
                    query.offset,
                    query.preagg_database.as_deref(),
                    query.preagg_schema.as_deref(),
                )? {
                    return Ok(preagg_sql);
                }
            }
        }

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
        let cte_models = self.collect_models_in_join_plan(&base_model, &join_paths);
        let mut alias_collisions: HashMap<String, usize> = HashMap::new();
        for dim_ref in &dimension_refs {
            *alias_collisions.entry(dim_ref.alias.clone()).or_insert(0) += 1;
        }
        for metric_ref in &metric_refs {
            *alias_collisions
                .entry(metric_ref.alias.clone())
                .or_insert(0) += 1;
        }
        let (where_filters, having_filters) =
            self.split_filters(&all_filters, &alias_collisions)?;
        let (cte_where_filters, where_filters) =
            self.classify_filters_for_cte_pushdown(&where_filters, &cte_models)?;
        let mut raw_metric_dependencies = HashSet::new();
        for metric_ref in &metric_refs {
            self.collect_simple_metric_dependencies(
                metric_ref,
                &mut raw_metric_dependencies,
                &mut HashSet::new(),
            )?;
        }
        let mut raw_column_dependencies = HashSet::new();
        for metric_ref in &metric_refs {
            self.collect_inline_metric_column_dependencies(
                metric_ref,
                &mut raw_column_dependencies,
                &mut HashSet::new(),
            )?;
        }
        let mut raw_metric_dependencies: Vec<(String, String)> =
            raw_metric_dependencies.into_iter().collect();
        raw_metric_dependencies.sort();
        let mut raw_column_dependencies: Vec<(String, String)> =
            raw_column_dependencies.into_iter().collect();
        raw_column_dependencies.sort();

        let mut raw_model_columns: HashMap<String, Vec<String>> = HashMap::new();
        let mut raw_model_aliases: HashMap<String, HashSet<String>> = HashMap::new();
        for model_name in &cte_models {
            let model = self.graph.get_model(model_name).ok_or_else(|| {
                let available: Vec<&str> = self.graph.models().map(|m| m.name.as_str()).collect();
                SidemanticError::model_not_found(model_name, &available)
            })?;
            for dimension in &model.dimensions {
                let Some(window_expr) = dimension.window.as_ref() else {
                    continue;
                };
                if raw_model_aliases
                    .get(model_name)
                    .is_some_and(|aliases| aliases.contains(&dimension.name))
                {
                    continue;
                }
                let window_sql = self.normalize_cte_source_expression(window_expr);
                raw_model_columns
                    .entry(model_name.clone())
                    .or_default()
                    .push(format!(
                        "{window_sql} AS {}",
                        self.quote_identifier(&dimension.name)
                    ));
                raw_model_aliases
                    .entry(model_name.clone())
                    .or_default()
                    .insert(dimension.name.clone());
            }
        }
        for (model_name, metric_name) in raw_metric_dependencies {
            let model = self.graph.get_model(&model_name).ok_or_else(|| {
                let available: Vec<&str> = self.graph.models().map(|m| m.name.as_str()).collect();
                SidemanticError::model_not_found(&model_name, &available)
            })?;
            let metric = model.get_metric(&metric_name).ok_or_else(|| {
                let available: Vec<&str> = model.metrics.iter().map(|m| m.name.as_str()).collect();
                SidemanticError::metric_not_found(&model_name, &metric_name, &available)
            })?;
            let raw_alias = format!("{metric_name}_raw");
            let mut raw_expr =
                self.normalize_cte_source_expression(&self.metric_raw_expression(metric, model));
            if !metric.filters.is_empty() {
                let metric_filter = self.normalize_metric_filters(
                    &metric.filters,
                    &model_name,
                    &self.model_alias(&model_name),
                );
                raw_expr = format!("CASE WHEN {metric_filter} THEN {raw_expr} END");
            }
            raw_model_columns
                .entry(model_name)
                .or_default()
                .push(format!(
                    "{raw_expr} AS {}",
                    self.quote_identifier(&raw_alias)
                ));
            raw_model_aliases
                .entry(model.name.clone())
                .or_default()
                .insert(raw_alias);
        }
        for (model_name, column_name) in raw_column_dependencies {
            if raw_model_aliases
                .get(&model_name)
                .is_some_and(|aliases| aliases.contains(&column_name))
            {
                continue;
            }
            let model = self.graph.get_model(&model_name).ok_or_else(|| {
                let available: Vec<&str> = self.graph.models().map(|m| m.name.as_str()).collect();
                SidemanticError::model_not_found(&model_name, &available)
            })?;
            let raw_expr = model
                .get_dimension(&column_name)
                .map(|dimension| self.normalize_cte_source_expression(dimension.sql_expr()))
                .unwrap_or_else(|| self.quote_identifier(&column_name));
            raw_model_columns
                .entry(model_name.clone())
                .or_default()
                .push(format!(
                    "{raw_expr} AS {}",
                    self.quote_identifier(&column_name)
                ));
            raw_model_aliases
                .entry(model_name)
                .or_default()
                .insert(column_name);
        }

        // Generate SQL
        let mut sql = String::new();

        if !cte_models.is_empty() {
            let mut cte_defs = Vec::with_capacity(cte_models.len());
            for model_name in &cte_models {
                let model = self.graph.get_model(model_name).ok_or_else(|| {
                    let available: Vec<&str> =
                        self.graph.models().map(|m| m.name.as_str()).collect();
                    SidemanticError::model_not_found(model_name, &available)
                })?;
                let cte_source = if let Some(model_sql) = &model.sql {
                    format!("({model_sql}) AS t")
                } else {
                    model.table_name().to_string()
                };
                let cte_select = if let Some(raw_cols) = raw_model_columns.get(model_name) {
                    format!("SELECT *,\n    {}", raw_cols.join(",\n    "))
                } else {
                    "SELECT *".to_string()
                };
                let cte_where = if let Some(filters) = cte_where_filters.get(model_name) {
                    let filter_sql = self.expand_filters_for_cte(model_name, filters)?;
                    if filter_sql.is_empty() {
                        String::new()
                    } else {
                        format!("\n  WHERE {}", filter_sql.join(" AND "))
                    }
                } else {
                    String::new()
                };
                cte_defs.push(format!(
                    "{model_name}_cte AS (\n  {cte_select}\n  FROM {cte_source}{cte_where}\n)"
                ));
            }
            sql.push_str("WITH ");
            sql.push_str(&cte_defs.join(",\n"));
            sql.push('\n');
        }

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
            let sql_expr = if dim_ref.granularity.is_some() || dimension.granularity.is_some() {
                self.normalize_select_expression(
                    &dimension.sql_with_granularity(dim_ref.granularity.as_deref()),
                    &alias,
                )
            } else if dimension.window.is_some() {
                format!("{}.{}", alias, self.quote_identifier(&dimension.name))
            } else {
                self.dimension_select_expression(dimension, &alias)
            };
            let output_alias = self.output_alias(&dim_ref.model, &dim_ref.alias, &alias_collisions);

            select_parts.push(format!("  {} AS {}", sql_expr, output_alias));
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
            let output_alias =
                self.output_alias(&metric_ref.model, &metric_ref.alias, &alias_collisions);
            let raw_alias = format!("{}_raw", metric_ref.name);
            let raw_col = format!("{alias}.{raw_alias}");

            let sql_expr = match metric.r#type {
                MetricType::Simple if query.ungrouped => raw_col.clone(),
                MetricType::Simple if use_symmetric => {
                    // Use symmetric aggregate to prevent fan-out inflation
                    let primary_key_expr = self.model_primary_key_expr(model, Some(&alias));
                    match metric.agg {
                        Some(Aggregation::Sum) => build_symmetric_aggregate_sql_with_key_expr(
                            &raw_alias,
                            &primary_key_expr,
                            SymmetricAggType::Sum,
                            Some(&alias),
                            SqlDialect::DuckDB,
                        ),
                        Some(Aggregation::Avg) => build_symmetric_aggregate_sql_with_key_expr(
                            &raw_alias,
                            &primary_key_expr,
                            SymmetricAggType::Avg,
                            Some(&alias),
                            SqlDialect::DuckDB,
                        ),
                        Some(Aggregation::Count) => build_symmetric_aggregate_sql_with_key_expr(
                            &raw_alias,
                            &primary_key_expr,
                            SymmetricAggType::Count,
                            Some(&alias),
                            SqlDialect::DuckDB,
                        ),
                        Some(Aggregation::CountDistinct) => {
                            build_symmetric_aggregate_sql_with_key_expr(
                                &raw_alias,
                                &primary_key_expr,
                                SymmetricAggType::CountDistinct,
                                Some(&alias),
                                SqlDialect::DuckDB,
                            )
                        }
                        // Min/Max/None don't need symmetric aggregates
                        _ => {
                            if let Some(agg) = &metric.agg {
                                format!("{}({raw_col})", agg.as_sql())
                            } else {
                                metric.to_sql(Some(&alias))
                            }
                        }
                    }
                }
                MetricType::Simple => match &metric.agg {
                    Some(Aggregation::CountDistinct) => format!("COUNT(DISTINCT {raw_col})"),
                    Some(Aggregation::Count) => format!("COUNT({raw_col})"),
                    Some(agg) if agg != &Aggregation::Expression => {
                        format!("{}({raw_col})", agg.as_sql())
                    }
                    _ => metric.to_sql(Some(&alias)),
                },
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
                MetricType::Conversion => metric.to_sql(Some(&alias)),
            };

            select_parts.push(format!("  {} AS {}", sql_expr, output_alias));
        }

        // Add table calculations to SELECT
        for calc in &query.table_calculations {
            let calc_sql = calc.to_sql().map_err(SidemanticError::Validation)?;
            select_parts.push(format!("  {} AS {}", calc_sql, calc.name));
        }

        sql.push_str(&select_parts.join(",\n"));
        sql.push('\n');

        // FROM clause
        sql.push_str(&format!(
            "FROM {}_cte AS {}\n",
            base_model,
            self.model_alias(&base_model)
        ));

        // JOIN clauses
        let mut joined_steps: HashSet<(String, String)> = HashSet::new();
        for (model_name, path) in &join_paths {
            if model_name == &base_model {
                continue;
            }

            for step in &path.steps {
                let step_key = (step.from_model.clone(), step.to_model.clone());
                if !joined_steps.insert(step_key) {
                    continue;
                }
                let from_alias = self.model_alias(&step.from_model);
                let to_alias = self.model_alias(&step.to_model);

                // Use custom condition if available, otherwise default FK/PK join
                let join_condition = if let Some(custom) = &step.custom_condition {
                    // Replace {from} and {to} placeholders with actual aliases
                    custom
                        .replace("{from}", &from_alias)
                        .replace("{to}", &to_alias)
                } else {
                    self.build_default_join_condition_sql(
                        &from_alias,
                        &step.from_keys,
                        &to_alias,
                        &step.to_keys,
                    )?
                };

                let join_type = if cte_where_filters
                    .get(&step.to_model)
                    .is_some_and(|filters| !filters.is_empty())
                {
                    "INNER JOIN"
                } else {
                    "LEFT JOIN"
                };
                sql.push_str(&format!(
                    "{join_type} {}_cte AS {} ON {}\n",
                    step.to_model, to_alias, join_condition
                ));
            }
        }

        if !where_filters.is_empty() {
            let filter_sql = self.expand_filters(&where_filters)?;
            sql.push_str(&format!("WHERE {}\n", filter_sql.join(" AND ")));
        }

        // GROUP BY clause (if we have aggregations)
        if !query.ungrouped && !dimension_refs.is_empty() && !metric_refs.is_empty() {
            let group_by_indices: Vec<String> =
                (1..=dimension_refs.len()).map(|i| i.to_string()).collect();
            sql.push_str(&format!("GROUP BY {}\n", group_by_indices.join(", ")));
        }

        if !having_filters.is_empty() {
            sql.push_str(&format!("HAVING {}\n", having_filters.join(" AND ")));
        }

        // ORDER BY clause
        if !query.order_by.is_empty() {
            let order_by = self.rewrite_order_by_items(
                &query.order_by,
                &dimension_refs,
                &metric_refs,
                &alias_collisions,
            );
            sql.push_str(&format!("ORDER BY {}\n", order_by.join(", ")));
        }

        // LIMIT clause
        if let Some(limit) = query.limit {
            sql.push_str(&format!("LIMIT {limit}\n"));
        }
        if let Some(offset) = query.offset {
            sql.push_str(&format!("OFFSET {offset}\n"));
        }

        Ok(sql.trim_end().to_string())
    }

    fn build_default_join_condition_sql(
        &self,
        from_alias: &str,
        from_keys: &[String],
        to_alias: &str,
        to_keys: &[String],
    ) -> Result<String> {
        if from_keys.is_empty() || to_keys.is_empty() {
            return Err(SidemanticError::Validation(
                "Join path is missing join key columns".to_string(),
            ));
        }
        if from_keys.len() != to_keys.len() {
            return Err(SidemanticError::Validation(format!(
                "Join key column count mismatch: {} vs {}",
                from_keys.len(),
                to_keys.len()
            )));
        }

        Ok(from_keys
            .iter()
            .zip(to_keys.iter())
            .map(|(from_key, to_key)| format!("{from_alias}.{from_key} = {to_alias}.{to_key}"))
            .collect::<Vec<_>>()
            .join(" AND "))
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
            let (model, name) = if metric.contains('.') {
                let (model, name, _) = self.graph.parse_reference(metric)?;
                (model, name)
            } else {
                let mut owners = Vec::new();
                for model in self.graph.models() {
                    if model.get_metric(metric).is_some() {
                        owners.push(model.name.clone());
                    }
                }
                if owners.len() == 1 {
                    (owners[0].clone(), metric.clone())
                } else {
                    return Err(SidemanticError::InvalidReference {
                        reference: metric.clone(),
                    });
                }
            };

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

    fn find_filter_models(&self, filters: &[String]) -> HashSet<String> {
        let mut models = HashSet::new();
        let ref_re = regex::Regex::new(r"\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b")
            .expect("valid model.field regex");
        for filter in filters {
            for cap in ref_re.captures_iter(filter) {
                let Some(model_match) = cap.get(1) else {
                    continue;
                };
                let model_name = model_match
                    .as_str()
                    .strip_suffix("_cte")
                    .unwrap_or(model_match.as_str());
                if self.graph.get_model(model_name).is_some() {
                    models.insert(model_name.to_string());
                }
            }
        }
        models
    }

    fn collect_metric_referenced_models(
        &self,
        metric_ref: &MetricRef,
        models: &mut HashSet<String>,
        visiting: &mut HashSet<(String, String)>,
    ) -> Result<()> {
        let key = (metric_ref.model.clone(), metric_ref.name.clone());
        if !visiting.insert(key.clone()) {
            return Ok(());
        }

        let model = self.graph.get_model(&metric_ref.model).ok_or_else(|| {
            let available: Vec<&str> = self.graph.models().map(|m| m.name.as_str()).collect();
            SidemanticError::model_not_found(&metric_ref.model, &available)
        })?;
        let metric = model.get_metric(&metric_ref.name).ok_or_else(|| {
            let available: Vec<&str> = model.metrics.iter().map(|m| m.name.as_str()).collect();
            SidemanticError::metric_not_found(&metric_ref.model, &metric_ref.name, &available)
        })?;

        let exprs: Vec<&str> = [
            metric.sql.as_deref(),
            metric.numerator.as_deref(),
            metric.denominator.as_deref(),
            metric.base_metric.as_deref(),
            metric.window_expression.as_deref(),
        ]
        .into_iter()
        .flatten()
        .collect();

        for expr in &exprs {
            self.collect_models_from_sql_references(expr, models);
        }

        if metric.r#type == MetricType::Simple {
            visiting.remove(&key);
            return Ok(());
        }

        let ref_re = regex::Regex::new(
            r"\b([A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*|[A-Za-z_][A-Za-z0-9_]*)\b",
        )
        .expect("valid metric token regex");
        for expr in exprs {
            for cap in ref_re.captures_iter(expr) {
                let Some(token_match) = cap.get(1) else {
                    continue;
                };
                let token = token_match.as_str();
                if Self::is_sql_keyword_or_function(token) {
                    continue;
                }
                if let Some((model_name, metric_name)) =
                    self.resolve_metric_reference_location(token, &metric_ref.model)?
                {
                    models.insert(model_name.clone());
                    self.collect_metric_referenced_models(
                        &MetricRef {
                            model: model_name,
                            name: metric_name.clone(),
                            alias: metric_name,
                        },
                        models,
                        visiting,
                    )?;
                }
            }
        }

        visiting.remove(&key);
        Ok(())
    }

    fn collect_models_from_sql_references(&self, expr: &str, models: &mut HashSet<String>) {
        let ref_re = regex::Regex::new(r"\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b")
            .expect("valid model.field regex");
        for cap in ref_re.captures_iter(expr) {
            let Some(model_match) = cap.get(1) else {
                continue;
            };
            let model_name = model_match.as_str();
            if self.graph.get_model(model_name).is_some() {
                models.insert(model_name.to_string());
            }
        }
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

    fn collect_models_in_join_plan(
        &self,
        base_model: &str,
        join_paths: &HashMap<String, JoinPath>,
    ) -> Vec<String> {
        let mut ordered = Vec::new();
        let mut seen = HashSet::new();
        let mut push_model = |name: &str| {
            if seen.insert(name.to_string()) {
                ordered.push(name.to_string());
            }
        };

        push_model(base_model);
        for (model_name, path) in join_paths {
            push_model(model_name);
            for step in &path.steps {
                push_model(&step.from_model);
                push_model(&step.to_model);
            }
        }
        ordered
    }

    fn metric_raw_expression(
        &self,
        metric: &crate::core::Metric,
        model: &crate::core::Model,
    ) -> String {
        match metric.agg {
            Some(Aggregation::CountDistinct)
                if metric.sql.as_deref().is_none_or(str::is_empty)
                    || metric.sql.as_deref() == Some("*") =>
            {
                self.model_primary_key_expr(model, None)
            }
            Some(Aggregation::Count)
                if metric.sql.as_deref().is_none_or(str::is_empty)
                    || metric.sql.as_deref() == Some("*") =>
            {
                "1".to_string()
            }
            _ => metric.sql_expr().to_string(),
        }
    }

    fn collect_simple_metric_dependencies(
        &self,
        metric_ref: &MetricRef,
        deps: &mut HashSet<(String, String)>,
        visiting: &mut HashSet<(String, String)>,
    ) -> Result<()> {
        let key = (metric_ref.model.clone(), metric_ref.name.clone());
        if !visiting.insert(key.clone()) {
            return Ok(());
        }

        let model = self.graph.get_model(&metric_ref.model).ok_or_else(|| {
            let available: Vec<&str> = self.graph.models().map(|m| m.name.as_str()).collect();
            SidemanticError::model_not_found(&metric_ref.model, &available)
        })?;
        let metric = model.get_metric(&metric_ref.name).ok_or_else(|| {
            let available: Vec<&str> = model.metrics.iter().map(|m| m.name.as_str()).collect();
            SidemanticError::metric_not_found(&metric_ref.model, &metric_ref.name, &available)
        })?;

        match metric.r#type {
            MetricType::Simple => {
                deps.insert(key.clone());
            }
            MetricType::Derived => {
                self.collect_simple_metric_dependencies_from_expr(
                    metric.sql_expr(),
                    &metric_ref.model,
                    deps,
                    visiting,
                )?;
            }
            MetricType::Ratio => {
                for expr in [metric.numerator.as_deref(), metric.denominator.as_deref()]
                    .into_iter()
                    .flatten()
                {
                    self.collect_simple_metric_dependencies_from_expr(
                        expr,
                        &metric_ref.model,
                        deps,
                        visiting,
                    )?;
                }
            }
            _ => {}
        }

        visiting.remove(&key);
        Ok(())
    }

    fn collect_simple_metric_dependencies_from_expr(
        &self,
        expr: &str,
        default_model: &str,
        deps: &mut HashSet<(String, String)>,
        visiting: &mut HashSet<(String, String)>,
    ) -> Result<()> {
        let ref_re = regex::Regex::new(
            r"\b([A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*|[A-Za-z_][A-Za-z0-9_]*)\b",
        )
        .expect("valid metric token regex");

        for cap in ref_re.captures_iter(expr) {
            let Some(token_match) = cap.get(1) else {
                continue;
            };
            let token = token_match.as_str();
            let Some((model, name)) =
                self.resolve_metric_reference_location(token, default_model)?
            else {
                continue;
            };
            self.collect_simple_metric_dependencies(
                &MetricRef {
                    model,
                    name: name.clone(),
                    alias: name,
                },
                deps,
                visiting,
            )?;
        }

        Ok(())
    }

    fn collect_inline_metric_column_dependencies(
        &self,
        metric_ref: &MetricRef,
        deps: &mut HashSet<(String, String)>,
        visiting: &mut HashSet<(String, String)>,
    ) -> Result<()> {
        let key = (metric_ref.model.clone(), metric_ref.name.clone());
        if !visiting.insert(key.clone()) {
            return Ok(());
        }

        let model = self.graph.get_model(&metric_ref.model).ok_or_else(|| {
            let available: Vec<&str> = self.graph.models().map(|m| m.name.as_str()).collect();
            SidemanticError::model_not_found(&metric_ref.model, &available)
        })?;
        let metric = model.get_metric(&metric_ref.name).ok_or_else(|| {
            let available: Vec<&str> = model.metrics.iter().map(|m| m.name.as_str()).collect();
            SidemanticError::metric_not_found(&metric_ref.model, &metric_ref.name, &available)
        })?;

        match metric.r#type {
            MetricType::Derived if Self::is_inline_aggregate_expression(metric.sql_expr()) => {
                self.collect_inline_metric_column_dependencies_from_expr(
                    metric.sql_expr(),
                    &metric_ref.model,
                    deps,
                )?;
            }
            MetricType::Derived => {
                let ref_re = regex::Regex::new(
                    r"\b([A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*|[A-Za-z_][A-Za-z0-9_]*)\b",
                )
                .expect("valid metric token regex");
                for cap in ref_re.captures_iter(metric.sql_expr()) {
                    let Some(token_match) = cap.get(1) else {
                        continue;
                    };
                    let token = token_match.as_str();
                    let Some((model, name)) =
                        self.resolve_metric_reference_location(token, &metric_ref.model)?
                    else {
                        continue;
                    };
                    self.collect_inline_metric_column_dependencies(
                        &MetricRef {
                            model,
                            name: name.clone(),
                            alias: name,
                        },
                        deps,
                        visiting,
                    )?;
                }
            }
            MetricType::Ratio => {
                for expr in [metric.numerator.as_deref(), metric.denominator.as_deref()]
                    .into_iter()
                    .flatten()
                {
                    if let Some((model, name)) =
                        self.resolve_metric_reference_location(expr, &metric_ref.model)?
                    {
                        self.collect_inline_metric_column_dependencies(
                            &MetricRef {
                                model,
                                name: name.clone(),
                                alias: name,
                            },
                            deps,
                            visiting,
                        )?;
                    }
                }
            }
            _ => {}
        }

        visiting.remove(&key);
        Ok(())
    }

    fn collect_inline_metric_column_dependencies_from_expr(
        &self,
        expr: &str,
        default_model: &str,
        deps: &mut HashSet<(String, String)>,
    ) -> Result<()> {
        let ref_re = regex::Regex::new(r"\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b")
            .expect("valid model.field regex");
        for cap in ref_re.captures_iter(expr) {
            let Some(model_match) = cap.get(1) else {
                continue;
            };
            let Some(field_match) = cap.get(2) else {
                continue;
            };
            let model_name = model_match.as_str();
            let field_name = field_match.as_str();
            let Some(model) = self.graph.get_model(model_name) else {
                continue;
            };
            if model.get_metric(field_name).is_none() {
                deps.insert((model_name.to_string(), field_name.to_string()));
            }
        }

        if let Some(model) = self.graph.get_model(default_model) {
            let quoted_re =
                regex::Regex::new(r#""([^"]+)""#).expect("valid quoted identifier regex");
            for cap in quoted_re.captures_iter(expr) {
                let Some(name_match) = cap.get(1) else {
                    continue;
                };
                let name = name_match.as_str();
                if model.get_dimension(name).is_some() {
                    deps.insert((default_model.to_string(), name.to_string()));
                }
            }

            for token in Self::identifier_tokens(expr) {
                if Self::is_sql_keyword_or_function(&token) || model.get_metric(&token).is_some() {
                    continue;
                }
                if model.get_dimension(&token).is_some() {
                    deps.insert((default_model.to_string(), token));
                }
            }
        }

        Ok(())
    }

    fn resolve_metric_reference_location(
        &self,
        reference: &str,
        default_model: &str,
    ) -> Result<Option<(String, String)>> {
        if reference.contains('.') {
            let (model_name, metric_name, _) = self.graph.parse_reference(reference)?;
            let Some(model) = self.graph.get_model(&model_name) else {
                return Ok(None);
            };
            return Ok(model
                .get_metric(&metric_name)
                .map(|_| (model_name, metric_name)));
        }

        let mut owners = Vec::new();
        for model in self.graph.models() {
            if model.get_metric(reference).is_some() {
                owners.push(model.name.clone());
            }
        }
        if owners.len() == 1 {
            return Ok(Some((owners[0].clone(), reference.to_string())));
        }
        if let Some(default) = self.graph.get_model(default_model) {
            if default.get_metric(reference).is_some() {
                return Ok(Some((default_model.to_string(), reference.to_string())));
            }
        }

        Ok(None)
    }

    fn output_alias(
        &self,
        model_name: &str,
        base_alias: &str,
        collisions: &HashMap<String, usize>,
    ) -> String {
        if collisions.get(base_alias).copied().unwrap_or(0) > 1 {
            format!("{model_name}_{base_alias}")
        } else {
            base_alias.to_string()
        }
    }

    fn rewrite_order_by_items(
        &self,
        order_by: &[String],
        dimension_refs: &[DimensionRef],
        metric_refs: &[MetricRef],
        alias_collisions: &HashMap<String, usize>,
    ) -> Vec<String> {
        order_by
            .iter()
            .map(|item| {
                self.rewrite_order_by_item(item, dimension_refs, metric_refs, alias_collisions)
            })
            .collect()
    }

    fn rewrite_order_by_item(
        &self,
        item: &str,
        dimension_refs: &[DimensionRef],
        metric_refs: &[MetricRef],
        alias_collisions: &HashMap<String, usize>,
    ) -> String {
        let trimmed = item.trim();
        let head_len = trimmed.find(char::is_whitespace).unwrap_or(trimmed.len());
        let (head, suffix) = trimmed.split_at(head_len);

        let Ok((model, field, granularity)) = self.graph.parse_reference(head) else {
            return trimmed.to_string();
        };

        for dim_ref in dimension_refs {
            if dim_ref.model == model
                && dim_ref.name == field
                && dim_ref.granularity.as_deref() == granularity.as_deref()
            {
                let alias = self.output_alias(&model, &dim_ref.alias, alias_collisions);
                return format!("{alias}{suffix}");
            }
        }

        if granularity.is_none() {
            for metric_ref in metric_refs {
                if metric_ref.model == model && metric_ref.name == field {
                    let alias = self.output_alias(&model, &metric_ref.alias, alias_collisions);
                    return format!("{alias}{suffix}");
                }
            }
        }

        trimmed.to_string()
    }

    fn model_primary_key_expr(&self, model: &crate::core::Model, alias: Option<&str>) -> String {
        let primary_keys = model.primary_keys();
        if primary_keys.len() <= 1 {
            return primary_keys
                .first()
                .map(|column| match alias {
                    Some(alias) => format!("{alias}.{column}"),
                    None => column.clone(),
                })
                .unwrap_or_else(|| model.primary_key.clone());
        }

        primary_keys
            .iter()
            .enumerate()
            .map(|(index, column)| {
                let qualified = match alias {
                    Some(alias) => format!("{alias}.{column}"),
                    None => column.clone(),
                };
                let casted = format!("COALESCE(CAST({qualified} AS VARCHAR), '')");
                if index == 0 {
                    casted
                } else {
                    format!("'|' || {casted}")
                }
            })
            .collect::<Vec<_>>()
            .join(" || ")
    }

    /// Generate alias for a model (first letter lowercase)
    fn model_alias(&self, model_name: &str) -> String {
        format!("{model_name}_cte")
    }

    fn has_cumulative_metrics(&self, metric_refs: &[MetricRef]) -> Result<bool> {
        for metric_ref in metric_refs {
            let model = self.graph.get_model(&metric_ref.model).ok_or_else(|| {
                let available: Vec<&str> = self.graph.models().map(|m| m.name.as_str()).collect();
                SidemanticError::model_not_found(&metric_ref.model, &available)
            })?;
            let metric = model.get_metric(&metric_ref.name).ok_or_else(|| {
                let available: Vec<&str> = model.metrics.iter().map(|m| m.name.as_str()).collect();
                SidemanticError::metric_not_found(&metric_ref.model, &metric_ref.name, &available)
            })?;
            if metric.r#type == MetricType::Cumulative
                || metric.r#type == MetricType::TimeComparison
                || metric.r#type == MetricType::Conversion
                || (metric.r#type == MetricType::Ratio && metric.offset_window.is_some())
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn needs_preaggregation_for_fanout(&self, metric_refs: &[MetricRef]) -> Result<bool> {
        if metric_refs.len() < 2 {
            return Ok(false);
        }

        let mut metric_models = Vec::new();
        let mut seen = HashSet::new();
        for metric_ref in metric_refs {
            if seen.insert(metric_ref.model.clone()) {
                metric_models.push(metric_ref.model.clone());
            }
        }
        if metric_models.len() < 2 {
            return Ok(false);
        }

        for i in 0..metric_models.len() {
            for model_b in metric_models.iter().skip(i + 1) {
                let model_a = &metric_models[i];
                for (from_model, to_model) in [(model_a, model_b), (model_b, model_a)] {
                    if let Ok(path) = self.graph.find_join_path(from_model, to_model) {
                        if path
                            .steps
                            .iter()
                            .any(|step| step.relationship_type == RelationshipType::ManyToOne)
                        {
                            return Ok(true);
                        }
                    }
                }
            }
        }

        Ok(false)
    }

    fn generate_with_preaggregation(
        &self,
        query: &SemanticQuery,
        effective_dimensions: &[String],
        dimension_refs: &[DimensionRef],
        metric_refs: &[MetricRef],
        filters: &[String],
    ) -> Result<String> {
        let mut model_order = Vec::new();
        let mut metrics_by_model: HashMap<String, Vec<String>> = HashMap::new();
        for metric_ref in metric_refs {
            if !metrics_by_model.contains_key(&metric_ref.model) {
                model_order.push(metric_ref.model.clone());
            }
            metrics_by_model
                .entry(metric_ref.model.clone())
                .or_default()
                .push(format!("{}.{}", metric_ref.model, metric_ref.name));
        }

        if model_order.len() < 2 {
            let mut subquery = query.clone();
            subquery.skip_default_time_dimensions = true;
            return self.generate(&subquery);
        }

        let metric_model_set: HashSet<&str> = model_order.iter().map(String::as_str).collect();
        let mut window_filters_by_model: HashMap<String, Vec<String>> = HashMap::new();
        let mut non_window_filters = Vec::new();
        for filter in filters {
            let window_models = self.filter_window_dimension_models(filter, &metric_model_set);
            if window_models.is_empty() {
                non_window_filters.push(filter.clone());
            } else {
                for model_name in window_models {
                    window_filters_by_model
                        .entry(model_name)
                        .or_default()
                        .push(filter.clone());
                }
            }
        }

        let (pushdown_by_model, shared_filters) =
            self.classify_filters_for_cte_pushdown(&non_window_filters, &model_order)?;

        let mut cte_defs = Vec::new();
        let mut cte_names = Vec::new();
        for model_name in &model_order {
            let cte_name = format!("{model_name}_preagg");
            cte_names.push(cte_name.clone());
            let subquery = SemanticQuery::new()
                .with_metrics(
                    metrics_by_model
                        .get(model_name)
                        .cloned()
                        .unwrap_or_default(),
                )
                .with_dimensions(effective_dimensions.to_vec())
                .with_filters({
                    let mut model_filters = pushdown_by_model
                        .get(model_name)
                        .cloned()
                        .unwrap_or_default();
                    model_filters.extend(
                        window_filters_by_model
                            .get(model_name)
                            .cloned()
                            .unwrap_or_default(),
                    );
                    model_filters
                })
                .with_ungrouped(false)
                .with_skip_default_time_dimensions(true);
            let subquery_sql = self.generate(&subquery)?;
            cte_defs.push(format!("{cte_name} AS (\n{subquery_sql}\n)"));
        }

        let mut metric_name_counts: HashMap<String, usize> = HashMap::new();
        for metric_ref in metric_refs {
            *metric_name_counts
                .entry(metric_ref.name.clone())
                .or_insert(0) += 1;
        }

        let mut select_parts = Vec::new();
        for dim_ref in dimension_refs {
            let dim_alias = dim_ref.alias.clone();
            let coalesce_parts = cte_names
                .iter()
                .map(|cte_name| format!("{cte_name}.{}", self.quote_identifier(&dim_alias)))
                .collect::<Vec<_>>();
            select_parts.push(format!(
                "  COALESCE({}) AS {}",
                coalesce_parts.join(", "),
                self.quote_identifier(&dim_alias)
            ));
        }

        for metric_ref in metric_refs {
            let cte_name = format!("{}_preagg", metric_ref.model);
            let output_alias = if metric_name_counts
                .get(&metric_ref.name)
                .copied()
                .unwrap_or(0)
                > 1
            {
                format!("{}_{}", metric_ref.model, metric_ref.name)
            } else {
                metric_ref.name.clone()
            };
            select_parts.push(format!(
                "  {cte_name}.{} AS {}",
                self.quote_identifier(&metric_ref.name),
                self.quote_identifier(&output_alias)
            ));
        }

        let mut sql = format!("WITH {}\nSELECT\n", cte_defs.join(",\n"));
        sql.push_str(&select_parts.join(",\n"));
        sql.push('\n');
        sql.push_str(&format!("FROM {}", cte_names[0]));

        for cte_name in cte_names.iter().skip(1) {
            if dimension_refs.is_empty() {
                sql.push_str(&format!("\nCROSS JOIN {cte_name}"));
            } else {
                let join_conditions = dimension_refs
                    .iter()
                    .map(|dim_ref| {
                        let dim_alias = self.quote_identifier(&dim_ref.alias);
                        format!(
                            "{}.{} IS NOT DISTINCT FROM {cte_name}.{dim_alias}",
                            cte_names[0], dim_alias
                        )
                    })
                    .collect::<Vec<_>>();
                sql.push_str(&format!(
                    "\nFULL OUTER JOIN {cte_name} ON {}",
                    join_conditions.join(" AND ")
                ));
            }
        }

        if !shared_filters.is_empty() {
            let rewritten_filters =
                self.rewrite_filters_for_preaggregation(&shared_filters, &model_order);
            if !rewritten_filters.is_empty() {
                sql.push_str(&format!("\nWHERE {}", rewritten_filters.join(" AND ")));
            }
        }

        if !query.order_by.is_empty() {
            let order_by = self.rewrite_order_by_items(
                &query.order_by,
                dimension_refs,
                metric_refs,
                &HashMap::new(),
            );
            sql.push_str(&format!("\nORDER BY {}", order_by.join(", ")));
        }

        if let Some(limit) = query.limit {
            sql.push_str(&format!("\nLIMIT {limit}"));
        }
        if let Some(offset) = query.offset {
            sql.push_str(&format!("\nOFFSET {offset}"));
        }

        Ok(sql)
    }

    fn rewrite_filters_for_preaggregation(
        &self,
        filters: &[String],
        model_order: &[String],
    ) -> Vec<String> {
        let mut rewritten = Vec::with_capacity(filters.len());
        for filter in filters {
            let mut filter_sql = filter.clone();
            for model_name in model_order {
                filter_sql =
                    filter_sql.replace(&format!("{model_name}."), &format!("{model_name}_preagg."));
                filter_sql = filter_sql.replace(
                    &format!("{model_name}_cte."),
                    &format!("{model_name}_preagg."),
                );
            }
            rewritten.push(filter_sql);
        }
        rewritten
    }

    fn generate_with_cumulative(
        &self,
        query: &SemanticQuery,
        effective_dimensions: &[String],
        dimension_refs: &[DimensionRef],
        metric_refs: &[MetricRef],
    ) -> Result<String> {
        let mut base_metrics: Vec<String> = Vec::new();
        let mut seen_metrics: HashSet<String> = HashSet::new();
        let mut cumulative_metrics: Vec<MetricRef> = Vec::new();
        let mut time_comparison_metrics: Vec<MetricRef> = Vec::new();
        let mut offset_ratio_metrics: Vec<MetricRef> = Vec::new();
        let mut conversion_metrics: Vec<MetricRef> = Vec::new();

        for metric_ref in metric_refs {
            let model = self.graph.get_model(&metric_ref.model).ok_or_else(|| {
                let available: Vec<&str> = self.graph.models().map(|m| m.name.as_str()).collect();
                SidemanticError::model_not_found(&metric_ref.model, &available)
            })?;
            let metric = model.get_metric(&metric_ref.name).ok_or_else(|| {
                let available: Vec<&str> = model.metrics.iter().map(|m| m.name.as_str()).collect();
                SidemanticError::metric_not_found(&metric_ref.model, &metric_ref.name, &available)
            })?;

            match metric.r#type {
                MetricType::Cumulative => {
                    cumulative_metrics.push(metric_ref.clone());
                    if let Some(window_expr) = metric.window_expression.as_ref() {
                        for base_ref in
                            self.metric_refs_from_window_expression(window_expr, &metric_ref.model)
                        {
                            if seen_metrics.insert(base_ref.clone()) {
                                base_metrics.push(base_ref);
                            }
                        }
                        continue;
                    }
                    let base_ref = metric
                        .sql
                        .as_ref()
                        .or(metric.base_metric.as_ref())
                        .ok_or_else(|| {
                            SidemanticError::Validation(format!(
                                "Cumulative metric '{}' requires a base metric reference",
                                metric_ref.alias
                            ))
                        })?;
                    let qualified = self.metric_ref_for_inner_query(base_ref, &metric_ref.model);
                    if seen_metrics.insert(qualified.clone()) {
                        base_metrics.push(qualified);
                    }
                }
                MetricType::TimeComparison => {
                    time_comparison_metrics.push(metric_ref.clone());
                    let base_ref = metric.base_metric.as_ref().ok_or_else(|| {
                        SidemanticError::Validation(format!(
                            "time_comparison metric '{}' requires 'base_metric' field",
                            metric_ref.alias
                        ))
                    })?;
                    let qualified = self.metric_ref_for_inner_query(base_ref, &metric_ref.model);
                    if seen_metrics.insert(qualified.clone()) {
                        base_metrics.push(qualified);
                    }
                }
                MetricType::Ratio if metric.offset_window.is_some() => {
                    offset_ratio_metrics.push(metric_ref.clone());
                    if let Some(numerator) = metric.numerator.as_ref() {
                        let qualified =
                            self.metric_ref_for_inner_query(numerator, &metric_ref.model);
                        if seen_metrics.insert(qualified.clone()) {
                            base_metrics.push(qualified);
                        }
                    }
                    if let Some(denominator) = metric.denominator.as_ref() {
                        let qualified =
                            self.metric_ref_for_inner_query(denominator, &metric_ref.model);
                        if seen_metrics.insert(qualified.clone()) {
                            base_metrics.push(qualified);
                        }
                    }
                }
                MetricType::Conversion => {
                    conversion_metrics.push(metric_ref.clone());
                }
                _ => {
                    let explicit_ref = format!("{}.{}", metric_ref.model, metric_ref.name);
                    if seen_metrics.insert(explicit_ref.clone()) {
                        base_metrics.push(explicit_ref);
                    }
                }
            }
        }

        if let Some(conversion_metric_ref) = conversion_metrics.first() {
            return self.generate_conversion_query(
                conversion_metric_ref,
                dimension_refs,
                &query.filters,
                &query.order_by,
                query.limit,
                query.offset,
            );
        }

        let inner_query = SemanticQuery::new()
            .with_metrics(base_metrics.clone())
            .with_dimensions(effective_dimensions.to_vec())
            .with_filters(query.filters.clone())
            .with_segments(query.segments.clone())
            .with_ungrouped(false);

        let inner_sql = self.generate(&inner_query)?;
        let mut select_exprs: Vec<String> = Vec::new();
        let mut lag_cte_columns: Vec<String> = Vec::new();

        for dim_ref in dimension_refs {
            select_exprs.push(format!("base.{}", dim_ref.alias));
            lag_cte_columns.push(dim_ref.alias.clone());
        }

        for base_ref in &base_metrics {
            let alias = self.metric_alias_from_ref(base_ref);
            select_exprs.push(format!("base.{alias}"));
            lag_cte_columns.push(alias);
        }

        for metric_ref in &cumulative_metrics {
            let model = self.graph.get_model(&metric_ref.model).ok_or_else(|| {
                let available: Vec<&str> = self.graph.models().map(|m| m.name.as_str()).collect();
                SidemanticError::model_not_found(&metric_ref.model, &available)
            })?;
            let metric = model.get_metric(&metric_ref.name).ok_or_else(|| {
                let available: Vec<&str> = model.metrics.iter().map(|m| m.name.as_str()).collect();
                SidemanticError::metric_not_found(&metric_ref.model, &metric_ref.name, &available)
            })?;

            let (order_col, _) = if let Some(window_order) = metric.window_order.as_ref() {
                (format!("base.{window_order}"), None)
            } else {
                self.find_time_order_column(dimension_refs, Some(&metric_ref.model))?
            };

            if let Some(window_expr) = metric.window_expression.as_ref() {
                let frame = metric
                    .window_frame
                    .as_deref()
                    .unwrap_or("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");
                select_exprs.push(format!(
                    "{window_expr} OVER (ORDER BY {order_col} {frame}) AS {}",
                    metric_ref.alias
                ));
                continue;
            }

            let base_ref = metric
                .sql
                .as_ref()
                .or(metric.base_metric.as_ref())
                .ok_or_else(|| {
                    SidemanticError::Validation(format!(
                        "Cumulative metric '{}' requires a base metric reference",
                        metric_ref.alias
                    ))
                })?;
            let base_alias = self.metric_alias_from_ref(base_ref);
            let base_col = format!("base.{base_alias}");

            let agg_sql = match metric.agg {
                Some(Aggregation::Avg) => "AVG",
                Some(Aggregation::Min) => "MIN",
                Some(Aggregation::Max) => "MAX",
                Some(Aggregation::Count) | Some(Aggregation::CountDistinct) => "SUM",
                _ => "SUM",
            };

            let window_clause = if let Some(grain) = metric.grain_to_date.as_ref() {
                let grain = match grain {
                    crate::core::TimeGrain::Day => "day",
                    crate::core::TimeGrain::Week => "week",
                    crate::core::TimeGrain::Month => "month",
                    crate::core::TimeGrain::Quarter => "quarter",
                    crate::core::TimeGrain::Year => "year",
                };
                format!(
                    "PARTITION BY DATE_TRUNC('{grain}', {order_col}) ORDER BY {order_col} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
                )
            } else if let Some(window) = metric.window.as_ref() {
                let parts: Vec<&str> = window.split_whitespace().collect();
                if parts.len() == 2 {
                    format!(
                        "ORDER BY {order_col} RANGE BETWEEN INTERVAL '{}' PRECEDING AND CURRENT ROW",
                        parts.join(" ")
                    )
                } else {
                    format!("ORDER BY {order_col} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW")
                }
            } else {
                format!("ORDER BY {order_col} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW")
            };

            select_exprs.push(format!(
                "{agg_sql}({base_col}) OVER ({window_clause}) AS {}",
                metric_ref.alias
            ));
        }

        let mut sql = if !offset_ratio_metrics.is_empty() || !time_comparison_metrics.is_empty() {
            let mut lag_selects: Vec<String> = lag_cte_columns
                .iter()
                .map(|column| format!("base.{column}"))
                .collect();

            for metric_ref in &time_comparison_metrics {
                let model = self.graph.get_model(&metric_ref.model).ok_or_else(|| {
                    let available: Vec<&str> =
                        self.graph.models().map(|m| m.name.as_str()).collect();
                    SidemanticError::model_not_found(&metric_ref.model, &available)
                })?;
                let metric = model.get_metric(&metric_ref.name).ok_or_else(|| {
                    let available: Vec<&str> =
                        model.metrics.iter().map(|m| m.name.as_str()).collect();
                    SidemanticError::metric_not_found(
                        &metric_ref.model,
                        &metric_ref.name,
                        &available,
                    )
                })?;
                let (time_col, time_granularity) =
                    self.find_time_order_column(dimension_refs, None)?;
                let base_ref = metric.base_metric.as_ref().ok_or_else(|| {
                    SidemanticError::Validation(format!(
                        "time_comparison metric '{}' requires 'base_metric' field",
                        metric_ref.alias
                    ))
                })?;
                let base_alias = self.metric_alias_from_ref(base_ref);
                let lag_offset = self.calculate_lag_offset(
                    metric.comparison_type.as_ref(),
                    time_granularity.as_deref(),
                );
                let prev_alias = format!("{}_prev_value", metric_ref.alias);
                lag_selects.push(format!(
                    "LAG(base.{base_alias}, {lag_offset}) OVER (ORDER BY {time_col}) AS {prev_alias}"
                ));
            }

            for metric_ref in &offset_ratio_metrics {
                let model = self.graph.get_model(&metric_ref.model).ok_or_else(|| {
                    let available: Vec<&str> =
                        self.graph.models().map(|m| m.name.as_str()).collect();
                    SidemanticError::model_not_found(&metric_ref.model, &available)
                })?;
                let metric = model.get_metric(&metric_ref.name).ok_or_else(|| {
                    let available: Vec<&str> =
                        model.metrics.iter().map(|m| m.name.as_str()).collect();
                    SidemanticError::metric_not_found(
                        &metric_ref.model,
                        &metric_ref.name,
                        &available,
                    )
                })?;
                let (time_col, _) = self.find_time_order_column(dimension_refs, None)?;
                let denominator = metric.denominator.as_ref().ok_or_else(|| {
                    SidemanticError::Validation(format!(
                        "offset ratio metric '{}' requires denominator",
                        metric_ref.alias
                    ))
                })?;
                let denom_alias = self.metric_alias_from_ref(denominator);
                let prev_alias = format!("{}_prev_denom", metric_ref.alias);
                lag_selects.push(format!(
                    "LAG(base.{denom_alias}) OVER (ORDER BY {time_col}) AS {prev_alias}"
                ));
            }

            let mut lag_cte_sql = String::new();
            lag_cte_sql.push_str("WITH lag_cte AS (\n  SELECT\n    ");
            lag_cte_sql.push_str(&lag_selects.join(",\n    "));
            lag_cte_sql.push_str("\n  FROM (\n");
            lag_cte_sql.push_str(&inner_sql);
            lag_cte_sql.push_str("\n  ) AS base\n)");

            let mut final_selects = lag_cte_columns.clone();

            for metric_ref in &time_comparison_metrics {
                let model = self.graph.get_model(&metric_ref.model).ok_or_else(|| {
                    let available: Vec<&str> =
                        self.graph.models().map(|m| m.name.as_str()).collect();
                    SidemanticError::model_not_found(&metric_ref.model, &available)
                })?;
                let metric = model.get_metric(&metric_ref.name).ok_or_else(|| {
                    let available: Vec<&str> =
                        model.metrics.iter().map(|m| m.name.as_str()).collect();
                    SidemanticError::metric_not_found(
                        &metric_ref.model,
                        &metric_ref.name,
                        &available,
                    )
                })?;
                let base_ref = metric.base_metric.as_ref().ok_or_else(|| {
                    SidemanticError::Validation(format!(
                        "time_comparison metric '{}' requires 'base_metric' field",
                        metric_ref.alias
                    ))
                })?;
                let base_alias = self.metric_alias_from_ref(base_ref);
                let prev_value_col = format!("{}_prev_value", metric_ref.alias);
                let calculation = metric
                    .calculation
                    .as_ref()
                    .unwrap_or(&crate::core::ComparisonCalculation::PercentChange);
                let expr = match calculation {
                    crate::core::ComparisonCalculation::Difference => {
                        format!("({base_alias} - {prev_value_col}) AS {}", metric_ref.alias)
                    }
                    crate::core::ComparisonCalculation::PercentChange => format!(
                        "(({base_alias} - {prev_value_col}) / NULLIF({prev_value_col}, 0) * 100) AS {}",
                        metric_ref.alias
                    ),
                    crate::core::ComparisonCalculation::Ratio => {
                        format!("({base_alias} / NULLIF({prev_value_col}, 0)) AS {}", metric_ref.alias)
                    }
                };
                final_selects.push(expr);
            }

            for metric_ref in &offset_ratio_metrics {
                let model = self.graph.get_model(&metric_ref.model).ok_or_else(|| {
                    let available: Vec<&str> =
                        self.graph.models().map(|m| m.name.as_str()).collect();
                    SidemanticError::model_not_found(&metric_ref.model, &available)
                })?;
                let metric = model.get_metric(&metric_ref.name).ok_or_else(|| {
                    let available: Vec<&str> =
                        model.metrics.iter().map(|m| m.name.as_str()).collect();
                    SidemanticError::metric_not_found(
                        &metric_ref.model,
                        &metric_ref.name,
                        &available,
                    )
                })?;
                let numerator = metric.numerator.as_ref().ok_or_else(|| {
                    SidemanticError::Validation(format!(
                        "offset ratio metric '{}' requires numerator",
                        metric_ref.alias
                    ))
                })?;
                let numerator_alias = self.metric_alias_from_ref(numerator);
                let prev_denom_col = format!("{}_prev_denom", metric_ref.alias);
                final_selects.push(format!(
                    "{numerator_alias} / NULLIF({prev_denom_col}, 0) AS {}",
                    metric_ref.alias
                ));
            }

            format!(
                "{lag_cte_sql}\nSELECT\n  {}\nFROM lag_cte",
                final_selects.join(",\n  ")
            )
        } else {
            let mut query_sql = String::new();
            query_sql.push_str("SELECT\n  ");
            query_sql.push_str(&select_exprs.join(",\n  "));
            query_sql.push_str("\nFROM (\n");
            query_sql.push_str(&inner_sql);
            query_sql.push_str("\n) AS base");
            query_sql
        };

        if !query.order_by.is_empty() {
            let mut order_parts = Vec::new();
            for order in &query.order_by {
                let mut tokens = order.split_whitespace();
                let field = tokens.next().unwrap_or(order);
                let suffix = tokens.collect::<Vec<&str>>().join(" ");
                let alias = field
                    .split('.')
                    .next_back()
                    .map(str::to_string)
                    .unwrap_or_else(|| field.to_string());
                if suffix.is_empty() {
                    order_parts.push(alias);
                } else {
                    order_parts.push(format!("{alias} {suffix}"));
                }
            }
            sql.push_str(&format!("\nORDER BY {}", order_parts.join(", ")));
        }

        if let Some(limit) = query.limit {
            sql.push_str(&format!("\nLIMIT {limit}"));
        }
        if let Some(offset) = query.offset {
            sql.push_str(&format!("\nOFFSET {offset}"));
        }

        Ok(sql.trim_end().to_string())
    }

    fn metric_refs_from_window_expression(&self, expr: &str, default_model: &str) -> Vec<String> {
        let base_re =
            regex::Regex::new(r"\bbase\.([A-Za-z_][A-Za-z0-9_]*)\b").expect("valid base ref regex");
        let mut refs = Vec::new();
        let mut seen = HashSet::new();
        for cap in base_re.captures_iter(expr) {
            let Some(metric_match) = cap.get(1) else {
                continue;
            };
            let metric_name = metric_match.as_str();
            let qualified = self.metric_ref_for_inner_query(metric_name, default_model);
            if seen.insert(qualified.clone()) {
                refs.push(qualified);
            }
        }
        refs
    }

    fn metric_ref_for_inner_query(&self, reference: &str, default_model: &str) -> String {
        if reference.contains('.') {
            return reference.to_string();
        }
        if let Some(model) = self.graph.get_model(default_model) {
            if model.get_metric(reference).is_some() {
                return format!("{default_model}.{reference}");
            }
        }
        reference.to_string()
    }

    fn metric_alias_from_ref(&self, reference: &str) -> String {
        reference
            .split('.')
            .next_back()
            .map(str::to_string)
            .unwrap_or_else(|| reference.to_string())
    }

    fn find_time_order_column(
        &self,
        dimension_refs: &[DimensionRef],
        preferred_model: Option<&str>,
    ) -> Result<(String, Option<String>)> {
        for dim_ref in dimension_refs {
            if let Some(model_name) = preferred_model {
                if dim_ref.model != model_name {
                    continue;
                }
            }
            let Some(model) = self.graph.get_model(&dim_ref.model) else {
                continue;
            };
            let Some(dim) = model.get_dimension(&dim_ref.name) else {
                continue;
            };
            if dim.r#type == crate::core::DimensionType::Time {
                return Ok((
                    format!("base.{}", dim_ref.alias),
                    dim_ref.granularity.clone(),
                ));
            }
        }
        Err(SidemanticError::Validation(
            "Window/time comparison metrics require a time dimension".to_string(),
        ))
    }

    fn calculate_lag_offset(
        &self,
        comparison_type: Option<&crate::core::ComparisonType>,
        time_granularity: Option<&str>,
    ) -> i64 {
        use crate::core::ComparisonType;
        let Some(comparison_type) = comparison_type else {
            return 1;
        };

        if time_granularity.is_none() {
            return match comparison_type {
                ComparisonType::Dod => 1,
                ComparisonType::Wow => 1,
                ComparisonType::Mom => 1,
                ComparisonType::Qoq => 1,
                ComparisonType::Yoy => 12,
                ComparisonType::PriorPeriod => 1,
            };
        }

        let granularity = time_granularity.unwrap_or("day");
        match comparison_type {
            ComparisonType::Dod => 1,
            ComparisonType::Wow => match granularity {
                "day" => 7,
                _ => 1,
            },
            ComparisonType::Mom => match granularity {
                "day" => 30,
                "week" => 4,
                _ => 1,
            },
            ComparisonType::Qoq => match granularity {
                "day" => 90,
                "week" => 13,
                "month" => 3,
                _ => 1,
            },
            ComparisonType::Yoy => match granularity {
                "day" => 365,
                "week" => 52,
                "month" => 12,
                "quarter" => 4,
                _ => 1,
            },
            ComparisonType::PriorPeriod => 1,
        }
    }

    fn generate_conversion_query(
        &self,
        metric_ref: &MetricRef,
        dimension_refs: &[DimensionRef],
        _filters: &[String],
        order_by: &[String],
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<String> {
        let model = self.graph.get_model(&metric_ref.model).ok_or_else(|| {
            let available: Vec<&str> = self.graph.models().map(|m| m.name.as_str()).collect();
            SidemanticError::model_not_found(&metric_ref.model, &available)
        })?;
        let metric = model.get_metric(&metric_ref.name).ok_or_else(|| {
            let available: Vec<&str> = model.metrics.iter().map(|m| m.name.as_str()).collect();
            SidemanticError::metric_not_found(&metric_ref.model, &metric_ref.name, &available)
        })?;

        let entity = metric.entity.as_ref().ok_or_else(|| {
            SidemanticError::Validation(format!(
                "Conversion metric {} missing required fields",
                metric_ref.alias
            ))
        })?;
        let base_event = metric.base_event.as_ref().ok_or_else(|| {
            SidemanticError::Validation(format!(
                "Conversion metric {} missing required fields",
                metric_ref.alias
            ))
        })?;
        let conversion_event = metric.conversion_event.as_ref().ok_or_else(|| {
            SidemanticError::Validation(format!(
                "Conversion metric {} missing required fields",
                metric_ref.alias
            ))
        })?;
        let conversion_window = metric
            .conversion_window
            .as_deref()
            .unwrap_or("7 days")
            .to_string();
        let window_parts: Vec<&str> = conversion_window.split_whitespace().collect();
        let window_num = window_parts.first().copied().unwrap_or("7");
        let window_unit = window_parts.get(1).copied().unwrap_or("days");

        let mut event_type_dim: Option<String> = None;
        let mut timestamp_dim: Option<String> = None;
        for dim in &model.dimensions {
            if dim.r#type == crate::core::DimensionType::Time {
                timestamp_dim = Some(dim.name.clone());
            }
            let dim_name = dim.name.to_lowercase();
            if dim_name.contains("event") && dim_name.contains("type") {
                event_type_dim = Some(dim.name.clone());
            }
        }
        let event_type_dim = event_type_dim.ok_or_else(|| {
            SidemanticError::Validation(
                "Conversion metrics require event_type and timestamp dimensions".to_string(),
            )
        })?;
        let timestamp_dim = timestamp_dim.ok_or_else(|| {
            SidemanticError::Validation(
                "Conversion metrics require event_type and timestamp dimensions".to_string(),
            )
        })?;

        let from_clause = if let Some(model_sql) = model.sql.as_ref() {
            format!("({model_sql}) AS t")
        } else {
            model.table_name().to_string()
        };

        let mut dim_entries: Vec<(String, String)> = Vec::new();
        for dim_ref in dimension_refs {
            if dim_ref.model != model.name {
                continue;
            }
            let Some(dim_obj) = model.get_dimension(&dim_ref.name) else {
                continue;
            };
            let mut sql_col = dim_obj.sql_expr().to_string();
            let alias = if let Some(gran) = dim_ref.granularity.as_ref() {
                if dim_obj.r#type == crate::core::DimensionType::Time {
                    sql_col = format!("DATE_TRUNC('{gran}', {sql_col})");
                    format!("{}__{gran}", dim_ref.name)
                } else {
                    dim_ref.name.clone()
                }
            } else {
                dim_ref.name.clone()
            };
            dim_entries.push((alias, sql_col));
        }

        let mut extra_base_cols = String::new();
        let mut extra_conv_cols = String::new();
        let mut extra_conversions_cols = String::new();
        if !dim_entries.is_empty() {
            let base_col_list = dim_entries
                .iter()
                .map(|(alias, sql_col)| format!("{sql_col} AS {alias}"))
                .collect::<Vec<String>>()
                .join(",\n    ");
            extra_base_cols = format!(",\n    {base_col_list}");
            extra_conv_cols = extra_base_cols.clone();
            let conv_col_list = dim_entries
                .iter()
                .map(|(alias, _)| format!("base.{alias}"))
                .collect::<Vec<String>>()
                .join(",\n    ");
            extra_conversions_cols = format!(",\n    {conv_col_list}");
        }

        let mut join_on_parts = vec!["base_events.entity = conversions.entity".to_string()];
        for (alias, _) in &dim_entries {
            join_on_parts.push(format!(
                "base_events.{alias} IS NOT DISTINCT FROM conversions.{alias}"
            ));
        }
        let join_condition = join_on_parts.join("\n  AND ");

        let mut dim_select = String::new();
        let mut group_by = String::new();
        if !dim_entries.is_empty() {
            let dim_select_list = dim_entries
                .iter()
                .map(|(alias, _)| format!("base_events.{alias} AS {alias}"))
                .collect::<Vec<String>>()
                .join(",\n  ");
            dim_select = format!("  {dim_select_list},\n");
            let group_positions: Vec<String> =
                (1..=dim_entries.len()).map(|i| i.to_string()).collect();
            group_by = format!("\nGROUP BY\n  {}", group_positions.join(",\n  "));
        }

        let mut order_clause = String::new();
        if !order_by.is_empty() {
            let mut order_fields = Vec::new();
            for field in order_by {
                let mut parts = field.split_whitespace();
                let field_ref = parts.next().unwrap_or(field);
                let suffix = parts.collect::<Vec<&str>>().join(" ");
                let field_name = field_ref.split('.').next_back().unwrap_or(field_ref);
                if suffix.is_empty() {
                    order_fields.push(field_name.to_string());
                } else {
                    order_fields.push(format!("{field_name} {suffix}"));
                }
            }
            order_clause = format!("\nORDER BY {}", order_fields.join(", "));
        }

        let limit_clause = limit
            .map(|value| format!("\nLIMIT {value}"))
            .unwrap_or_default();
        let offset_clause = offset
            .map(|value| format!("\nOFFSET {value}"))
            .unwrap_or_default();

        Ok(format!(
            "WITH base_events AS (\n  SELECT\n    {entity} AS entity,\n    {timestamp_dim} AS event_time{extra_base_cols}\n  FROM {from_clause}\n  WHERE {event_type_dim} = '{base_event}'\n),\nconversion_events AS (\n  SELECT\n    {entity} AS entity,\n    {timestamp_dim} AS event_time{extra_conv_cols}\n  FROM {from_clause}\n  WHERE {event_type_dim} = '{conversion_event}'\n),\nconversions AS (\n  SELECT DISTINCT\n    base.entity{extra_conversions_cols}\n  FROM base_events base\n  JOIN conversion_events conv\n    ON base.entity = conv.entity\n    AND conv.event_time BETWEEN base.event_time AND base.event_time + INTERVAL '{window_num} {window_unit}'\n)\nSELECT\n{dim_select}  COUNT(DISTINCT conversions.entity)::FLOAT / NULLIF(COUNT(DISTINCT base_events.entity), 0) AS {}\nFROM base_events\nLEFT JOIN conversions ON {join_condition}{group_by}{order_clause}{limit_clause}{offset_clause}",
            metric.name
        ))
    }

    fn apply_default_time_dimensions(
        &self,
        metrics: &[String],
        dimensions: &[String],
    ) -> Result<Vec<String>> {
        let mut result = dimensions.to_vec();
        let mut models_with_time_dims: HashSet<String> = HashSet::new();

        for dim_ref in dimensions {
            let dim_base = dim_ref
                .split_once("__")
                .map(|(left, _)| left)
                .unwrap_or(dim_ref);
            let (model_name, dim_name, _) = self.graph.parse_reference(dim_base)?;
            if let Some(model) = self.graph.get_model(&model_name) {
                if let Some(dim) = model.get_dimension(&dim_name) {
                    if dim.r#type == crate::core::DimensionType::Time {
                        models_with_time_dims.insert(model_name);
                    }
                }
            }
        }

        let mut seen_models = HashSet::new();
        for metric_ref in metrics {
            if !metric_ref.contains('.') {
                continue;
            };
            let model_name = self.graph.parse_reference(metric_ref)?.0;

            if !seen_models.insert(model_name.clone()) {
                continue;
            }

            let Some(model) = self.graph.get_model(&model_name) else {
                continue;
            };
            let Some(default_time_dimension) = model.default_time_dimension.as_ref() else {
                continue;
            };

            if models_with_time_dims.contains(&model_name) {
                continue;
            }

            let mut dim_ref = format!("{model_name}.{default_time_dimension}");
            if let Some(grain) = model.default_grain.as_ref() {
                dim_ref = format!("{dim_ref}__{grain}");
            }

            if !result.contains(&dim_ref) {
                result.push(dim_ref);
            }
            models_with_time_dims.insert(model_name);
        }

        Ok(result)
    }

    #[allow(clippy::too_many_arguments)]
    fn try_use_preaggregation(
        &self,
        model_name: &str,
        metric_refs: &[MetricRef],
        dimension_refs: &[DimensionRef],
        filters: &[String],
        order_by: &[String],
        limit: Option<usize>,
        offset: Option<usize>,
        preagg_database: Option<&str>,
        preagg_schema: Option<&str>,
    ) -> Result<Option<String>> {
        let model = self.graph.get_model(model_name).ok_or_else(|| {
            let available: Vec<&str> = self.graph.models().map(|m| m.name.as_str()).collect();
            SidemanticError::model_not_found(model_name, &available)
        })?;

        if model.pre_aggregations.is_empty() {
            return Ok(None);
        }

        let query_metric_names: Vec<String> = metric_refs.iter().map(|m| m.name.clone()).collect();
        let query_dimension_names: Vec<String> =
            dimension_refs.iter().map(|d| d.name.clone()).collect();
        let query_granularity = dimension_refs.iter().find_map(|d| d.granularity.clone());
        let filter_columns = self.extract_filter_columns(filters);

        let mut best_match: Option<(crate::core::PreAggregation, i32)> = None;
        for preagg in &model.pre_aggregations {
            if !self.preaggregation_can_satisfy_query(
                model,
                preagg,
                &query_metric_names,
                &query_dimension_names,
                query_granularity.as_deref(),
                &filter_columns,
            ) {
                continue;
            }

            let score = self.score_preaggregation_match(
                preagg,
                &query_dimension_names,
                query_granularity.as_deref(),
            );
            if best_match
                .as_ref()
                .is_none_or(|(_, best_score)| score > *best_score)
            {
                best_match = Some((preagg.clone(), score));
            }
        }

        let Some((best_preagg, _)) = best_match else {
            return Ok(None);
        };

        let preagg_sql = self.generate_from_preaggregation(
            model,
            &best_preagg,
            metric_refs,
            dimension_refs,
            filters,
            order_by,
            limit,
            offset,
            preagg_database,
            preagg_schema,
        );

        Ok(Some(format!("{preagg_sql}\n-- used_preagg=true")))
    }

    fn preaggregation_can_satisfy_query(
        &self,
        model: &crate::core::Model,
        preagg: &crate::core::PreAggregation,
        query_metrics: &[String],
        query_dimensions: &[String],
        query_granularity: Option<&str>,
        filter_columns: &HashSet<String>,
    ) -> bool {
        let preagg_dims: HashSet<String> = preagg
            .dimensions
            .as_ref()
            .map(|d| d.iter().cloned().collect())
            .unwrap_or_default();
        let mut query_dims: HashSet<String> = query_dimensions.iter().cloned().collect();
        if let Some(time_dim) = preagg.time_dimension.as_ref() {
            query_dims.remove(time_dim);
        }
        if !query_dims.is_subset(&preagg_dims) {
            return false;
        }

        let preagg_measures: HashSet<String> = preagg
            .measures
            .as_ref()
            .map(|m| m.iter().cloned().collect())
            .unwrap_or_default();

        for metric_name in query_metrics {
            let Some(metric) = model.get_metric(metric_name) else {
                return false;
            };
            if !self.metric_derivable_from_preaggregation(metric, &preagg_measures) {
                return false;
            }
        }

        if let (Some(query_grain), Some(preagg_grain)) =
            (query_granularity, preagg.granularity.as_deref())
        {
            if !self.is_granularity_compatible(query_grain, preagg_grain) {
                return false;
            }
        }

        if !filter_columns.is_empty() {
            let mut available_columns: HashSet<String> = preagg
                .dimensions
                .as_ref()
                .map(|d| d.iter().cloned().collect())
                .unwrap_or_default();
            if let Some(time_dim) = preagg.time_dimension.as_ref() {
                available_columns.insert(time_dim.clone());
            }
            if !filter_columns.is_subset(&available_columns) {
                return false;
            }
        }

        true
    }

    fn metric_derivable_from_preaggregation(
        &self,
        metric: &crate::core::Metric,
        preagg_measures: &HashSet<String>,
    ) -> bool {
        if !preagg_measures.contains(&metric.name) {
            return false;
        }

        match metric.agg.as_ref() {
            None => true,
            Some(Aggregation::Sum | Aggregation::Count | Aggregation::Min | Aggregation::Max) => {
                true
            }
            Some(Aggregation::Avg) => self
                .find_count_measure_for_avg(&metric.name, preagg_measures)
                .is_some(),
            Some(Aggregation::CountDistinct) => false,
            Some(Aggregation::Median | Aggregation::Expression) => true,
        }
    }

    fn find_count_measure_for_avg(
        &self,
        avg_metric_name: &str,
        preagg_measures: &HashSet<String>,
    ) -> Option<String> {
        if let Some(base_name) = avg_metric_name.strip_prefix("avg_") {
            let candidate = format!("count_{base_name}");
            if preagg_measures.contains(&candidate) {
                return Some(candidate);
            }
        }

        if avg_metric_name.contains("_avg") {
            let candidate = avg_metric_name.replace("_avg", "_count");
            if preagg_measures.contains(&candidate) {
                return Some(candidate);
            }
        }

        if preagg_measures.contains("count") {
            return Some("count".to_string());
        }

        let count_word_re =
            regex::Regex::new(r"(?:^|_)count(?:$|_)").expect("valid count-word regex");
        let mut measures: Vec<&String> = preagg_measures.iter().collect();
        measures.sort();
        for measure in measures {
            if count_word_re.is_match(measure) {
                return Some(measure.clone());
            }
        }

        None
    }

    fn extract_filter_columns(&self, filters: &[String]) -> HashSet<String> {
        let mut columns = HashSet::new();
        let col_re =
            regex::Regex::new(r"(\w+\.)?(\w+)\s*[=<>!]").expect("valid filter-column regex");
        for filter in filters {
            for captures in col_re.captures_iter(filter) {
                let Some(column) = captures.get(2) else {
                    continue;
                };
                columns.insert(column.as_str().to_string());
            }
        }
        columns
    }

    fn granularity_level(granularity: &str) -> Option<i32> {
        match granularity {
            "year" => Some(1),
            "quarter" => Some(2),
            "month" => Some(3),
            "week" => Some(4),
            "day" => Some(5),
            "hour" => Some(6),
            _ => None,
        }
    }

    fn is_granularity_compatible(&self, query_grain: &str, preagg_grain: &str) -> bool {
        if preagg_grain == "week" && matches!(query_grain, "month" | "quarter" | "year") {
            return false;
        }

        let query_level = Self::granularity_level(query_grain);
        let preagg_level = Self::granularity_level(preagg_grain);
        match (query_level, preagg_level) {
            (Some(q), Some(p)) => q <= p,
            _ => query_grain == preagg_grain,
        }
    }

    fn score_preaggregation_match(
        &self,
        preagg: &crate::core::PreAggregation,
        query_dimensions: &[String],
        query_granularity: Option<&str>,
    ) -> i32 {
        let preagg_dims: HashSet<String> = preagg
            .dimensions
            .as_ref()
            .map(|d| d.iter().cloned().collect())
            .unwrap_or_default();
        let mut query_dims: HashSet<String> = query_dimensions.iter().cloned().collect();
        if let Some(time_dim) = preagg.time_dimension.as_ref() {
            query_dims.remove(time_dim);
        }

        let mut score: i32 = 0;
        if preagg_dims == query_dims {
            score += 1000;
        }

        let extra_dims = preagg_dims.difference(&query_dims).count() as i32;
        score -= extra_dims * 10;

        if let (Some(query_grain), Some(preagg_grain)) =
            (query_granularity, preagg.granularity.as_deref())
        {
            if query_grain == preagg_grain {
                score += 100;
            } else {
                let query_level = Self::granularity_level(query_grain).unwrap_or(0);
                let preagg_level = Self::granularity_level(preagg_grain).unwrap_or(0);
                score -= (query_level - preagg_level).abs() * 5;
            }
        }

        score
    }

    #[allow(clippy::too_many_arguments)]
    fn generate_from_preaggregation(
        &self,
        model: &crate::core::Model,
        preagg: &crate::core::PreAggregation,
        metric_refs: &[MetricRef],
        dimension_refs: &[DimensionRef],
        filters: &[String],
        order_by: &[String],
        limit: Option<usize>,
        offset: Option<usize>,
        preagg_database: Option<&str>,
        preagg_schema: Option<&str>,
    ) -> String {
        let preagg_table = preagg.table_name(&model.name, preagg_database, preagg_schema);
        let mut select_parts: Vec<String> = Vec::new();

        for dim_ref in dimension_refs {
            let dim_name = &dim_ref.name;
            if let (Some(query_grain), Some(preagg_time_dim), Some(preagg_grain)) = (
                dim_ref.granularity.as_deref(),
                preagg.time_dimension.as_deref(),
                preagg.granularity.as_deref(),
            ) {
                if preagg_time_dim == dim_name {
                    let preagg_col = format!("{dim_name}_{preagg_grain}");
                    if query_grain == preagg_grain {
                        select_parts.push(format!("{preagg_col} AS {}__{query_grain}", dim_name));
                    } else {
                        select_parts.push(format!(
                            "DATE_TRUNC('{query_grain}', {preagg_col}) AS {}__{query_grain}",
                            dim_name
                        ));
                    }
                    continue;
                }
            }
            select_parts.push(dim_name.clone());
        }

        let preagg_measures: HashSet<String> = preagg
            .measures
            .as_ref()
            .map(|m| m.iter().cloned().collect())
            .unwrap_or_default();

        for metric_ref in metric_refs {
            let Some(metric) = model.get_metric(&metric_ref.name) else {
                continue;
            };
            let raw_col = format!("{}_raw", metric_ref.name);
            let expr = match metric.agg.as_ref() {
                Some(Aggregation::Sum | Aggregation::Count) => {
                    format!("SUM({raw_col}) AS {}", metric_ref.name)
                }
                Some(Aggregation::Avg) => {
                    let count_measure = self
                        .find_count_measure_for_avg(&metric_ref.name, &preagg_measures)
                        .unwrap_or_else(|| "count".to_string());
                    let count_col = format!("{count_measure}_raw");
                    format!(
                        "SUM({raw_col}) / NULLIF(SUM({count_col}), 0) AS {}",
                        metric_ref.name
                    )
                }
                Some(Aggregation::Min) => format!("MIN({raw_col}) AS {}", metric_ref.name),
                Some(Aggregation::Max) => format!("MAX({raw_col}) AS {}", metric_ref.name),
                _ => format!("SUM({raw_col}) AS {}", metric_ref.name),
            };
            select_parts.push(expr);
        }

        let mut sql = format!(
            "SELECT\n  {}\nFROM {preagg_table}",
            select_parts.join(",\n  ")
        );

        if !filters.is_empty() {
            let mut rewritten = Vec::with_capacity(filters.len());
            let time_col_re = preagg
                .time_dimension
                .as_ref()
                .zip(preagg.granularity.as_ref())
                .map(|(dim, grain)| {
                    (
                        regex::Regex::new(&format!(r"\b{}\b", regex::escape(dim)))
                            .expect("valid preagg time-column regex"),
                        format!("{dim}_{grain}"),
                    )
                });
            for filter in filters {
                let mut filter_sql = filter.replace(&format!("{}.", model.name), "");
                filter_sql = filter_sql.replace(&format!("{}_cte.", model.name), "");
                if let Some((pattern, replacement)) = &time_col_re {
                    filter_sql = pattern
                        .replace_all(&filter_sql, replacement.as_str())
                        .into_owned();
                }
                rewritten.push(filter_sql);
            }
            sql.push_str(&format!("\nWHERE {}", rewritten.join(" AND ")));
        }

        if !dimension_refs.is_empty() {
            let group_by: Vec<String> = (1..=dimension_refs.len()).map(|i| i.to_string()).collect();
            sql.push_str(&format!("\nGROUP BY {}", group_by.join(", ")));
        }

        if !order_by.is_empty() {
            let mut order_clauses = Vec::new();
            for order in order_by {
                let mut parts = order.split_whitespace();
                let field = parts.next().unwrap_or(order);
                let suffix = parts.collect::<Vec<&str>>().join(" ");
                let field_name = field.split('.').next_back().unwrap_or(field);
                if suffix.is_empty() {
                    order_clauses.push(field_name.to_string());
                } else {
                    order_clauses.push(format!("{field_name} {suffix}"));
                }
            }
            sql.push_str(&format!("\nORDER BY {}", order_clauses.join(", ")));
        }

        if let Some(limit) = limit {
            sql.push_str(&format!("\nLIMIT {limit}"));
        }
        if let Some(offset) = offset {
            sql.push_str(&format!("\nOFFSET {offset}"));
        }

        sql
    }

    fn split_filters(
        &self,
        filters: &[String],
        collisions: &HashMap<String, usize>,
    ) -> Result<(Vec<String>, Vec<String>)> {
        let mut where_filters = Vec::new();
        let mut having_filters = Vec::new();
        let ref_re = regex::Regex::new(r"\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b")
            .expect("valid model.field regex");

        for filter in filters {
            let mut rewritten = filter.clone();
            let mut uses_metric = false;

            for cap in ref_re.captures_iter(filter) {
                let Some(model_match) = cap.get(1) else {
                    continue;
                };
                let Some(field_match) = cap.get(2) else {
                    continue;
                };
                let model_name = model_match.as_str();
                let field_name = field_match.as_str();
                let Some(model) = self.graph.get_model(model_name) else {
                    continue;
                };
                if model.get_metric(field_name).is_none() {
                    continue;
                }

                uses_metric = true;
                let replacement = self.output_alias(model_name, field_name, collisions);
                let full_ref = format!("{model_name}.{field_name}");
                rewritten = rewritten.replace(&full_ref, &replacement);
            }

            if uses_metric {
                having_filters.push(rewritten);
            } else {
                where_filters.push(filter.clone());
            }
        }

        Ok((where_filters, having_filters))
    }

    fn classify_filters_for_cte_pushdown(
        &self,
        filters: &[String],
        cte_models: &[String],
    ) -> Result<CtePushdownClassification> {
        let cte_model_set: HashSet<&str> = cte_models.iter().map(String::as_str).collect();
        let mut pushdown_filters: HashMap<String, Vec<String>> = HashMap::new();
        let mut main_filters = Vec::new();

        for filter in filters {
            for filter_part in self.split_conjunctive_filter(filter) {
                let referenced_models = self.filter_referenced_models(&filter_part, &cte_model_set);
                let references_metric = self.filter_references_metric(&filter_part, &cte_model_set);
                let references_window_dimension =
                    self.filter_references_window_dimension(&filter_part, &cte_model_set);

                if !references_metric
                    && !references_window_dimension
                    && referenced_models.len() == 1
                {
                    let model_name = referenced_models
                        .iter()
                        .next()
                        .expect("one referenced model")
                        .clone();
                    pushdown_filters
                        .entry(model_name)
                        .or_default()
                        .push(filter_part);
                } else if !references_metric
                    && !references_window_dimension
                    && referenced_models.is_empty()
                    && cte_models.len() == 1
                    && !filter_part.contains('.')
                {
                    pushdown_filters
                        .entry(cte_models[0].clone())
                        .or_default()
                        .push(filter_part);
                } else {
                    main_filters.push(filter_part);
                }
            }
        }

        Ok((pushdown_filters, main_filters))
    }

    fn split_conjunctive_filter(&self, filter: &str) -> Vec<String> {
        let upper = filter.to_ascii_uppercase();
        if upper.contains(" OR ") || upper.contains(" BETWEEN ") {
            return vec![filter.to_string()];
        }
        let and_re = regex::Regex::new(r"(?i)\s+AND\s+").expect("valid AND splitter regex");
        let parts: Vec<String> = and_re
            .split(filter)
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .map(str::to_string)
            .collect();
        if parts.is_empty() {
            vec![filter.to_string()]
        } else {
            parts
        }
    }

    fn filter_referenced_models(
        &self,
        filter: &str,
        cte_models: &HashSet<&str>,
    ) -> HashSet<String> {
        let mut referenced_models = HashSet::new();
        let ref_re = regex::Regex::new(r"\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b")
            .expect("valid model.field regex");

        for cap in ref_re.captures_iter(filter) {
            let Some(table_match) = cap.get(1) else {
                continue;
            };
            let table_name = table_match.as_str();
            let model_name = table_name.strip_suffix("_cte").unwrap_or(table_name);
            if cte_models.contains(model_name) {
                referenced_models.insert(model_name.to_string());
            }
        }

        referenced_models
    }

    fn filter_references_metric(&self, filter: &str, cte_models: &HashSet<&str>) -> bool {
        let ref_re = regex::Regex::new(r"\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b")
            .expect("valid model.field regex");

        for cap in ref_re.captures_iter(filter) {
            let Some(table_match) = cap.get(1) else {
                continue;
            };
            let Some(field_match) = cap.get(2) else {
                continue;
            };
            let table_name = table_match.as_str();
            let model_name = table_name.strip_suffix("_cte").unwrap_or(table_name);
            if !cte_models.contains(model_name) {
                continue;
            }
            if self
                .graph
                .get_model(model_name)
                .and_then(|model| model.get_metric(field_match.as_str()))
                .is_some()
            {
                return true;
            }
        }

        for token in Self::identifier_tokens(filter) {
            if Self::is_sql_keyword_or_function(&token) {
                continue;
            }
            for model_name in cte_models {
                if self
                    .graph
                    .get_model(model_name)
                    .and_then(|model| model.get_metric(&token))
                    .is_some()
                {
                    return true;
                }
            }
        }

        false
    }

    fn filter_references_window_dimension(&self, filter: &str, cte_models: &HashSet<&str>) -> bool {
        !self
            .filter_window_dimension_models(filter, cte_models)
            .is_empty()
    }

    fn filter_window_dimension_models(
        &self,
        filter: &str,
        cte_models: &HashSet<&str>,
    ) -> HashSet<String> {
        let mut models = HashSet::new();
        let ref_re = regex::Regex::new(r"\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b")
            .expect("valid model.field regex");

        for cap in ref_re.captures_iter(filter) {
            let Some(table_match) = cap.get(1) else {
                continue;
            };
            let Some(field_match) = cap.get(2) else {
                continue;
            };
            let table_name = table_match.as_str();
            let model_name = table_name.strip_suffix("_cte").unwrap_or(table_name);
            if !cte_models.contains(model_name) {
                continue;
            }
            if self
                .graph
                .get_model(model_name)
                .and_then(|model| model.get_dimension(field_match.as_str()))
                .and_then(|dimension| dimension.window.as_ref())
                .is_some()
            {
                models.insert(model_name.to_string());
            }
        }

        for token in Self::identifier_tokens(filter) {
            if Self::is_sql_keyword_or_function(&token) {
                continue;
            }
            for model_name in cte_models {
                if self
                    .graph
                    .get_model(model_name)
                    .and_then(|model| model.get_dimension(&token))
                    .and_then(|dimension| dimension.window.as_ref())
                    .is_some()
                {
                    models.insert((*model_name).to_string());
                }
            }
        }

        models
    }

    fn expand_filters_for_cte(&self, model_name: &str, filters: &[String]) -> Result<Vec<String>> {
        let model = self.graph.get_model(model_name).ok_or_else(|| {
            let available: Vec<&str> = self.graph.models().map(|m| m.name.as_str()).collect();
            SidemanticError::model_not_found(model_name, &available)
        })?;
        let alias = self.model_alias(model_name);
        let cte_name = format!("{model_name}_cte");
        let mut expanded = Vec::with_capacity(filters.len());

        for filter in filters {
            let mut filter_sql = filter.clone();
            for dim in &model.dimensions {
                let source_expr = self.normalize_cte_source_expression(dim.sql_expr());
                for qualified in [
                    format!("{model_name}.{}", dim.name),
                    format!("{cte_name}.{}", dim.name),
                    format!("{alias}.{}", dim.name),
                ] {
                    filter_sql = filter_sql.replace(&qualified, &source_expr);
                }
            }

            for prefix in [
                format!("{model_name}."),
                format!("{cte_name}."),
                format!("{alias}."),
            ] {
                filter_sql = filter_sql.replace(&prefix, "");
            }
            filter_sql = filter_sql.replace("{model}.", "");
            filter_sql = filter_sql.replace("{model}", "");
            expanded.push(self.expand_relative_dates(&filter_sql));
        }

        Ok(expanded)
    }

    fn normalize_metric_filters(
        &self,
        filters: &[String],
        model_name: &str,
        alias: &str,
    ) -> String {
        let mut rendered = Vec::with_capacity(filters.len());
        for filter in filters {
            let mut f = filter.clone();
            f = f.replace("{model}.", "");
            f = f.replace("{model}", alias);
            f = f.replace(&format!("{model_name}."), "");
            f = f.replace(&format!("{model_name}_cte."), "");
            f = f.replace(&format!("{alias}."), "");
            rendered.push(f);
        }
        rendered.join(" AND ")
    }

    fn normalize_cte_source_expression(&self, expr: &str) -> String {
        expr.replace("{model}.", "").replace("{model}", "")
    }

    fn normalize_select_expression(&self, expr: &str, alias: &str) -> String {
        expr.replace("{model}", alias)
    }

    fn dimension_select_expression(
        &self,
        dimension: &crate::core::Dimension,
        alias: &str,
    ) -> String {
        let expr = dimension.sql_expr();
        if expr.contains("{model}") {
            self.normalize_select_expression(expr, alias)
        } else {
            format!("{}.{}", alias, expr)
        }
    }

    fn is_simple_identifier(identifier: &str) -> bool {
        let ident_re =
            regex::Regex::new(r"^[A-Za-z_][A-Za-z0-9_]*$").expect("valid identifier regex");
        ident_re.is_match(identifier)
    }

    fn quote_identifier(&self, identifier: &str) -> String {
        if Self::is_simple_identifier(identifier) {
            identifier.to_string()
        } else {
            format!("\"{}\"", identifier.replace('"', "\"\""))
        }
    }

    fn identifier_tokens(expr: &str) -> Vec<String> {
        let ref_re =
            regex::Regex::new(r"\b[A-Za-z_][A-Za-z0-9_]*\b").expect("valid identifier regex");
        ref_re
            .find_iter(expr)
            .map(|token| token.as_str().to_string())
            .collect()
    }

    fn is_sql_keyword_or_function(token: &str) -> bool {
        matches!(
            token.to_ascii_uppercase().as_str(),
            "AND"
                | "AS"
                | "BETWEEN"
                | "BY"
                | "CASE"
                | "CAST"
                | "COALESCE"
                | "COUNT"
                | "CURRENT_DATE"
                | "CURRENT_TIMESTAMP"
                | "DATE"
                | "DATE_TRUNC"
                | "DAY"
                | "DECIMAL"
                | "DISTINCT"
                | "ELSE"
                | "END"
                | "FALSE"
                | "FLOAT"
                | "FROM"
                | "GROUP"
                | "IF"
                | "IN"
                | "INTEGER"
                | "INTERVAL"
                | "IS"
                | "LIKE"
                | "MAX"
                | "MEDIAN"
                | "MIN"
                | "MODE"
                | "MONTH"
                | "NOT"
                | "NULL"
                | "NULLIF"
                | "OR"
                | "ORDER"
                | "OVER"
                | "PARTITION"
                | "PERCENTILE_CONT"
                | "PERCENTILE_DISC"
                | "QUARTER"
                | "QUANTILE_CONT"
                | "QUANTILE_DISC"
                | "RANGE"
                | "ROWS"
                | "STDDEV"
                | "STDDEV_POP"
                | "SUM"
                | "THEN"
                | "TRUE"
                | "VARIANCE"
                | "VARIANCE_POP"
                | "VARCHAR"
                | "WHEN"
                | "WHERE"
                | "WITHIN"
                | "YEAR"
        )
    }

    fn simple_metric_reference_sql(
        &self,
        metric: &crate::core::Metric,
        metric_name: &str,
        alias: &str,
    ) -> String {
        let raw_col = format!("{alias}.{metric_name}_raw");
        match metric.agg.as_ref() {
            Some(Aggregation::CountDistinct) => format!("COUNT(DISTINCT {raw_col})"),
            Some(Aggregation::Count) => format!("COUNT({raw_col})"),
            Some(agg) if agg != &Aggregation::Expression => {
                format!("{}({raw_col})", agg.as_sql())
            }
            _ => format!("SUM({raw_col})"),
        }
    }

    fn metric_expression_for_reference(
        &self,
        reference: &str,
        default_model: &str,
        visited: &mut HashSet<(String, String)>,
    ) -> Result<Option<String>> {
        let (model_name, metric_name) = if reference.contains('.') {
            let (m, n, _) = self.graph.parse_reference(reference)?;
            (m, n)
        } else {
            let mut owners = Vec::new();
            for model in self.graph.models() {
                if model.get_metric(reference).is_some() {
                    owners.push(model.name.clone());
                }
            }
            if owners.len() == 1 {
                (owners[0].clone(), reference.to_string())
            } else if let Some(default) = self.graph.get_model(default_model) {
                if default.get_metric(reference).is_some() {
                    (default_model.to_string(), reference.to_string())
                } else {
                    return Ok(None);
                }
            } else {
                return Ok(None);
            }
        };

        let key = (model_name.clone(), metric_name.clone());
        if !visited.insert(key.clone()) {
            return Ok(None);
        }

        let model = self.graph.get_model(&model_name).ok_or_else(|| {
            let available: Vec<&str> = self.graph.models().map(|m| m.name.as_str()).collect();
            SidemanticError::model_not_found(&model_name, &available)
        })?;
        let metric = model.get_metric(&metric_name).ok_or_else(|| {
            let available: Vec<&str> = model.metrics.iter().map(|m| m.name.as_str()).collect();
            SidemanticError::metric_not_found(&model_name, &metric_name, &available)
        })?;

        let alias = self.model_alias(&model_name);
        let expanded = match metric.r#type {
            MetricType::Simple => self.simple_metric_reference_sql(metric, &metric_name, &alias),
            MetricType::Derived => {
                self.expand_derived_metric_inner(metric.sql_expr(), &model_name, visited)?
            }
            MetricType::Ratio => {
                let num_ref = metric.numerator.as_deref().unwrap_or("1");
                let den_ref = metric.denominator.as_deref().unwrap_or("1");
                let num_sql = self
                    .metric_expression_for_reference(num_ref, &model_name, visited)?
                    .unwrap_or_else(|| num_ref.to_string());
                let den_sql = self
                    .metric_expression_for_reference(den_ref, &model_name, visited)?
                    .unwrap_or_else(|| den_ref.to_string());
                format!("({num_sql}) / NULLIF({den_sql}, 0)")
            }
            _ => metric.to_sql(Some(&alias)),
        };

        visited.remove(&key);
        Ok(Some(expanded))
    }

    /// Expand a derived metric expression, replacing metric references with their SQL
    fn expand_derived_metric(&self, expr: &str, default_model: &str) -> Result<String> {
        let mut visited = HashSet::new();
        self.expand_derived_metric_inner(expr, default_model, &mut visited)
    }

    fn expand_derived_metric_inner(
        &self,
        expr: &str,
        default_model: &str,
        visited: &mut HashSet<(String, String)>,
    ) -> Result<String> {
        if Self::is_inline_aggregate_expression(expr) {
            return self.rewrite_inline_aggregate_expression(expr, default_model);
        }

        let mut result = expr.to_string();
        let ref_re = regex::Regex::new(
            r"\b([A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*|[A-Za-z_][A-Za-z0-9_]*)\b",
        )
        .expect("valid metric token regex");

        let mut replacements: Vec<(String, String)> = Vec::new();
        let mut unresolved_tokens: Vec<String> = Vec::new();
        for cap in ref_re.captures_iter(expr) {
            let Some(token_match) = cap.get(1) else {
                continue;
            };
            let token = token_match.as_str();
            if let Some(expanded) =
                self.metric_expression_for_reference(token, default_model, visited)?
            {
                replacements.push((token.to_string(), expanded));
            } else if self.should_error_for_unresolved_derived_token(token) {
                unresolved_tokens.push(token.to_string());
            }
        }

        unresolved_tokens.sort();
        unresolved_tokens.dedup();
        if let Some(token) = unresolved_tokens.first() {
            return Err(SidemanticError::Validation(format!(
                "Metric not found: '{token}'"
            )));
        }

        // Replace longer tokens first to avoid partial replacement collisions.
        replacements.sort_by(|a, b| b.0.len().cmp(&a.0.len()));
        for (token, replacement) in replacements {
            let pattern = regex::Regex::new(&format!(r"\b{}\b", regex::escape(&token)))
                .expect("escaped token regex");
            result = pattern
                .replace_all(&result, format!("({replacement})"))
                .into_owned();
        }

        Ok(result)
    }

    fn should_error_for_unresolved_derived_token(&self, token: &str) -> bool {
        if Self::is_sql_keyword_or_function(token) {
            return false;
        }
        if token.contains('.') {
            let field_name = token.split('.').next_back().unwrap_or(token);
            return !Self::is_sql_keyword_or_function(field_name);
        }
        true
    }

    fn is_inline_aggregate_expression(expr: &str) -> bool {
        let aggregate_re = regex::Regex::new(
            r"(?i)\b(SUM|AVG|COUNT|MIN|MAX|MEDIAN|MODE|PERCENTILE_CONT|PERCENTILE_DISC|QUANTILE_CONT|QUANTILE_DISC|STDDEV|STDDEV_POP|VARIANCE|VARIANCE_POP)\s*\(",
        )
        .expect("valid aggregate regex");
        aggregate_re.is_match(expr)
    }

    fn rewrite_inline_aggregate_expression(
        &self,
        expr: &str,
        default_model: &str,
    ) -> Result<String> {
        let mut result = expr.to_string();
        if self.graph.get_model(default_model).is_some() {
            let default_alias = self.model_alias(default_model);
            result = result.replace("{model}", &default_alias);
        }

        for model in self.graph.models() {
            let alias = self.model_alias(&model.name);
            for dim in &model.dimensions {
                let replacement = format!("{}.{}", alias, self.quote_identifier(&dim.name));
                for token in [
                    format!("{}.{}", model.name, dim.name),
                    format!("{}_cte.{}", model.name, dim.name),
                    format!("{}.{}", alias, dim.name),
                ] {
                    let pattern = regex::Regex::new(&format!(r"\b{}\b", regex::escape(&token)))
                        .expect("escaped dimension regex");
                    result = pattern
                        .replace_all(&result, replacement.as_str())
                        .into_owned();
                }
            }
            result = result.replace(&format!("{}.", model.name), &format!("{alias}."));
            result = result.replace(&format!("{}_cte.", model.name), &format!("{alias}."));
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
                let cte_name = format!("{}_cte", model.name);

                // Replace model references with aliases
                for dim in &model.dimensions {
                    let replacement = format!("{}.{}", alias, dim.sql_expr());
                    let model_pattern = format!("{}.{}", model.name, dim.name);
                    let cte_pattern = format!("{}.{}", cte_name, dim.name);
                    expanded_filter = expanded_filter.replace(&model_pattern, &replacement);
                    expanded_filter = expanded_filter.replace(&cte_pattern, &replacement);
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
        let comparison_re = regex::Regex::new(r#"^(.+?)\s*(>=|<=|>|<|=)\s*['"](.+?)['"]$"#)
            .expect("valid relative date comparison regex");
        if let Some(cap) = comparison_re.captures(filter.trim()) {
            let column = cap.get(1).map(|m| m.as_str().trim()).unwrap_or("");
            let operator = cap.get(2).map(|m| m.as_str()).unwrap_or("");
            let value = cap.get(3).map(|m| m.as_str()).unwrap_or("");

            if RelativeDate::is_relative_date(value) {
                if operator == "=" {
                    if let Some(range_sql) = RelativeDate::to_range(value, column) {
                        return range_sql;
                    }
                } else if matches!(operator, ">=" | ">") {
                    if let Some(sql_date) = RelativeDate::parse(value) {
                        return format!("{column} {operator} {sql_date}");
                    }
                }
            }
        }

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
                if step.relationship_type == RelationshipType::ManyToOne {
                    // Joining from a many-side base grain to a one-side model duplicates
                    // metrics owned by the one-side model across the base rows.
                    at_risk.insert(step.to_model.clone());
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
        assert!(sql.contains("SUM(orders_cte.revenue_raw) AS revenue"));
        assert!(sql.contains("orders_cte.status AS status"));
        assert!(sql.contains("FROM orders_cte AS orders_cte"));
        assert!(sql.contains("GROUP BY 1"));
    }

    #[test]
    fn test_count_builder_uses_valid_raw_cte_expression() {
        let graph = create_test_graph();
        let generator = SqlGenerator::new(&graph);

        let query = SemanticQuery::new().with_metrics(vec!["orders.order_count".into()]);

        let sql = generator.generate(&query).unwrap();

        assert!(
            sql.contains("1 AS order_count_raw"),
            "expected count raw expression to be a valid scalar: {sql}"
        );
        assert!(
            !sql.contains("* AS order_count_raw"),
            "count raw expression must not project bare star: {sql}"
        );
    }

    #[test]
    fn test_derived_metric_includes_simple_raw_dependencies() {
        let mut graph = SemanticGraph::new();
        let orders = Model::new("orders", "order_id")
            .with_table("orders")
            .with_metric(Metric::sum("revenue", "amount"))
            .with_metric(Metric::count("order_count"))
            .with_metric(Metric::derived("avg_order_value", "revenue / order_count"));
        graph.add_model(orders).unwrap();
        let generator = SqlGenerator::new(&graph);

        let query = SemanticQuery::new().with_metrics(vec!["orders.avg_order_value".into()]);

        let sql = generator.generate(&query).unwrap();

        assert!(
            sql.contains("amount AS revenue_raw"),
            "expected derived metric dependency raw column: {sql}"
        );
        assert!(
            sql.contains("1 AS order_count_raw"),
            "expected derived count dependency raw column: {sql}"
        );
        assert!(
            sql.contains("SUM(orders_cte.revenue_raw)"),
            "expected derived metric to aggregate revenue dependency: {sql}"
        );
        assert!(
            sql.contains("COUNT(orders_cte.order_count_raw)"),
            "expected derived metric to aggregate count dependency: {sql}"
        );
    }

    #[test]
    fn test_ordered_set_aggregate_metric_is_not_treated_as_metric_reference() {
        let mut graph = SemanticGraph::new();
        let ordered_set = Model::new("ordered_set_v", "category")
            .with_table("ordered_set_test")
            .with_dimension(Dimension::categorical("category"))
            .with_metric(Metric::derived("mode_value", "MODE(value)"));
        graph.add_model(ordered_set).unwrap();
        let generator = SqlGenerator::new(&graph);

        let query = SemanticQuery::new()
            .with_metrics(vec!["ordered_set_v.mode_value".into()])
            .with_dimensions(vec!["ordered_set_v.category".into()]);

        let sql = generator.generate(&query).unwrap();

        assert!(sql.contains("MODE(value) AS mode_value"), "{sql}");
    }

    #[test]
    fn test_ratio_metric_includes_simple_raw_dependencies() {
        let mut graph = SemanticGraph::new();
        let orders = Model::new("orders", "order_id")
            .with_table("orders")
            .with_metric(Metric::sum("revenue", "amount"))
            .with_metric(Metric::count("order_count"))
            .with_metric(Metric::ratio("avg_order_value", "revenue", "order_count"));
        graph.add_model(orders).unwrap();
        let generator = SqlGenerator::new(&graph);

        let query = SemanticQuery::new().with_metrics(vec!["orders.avg_order_value".into()]);

        let sql = generator.generate(&query).unwrap();

        assert!(sql.contains("amount AS revenue_raw"), "{sql}");
        assert!(sql.contains("1 AS order_count_raw"), "{sql}");
        assert!(sql.contains("SUM(orders_cte.revenue_raw)"), "{sql}");
        assert!(sql.contains("COUNT(orders_cte.order_count_raw)"), "{sql}");
    }

    #[test]
    fn test_semantic_order_by_rewrites_to_output_aliases() {
        let graph = create_test_graph();
        let generator = SqlGenerator::new(&graph);

        let query = SemanticQuery::new()
            .with_metrics(vec!["orders.revenue".into()])
            .with_dimensions(vec!["orders.status".into()])
            .with_order_by(vec![
                "orders.revenue DESC".into(),
                "orders.status ASC".into(),
            ]);

        let sql = generator.generate(&query).unwrap();

        assert!(
            sql.contains("ORDER BY revenue DESC, status ASC"),
            "expected semantic order by refs to use output aliases: {sql}"
        );
        assert!(
            !sql.contains("ORDER BY orders.revenue"),
            "semantic metric ref leaked into ORDER BY: {sql}"
        );
    }

    #[test]
    fn test_query_with_join() {
        let graph = create_test_graph();
        let generator = SqlGenerator::new(&graph);

        let query = SemanticQuery::new()
            .with_metrics(vec!["orders.revenue".into()])
            .with_dimensions(vec!["customers.country".into()]);

        let sql = generator.generate(&query).unwrap();

        assert!(sql.contains("LEFT JOIN customers_cte AS customers_cte"));
        assert!(sql.contains("orders_cte.customers_id = customers_cte.id"));
    }

    #[test]
    fn test_query_with_composite_join() {
        let mut graph = SemanticGraph::new();

        let shipments = Model::new("shipments", "shipment_id")
            .with_table("shipments")
            .with_metric(Metric::count("shipment_count"))
            .with_relationship(Relationship::many_to_one("order_items").with_key_columns(
                vec!["order_id".to_string(), "item_id".to_string()],
                vec!["order_id".to_string(), "item_id".to_string()],
            ));
        let order_items = Model::new("order_items", "order_id")
            .with_primary_key_columns(vec!["order_id".to_string(), "item_id".to_string()])
            .with_table("order_items")
            .with_dimension(Dimension::categorical("sku"));

        graph.add_model(shipments).unwrap();
        graph.add_model(order_items).unwrap();

        let generator = SqlGenerator::new(&graph);
        let query = SemanticQuery::new()
            .with_metrics(vec!["shipments.shipment_count".into()])
            .with_dimensions(vec!["order_items.sku".into()]);

        let sql = generator.generate(&query).unwrap();

        assert!(sql.contains("shipments_cte.order_id = order_items_cte.order_id"));
        assert!(sql.contains("shipments_cte.item_id = order_items_cte.item_id"));
        assert!(sql.contains(" AND "));
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

        assert!(sql.contains("WHERE status = 'completed'"));
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
            sql.contains("HASH(customers_cte.id)"),
            "Expected hash on primary key: {sql}"
        );
    }

    #[test]
    fn test_simple_metric_sql_column_does_not_pull_same_named_metric_model() {
        let mut graph = SemanticGraph::new();

        let invoices = Model::new("invoices", "id")
            .with_table("invoices")
            .with_dimension(Dimension::categorical("invoice_number"))
            .with_metric(Metric::sum("total_invoiced", "total_amount"))
            .with_relationship(Relationship::many_to_one("projects").with_keys("project_id", "id"))
            .with_relationship(
                Relationship::one_to_many("invoice_line_items").with_keys("invoice_id", "id"),
            );
        let invoice_line_items = Model::new("invoice_line_items", "id")
            .with_table("invoice_line_items")
            .with_metric(Metric::count("count"))
            .with_relationship(Relationship::many_to_one("invoices").with_keys("invoice_id", "id"));
        let projects = Model::new("projects", "id")
            .with_table("projects")
            .with_relationship(Relationship::one_to_many("expenses").with_keys("project_id", "id"));
        let expenses = Model::new("expenses", "id")
            .with_table("expenses")
            .with_metric(Metric::sum("total_amount", "amount"));

        graph.add_model(invoices).unwrap();
        graph.add_model(invoice_line_items).unwrap();
        graph.add_model(projects).unwrap();
        graph.add_model(expenses).unwrap();

        let generator = SqlGenerator::new(&graph);
        let query = SemanticQuery::new()
            .with_metrics(vec![
                "invoices.total_invoiced".into(),
                "invoice_line_items.count".into(),
            ])
            .with_dimensions(vec!["invoices.invoice_number".into()]);

        let sql = generator.generate(&query).unwrap();

        assert!(
            !sql.contains("expenses_cte"),
            "simple column SQL was misread as expenses.total_amount metric dependency: {sql}"
        );
    }

    #[test]
    fn test_fan_out_warning_with_composite_primary_key() {
        let mut graph = SemanticGraph::new();

        let order_items = Model::new("order_items", "order_id")
            .with_primary_key_columns(vec!["order_id".to_string(), "item_id".to_string()])
            .with_table("order_items")
            .with_dimension(Dimension::categorical("sku"))
            .with_metric(Metric::sum("item_revenue", "amount"));

        let shipments = Model::new("shipments", "shipment_id")
            .with_table("shipments")
            .with_dimension(Dimension::categorical("status"))
            .with_relationship(Relationship::many_to_one("order_items").with_key_columns(
                vec!["order_id".to_string(), "item_id".to_string()],
                vec!["order_id".to_string(), "item_id".to_string()],
            ));

        graph.add_model(order_items).unwrap();
        graph.add_model(shipments).unwrap();

        let generator = SqlGenerator::new(&graph);
        let query = SemanticQuery::new()
            .with_metrics(vec!["order_items.item_revenue".into()])
            .with_dimensions(vec!["shipments.status".into()]);

        let sql = generator.generate(&query).unwrap();
        assert!(sql.contains("HASH(COALESCE(CAST(order_items_cte.order_id AS VARCHAR), '')"));
        assert!(sql.contains("CAST(order_items_cte.item_id AS VARCHAR)"));
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
