//! Query rewriter: rewrites SQL using semantic layer definitions

use std::collections::HashSet;

use polyglot_sql::expressions::*;
use polyglot_sql::DialectType;

use crate::core::{MetricType, SemanticGraph};
use crate::error::{Result, SidemanticError};

const DIALECT: DialectType = DialectType::Generic;

/// SQL query rewriter using semantic definitions
pub struct QueryRewriter<'a> {
    graph: &'a SemanticGraph,
}

impl<'a> QueryRewriter<'a> {
    pub fn new(graph: &'a SemanticGraph) -> Self {
        Self { graph }
    }

    /// Rewrite a SQL query using semantic layer definitions
    pub fn rewrite(&self, sql: &str) -> Result<String> {
        let expressions = polyglot_sql::parse(sql, DIALECT)
            .map_err(|e| SidemanticError::SqlParse(e.to_string()))?;

        if expressions.is_empty() {
            return Err(SidemanticError::SqlParse("Empty SQL".into()));
        }

        let mut results = Vec::new();
        for expr in expressions {
            let rewritten = self.rewrite_top_level(expr)?;
            let sql = polyglot_sql::generate(&rewritten, DIALECT)
                .map_err(|e| SidemanticError::SqlParse(e.to_string()))?;
            results.push(sql);
        }

        Ok(results.join(";\n"))
    }

    fn rewrite_top_level(&self, expr: Expression) -> Result<Expression> {
        match expr {
            Expression::Select(select) => {
                let rewritten = self.rewrite_select(*select)?;
                Ok(Expression::Select(Box::new(rewritten)))
            }
            other => Ok(other),
        }
    }

    fn rewrite_select(&self, select: Select) -> Result<Select> {
        // Find semantic model references in FROM clause
        let mut model_refs = self.find_model_references(&select.from);

        if model_refs.is_empty() {
            // No semantic models, return as-is
            return Ok(select);
        }

        // Find all models referenced in projection AND WHERE clause
        let mut referenced_models = self.find_referenced_models(&select.expressions);
        if let Some(ref where_clause) = select.where_clause {
            self.collect_model_refs_from_expr(&where_clause.this, &mut referenced_models);
        }

        // Find models that need to be joined (referenced but not in FROM)
        let base_model = model_refs.first().map(|(m, _)| m.clone());
        let models_in_from: HashSet<_> = model_refs.iter().map(|(m, _)| m.clone()).collect();
        let models_to_join: Vec<_> = referenced_models
            .iter()
            .filter(|m| !models_in_from.contains(*m))
            .cloned()
            .collect();

        // Add aliases for joined models
        for model_name in &models_to_join {
            let alias = model_name.chars().next().unwrap_or('t').to_string();
            model_refs.push((model_name.clone(), alias));
        }

        // Rewrite SELECT items
        let expressions = self.rewrite_projection(&select.expressions, &model_refs)?;

        // Rewrite FROM clause with JOINs
        let (from, joins) = self.rewrite_from_with_joins(
            &select.from,
            &select.joins,
            &model_refs,
            base_model.as_deref(),
            &models_to_join,
        )?;

        // Rewrite WHERE clause
        let where_clause = select
            .where_clause
            .map(|w| -> Result<Where> {
                Ok(Where {
                    this: self.rewrite_expr(w.this, &model_refs)?,
                })
            })
            .transpose()?;

        // Add GROUP BY if we have aggregations and dimensions
        let has_aggregations = self.has_aggregations(&expressions);
        let has_dimensions = self.has_non_aggregated_columns(&expressions);

        let group_by = if has_aggregations && has_dimensions {
            Some(self.build_group_by(&expressions))
        } else {
            select.group_by
        };

        Ok(Select {
            expressions,
            from,
            joins,
            where_clause,
            group_by,
            ..select
        })
    }

    /// Find semantic model references in FROM clause
    fn find_model_references(&self, from: &Option<From>) -> Vec<(String, String)> {
        let mut refs = Vec::new();

        let Some(from) = from else {
            return refs;
        };

        for expr in &from.expressions {
            if let Expression::Table(table_ref) = expr {
                let table_name = &table_ref.name.name;

                if self.graph.get_model(table_name).is_some() {
                    let alias_name = table_ref
                        .alias
                        .as_ref()
                        .map(|a| a.name.clone())
                        .unwrap_or_else(|| table_name.clone());
                    refs.push((table_name.clone(), alias_name));
                }
            }
        }

        refs
    }

    /// Find all models referenced in the SELECT projection
    fn find_referenced_models(&self, projection: &[Expression]) -> HashSet<String> {
        let mut models = HashSet::new();

        for item in projection {
            match item {
                Expression::Alias(alias) => {
                    self.collect_model_refs_from_expr(&alias.this, &mut models);
                }
                _ => {
                    self.collect_model_refs_from_expr(item, &mut models);
                }
            }
        }

        models
    }

    /// Recursively collect model references from an expression
    fn collect_model_refs_from_expr(&self, expr: &Expression, models: &mut HashSet<String>) {
        use polyglot_sql::ExpressionWalk;

        for node in expr.dfs() {
            if let Expression::Column(col) = node {
                if let Some(table) = &col.table {
                    if self.graph.get_model(&table.name).is_some() {
                        models.insert(table.name.clone());
                    }
                }
            }
        }
    }

    /// Rewrite SELECT projection items
    fn rewrite_projection(
        &self,
        projection: &[Expression],
        model_refs: &[(String, String)],
    ) -> Result<Vec<Expression>> {
        let mut result = Vec::new();

        for item in projection {
            match item {
                Expression::Alias(alias) => {
                    let rewritten_inner =
                        self.rewrite_select_expr(alias.this.clone(), model_refs)?;
                    result.push(Expression::Alias(Box::new(Alias {
                        this: rewritten_inner,
                        alias: alias.alias.clone(),
                        column_aliases: alias.column_aliases.clone(),
                        pre_alias_comments: alias.pre_alias_comments.clone(),
                        trailing_comments: alias.trailing_comments.clone(),
                    })));
                }
                Expression::Star(_) => result.push(item.clone()),
                other => {
                    let rewritten = self.rewrite_select_expr(other.clone(), model_refs)?;
                    result.push(rewritten);
                }
            }
        }

        Ok(result)
    }

    /// Rewrite a SELECT expression (could be metric or dimension)
    fn rewrite_select_expr(
        &self,
        expr: Expression,
        model_refs: &[(String, String)],
    ) -> Result<Expression> {
        match &expr {
            Expression::Column(col) if col.table.is_some() => {
                let table_ident = col.table.as_ref().unwrap();
                let model_name = &table_ident.name;
                let field_name = &col.name.name;

                // Find the model
                if let Some((actual_model, alias)) = model_refs
                    .iter()
                    .find(|(m, a)| m == model_name || a == model_name)
                {
                    let model = self.graph.get_model(actual_model).unwrap();

                    // Check if it's a metric
                    if let Some(metric) = model.get_metric(field_name) {
                        return Ok(self.metric_to_expr(metric, alias));
                    }

                    // Check if it's a dimension
                    if let Some(dimension) = model.get_dimension(field_name) {
                        return Ok(Expression::qualified_column(
                            alias.as_str(),
                            dimension.sql_expr(),
                        ));
                    }
                }

                // Not a semantic reference, return as-is
                Ok(expr)
            }
            _ => self.rewrite_expr(expr, model_refs),
        }
    }

    /// Convert a metric to an expression
    fn metric_to_expr(&self, metric: &crate::core::Metric, alias: &str) -> Expression {
        match metric.r#type {
            MetricType::Simple => {
                // Handle Expression type: sql field contains the full expression
                if let Some(crate::core::Aggregation::Expression) = &metric.agg {
                    return self.parse_sql_fragment(metric.sql_expr());
                }

                let agg = metric.agg.as_ref().unwrap();
                // COUNT without explicit sql defaults to COUNT(*)
                let use_wildcard = metric.sql.as_deref() == Some("*")
                    || (*agg == crate::core::Aggregation::Count && metric.sql.is_none());

                if use_wildcard {
                    return Expression::Count(Box::new(CountFunc {
                        this: None,
                        star: true,
                        distinct: false,
                        filter: None,
                        ignore_nulls: None,
                        original_name: None,
                    }));
                }

                let col_expr = Expression::qualified_column(alias, metric.sql_expr().to_string());

                let make_agg = |this: Expression| AggFunc {
                    this,
                    distinct: false,
                    filter: None,
                    order_by: vec![],
                    name: None,
                    ignore_nulls: None,
                    having_max: None,
                    limit: None,
                };

                match agg {
                    crate::core::Aggregation::Sum => Expression::Sum(Box::new(make_agg(col_expr))),
                    crate::core::Aggregation::Count => Expression::Count(Box::new(CountFunc {
                        this: Some(col_expr),
                        star: false,
                        distinct: false,
                        filter: None,
                        ignore_nulls: None,
                        original_name: None,
                    })),
                    crate::core::Aggregation::CountDistinct => {
                        Expression::Count(Box::new(CountFunc {
                            this: Some(col_expr),
                            star: false,
                            distinct: true,
                            filter: None,
                            ignore_nulls: None,
                            original_name: None,
                        }))
                    }
                    crate::core::Aggregation::Avg => Expression::Avg(Box::new(make_agg(col_expr))),
                    crate::core::Aggregation::Min => Expression::Min(Box::new(make_agg(col_expr))),
                    crate::core::Aggregation::Max => Expression::Max(Box::new(make_agg(col_expr))),
                    crate::core::Aggregation::Median => {
                        Expression::Median(Box::new(make_agg(col_expr)))
                    }
                    crate::core::Aggregation::Expression => unreachable!(),
                }
            }
            MetricType::Derived | MetricType::Ratio => {
                // For derived/ratio metrics, parse the SQL expression
                self.parse_sql_fragment(metric.sql_expr())
            }
            MetricType::Cumulative | MetricType::TimeComparison => {
                // Complex metric types require special handling with window functions
                self.parse_sql_fragment(&metric.to_sql(Some(alias)))
            }
        }
    }

    /// Parse a SQL expression fragment by wrapping in SELECT
    fn parse_sql_fragment(&self, expr_sql: &str) -> Expression {
        let sql = format!("SELECT {expr_sql}");
        if let Ok(expressions) = polyglot_sql::parse(&sql, DIALECT) {
            if let Some(Expression::Select(select)) = expressions.into_iter().next() {
                if let Some(expr) = select.expressions.into_iter().next() {
                    // Unwrap Alias if present
                    if let Expression::Alias(alias) = expr {
                        return alias.this;
                    }
                    return expr;
                }
            }
        }
        // Fallback: return as identifier
        Expression::identifier(expr_sql)
    }

    /// Rewrite FROM clause with JOINs for cross-model references
    fn rewrite_from_with_joins(
        &self,
        from: &Option<From>,
        existing_joins: &[Join],
        model_refs: &[(String, String)],
        base_model: Option<&str>,
        models_to_join: &[String],
    ) -> Result<(Option<From>, Vec<Join>)> {
        let Some(from) = from else {
            return Ok((None, existing_joins.to_vec()));
        };

        let mut new_from_exprs = Vec::new();
        let mut new_joins = existing_joins.to_vec();

        for expr in &from.expressions {
            if let Expression::Table(table_ref) = expr {
                let table_name = &table_ref.name.name;

                if let Some(model) = self.graph.get_model(table_name) {
                    // Build JOINs for models referenced but not in FROM
                    if Some(table_name.as_str()) == base_model {
                        for target_model_name in models_to_join {
                            if let Ok(join_path) =
                                self.graph.find_join_path(table_name, target_model_name)
                            {
                                for step in &join_path.steps {
                                    let target_model =
                                        self.graph.get_model(&step.to_model).unwrap();

                                    // Find the alias for this model
                                    let to_alias = model_refs
                                        .iter()
                                        .find(|(m, _)| m == &step.to_model)
                                        .map(|(_, a)| a.clone())
                                        .unwrap_or_else(|| step.to_model.clone());

                                    let from_alias = model_refs
                                        .iter()
                                        .find(|(m, _)| m == &step.from_model)
                                        .map(|(_, a)| a.clone())
                                        .unwrap_or_else(|| step.from_model.clone());

                                    // Build JOIN condition
                                    let join_condition =
                                        if let Some(custom) = &step.custom_condition {
                                            let condition_sql = custom
                                                .replace("{from}", &from_alias)
                                                .replace("{to}", &to_alias);
                                            self.parse_where_fragment(&condition_sql)
                                                .unwrap_or_else(|| {
                                                    self.build_default_join_condition(
                                                        &from_alias,
                                                        &step.from_key,
                                                        &to_alias,
                                                        &step.to_key,
                                                    )
                                                })
                                        } else {
                                            self.build_default_join_condition(
                                                &from_alias,
                                                &step.from_key,
                                                &to_alias,
                                                &step.to_key,
                                            )
                                        };

                                    let join_table = make_table_ref_with_alias(
                                        target_model.table_name(),
                                        &to_alias,
                                    );

                                    new_joins.push(Join {
                                        this: Expression::Table(join_table),
                                        on: Some(join_condition),
                                        using: vec![],
                                        kind: JoinKind::Left,
                                        use_inner_keyword: false,
                                        use_outer_keyword: true,
                                        deferred_condition: false,
                                        join_hint: None,
                                        match_condition: None,
                                        pivots: vec![],
                                        comments: vec![],
                                        nesting_group: 0,
                                        directed: false,
                                    });
                                }
                            }
                        }
                    }

                    let mut new_table = make_table_ref(model.table_name());
                    new_table.alias = table_ref.alias.clone();
                    new_table.alias_explicit_as = table_ref.alias_explicit_as;
                    new_from_exprs.push(Expression::Table(new_table));
                } else {
                    new_from_exprs.push(expr.clone());
                }
            } else {
                new_from_exprs.push(expr.clone());
            }
        }

        Ok((
            Some(From {
                expressions: new_from_exprs,
            }),
            new_joins,
        ))
    }

    /// Parse a WHERE condition fragment
    fn parse_where_fragment(&self, condition_sql: &str) -> Option<Expression> {
        let sql = format!("SELECT 1 WHERE {condition_sql}");
        let exprs = polyglot_sql::parse(&sql, DIALECT).ok()?;
        if let Some(Expression::Select(s)) = exprs.into_iter().next() {
            s.where_clause.map(|w| w.this)
        } else {
            None
        }
    }

    /// Build default JOIN condition (from.fk = to.pk)
    fn build_default_join_condition(
        &self,
        from_alias: &str,
        from_key: &str,
        to_alias: &str,
        to_key: &str,
    ) -> Expression {
        Expression::Eq(Box::new(BinaryOp {
            left: Expression::qualified_column(from_alias, from_key),
            right: Expression::qualified_column(to_alias, to_key),
            left_comments: vec![],
            operator_comments: vec![],
            trailing_comments: vec![],
        }))
    }

    /// Rewrite general expressions (WHERE clause, etc.)
    fn rewrite_expr(
        &self,
        expr: Expression,
        model_refs: &[(String, String)],
    ) -> Result<Expression> {
        let model_refs_vec = model_refs.to_vec();
        let graph = self.graph;

        polyglot_sql::transform_map(expr, &|node| {
            if let Expression::Column(ref col) = node {
                if let Some(ref table_ident) = col.table {
                    let table_name = &table_ident.name;
                    let field_name = &col.name.name;

                    if let Some((actual_model, alias)) = model_refs_vec
                        .iter()
                        .find(|(m, a)| m.as_str() == table_name || a.as_str() == table_name)
                    {
                        if let Some(model) = graph.get_model(actual_model) {
                            if let Some(dimension) = model.get_dimension(field_name) {
                                return Ok(Expression::qualified_column(
                                    alias.as_str(),
                                    dimension.sql_expr(),
                                ));
                            }
                        }
                    }
                }
            }
            Ok(node)
        })
        .map_err(|e| SidemanticError::SqlParse(e.to_string()))
    }

    /// Check if projection has any aggregation functions
    fn has_aggregations(&self, projection: &[Expression]) -> bool {
        projection.iter().any(|item| {
            let expr = match item {
                Expression::Alias(a) => &a.this,
                other => other,
            };
            self.is_aggregation(expr)
        })
    }

    /// Check if expression is an aggregation function
    fn is_aggregation(&self, expr: &Expression) -> bool {
        match expr {
            Expression::Sum(_)
            | Expression::Count(_)
            | Expression::Avg(_)
            | Expression::Min(_)
            | Expression::Max(_)
            | Expression::Median(_)
            | Expression::AggregateFunction(_) => true,
            Expression::Function(f) => is_aggregate_function_name(&f.name),
            _ => false,
        }
    }

    /// Check if projection has non-aggregated columns
    fn has_non_aggregated_columns(&self, projection: &[Expression]) -> bool {
        projection.iter().any(|item| {
            let expr = match item {
                Expression::Alias(a) => &a.this,
                other => other,
            };
            !self.is_aggregation(expr)
        })
    }

    /// Build GROUP BY clause from non-aggregated columns
    fn build_group_by(&self, projection: &[Expression]) -> GroupBy {
        let mut group_by_exprs = Vec::new();

        for (i, item) in projection.iter().enumerate() {
            let expr = match item {
                Expression::Alias(a) => &a.this,
                other => other,
            };
            if !self.is_aggregation(expr) {
                // Use positional reference
                group_by_exprs.push(Expression::Literal(Literal::Number((i + 1).to_string())));
            }
        }

        GroupBy {
            expressions: group_by_exprs,
            all: None,
            totals: false,
            comments: vec![],
        }
    }
}

/// Check if a function name is a known SQL aggregate
fn is_aggregate_function_name(name: &str) -> bool {
    matches!(
        name.to_uppercase().as_str(),
        // Standard SQL aggregates
        "SUM" | "COUNT" | "AVG" | "MIN" | "MAX" | "MEDIAN"
        | "COUNT_DISTINCT"
        // Statistical
        | "STDDEV" | "STDDEV_POP" | "STDDEV_SAMP"
        | "VARIANCE" | "VAR_POP" | "VAR_SAMP"
        | "CORR" | "COVAR_POP" | "COVAR_SAMP"
        | "REGR_SLOPE" | "REGR_INTERCEPT" | "REGR_COUNT" | "REGR_R2"
        | "REGR_AVGX" | "REGR_AVGY" | "REGR_SXX" | "REGR_SYY" | "REGR_SXY"
        // Percentile / ordered-set
        | "PERCENTILE_CONT" | "PERCENTILE_DISC" | "MODE"
        // Boolean
        | "BOOL_AND" | "BOOL_OR" | "EVERY"
        // Bitwise
        | "BIT_AND" | "BIT_OR" | "BIT_XOR"
        // Collection / string
        | "ARRAY_AGG" | "STRING_AGG" | "GROUP_CONCAT" | "LISTAGG"
        | "COLLECT_LIST" | "COLLECT_SET"
        // Approximate
        | "APPROX_COUNT_DISTINCT" | "APPROX_PERCENTILE" | "HLL_COUNT_DISTINCT"
        | "APPROX_TOP_COUNT"
        // Misc
        | "ANY_VALUE" | "FIRST_VALUE" | "LAST_VALUE"
        | "NTH_VALUE" | "XMLAGG" | "JSON_ARRAYAGG" | "JSON_OBJECTAGG"
    )
}

/// Build a TableRef from a possibly-qualified table name
fn make_table_ref(full_name: &str) -> TableRef {
    let parts: Vec<&str> = full_name.split('.').collect();
    match parts.len() {
        2 => TableRef::new_with_schema(parts[1], parts[0]),
        3 => TableRef::new_with_catalog(parts[2], parts[1], parts[0]),
        _ => TableRef::new(full_name),
    }
}

/// Build a TableRef with an alias
fn make_table_ref_with_alias(full_name: &str, alias: &str) -> TableRef {
    let mut table = make_table_ref(full_name);
    table.alias = Some(Identifier::new(alias));
    table.alias_explicit_as = true;
    table
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{Dimension, Metric, Model, Relationship};

    fn create_test_graph() -> SemanticGraph {
        let mut graph = SemanticGraph::new();

        let orders = Model::new("orders", "order_id")
            .with_table("public.orders")
            .with_dimension(Dimension::categorical("status"))
            .with_dimension(Dimension::time("order_date").with_sql("created_at"))
            .with_metric(Metric::sum("revenue", "amount"))
            .with_metric(Metric::count("order_count"))
            .with_relationship(Relationship::many_to_one("customers"));

        let customers = Model::new("customers", "id")
            .with_table("public.customers")
            .with_dimension(Dimension::categorical("name"))
            .with_dimension(Dimension::categorical("country"));

        graph.add_model(orders).unwrap();
        graph.add_model(customers).unwrap();

        graph
    }

    #[test]
    fn test_simple_rewrite() {
        let graph = create_test_graph();
        let rewriter = QueryRewriter::new(&graph);

        let sql = "SELECT orders.revenue, orders.status FROM orders";
        let rewritten = rewriter.rewrite(sql).unwrap();

        assert!(rewritten.contains("public") || rewritten.contains("orders"));
        assert!(rewritten.to_uppercase().contains("SUM("));
        assert!(rewritten.to_uppercase().contains("GROUP BY"));
    }

    #[test]
    fn test_rewrite_with_alias() {
        let graph = create_test_graph();
        let rewriter = QueryRewriter::new(&graph);

        let sql = "SELECT o.revenue, o.status FROM orders AS o";
        let rewritten = rewriter.rewrite(sql).unwrap();

        assert!(rewritten.contains("public") || rewritten.contains("orders"));
    }

    #[test]
    fn test_rewrite_with_filter() {
        let graph = create_test_graph();
        let rewriter = QueryRewriter::new(&graph);

        let sql = "SELECT orders.revenue FROM orders WHERE orders.status = 'completed'";
        let rewritten = rewriter.rewrite(sql).unwrap();

        assert!(rewritten.to_uppercase().contains("WHERE"));
        assert!(rewritten.contains("status"));
    }

    #[test]
    fn test_cross_model_join() {
        let graph = create_test_graph();
        let rewriter = QueryRewriter::new(&graph);

        // Query orders metric with customers dimension - should auto-join
        let sql = "SELECT orders.revenue, customers.country FROM orders";
        let rewritten = rewriter.rewrite(sql).unwrap();

        // Should have JOIN clause
        assert!(
            rewritten.to_uppercase().contains("JOIN"),
            "Expected JOIN in: {rewritten}"
        );
        assert!(
            rewritten.contains("customers"),
            "Expected customers table in: {rewritten}"
        );
    }

    #[test]
    fn test_cross_model_join_in_where() {
        let graph = create_test_graph();
        let rewriter = QueryRewriter::new(&graph);

        // Model referenced only in WHERE should still trigger JOIN
        let sql = "SELECT orders.revenue FROM orders WHERE customers.country = 'US'";
        let rewritten = rewriter.rewrite(sql).unwrap();

        // Should have JOIN clause even though customers only in WHERE
        assert!(
            rewritten.to_uppercase().contains("JOIN"),
            "Expected JOIN in: {rewritten}"
        );
        assert!(
            rewritten.contains("customers"),
            "Expected customers table in: {rewritten}"
        );
    }

    #[test]
    fn test_count_without_sql() {
        // Test COUNT metric without explicit sql (simulates parsed definition)
        let mut graph = SemanticGraph::new();

        // Create metric with sql: None (like what SQL parser produces)
        let mut count_metric = Metric::new("order_count");
        count_metric.agg = Some(crate::core::Aggregation::Count);
        count_metric.sql = None; // Explicit None to simulate parsed metric

        let orders = Model::new("orders", "order_id")
            .with_table("orders")
            .with_dimension(Dimension::categorical("status"))
            .with_metric(count_metric);

        graph.add_model(orders).unwrap();
        let rewriter = QueryRewriter::new(&graph);

        let sql = "SELECT orders.order_count FROM orders";
        let rewritten = rewriter.rewrite(sql).unwrap();

        // Should be COUNT(*) not COUNT(order_count)
        assert!(
            rewritten.to_uppercase().contains("COUNT(*)"),
            "Expected COUNT(*) but got: {rewritten}"
        );
        assert!(
            !rewritten.contains("order_count"),
            "Should not contain order_count in COUNT: {rewritten}"
        );
    }
}
