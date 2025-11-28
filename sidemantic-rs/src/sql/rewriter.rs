//! Query rewriter: rewrites SQL using semantic layer definitions

use std::collections::HashSet;

use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, GroupByExpr, Ident, Join, JoinConstraint, JoinOperator,
    ObjectName, Query, Select, SelectItem, SetExpr, Statement, TableAlias, TableFactor,
    TableWithJoins,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::core::{MetricType, SemanticGraph};
use crate::error::{Result, SidemanticError};

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
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql)
            .map_err(|e| SidemanticError::SqlParse(e.to_string()))?;

        if statements.is_empty() {
            return Err(SidemanticError::SqlParse("Empty SQL".into()));
        }

        let mut rewritten_statements = Vec::new();

        for statement in statements {
            let rewritten = self.rewrite_statement(statement)?;
            rewritten_statements.push(rewritten.to_string());
        }

        Ok(rewritten_statements.join(";\n"))
    }

    fn rewrite_statement(&self, statement: Statement) -> Result<Statement> {
        match statement {
            Statement::Query(query) => {
                let rewritten_query = self.rewrite_query(*query)?;
                Ok(Statement::Query(Box::new(rewritten_query)))
            }
            _ => Ok(statement),
        }
    }

    fn rewrite_query(&self, query: Query) -> Result<Query> {
        let body = match *query.body {
            SetExpr::Select(select) => {
                let rewritten_select = self.rewrite_select(*select)?;
                SetExpr::Select(Box::new(rewritten_select))
            }
            other => other,
        };

        Ok(Query {
            body: Box::new(body),
            ..query
        })
    }

    fn rewrite_select(&self, select: Select) -> Result<Select> {
        // Find semantic model references in FROM clause
        let mut model_refs = self.find_model_references(&select.from);

        if model_refs.is_empty() {
            // No semantic models, return as-is
            return Ok(select);
        }

        // Find all models referenced in projection AND WHERE clause
        let mut referenced_models = self.find_referenced_models(&select.projection);
        if let Some(ref selection) = select.selection {
            self.collect_model_refs_from_expr(selection, &mut referenced_models);
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
        let projection = self.rewrite_projection(&select.projection, &model_refs)?;

        // Rewrite FROM clause with JOINs
        let from = self.rewrite_from_with_joins(
            &select.from,
            &model_refs,
            base_model.as_deref(),
            &models_to_join,
        )?;

        // Rewrite WHERE clause
        let selection = select
            .selection
            .map(|expr| self.rewrite_expr(expr, &model_refs))
            .transpose()?;

        // Add GROUP BY if we have aggregations and dimensions
        let has_aggregations = self.has_aggregations(&projection);
        let has_dimensions = self.has_non_aggregated_columns(&projection);

        let group_by = if has_aggregations && has_dimensions {
            self.build_group_by(&projection)
        } else {
            select.group_by
        };

        Ok(Select {
            projection,
            from,
            selection,
            group_by,
            ..select
        })
    }

    /// Find semantic model references in FROM clause
    fn find_model_references(&self, from: &[TableWithJoins]) -> Vec<(String, String)> {
        let mut refs = Vec::new();

        for table in from {
            if let TableFactor::Table { name, alias, .. } = &table.relation {
                let table_name = name.0.first().map(|i| i.value.clone()).unwrap_or_default();

                if self.graph.get_model(&table_name).is_some() {
                    let alias_name = alias
                        .as_ref()
                        .map(|a| a.name.value.clone())
                        .unwrap_or_else(|| table_name.clone());
                    refs.push((table_name, alias_name));
                }
            }
        }

        refs
    }

    /// Find all models referenced in the SELECT projection
    fn find_referenced_models(&self, projection: &[SelectItem]) -> HashSet<String> {
        let mut models = HashSet::new();

        for item in projection {
            match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    self.collect_model_refs_from_expr(expr, &mut models);
                }
                _ => {}
            }
        }

        models
    }

    /// Recursively collect model references from an expression
    fn collect_model_refs_from_expr(&self, expr: &Expr, models: &mut HashSet<String>) {
        match expr {
            Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                let model_name = &parts[0].value;
                if self.graph.get_model(model_name).is_some() {
                    models.insert(model_name.clone());
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                self.collect_model_refs_from_expr(left, models);
                self.collect_model_refs_from_expr(right, models);
            }
            Expr::UnaryOp { expr, .. } => {
                self.collect_model_refs_from_expr(expr, models);
            }
            Expr::Nested(inner) => {
                self.collect_model_refs_from_expr(inner, models);
            }
            Expr::Function(f) => {
                if let sqlparser::ast::FunctionArguments::List(args) = &f.args {
                    for arg in &args.args {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                            self.collect_model_refs_from_expr(e, models);
                        }
                    }
                }
            }
            _ => {}
        }
    }

    /// Rewrite SELECT projection items
    fn rewrite_projection(
        &self,
        projection: &[SelectItem],
        model_refs: &[(String, String)],
    ) -> Result<Vec<SelectItem>> {
        let mut result = Vec::new();

        for item in projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let rewritten = self.rewrite_select_expr(expr.clone(), model_refs)?;
                    result.push(SelectItem::UnnamedExpr(rewritten));
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let rewritten = self.rewrite_select_expr(expr.clone(), model_refs)?;
                    result.push(SelectItem::ExprWithAlias {
                        expr: rewritten,
                        alias: alias.clone(),
                    });
                }
                other => result.push(other.clone()),
            }
        }

        Ok(result)
    }

    /// Rewrite a SELECT expression (could be metric or dimension)
    fn rewrite_select_expr(&self, expr: Expr, model_refs: &[(String, String)]) -> Result<Expr> {
        match &expr {
            Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                let model_name = &parts[0].value;
                let field_name = &parts[1].value;

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
                        return Ok(Expr::CompoundIdentifier(vec![
                            Ident::new(alias.clone()),
                            Ident::new(dimension.sql_expr().to_string()),
                        ]));
                    }
                }

                // Not a semantic reference, return as-is
                Ok(expr)
            }
            _ => self.rewrite_expr(expr, model_refs),
        }
    }

    /// Convert a metric to an expression
    fn metric_to_expr(&self, metric: &crate::core::Metric, alias: &str) -> Expr {
        match metric.r#type {
            MetricType::Simple => {
                // Handle Expression type: sql field contains the full expression
                if let Some(crate::core::Aggregation::Expression) = &metric.agg {
                    let dialect = GenericDialect {};
                    let sql = format!("SELECT {}", metric.sql_expr());
                    if let Ok(statements) = Parser::parse_sql(&dialect, &sql) {
                        if let Some(Statement::Query(query)) = statements.into_iter().next() {
                            if let SetExpr::Select(select) = *query.body {
                                if let Some(SelectItem::UnnamedExpr(expr)) =
                                    select.projection.into_iter().next()
                                {
                                    return expr;
                                }
                            }
                        }
                    }
                    // Fallback: return as identifier
                    return Expr::Identifier(Ident::new(metric.name.clone()));
                }

                let agg = metric.agg.as_ref().unwrap();
                // COUNT without explicit sql defaults to COUNT(*)
                let use_wildcard = metric.sql.as_deref() == Some("*")
                    || (*agg == crate::core::Aggregation::Count && metric.sql.is_none());

                let arg = if use_wildcard {
                    FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
                } else {
                    let sql_expr = metric.sql_expr();
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::CompoundIdentifier(vec![
                        Ident::new(alias.to_string()),
                        Ident::new(sql_expr.to_string()),
                    ])))
                };

                let func_name = match agg {
                    crate::core::Aggregation::CountDistinct => "COUNT",
                    _ => agg.as_sql(),
                };

                Expr::Function(sqlparser::ast::Function {
                    name: ObjectName(vec![Ident::new(func_name.to_string())]),
                    args: sqlparser::ast::FunctionArguments::List(
                        sqlparser::ast::FunctionArgumentList {
                            args: vec![arg],
                            duplicate_treatment: if matches!(
                                agg,
                                crate::core::Aggregation::CountDistinct
                            ) {
                                Some(sqlparser::ast::DuplicateTreatment::Distinct)
                            } else {
                                None
                            },
                            clauses: vec![],
                        },
                    ),
                    over: None,
                    filter: None,
                    null_treatment: None,
                    within_group: vec![],
                    parameters: sqlparser::ast::FunctionArguments::None,
                })
            }
            MetricType::Derived | MetricType::Ratio => {
                // For derived/ratio metrics, parse the SQL expression
                // This is simplified; a full implementation would parse and rewrite
                let dialect = GenericDialect {};
                let sql = format!("SELECT {}", metric.sql_expr());
                if let Ok(statements) = Parser::parse_sql(&dialect, &sql) {
                    if let Some(Statement::Query(query)) = statements.into_iter().next() {
                        if let SetExpr::Select(select) = *query.body {
                            if let Some(SelectItem::UnnamedExpr(expr)) =
                                select.projection.into_iter().next()
                            {
                                return expr;
                            }
                        }
                    }
                }
                // Fallback: return as identifier
                Expr::Identifier(Ident::new(metric.name.clone()))
            }
            MetricType::Cumulative | MetricType::TimeComparison => {
                // Complex metric types require special handling with window functions
                // For now, fall back to the metric's to_sql() output parsed as an expression
                let dialect = GenericDialect {};
                let sql = format!("SELECT {}", metric.to_sql(Some(alias)));
                if let Ok(statements) = Parser::parse_sql(&dialect, &sql) {
                    if let Some(Statement::Query(query)) = statements.into_iter().next() {
                        if let SetExpr::Select(select) = *query.body {
                            if let Some(SelectItem::UnnamedExpr(expr)) =
                                select.projection.into_iter().next()
                            {
                                return expr;
                            }
                        }
                    }
                }
                // Fallback: return as identifier
                Expr::Identifier(Ident::new(metric.name.clone()))
            }
        }
    }

    /// Rewrite FROM clause with JOINs for cross-model references
    fn rewrite_from_with_joins(
        &self,
        from: &[TableWithJoins],
        model_refs: &[(String, String)],
        base_model: Option<&str>,
        models_to_join: &[String],
    ) -> Result<Vec<TableWithJoins>> {
        let mut result = Vec::new();

        for table in from {
            if let TableFactor::Table { name, alias, .. } = &table.relation {
                let table_name = name.0.first().map(|i| i.value.clone()).unwrap_or_default();

                if let Some(model) = self.graph.get_model(&table_name) {
                    // Build JOINs for models referenced but not in FROM
                    let mut joins = table.joins.clone();

                    if Some(table_name.as_str()) == base_model {
                        for target_model_name in models_to_join {
                            if let Ok(join_path) =
                                self.graph.find_join_path(&table_name, target_model_name)
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
                                    let join_condition = if let Some(custom) =
                                        &step.custom_condition
                                    {
                                        // Parse custom condition
                                        let condition_sql = custom
                                            .replace("{from}", &from_alias)
                                            .replace("{to}", &to_alias);
                                        let dialect = GenericDialect {};
                                        let expr_sql = format!("SELECT 1 WHERE {condition_sql}");
                                        if let Ok(stmts) = Parser::parse_sql(&dialect, &expr_sql) {
                                            if let Some(Statement::Query(q)) =
                                                stmts.into_iter().next()
                                            {
                                                if let SetExpr::Select(s) = *q.body {
                                                    s.selection.unwrap_or_else(|| {
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
                                                }
                                            } else {
                                                self.build_default_join_condition(
                                                    &from_alias,
                                                    &step.from_key,
                                                    &to_alias,
                                                    &step.to_key,
                                                )
                                            }
                                        } else {
                                            self.build_default_join_condition(
                                                &from_alias,
                                                &step.from_key,
                                                &to_alias,
                                                &step.to_key,
                                            )
                                        }
                                    } else {
                                        self.build_default_join_condition(
                                            &from_alias,
                                            &step.from_key,
                                            &to_alias,
                                            &step.to_key,
                                        )
                                    };

                                    joins.push(Join {
                                        relation: TableFactor::Table {
                                            name: ObjectName(vec![Ident::new(
                                                target_model.table_name().to_string(),
                                            )]),
                                            alias: Some(TableAlias {
                                                name: Ident::new(to_alias),
                                                columns: vec![],
                                            }),
                                            args: None,
                                            with_hints: vec![],
                                            version: None,
                                            partitions: vec![],
                                            with_ordinality: false,
                                        },
                                        join_operator: JoinOperator::LeftOuter(JoinConstraint::On(
                                            join_condition,
                                        )),
                                        global: false,
                                    });
                                }
                            }
                        }
                    }

                    let new_table = TableWithJoins {
                        relation: TableFactor::Table {
                            name: ObjectName(vec![Ident::new(model.table_name().to_string())]),
                            alias: alias.clone(),
                            args: None,
                            with_hints: vec![],
                            version: None,
                            partitions: vec![],
                            with_ordinality: false,
                        },
                        joins,
                    };
                    result.push(new_table);
                } else {
                    result.push(table.clone());
                }
            } else {
                result.push(table.clone());
            }
        }

        Ok(result)
    }

    /// Build default JOIN condition (from.fk = to.pk)
    fn build_default_join_condition(
        &self,
        from_alias: &str,
        from_key: &str,
        to_alias: &str,
        to_key: &str,
    ) -> Expr {
        Expr::BinaryOp {
            left: Box::new(Expr::CompoundIdentifier(vec![
                Ident::new(from_alias.to_string()),
                Ident::new(from_key.to_string()),
            ])),
            op: sqlparser::ast::BinaryOperator::Eq,
            right: Box::new(Expr::CompoundIdentifier(vec![
                Ident::new(to_alias.to_string()),
                Ident::new(to_key.to_string()),
            ])),
        }
    }

    /// Rewrite general expressions
    fn rewrite_expr(&self, expr: Expr, model_refs: &[(String, String)]) -> Result<Expr> {
        match expr {
            Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                let model_name = &parts[0].value;
                let field_name = &parts[1].value;

                // Find the model and rewrite field reference
                if let Some((actual_model, alias)) = model_refs
                    .iter()
                    .find(|(m, a)| m == model_name || a == model_name)
                {
                    let model = self.graph.get_model(actual_model).unwrap();

                    if let Some(dimension) = model.get_dimension(field_name) {
                        return Ok(Expr::CompoundIdentifier(vec![
                            Ident::new(alias.clone()),
                            Ident::new(dimension.sql_expr().to_string()),
                        ]));
                    }
                }

                Ok(Expr::CompoundIdentifier(parts))
            }
            Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
                left: Box::new(self.rewrite_expr(*left, model_refs)?),
                op,
                right: Box::new(self.rewrite_expr(*right, model_refs)?),
            }),
            Expr::UnaryOp { op, expr } => Ok(Expr::UnaryOp {
                op,
                expr: Box::new(self.rewrite_expr(*expr, model_refs)?),
            }),
            Expr::Nested(inner) => Ok(Expr::Nested(Box::new(
                self.rewrite_expr(*inner, model_refs)?,
            ))),
            _ => Ok(expr),
        }
    }

    /// Check if projection has any aggregation functions
    fn has_aggregations(&self, projection: &[SelectItem]) -> bool {
        for item in projection {
            match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    if self.is_aggregation(expr) {
                        return true;
                    }
                }
                _ => {}
            }
        }
        false
    }

    /// Check if expression is an aggregation function
    fn is_aggregation(&self, expr: &Expr) -> bool {
        match expr {
            Expr::Function(f) => {
                let name = f.name.0.first().map(|i| i.value.to_uppercase());
                matches!(
                    name.as_deref(),
                    Some("SUM" | "COUNT" | "AVG" | "MIN" | "MAX" | "MEDIAN")
                )
            }
            _ => false,
        }
    }

    /// Check if projection has non-aggregated columns
    fn has_non_aggregated_columns(&self, projection: &[SelectItem]) -> bool {
        for item in projection {
            match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    if !self.is_aggregation(expr) {
                        return true;
                    }
                }
                _ => {}
            }
        }
        false
    }

    /// Build GROUP BY clause from non-aggregated columns
    fn build_group_by(&self, projection: &[SelectItem]) -> GroupByExpr {
        let mut group_by_exprs = Vec::new();

        for (i, item) in projection.iter().enumerate() {
            match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    if !self.is_aggregation(expr) {
                        // Use positional reference
                        group_by_exprs.push(Expr::Value(sqlparser::ast::Value::Number(
                            (i + 1).to_string(),
                            false,
                        )));
                    }
                }
                _ => {}
            }
        }

        GroupByExpr::Expressions(group_by_exprs, vec![])
    }
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

        assert!(rewritten.contains("public.orders"));
        assert!(rewritten.contains("SUM("));
        assert!(rewritten.contains("GROUP BY"));
    }

    #[test]
    fn test_rewrite_with_alias() {
        let graph = create_test_graph();
        let rewriter = QueryRewriter::new(&graph);

        let sql = "SELECT o.revenue, o.status FROM orders AS o";
        let rewritten = rewriter.rewrite(sql).unwrap();

        assert!(rewritten.contains("public.orders"));
    }

    #[test]
    fn test_rewrite_with_filter() {
        let graph = create_test_graph();
        let rewriter = QueryRewriter::new(&graph);

        let sql = "SELECT orders.revenue FROM orders WHERE orders.status = 'completed'";
        let rewritten = rewriter.rewrite(sql).unwrap();

        assert!(rewritten.contains("WHERE"));
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
            rewritten.contains("COUNT(*)"),
            "Expected COUNT(*) but got: {rewritten}"
        );
        assert!(
            !rewritten.contains("order_count"),
            "Should not contain order_count in COUNT: {rewritten}"
        );
    }
}
