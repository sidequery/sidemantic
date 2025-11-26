//! Query rewriter: rewrites SQL using semantic layer definitions

use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, GroupByExpr, Ident, ObjectName, Query, Select,
    SelectItem, SetExpr, Statement, TableFactor, TableWithJoins,
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
        let model_refs = self.find_model_references(&select.from);

        if model_refs.is_empty() {
            // No semantic models, return as-is
            return Ok(select);
        }

        // Rewrite SELECT items
        let projection = self.rewrite_projection(&select.projection, &model_refs)?;

        // Rewrite FROM clause
        let from = self.rewrite_from(&select.from, &model_refs)?;

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
    fn rewrite_select_expr(
        &self,
        expr: Expr,
        model_refs: &[(String, String)],
    ) -> Result<Expr> {
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
                let agg = metric.agg.as_ref().unwrap();
                let sql_expr = metric.sql_expr();

                let arg = if sql_expr == "*" {
                    FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
                } else {
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
        }
    }

    /// Rewrite FROM clause to use actual table names
    fn rewrite_from(
        &self,
        from: &[TableWithJoins],
        _model_refs: &[(String, String)],
    ) -> Result<Vec<TableWithJoins>> {
        let mut result = Vec::new();

        for table in from {
            if let TableFactor::Table { name, alias, .. } = &table.relation {
                let table_name = name.0.first().map(|i| i.value.clone()).unwrap_or_default();

                if let Some(model) = self.graph.get_model(&table_name) {
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
                        joins: table.joins.clone(),
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
                        group_by_exprs.push(Expr::Value(
                            sqlparser::ast::Value::Number((i + 1).to_string(), false).into(),
                        ));
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
}
