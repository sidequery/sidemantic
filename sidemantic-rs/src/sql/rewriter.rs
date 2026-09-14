//! Query rewriter: rewrites SQL using semantic layer definitions

use std::collections::{HashMap, HashSet};

use polyglot_sql::parse as polyglot_parse;
use polyglot_sql::{
    expressions::{Identifier, Join, JoinKind, Select, TableRef, With},
    generate as polyglot_generate, DialectType, Expression,
};

use crate::core::SemanticGraph;
use crate::error::{Result, SidemanticError};
use crate::sql::SemanticQuery;

type QueryPreparer<'a> = &'a dyn Fn(&SemanticGraph, &mut SemanticQuery) -> Result<()>;

mod binding;
mod policy;
mod yardstick;

/// SQL query rewriter using semantic definitions
pub struct QueryRewriter<'a> {
    graph: &'a SemanticGraph,
    query_preparer: Option<QueryPreparer<'a>>,
    policy_definitions: &'a str,
    rename_only: bool,
    security_controls: bool,
    warnings: std::cell::RefCell<Vec<String>>,
}

impl<'a> QueryRewriter<'a> {
    pub fn new(graph: &'a SemanticGraph) -> Self {
        Self {
            graph,
            query_preparer: None,
            policy_definitions: "",
            rename_only: false,
            security_controls: false,
            warnings: std::cell::RefCell::new(Vec::new()),
        }
    }

    pub(crate) fn take_warnings(&self) -> Vec<String> {
        self.warnings.take()
    }

    /// Apply request policies to each semantic leaf before retaining its
    /// relational wrappers. Synthetic aggregate inputs remain visible to policy validation.
    pub(crate) fn with_query_preparer(
        mut self,
        prepare: QueryPreparer<'a>,
        policy_definitions: &'a str,
        security_controls: bool,
    ) -> Self {
        self.query_preparer = Some(prepare);
        self.policy_definitions = policy_definitions;
        self.security_controls = security_controls;
        self
    }

    /// Rewrite a SQL query using semantic layer definitions
    pub fn rewrite(&self, sql: &str) -> Result<String> {
        self.rewrite_with_dialect(sql, DialectType::Generic)
    }

    pub fn rewrite_with_dialect(&self, sql: &str, dialect: DialectType) -> Result<String> {
        self.rewrite_with_output_dialect(sql, dialect, dialect)
    }

    pub(crate) fn rewrite_with_output_dialect(
        &self,
        sql: &str,
        input_dialect: DialectType,
        output_dialect: DialectType,
    ) -> Result<String> {
        if let Some(rewritten) = self.rewrite_yardstick(sql, input_dialect, output_dialect)? {
            return Ok(rewritten);
        }
        // Yardstick must remove its extension syntax before the ordinary SQL
        // normalizer runs. Semantic binding and graph SQL share DuckDB syntax.
        let sql = crate::semantic_input::dialects::query_batch(sql, input_dialect)?;
        let statements = parse_sql_with_dialect(&sql, DialectType::DuckDB)?;

        if statements.is_empty() {
            return Err(SidemanticError::SqlParse("Empty SQL".into()));
        }
        if self.query_preparer.is_some() && statements.len() != 1 {
            return Err(policy::unsupported());
        }

        let mut rewritten_statements = Vec::new();
        for statement in statements {
            let rewritten = self.rewrite_statement(statement)?;
            rewritten_statements.push(crate::semantic_input::dialects::emit(
                rewritten,
                DialectType::DuckDB,
                output_dialect,
            )?);
        }

        Ok(rewritten_statements.join(";\n"))
    }

    fn rewrite_statement(&self, statement: Expression) -> Result<Expression> {
        self.rewrite_policy_statement(statement)
    }
}

pub(super) fn parse_sql_with_large_stack(sql: &str) -> Result<Vec<Expression>> {
    parse_sql_with_dialect(sql, DialectType::Generic)
}

fn parse_sql_with_dialect(sql: &str, dialect: DialectType) -> Result<Vec<Expression>> {
    #[cfg(target_arch = "wasm32")]
    {
        // Bound parser recursion before using the fixed WASM host stack.
        crate::wasm_sql_guard::check(sql, dialect)?;
        polyglot_parse(sql, dialect).map_err(|error| SidemanticError::SqlParse(error.to_string()))
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        let sql_owned = sql.to_string();
        let handle = std::thread::Builder::new()
            .stack_size(16 * 1024 * 1024)
            .spawn(move || polyglot_parse(&sql_owned, dialect).map_err(|e| e.to_string()))
            .map_err(|e| SidemanticError::SqlParse(e.to_string()))?;

        let parse_result = handle
            .join()
            .map_err(|_| SidemanticError::SqlParse("Polyglot parser thread panicked".into()))?;

        parse_result.map_err(SidemanticError::SqlParse)
    }
}

fn resolve_model_ref<'a>(
    table_or_alias: &str,
    model_refs: &'a [(String, String)],
) -> Option<(&'a str, &'a str)> {
    model_refs
        .iter()
        .find(|(model, alias)| model == table_or_alias || alias == table_or_alias)
        .map(|(model, alias)| (model.as_str(), alias.as_str()))
}

fn split_granularity(field: &str) -> (&str, Option<&str>) {
    const VALID_GRANULARITIES: [&str; 8] = [
        "year", "quarter", "month", "week", "day", "hour", "minute", "second",
    ];

    if let Some((base, gran)) = field.rsplit_once("__") {
        if VALID_GRANULARITIES.contains(&gran) {
            return (base, Some(gran));
        }
    }

    (field, None)
}

fn has_star_projection(projection: &[Expression]) -> bool {
    projection
        .iter()
        .any(|expr| matches!(expr, Expression::Star(_)))
}

fn table_name_and_alias(source: &Expression) -> Option<(String, Option<String>)> {
    match source {
        Expression::Table(table) => Some((
            table.name.name.clone(),
            table.alias.as_ref().map(|a| a.name.clone()),
        )),
        Expression::Alias(alias) => {
            if let Expression::Table(table) = &alias.this {
                Some((table.name.name.clone(), Some(alias.alias.name.clone())))
            } else {
                None
            }
        }
        Expression::Paren(paren) => table_name_and_alias(&paren.this),
        _ => None,
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
    fn scoped_graph_metric_select_preserves_alias_order_and_pagination() {
        let mut graph = create_test_graph();
        graph
            .add_metric_unvalidated(Metric::derived("total", "orders.revenue"))
            .unwrap();
        graph.set_metric_scopes(HashMap::new()).unwrap();
        let sql = QueryRewriter::new(&graph)
            .rewrite_with_dialect(
                "SELECT total AS amount FROM metrics ORDER BY amount DESC LIMIT 2 OFFSET 1",
                DialectType::DuckDB,
            )
            .unwrap();
        let parsed = parse_sql_with_dialect(&sql, DialectType::DuckDB).unwrap();
        let Expression::Select(select) = &parsed[0] else {
            panic!("expected select")
        };
        assert_eq!(select.expressions.len(), 1);
        assert!(
            matches!(&select.expressions[0], Expression::Alias(alias) if alias.alias.name == "amount")
        );
        assert!(select.limit.is_some());
        assert!(select.offset.is_some());
        assert!(sql.contains("ORDER BY amount DESC"));
    }

    #[test]
    fn scoped_metrics_rejects_unhandled_clauses_and_invalid_fields_separately() {
        let mut graph = create_test_graph();
        graph.set_metric_scopes(HashMap::new()).unwrap();
        let rewriter = QueryRewriter::new(&graph);
        assert!(matches!(
            rewriter.rewrite("SELECT orders.revenue FROM metrics QUALIFY 1 = 1"),
            Err(SidemanticError::UnsupportedSemanticFeatures { .. })
        ));
        assert!(matches!(
            rewriter.rewrite("SELECT orders.missing FROM metrics"),
            Err(SidemanticError::Validation(_))
        ));
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
    fn test_cross_model_composite_join() {
        let mut graph = SemanticGraph::new();

        let shipments = Model::new("shipments", "shipment_id")
            .with_table("public.shipments")
            .with_metric(Metric::count("shipment_count"))
            .with_relationship(Relationship::many_to_one("order_items").with_key_columns(
                vec!["order_id".to_string(), "item_id".to_string()],
                vec!["order_id".to_string(), "item_id".to_string()],
            ));
        let order_items = Model::new("order_items", "order_id")
            .with_primary_key_columns(vec!["order_id".to_string(), "item_id".to_string()])
            .with_table("public.order_items")
            .with_dimension(Dimension::categorical("sku"));

        graph.add_model(shipments).unwrap();
        graph.add_model(order_items).unwrap();

        let rewriter = QueryRewriter::new(&graph);
        let sql = "SELECT shipments.shipment_count, order_items.sku FROM shipments";
        let rewritten = rewriter.rewrite(sql).unwrap();

        assert!(rewritten.contains(".order_id = "));
        assert!(rewritten.contains(".item_id = "));
        assert!(rewritten.to_uppercase().contains(" AND "));
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
    fn test_source_uri_only_model_rejects_rewrite() {
        let mut graph = SemanticGraph::new();
        let mut events = Model::new("events", "event_id")
            .with_dimension(Dimension::categorical("event_type"))
            .with_metric(Metric::count("event_count"));
        events.source_uri = Some("s3://warehouse/events.parquet".to_string());
        graph.add_model(events).unwrap();

        let rewriter = QueryRewriter::new(&graph);
        let err = rewriter
            .rewrite("SELECT events.event_count FROM events")
            .unwrap_err();

        assert!(matches!(
            err,
            SidemanticError::ValidationIssue { ref code, .. }
                if code == "unsupported_source_uri_query"
        ));
        assert!(err.to_string().contains("source_uri"));
    }

    #[test]
    fn test_order_by_projected_semantic_refs_rewrite_to_output_aliases() {
        let graph = create_test_graph();
        let rewriter = QueryRewriter::new(&graph);

        let sql = "SELECT orders.revenue AS total, orders.status FROM orders ORDER BY orders.revenue DESC, orders.status ASC";
        let rewritten = rewriter.rewrite(sql).unwrap();

        assert!(
            rewritten.contains("ORDER BY total DESC, status ASC"),
            "expected ORDER BY aliases, got: {rewritten}"
        );
        assert!(
            !rewritten.contains("ORDER BY orders.revenue"),
            "semantic metric reference leaked into ORDER BY: {rewritten}"
        );
    }

    #[test]
    fn test_order_by_projected_alias_is_preserved() {
        let graph = create_test_graph();
        let rewriter = QueryRewriter::new(&graph);

        let sql = "SELECT orders.revenue AS total, orders.status FROM orders ORDER BY total DESC";
        let rewritten = rewriter.rewrite(sql).unwrap();

        assert!(
            rewritten.contains("ORDER BY total DESC"),
            "expected projected alias ORDER BY, got: {rewritten}"
        );
    }

    #[test]
    fn test_order_by_order_only_semantic_metric_rewrites_to_aggregate() {
        let graph = create_test_graph();
        let rewriter = QueryRewriter::new(&graph);

        let sql = "SELECT orders.status FROM orders ORDER BY orders.revenue DESC";
        let rewritten = rewriter.rewrite(sql).unwrap();

        assert!(
            rewritten.contains("ORDER BY __semantic_query.__sd_field_1 DESC"),
            "expected ordering over the compiled measure, got: {rewritten}"
        );
        assert!(
            !rewritten.contains("ORDER BY orders.revenue"),
            "semantic metric reference leaked into ORDER BY: {rewritten}"
        );
    }
}
