//! Query-only boundary for network execution surfaces. Local database APIs can
//! still perform explicit updates and semantic rewriting remains general purpose.

use polyglot_sql::{traversal::ExpressionWalk, Expression};

use super::rewriter::parse_sql_with_large_stack;

fn is_query(expression: &Expression) -> bool {
    match expression {
        Expression::Select(_)
        | Expression::Union(_)
        | Expression::Intersect(_)
        | Expression::Except(_)
        | Expression::Values(_) => true,
        Expression::Subquery(query) => is_query(&query.this),
        Expression::Annotated(annotation) => is_query(&annotation.this),
        Expression::Alias(alias) => is_query(&alias.this),
        _ => false,
    }
}

/// Require one query expression, including all nested statements and CTEs.
/// This limits statement authority; database grants still govern functions and
/// tables accessible to SELECT statements.
pub fn require_query_only_sql(sql: &str) -> Result<(), String> {
    let statements = parse_sql_with_large_stack(sql).map_err(|error| error.to_string())?;
    if statements.len() != 1 || !is_query(&statements[0]) {
        return Err("SQL execution only supports one query statement".into());
    }
    for node in statements[0].dfs() {
        if node.is_statement() && !is_query(node) {
            return Err("SQL execution does not support nested non-query statements".into());
        }
        if let Expression::Select(select) = node {
            if select.into.is_some() || !select.locks.is_empty() {
                return Err("SQL execution does not support SELECT INTO or locking clauses".into());
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::require_query_only_sql;

    #[test]
    fn query_only_rejects_mutations_and_admin_statements() {
        for sql in [
            "DELETE FROM orders",
            "DROP TABLE orders",
            "CREATE TABLE stolen AS SELECT 1",
            "INSERT INTO orders VALUES (1)",
            "UPDATE orders SET id = 1",
            "COPY orders TO '/tmp/stolen.csv'",
            "CALL procedure()",
            "SET threads = 4",
            "ATTACH '/tmp/other.db' AS other",
            "SELECT 1; DELETE FROM orders",
            "WITH gone AS (DELETE FROM orders RETURNING *) SELECT * FROM gone",
            "WITH ids AS (SELECT 1) DELETE FROM orders",
            "SELECT * INTO stolen FROM orders",
            "SELECT * FROM orders FOR UPDATE",
            "SELECT * FROM (SELECT * INTO stolen FROM orders) AS nested",
            "SELECT (SELECT id INTO stolen FROM orders) AS value",
            "SELECT (DELETE FROM orders RETURNING id) AS value",
            "SELECT * INTO stolen FROM orders /* trailing */;",
            "",
        ] {
            assert!(require_query_only_sql(sql).is_err(), "accepted {sql}");
        }
    }

    #[test]
    fn query_only_preserves_queries_and_quoted_text() {
        for sql in [
            "SELECT 1",
            "-- comment\nSELECT 'delete; drop table x' AS \"update\";",
            "WITH ids AS (SELECT 1 AS id) SELECT * FROM ids",
            "SELECT 1 UNION ALL SELECT 2",
            "SELECT 1 INTERSECT SELECT 1",
            "SELECT 1 EXCEPT SELECT 2",
            "SELECT * FROM (SELECT 1) AS nested",
            "SELECT (SELECT 1) AS value",
            "WITH ids AS (SELECT 1 AS id) SELECT (SELECT id FROM ids) AS value",
            "VALUES (1), (2)",
        ] {
            assert!(require_query_only_sql(sql).is_ok(), "rejected {sql}");
        }
    }
}
