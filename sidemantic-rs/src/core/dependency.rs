//! Dependency analysis for derived metrics
//!
//! Extracts metric dependencies from SQL expressions using polyglot-sql.

use std::collections::HashSet;

use polyglot_sql::{parse, traversal, DialectType, Expression};

use super::model::{Metric, MetricType};
use super::SemanticGraph;

/// Extract all metric/measure dependencies from a metric definition
///
/// Returns a set of metric names that this metric depends on.
/// For qualified references (model.metric), returns the full reference.
/// For unqualified references, attempts to resolve using the graph.
pub fn extract_dependencies(metric: &Metric, graph: Option<&SemanticGraph>) -> HashSet<String> {
    extract_dependencies_with_context(metric, graph, None)
}

/// Extract dependencies with optional model context for unqualified reference resolution.
pub fn extract_dependencies_with_context(
    metric: &Metric,
    graph: Option<&SemanticGraph>,
    model_context: Option<&str>,
) -> HashSet<String> {
    let mut deps = HashSet::new();

    match metric.r#type {
        MetricType::Ratio => {
            // Ratio metrics depend on numerator and denominator
            if let Some(ref num) = metric.numerator {
                deps.insert(num.clone());
            }
            if let Some(ref denom) = metric.denominator {
                deps.insert(denom.clone());
            }
        }
        MetricType::Derived => {
            // Derived metrics: parse SQL to find references
            if let Some(ref sql) = metric.sql {
                // Check if it's a simple qualified reference (model.metric)
                if is_simple_reference(sql) {
                    deps.insert(sql.clone());
                } else {
                    // Parse SQL and extract column references
                    let refs = extract_column_references(sql);

                    // Resolve references using graph if available
                    if let Some(g) = graph {
                        for ref_name in refs {
                            let resolved = resolve_reference(&ref_name, g, model_context);
                            deps.insert(resolved);
                        }
                    } else {
                        deps.extend(refs);
                    }
                }
            }
        }
        MetricType::Simple => {
            // Simple aggregations don't have metric dependencies
        }
        MetricType::Cumulative => {
            // Cumulative metrics depend on the base metric in sql field
            if let Some(ref sql) = metric.sql {
                deps.insert(sql.clone());
            } else if let Some(ref base_metric) = metric.base_metric {
                deps.insert(base_metric.clone());
            }
        }
        MetricType::TimeComparison => {
            // Time comparison metrics depend on the base_metric
            if let Some(ref base) = metric.base_metric {
                deps.insert(base.clone());
            }
        }
        MetricType::Conversion => {
            // Conversion metrics are modeled via event filters, not metric dependencies.
        }
    }

    deps
}

/// Check if SQL is a simple qualified reference (model.metric with no operators)
fn is_simple_reference(sql: &str) -> bool {
    let trimmed = sql.trim();
    trimmed.contains('.') && !trimmed.contains(' ') && !has_operators(trimmed)
}

/// Check if string contains SQL operators
fn has_operators(s: &str) -> bool {
    ['+', '-', '*', '/', '(', ')', ',', '>', '<', '=']
        .iter()
        .any(|&op| s.contains(op))
}

/// Extract column references from a SQL expression
///
/// Uses polyglot-sql to parse the expression and find all column identifiers.
fn extract_column_references(sql: &str) -> HashSet<String> {
    let mut refs = HashSet::new();
    let normalized_sql = sql.replace("${CUBE}.", "").replace("${CUBE}", "");

    // polyglot-sql traversal can recurse indefinitely on some PostgreSQL cast
    // forms (expr::type). Fall back to the tokenizer path for these expressions.
    if normalized_sql.contains("::") {
        return extract_simple_references(&normalized_sql);
    }

    // Wrap in SELECT to make it valid SQL
    let wrapped = format!("SELECT {normalized_sql}");

    let Ok(statements) = parse(&wrapped, DialectType::Generic) else {
        // If parsing fails, try simple extraction
        return extract_simple_references(&normalized_sql);
    };

    for statement in &statements {
        if let Expression::Select(select) = statement {
            for projection in &select.expressions {
                for column_ref in traversal::get_columns(projection) {
                    if let Expression::Column(column) = column_ref {
                        let candidate = if let Some(table) = &column.table {
                            if table.name.is_empty() {
                                column.name.name.clone()
                            } else {
                                format!("{}.{}", table.name, column.name.name)
                            }
                        } else {
                            column.name.name.clone()
                        };
                        if let Some(cleaned) = sanitize_reference(&candidate) {
                            refs.insert(cleaned);
                        }
                    }
                }
            }
        }
    }

    if refs.is_empty() {
        return extract_simple_references(&normalized_sql);
    }

    refs
}

/// Public wrapper used by language bindings for dependency analysis helpers.
pub fn extract_column_references_from_expr(sql: &str) -> HashSet<String> {
    extract_column_references(sql)
}

/// Simple fallback extraction for when parsing fails
fn extract_simple_references(sql: &str) -> HashSet<String> {
    let mut refs = HashSet::new();

    // Simple regex-like extraction: find word characters with dots
    let mut current = String::new();
    let mut in_string = false;
    let mut prev_char = ' ';

    for c in sql.chars() {
        if c == '\'' && prev_char != '\\' {
            in_string = !in_string;
        }

        if !in_string {
            if c.is_alphanumeric() || c == '_' || c == '.' {
                current.push(c);
            } else {
                let is_function_call = c == '(';
                if !is_function_call {
                    if let Some(cleaned) = sanitize_reference(&current) {
                        refs.insert(cleaned);
                    }
                }
                current.clear();
            }
        }

        prev_char = c;
    }

    if let Some(cleaned) = sanitize_reference(&current) {
        refs.insert(cleaned);
    }

    refs
}

fn sanitize_reference(raw: &str) -> Option<String> {
    let mut candidate = raw.trim();
    while let Some(stripped) = candidate.strip_prefix('.') {
        candidate = stripped;
    }
    if candidate.is_empty() {
        return None;
    }
    if is_keyword(candidate) || is_number(candidate) || is_cast_type(candidate) {
        return None;
    }
    if candidate.eq_ignore_ascii_case("cube") {
        return None;
    }
    Some(candidate.to_string())
}

/// Check if string is a SQL keyword
fn is_keyword(s: &str) -> bool {
    let keywords = [
        "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "NULL", "CASE", "WHEN", "THEN", "ELSE",
        "END", "AS", "DISTINCT",
    ];
    keywords.iter().any(|k| k.eq_ignore_ascii_case(s))
}

/// Check if string is a number
fn is_number(s: &str) -> bool {
    s.parse::<f64>().is_ok()
}

fn is_cast_type(s: &str) -> bool {
    let cast_types = [
        "float",
        "double",
        "decimal",
        "numeric",
        "integer",
        "int",
        "bigint",
        "smallint",
        "real",
        "boolean",
        "bool",
        "date",
        "time",
        "timestamp",
        "varchar",
        "text",
    ];
    cast_types.iter().any(|ty| ty.eq_ignore_ascii_case(s))
}

/// Resolve a reference using the semantic graph
///
/// If the reference is already qualified (model.metric), returns as-is.
/// Otherwise, searches all models for a matching metric.
fn resolve_reference(ref_name: &str, graph: &SemanticGraph, model_context: Option<&str>) -> String {
    // Already qualified
    if ref_name.contains('.') {
        return ref_name.to_string();
    }

    if let Some(context_model_name) = model_context {
        if let Some(model) = graph.get_model(context_model_name) {
            if model.get_metric(ref_name).is_some() {
                return format!("{context_model_name}.{ref_name}");
            }
        }
    }

    // Search models for matching metric
    for model in graph.models() {
        if model.get_metric(ref_name).is_some() {
            return format!("{}.{}", model.name, ref_name);
        }
    }

    // Not found, return as-is
    ref_name.to_string()
}

/// Build a dependency graph for all metrics and check for cycles
pub fn check_circular_dependencies(
    metrics: &[(&str, &Metric)],
    graph: &SemanticGraph,
) -> Result<(), String> {
    use std::collections::HashMap;

    // Build adjacency list with owned strings
    let mut adj: HashMap<String, HashSet<String>> = HashMap::new();

    for (name, metric) in metrics {
        let deps = extract_dependencies(metric, Some(graph));
        adj.insert(name.to_string(), deps);
    }

    // DFS to detect cycles
    let mut visited: HashSet<String> = HashSet::new();
    let mut rec_stack: HashSet<String> = HashSet::new();

    fn has_cycle(
        node: &str,
        adj: &HashMap<String, HashSet<String>>,
        visited: &mut HashSet<String>,
        rec_stack: &mut HashSet<String>,
    ) -> bool {
        visited.insert(node.to_string());
        rec_stack.insert(node.to_string());

        if let Some(neighbors) = adj.get(node) {
            for neighbor in neighbors {
                if !visited.contains(neighbor) {
                    if has_cycle(neighbor, adj, visited, rec_stack) {
                        return true;
                    }
                } else if rec_stack.contains(neighbor) {
                    return true;
                }
            }
        }

        rec_stack.remove(node);
        false
    }

    for (name, _) in metrics {
        if !visited.contains(*name) && has_cycle(name, &adj, &mut visited, &mut rec_stack) {
            return Err(format!(
                "Circular dependency detected involving metric '{name}'"
            ));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[allow(unused_imports)]
    use crate::core::model::Aggregation;

    #[test]
    fn test_ratio_dependencies() {
        let metric = Metric::ratio("profit_margin", "profit", "revenue");

        let deps = extract_dependencies(&metric, None);
        assert!(deps.contains("profit"));
        assert!(deps.contains("revenue"));
    }

    #[test]
    fn test_derived_simple_reference() {
        let metric = Metric::derived("total_revenue", "orders.revenue");

        let deps = extract_dependencies(&metric, None);
        assert!(deps.contains("orders.revenue"));
    }

    #[test]
    fn test_derived_expression() {
        let metric = Metric::derived("avg_order_value", "revenue / order_count");

        let deps = extract_dependencies(&metric, None);
        assert!(deps.contains("revenue"));
        assert!(deps.contains("order_count"));
    }

    #[test]
    fn test_simple_aggregation_no_deps() {
        let metric = Metric::sum("revenue", "amount");

        let deps = extract_dependencies(&metric, None);
        assert!(deps.is_empty());
    }

    #[test]
    fn test_extract_column_references() {
        let refs = extract_column_references("(revenue - cost) / revenue");
        assert!(refs.contains("revenue"));
        assert!(refs.contains("cost"));
    }

    #[test]
    fn test_extract_column_references_ignores_cube_placeholder_and_cast_type() {
        let refs = extract_column_references(
            "COUNT(CASE WHEN ${CUBE}.status = 'approved' THEN 1 END)::float / NULLIF(COUNT(*), 0)",
        );
        assert!(refs.contains("status"));
        assert!(!refs.contains("CUBE"));
        assert!(!refs.contains("float"));
    }
}
