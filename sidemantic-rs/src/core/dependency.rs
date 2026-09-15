//! Dependency analysis for derived metrics
//!
//! Extracts metric dependencies from SQL expressions using polyglot-sql.

use std::collections::HashSet;

use polyglot_sql::{parse, traversal, DialectType, Expression};

use super::model::{Metric, MetricType};
use super::SemanticGraph;

/// A SQL column reference, distinguished from literals, comments and function names.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SemanticColumnReference {
    pub model: Option<String>,
    pub field: String,
    pub aggregate_input: bool,
}

impl SemanticColumnReference {
    pub fn name(&self) -> String {
        self.model.as_ref().map_or_else(
            || self.field.clone(),
            |model| format!("{model}.{}", self.field),
        )
    }
}

/// Parse one scalar expression in the handoff's declared DuckDB dialect.
/// Unlike legacy helpers, failure never falls back to scanning source text.
pub fn parse_semantic_expression(sql: &str) -> crate::error::Result<Expression> {
    #[cfg(target_arch = "wasm32")]
    crate::wasm_sql_guard::check(sql, DialectType::DuckDB)?;
    let statement =
        crate::semantic_input::dialects::parse(&format!("SELECT {sql}"), DialectType::DuckDB)?;
    let Expression::Select(mut select) = statement else {
        return Err(crate::error::SidemanticError::SqlParse(
            "expected scalar expression".into(),
        ));
    };
    if select.expressions.len() != 1 || select.from.is_some() || select.where_clause.is_some() {
        return Err(crate::error::SidemanticError::SqlParse(
            "expected one scalar expression".into(),
        ));
    }
    Ok(select.expressions.remove(0))
}

pub fn semantic_column_references(sql: &str) -> crate::error::Result<Vec<SemanticColumnReference>> {
    column_references(sql, false)
}

/// Query-filter dependencies belong to the containing SQL scope. Nested query
/// scopes have already been bound by the rewriter and must remain opaque here.
pub fn outer_semantic_column_references(
    sql: &str,
) -> crate::error::Result<Vec<SemanticColumnReference>> {
    column_references(sql, true)
}

fn column_references(
    sql: &str,
    allow_subqueries: bool,
) -> crate::error::Result<Vec<SemanticColumnReference>> {
    let expression = parse_semantic_expression(sql)?;
    // polyglot 0.1.15's public traversal omits typed aggregate/scalar children
    // (including Sum.this). Its serialized AST covers those children faithfully.
    // Walk that structure; strings/comments are never parsed as references.
    let ast = serde_json::to_value(expression)
        .map_err(|error| crate::error::SidemanticError::SqlParse(error.to_string()))?;
    let mut references = Vec::new();
    let mut stack = vec![(&ast, false)];
    while let Some((node, aggregate_input)) = stack.pop() {
        match node {
            serde_json::Value::Object(fields) => {
                let kind = (fields.len() == 1).then(|| fields.keys().next().unwrap().as_str());
                if allow_subqueries
                    && matches!(
                        kind,
                        Some("select" | "subquery" | "union" | "intersect" | "except")
                    )
                {
                    continue;
                }
                if matches!(kind, Some("select" | "subquery" | "raw")) {
                    return Err(crate::error::SidemanticError::UnsupportedSemanticFeatures {
                        capabilities: vec!["expression.subquery_or_raw_scope".into()],
                    });
                }
                let aggregate_input = aggregate_input
                    || matches!(
                        kind,
                        Some(
                            "count"
                                | "sum"
                                | "avg"
                                | "min"
                                | "max"
                                | "median"
                                | "mode"
                                | "stddev"
                                | "stddev_pop"
                                | "stddev_samp"
                                | "variance"
                                | "var_pop"
                                | "var_samp"
                                | "aggregate_function"
                                | "group_concat"
                                | "string_agg"
                                | "list_agg"
                                | "array_agg"
                                | "count_if"
                                | "sum_if"
                                | "first"
                                | "last"
                                | "any_value"
                                | "approx_distinct"
                                | "approx_count_distinct"
                                | "approx_percentile"
                                | "percentile"
                                | "logical_and"
                                | "logical_or"
                                | "skewness"
                                | "array_concat_agg"
                                | "array_unique_agg"
                                | "bool_xor_agg"
                        )
                    );
                if kind == Some("column") {
                    let column: polyglot_sql::expressions::Column =
                        serde_json::from_value(fields["column"].clone()).map_err(|error| {
                            crate::error::SidemanticError::SqlParse(error.to_string())
                        })?;
                    references.push(SemanticColumnReference {
                        model: column.table.map(|table| table.name),
                        field: column.name.name,
                        aggregate_input,
                    });
                } else {
                    stack.extend(fields.values().map(|child| (child, aggregate_input)));
                }
            }
            serde_json::Value::Array(children) => {
                stack.extend(children.iter().map(|child| (child, aggregate_input)))
            }
            _ => {}
        }
    }
    Ok(references)
}

/// Replace column nodes in every AST child, including typed functions that the
/// pinned polyglot transform visitor does not descend into.
pub fn replace_semantic_columns(
    expression: Expression,
    replacements: &std::collections::HashMap<(Option<String>, String), String>,
) -> crate::error::Result<Expression> {
    replace_columns(expression, replacements, false)
}

/// Replace only the containing scope's columns, preserving nested bindings.
pub fn replace_outer_semantic_columns(
    expression: Expression,
    replacements: &std::collections::HashMap<(Option<String>, String), String>,
) -> crate::error::Result<Expression> {
    replace_columns(expression, replacements, true)
}

fn replace_columns(
    expression: Expression,
    replacements: &std::collections::HashMap<(Option<String>, String), String>,
    skip_subqueries: bool,
) -> crate::error::Result<Expression> {
    use crate::error::SidemanticError;
    fn replace(
        value: &mut serde_json::Value,
        replacements: &std::collections::HashMap<(Option<String>, String), String>,
        skip_subqueries: bool,
    ) -> crate::error::Result<()> {
        match value {
            serde_json::Value::Object(fields) => {
                if skip_subqueries
                    && fields.len() == 1
                    && fields.keys().any(|name| {
                        matches!(
                            name.as_str(),
                            "select" | "subquery" | "union" | "intersect" | "except"
                        )
                    })
                {
                    return Ok(());
                }
                if fields.len() == 1 && fields.contains_key("column") {
                    let column: polyglot_sql::expressions::Column =
                        serde_json::from_value(fields["column"].clone())
                            .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
                    let key = (column.table.map(|table| table.name), column.name.name);
                    if let Some(sql) = replacements.get(&key) {
                        *value =
                            serde_json::to_value(Expression::Raw(polyglot_sql::expressions::Raw {
                                sql: sql.clone(),
                            }))
                            .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
                    }
                } else {
                    for child in fields.values_mut() {
                        replace(child, replacements, skip_subqueries)?;
                    }
                }
            }
            serde_json::Value::Array(children) => {
                for child in children {
                    replace(child, replacements, skip_subqueries)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    let mut value = serde_json::to_value(expression)
        .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
    replace(&mut value, replacements, skip_subqueries)?;
    serde_json::from_value(value).map_err(|error| SidemanticError::SqlGeneration(error.to_string()))
}

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
                            if has_inline_aggregation(sql) {
                                if let Some(resolved) =
                                    resolve_metric_reference(&ref_name, g, model_context)
                                {
                                    deps.insert(resolved);
                                }
                                continue;
                            }
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
        MetricType::Conversion | MetricType::Retention | MetricType::Cohort => {
            // Complex event/cohort metrics are modeled via event filters, not metric dependencies.
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

fn has_inline_aggregation(sql: &str) -> bool {
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
                && (bytes[name_start - 1].is_ascii_alphanumeric() || bytes[name_start - 1] == b'_');
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

/// Extract column references from a SQL expression
///
/// Uses polyglot-sql to parse the expression and find all column identifiers.
fn extract_column_references(sql: &str) -> HashSet<String> {
    let mut refs = HashSet::new();
    let normalized_sql = sql.replace("${CUBE}.", "").replace("${CUBE}", "");

    if has_inline_aggregation(&normalized_sql) {
        return extract_simple_references(&normalized_sql);
    }

    // polyglot-sql traversal can recurse indefinitely on some PostgreSQL cast
    // forms (expr::type). Fall back to the tokenizer path for these expressions.
    if normalized_sql.contains("::") {
        return extract_simple_references(&normalized_sql);
    }

    // Wrap in SELECT to make it valid SQL
    let wrapped = format!("SELECT {normalized_sql}");

    #[cfg(target_arch = "wasm32")]
    if crate::wasm_sql_guard::check(&wrapped, DialectType::Generic).is_err() {
        return extract_simple_references(&normalized_sql);
    }

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

fn resolve_metric_reference(
    ref_name: &str,
    graph: &SemanticGraph,
    model_context: Option<&str>,
) -> Option<String> {
    if graph.get_metric(ref_name).is_some() {
        return Some(ref_name.to_string());
    }

    if let Some((model_name, metric_name)) = ref_name.rsplit_once('.') {
        if graph
            .get_model(model_name)
            .and_then(|model| model.get_metric(metric_name))
            .is_some()
        {
            return Some(ref_name.to_string());
        }
        return None;
    }

    if let Some(context_model_name) = model_context {
        if graph
            .get_model(context_model_name)
            .and_then(|model| model.get_metric(ref_name))
            .is_some()
        {
            return Some(format!("{context_model_name}.{ref_name}"));
        }
    }

    for model in graph.models() {
        if model.get_metric(ref_name).is_some() {
            return Some(format!("{}.{}", model.name, ref_name));
        }
    }

    None
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
    use crate::core::model::{Aggregation, Dimension, Model};

    #[test]
    fn outer_filter_dependencies_do_not_capture_nested_columns_or_literals() {
        let sql = "orders.status IN (SELECT status FROM allowed WHERE note = 'orders.revenue AND customers.id')";
        let columns = outer_semantic_column_references(sql).unwrap();
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].name(), "orders.status");
        // Ordinary metric declarations retain their stricter no-subquery contract.
        assert!(semantic_column_references(sql).is_err());
    }

    #[test]
    fn outer_replacement_preserves_inner_binding_with_the_same_qualifier() {
        let expression = parse_semantic_expression(
            "orders.status IN (SELECT orders.status FROM allowed orders)",
        )
        .unwrap();
        let replacements = std::collections::HashMap::from([(
            (Some("orders".into()), "status".into()),
            "source_status".into(),
        )]);
        let expression = replace_outer_semantic_columns(expression, &replacements).unwrap();
        let sql = polyglot_sql::generate(&expression, DialectType::DuckDB).unwrap();
        assert!(sql.starts_with("source_status IN"), "{sql}");
        assert!(sql.contains("SELECT orders.status FROM allowed"), "{sql}");
    }

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
    fn test_inline_aggregation_skips_raw_field_references_with_graph() {
        let mut graph = SemanticGraph::new();
        graph
            .add_model(
                Model::new("orders", "id")
                    .with_table("orders")
                    .with_dimension(Dimension::categorical("status"))
                    .with_metric(Metric::sum("revenue", "amount")),
            )
            .unwrap();
        let metric = Metric::derived("computed_revenue", "SUM(orders.amount) * 2");

        let deps = extract_dependencies(&metric, Some(&graph));

        assert!(deps.is_empty());
    }

    #[test]
    fn test_inline_aggregation_keeps_metric_references_with_graph() {
        let mut graph = SemanticGraph::new();
        graph
            .add_model(
                Model::new("orders", "id")
                    .with_table("orders")
                    .with_metric(Metric::sum("revenue", "amount")),
            )
            .unwrap();
        let metric = Metric::derived("computed_revenue", "SUM(orders.revenue) * 2");

        let deps = extract_dependencies(&metric, Some(&graph));

        assert_eq!(deps, HashSet::from(["orders.revenue".to_string()]));
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

/// Reject expressions that change the source-row scope of a scalar input.
pub fn validate_row_expression(
    expression: &Expression,
    capability: &str,
) -> crate::error::Result<()> {
    // Typed aggregate children are not all covered by polyglot's public walker.
    // Inspect the complete AST, including aggregates without column inputs.
    fn visit(value: &serde_json::Value) -> bool {
        match value {
            serde_json::Value::Object(fields) => {
                let kind = (fields.len() == 1).then(|| fields.keys().next().unwrap().as_str());
                matches!(
                    kind,
                    Some(
                        "select"
                            | "subquery"
                            | "raw"
                            | "window"
                            | "window_function"
                            | "count"
                            | "sum"
                            | "avg"
                            | "min"
                            | "max"
                            | "median"
                            | "mode"
                            | "stddev"
                            | "stddev_pop"
                            | "stddev_samp"
                            | "variance"
                            | "var_pop"
                            | "var_samp"
                            | "aggregate_function"
                            | "group_concat"
                            | "string_agg"
                            | "list_agg"
                            | "array_agg"
                            | "count_if"
                            | "sum_if"
                            | "first"
                            | "last"
                            | "any_value"
                            | "approx_distinct"
                            | "approx_count_distinct"
                            | "approx_percentile"
                            | "percentile"
                            | "logical_and"
                            | "logical_or"
                            | "skewness"
                            | "array_concat_agg"
                            | "array_unique_agg"
                            | "bool_xor_agg"
                    )
                ) || fields.values().any(visit)
            }
            serde_json::Value::Array(values) => values.iter().any(visit),
            _ => false,
        }
    }
    let value = serde_json::to_value(expression)
        .map_err(|error| crate::error::SidemanticError::SqlParse(error.to_string()))?;
    if visit(&value) {
        return Err(crate::error::SidemanticError::UnsupportedSemanticFeatures {
            capabilities: vec![capability.to_owned()],
        });
    }
    Ok(())
}

#[cfg(test)]
mod row_expression_tests {
    use super::*;

    #[test]
    fn row_scope_checks_constant_aggregates_and_nested_nodes() {
        crate::semantic_input::with_semantic_stack(|| {
            for expression in [
                "count(*)",
                "sum(value)",
                "sum(value) over ()",
                "(select count(*) from other)",
            ] {
                let parsed = parse_semantic_expression(expression)?;
                let error = validate_row_expression(&parsed, "test.row_scope").unwrap_err();
                let crate::error::SidemanticError::UnsupportedSemanticFeatures { capabilities } =
                    error
                else {
                    panic!("expected a row-scope capability error");
                };
                assert_eq!(capabilities, vec!["test.row_scope"]);
            }
            let scalar = parse_semantic_expression("coalesce(value, 0) + 2")?;
            validate_row_expression(&scalar, "test.row_scope")?;
            Ok(())
        })
        .unwrap();
    }
}
