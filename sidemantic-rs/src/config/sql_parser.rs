//! SQL-based model definition parser using nom
//!
//! Parses Sidemantic's SQL definition format:
//! ```sql
//! MODEL (name orders, table orders, primary_key order_id);
//! DIMENSION (name status, type categorical);
//! METRIC (name revenue, expression SUM(amount));
//! SEGMENT (name active, sql status = 'active');
//! ```
//!
//! Also supports simpler SQL-like syntax:
//! ```sql
//! METRIC revenue AS SUM(amount);
//! DIMENSION status AS status;
//! ```

use std::collections::HashMap;

use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_until, take_while, take_while1},
    character::complete::{char, multispace0, multispace1},
    combinator::{map, opt, recognize},
    multi::separated_list1,
    sequence::{delimited, pair, tuple},
    IResult,
};

use sqlparser::ast::{Expr, FunctionArgExpr, SelectItem};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::core::{
    Aggregation, Dimension, DimensionType, Metric, MetricType, Model, Relationship,
    RelationshipType, Segment,
};
use crate::error::{Result, SidemanticError};

/// Property name aliases (SQL syntax -> Rust field name)
fn resolve_alias(name: &str) -> &str {
    match name.to_lowercase().as_str() {
        "expression" => "sql",
        "aggregation" => "agg",
        "filter" => "filters",
        _ => name,
    }
}

/// Statement type from parsing
#[derive(Debug, Clone)]
enum Statement {
    Model(HashMap<String, String>),
    Dimension(HashMap<String, String>),
    Metric(HashMap<String, String>),
    Segment(HashMap<String, String>),
    Relationship(HashMap<String, String>),
}

// ============================================================================
// Nom Parsers
// ============================================================================

/// Parse identifier: [a-zA-Z_][a-zA-Z0-9_]*
fn identifier(input: &str) -> IResult<&str, &str> {
    recognize(pair(
        take_while1(|c: char| c.is_alphabetic() || c == '_'),
        take_while(|c: char| c.is_alphanumeric() || c == '_'),
    ))(input)
}

/// Parse single-quoted string: 'content'
fn single_quoted_string(input: &str) -> IResult<&str, &str> {
    delimited(char('\''), take_until("'"), char('\''))(input)
}

/// Parse double-quoted string: "content"
fn double_quoted_string(input: &str) -> IResult<&str, &str> {
    delimited(char('"'), take_until("\""), char('"'))(input)
}

/// Parse any quoted string
fn quoted_string(input: &str) -> IResult<&str, String> {
    alt((
        map(single_quoted_string, String::from),
        map(double_quoted_string, String::from),
    ))(input)
}

/// Parse expression with balanced parentheses: SUM(amount), CASE WHEN ... END
fn expression_with_parens(input: &str) -> IResult<&str, String> {
    let (input, name) = identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('(')(input)?;
    let (input, content) = parse_balanced_parens(input)?;
    let (input, _) = char(')')(input)?;
    Ok((input, format!("{name}({content})")))
}

/// Parse content with balanced parentheses
fn parse_balanced_parens(input: &str) -> IResult<&str, String> {
    let mut result = String::new();
    let mut depth = 0;
    let mut chars = input.char_indices().peekable();

    while let Some((i, c)) = chars.next() {
        match c {
            '(' => {
                depth += 1;
                result.push(c);
            }
            ')' => {
                if depth == 0 {
                    return Ok((&input[i..], result));
                }
                depth -= 1;
                result.push(c);
            }
            '\'' => {
                // Handle quoted string inside expression
                result.push(c);
                for (_, inner_c) in chars.by_ref() {
                    result.push(inner_c);
                    if inner_c == '\'' {
                        break;
                    }
                }
            }
            _ => result.push(c),
        }
    }

    Ok(("", result))
}

/// Parse a simple value (identifier without following paren)
fn simple_value(input: &str) -> IResult<&str, String> {
    // Take chars until we hit comma, closing paren, or whitespace followed by comma/paren
    let mut result = String::new();
    let mut chars = input.char_indices().peekable();

    while let Some((i, c)) = chars.peek() {
        match c {
            ',' | ')' => {
                return Ok((&input[*i..], result.trim().to_string()));
            }
            ' ' | '\t' | '\n' | '\r' => {
                // Look ahead to see if next non-ws is comma or paren
                let rest = &input[*i..];
                let trimmed = rest.trim_start();
                if trimmed.starts_with(',') || trimmed.starts_with(')') {
                    return Ok((trimmed, result.trim().to_string()));
                }
                result.push(*c);
                chars.next();
            }
            _ => {
                result.push(*c);
                chars.next();
            }
        }
    }

    Ok(("", result.trim().to_string()))
}

/// Parse a property value (quoted string, expression with parens, or simple value)
fn property_value(input: &str) -> IResult<&str, String> {
    let (input, _) = multispace0(input)?;

    // Try quoted string first
    if let Ok((rest, s)) = quoted_string(input) {
        return Ok((rest, s));
    }

    // Try expression with parentheses
    if let Ok((rest, expr)) = expression_with_parens(input) {
        return Ok((rest, expr));
    }

    // Fall back to simple value
    simple_value(input)
}

/// Parse a single property: name value
fn property(input: &str) -> IResult<&str, (String, String)> {
    let (input, _) = multispace0(input)?;
    let (input, name) = identifier(input)?;
    let (input, _) = multispace1(input)?;
    let (input, value) = property_value(input)?;

    let resolved_name = resolve_alias(name).to_lowercase();
    Ok((input, (resolved_name, value)))
}

/// Parse property list: prop1, prop2, prop3
fn property_list(input: &str) -> IResult<&str, HashMap<String, String>> {
    let (input, _) = multispace0(input)?;
    let (input, props) =
        separated_list1(tuple((multispace0, char(','), multispace0)), property)(input)?;
    let (input, _) = opt(tuple((multispace0, char(','))))(input)?;
    let (input, _) = multispace0(input)?;

    Ok((input, props.into_iter().collect()))
}

/// Parse a definition: KEYWORD (properties)
fn definition<'a>(
    keyword: &'static str,
) -> impl FnMut(&'a str) -> IResult<&'a str, HashMap<String, String>> {
    move |input: &'a str| {
        let (input, _) = multispace0(input)?;
        let (input, _) = tag_no_case(keyword)(input)?;
        let (input, _) = multispace0(input)?;
        let (input, props) = delimited(char('('), property_list, char(')'))(input)?;
        let (input, _) = multispace0(input)?;
        let (input, _) = opt(char(';'))(input)?;
        Ok((input, props))
    }
}

// ============================================================================
// Simple AS-syntax parsers (METRIC name AS expr)
// ============================================================================

/// Parse simple METRIC: METRIC name AS expr
fn simple_metric(input: &str) -> IResult<&str, Statement> {
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("METRIC")(input)?;
    let (input, _) = multispace1(input)?;

    // Get metric name (may include model prefix like orders.revenue)
    let (input, name) = recognize(pair(identifier, opt(pair(char('.'), identifier))))(input)?;

    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("AS")(input)?;
    let (input, _) = multispace1(input)?;

    // Get the expression (everything until semicolon or end)
    let (input, expr) = take_while(|c| c != ';')(input)?;
    let (input, _) = opt(char(';'))(input)?;

    // Parse the expression using sqlparser to extract aggregation
    let props = parse_metric_expression(name.trim(), expr.trim());
    Ok((input, Statement::Metric(props)))
}

/// Parse simple DIMENSION: DIMENSION name AS expr
fn simple_dimension(input: &str) -> IResult<&str, Statement> {
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("DIMENSION")(input)?;
    let (input, _) = multispace1(input)?;

    // Get dimension name (may include model prefix)
    let (input, name) = recognize(pair(identifier, opt(pair(char('.'), identifier))))(input)?;

    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("AS")(input)?;
    let (input, _) = multispace1(input)?;

    // Get the expression
    let (input, expr) = take_while(|c| c != ';')(input)?;
    let (input, _) = opt(char(';'))(input)?;

    let mut props = HashMap::new();
    props.insert("name".to_string(), name.trim().to_string());
    props.insert("sql".to_string(), expr.trim().to_string());
    // Try to infer type from expression
    props.insert("type".to_string(), infer_dimension_type(expr.trim()));

    Ok((input, Statement::Dimension(props)))
}

/// Parse metric expression to extract aggregation function
fn parse_metric_expression(name: &str, expr: &str) -> HashMap<String, String> {
    let mut props = HashMap::new();
    props.insert("name".to_string(), name.to_string());

    // Try to parse as SQL and extract aggregation
    let sql = format!("SELECT {expr}");
    let dialect = GenericDialect {};

    if let Ok(statements) = Parser::parse_sql(&dialect, &sql) {
        if let Some(sqlparser::ast::Statement::Query(query)) = statements.into_iter().next() {
            if let sqlparser::ast::SetExpr::Select(select) = *query.body {
                if let Some(SelectItem::UnnamedExpr(parsed_expr)) =
                    select.projection.into_iter().next()
                {
                    if let Some((agg, inner_expr)) = extract_aggregation(&parsed_expr) {
                        props.insert("agg".to_string(), agg);
                        if !inner_expr.is_empty() {
                            props.insert("sql".to_string(), inner_expr);
                        }
                        return props;
                    }
                }
            }
        }
    }

    // Fall back to storing the whole expression as sql with "expression" type
    // This allows complex expressions like SUM(amount) * 2
    props.insert("agg".to_string(), "expression".to_string());
    props.insert("sql".to_string(), expr.to_string());
    props
}

/// Extract aggregation function and inner expression from SQL AST
fn extract_aggregation(expr: &Expr) -> Option<(String, String)> {
    match expr {
        Expr::Function(func) => {
            let func_name = func.name.to_string().to_lowercase();
            let agg = match func_name.as_str() {
                "sum" => "sum",
                "count" => "count",
                "avg" | "average" => "avg",
                "min" => "min",
                "max" => "max",
                "count_distinct" => "count_distinct",
                _ => return None,
            };

            // Extract the inner expression from function arguments
            let inner = match &func.args {
                sqlparser::ast::FunctionArguments::None => String::new(),
                sqlparser::ast::FunctionArguments::Subquery(_) => return None,
                sqlparser::ast::FunctionArguments::List(arg_list) => {
                    if arg_list.args.is_empty() {
                        String::new()
                    } else {
                        match &arg_list.args[0] {
                            sqlparser::ast::FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
                                e.to_string()
                            }
                            sqlparser::ast::FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
                                String::new() // COUNT(*)
                            }
                            sqlparser::ast::FunctionArg::Unnamed(
                                FunctionArgExpr::QualifiedWildcard(_),
                            ) => String::new(),
                            _ => return None,
                        }
                    }
                }
            };

            Some((agg.to_string(), inner))
        }
        _ => None,
    }
}

/// Infer dimension type from expression
fn infer_dimension_type(expr: &str) -> String {
    let expr_lower = expr.to_lowercase();
    if expr_lower.contains("date")
        || expr_lower.contains("time")
        || expr_lower.contains("timestamp")
    {
        "time".to_string()
    } else {
        "categorical".to_string()
    }
}

/// Parse METRIC with model prefix: METRIC model.name (props)
fn prefixed_metric(input: &str) -> IResult<&str, Statement> {
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("METRIC")(input)?;
    let (input, _) = multispace1(input)?;

    // Must have model.name pattern
    let (input, model) = identifier(input)?;
    let (input, _) = char('.')(input)?;
    let (input, name) = identifier(input)?;

    let (input, _) = multispace0(input)?;
    let (input, props) = delimited(char('('), property_list, char(')'))(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = opt(char(';'))(input)?;

    // Add the name to props and include model prefix
    let mut props = props;
    props.insert("name".to_string(), format!("{model}.{name}"));
    Ok((input, Statement::Metric(props)))
}

/// Parse DIMENSION with model prefix: DIMENSION model.name (props)
fn prefixed_dimension(input: &str) -> IResult<&str, Statement> {
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("DIMENSION")(input)?;
    let (input, _) = multispace1(input)?;

    // Must have model.name pattern
    let (input, model) = identifier(input)?;
    let (input, _) = char('.')(input)?;
    let (input, name) = identifier(input)?;

    let (input, _) = multispace0(input)?;
    let (input, props) = delimited(char('('), property_list, char(')'))(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = opt(char(';'))(input)?;

    let mut props = props;
    props.insert("name".to_string(), format!("{model}.{name}"));
    Ok((input, Statement::Dimension(props)))
}

/// Parse any statement (tries simple AS syntax first, then parenthesized)
fn statement(input: &str) -> IResult<&str, Statement> {
    let (input, _) = multispace0(input)?;

    alt((
        map(definition("MODEL"), Statement::Model),
        // Try simple AS syntax first for METRIC and DIMENSION
        simple_metric,
        simple_dimension,
        // Try model.name (props) syntax
        prefixed_metric,
        prefixed_dimension,
        // Fall back to simple parenthesized syntax
        map(definition("DIMENSION"), Statement::Dimension),
        map(definition("METRIC"), Statement::Metric),
        map(definition("SEGMENT"), Statement::Segment),
        map(definition("RELATIONSHIP"), Statement::Relationship),
    ))(input)
}

/// Skip comment line
fn comment(input: &str) -> IResult<&str, ()> {
    let (input, _) = tag("--")(input)?;
    let (input, _) = take_while(|c| c != '\n')(input)?;
    let (input, _) = opt(char('\n'))(input)?;
    Ok((input, ()))
}

/// Parse file with statements and comments
fn parse_file(input: &str) -> IResult<&str, Vec<Statement>> {
    let mut statements = Vec::new();
    let mut remaining = input;

    loop {
        // Skip whitespace
        let (input, _) = multispace0(remaining)?;
        remaining = input;

        if remaining.is_empty() {
            break;
        }

        // Try to skip comment
        if let Ok((input, _)) = comment(remaining) {
            remaining = input;
            continue;
        }

        // Try to parse statement
        match statement(remaining) {
            Ok((input, stmt)) => {
                statements.push(stmt);
                remaining = input;
            }
            Err(_) => {
                // Skip unknown content until next statement or end
                if let Some(pos) = remaining.find(|c: char| c.is_alphabetic()) {
                    remaining = &remaining[pos..];
                } else {
                    break;
                }
            }
        }
    }

    Ok((remaining, statements))
}

// ============================================================================
// Public API
// ============================================================================

/// Parse SQL definitions into a Model
pub fn parse_sql_model(sql: &str) -> Result<Model> {
    let (_, statements) =
        parse_file(sql).map_err(|e| SidemanticError::Validation(format!("Parse error: {e}")))?;

    let mut model: Option<Model> = None;
    let mut dimensions = Vec::new();
    let mut metrics = Vec::new();
    let mut segments = Vec::new();
    let mut relationships = Vec::new();

    for stmt in statements {
        match stmt {
            Statement::Model(props) => {
                model = Some(build_model(&props)?);
            }
            Statement::Dimension(props) => {
                if let Some(dim) = build_dimension(&props) {
                    dimensions.push(dim);
                }
            }
            Statement::Metric(props) => {
                if let Some(metric) = build_metric(&props) {
                    metrics.push(metric);
                }
            }
            Statement::Segment(props) => {
                if let Some(seg) = build_segment(&props) {
                    segments.push(seg);
                }
            }
            Statement::Relationship(props) => {
                if let Some(rel) = build_relationship(&props) {
                    relationships.push(rel);
                }
            }
        }
    }

    let mut model = model.ok_or_else(|| {
        SidemanticError::Validation("SQL definitions must include a MODEL statement".into())
    })?;

    model.dimensions.extend(dimensions);
    model.metrics.extend(metrics);
    model.segments.extend(segments);
    model.relationships.extend(relationships);

    Ok(model)
}

/// Parse SQL definitions for metrics and segments only
pub fn parse_sql_definitions(sql: &str) -> Result<(Vec<Metric>, Vec<Segment>)> {
    let (_, statements) =
        parse_file(sql).map_err(|e| SidemanticError::Validation(format!("Parse error: {e}")))?;

    let mut metrics = Vec::new();
    let mut segments = Vec::new();

    for stmt in statements {
        match stmt {
            Statement::Metric(props) => {
                if let Some(metric) = build_metric(&props) {
                    metrics.push(metric);
                }
            }
            Statement::Segment(props) => {
                if let Some(seg) = build_segment(&props) {
                    segments.push(seg);
                }
            }
            _ => {}
        }
    }

    Ok((metrics, segments))
}

// ============================================================================
// Builders
// ============================================================================

fn build_model(props: &HashMap<String, String>) -> Result<Model> {
    let name = props
        .get("name")
        .ok_or_else(|| SidemanticError::Validation("MODEL requires 'name' property".into()))?;

    let mut model = Model::new(
        name,
        props.get("primary_key").map(|s| s.as_str()).unwrap_or("id"),
    );

    if let Some(table) = props.get("table") {
        model.table = Some(table.clone());
    }
    if let Some(sql) = props.get("sql") {
        model.sql = Some(sql.clone());
    }
    if let Some(desc) = props.get("description") {
        model.description = Some(desc.clone());
    }
    if let Some(label) = props.get("label") {
        model.label = Some(label.clone());
    }

    Ok(model)
}

fn build_dimension(props: &HashMap<String, String>) -> Option<Dimension> {
    let name = props.get("name")?;
    let dim_type = props
        .get("type")
        .map(|t| t.as_str())
        .unwrap_or("categorical");

    let dtype = match dim_type.to_lowercase().as_str() {
        "time" | "timestamp" | "date" => DimensionType::Time,
        "number" | "numeric" | "integer" | "float" => DimensionType::Numeric,
        "boolean" | "bool" => DimensionType::Boolean,
        _ => DimensionType::Categorical,
    };

    let mut dim = Dimension::new(name);
    dim.r#type = dtype;

    if let Some(sql) = props.get("sql") {
        dim.sql = Some(sql.clone());
    }
    if let Some(desc) = props.get("description") {
        dim.description = Some(desc.clone());
    }
    if let Some(label) = props.get("label") {
        dim.label = Some(label.clone());
    }

    Some(dim)
}

fn build_metric(props: &HashMap<String, String>) -> Option<Metric> {
    let name = props.get("name")?;

    let mut metric = Metric::new(name);
    metric.sql = props.get("sql").cloned();
    metric.numerator = props.get("numerator").cloned();
    metric.denominator = props.get("denominator").cloned();
    metric.description = props.get("description").cloned();
    metric.label = props.get("label").cloned();
    metric.format = props.get("format").cloned();

    if let Some(agg_str) = props.get("agg") {
        metric.agg = match agg_str.to_lowercase().as_str() {
            "sum" => Some(Aggregation::Sum),
            "count" => Some(Aggregation::Count),
            "count_distinct" | "countdistinct" => Some(Aggregation::CountDistinct),
            "avg" | "average" => Some(Aggregation::Avg),
            "min" => Some(Aggregation::Min),
            "max" => Some(Aggregation::Max),
            "expression" => Some(Aggregation::Expression),
            _ => None,
        };
    }

    if metric.numerator.is_some() && metric.denominator.is_some() {
        metric.r#type = MetricType::Ratio;
    } else if metric
        .sql
        .as_ref()
        .map(|s| s.contains('{'))
        .unwrap_or(false)
    {
        metric.r#type = MetricType::Derived;
    }

    Some(metric)
}

fn build_segment(props: &HashMap<String, String>) -> Option<Segment> {
    let name = props.get("name")?;
    let sql = props.get("sql")?;

    Some(Segment {
        name: name.clone(),
        sql: sql.clone(),
        description: props.get("description").cloned(),
        public: props
            .get("public")
            .map(|s| s.to_lowercase() == "true")
            .unwrap_or(true),
    })
}

fn build_relationship(props: &HashMap<String, String>) -> Option<Relationship> {
    let name = props.get("name")?;
    let rel_type = props
        .get("type")
        .map(|t| t.as_str())
        .unwrap_or("many_to_one");

    let rtype = match rel_type.to_lowercase().as_str() {
        "one_to_one" | "onetoone" => RelationshipType::OneToOne,
        "one_to_many" | "onetomany" => RelationshipType::OneToMany,
        "many_to_many" | "manytomany" => RelationshipType::ManyToMany,
        _ => RelationshipType::ManyToOne,
    };

    Some(Relationship {
        name: name.clone(),
        r#type: rtype,
        foreign_key: props.get("foreign_key").cloned(),
        primary_key: props.get("primary_key").cloned(),
        sql: props.get("sql").cloned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_model() {
        let sql = r#"
            MODEL (
                name orders,
                table orders,
                primary_key order_id
            );
        "#;

        let model = parse_sql_model(sql).unwrap();
        assert_eq!(model.name, "orders");
        assert_eq!(model.table, Some("orders".to_string()));
        assert_eq!(model.primary_key, "order_id");
    }

    #[test]
    fn test_parse_model_with_dimensions() {
        let sql = r#"
            MODEL (name orders, table orders, primary_key order_id);
            DIMENSION (name status, type categorical);
            DIMENSION (name order_date, type time, sql created_at);
        "#;

        let model = parse_sql_model(sql).unwrap();
        assert_eq!(model.dimensions.len(), 2);
        assert!(model.get_dimension("status").is_some());
        assert!(model.get_dimension("order_date").is_some());
    }

    #[test]
    fn test_parse_model_with_metrics() {
        let sql = r#"
            MODEL (name orders, table orders);
            METRIC (name revenue, agg sum, sql amount);
            METRIC (name order_count, agg count);
        "#;

        let model = parse_sql_model(sql).unwrap();
        assert_eq!(model.metrics.len(), 2);

        let revenue = model.get_metric("revenue").unwrap();
        assert_eq!(revenue.agg, Some(Aggregation::Sum));
        assert_eq!(revenue.sql, Some("amount".to_string()));
    }

    #[test]
    fn test_parse_metric_with_expression() {
        let sql = r#"
            MODEL (name orders, table orders);
            METRIC (name revenue, expression SUM(amount));
        "#;

        let model = parse_sql_model(sql).unwrap();
        let revenue = model.get_metric("revenue").unwrap();
        assert_eq!(revenue.sql, Some("SUM(amount)".to_string()));
    }

    #[test]
    fn test_parse_segment() {
        let sql = r#"
            MODEL (name orders, table orders);
            SEGMENT (name completed, sql status = 'completed');
        "#;

        let model = parse_sql_model(sql).unwrap();
        assert_eq!(model.segments.len(), 1);

        let seg = model.get_segment("completed").unwrap();
        assert!(seg.sql.contains("status"));
    }

    #[test]
    fn test_parse_relationship() {
        let sql = r#"
            MODEL (name orders, table orders);
            RELATIONSHIP (name customers, type many_to_one, foreign_key customer_id);
        "#;

        let model = parse_sql_model(sql).unwrap();
        assert_eq!(model.relationships.len(), 1);

        let rel = model.get_relationship("customers").unwrap();
        assert_eq!(rel.r#type, RelationshipType::ManyToOne);
        assert_eq!(rel.foreign_key, Some("customer_id".to_string()));
    }

    #[test]
    fn test_parse_with_quoted_strings() {
        let sql = r#"
            MODEL (name orders, table orders, description 'Order transactions');
            METRIC (name revenue, agg sum, sql amount, description 'Total revenue in USD');
        "#;

        let model = parse_sql_model(sql).unwrap();
        assert_eq!(model.description, Some("Order transactions".to_string()));

        let revenue = model.get_metric("revenue").unwrap();
        assert_eq!(
            revenue.description,
            Some("Total revenue in USD".to_string())
        );
    }

    #[test]
    fn test_parse_with_comments() {
        let sql = r#"
            -- This is a comment
            MODEL (name orders, table orders);
            -- Another comment
            METRIC (name revenue, agg sum, sql amount);
        "#;

        let model = parse_sql_model(sql).unwrap();
        assert_eq!(model.name, "orders");
        assert_eq!(model.metrics.len(), 1);
    }

    #[test]
    fn test_simple_metric_syntax() {
        let sql = r#"
            MODEL (name orders, table orders);
            METRIC revenue AS SUM(amount);
            METRIC order_count AS COUNT(*);
        "#;

        let model = parse_sql_model(sql).unwrap();
        assert_eq!(model.metrics.len(), 2);

        let revenue = model.get_metric("revenue").unwrap();
        assert_eq!(revenue.agg, Some(Aggregation::Sum));
        assert_eq!(revenue.sql, Some("amount".to_string()));

        let count = model.get_metric("order_count").unwrap();
        assert_eq!(count.agg, Some(Aggregation::Count));
    }

    #[test]
    fn test_simple_dimension_syntax() {
        let sql = r#"
            MODEL (name orders, table orders);
            DIMENSION status AS status;
            DIMENSION order_date AS created_at;
        "#;

        let model = parse_sql_model(sql).unwrap();
        assert_eq!(model.dimensions.len(), 2);

        let status = model.get_dimension("status").unwrap();
        assert_eq!(status.sql, Some("status".to_string()));

        let order_date = model.get_dimension("order_date").unwrap();
        assert_eq!(order_date.sql, Some("created_at".to_string()));
    }

    #[test]
    fn test_mixed_syntax() {
        let sql = r#"
            MODEL (name orders, table orders);
            METRIC revenue AS SUM(amount);
            METRIC (name avg_value, agg avg, sql amount);
            DIMENSION status AS status;
            DIMENSION (name category, type categorical);
        "#;

        let model = parse_sql_model(sql).unwrap();
        assert_eq!(model.metrics.len(), 2);
        assert_eq!(model.dimensions.len(), 2);
    }
}
