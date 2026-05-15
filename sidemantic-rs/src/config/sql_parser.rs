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
    error::{Error as NomError, ErrorKind},
    multi::separated_list1,
    sequence::{delimited, pair, tuple},
    IResult,
};

use polyglot_sql::Expression;
#[cfg(not(target_arch = "wasm32"))]
use polyglot_sql::{parse as polyglot_parse, DialectType};

use crate::core::{
    Aggregation, ComparisonCalculation, ComparisonType, Dimension, DimensionType, Index, Metric,
    MetricType, Model, Parameter, ParameterType, PreAggregation, PreAggregationType, RefreshKey,
    Relationship, RelationshipType, Segment, TimeGrain,
};
use crate::error::{Result, SidemanticError};

type SqlGraphDefinitionParts = (
    Vec<Metric>,
    Vec<Segment>,
    Vec<Parameter>,
    Vec<PreAggregation>,
);

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
    Parameter(HashMap<String, String>),
    PreAggregation(HashMap<String, String>),
}

/// Serializable SQL statement block payload for Python bridge consumers.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SqlStatementBlock {
    pub kind: String,
    pub properties: HashMap<String, String>,
}

fn split_top_level(text: &str, delimiter: char) -> Vec<String> {
    let mut items = Vec::new();
    let mut depth: i32 = 0;
    let mut in_quote: Option<char> = None;
    let mut escape = false;
    let mut buf = String::new();

    for c in text.chars() {
        if let Some(q) = in_quote {
            buf.push(c);
            if escape {
                escape = false;
                continue;
            }
            if c == '\\' {
                escape = true;
                continue;
            }
            if c == q {
                in_quote = None;
            }
            continue;
        }

        if c == '\'' || c == '"' {
            in_quote = Some(c);
            buf.push(c);
            continue;
        }

        if c == '[' || c == '{' {
            depth += 1;
        } else if c == ']' || c == '}' {
            depth = (depth - 1).max(0);
        }

        if c == delimiter && depth == 0 {
            let item = buf.trim().to_string();
            if !item.is_empty() {
                items.push(item);
            }
            buf.clear();
            continue;
        }

        buf.push(c);
    }

    let trailing = buf.trim().to_string();
    if !trailing.is_empty() {
        items.push(trailing);
    }

    items
}

fn split_key_value(text: &str) -> (String, String) {
    let mut depth: i32 = 0;
    let mut in_quote: Option<char> = None;

    for (idx, c) in text.char_indices() {
        if let Some(q) = in_quote {
            if c == q {
                in_quote = None;
            }
            continue;
        }

        if c == '\'' || c == '"' {
            in_quote = Some(c);
            continue;
        }

        if depth == 0 && (c == '[' || c == '{') && idx > 0 {
            return (
                text[..idx].trim().to_string(),
                text[idx..].trim().to_string(),
            );
        }

        if c == '[' || c == '{' {
            depth += 1;
            continue;
        }
        if c == ']' || c == '}' {
            depth = (depth - 1).max(0);
            continue;
        }

        if depth == 0 && (c == ':' || c == '=') {
            return (
                text[..idx].trim().to_string(),
                text[idx + 1..].trim().to_string(),
            );
        }

        if depth == 0 && c.is_whitespace() {
            return (
                text[..idx].trim().to_string(),
                text[idx..].trim().to_string(),
            );
        }
    }

    (text.trim().to_string(), String::new())
}

fn parse_scalar_literal(value: &str) -> serde_json::Value {
    if value.is_empty() {
        return serde_json::Value::String(String::new());
    }

    if value.len() >= 2 {
        let first = value.chars().next().unwrap_or_default();
        let last = value.chars().next_back().unwrap_or_default();
        if (first == '\'' || first == '"') && first == last {
            let mut inner = value[1..value.len() - 1].to_string();
            if first == '\'' {
                inner = inner.replace("''", "'");
            }
            return serde_json::Value::String(inner);
        }
    }

    let lowered = value.to_lowercase();
    if lowered == "true" {
        return serde_json::Value::Bool(true);
    }
    if lowered == "false" {
        return serde_json::Value::Bool(false);
    }
    if lowered == "null" || lowered == "none" {
        return serde_json::Value::Null;
    }

    if let Ok(v) = value.parse::<i64>() {
        return serde_json::json!(v);
    }
    if let Ok(v) = value.parse::<f64>() {
        if let Some(num) = serde_json::Number::from_f64(v) {
            return serde_json::Value::Number(num);
        }
    }

    serde_json::Value::String(value.to_string())
}

fn parse_literal(value: &str) -> serde_json::Value {
    let raw = value.trim();
    if raw.is_empty() {
        return serde_json::Value::String(String::new());
    }

    if raw.starts_with('[') && raw.ends_with(']') {
        let inner = raw[1..raw.len() - 1].trim();
        let items = split_top_level(inner, ',')
            .into_iter()
            .map(|item| parse_literal(&item))
            .collect::<Vec<_>>();
        return serde_json::Value::Array(items);
    }

    if raw.starts_with('{') && raw.ends_with('}') {
        let inner = raw[1..raw.len() - 1].trim();
        let mut object = serde_json::Map::new();
        for pair in split_top_level(inner, ',') {
            if pair.is_empty() {
                continue;
            }
            let (key, raw_value) = split_key_value(&pair);
            if key.is_empty() {
                continue;
            }
            let parsed_key = parse_scalar_literal(&key);
            let key_str = match parsed_key {
                serde_json::Value::String(s) => s,
                _ => key,
            };
            if raw_value.is_empty() {
                object.insert(key_str, serde_json::Value::Bool(true));
            } else {
                object.insert(key_str, parse_literal(&raw_value));
            }
        }
        return serde_json::Value::Object(object);
    }

    parse_scalar_literal(raw)
}

fn json_value_to_string(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::Null => None,
        serde_json::Value::String(v) => Some(v.clone()),
        serde_json::Value::Bool(v) => Some(if *v { "true".into() } else { "false".into() }),
        serde_json::Value::Number(v) => Some(v.to_string()),
        _ => Some(value.to_string()),
    }
}

fn json_value_to_string_list(value: serde_json::Value) -> Vec<String> {
    match value {
        serde_json::Value::Array(items) => items
            .into_iter()
            .filter_map(|item| json_value_to_string(&item))
            .collect(),
        other => json_value_to_string(&other).into_iter().collect(),
    }
}

fn parse_key_columns(props: &HashMap<String, String>, key: &str) -> Option<Vec<String>> {
    props.get(key).and_then(|value| {
        let parsed = parse_literal(value);
        let columns = json_value_to_string_list(parsed);
        if columns.is_empty() {
            None
        } else {
            Some(columns)
        }
    })
}

fn parse_metric_type(value: Option<&String>) -> MetricType {
    match value.map(|s| s.to_lowercase()) {
        Some(metric_type) if metric_type == "derived" => MetricType::Derived,
        Some(metric_type) if metric_type == "ratio" => MetricType::Ratio,
        Some(metric_type) if metric_type == "cumulative" => MetricType::Cumulative,
        Some(metric_type)
            if metric_type == "time_comparison" || metric_type == "timecomparison" =>
        {
            MetricType::TimeComparison
        }
        Some(metric_type) if metric_type == "conversion" => MetricType::Conversion,
        _ => MetricType::Simple,
    }
}

fn parse_metric_aggregation(value: Option<&String>) -> Option<Aggregation> {
    value.and_then(|agg_str| match agg_str.to_lowercase().as_str() {
        "sum" => Some(Aggregation::Sum),
        "count" => Some(Aggregation::Count),
        "count_distinct" | "countdistinct" => Some(Aggregation::CountDistinct),
        "avg" | "average" => Some(Aggregation::Avg),
        "min" => Some(Aggregation::Min),
        "max" => Some(Aggregation::Max),
        "median" => Some(Aggregation::Median),
        "expression" => Some(Aggregation::Expression),
        _ => None,
    })
}

fn parse_time_grain(value: Option<&String>) -> Option<TimeGrain> {
    value.and_then(|grain| match grain.to_lowercase().as_str() {
        "day" => Some(TimeGrain::Day),
        "week" => Some(TimeGrain::Week),
        "month" => Some(TimeGrain::Month),
        "quarter" => Some(TimeGrain::Quarter),
        "year" => Some(TimeGrain::Year),
        _ => None,
    })
}

fn parse_comparison_type(value: Option<&String>) -> Option<ComparisonType> {
    value.and_then(|comparison| match comparison.to_lowercase().as_str() {
        "yoy" => Some(ComparisonType::Yoy),
        "mom" => Some(ComparisonType::Mom),
        "wow" => Some(ComparisonType::Wow),
        "dod" => Some(ComparisonType::Dod),
        "qoq" => Some(ComparisonType::Qoq),
        "prior_period" => Some(ComparisonType::PriorPeriod),
        _ => None,
    })
}

fn parse_comparison_calculation(value: Option<&String>) -> Option<ComparisonCalculation> {
    value.and_then(|calc| match calc.to_lowercase().as_str() {
        "difference" => Some(ComparisonCalculation::Difference),
        "percent_change" => Some(ComparisonCalculation::PercentChange),
        "ratio" => Some(ComparisonCalculation::Ratio),
        _ => None,
    })
}

fn parse_parameter_type(value: Option<&String>) -> Option<ParameterType> {
    value.and_then(
        |parameter_type| match parameter_type.to_lowercase().as_str() {
            "string" => Some(ParameterType::String),
            "number" => Some(ParameterType::Number),
            "date" => Some(ParameterType::Date),
            "unquoted" => Some(ParameterType::Unquoted),
            "yesno" => Some(ParameterType::Yesno),
            _ => None,
        },
    )
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

/// Parse a raw value until the next top-level comma or ')' delimiter.
fn simple_value(input: &str) -> IResult<&str, String> {
    let mut depth_paren: i32 = 0;
    let mut depth_bracket: i32 = 0;
    let mut depth_brace: i32 = 0;
    let mut in_quote: Option<char> = None;
    let mut escape = false;

    for (idx, c) in input.char_indices() {
        if let Some(q) = in_quote {
            if escape {
                escape = false;
                continue;
            }
            if c == '\\' {
                escape = true;
                continue;
            }
            if c == q {
                in_quote = None;
            }
            continue;
        }

        if c == '\'' || c == '"' {
            in_quote = Some(c);
            continue;
        }

        match c {
            '(' => depth_paren += 1,
            ')' => {
                if depth_paren > 0 {
                    depth_paren -= 1;
                } else if depth_bracket == 0 && depth_brace == 0 {
                    return Ok((&input[idx..], input[..idx].trim().to_string()));
                }
            }
            '[' => depth_bracket += 1,
            ']' => depth_bracket = (depth_bracket - 1).max(0),
            '{' => depth_brace += 1,
            '}' => depth_brace = (depth_brace - 1).max(0),
            ',' if depth_paren == 0 && depth_bracket == 0 && depth_brace == 0 => {
                return Ok((&input[idx..], input[..idx].trim().to_string()));
            }
            _ => {}
        }
    }

    Ok(("", input.trim().to_string()))
}

/// Parse a property value (quoted string, expression with parens, or simple value)
fn property_value(input: &str) -> IResult<&str, String> {
    let (input, _) = multispace0(input)?;

    // Try quoted string first
    if let Ok((rest, s)) = quoted_string(input) {
        return Ok((rest, s));
    }

    // Fall back to raw value parsing (supports nested (), [], and {})
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

    if let Some((agg, inner_expr)) = extract_aggregation_with_polyglot(expr) {
        props.insert("agg".to_string(), agg);
        if !inner_expr.is_empty() {
            props.insert("sql".to_string(), inner_expr);
        }
        return props;
    }

    // Fall back to storing the whole expression as sql with "expression" type
    // This allows complex expressions like SUM(amount) * 2
    props.insert("agg".to_string(), "expression".to_string());
    props.insert("sql".to_string(), expr.to_string());
    props
}

fn extract_aggregation_with_polyglot(expr: &str) -> Option<(String, String)> {
    let sql = format!("SELECT {expr}");
    let statements = parse_polyglot_with_large_stack(sql)?;
    let statement = statements.first()?;
    let select = statement.as_select()?;
    let parsed_expr = select.expressions.first()?;
    extract_aggregation_polyglot_expr(parsed_expr)
}

fn parse_polyglot_with_large_stack(sql: String) -> Option<Vec<Expression>> {
    #[cfg(target_arch = "wasm32")]
    {
        let _ = sql;
        return None;
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        std::thread::Builder::new()
            .stack_size(16 * 1024 * 1024)
            .spawn(move || polyglot_parse(&sql, DialectType::Generic).ok()) // stack-heavy parser path
            .ok()?
            .join()
            .ok()?
    }
}

fn extract_aggregation_polyglot_expr(expr: &Expression) -> Option<(String, String)> {
    match expr {
        Expression::Alias(alias) => extract_aggregation_polyglot_expr(&alias.this),
        Expression::Sum(agg) => Some((
            "sum".to_string(),
            extract_inner_expression_polyglot(&agg.this),
        )),
        Expression::Count(count) => {
            if count.star {
                return Some(("count".to_string(), String::new()));
            }

            let agg_name = if count.distinct {
                "count_distinct"
            } else {
                "count"
            };
            let inner = count
                .this
                .as_ref()
                .map(extract_inner_expression_polyglot)
                .unwrap_or_default();
            Some((agg_name.to_string(), inner))
        }
        Expression::Avg(agg) => Some((
            "avg".to_string(),
            extract_inner_expression_polyglot(&agg.this),
        )),
        Expression::Min(agg) => Some((
            "min".to_string(),
            extract_inner_expression_polyglot(&agg.this),
        )),
        Expression::Max(agg) => Some((
            "max".to_string(),
            extract_inner_expression_polyglot(&agg.this),
        )),
        Expression::AggregateFunction(func) => {
            extract_aggregation_from_function_name_polyglot(&func.name, func.args.first())
        }
        Expression::Function(func) => {
            extract_aggregation_from_function_name_polyglot(&func.name, func.args.first())
        }
        _ => None,
    }
}

fn extract_aggregation_from_function_name_polyglot(
    function_name: &str,
    first_arg: Option<&Expression>,
) -> Option<(String, String)> {
    let agg = match function_name.to_lowercase().as_str() {
        "sum" => "sum",
        "count" => "count",
        "avg" | "average" => "avg",
        "min" => "min",
        "max" => "max",
        "count_distinct" => "count_distinct",
        _ => return None,
    };

    let inner = match first_arg {
        Some(Expression::Star(_)) | None => String::new(),
        Some(arg) => extract_inner_expression_polyglot(arg),
    };

    Some((agg.to_string(), inner))
}

fn extract_inner_expression_polyglot(expr: &Expression) -> String {
    match expr {
        Expression::Column(column) => {
            if let Some(table) = &column.table {
                format!("{}.{}", table.name, column.name.name)
            } else {
                column.name.name.clone()
            }
        }
        Expression::Identifier(ident) => ident.name.clone(),
        Expression::Star(_) => String::new(),
        _ => String::new(),
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
        map(definition("PARAMETER"), Statement::Parameter),
        map(definition("PRE_AGGREGATION"), Statement::PreAggregation),
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
                    if pos == 0 {
                        return Err(nom::Err::Error(NomError::new(remaining, ErrorKind::Tag)));
                    }
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
    let mut pre_aggregations = Vec::new();

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
            Statement::Parameter(_) => {}
            Statement::PreAggregation(props) => {
                if let Some(preagg) = build_pre_aggregation(&props) {
                    pre_aggregations.push(preagg);
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
    model.pre_aggregations.extend(pre_aggregations);

    Ok(model)
}

/// Parse SQL into statement blocks preserving high-level statement kinds/properties.
pub fn parse_sql_statement_blocks(sql: &str) -> Result<Vec<SqlStatementBlock>> {
    let (_, statements) =
        parse_file(sql).map_err(|e| SidemanticError::Validation(format!("Parse error: {e}")))?;

    let mut blocks = Vec::with_capacity(statements.len());

    for stmt in statements {
        let (kind, properties) = match stmt {
            Statement::Model(props) => ("model".to_string(), props),
            Statement::Dimension(props) => ("dimension".to_string(), props),
            Statement::Metric(props) => ("metric".to_string(), props),
            Statement::Segment(props) => ("segment".to_string(), props),
            Statement::Relationship(props) => ("relationship".to_string(), props),
            Statement::Parameter(props) => ("parameter".to_string(), props),
            Statement::PreAggregation(props) => ("pre_aggregation".to_string(), props),
        };

        blocks.push(SqlStatementBlock { kind, properties });
    }

    Ok(blocks)
}

/// Parse SQL definitions for metrics and segments only
pub fn parse_sql_definitions(sql: &str) -> Result<(Vec<Metric>, Vec<Segment>)> {
    let (metrics, segments, _) = parse_sql_graph_definitions(sql)?;
    Ok((metrics, segments))
}

/// Parse SQL definitions for graph-level definitions (metrics, segments, parameters)
pub fn parse_sql_graph_definitions(
    sql: &str,
) -> Result<(Vec<Metric>, Vec<Segment>, Vec<Parameter>)> {
    let (metrics, segments, parameters, _) = parse_sql_graph_definitions_extended(sql)?;
    Ok((metrics, segments, parameters))
}

/// Parse SQL definitions for graph-level definitions including pre-aggregations.
pub fn parse_sql_graph_definitions_extended(sql: &str) -> Result<SqlGraphDefinitionParts> {
    let (_, statements) =
        parse_file(sql).map_err(|e| SidemanticError::Validation(format!("Parse error: {e}")))?;

    let mut metrics = Vec::new();
    let mut segments = Vec::new();
    let mut parameters = Vec::new();
    let mut pre_aggregations = Vec::new();

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
            Statement::Parameter(props) => {
                if let Some(parameter) = build_parameter(&props) {
                    parameters.push(parameter);
                }
            }
            Statement::PreAggregation(props) => {
                if let Some(pre_aggregation) = build_pre_aggregation(&props) {
                    pre_aggregations.push(pre_aggregation);
                }
            }
            _ => {}
        }
    }

    Ok((metrics, segments, parameters, pre_aggregations))
}

// ============================================================================
// Builders
// ============================================================================

fn build_model(props: &HashMap<String, String>) -> Result<Model> {
    let name = props
        .get("name")
        .ok_or_else(|| SidemanticError::Validation("MODEL requires 'name' property".into()))?;

    let primary_key_columns =
        parse_key_columns(props, "primary_key").unwrap_or_else(|| vec!["id".to_string()]);
    let mut model = Model::new(
        name,
        primary_key_columns
            .first()
            .map(|s| s.as_str())
            .unwrap_or("id"),
    )
    .with_primary_key_columns(primary_key_columns);

    if let Some(table) = props.get("table") {
        model.table = Some(table.clone());
    }
    if let Some(sql) = props.get("sql") {
        model.sql = Some(sql.clone());
    }
    if let Some(source_uri) = props.get("source_uri") {
        model.source_uri = Some(source_uri.clone());
    }
    if let Some(extends) = props.get("extends") {
        model.extends = Some(extends.clone());
    }
    if let Some(desc) = props.get("description") {
        model.description = Some(desc.clone());
    }
    if let Some(label) = props.get("label") {
        model.label = Some(label.clone());
    }
    if let Some(default_time_dimension) = props.get("default_time_dimension") {
        model.default_time_dimension = Some(default_time_dimension.clone());
    }
    if let Some(default_grain) = props.get("default_grain") {
        model.default_grain = Some(default_grain.clone());
    }
    if let Some(unique_keys) = props.get("unique_keys") {
        let parsed = parse_literal(unique_keys);
        if let serde_json::Value::Array(groups) = parsed {
            let normalized = groups
                .into_iter()
                .filter_map(|group| match group {
                    serde_json::Value::Array(columns) => Some(
                        columns
                            .into_iter()
                            .filter_map(|column| json_value_to_string(&column))
                            .collect::<Vec<_>>(),
                    ),
                    _ => None,
                })
                .collect::<Vec<_>>();
            if !normalized.is_empty() {
                model.unique_keys = Some(normalized);
            }
        }
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
    if let Some(granularity) = props.get("granularity") {
        dim.granularity = Some(granularity.clone());
    }
    if let Some(supported_granularities) = props.get("supported_granularities") {
        let parsed = parse_literal(supported_granularities);
        let values = json_value_to_string_list(parsed);
        if !values.is_empty() {
            dim.supported_granularities = Some(values);
        }
    }
    if let Some(desc) = props.get("description") {
        dim.description = Some(desc.clone());
    }
    if let Some(label) = props.get("label") {
        dim.label = Some(label.clone());
    }
    if let Some(format) = props.get("format") {
        dim.format = Some(format.clone());
    }
    if let Some(value_format_name) = props.get("value_format_name") {
        dim.value_format_name = Some(value_format_name.clone());
    }
    if let Some(parent) = props.get("parent") {
        dim.parent = Some(parent.clone());
    }

    Some(dim)
}

fn build_metric(props: &HashMap<String, String>) -> Option<Metric> {
    let name = props.get("name")?;

    let mut metric = Metric::new(name);
    metric.agg = None;
    metric.r#type = parse_metric_type(props.get("type"));
    metric.sql = props.get("sql").cloned();
    metric.numerator = props.get("numerator").cloned();
    metric.denominator = props.get("denominator").cloned();
    metric.offset_window = props.get("offset_window").cloned();
    metric.window = props.get("window").cloned();
    metric.grain_to_date = parse_time_grain(props.get("grain_to_date"));
    metric.window_expression = props.get("window_expression").cloned();
    metric.window_frame = props.get("window_frame").cloned();
    metric.window_order = props.get("window_order").cloned();
    metric.base_metric = props.get("base_metric").cloned();
    metric.comparison_type = parse_comparison_type(props.get("comparison_type"));
    metric.time_offset = props.get("time_offset").cloned();
    metric.calculation = parse_comparison_calculation(props.get("calculation"));
    metric.entity = props.get("entity").cloned();
    metric.base_event = props.get("base_event").cloned();
    metric.conversion_event = props.get("conversion_event").cloned();
    metric.conversion_window = props.get("conversion_window").cloned();
    metric.description = props.get("description").cloned();
    metric.label = props.get("label").cloned();
    metric.format = props.get("format").cloned();
    metric.value_format_name = props.get("value_format_name").cloned();
    metric.non_additive_dimension = props.get("non_additive_dimension").cloned();

    if let Some(fill_nulls_with) = props.get("fill_nulls_with") {
        let parsed = parse_literal(fill_nulls_with);
        if !parsed.is_null() {
            metric.fill_nulls_with = Some(parsed);
        }
    }

    if let Some(filters) = props.get("filters") {
        metric.filters = json_value_to_string_list(parse_literal(filters));
    }
    if let Some(drill_fields) = props.get("drill_fields") {
        metric.drill_fields = Some(json_value_to_string_list(parse_literal(drill_fields)));
    }

    metric.agg = parse_metric_aggregation(props.get("agg"));

    if metric.agg.is_none()
        && matches!(
            metric.r#type,
            MetricType::Simple | MetricType::Cumulative | MetricType::Derived
        )
    {
        if let Some(sql) = metric.sql.as_deref() {
            if let Some((agg, inner_expr)) = extract_aggregation_with_polyglot(sql) {
                metric.agg = parse_metric_aggregation(Some(&agg));
                metric.sql = if inner_expr.is_empty() {
                    Some("*".to_string())
                } else {
                    Some(inner_expr)
                };
            }
        }
    }

    if matches!(metric.r#type, MetricType::Simple)
        && metric.numerator.is_some()
        && metric.denominator.is_some()
    {
        metric.r#type = MetricType::Ratio;
    } else if matches!(metric.r#type, MetricType::Simple)
        && metric
            .sql
            .as_ref()
            .map(|s| s.contains('{'))
            .unwrap_or(false)
    {
        metric.r#type = MetricType::Derived;
    }

    if !matches!(metric.r#type, MetricType::Simple) {
        metric.agg = None;
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

    let foreign_key_columns = parse_key_columns(props, "foreign_key");
    let primary_key_columns = parse_key_columns(props, "primary_key");

    Some(Relationship {
        name: name.clone(),
        r#type: rtype,
        foreign_key: foreign_key_columns
            .as_ref()
            .and_then(|columns| columns.first().cloned()),
        foreign_key_columns,
        primary_key: primary_key_columns
            .as_ref()
            .and_then(|columns| columns.first().cloned()),
        primary_key_columns,
        through: props.get("through").cloned(),
        through_foreign_key: props.get("through_foreign_key").cloned(),
        related_foreign_key: props.get("related_foreign_key").cloned(),
        sql: props.get("sql").cloned(),
    })
}

fn build_pre_aggregation(props: &HashMap<String, String>) -> Option<PreAggregation> {
    let name = props.get("name")?;

    let preagg_type = match props.get("type").map(|t| t.to_lowercase()) {
        Some(kind) if kind == "original_sql" => PreAggregationType::OriginalSql,
        Some(kind) if kind == "rollup_join" => PreAggregationType::RollupJoin,
        Some(kind) if kind == "lambda" => PreAggregationType::Lambda,
        _ => PreAggregationType::Rollup,
    };

    let measures = props
        .get("measures")
        .map(|value| json_value_to_string_list(parse_literal(value)));
    let dimensions = props
        .get("dimensions")
        .map(|value| json_value_to_string_list(parse_literal(value)));

    let scheduled_refresh = props
        .get("scheduled_refresh")
        .map(|value| match parse_literal(value) {
            serde_json::Value::Bool(v) => v,
            serde_json::Value::String(v) => v.eq_ignore_ascii_case("true"),
            _ => true,
        })
        .unwrap_or(true);

    let refresh_key = props
        .get("refresh_key")
        .and_then(|value| match parse_literal(value) {
            serde_json::Value::Object(map) => Some(RefreshKey {
                every: map.get("every").and_then(json_value_to_string),
                sql: map.get("sql").and_then(json_value_to_string),
                incremental: map
                    .get("incremental")
                    .and_then(|v| match v {
                        serde_json::Value::Bool(b) => Some(*b),
                        serde_json::Value::String(s) => Some(s.eq_ignore_ascii_case("true")),
                        _ => None,
                    })
                    .unwrap_or(false),
                update_window: map.get("update_window").and_then(json_value_to_string),
            }),
            _ => None,
        });

    let indexes = props
        .get("indexes")
        .and_then(|value| match parse_literal(value) {
            serde_json::Value::Array(items) => {
                let parsed = items
                    .into_iter()
                    .filter_map(|item| match item {
                        serde_json::Value::Object(map) => {
                            let name = map.get("name").and_then(json_value_to_string)?;
                            let columns = map
                                .get("columns")
                                .cloned()
                                .map(json_value_to_string_list)
                                .unwrap_or_default();
                            let index_type = map
                                .get("type")
                                .and_then(json_value_to_string)
                                .unwrap_or_else(|| "regular".to_string());
                            Some(Index {
                                name,
                                columns,
                                index_type,
                            })
                        }
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                Some(parsed)
            }
            _ => None,
        });

    Some(PreAggregation {
        name: name.clone(),
        preagg_type,
        measures,
        dimensions,
        time_dimension: props.get("time_dimension").cloned(),
        granularity: props.get("granularity").cloned(),
        partition_granularity: props.get("partition_granularity").cloned(),
        build_range_start: props.get("build_range_start").cloned(),
        build_range_end: props.get("build_range_end").cloned(),
        scheduled_refresh,
        refresh_key,
        indexes,
    })
}

fn build_parameter(props: &HashMap<String, String>) -> Option<Parameter> {
    let name = props.get("name")?;
    let parameter_type = parse_parameter_type(props.get("type"))?;

    let default_value = props.get("default_value").map(|value| parse_literal(value));

    let allowed_values = props
        .get("allowed_values")
        .map(|value| match parse_literal(value) {
            serde_json::Value::Array(items) => items,
            other => vec![other],
        });

    let default_to_today = props
        .get("default_to_today")
        .map(|value| match parse_literal(value) {
            serde_json::Value::Bool(v) => v,
            serde_json::Value::String(v) => v.eq_ignore_ascii_case("true"),
            _ => false,
        })
        .unwrap_or(false);

    Some(Parameter {
        name: name.clone(),
        parameter_type,
        description: props.get("description").cloned(),
        label: props.get("label").cloned(),
        default_value,
        allowed_values,
        default_to_today,
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
        assert_eq!(revenue.agg, Some(Aggregation::Sum));
        assert_eq!(revenue.sql, Some("amount".to_string()));
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

    #[test]
    fn test_parse_sql_graph_definitions_with_parameter() {
        let sql = r#"
            METRIC (name revenue, agg sum, sql amount);
            SEGMENT (name completed, sql status = 'completed');
            PARAMETER (
                name region,
                type string,
                allowed_values [us, eu],
                default_value 'us',
                default_to_today false
            );
        "#;

        let (metrics, segments, parameters) = parse_sql_graph_definitions(sql).unwrap();
        assert_eq!(metrics.len(), 1);
        assert_eq!(segments.len(), 1);
        assert_eq!(parameters.len(), 1);

        let parameter = &parameters[0];
        assert_eq!(parameter.name, "region");
        assert_eq!(parameter.parameter_type, ParameterType::String);
        assert_eq!(
            parameter.allowed_values,
            Some(vec![
                serde_json::Value::String("us".to_string()),
                serde_json::Value::String("eu".to_string())
            ])
        );
        assert_eq!(
            parameter.default_value,
            Some(serde_json::Value::String("us".to_string()))
        );
    }

    #[test]
    fn test_parse_model_with_extended_metadata_fields() {
        let sql = r#"
            MODEL (
                name orders,
                table orders,
                source_uri s3://warehouse/orders,
                extends base_orders,
                default_time_dimension order_date,
                default_grain day
            );
            DIMENSION (
                name order_date,
                type time,
                sql order_date,
                supported_granularities [day, week, month],
                format yyyy-mm-dd,
                value_format_name iso_date
            );
        "#;

        let model = parse_sql_model(sql).unwrap();
        assert_eq!(model.source_uri.as_deref(), Some("s3://warehouse/orders"));
        assert_eq!(model.extends.as_deref(), Some("base_orders"));
        assert_eq!(model.default_time_dimension.as_deref(), Some("order_date"));
        assert_eq!(model.default_grain.as_deref(), Some("day"));

        let order_date = model.get_dimension("order_date").unwrap();
        assert_eq!(
            order_date.supported_granularities,
            Some(vec![
                "day".to_string(),
                "week".to_string(),
                "month".to_string()
            ])
        );
        assert_eq!(order_date.format.as_deref(), Some("yyyy-mm-dd"));
        assert_eq!(order_date.value_format_name.as_deref(), Some("iso_date"));
    }

    #[test]
    fn test_parse_model_with_composite_primary_key() {
        let sql = r#"
            MODEL (
                name order_items,
                table order_items,
                primary_key [order_id, item_id]
            );
        "#;

        let model = parse_sql_model(sql).unwrap();
        assert_eq!(model.primary_key, "order_id");
        assert_eq!(
            model.primary_key_columns,
            vec!["order_id".to_string(), "item_id".to_string()]
        );
    }

    #[test]
    fn test_parse_relationship_with_composite_keys() {
        let sql = r#"
            MODEL (name shipments, table shipments);
            RELATIONSHIP (
                name order_items,
                type many_to_one,
                foreign_key [order_id, item_id],
                primary_key [order_id, item_id]
            );
        "#;

        let model = parse_sql_model(sql).unwrap();
        let rel = model.get_relationship("order_items").unwrap();
        assert_eq!(rel.foreign_key.as_deref(), Some("order_id"));
        assert_eq!(
            rel.foreign_key_columns.as_ref().unwrap(),
            &vec!["order_id".to_string(), "item_id".to_string()]
        );
        assert_eq!(rel.primary_key.as_deref(), Some("order_id"));
        assert_eq!(
            rel.primary_key_columns.as_ref().unwrap(),
            &vec!["order_id".to_string(), "item_id".to_string()]
        );
    }

    #[test]
    fn test_parse_sql_statement_blocks() {
        let sql = r#"
            MODEL (name orders, table orders);
            METRIC (name revenue, expression SUM(amount));
            SEGMENT (name completed, sql status = 'completed');
        "#;

        let blocks = parse_sql_statement_blocks(sql).unwrap();
        assert_eq!(blocks.len(), 3);

        assert_eq!(blocks[0].kind, "model");
        assert_eq!(
            blocks[0].properties.get("name"),
            Some(&"orders".to_string())
        );

        assert_eq!(blocks[1].kind, "metric");
        assert_eq!(
            blocks[1].properties.get("name"),
            Some(&"revenue".to_string())
        );
        // Alias keys are resolved in rust parser payloads.
        assert_eq!(
            blocks[1].properties.get("sql"),
            Some(&"SUM(amount)".to_string())
        );

        assert_eq!(blocks[2].kind, "segment");
        assert_eq!(
            blocks[2].properties.get("sql"),
            Some(&"status = 'completed'".to_string())
        );
    }
}
