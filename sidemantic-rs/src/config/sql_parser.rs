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

use std::collections::{HashMap, HashSet};

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

#[cfg(not(target_arch = "wasm32"))]
use polyglot_sql::parse as polyglot_parse;
use polyglot_sql::{DialectType, Expression};
use regex::Regex;

use crate::core::{
    Aggregation, CohortInnerMetric, ComparisonCalculation, ComparisonType, Dimension,
    DimensionType, Index, Metric, MetricType, Model, Parameter, ParameterType, PreAggregation,
    PreAggregationType, RefreshKey, Relationship, RelationshipType, Segment, TimeGrain,
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
        Some(metric_type) if metric_type == "retention" => MetricType::Retention,
        Some(metric_type) if metric_type == "cohort" => MetricType::Cohort,
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
        "stddev" => Some(Aggregation::Stddev),
        "stddev_pop" => Some(Aggregation::StddevPop),
        "variance" => Some(Aggregation::Variance),
        "variance_pop" | "var_pop" => Some(Aggregation::VariancePop),
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

/// Parse simple SEGMENT: SEGMENT name AS expr
fn simple_segment(input: &str) -> IResult<&str, Statement> {
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("SEGMENT")(input)?;
    let (input, _) = multispace1(input)?;

    let (input, name) = recognize(pair(identifier, opt(pair(char('.'), identifier))))(input)?;

    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("AS")(input)?;
    let (input, _) = multispace1(input)?;

    let (input, expr) = take_while(|c| c != ';')(input)?;
    let (input, _) = opt(char(';'))(input)?;

    let mut props = HashMap::new();
    props.insert("name".to_string(), name.trim().to_string());
    props.insert("sql".to_string(), expr.trim().to_string());

    Ok((input, Statement::Segment(props)))
}

/// Parse metric expression to extract aggregation function
fn parse_metric_expression(name: &str, expr: &str) -> HashMap<String, String> {
    let mut props = HashMap::new();
    props.insert("name".to_string(), name.to_string());

    if let Some((agg, inner_expr)) = parse_top_level_function_metric(expr) {
        props.insert("agg".to_string(), agg);
        if !inner_expr.is_empty() {
            props.insert("sql".to_string(), inner_expr);
        }
        return props;
    }

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

fn parse_top_level_function_metric(expr: &str) -> Option<(String, String)> {
    let trimmed = expr.trim();
    let (rest, function_name) = identifier(trimmed).ok()?;
    let rest = rest.trim_start().strip_prefix('(')?;
    let (inner_expr, rest) = parse_balanced_parens(rest)?;

    if !rest.trim().is_empty() {
        return None;
    }

    let function_name = function_name.to_lowercase();
    let mut inner_expr = inner_expr.trim().to_string();

    let agg = match function_name.as_str() {
        "sum" => "sum",
        "avg" | "average" => "avg",
        "min" => "min",
        "max" => "max",
        "median" => "median",
        "stddev" => "stddev",
        "stddev_pop" => "stddev_pop",
        "variance" => "variance",
        "variance_pop" | "var_pop" => "variance_pop",
        "count_distinct" | "countdistinct" => "count_distinct",
        "count" => {
            if inner_expr == "*" {
                inner_expr.clear();
                "count"
            } else if let Some(distinct_inner) = strip_distinct_prefix(&inner_expr) {
                inner_expr = distinct_inner.to_string();
                "count_distinct"
            } else {
                "count"
            }
        }
        _ => return Some(("expression".to_string(), trimmed.to_string())),
    };

    Some((agg.to_string(), inner_expr))
}

fn parse_balanced_parens(input: &str) -> Option<(&str, &str)> {
    let mut depth = 0usize;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut chars = input.char_indices().peekable();

    while let Some((idx, ch)) = chars.next() {
        if in_single_quote {
            if ch == '\'' {
                if matches!(chars.peek(), Some((_, '\''))) {
                    chars.next();
                } else {
                    in_single_quote = false;
                }
            }
            continue;
        }

        if in_double_quote {
            if ch == '"' {
                in_double_quote = false;
            }
            continue;
        }

        match ch {
            '\'' => in_single_quote = true,
            '"' => in_double_quote = true,
            '(' => depth += 1,
            ')' if depth == 0 => return Some((&input[..idx], &input[idx + ch.len_utf8()..])),
            ')' => depth -= 1,
            _ => {}
        }
    }

    None
}

fn strip_distinct_prefix(expr: &str) -> Option<&str> {
    let trimmed = expr.trim_start();
    let prefix = trimmed.get(..8)?;

    if prefix.eq_ignore_ascii_case("distinct") {
        let rest = trimmed.get(8..)?;
        if !rest.chars().next()?.is_whitespace() {
            return None;
        }
        let rest = rest.trim_start();
        if !rest.is_empty() {
            return Some(rest);
        }
    }

    None
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
        "median" => "median",
        "stddev" => "stddev",
        "stddev_pop" => "stddev_pop",
        "variance" => "variance",
        "variance_pop" | "var_pop" => "variance_pop",
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
        _ => generate_expr_str(expr),
    }
}

fn generate_expr_str(expr: &Expression) -> String {
    match expr {
        Expression::Column(_)
        | Expression::Identifier(_)
        | Expression::Star(_)
        | Expression::Literal(_) => expr.to_string(),
        _ => {
            polyglot_sql::generate(expr, DialectType::Generic).unwrap_or_else(|_| expr.to_string())
        }
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

/// Parse SEGMENT with model prefix: SEGMENT model.name (props)
fn prefixed_segment(input: &str) -> IResult<&str, Statement> {
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("SEGMENT")(input)?;
    let (input, _) = multispace1(input)?;

    let (input, model) = identifier(input)?;
    let (input, _) = char('.')(input)?;
    let (input, name) = identifier(input)?;

    let (input, _) = multispace0(input)?;
    let (input, props) = delimited(char('('), property_list, char(')'))(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = opt(char(';'))(input)?;

    let mut props = props;
    props.insert("name".to_string(), format!("{model}.{name}"));
    Ok((input, Statement::Segment(props)))
}

/// Parse any statement (tries simple AS syntax first, then parenthesized)
fn statement(input: &str) -> IResult<&str, Statement> {
    let (input, _) = multispace0(input)?;

    alt((
        map(definition("MODEL"), Statement::Model),
        // Try simple AS syntax first for METRIC and DIMENSION
        simple_metric,
        simple_dimension,
        simple_segment,
        // Try model.name (props) syntax
        prefixed_metric,
        prefixed_dimension,
        prefixed_segment,
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

fn has_compact_model_syntax(sql: &str) -> bool {
    Regex::new(r"(?i)\bmodel\s+[A-Za-z_][A-Za-z0-9_]*\s+from\b")
        .expect("valid compact model regex")
        .is_match(sql)
}

fn parse_legacy_sql_models_from_statements(statements: Vec<Statement>) -> Result<Vec<Model>> {
    let mut models = Vec::new();
    let mut current_model: Option<Model> = None;
    let mut dimensions = Vec::new();
    let mut metrics = Vec::new();
    let mut segments = Vec::new();
    let mut relationships = Vec::new();
    let mut pre_aggregations = Vec::new();

    for stmt in statements {
        match stmt {
            Statement::Model(props) => {
                flush_legacy_sql_model(
                    &mut models,
                    &mut current_model,
                    &mut dimensions,
                    &mut metrics,
                    &mut segments,
                    &mut relationships,
                    &mut pre_aggregations,
                );
                current_model = Some(build_model(&props)?);
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

    flush_legacy_sql_model(
        &mut models,
        &mut current_model,
        &mut dimensions,
        &mut metrics,
        &mut segments,
        &mut relationships,
        &mut pre_aggregations,
    );

    if models.is_empty() {
        return Err(SidemanticError::Validation(
            "SQL definitions must include a MODEL statement".into(),
        ));
    }

    Ok(models)
}

#[allow(clippy::too_many_arguments)]
fn flush_legacy_sql_model(
    models: &mut Vec<Model>,
    current_model: &mut Option<Model>,
    dimensions: &mut Vec<Dimension>,
    metrics: &mut Vec<Metric>,
    segments: &mut Vec<Segment>,
    relationships: &mut Vec<Relationship>,
    pre_aggregations: &mut Vec<PreAggregation>,
) {
    let Some(mut model) = current_model.take() else {
        dimensions.clear();
        metrics.clear();
        segments.clear();
        relationships.clear();
        pre_aggregations.clear();
        return;
    };

    model.dimensions.append(dimensions);
    model.metrics.append(metrics);
    model.segments.append(segments);
    model.relationships.append(relationships);
    model.pre_aggregations.append(pre_aggregations);
    models.push(model);
}

fn strip_line_comment(line: &str) -> &str {
    line.split_once("--")
        .map(|(before, _)| before)
        .unwrap_or(line)
}

fn split_annotation(line: &str) -> (&str, Option<&str>) {
    let Some(idx) = line.rfind(" : ") else {
        return (line, None);
    };
    (&line[..idx], Some(line[idx + 3..].trim()))
}

fn split_field_alias(line: &str) -> (String, String) {
    let lower = line.to_ascii_lowercase();
    if let Some(idx) = lower.rfind(" as ") {
        let expr = line[..idx].trim().to_string();
        let name = line[idx + 4..].trim().to_string();
        (expr, name)
    } else {
        let name = line.trim().to_string();
        (name.clone(), name)
    }
}

fn split_columns(value: &str) -> Result<Vec<String>> {
    let columns = value
        .split(',')
        .map(str::trim)
        .filter(|column| !column.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    if columns.is_empty() {
        return Err(SidemanticError::Validation(
            "Primary key requires at least one column".to_string(),
        ));
    }
    Ok(columns)
}

fn parse_compact_annotation(annotation: Option<&str>) -> Result<(Option<String>, Option<String>)> {
    let Some(annotation) = annotation else {
        return Ok((None, None));
    };

    let mut dim_type = None;
    let mut granularity = None;
    let mut parts = annotation.split_whitespace().peekable();
    while let Some(part) = parts.next() {
        if part.eq_ignore_ascii_case("grain") {
            let Some(grain) = parts.next() else {
                return Err(SidemanticError::Validation(
                    "field annotation grain requires a value".to_string(),
                ));
            };
            granularity = Some(grain.to_ascii_lowercase());
        } else if dim_type.is_none() {
            dim_type = Some(part.to_ascii_lowercase());
        } else {
            return Err(SidemanticError::Validation(format!(
                "Unrecognized field annotation '{annotation}'"
            )));
        }
    }

    if granularity.is_some()
        && dim_type
            .as_deref()
            .is_some_and(|value| value != "time" && value != "timestamp" && value != "date")
    {
        return Err(SidemanticError::Validation(format!(
            "field annotation cannot use grain with type '{}'",
            dim_type.unwrap_or_default()
        )));
    }

    Ok((dim_type, granularity))
}

fn compact_relationship_type(kind: &str) -> Result<RelationshipType> {
    match kind.to_ascii_lowercase().as_str() {
        "one" => Ok(RelationshipType::ManyToOne),
        "many" => Ok(RelationshipType::OneToMany),
        "one_to_one" => Ok(RelationshipType::OneToOne),
        "many_to_one" => Ok(RelationshipType::ManyToOne),
        "one_to_many" => Ok(RelationshipType::OneToMany),
        "many_to_many" => Ok(RelationshipType::ManyToMany),
        _ => Err(SidemanticError::Validation(format!(
            "unsupported compact join relationship type '{kind}'"
        ))),
    }
}

fn parse_compact_join(line: &str) -> Result<Relationship> {
    let join_re = Regex::new(r"(?i)^join\s+(\w+)\s+([A-Za-z_][A-Za-z0-9_]*)\s+on\s+(.+)$")
        .expect("valid compact join regex");
    let captures = join_re.captures(line).ok_or_else(|| {
        SidemanticError::Validation(format!("Unrecognized compact join statement: {line}"))
    })?;
    let rel_type = compact_relationship_type(&captures[1])?;
    let target_model = captures[2].to_string();
    let mut predicate = captures[3].trim();
    if predicate.starts_with('(') && predicate.ends_with(')') {
        predicate = predicate[1..predicate.len() - 1].trim();
    }

    let mut local_keys = Vec::new();
    let mut target_keys = Vec::new();
    for part in Regex::new(r"(?i)\s+and\s+")
        .expect("valid and regex")
        .split(predicate)
    {
        let Some((left, right)) = part.split_once('=') else {
            return Err(SidemanticError::Validation(format!(
                "compact join '{line}' must compare model columns"
            )));
        };
        let left = left.trim().trim_matches(|c| c == '(' || c == ')').trim();
        let right = right.trim().trim_matches(|c| c == '(' || c == ')').trim();
        let Some((right_model, right_col)) = right.split_once('.') else {
            return Err(SidemanticError::Validation(format!(
                "compact join '{line}' must compare model columns"
            )));
        };
        if right_model.trim() != target_model {
            return Err(SidemanticError::Validation(format!(
                "compact join '{line}' must compare columns from target model '{target_model}'"
            )));
        }
        if left.contains('.') {
            return Err(SidemanticError::Validation(format!(
                "compact join '{line}' must use local columns on the left side"
            )));
        }
        local_keys.push(left.to_string());
        target_keys.push(right_col.trim().to_string());
    }

    if local_keys.is_empty() {
        return Err(SidemanticError::Validation(format!(
            "compact join '{line}' must compare model columns"
        )));
    }

    let (foreign_keys, primary_keys) = match rel_type {
        RelationshipType::ManyToOne | RelationshipType::OneToOne => (local_keys, target_keys),
        _ => (target_keys, local_keys),
    };

    let mut rel = Relationship::new(target_model);
    rel.r#type = rel_type;
    Ok(rel.with_key_columns(foreign_keys, primary_keys))
}

fn infer_compact_dimension_type(name: &str, expression: &str) -> String {
    let lowered_name = name.to_ascii_lowercase();
    let lowered_expression = expression.to_ascii_lowercase();
    if lowered_name.contains("date")
        || lowered_name.contains("time")
        || lowered_name.ends_with("_at")
        || lowered_expression.contains("date_trunc")
        || lowered_expression.contains("timestamp")
        || lowered_expression.contains("::date")
    {
        "time".to_string()
    } else if lowered_expression.contains(" = ")
        || lowered_expression.contains(" != ")
        || lowered_expression.contains(" <> ")
        || lowered_expression.contains(" > ")
        || lowered_expression.contains(" < ")
    {
        "boolean".to_string()
    } else if [" + ", " - ", " * ", " / "]
        .iter()
        .any(|operator| lowered_expression.contains(operator))
    {
        "numeric".to_string()
    } else {
        infer_dimension_type(expression)
    }
}

fn compact_expression_references_metrics(expression: &str, metric_names: &HashSet<String>) -> bool {
    if metric_names.is_empty() {
        return false;
    }
    let tokens = Regex::new(r"\b[A-Za-z_][A-Za-z0-9_]*\b")
        .expect("valid identifier regex")
        .find_iter(expression)
        .map(|m| m.as_str())
        .collect::<HashSet<_>>();
    metric_names
        .iter()
        .any(|name| tokens.contains(name.as_str()))
}

type CompactFieldDeclaration = (usize, String, String, Option<String>, Option<String>);

fn build_compact_model(
    name: String,
    table: Option<String>,
    source_sql: Option<String>,
    body: &str,
) -> Result<Model> {
    let mut model = Model::new(&name, "id");
    model.table = table;
    model.sql = source_sql;

    let mut field_declarations: Vec<CompactFieldDeclaration> = Vec::new();
    let mut metric_names = HashSet::new();
    let mut seen_fields = HashSet::new();
    let mut seen_segments = HashSet::new();

    for (idx, raw_line) in body.lines().enumerate() {
        let line = strip_line_comment(raw_line).trim();
        if line.is_empty() {
            continue;
        }

        let lower = line.to_ascii_lowercase();
        if lower.starts_with("primary key") {
            let open = line.find('(').ok_or_else(|| {
                SidemanticError::Validation("Primary key requires column list".to_string())
            })?;
            let close = line.rfind(')').ok_or_else(|| {
                SidemanticError::Validation("Primary key requires column list".to_string())
            })?;
            if close <= open {
                return Err(SidemanticError::Validation(
                    "Primary key requires at least one column".to_string(),
                ));
            }
            let columns = split_columns(&line[open + 1..close])?;
            model.primary_key = columns[0].clone();
            model.primary_key_columns = columns;
            continue;
        }

        if lower.starts_with("default time ") {
            let rest = line["default time ".len()..].trim();
            let mut parts = rest.split_whitespace();
            let Some(dimension) = parts.next() else {
                return Err(SidemanticError::Validation(
                    "default time requires a dimension".to_string(),
                ));
            };
            model.default_time_dimension = Some(dimension.to_string());
            if parts
                .next()
                .is_some_and(|part| part.eq_ignore_ascii_case("grain"))
            {
                if let Some(grain) = parts.next() {
                    model.default_grain = Some(grain.to_ascii_lowercase());
                }
            }
            continue;
        }

        if lower.starts_with("segment ") {
            let rest = line["segment ".len()..].trim();
            let lower_rest = rest.to_ascii_lowercase();
            let Some(as_idx) = lower_rest.find(" as ") else {
                return Err(SidemanticError::Validation(format!(
                    "Unrecognized compact segment statement: {line}"
                )));
            };
            let segment_name = rest[..as_idx].trim();
            let segment_sql = rest[as_idx + 4..].trim();
            if !seen_segments.insert(segment_name.to_string()) {
                return Err(SidemanticError::Validation(format!(
                    "Model '{name}' defines segment '{segment_name}' more than once"
                )));
            }
            model
                .segments
                .push(Segment::new(segment_name, segment_sql.to_string()));
            continue;
        }

        if lower.starts_with("join ") {
            model.relationships.push(parse_compact_join(line)?);
            continue;
        }

        if lower.starts_with("table ") {
            return Err(SidemanticError::Validation(format!(
                "compact model '{name}' must use `model {name} from <table>` instead of a table statement"
            )));
        }

        let (field_line, annotation) = split_annotation(line);
        let (dimension_type, granularity) = parse_compact_annotation(annotation)?;
        let (expression, field_name) = split_field_alias(field_line);
        if field_name.is_empty() {
            return Err(SidemanticError::Validation(format!(
                "Unrecognized statement in model '{name}': {line}"
            )));
        }
        if !seen_fields.insert(field_name.clone()) {
            return Err(SidemanticError::Validation(format!(
                "Model '{name}' defines field '{field_name}' more than once"
            )));
        }
        field_declarations.push((idx, field_name, expression, dimension_type, granularity));
    }

    let mut pending = Vec::new();
    let mut parsed_fields: Vec<(usize, bool, Dimension, Option<Metric>)> = Vec::new();
    for (idx, field_name, expression, dimension_type, granularity) in field_declarations {
        let metric_props = parse_metric_expression(&field_name, &expression);
        if metric_props
            .get("agg")
            .is_some_and(|agg| agg != "expression")
        {
            if dimension_type.is_some() {
                return Err(SidemanticError::Validation(format!(
                    "Field '{field_name}' in model '{name}' is a metric and cannot use dimension annotation"
                )));
            }
            let metric = build_metric(&metric_props).ok_or_else(|| {
                SidemanticError::Validation(format!(
                    "failed to build compact metric '{field_name}'"
                ))
            })?;
            metric_names.insert(field_name);
            parsed_fields.push((idx, false, Dimension::new("__unused"), Some(metric)));
        } else {
            pending.push((idx, field_name, expression, dimension_type, granularity));
        }
    }

    let mut remaining = Vec::new();
    for (idx, field_name, expression, dimension_type, granularity) in pending {
        if compact_expression_references_metrics(&expression, &metric_names) {
            if dimension_type.is_some() {
                return Err(SidemanticError::Validation(format!(
                    "Field '{field_name}' in model '{name}' is a metric and cannot use dimension annotation"
                )));
            }
            let mut props = HashMap::new();
            props.insert("name".to_string(), field_name.clone());
            props.insert("type".to_string(), "derived".to_string());
            props.insert("sql".to_string(), expression);
            let metric = build_metric(&props).ok_or_else(|| {
                SidemanticError::Validation(format!(
                    "failed to build compact metric '{field_name}'"
                ))
            })?;
            metric_names.insert(field_name);
            parsed_fields.push((idx, false, Dimension::new("__unused"), Some(metric)));
        } else {
            remaining.push((idx, field_name, expression, dimension_type, granularity));
        }
    }

    for (idx, field_name, expression, dimension_type, granularity) in remaining {
        let mut props = HashMap::new();
        props.insert("name".to_string(), field_name.clone());
        props.insert(
            "type".to_string(),
            dimension_type
                .unwrap_or_else(|| infer_compact_dimension_type(&field_name, &expression)),
        );
        if expression != field_name {
            props.insert("sql".to_string(), expression);
        }
        if let Some(granularity) = granularity {
            props.insert("granularity".to_string(), granularity);
        }
        let dimension = build_dimension(&props).ok_or_else(|| {
            SidemanticError::Validation(format!("failed to build compact dimension '{field_name}'"))
        })?;
        parsed_fields.push((idx, true, dimension, None));
    }

    parsed_fields.sort_by_key(|(idx, _, _, _)| *idx);
    for (_, is_dimension, dimension, metric) in parsed_fields {
        if is_dimension {
            model.dimensions.push(dimension);
        } else if let Some(metric) = metric {
            model.metrics.push(metric);
        }
    }

    if let Some(default_time) = model.default_time_dimension.as_ref() {
        let Some(dimension) = model.get_dimension(default_time) else {
            return Err(SidemanticError::Validation(format!(
                "Default time dimension '{default_time}' in model '{name}' is not defined"
            )));
        };
        if dimension.r#type != DimensionType::Time {
            return Err(SidemanticError::Validation(format!(
                "Default time dimension '{default_time}' in model '{name}' must be a time dimension"
            )));
        }
    }

    Ok(model)
}

fn parse_compact_sql_model_prefix(sql: &str) -> Result<(Vec<Model>, &str)> {
    let header_re = Regex::new(r"(?is)\bmodel\s+([A-Za-z_][A-Za-z0-9_]*)\s+from\s*")
        .expect("valid compact model header regex");
    let mut models = Vec::new();
    let mut remaining = sql;

    loop {
        remaining = remaining.trim_start();
        if let Some(after_semicolon) = remaining.strip_prefix(';') {
            remaining = after_semicolon;
            continue;
        }
        let Some(captures) = header_re.captures(remaining) else {
            break;
        };
        let matched = captures.get(0).unwrap();
        if matched.start() != 0 {
            return Err(SidemanticError::Validation(
                "Rust compact SQL model parser does not support non-model statements before compact model blocks".to_string(),
            ));
        }
        let name = captures[1].to_string();
        let after_from = &remaining[matched.end()..];
        let after_from = after_from.trim_start();

        let (table, source_sql, before_body) =
            if let Some(source_rest) = after_from.strip_prefix('(') {
                let (source_sql, source_remainder) = parse_balanced_parens(source_rest)
                    .ok_or_else(|| {
                        SidemanticError::Validation(format!(
                            "compact model '{name}' has an unterminated SQL source"
                        ))
                    })?;
                (
                    None,
                    Some(source_sql.trim().to_string()),
                    source_remainder.trim_start(),
                )
            } else {
                let open = after_from.find('(').ok_or_else(|| {
                    SidemanticError::Validation(format!(
                        "compact model '{name}' must use `model {name} from <table> (...)`"
                    ))
                })?;
                let table = after_from[..open].trim();
                if table.is_empty() {
                    return Err(SidemanticError::Validation(format!(
                        "compact model '{name}' must use `model {name} from <table> (...)`"
                    )));
                }
                (
                    Some(table.to_string()),
                    None,
                    after_from[open..].trim_start(),
                )
            };

        let Some(body_rest) = before_body.strip_prefix('(') else {
            return Err(SidemanticError::Validation(format!(
                "compact model '{name}' must include a model body"
            )));
        };
        let (body, rest) = parse_balanced_parens(body_rest).ok_or_else(|| {
            SidemanticError::Validation(format!("compact model '{name}' has an unterminated body"))
        })?;
        models.push(build_compact_model(name, table, source_sql, body)?);
        remaining = rest;
    }

    if models.is_empty() {
        return Err(SidemanticError::Validation(
            "SQL definitions must include a compact model statement".to_string(),
        ));
    }

    Ok((models, remaining))
}

fn parse_compact_sql_models(sql: &str) -> Result<Vec<Model>> {
    let (models, remaining) = parse_compact_sql_model_prefix(sql)?;
    let trailing = remaining.trim();
    if !trailing.is_empty() {
        parse_sql_graph_definitions_extended(trailing).map_err(|err| {
            SidemanticError::Validation(format!(
                "failed to parse trailing graph-level definitions after compact model blocks: {err}"
            ))
        })?;
    }

    Ok(models)
}

// ============================================================================
// Public API
// ============================================================================

/// Parse SQL definitions into one or more models.
pub fn parse_sql_models(sql: &str) -> Result<Vec<Model>> {
    if has_compact_model_syntax(sql) {
        return parse_compact_sql_models(sql);
    }

    let (_, statements) =
        parse_file(sql).map_err(|e| SidemanticError::Validation(format!("Parse error: {e}")))?;

    parse_legacy_sql_models_from_statements(statements)
}

/// Parse SQL definitions into the first model.
pub fn parse_sql_model(sql: &str) -> Result<Model> {
    parse_sql_models(sql).and_then(|models| {
        models.into_iter().next().ok_or_else(|| {
            SidemanticError::Validation("SQL definitions must include a MODEL statement".into())
        })
    })
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
    let sql = if has_compact_model_syntax(sql) {
        let (_, remaining) = parse_compact_sql_model_prefix(sql)?;
        remaining
    } else {
        sql
    };
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
    if let Some(metadata) = props.get("metadata") {
        let parsed = parse_literal(metadata);
        if !parsed.is_null() {
            model.metadata = Some(parsed);
        }
    }
    if let Some(meta) = props.get("meta") {
        let parsed = parse_literal(meta);
        if !parsed.is_null() {
            model.meta = Some(parsed);
        }
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
    if let Some(metadata) = props.get("metadata") {
        let parsed = parse_literal(metadata);
        if !parsed.is_null() {
            dim.metadata = Some(parsed);
        }
    }
    if let Some(meta) = props.get("meta") {
        let parsed = parse_literal(meta);
        if !parsed.is_null() {
            dim.meta = Some(parsed);
        }
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
    if let Some(public) = props.get("public") {
        dim.public = public.to_lowercase() != "false";
    }

    Some(dim)
}

fn build_metric(props: &HashMap<String, String>) -> Option<Metric> {
    let name = props.get("name")?;

    let mut metric = Metric::new(name);
    metric.extends = props.get("extends").cloned();
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
    if let Some(steps) = props.get("steps") {
        let parsed = json_value_to_string_list(parse_literal(steps));
        if !parsed.is_empty() {
            metric.steps = Some(parsed);
        }
    }
    metric.cohort_event = props.get("cohort_event").cloned();
    metric.activity_event = props.get("activity_event").cloned();
    metric.periods = props.get("periods").and_then(|value| value.parse().ok());
    metric.retention_granularity = props
        .get("retention_granularity")
        .or_else(|| props.get("granularity"))
        .cloned();
    if let Some(entity_dimensions) = props.get("entity_dimensions") {
        let parsed = json_value_to_string_list(parse_literal(entity_dimensions));
        if !parsed.is_empty() {
            metric.entity_dimensions = Some(parsed);
        }
    }
    if let Some(inner_metrics) = props.get("inner_metrics") {
        let parsed = parse_literal(inner_metrics);
        if let serde_json::Value::Array(items) = parsed {
            let inner = items
                .into_iter()
                .filter_map(|item| {
                    let serde_json::Value::Object(obj) = item else {
                        return None;
                    };
                    let name = obj.get("name").and_then(json_value_to_string)?;
                    let agg = obj
                        .get("agg")
                        .and_then(json_value_to_string)
                        .and_then(|value| parse_metric_aggregation(Some(&value)));
                    let sql = obj.get("sql").and_then(json_value_to_string);
                    Some(CohortInnerMetric { name, agg, sql })
                })
                .collect::<Vec<_>>();
            if !inner.is_empty() {
                metric.inner_metrics = Some(inner);
            }
        }
    }
    metric.having = props.get("having").cloned();
    metric.description = props.get("description").cloned();
    metric.label = props.get("label").cloned();
    if let Some(metadata) = props.get("metadata") {
        let parsed = parse_literal(metadata);
        if !parsed.is_null() {
            metric.metadata = Some(parsed);
        }
    }
    if let Some(meta) = props.get("meta") {
        let parsed = parse_literal(meta);
        if !parsed.is_null() {
            metric.meta = Some(parsed);
        }
    }
    if let Some(public) = props.get("public") {
        metric.public = public.to_lowercase() != "false";
    }
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

    let explicit_metric_type = props.get("type").map(|value| value.to_ascii_lowercase());
    if metric.agg.is_none() && matches!(explicit_metric_type.as_deref(), None | Some("simple")) {
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

    if !matches!(metric.r#type, MetricType::Simple | MetricType::Cohort) {
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
    let through_foreign_key_columns = parse_key_columns(props, "through_foreign_key");
    let related_foreign_key_columns = parse_key_columns(props, "related_foreign_key");

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
        through_foreign_key: through_foreign_key_columns
            .as_ref()
            .and_then(|columns| columns.first().cloned()),
        through_foreign_key_columns,
        related_foreign_key: related_foreign_key_columns
            .as_ref()
            .and_then(|columns| columns.first().cloned()),
        related_foreign_key_columns,
        sql: props.get("sql").cloned(),
        metadata: props.get("metadata").map(|value| parse_literal(value)),
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
        sql: props.get("sql").cloned(),
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
        meta: props.get("meta").map(|value| parse_literal(value)),
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
    fn test_parse_explicit_derived_metric_preserves_inline_aggregate_expression() {
        let sql = r#"
            MODEL (name orders, table orders);
            METRIC (name revenue, type derived, sql SUM(orders.amount));
        "#;

        let model = parse_sql_model(sql).unwrap();
        let revenue = model.get_metric("revenue").unwrap();
        assert_eq!(revenue.r#type, MetricType::Derived);
        assert_eq!(revenue.agg, None);
        assert_eq!(revenue.sql, Some("SUM(orders.amount)".to_string()));
    }

    #[test]
    fn test_parse_cohort_metric_preserves_outer_aggregation() {
        let sql = r#"
            MODEL (name events, table events);
            METRIC (
                name scored_users,
                type cohort,
                entity user_id,
                inner_metrics [{ name total_score, agg sum, sql score }],
                having total_score > 10,
                agg avg,
                sql cohort_sub.total_score
            );
        "#;

        let model = parse_sql_model(sql).unwrap();
        let metric = model.get_metric("scored_users").unwrap();
        assert_eq!(metric.r#type, MetricType::Cohort);
        assert_eq!(metric.agg, Some(Aggregation::Avg));
        assert_eq!(metric.sql, Some("cohort_sub.total_score".to_string()));
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
    fn test_simple_metric_function_coverage() {
        let sql = r#"
            MODEL (name orders, table orders);
            METRIC revenue AS SUM(COALESCE(amount, 0));
            METRIC unique_customers AS COUNT(DISTINCT customer_id);
            METRIC median_amount AS MEDIAN(amount);
            METRIC amount_stddev AS STDDEV(amount);
            METRIC amount_stddev_pop AS STDDEV_POP(amount);
            METRIC amount_variance AS VARIANCE(amount);
            METRIC amount_variance_pop AS VARIANCE_POP(amount);
            METRIC approximate_customers AS APPROX_COUNT_DISTINCT(customer_id);
        "#;

        let model = parse_sql_model(sql).unwrap();

        let revenue = model.get_metric("revenue").unwrap();
        assert_eq!(revenue.agg, Some(Aggregation::Sum));
        assert_eq!(revenue.sql, Some("COALESCE(amount, 0)".to_string()));

        let unique_customers = model.get_metric("unique_customers").unwrap();
        assert_eq!(unique_customers.agg, Some(Aggregation::CountDistinct));
        assert_eq!(unique_customers.sql, Some("customer_id".to_string()));

        let median_amount = model.get_metric("median_amount").unwrap();
        assert_eq!(median_amount.agg, Some(Aggregation::Median));
        assert_eq!(median_amount.sql, Some("amount".to_string()));

        let amount_stddev = model.get_metric("amount_stddev").unwrap();
        assert_eq!(amount_stddev.agg, Some(Aggregation::Stddev));
        assert_eq!(amount_stddev.sql, Some("amount".to_string()));

        let amount_stddev_pop = model.get_metric("amount_stddev_pop").unwrap();
        assert_eq!(amount_stddev_pop.agg, Some(Aggregation::StddevPop));
        assert_eq!(amount_stddev_pop.sql, Some("amount".to_string()));

        let amount_variance = model.get_metric("amount_variance").unwrap();
        assert_eq!(amount_variance.agg, Some(Aggregation::Variance));
        assert_eq!(amount_variance.sql, Some("amount".to_string()));

        let amount_variance_pop = model.get_metric("amount_variance_pop").unwrap();
        assert_eq!(amount_variance_pop.agg, Some(Aggregation::VariancePop));
        assert_eq!(amount_variance_pop.sql, Some("amount".to_string()));

        let approximate_customers = model.get_metric("approximate_customers").unwrap();
        assert_eq!(approximate_customers.agg, Some(Aggregation::Expression));
        assert_eq!(
            approximate_customers.sql,
            Some("APPROX_COUNT_DISTINCT(customer_id)".to_string())
        );
    }

    #[test]
    fn test_parse_compact_sql_model() {
        let sql = r#"
model orders from orders (
  primary key (order_id, store_id)
  default time order_date grain day

  status
  date_trunc('day', created_at) as order_date : time grain day
  status = 'completed' as is_complete : boolean
  amount - discount as net_amount : numeric

  segment completed as status = 'completed'

  join one customers on customer_id = customers.id
  join many order_items on (order_id = order_items.order_id and store_id = order_items.store_id)

  revenue / order_count as average_order_value
  sum(amount) as revenue
  count(*) as order_count
)
"#;

        let model = parse_sql_model(sql).unwrap();

        assert_eq!(model.name, "orders");
        assert_eq!(model.table.as_deref(), Some("orders"));
        assert_eq!(
            model.primary_key_columns,
            vec!["order_id".to_string(), "store_id".to_string()]
        );
        assert_eq!(model.default_time_dimension.as_deref(), Some("order_date"));
        assert_eq!(model.default_grain.as_deref(), Some("day"));

        let status = model.get_dimension("status").unwrap();
        assert_eq!(status.r#type, DimensionType::Categorical);
        assert_eq!(status.sql, None);

        let order_date = model.get_dimension("order_date").unwrap();
        assert_eq!(order_date.r#type, DimensionType::Time);
        assert_eq!(
            order_date.sql.as_deref(),
            Some("date_trunc('day', created_at)")
        );
        assert_eq!(order_date.granularity.as_deref(), Some("day"));

        let is_complete = model.get_dimension("is_complete").unwrap();
        assert_eq!(is_complete.r#type, DimensionType::Boolean);
        assert_eq!(is_complete.sql.as_deref(), Some("status = 'completed'"));

        let net_amount = model.get_dimension("net_amount").unwrap();
        assert_eq!(net_amount.r#type, DimensionType::Numeric);
        assert_eq!(net_amount.sql.as_deref(), Some("amount - discount"));

        let completed = model.get_segment("completed").unwrap();
        assert_eq!(completed.sql, "status = 'completed'");

        let customers = model.get_relationship("customers").unwrap();
        assert_eq!(customers.r#type, RelationshipType::ManyToOne);
        assert_eq!(
            customers.foreign_key_columns(),
            vec!["customer_id".to_string()]
        );
        assert_eq!(customers.primary_key_columns(), vec!["id".to_string()]);

        let order_items = model.get_relationship("order_items").unwrap();
        assert_eq!(order_items.r#type, RelationshipType::OneToMany);
        assert_eq!(
            order_items.foreign_key_columns(),
            vec!["order_id".to_string(), "store_id".to_string()]
        );
        assert_eq!(
            order_items.primary_key_columns(),
            vec!["order_id".to_string(), "store_id".to_string()]
        );

        let revenue = model.get_metric("revenue").unwrap();
        assert_eq!(revenue.agg, Some(Aggregation::Sum));
        assert_eq!(revenue.sql.as_deref(), Some("amount"));

        let order_count = model.get_metric("order_count").unwrap();
        assert_eq!(order_count.agg, Some(Aggregation::Count));
        assert_eq!(order_count.sql, None);

        let average_order_value = model.get_metric("average_order_value").unwrap();
        assert_eq!(average_order_value.r#type, MetricType::Derived);
        assert_eq!(
            average_order_value.sql.as_deref(),
            Some("revenue / order_count")
        );
    }

    #[test]
    fn test_parse_compact_sql_models_multiple_and_derived_source() {
        let sql = r#"
model completed_orders from (
  select *
  from raw.orders
  where status = 'completed'
) (
  primary key (order_id)
  created_at as order_date : time grain day
  sum(amount) as revenue
)

model customers from public.customers (
  primary key (id)
  region
)
"#;

        let models = parse_sql_models(sql).unwrap();

        assert_eq!(
            models.iter().map(|m| m.name.as_str()).collect::<Vec<_>>(),
            vec!["completed_orders", "customers"]
        );
        assert!(models[0].table.is_none());
        assert_eq!(
            models[0].sql.as_deref(),
            Some("select *\n  from raw.orders\n  where status = 'completed'")
        );
        assert_eq!(
            models[0].get_metric("revenue").unwrap().agg,
            Some(Aggregation::Sum)
        );
        assert_eq!(models[1].table.as_deref(), Some("public.customers"));
        assert!(models[1].get_dimension("region").is_some());
    }

    #[test]
    fn test_parse_compact_sql_models_with_trailing_graph_definitions() {
        let sql = r#"
model orders from orders (
  primary key (order_id)
  sum(amount) as revenue
)

METRIC (
  name total_revenue,
  sql orders.revenue
);

PARAMETER (
  name region,
  type string,
  allowed_values [us, eu]
);
"#;

        let models = parse_sql_models(sql).unwrap();
        assert_eq!(
            models.iter().map(|m| m.name.as_str()).collect::<Vec<_>>(),
            vec!["orders"]
        );

        let (metrics, segments, parameters) = parse_sql_graph_definitions(sql).unwrap();
        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0].name, "total_revenue");
        assert!(segments.is_empty());
        assert_eq!(parameters.len(), 1);
        assert_eq!(parameters[0].name, "region");
    }

    #[test]
    fn test_parse_legacy_sql_models_multiple() {
        let sql = r#"
MODEL (name orders, table orders, primary_key order_id);
METRIC order_count AS COUNT(*);

MODEL (name customers, table customers, primary_key customer_id);
METRIC customer_count AS COUNT(*);
"#;

        let models = parse_sql_models(sql).unwrap();

        assert_eq!(
            models.iter().map(|m| m.name.as_str()).collect::<Vec<_>>(),
            vec!["orders", "customers"]
        );
        assert!(models[0].get_metric("order_count").is_some());
        assert!(models[0].get_metric("customer_count").is_none());
        assert!(models[1].get_metric("customer_count").is_some());
    }

    #[test]
    fn test_parse_compact_sql_model_rejects_bad_join() {
        let sql = r#"
model orders from orders (
  primary key (order_id)
  join one customers on customer_id = 1
)
"#;

        let err = parse_sql_model(sql).unwrap_err();
        assert!(err.to_string().contains("must compare model columns"));
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
    fn test_simple_segment_syntax() {
        let sql = r#"
            MODEL (name orders, table orders);
            SEGMENT completed AS status = 'completed';
        "#;

        let model = parse_sql_model(sql).unwrap();
        assert_eq!(model.segments.len(), 1);

        let completed = model.get_segment("completed").unwrap();
        assert_eq!(completed.sql, "status = 'completed'");
    }

    #[test]
    fn test_simple_segment_graph_definition_syntax() {
        let sql = "SEGMENT completed AS status = 'completed';";

        let (_metrics, segments) = parse_sql_definitions(sql).unwrap();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].name, "completed");
        assert_eq!(segments[0].sql, "status = 'completed'");
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
