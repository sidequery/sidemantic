//! OSSIE_SQL_2026 source expressions and portable function normalization.
//!
//! Warehouse SQL alternatives do not use this module. Unknown function calls
//! remain extensions, as requested by the expression-language proposal.

use polyglot_sql::expressions::{DateTimeField, ExtractFunc, Function, Literal};
use polyglot_sql::tokens::TokenType;
use polyglot_sql::{Dialect, DialectType, Error, Expression};

fn invalid(message: impl Into<String>) -> Error {
    Error::Generate(message.into())
}

/// Parse the portable source grammar independently from the execution target.
pub fn parse_portable_expression(sql: &str) -> Result<Expression, String> {
    // Enter expression grammar before tokens such as TRUNCATE can be mistaken
    // for statement keywords. The wrapper also prevents trailing SQL clauses.
    let prepared = prepare_source_literals(sql)?;
    let mut expressions = polyglot_sql::parse(&format!("({prepared})"), DialectType::Snowflake)
        .map_err(|error| error.to_string())?;
    if expressions.len() != 1 {
        return Err("OSSIE_SQL_2026 requires exactly one expression".into());
    }
    let expression = match expressions.remove(0) {
        Expression::Paren(parentheses) => parentheses.this,
        expression => expression,
    };
    if matches!(
        expression,
        Expression::Alias(_) | Expression::Star(_) | Expression::Tuple(_)
    ) {
        return Err("OSSIE_SQL_2026 requires a scalar expression without an alias".into());
    }
    fn validate_tree(value: &serde_json::Value) -> Result<(), String> {
        // The pinned DFS walker does not enumerate typed aggregate arguments.
        // Validate the same complete tree that the lowering pass transforms.
        if let Ok(node) = serde_json::from_value::<Expression>(value.clone()) {
            validate_node(&node)?;
        }
        match value {
            serde_json::Value::Object(fields) => {
                for child in fields.values() {
                    validate_tree(child)?;
                }
            }
            serde_json::Value::Array(values) => {
                for child in values {
                    validate_tree(child)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    let value = serde_json::to_value(&expression).map_err(|error| error.to_string())?;
    validate_tree(&value)?;
    Ok(expression)
}

fn prepare_source_literals(sql: &str) -> Result<String, String> {
    // Polyglot recognizes TIMESTAMP_NTZ in CAST but not as a typed literal.
    // Rewrite only the lexer-confirmed type/literal pair, never quoted text.
    let tokens = Dialect::get(DialectType::Snowflake)
        .tokenize(sql)
        .map_err(|error| error.to_string())?;
    let offsets = sql
        .char_indices()
        .map(|(offset, _)| offset)
        .chain([sql.len()])
        .collect::<Vec<_>>();
    let mut prepared = sql.to_owned();
    for pair in tokens.windows(2).rev() {
        if pair[0].text.eq_ignore_ascii_case("TIMESTAMP_NTZ")
            && !matches!(
                pair[0].token_type,
                TokenType::String | TokenType::QuotedIdentifier
            )
            && pair[1].token_type == TokenType::String
        {
            let start = offsets[pair[0].span.start];
            let literal_start = offsets[pair[1].span.start];
            let end = offsets[pair[1].span.end];
            prepared.replace_range(
                start..end,
                &format!("CAST({} AS TIMESTAMP_NTZ)", &sql[literal_start..end]),
            );
        }
    }
    Ok(prepared)
}

fn validate_node(node: &Expression) -> Result<(), String> {
    if polyglot_sql::is_ddl(node)
        || matches!(
            node,
            Expression::Select(_)
                | Expression::Subquery(_)
                | Expression::Union(_)
                | Expression::Intersect(_)
                | Expression::Except(_)
                | Expression::Insert(_)
                | Expression::Update(_)
                | Expression::Delete(_)
                | Expression::Copy(_)
                | Expression::From(_)
                | Expression::Join(_)
                | Expression::Where(_)
                | Expression::GroupBy(_)
                | Expression::With(_)
                | Expression::Array(_)
                | Expression::ArrayFunc(_)
                | Expression::Subscript(_)
                | Expression::ArraySlice(_)
                | Expression::Command(_)
        )
    {
        return Err(
            "OSSIE_SQL_2026 expressions cannot contain queries, statements, or arrays".into(),
        );
    }
    if matches!(node, Expression::Dot(_)) {
        // Two-part field references are Column nodes with a table qualifier;
        // Dot is the parser's representation of additional path segments.
        return Err("OSSIE_SQL_2026 field references have at most two identifiers".into());
    }
    if let Expression::Identifier(identifier) = node {
        if identifier.name.chars().count() > 128 {
            return Err("OSSIE_SQL_2026 identifiers cannot exceed 128 characters".into());
        }
    }
    if let Expression::Column(column) = node {
        if column.name.name.chars().count() > 128
            || column
                .table
                .as_ref()
                .is_some_and(|name| name.name.chars().count() > 128)
        {
            return Err("OSSIE_SQL_2026 identifiers cannot exceed 128 characters".into());
        }
    }
    Ok(())
}

fn source_sql(expression: &Expression) -> polyglot_sql::Result<String> {
    Dialect::get(DialectType::Snowflake).generate(expression)
}

fn parse_source(sql: &str) -> polyglot_sql::Result<Expression> {
    polyglot_sql::parse_one(sql, DialectType::Snowflake)
}

fn extract(part: &str, value: Expression) -> polyglot_sql::Result<Expression> {
    let field = match part.to_uppercase().as_str() {
        "YEAR" => DateTimeField::Year,
        "QUARTER" => DateTimeField::Quarter,
        "MONTH" => DateTimeField::Month,
        "WEEK" => DateTimeField::Week,
        "DAY" => DateTimeField::Day,
        "DAYOFWEEK" => DateTimeField::DayOfWeek,
        "DAYOFYEAR" => DateTimeField::DayOfYear,
        "HOUR" => DateTimeField::Hour,
        "MINUTE" => DateTimeField::Minute,
        "SECOND" => DateTimeField::Second,
        "MILLISECOND" => DateTimeField::Millisecond,
        _ => {
            return Err(invalid(format!(
                "Unsupported OSSIE_SQL_2026 date part {part:?}"
            )))
        }
    };
    Ok(Expression::Extract(Box::new(ExtractFunc {
        this: value,
        field,
    })))
}

fn truncate_sql(value: &str, decimals: &str) -> String {
    // Preserve decimal literals instead of flooring an inexact POWER product.
    let quantum = match decimals.parse::<i32>() {
        Ok(places) if (1..=38).contains(&places) => {
            format!("0.{}1", "0".repeat((places - 1) as usize))
        }
        Ok(places) if (-38..=0).contains(&places) => format!("1{}", "0".repeat((-places) as usize)),
        _ => format!("POWER(10, -({decimals}))"),
    };
    format!("CASE WHEN ABS(ROUND({value}, {decimals})) > ABS({value}) THEN ROUND({value}, {decimals}) - SIGN({value}) * {quantum} ELSE ROUND({value}, {decimals}) END")
}

fn duckdb_date_add(unit: &str, amount: &str, value: &str) -> polyglot_sql::Result<Expression> {
    let unit = unit.trim_matches('\'').to_uppercase();
    if !matches!(
        unit.as_str(),
        "YEAR"
            | "QUARTER"
            | "MONTH"
            | "WEEK"
            | "DAY"
            | "HOUR"
            | "MINUTE"
            | "SECOND"
            | "MILLISECOND"
    ) {
        return Err(invalid(format!(
            "Unsupported OSSIE_SQL_2026 DATEADD unit {unit:?}"
        )));
    }
    // INTERVAL -1 MONTH is invalid DuckDB syntax. Multiplication handles
    // negative literals and arbitrary numeric expressions without ambiguity.
    polyglot_sql::parse_one(
        &format!("(({value}) + ({amount}) * INTERVAL '1 {unit}')"),
        DialectType::DuckDB,
    )
}

fn normalize(node: Expression, target: DialectType) -> polyglot_sql::Result<Expression> {
    match node {
        Expression::CurrentTime(_) if target == DialectType::DuckDB => {
            polyglot_sql::parse_one("LOCALTIME", target)
        }
        Expression::DateAdd(ref function) if target == DialectType::DuckDB => duckdb_date_add(
            &format!("{:?}", function.unit),
            &source_sql(&function.interval)?,
            &source_sql(&function.this)?,
        ),
        Expression::DateDiff(ref function) if target == DialectType::DuckDB => {
            // The canonical node stores (end, start), while Polyglot's generic
            // DuckDB emitter leaves that order unchanged and the unit unquoted.
            let unit = format!(
                "{:?}",
                function
                    .unit
                    .as_ref()
                    .ok_or_else(|| invalid("DATEDIFF requires a unit"))?
            )
            .to_uppercase();
            Ok(Expression::Function(Box::new(Function::new(
                "DATE_DIFF",
                vec![
                    Expression::Literal(Literal::String(unit)),
                    function.expression.clone(),
                    function.this.clone(),
                ],
            ))))
        }
        Expression::DateDiff(ref function) if target == DialectType::PostgreSQL => {
            let unit = format!(
                "{:?}",
                function
                    .unit
                    .as_ref()
                    .ok_or_else(|| invalid("DATEDIFF requires a unit"))?
            )
            .to_uppercase();
            let start = source_sql(&function.expression)?;
            let end = source_sql(&function.this)?;
            let years = format!("(EXTRACT(YEAR FROM {end}) - EXTRACT(YEAR FROM {start}))");
            let sql = match unit.as_str() {
                "YEAR" => years,
                "MONTH" => format!(
                    "({years} * 12 + EXTRACT(MONTH FROM {end}) - EXTRACT(MONTH FROM {start}))"
                ),
                "QUARTER" => format!(
                    "({years} * 4 + EXTRACT(QUARTER FROM {end}) - EXTRACT(QUARTER FROM {start}))"
                ),
                "DAY" | "HOUR" | "MINUTE" | "SECOND" => {
                    let divisor = match unit.as_str() {
                        "DAY" => 86400,
                        "HOUR" => 3600,
                        "MINUTE" => 60,
                        _ => 1,
                    };
                    format!("(EXTRACT(EPOCH FROM (DATE_TRUNC('{unit}', CAST({end} AS TIMESTAMP)) - DATE_TRUNC('{unit}', CAST({start} AS TIMESTAMP)))) / {divisor})")
                }
                _ => {
                    return Err(invalid(format!(
                        "Unsupported PostgreSQL DATEDIFF unit {unit}"
                    )))
                }
            };
            polyglot_sql::parse_one(&sql, target)
        }
        Expression::Log(ref function) if function.base.is_some() => {
            let value = source_sql(&function.this)?;
            let base = source_sql(function.base.as_ref().unwrap())?;
            parse_source(&format!("(LN({value}) / LN({base}))"))
        }
        Expression::ToTimestamp(ref function) if function.format.is_none() => parse_source(
            &format!("CAST({} AS TIMESTAMP_NTZ)", source_sql(&function.this)?),
        ),
        Expression::ToDate(ref function) if function.format.is_none() => {
            parse_source(&format!("CAST({} AS DATE)", source_sql(&function.this)?))
        }
        Expression::RegexpLike(ref function) => {
            let value = source_sql(&function.this)?;
            let pattern = source_sql(&function.pattern)?;
            match target {
                DialectType::DuckDB => {
                    polyglot_sql::parse_one(&format!("REGEXP_MATCHES({value}, {pattern})"), target)
                }
                DialectType::Snowflake => {
                    parse_source(&format!("REGEXP_INSTR({value}, {pattern}) > 0"))
                }
                _ => Ok(node),
            }
        }
        Expression::Extract(mut function) => {
            if target == DialectType::PostgreSQL {
                function.field = match function.field {
                    DateTimeField::DayOfYear => DateTimeField::Custom("DOY".into()),
                    DateTimeField::DayOfWeek => DateTimeField::Custom("DOW".into()),
                    part => part,
                };
            }
            Ok(Expression::Extract(function))
        }
        Expression::DateTrunc(ref function) if function.unit == DateTimeField::Week => {
            let value = source_sql(&function.this)?;
            match target {
                DialectType::BigQuery => {
                    polyglot_sql::parse_one(&format!("DATE_TRUNC({value}, WEEK(MONDAY))"), target)
                }
                DialectType::Snowflake => parse_source(&format!(
                    "DATEADD(day, 1 - DAYOFWEEKISO({value}), DATE_TRUNC('day', {value}))"
                )),
                _ => Ok(node),
            }
        }
        Expression::Median(_) if target == DialectType::BigQuery => Err(invalid(
            "BigQuery exact MEDIAN requires query-level lowering",
        )),
        Expression::WithinGroup(ref group) if target == DialectType::BigQuery => {
            let sql = source_sql(&group.this)?.to_uppercase();
            if sql.starts_with("PERCENTILE_CONT(") || sql.starts_with("PERCENTILE_DISC(") {
                Err(invalid(
                    "BigQuery exact ordered-set percentiles require query-level lowering",
                ))
            } else {
                Ok(node)
            }
        }
        Expression::Function(function) => {
            let name = function.name.to_uppercase();
            let args = &function.args;
            let rendered = args
                .iter()
                .map(source_sql)
                .collect::<polyglot_sql::Result<Vec<_>>>()?;
            if target == DialectType::DuckDB {
                match (name.as_str(), rendered.as_slice()) {
                    ("CURRENT_TIME", []) => return polyglot_sql::parse_one("LOCALTIME", target),
                    ("CURRENT_TIMESTAMP", []) => {
                        return polyglot_sql::parse_one("CURRENT_TIMESTAMP", target)
                    }
                    ("CURRENT_DATE", []) => return polyglot_sql::parse_one("CURRENT_DATE", target),
                    ("DATEADD", [unit, amount, value]) => {
                        return duckdb_date_add(unit, amount, value)
                    }
                    _ => {}
                }
            }
            let replacement = match (name.as_str(), rendered.as_slice()) {
                ("TO_TIMESTAMP", [value]) => Some(format!("CAST({value} AS TIMESTAMP_NTZ)")),
                ("TO_DATE", [value]) => Some(format!("CAST({value} AS DATE)")),
                ("LOG10", [value]) => Some(format!("(LN({value}) / LN(10))")),
                ("ZEROIFNULL", [value]) => Some(format!("COALESCE({value}, 0)")),
                ("NULLIFZERO", [value]) => Some(format!("NULLIF({value}, 0)")),
                ("TRUNC" | "TRUNCATE", [value, decimals]) => Some(truncate_sql(value, decimals)),
                ("TRUNC" | "TRUNCATE", [value]) => Some(truncate_sql(value, "0")),
                ("CONTAINS", [value, part]) => Some(format!("(POSITION({part} IN {value}) > 0)")),
                ("STARTSWITH", [value, part]) => {
                    Some(format!("(LEFT({value}, LENGTH({part})) = {part})"))
                }
                ("ENDSWITH", [value, part]) => {
                    Some(format!("(RIGHT({value}, LENGTH({part})) = {part})"))
                }
                ("CHARINDEX", [part, value]) => Some(format!("POSITION({part} IN {value})")),
                (
                    "YEAR" | "QUARTER" | "MONTH" | "DAY" | "DAYOFYEAR" | "HOUR" | "MINUTE"
                    | "SECOND",
                    [_],
                ) => {
                    return normalize(extract(&name, args[0].clone())?, target);
                }
                ("DATE_PART", [part, _]) => {
                    return normalize(extract(part.trim_matches('\''), args[1].clone())?, target);
                }
                _ => None,
            };
            if let Some(sql) = replacement {
                parse_source(&sql)
            } else {
                Ok(Expression::Function(function))
            }
        }
        _ => Ok(node),
    }
}

/// Lower the portable expression, preserving exact rather than approximate SQL.
pub fn lower_ossie_sql(sql: &str, target: DialectType) -> Result<String, String> {
    // Polyglot's recursive transformer has large stack frames. Reuse the
    // established native semantic boundary, including its WASM-safe fallback.
    crate::semantic_input::with_semantic_stack(|| {
        lower_ossie_sql_inner(sql, target).map_err(crate::error::SidemanticError::Validation)
    })
    .map_err(|error| error.to_string())
}

fn lower_ossie_sql_inner(sql: &str, target: DialectType) -> Result<String, String> {
    let expression = parse_portable_expression(sql)?;
    let dialect = Dialect::get(target);
    // The pinned Polyglot walker omits children of typed aggregates such as
    // SUM/AVG. Follow the complete serialized AST, as semantic_input/dates does,
    // so portable functions inside aggregates receive the same corrections.
    fn rewrite(
        value: &mut serde_json::Value,
        target: DialectType,
        dialect: &Dialect,
    ) -> Result<(), String> {
        match value {
            serde_json::Value::Object(fields) => {
                for child in fields.values_mut() {
                    rewrite(child, target, dialect)?;
                }
            }
            serde_json::Value::Array(values) => {
                for child in values {
                    rewrite(child, target, dialect)?;
                }
            }
            _ => return Ok(()),
        }
        if let Ok(node) = serde_json::from_value::<Expression>(value.clone()) {
            let normalized = normalize(node, target).map_err(|error| error.to_string())?;
            let transformed = dialect
                .transform(normalized)
                .map_err(|error| error.to_string())?;
            *value = serde_json::to_value(transformed).map_err(|error| error.to_string())?;
        }
        Ok(())
    }
    let mut value = serde_json::to_value(expression).map_err(|error| error.to_string())?;
    rewrite(&mut value, target, &dialect)?;
    let expression = serde_json::from_value(value).map_err(|error| error.to_string())?;
    dialect
        .generate_with_source(&expression, DialectType::Snowflake)
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_queries_and_multiple_expressions() {
        for sql in [
            "SELECT x",
            "x IN (SELECT x FROM t)",
            "x; y",
            "x AS y",
            "DROP TABLE t",
        ] {
            assert!(lower_ossie_sql(sql, DialectType::DuckDB).is_err(), "{sql}");
        }
    }

    #[test]
    fn rejects_forbidden_nodes_inside_typed_aggregate_arguments() {
        let mut failures = Vec::new();
        for (sql, reason) in [
            ("SUM((SELECT 1))".to_string(), "cannot contain queries"),
            (
                "AVG(COALESCE((SELECT 1), 0))".to_string(),
                "cannot contain queries",
            ),
            ("SUM([1, 2])".to_string(), "cannot contain queries"),
            (
                "AVG(COALESCE([], [1]))".to_string(),
                "cannot contain queries",
            ),
            ("SUM(values_array[1])".to_string(), "cannot contain queries"),
            ("SUM(a.b.c)".to_string(), "at most two identifiers"),
            ("AVG(ABS(a.b.c))".to_string(), "at most two identifiers"),
            (format!("SUM(\"{}\")", "x".repeat(129)), "cannot exceed 128"),
            (
                format!("AVG(\"{}\".x)", "t".repeat(129)),
                "cannot exceed 128",
            ),
        ] {
            match lower_ossie_sql(&sql, DialectType::DuckDB) {
                Err(error) if error.contains(reason) => {}
                outcome => {
                    failures.push(format!("{sql}: expected {reason:?}, received {outcome:?}"))
                }
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }

    #[test]
    fn preserves_valid_qualified_and_quoted_aggregate_arguments() {
        for sql in ["SUM(ABS(t.x))", "AVG(COALESCE(t.x, 0))", "SUM(t.\"a.b\")"] {
            let lowered = lower_ossie_sql(sql, DialectType::DuckDB).unwrap();
            assert!(lowered.contains("t."), "{sql}: {lowered}");
        }
    }

    #[test]
    fn required_portable_forms_produce_parseable_sql() {
        for sql in [
            "LOG(2, 8)",
            "LOG10(100)",
            "TRUNC(-12.345, 2)",
            "TRUNCATE(123.45, -1)",
            "ZEROIFNULL(x)",
            "NULLIFZERO(x)",
            "DATEADD(day, 7, d)",
            "DATEDIFF(day, a, b)",
            "DAYOFYEAR(d)",
            "DATE_PART('year', d)",
            "TO_TIMESTAMP('2024-01-15 10:30:00')",
            "TO_DATE('2024-01-15')",
            "CONTAINS(s, 'x')",
            "STARTSWITH(s, 'x')",
            "ENDSWITH(s, 'x')",
            "PERCENTILE_CONT(.5) WITHIN GROUP (ORDER BY x)",
            "LAG(x, 1, 0) OVER (ORDER BY d)",
            "CAST(x AS VARCHAR)",
        ] {
            for target in [
                DialectType::DuckDB,
                DialectType::PostgreSQL,
                DialectType::Snowflake,
                DialectType::Databricks,
            ] {
                let lowered =
                    lower_ossie_sql(sql, target).unwrap_or_else(|error| panic!("{sql}: {error}"));
                crate::semantic_input::with_semantic_stack(|| {
                    crate::semantic_input::dialects::parse(&lowered, target)?;
                    Ok(())
                })
                .unwrap_or_else(|error| panic!("{lowered}: {error}"));
                assert!(!lowered.contains("APPROX"), "{sql}: {lowered}");
            }
        }
    }

    #[test]
    fn keeps_logarithm_order_and_truncation_precision() {
        let log = lower_ossie_sql("LOG(2, 8)", DialectType::DuckDB).unwrap();
        assert!(log.contains("LN(8) / LN(2)"), "{log}");
        let trunc = lower_ossie_sql("TRUNC(-12.345, 2)", DialectType::DuckDB).unwrap();
        assert!(
            trunc.contains("ROUND(-12.345, 2)") && trunc.contains("0.01"),
            "{trunc}"
        );
        let timestamp =
            lower_ossie_sql("TO_TIMESTAMP('2024-01-15 10:30:00')", DialectType::DuckDB).unwrap();
        assert!(
            timestamp.contains("CAST(") && timestamp.contains("AS TIMESTAMP"),
            "{timestamp}"
        );
    }

    #[test]
    fn normalizes_functions_inside_typed_aggregates() {
        let sql = lower_ossie_sql(
            "SUM(LOG(2, x)) + AVG(DATEDIFF(day, a, b))",
            DialectType::DuckDB,
        )
        .unwrap();
        assert!(sql.contains("LN(x) / LN(2)"), "{sql}");
        assert!(sql.contains("DATE_DIFF('DAY', a, b)"), "{sql}");
    }

    #[test]
    fn normalizes_portable_time_and_timestamp_source_forms() {
        for source in ["CURRENT_TIME", "CURRENT_TIME()"] {
            assert_eq!(
                lower_ossie_sql(source, DialectType::DuckDB).unwrap(),
                "LOCALTIME"
            );
        }
        for source in ["CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP()"] {
            assert_eq!(
                lower_ossie_sql(source, DialectType::DuckDB).unwrap(),
                "CURRENT_TIMESTAMP"
            );
        }
        for part in ["HOUR", "MINUTE", "SECOND"] {
            let source = format!("{part}(TIMESTAMP_NTZ '2024-03-01 12:34:56')");
            let lowered = lower_ossie_sql(&source, DialectType::DuckDB).unwrap();
            let explicit_cast = format!("{part}(CAST('2024-03-01 12:34:56' AS TIMESTAMP_NTZ))");
            assert_eq!(
                lowered,
                lower_ossie_sql(&explicit_cast, DialectType::DuckDB).unwrap()
            );
        }
    }

    #[test]
    fn typed_timestamp_rewriting_preserves_literals_and_unicode_offsets() {
        let source =
            "CONCAT('é TIMESTAMP_NTZ', CAST(TIMESTAMP_NTZ '2024-03-01 12:34:56' AS VARCHAR))";
        assert_eq!(prepare_source_literals(source).unwrap(), "CONCAT('é TIMESTAMP_NTZ', CAST(CAST('2024-03-01 12:34:56' AS TIMESTAMP_NTZ) AS VARCHAR))");
    }

    #[test]
    fn dateadd_negative_amount_uses_multiplication_not_an_unquoted_interval() {
        let lowered = lower_ossie_sql(
            "CAST(DATEADD(month, -1, DATE '2024-03-15') AS DATE)",
            DialectType::DuckDB,
        )
        .unwrap();
        assert!(
            lowered.contains("-1") && lowered.contains("* INTERVAL"),
            "{lowered}"
        );
        assert!(!lowered.contains("INTERVAL -1 MONTH"), "{lowered}");
    }
}
