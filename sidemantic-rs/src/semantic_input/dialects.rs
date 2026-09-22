//! Normalize declared SQL syntax with polyglot before semantic binding.
//!
//! The executable graph uses DuckDB AST syntax internally; source declarations
//! remain unchanged in `SemanticInput::source`. This is syntax translation, not
//! a second resolver for metric names, policies, or query populations.

use std::collections::HashMap;

use polyglot_sql::expressions::Select;
use polyglot_sql::{Dialect, DialectType, Expression};
use serde_json::Value;

use crate::error::{Result, SidemanticError};

pub(crate) fn parse_dialect(name: &str) -> Result<DialectType> {
    name.parse().map_err(|error| {
        SidemanticError::Validation(format!("Invalid SQL dialect '{name}': {error}"))
    })
}

pub(crate) fn emit(
    expression: Expression,
    source: DialectType,
    target: DialectType,
) -> Result<String> {
    if source == target {
        return polyglot_sql::generate(&expression, target)
            .map_err(|error| SidemanticError::SqlGeneration(error.to_string()));
    }

    // The pinned dialect transformer has large recursive frames even for the
    // few nested SELECTs in an ordinary generated query. Run final emission on
    // the same stack budget used by the SQL parser, not the caller's test or
    // application thread. This boundary also covers Yardstick and policy SQL.
    #[cfg(not(target_arch = "wasm32"))]
    {
        std::thread::Builder::new()
            .stack_size(16 * 1024 * 1024)
            .spawn(move || emit_transformed(expression, source, target))
            .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?
            .join()
            .map_err(|_| {
                SidemanticError::SqlGeneration("Polyglot emission thread panicked".into())
            })?
    }
    #[cfg(target_arch = "wasm32")]
    {
        emit_transformed(expression, source, target)
    }
}

fn emit_transformed(
    expression: Expression,
    source: DialectType,
    target: DialectType,
) -> Result<String> {
    let expression = super::literals::target_expression(expression, target)?;
    // Generated ASTs can contain target SQL leaves. Keep them as ASTs instead
    // of reparsing target-quoted identifiers as source SQL. Authored input uses
    // the complete transpile pipeline below before reaching this boundary.
    let dialect = Dialect::get(target);
    let expression = dialect
        .transform(expression)
        .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
    dialect
        .generate_with_source(&expression, source)
        .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))
}

fn transpile(sql: &str, source: DialectType, target: DialectType) -> Result<String> {
    let sql = super::literals::source_sql(sql, source)?;
    #[cfg(target_arch = "wasm32")]
    crate::wasm_sql_guard::check(&sql, source)?;
    let mut statements = Dialect::get(source)
        .transpile_to(&sql, target)
        .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
    if statements.len() != 1 {
        return Err(SidemanticError::SqlParse(
            "Expected one SQL statement".into(),
        ));
    }
    super::dates::normalize_transpiled(&statements.remove(0), source, target)
}

pub(crate) fn parse(sql: &str, source: DialectType) -> Result<Expression> {
    let sql = super::literals::source_sql(sql, source)?;
    #[cfg(target_arch = "wasm32")]
    crate::wasm_sql_guard::check(&sql, source)?;
    let mut statements = parse_many(&sql, source)?;
    if statements.len() != 1 {
        return Err(SidemanticError::SqlParse(
            "Expected one SQL statement".into(),
        ));
    }
    Ok(statements.remove(0))
}

/// QUANTILE_CONT/DISC are absent from the pinned parser's aggregate registry.
/// Parse their argument lists with its generic aggregate grammar, then restore
/// the actual name in the AST. ORDER BY, DISTINCT, FILTER and window clauses
/// must survive; deleting the aggregate ordering changes percentile semantics.
pub(crate) fn parse_many(sql: &str, dialect: DialectType) -> Result<Vec<Expression>> {
    use polyglot_sql::expressions::AggregateFunction;
    use polyglot_sql::tokens::TokenType;

    let mut replacements: HashMap<String, AggregateFunction> = HashMap::new();
    let mut prepared = sql.to_owned();
    if sql.to_ascii_uppercase().contains("QUANTILE_") {
        let stream = Dialect::get(dialect)
            .tokenize(sql)
            .map_err(|error| SidemanticError::SqlParse(error.to_string()))?;
        let offsets = sql
            .char_indices()
            .map(|(index, _)| index)
            .chain([sql.len()])
            .collect::<Vec<_>>();
        let mut prefix = "__sidemantic_quantile_".to_owned();
        while sql.to_ascii_lowercase().contains(&prefix) {
            prefix.push('_');
        }
        prepared.clear();
        let mut cursor = 0;
        let mut index = 0;
        while index + 1 < stream.len() {
            let token = &stream[index];
            if token.token_type == TokenType::Identifier
                || !["QUANTILE_CONT", "QUANTILE_DISC"]
                    .contains(&token.text.to_ascii_uppercase().as_str())
                || stream[index + 1].token_type != TokenType::LParen
                || index > 0 && stream[index - 1].token_type == TokenType::Dot
            {
                index += 1;
                continue;
            }
            let open = index + 1;
            let mut end = open + 1;
            let mut depth = 1;
            while end < stream.len() {
                match stream[end].token_type {
                    TokenType::LParen => depth += 1,
                    TokenType::RParen => depth -= 1,
                    _ => {}
                }
                if depth == 0 {
                    break;
                }
                end += 1;
            }
            if end == stream.len() {
                return Err(SidemanticError::SqlParse(
                    "Unclosed quantile aggregate".into(),
                ));
            }
            let body = &sql[offsets[stream[open].span.end]..offsets[stream[end].span.start]];
            // RESERVOIR_SAMPLE uses the generic aggregate grammar without a
            // specialized argument parser in the pinned registry. Only
            // this root node is renamed; nested calls keep their own identity.
            let mut parsed = parse_many(&format!("SELECT RESERVOIR_SAMPLE({body})"), dialect)?;
            let Expression::Select(select) = parsed.remove(0) else {
                unreachable!()
            };
            let Expression::AggregateFunction(mut aggregate) = select.expressions[0].clone() else {
                return Err(SidemanticError::SqlParse(
                    "Expected quantile aggregate arguments".into(),
                ));
            };
            aggregate.name = token.text.to_ascii_uppercase();
            let placeholder = format!("{prefix}{}", replacements.len());
            replacements.insert(placeholder.clone(), *aggregate);
            prepared.push_str(&sql[cursor..offsets[token.span.start]]);
            prepared.push_str(&format!("{placeholder}()"));
            cursor = offsets[stream[end].span.end];
            index = end + 1;
        }
        prepared.push_str(&sql[cursor..]);
    }
    let statements = polyglot_sql::parse(&prepared, dialect)
        .map_err(|error| SidemanticError::SqlParse(error.to_string()))?;
    let complete_conditionals =
        dialect == DialectType::DuckDB && prepared.to_ascii_uppercase().contains("IF");
    if replacements.is_empty() && !complete_conditionals {
        return Ok(statements);
    }
    fn restore(
        value: &mut Value,
        replacements: &HashMap<String, AggregateFunction>,
        complete_conditionals: bool,
    ) -> Result<()> {
        match value {
            Value::Object(fields) => {
                for child in fields.values_mut() {
                    restore(child, replacements, complete_conditionals)?;
                }
                if complete_conditionals {
                    if let Some(function) = fields.get_mut("if_func").and_then(Value::as_object_mut)
                    {
                        if function.get("false_value").is_none_or(Value::is_null) {
                            // The parser accepts IF(condition, value), but the
                            // DuckDB emitter preserves that invalid two-argument
                            // call. Its omitted false branch has NULL semantics.
                            function.insert(
                                "false_value".into(),
                                serde_json::to_value(Expression::null()).map_err(|error| {
                                    SidemanticError::SqlParse(error.to_string())
                                })?,
                            );
                        }
                    }
                }
                for kind in ["function", "aggregate_function"] {
                    let Some(function) = fields.get(kind) else {
                        continue;
                    };
                    let Some(name) = function.get("name").and_then(Value::as_str) else {
                        continue;
                    };
                    let Some(saved) = replacements.get(&name.to_ascii_lowercase()) else {
                        continue;
                    };
                    let mut aggregate = saved.clone();
                    if kind == "aggregate_function" {
                        let wrapper: AggregateFunction =
                            serde_json::from_value(function.clone())
                                .map_err(|error| SidemanticError::SqlParse(error.to_string()))?;
                        aggregate.filter = wrapper.filter;
                        aggregate.ignore_nulls = wrapper.ignore_nulls.or(aggregate.ignore_nulls);
                    }
                    *value =
                        serde_json::to_value(Expression::AggregateFunction(Box::new(aggregate)))
                            .map_err(|error| SidemanticError::SqlParse(error.to_string()))?;
                    break;
                }
            }
            Value::Array(children) => {
                for child in children {
                    restore(child, replacements, complete_conditionals)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    let mut value = serde_json::to_value(statements)
        .map_err(|error| SidemanticError::SqlParse(error.to_string()))?;
    restore(&mut value, &replacements, complete_conditionals)?;
    serde_json::from_value(value).map_err(|error| SidemanticError::SqlParse(error.to_string()))
}

pub(crate) fn query(sql: &str, source: DialectType) -> Result<String> {
    if source == DialectType::DuckDB {
        return Ok(sql.to_owned());
    }
    parse(sql, source)?;
    transpile(sql, source, DialectType::DuckDB)
}

/// The native rewriter accepts ordinary statement batches without request
/// policies. Fragment/model validation continues to require exactly one query.
pub(crate) fn query_batch(sql: &str, source: DialectType) -> Result<String> {
    if source == DialectType::DuckDB {
        return Ok(sql.to_owned());
    }
    let sql = super::literals::source_sql(sql, source)?;
    #[cfg(target_arch = "wasm32")]
    crate::wasm_sql_guard::check(&sql, source)?;
    Dialect::get(source)
        .transpile_to(&sql, DialectType::DuckDB)
        .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?
        .iter()
        .map(|statement| super::dates::normalize_transpiled(statement, source, DialectType::DuckDB))
        .collect::<Result<Vec<_>>>()
        .map(|statements| statements.join(";\n"))
}

#[derive(Clone, Copy)]
pub(crate) enum Fragment {
    Scalar,
    Order,
    Table,
}

/// Wrappers let the SQL parser validate the complete fragment. A trailing
/// clause cannot disappear while extracting just its first expression.
pub(crate) fn fragment(sql: &str, source: DialectType, kind: Fragment) -> Result<String> {
    if source == DialectType::DuckDB || sql.trim().is_empty() {
        return Ok(sql.to_owned());
    }
    let wrapper = match kind {
        Fragment::Scalar => format!("SELECT {sql}"),
        Fragment::Order => format!("SELECT 1 ORDER BY {sql}"),
        Fragment::Table => format!("SELECT * FROM {sql}"),
    };
    extract_fragment(&wrapper, source, kind)?;
    let normalized = query(&wrapper, source)?;
    let result = extract_fragment(&normalized, DialectType::DuckDB, kind)?;
    let sql = polyglot_sql::generate(&result, DialectType::DuckDB)
        .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
    Ok(match kind {
        Fragment::Order => sql.strip_prefix("ORDER BY ").unwrap_or(&sql).to_owned(),
        _ => sql,
    })
}

fn extract_fragment(wrapper: &str, source: DialectType, kind: Fragment) -> Result<Expression> {
    let Expression::Select(mut select) = parse(wrapper, source)? else {
        return Err(SidemanticError::SqlParse("Expected a SQL fragment".into()));
    };
    let expressions = std::mem::take(&mut select.expressions);
    let result = match kind {
        Fragment::Scalar if expressions.len() == 1 => expressions.into_iter().next().unwrap(),
        Fragment::Order => {
            Expression::OrderBy(Box::new(select.order_by.take().ok_or_else(|| {
                SidemanticError::SqlParse("Expected an ordering expression".into())
            })?))
        }
        Fragment::Table => {
            let mut from = select
                .from
                .take()
                .ok_or_else(|| SidemanticError::SqlParse("Expected a table expression".into()))?;
            if from.expressions.len() != 1 {
                return Err(SidemanticError::SqlParse(
                    "Expected one table expression".into(),
                ));
            }
            from.expressions.remove(0)
        }
        _ => {
            return Err(SidemanticError::SqlParse(
                "Expected one scalar expression".into(),
            ))
        }
    };
    select.leading_comments.clear();
    select.post_select_comments.clear();
    if *select != Select::new() {
        return Err(SidemanticError::SqlParse(
            "SQL fragment contains extra clauses".into(),
        ));
    }
    Ok(result)
}

fn field(definition: &mut Value, name: &str, dialect: DialectType, kind: Fragment) -> Result<()> {
    if let Some(Value::String(sql)) = definition.get_mut(name) {
        *sql = template_sql(sql, dialect, Some(kind))?;
    }
    Ok(())
}

fn fields(definition: &mut Value, name: &str, dialect: DialectType) -> Result<()> {
    if let Some(Value::Array(values)) = definition.get_mut(name) {
        for value in values {
            if let Value::String(sql) = value {
                *sql = template_sql(sql, dialect, Some(Fragment::Scalar))?;
            }
        }
    }
    Ok(())
}

/// Protect interpolation tokens while the library translates surrounding SQL.
/// Runtime interpolation still owns values and escaping. A collision-free name
/// prevents ordinary source identifiers or literals from being substituted.
pub(crate) fn template_sql(
    sql: &str,
    dialect: DialectType,
    kind: Option<Fragment>,
) -> Result<String> {
    if dialect == DialectType::DuckDB {
        return Ok(sql.to_owned());
    }
    let token = regex::Regex::new(r"\{\{[^{}]*\}\}|\$\{[^{}]*\}|\{(?:model|from|to)\}").unwrap();
    let mut prefix = "__sidemantic_dialect_token_".to_owned();
    while sql.to_ascii_lowercase().contains(&prefix) {
        prefix.push('_');
    }
    let mut replacements = Vec::new();
    let protected = token.replace_all(sql, |captures: &regex::Captures<'_>| {
        let name = format!("{prefix}{}__", replacements.len());
        replacements.push((name.clone(), captures[0].to_owned()));
        name
    });
    let mut result = match kind {
        Some(kind) => fragment(&protected, dialect, kind)?,
        None => query(&protected, dialect)?,
    };
    for (name, original) in replacements {
        result = result.replace(&name, &original);
    }
    Ok(result)
}

fn definition_dialect(definition: &mut Value, fallback: DialectType) -> Result<DialectType> {
    let Some(metadata) = definition
        .get_mut("metadata")
        .and_then(Value::as_object_mut)
    else {
        return Ok(fallback);
    };
    let field = if metadata.contains_key("ossie_target_dialect") {
        "ossie_target_dialect"
    } else {
        "ossie_expression_dialect"
    };
    let Some(value) = metadata.get_mut(field) else {
        return Ok(fallback);
    };
    let name = value
        .as_str()
        .ok_or_else(|| SidemanticError::Validation("Expression dialect must be a string".into()))?;
    let dialect = parse_dialect(name)?;
    *value = Value::String("duckdb".into());
    Ok(dialect)
}

fn metric(definition: &mut Value, source: DialectType) -> Result<()> {
    let source = definition_dialect(definition, source)?;
    for name in [
        "sql",
        "entity",
        "window_expression",
        "having",
        "cohort_event",
        "activity_event",
    ] {
        field(definition, name, source, Fragment::Scalar)?;
    }
    for name in ["base_event", "conversion_event"] {
        // Bare names are event values, not expressions. Predicate-shaped
        // events are SQL and need the same translation as funnel steps.
        if definition
            .get(name)
            .and_then(Value::as_str)
            .is_some_and(|sql| sql.contains(['=', '<', '>', '(', ')']))
        {
            field(definition, name, source, Fragment::Scalar)?;
        }
    }
    field(definition, "window_order", source, Fragment::Order)?;
    for name in ["filters", "steps"] {
        fields(definition, name, source)?;
    }
    if let Some(Value::Array(inner)) = definition.get_mut("inner_metrics") {
        for item in inner {
            field(item, "sql", source, Fragment::Scalar)?;
        }
    }
    Ok(())
}

pub(super) type SegmentDialects = HashMap<(String, String), DialectType>;

pub(super) fn normalize(
    envelope: &mut super::Envelope,
    source: DialectType,
) -> Result<SegmentDialects> {
    let mut deferred_segments = HashMap::new();
    for model in &mut envelope.models {
        let source = definition_dialect(model, source)?;
        let model_name = model
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        if let Some(Value::String(sql)) = model.get_mut("sql") {
            *sql = template_sql(sql, source, None)?;
        }
        field(model, "table", source, Fragment::Table)?;
        for collection in ["dimensions", "relationships", "segments"] {
            if let Some(Value::Array(values)) = model.get_mut(collection) {
                for value in values {
                    let dialect = definition_dialect(value, source)?;
                    if collection == "segments"
                        && value
                            .get("sql")
                            .and_then(Value::as_str)
                            .is_some_and(crate::runtime::is_sql_template)
                    {
                        // Selected segments are rendered with request parameters
                        // before syntax normalization. Unselected templates stay
                        // inert, including Jinja control blocks and unused names.
                        let name = value
                            .get("name")
                            .and_then(Value::as_str)
                            .unwrap_or_default();
                        deferred_segments.insert((model_name.clone(), name.to_owned()), dialect);
                        continue;
                    }
                    field(value, "sql", dialect, Fragment::Scalar)?;
                }
            }
        }
        if let Some(Value::Array(metrics)) = model.get_mut("metrics") {
            for value in metrics {
                metric(value, source)?;
            }
        }
        if let Some(Value::Array(preaggregations)) = model.get_mut("pre_aggregations") {
            for preaggregation in preaggregations {
                if let Some(Value::String(sql)) = preaggregation.get_mut("sql") {
                    *sql = template_sql(sql, source, None)?;
                }
            }
        }
    }
    for value in &mut envelope.metrics {
        metric(value, source)?;
    }
    Ok(deferred_segments)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordered_quantiles_preserve_arguments_order_filter_and_window() {
        for name in ["quantile_cont", "quantile_disc"] {
            let sql = format!("SELECT {name}(DISTINCT value, 0.25 ORDER BY sort_key DESC NULLS FIRST) FILTER (WHERE included) OVER (PARTITION BY category), 'é QUANTILE_CONT(x ORDER BY y)' AS label");
            let parsed = parse(&sql, DialectType::DuckDB).unwrap();
            let generated = polyglot_sql::generate(&parsed, DialectType::DuckDB).unwrap();
            assert!(
                generated.contains(&format!(
                    "{}(DISTINCT value, 0.25 ORDER BY sort_key DESC NULLS FIRST)",
                    name.to_ascii_uppercase()
                )),
                "{generated}"
            );
            assert!(
                generated.contains("FILTER(WHERE included)")
                    || generated.contains("FILTER (WHERE included)"),
                "{generated}"
            );
            assert!(generated.contains("PARTITION BY category"), "{generated}");
            assert!(
                generated.contains("'é QUANTILE_CONT(x ORDER BY y)'"),
                "{generated}"
            );
            let reparsed = parse(&generated, DialectType::DuckDB).unwrap();
            assert_eq!(
                polyglot_sql::generate(&reparsed, DialectType::DuckDB).unwrap(),
                generated
            );
        }
    }

    #[test]
    fn omitted_conditional_false_branches_are_explicit_nulls_for_duckdb() {
        for (input, expected) in [
            ("IF(flag, 1)", "IF(flag, 1, NULL)"),
            ("COUNT(IF(flag, 1))", "COUNT(IF(flag, 1, NULL))"),
            (
                "IF(flag, IF(other, 1), 0)",
                "IF(flag, IF(other, 1, NULL), 0)",
            ),
            ("IF(flag, 1, 0)", "IF(flag, 1, 0)"),
        ] {
            let parsed = parse(&format!("SELECT {input}"), DialectType::DuckDB).unwrap();
            let generated = polyglot_sql::generate(&parsed, DialectType::DuckDB).unwrap();
            assert_eq!(generated, format!("SELECT {expected}"));
        }
        let sql = "SELECT 'IF(flag, 1)' AS label";
        let parsed = parse(sql, DialectType::DuckDB).unwrap();
        assert_eq!(
            polyglot_sql::generate(&parsed, DialectType::DuckDB).unwrap(),
            sql
        );
    }

    #[test]
    fn nested_quantiles_keep_each_function_identity() {
        let parsed = parse("SELECT quantile_cont((SELECT quantile_disc(value, 0.5 ORDER BY value) FROM raw), 0.25 ORDER BY rank)", DialectType::DuckDB).unwrap();
        let generated = polyglot_sql::generate(&parsed, DialectType::DuckDB).unwrap();
        assert!(generated.contains("QUANTILE_CONT("), "{generated}");
        assert!(
            generated.contains("QUANTILE_DISC(value, 0.5 ORDER BY value)"),
            "{generated}"
        );
        assert!(!generated.contains("RESERVOIR_SAMPLE"), "{generated}");
        assert!(!generated.contains("__sidemantic_quantile_"), "{generated}");
    }

    #[test]
    fn nested_query_emission_uses_production_stack_on_standard_test_thread() {
        // Construct the AST directly to isolate final emission from parsing.
        let mut expression = Expression::Select(Box::new(
            Select::new().column(Expression::number(7).alias("value")),
        ));
        for index in 0..5 {
            let source = Expression::Paren(Box::new(polyglot_sql::expressions::Paren {
                this: expression,
                trailing_comments: Vec::new(),
            }))
            .alias(format!("q{index}"));
            expression = Expression::Select(Box::new(
                Select::new()
                    .column(Expression::column("value"))
                    .from(source),
            ));
        }
        for target in [DialectType::Generic, DialectType::PostgreSQL] {
            let sql = emit(expression.clone(), DialectType::DuckDB, target).unwrap();
            assert_eq!(sql.matches("SELECT").count(), 6, "{sql}");
            assert!(sql.contains("7 AS value"), "{sql}");
        }
    }

    #[test]
    fn library_normalizes_source_functions_and_identifiers() {
        for (dialect, sql) in [
            (DialectType::BigQuery, "IFNULL(`amount`, 0)"),
            (DialectType::MySQL, "IFNULL(`amount`, 0)"),
            (DialectType::TSQL, "ISNULL([amount], 0)"),
        ] {
            let result = fragment(sql, dialect, Fragment::Scalar).unwrap();
            assert!(result.to_uppercase().contains("COALESCE"), "{result}");
            let expression = crate::core::parse_semantic_expression(&result).unwrap();
            assert!(matches!(expression, Expression::Coalesce(_)), "{result}");
        }
    }

    #[test]
    fn source_order_defaults_survive_the_intermediate_dialect() {
        let result = fragment("amount DESC", DialectType::PostgreSQL, Fragment::Order).unwrap();
        assert!(result.contains("NULLS FIRST"), "{result}");
        let result = fragment("amount ASC", DialectType::BigQuery, Fragment::Order).unwrap();
        assert!(result.contains("NULLS FIRST"), "{result}");
    }

    #[test]
    fn fragments_cannot_hide_trailing_clauses_or_statements() {
        for sql in ["amount LIMIT 1", "amount FROM orders", "amount; SELECT 2"] {
            assert!(
                fragment(sql, DialectType::PostgreSQL, Fragment::Scalar).is_err(),
                "{sql}"
            );
        }
        assert!(fragment(
            "amount DESC LIMIT 1",
            DialectType::BigQuery,
            Fragment::Order
        )
        .is_err());
    }

    #[test]
    fn interpolation_tokens_and_matching_literal_text_are_preserved() {
        let sql =
            "IFNULL({model}.`amount`, {{ minimum }}) + LENGTH('__sidemantic_dialect_token_0__')";
        let result = template_sql(sql, DialectType::BigQuery, Some(Fragment::Scalar)).unwrap();
        assert!(result.contains("{model}."), "{result}");
        assert!(result.contains("{{ minimum }}"), "{result}");
        assert!(
            result.contains("'__sidemantic_dialect_token_0__'"),
            "{result}"
        );
        let relationship = template_sql(
            "{from}.`customer_id` = {to}.`id`",
            DialectType::BigQuery,
            Some(Fragment::Scalar),
        )
        .unwrap();
        assert!(relationship.contains("{from}."), "{relationship}");
        assert!(relationship.contains("{to}."), "{relationship}");
    }
}
