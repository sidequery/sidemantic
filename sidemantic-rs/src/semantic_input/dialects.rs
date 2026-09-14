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
    polyglot_sql::parse_one(&sql, source)
        .map_err(|error| SidemanticError::SqlParse(error.to_string()))
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
