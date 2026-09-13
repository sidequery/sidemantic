//! A semantic key expression always binds its inputs to the physical source.
//! Reusing a key's own name in its SQL is a raw column reference, not recursion.

use std::collections::{HashMap, HashSet};

use polyglot_sql::{DialectType, Expression};

use super::{
    parse_semantic_expression, replace_semantic_columns, semantic_column_references, Model,
    SemanticGraph,
};
use crate::error::{Result, SidemanticError};

fn unsupported() -> SidemanticError {
    SidemanticError::UnsupportedSemanticFeatures {
        capabilities: vec!["dimension.computed_key_expression".into()],
    }
}

// Deliberately exclude aggregates, windows, subqueries and arbitrary functions.
// These row-local operators cannot depend on another row or evaluation order.
fn deterministic_scalar(expression: &Expression) -> bool {
    match expression {
        Expression::Column(column) => !column.join_mark,
        Expression::Literal(_) => true,
        Expression::Add(binary)
        | Expression::Sub(binary)
        | Expression::Mul(binary)
        | Expression::Div(binary)
        | Expression::Mod(binary)
        | Expression::Concat(binary) => {
            deterministic_scalar(&binary.left) && deterministic_scalar(&binary.right)
        }
        Expression::Paren(paren) => deterministic_scalar(&paren.this),
        Expression::Neg(unary) => deterministic_scalar(&unary.this),
        Expression::Cast(cast) => {
            cast.format.is_none() && cast.default.is_none() && deterministic_scalar(&cast.this)
        }
        Expression::DPipe(concat) => {
            concat.safe.is_none()
                && deterministic_scalar(&concat.this)
                && deterministic_scalar(&concat.expression)
        }
        Expression::Coalesce(arguments) => arguments.expressions.iter().all(deterministic_scalar),
        _ => false,
    }
}

fn source_column(alias: Option<&str>, key: &str) -> Result<Expression> {
    fn identifier(name: &str) -> String {
        let mut chars = name.chars();
        if chars
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            name.to_string()
        } else {
            format!("\"{}\"", name.replace('"', "\"\""))
        }
    }
    parse_semantic_expression(&alias.map_or_else(
        || identifier(key),
        |alias| format!("{}.{}", identifier(alias), identifier(key)),
    ))
}

/// Validate and bind a declared key (including a foreign-key dimension).
/// SQL inputs remain physical columns even when their names shadow dimensions.
pub fn key_expression(
    model: &Model,
    key: &str,
    alias: Option<&str>,
    dialect: DialectType,
) -> Result<Expression> {
    let Some(dimension) = model.get_dimension(key) else {
        return source_column(alias, key);
    };
    if dimension.window.is_some() {
        return Err(unsupported());
    }
    let sql = dimension
        .sql
        .as_deref()
        .map(str::to_string)
        .unwrap_or_else(|| format!("\"{}\"", key.replace('"', "\"\"")));
    let sql = sql.replace("{model}", &model.name);
    let expression = parse_semantic_expression(&sql)?;
    let identity = matches!(&expression, Expression::Column(column)
        if column.name.name == key && column.table.as_ref().is_none_or(|owner| owner.name == model.name));
    if !identity && dimension.granularity.is_some() {
        return Err(unsupported());
    }
    if !deterministic_scalar(&expression) {
        return Err(unsupported());
    }
    let columns = semantic_column_references(&sql)?;
    if columns.is_empty() {
        return Err(unsupported());
    }
    let mut replacements = HashMap::new();
    for column in columns {
        if column
            .model
            .as_deref()
            .is_some_and(|owner| owner != model.name)
            || column.aggregate_input
        {
            return Err(unsupported());
        }
        let replacement = source_column(alias, &column.field)?;
        let replacement = polyglot_sql::generate(&replacement, dialect)
            .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
        replacements.insert((column.model, column.field), replacement);
    }
    replace_semantic_columns(expression, &replacements)
}

pub fn semantic_key_names(graph: &SemanticGraph, model: &Model) -> HashSet<String> {
    let mut keys: HashSet<_> = model.primary_keys().into_iter().collect();
    for source in graph.models() {
        for relationship in &source.relationships {
            if !relationship.active {
                continue;
            }
            if source.name == model.name {
                keys.extend(relationship.foreign_key_columns());
            }
            if relationship.related_model() == model.name {
                keys.extend(relationship.primary_key_columns());
            }
        }
    }
    keys
}

pub fn is_computed_key(model: &Model, key: &str) -> Result<bool> {
    let expression = key_expression(model, key, None, DialectType::DuckDB)?;
    // Column replacement nodes are Raw; parse the bound result to distinguish
    // an identity column from a renamed column or a computed expression.
    let sql = polyglot_sql::generate(&expression, DialectType::DuckDB)
        .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?;
    Ok(
        !matches!(parse_semantic_expression(&sql)?, Expression::Column(column) if column.name.name == key && column.table.is_none()),
    )
}

pub fn has_computed_keys(graph: &SemanticGraph, model: &Model) -> Result<bool> {
    for key in semantic_key_names(graph, model) {
        if is_computed_key(model, &key)? {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::Dimension;

    #[test]
    fn computed_identity_binds_every_input_without_recursing_into_its_own_name() {
        let model = Model::new("accounts", "id")
            .with_dimension(Dimension::new("id").with_sql("{model}.tenant * 100 + id"));
        let expression = key_expression(&model, "id", Some("a"), DialectType::DuckDB).unwrap();
        let sql = polyglot_sql::generate(&expression, DialectType::DuckDB).unwrap();
        assert_eq!(sql, "a.tenant * 100 + a.id");
        assert!(is_computed_key(&model, "id").unwrap());
        let expression = key_expression(&model, "id", None, DialectType::DuckDB).unwrap();
        assert_eq!(
            polyglot_sql::generate(&expression, DialectType::DuckDB).unwrap(),
            "tenant * 100 + id"
        );
    }

    #[test]
    fn identity_and_renamed_keys_are_distinguished() {
        for (sql, computed) in [
            ("id", false),
            ("\"accounts\".\"id\"", false),
            ("raw_id", true),
        ] {
            let model =
                Model::new("accounts", "id").with_dimension(Dimension::new("id").with_sql(sql));
            assert_eq!(is_computed_key(&model, "id").unwrap(), computed);
        }
    }

    #[test]
    fn deterministic_string_keys_preserve_raw_column_scope() {
        let model = Model::new("accounts", "id").with_dimension(
            Dimension::new("id").with_sql("CAST(tenant AS VARCHAR) || ':' || CAST(id AS VARCHAR)"),
        );
        let expression = key_expression(&model, "id", Some("a"), DialectType::DuckDB).unwrap();
        let sql = polyglot_sql::generate(&expression, DialectType::DuckDB).unwrap();
        assert!(sql.contains("a.tenant"), "{sql}");
        assert!(sql.contains("a.id"), "{sql}");
        assert!(sql.contains("':'"), "{sql}");
    }
}
