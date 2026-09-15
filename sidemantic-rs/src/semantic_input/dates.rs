//! Source-aware corrections for date expressions emitted by polyglot-sql 0.1.x.

use polyglot_sql::expressions::{Function, Literal};
use polyglot_sql::{DialectType, Expression};
use serde_json::Value;

use crate::error::{Result, SidemanticError};

/// Repair Snowflake DATEDIFF after the complete library transpile pipeline.
/// The source parser stores (end, start), but DuckDB's generic generator emits
/// that order unchanged and leaves the unit unquoted. Restrict this correction
/// to that exact generated shape; canonical quoted-unit calls are untouched.
pub(super) fn normalize_transpiled(
    sql: &str,
    source: DialectType,
    target: DialectType,
) -> Result<String> {
    if source != DialectType::Snowflake || target != DialectType::DuckDB {
        return Ok(sql.to_owned());
    }
    let expression = super::dialects::parse(sql, target)?;
    let convert_error =
        |error: serde_json::Error| SidemanticError::SqlGeneration(error.to_string());
    let mut value = serde_json::to_value(expression).map_err(convert_error)?;
    if !normalize(&mut value).map_err(convert_error)? {
        return Ok(sql.to_owned());
    }
    let expression = serde_json::from_value(value).map_err(convert_error)?;
    polyglot_sql::generate(&expression, target)
        .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))
}

fn normalize(value: &mut Value) -> std::result::Result<bool, serde_json::Error> {
    // The library's AST visitor misses some typed-function children. Walk all
    // serialized descendants first so nested DATEDIFF calls are corrected too.
    let mut changed = false;
    match value {
        Value::Object(fields) => {
            for child in fields.values_mut() {
                changed |= normalize(child)?;
            }
            if fields.len() == 1 && fields.contains_key("function") {
                let Expression::Function(mut function) = serde_json::from_value(value.clone())?
                else {
                    unreachable!();
                };
                if function.name.eq_ignore_ascii_case("DATEDIFF") && function.args.len() == 3 {
                    if let Some(unit) = generated_unit(&function.args[0]) {
                        function.args.swap(1, 2);
                        function.args[0] = Expression::Literal(Literal::String(unit.clone()));
                        function.name = "DATE_DIFF".into();
                        if unit == "week" {
                            // Snowflake's default WEEK_START=0 (also 1) counts
                            // Monday boundaries. DuckDB's week difference counts
                            // complete seven-day spans unless both ends are
                            // truncated to their calendar weeks first.
                            for argument in &mut function.args[1..] {
                                *argument = Expression::Function(Box::new(Function::new(
                                    "DATE_TRUNC".to_owned(),
                                    vec![
                                        Expression::Literal(Literal::String("week".into())),
                                        argument.clone(),
                                    ],
                                )));
                            }
                        }
                        *value = serde_json::to_value(Expression::Function(function))?;
                        changed = true;
                    }
                }
            }
        }
        Value::Array(values) => {
            for child in values {
                changed |= normalize(child)?;
            }
        }
        _ => {}
    }
    Ok(changed)
}

fn generated_unit(expression: &Expression) -> Option<String> {
    let name = match expression {
        Expression::Column(column) if column.table.is_none() && !column.name.quoted => {
            &column.name.name
        }
        Expression::Identifier(identifier) if !identifier.quoted => &identifier.name,
        Expression::Var(variable) => &variable.this,
        _ => return None,
    };
    let unit = name.to_ascii_lowercase();
    matches!(
        unit.as_str(),
        "year"
            | "quarter"
            | "month"
            | "week"
            | "day"
            | "hour"
            | "minute"
            | "second"
            | "millisecond"
            | "microsecond"
            | "nanosecond"
    )
    .then_some(unit)
}

#[cfg(test)]
mod tests {
    use super::*;
    use polyglot_sql::expressions::DataType;

    fn transpile(sql: &str) -> String {
        let generated = polyglot_sql::Dialect::get(DialectType::Snowflake)
            .transpile_to(sql, DialectType::DuckDB)
            .unwrap();
        normalize_transpiled(&generated[0], DialectType::Snowflake, DialectType::DuckDB).unwrap()
    }

    fn projection(sql: &str) -> Expression {
        let Expression::Select(mut select) =
            polyglot_sql::parse_one(sql, DialectType::DuckDB).unwrap()
        else {
            panic!("expected SELECT");
        };
        select.expressions.remove(0)
    }

    fn date_function<'a>(expression: &'a Expression, name: &str, unit: &str) -> &'a [Expression] {
        let Expression::Function(function) = expression else {
            panic!("expected {name}, got {expression:?}");
        };
        assert!(function.name.eq_ignore_ascii_case(name));
        let Expression::Literal(Literal::String(actual_unit)) = &function.args[0] else {
            panic!("expected quoted date unit");
        };
        assert!(actual_unit.eq_ignore_ascii_case(unit));
        &function.args[1..]
    }

    fn date_column(expression: &Expression, name: &str) {
        // The library may insert DATE casts around these date-valued inputs.
        let expression = if let Expression::Cast(cast) = expression {
            assert_eq!(cast.to, DataType::Date);
            &cast.this
        } else {
            expression
        };
        let Expression::Column(column) = expression else {
            panic!("expected date column");
        };
        assert_eq!(column.name.name, name);
        assert!(column.table.is_none());
    }

    #[test]
    fn source_aliases_and_endpoint_order_are_normalized() {
        for unit in ["day", "'DD'", "month", "'MM'"] {
            let sql = transpile(&format!("SELECT DATEDIFF({unit}, start_date, end_date)"));
            let expected_unit = if unit == "day" || unit == "'DD'" {
                "day"
            } else {
                "month"
            };
            let expression = projection(&sql);
            let arguments = date_function(&expression, "DATE_DIFF", expected_unit);
            assert_eq!(arguments.len(), 2);
            date_column(&arguments[0], "start_date");
            date_column(&arguments[1], "end_date");
        }
    }

    #[test]
    fn week_uses_calendar_boundaries_and_nested_functions_are_visited() {
        let sql = transpile("SELECT COALESCE(DATEDIFF(wk, start_date, end_date), 0)");
        let Expression::Coalesce(coalesce) = projection(&sql) else {
            panic!("expected COALESCE");
        };
        assert_eq!(coalesce.expressions.len(), 2);
        let arguments = date_function(&coalesce.expressions[0], "DATE_DIFF", "week");
        assert_eq!(arguments.len(), 2);
        for (argument, column) in arguments.iter().zip(["start_date", "end_date"]) {
            let truncated = date_function(argument, "DATE_TRUNC", "week");
            assert_eq!(truncated.len(), 1);
            date_column(&truncated[0], column);
        }
        assert_eq!(
            coalesce.expressions[1],
            Expression::Literal(Literal::Number("0".into()))
        );
    }

    #[test]
    fn quoted_units_and_other_dialect_paths_are_not_changed() {
        let canonical = "SELECT DATEDIFF('day', start_date, end_date)";
        assert_eq!(
            normalize_transpiled(canonical, DialectType::Snowflake, DialectType::DuckDB).unwrap(),
            canonical
        );
        let generated = "SELECT DATEDIFF(DAY, end_date, start_date)";
        for (source, target) in [
            (DialectType::DuckDB, DialectType::DuckDB),
            (DialectType::Snowflake, DialectType::PostgreSQL),
        ] {
            assert_eq!(
                normalize_transpiled(generated, source, target).unwrap(),
                generated
            );
        }
        let once =
            normalize_transpiled(generated, DialectType::Snowflake, DialectType::DuckDB).unwrap();
        assert_eq!(
            normalize_transpiled(&once, DialectType::Snowflake, DialectType::DuckDB).unwrap(),
            once
        );
    }
}
