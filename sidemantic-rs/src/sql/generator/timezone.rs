//! Query timezones localize UTC-stored timestamps before time bucketing.
use super::*;
use polyglot_sql::expressions::{Anonymous, AtTimeZone};

pub(super) fn validate_query_timezone(timezone: Option<&str>) -> Result<()> {
    if let Some(timezone) = timezone {
        if !timezone
            .chars()
            .all(|character| character.is_alphanumeric() || "_+-/".contains(character))
        {
            return Err(SidemanticError::Validation(format!(
                "Invalid timezone {timezone:?}: expected an IANA timezone name like \
                 'America/New_York' (letters, digits, '_', '/', '+', '-'). The value is \
                 embedded into generated SQL, so other characters are rejected."
            )));
        }
    }
    Ok(())
}

impl SqlGenerator<'_> {
    pub(super) fn localize_to_timezone(&self, column_expr: &str) -> Result<String> {
        let Some(timezone) = self.timezone.as_deref().filter(|value| !value.is_empty()) else {
            return Ok(column_expr.to_string());
        };
        validate_query_timezone(Some(timezone))?;
        // The caller has already resolved and quoted this expression for the
        // target dialect. Retain it as an AST leaf instead of reparsing it as DuckDB.
        let column = Expression::Raw(Raw {
            sql: format!("({column_expr})"),
        });
        let zone = Expression::Literal(Literal::String(timezone.to_string()));
        let utc = Expression::Literal(Literal::String("UTC".to_string()));
        let function = |name: &str, arguments: Vec<Expression>| {
            Expression::Anonymous(Box::new(Anonymous {
                this: Box::new(Expression::Identifier(Identifier::new(name))),
                expressions: arguments,
            }))
        };
        let localized = match self.dialect {
            DialectType::DuckDB | DialectType::PostgreSQL => {
                Expression::AtTimeZone(Box::new(AtTimeZone {
                    this: Expression::AtTimeZone(Box::new(AtTimeZone {
                        this: column,
                        zone: utc,
                    })),
                    zone,
                }))
            }
            DialectType::Snowflake => function("CONVERT_TIMEZONE", vec![utc, zone, column]),
            DialectType::BigQuery => function("DATETIME", vec![column, zone]),
            DialectType::Spark | DialectType::Databricks => {
                function("from_utc_timestamp", vec![column, zone])
            }
            DialectType::ClickHouse => function("toTimeZone", vec![column, zone]),
            _ => {
                return Err(SidemanticError::Validation(format!(
                    "Query timezone is not supported for dialect '{}'. Supported: \
                     duckdb, postgres, snowflake, bigquery, spark, databricks, clickhouse.",
                    self.dialect
                )))
            }
        };
        self.emit_expression(&localized)
    }
}
