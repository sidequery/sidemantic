//! Caller expressions cannot introduce physical reads or escape their clause.
//! Trusted segment and security definitions do not cross this request boundary.

use polyglot_sql::{expressions::Select, DialectType, Expression};
use serde_json::Value;

use crate::error::Result;

// Keep this contract aligned with sidemantic/sql/fragment.py. Unknown/UDF
// functions belong in trusted model SQL, not caller-supplied filters.
const SCALAR_FUNCTIONS: &str = "
ABS ACOS ASIN ATAN ATAN2 CEIL CEILING FLOOR ROUND SIGN SQRT CBRT POWER POW EXP LN LOG LOG2 LOG10
SIN COS TAN COT DEGREES RADIANS PI MOD GREATEST LEAST COALESCE NULLIF IF IIF CASE CAST TRY_CAST
LOWER UPPER LENGTH CHAR_LENGTH CHARACTER_LENGTH CONCAT CONCAT_WS SUBSTRING SUBSTR LEFT RIGHT
TRIM LTRIM RTRIM REPLACE REPEAT REVERSE LPAD RPAD SPLIT SPLIT_PART STARTS_WITH ENDS_WITH
CONTAINS POSITION STR_POSITION REGEXP_LIKE REGEXP_REPLACE REGEXP_EXTRACT REGEXP_SPLIT
COUNT SUM AVG MIN MAX MEDIAN STDDEV STDDEV_POP STDDEV_SAMP VARIANCE VAR_POP VAR_SAMP
DATE TIME TIMESTAMP DATE_TRUNC TIMESTAMP_TRUNC DATETIME_TRUNC TIME_TRUNC DATE_ADD DATE_SUB
DATE_DIFF DATEDIFF TIMESTAMP_ADD TIMESTAMP_SUB TIMESTAMP_DIFF EXTRACT YEAR MONTH DAY
DAY_OF_MONTH DAY_OF_WEEK DAY_OF_YEAR WEEK WEEK_OF_YEAR QUARTER HOUR MINUTE SECOND
CURRENT_DATE CURRENT_TIME CURRENT_TIMESTAMP CURRENT_DATETIME TIME_TO_STR STR_TO_TIME
TS_OR_DS_TO_DATE TS_OR_DS_TO_TIMESTAMP TS_OR_DS_TO_DATE_STR TIME_TO_UNIX UNIX_TO_TIME
DATE_TO_DATE_STR LAST_DAY DATE_FROM_PARTS TIMESTAMP_FROM_PARTS INTERVAL
ARRAY ARRAY_SIZE ARRAY_LENGTH ARRAY_CONTAINS ARRAY_SLICE ARRAY_TO_STRING
JSON_EXTRACT JSON_EXTRACT_SCALAR JSONB_EXTRACT JSONB_EXTRACT_SCALAR JSON_TYPE
STRUCT MAP EXISTS ISNULL IFNULL NVL";

fn normalized_name(name: &str) -> String {
    name.chars()
        .filter(|character| *character != '_')
        .flat_map(char::to_uppercase)
        .collect()
}

fn allowed_function(name: &str) -> bool {
    let name = normalized_name(name);
    SCALAR_FUNCTIONS
        .split_whitespace()
        .any(|allowed| normalized_name(allowed) == name)
}

fn validate_nodes(value: &Value, path: &str) -> Result<()> {
    // The pinned public AST walker omits typed function children. Inspect the
    // serialized tree, deserializing enum nodes to distinguish them from their
    // payload objects (which can also have a single field).
    if let Some(fields) = value.as_object().filter(|fields| fields.len() == 1) {
        if let Ok(expression) = serde_json::from_value::<Expression>(value.clone()) {
            let kind = fields.keys().next().unwrap().as_str();
            match expression {
                Expression::Table(_) => {
                    return Err(super::invalid(
                        path,
                        "Query expressions cannot introduce physical data sources",
                    ));
                }
                Expression::Select(select) if select.into.is_some() => {
                    return Err(super::invalid(
                        path,
                        "Query expressions cannot introduce physical data sources",
                    ));
                }
                Expression::Function(function) => {
                    if !allowed_function(&function.name) {
                        return Err(super::invalid(
                            path,
                            format!(
                                "Function {} is not allowed in query expressions",
                                function.name
                            ),
                        ));
                    }
                }
                Expression::AggregateFunction(function) => {
                    if !allowed_function(&function.name) {
                        return Err(super::invalid(
                            path,
                            format!(
                                "Function {} is not allowed in query expressions",
                                function.name
                            ),
                        ));
                    }
                }
                Expression::MethodCall(_) => {
                    return Err(super::invalid(
                        path,
                        "Qualified functions are not allowed in query expressions",
                    ));
                }
                _ => {
                    let structural = matches!(
                        kind,
                        "literal"
                            | "boolean"
                            | "null"
                            | "identifier"
                            | "column"
                            | "star"
                            | "select"
                            | "union"
                            | "intersect"
                            | "except"
                            | "subquery"
                            | "values"
                            | "alias"
                            | "and"
                            | "or"
                            | "xor"
                            | "add"
                            | "sub"
                            | "mul"
                            | "div"
                            | "eq"
                            | "neq"
                            | "lt"
                            | "lte"
                            | "gt"
                            | "gte"
                            | "like"
                            | "i_like"
                            | "bitwise_and"
                            | "bitwise_or"
                            | "bitwise_xor"
                            | "not"
                            | "neg"
                            | "bitwise_not"
                            | "in"
                            | "between"
                            | "is_null"
                            | "is_true"
                            | "is_false"
                            | "is"
                            | "from"
                            | "join"
                            | "where"
                            | "group_by"
                            | "having"
                            | "order_by"
                            | "ordered"
                            | "limit"
                            | "offset"
                            | "data_type"
                            | "tuple"
                            | "paren"
                            | "var"
                            | "dot"
                            | "bracket"
                            | "at_time_zone"
                            | "window"
                            | "window_function"
                            | "over"
                            | "within_group"
                            | "when"
                            | "whens"
                    );
                    let canonical = match kind {
                        "if_func" => "IF",
                        "safe_cast" => "TRY_CAST",
                        _ => kind,
                    };
                    if !structural && !allowed_function(canonical) {
                        return Err(super::invalid(
                            path,
                            format!("SQL expression {kind} is not allowed in query expressions"),
                        ));
                    }
                }
            }
        }
    }
    match value {
        Value::Object(fields) => {
            for child in fields.values() {
                validate_nodes(child, path)?;
            }
        }
        Value::Array(children) => {
            for child in children {
                validate_nodes(child, path)?;
            }
        }
        _ => {}
    }
    Ok(())
}

pub(super) fn validate_request_expression(
    sql: &str,
    dialect: DialectType,
    order: bool,
) -> Result<()> {
    let (path, prefix) = if order {
        ("query.order_by", "SELECT 1 ORDER BY ")
    } else {
        ("query.filters", "SELECT 1 WHERE ")
    };
    let parsed = super::dialects::parse(&format!("{prefix}{sql}"), dialect)
        .map_err(|error| super::invalid(path, format!("Invalid query expression: {error}")))?;
    let Expression::Select(mut select) = parsed else {
        return Err(super::invalid(
            path,
            "Query expression contains disallowed SQL",
        ));
    };
    let expression = if order {
        let clause = select
            .order_by
            .take()
            .filter(|clause| clause.expressions.len() == 1)
            .ok_or_else(|| super::invalid(path, "Expected one ordering expression"))?;
        Expression::OrderBy(Box::new(clause))
    } else {
        select
            .where_clause
            .take()
            .ok_or_else(|| super::invalid(path, "Expected one query expression"))?
            .this
    };
    select.expressions.clear();
    select.leading_comments.clear();
    select.post_select_comments.clear();
    if *select != Select::new() {
        return Err(super::invalid(
            path,
            "Query expression contains extra clauses",
        ));
    }
    let value = serde_json::to_value(expression).map_err(|error| super::invalid(path, error))?;
    validate_nodes(&value, path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn caller_filters_reject_reads_functions_and_clause_escapes() {
        super::super::with_semantic_stack(|| {
            for sql in [
                "EXISTS (SELECT 1 FROM secret_table)",
                "events.user_id IN (SELECT user_id FROM events_raw)",
                "EXISTS (SELECT 1 FROM read_csv('/tmp/private.csv'))",
                "1 = 1 UNION SELECT 1",
                "1 = 1 ORDER BY 1",
                "1 = 1; SELECT 2",
                "(SELECT pg_read_file('/tmp/private')) IS NOT NULL",
                "readfile('/tmp/private') IS NOT NULL",
                "lo_get(123) IS NOT NULL",
                "custom_schema.abs('/tmp/private') IS NOT NULL",
                "coalesce(readfile('/tmp/private'), '') != ''",
            ] {
                assert!(
                    validate_request_expression(sql, DialectType::DuckDB, false).is_err(),
                    "{sql}"
                );
            }
            for sql in [
                "count; SELECT 2",
                "count DESC LIMIT 1",
                "count, user_id",
                "random()",
            ] {
                assert!(
                    validate_request_expression(sql, DialectType::DuckDB, true).is_err(),
                    "{sql}"
                );
            }
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn caller_filters_keep_scalar_subqueries_literals_and_known_functions() {
        super::super::with_semantic_stack(|| {
            for sql in [
                "EXISTS (SELECT 1 WHERE 2 > 1)",
                "event_type != '; -- FROM secret_table'",
                "coalesce(user_id, 0) >= 1",
                "CASE WHEN user_id > 0 THEN user_id ELSE 0 END > 0",
                "lower(event_type) = 'signup'",
                "COUNT(DISTINCT user_id) > 0",
                "CAST(user_id AS VARCHAR) != ''",
            ] {
                validate_request_expression(sql, DialectType::DuckDB, false)?;
            }
            validate_request_expression("count DESC NULLS LAST", DialectType::DuckDB, true)?;
            validate_request_expression(
                "CASE WHEN user_id > 0 THEN user_id ELSE 0 END DESC",
                DialectType::DuckDB,
                true,
            )?;
            Ok(())
        })
        .unwrap();
    }
}
