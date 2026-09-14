//! Compatibility for Snowflake string literals in polyglot-sql 0.1.x.
//!
//! Its Snowflake tokenizer preserves backslashes and cannot read escaped quotes;
//! its string generator expects backslashes to have been escaped already.
//! Keep this adjustment at the syntax boundary, before semantic binding.

use std::borrow::Cow;

use polyglot_sql::dialects::{DialectImpl, SnowflakeDialect};
use polyglot_sql::expressions::Literal;
use polyglot_sql::tokens::{TokenType, Tokenizer};
use polyglot_sql::{DialectType, Expression};
use serde_json::Value;

use crate::error::{Result, SidemanticError};

/// Convert authored Snowflake strings to the representation its library parser
/// expects. Call once on the original SQL immediately before each library parse
/// or transpile call; never feed the returned text through this helper again.
/// Dollar strings, identifiers, comments, and all other SQL remain untouched.
pub(super) fn source_sql(sql: &str, source: DialectType) -> Result<Cow<'_, str>> {
    if source != DialectType::Snowflake || !sql.contains('\\') {
        return Ok(Cow::Borrowed(sql));
    }
    let mut config = SnowflakeDialect.tokenizer_config();
    // Use the existing lexer to locate whole literals, including escaped quotes.
    // Decode raw spans below because the lexer lacks Snowflake Unicode/octal
    // escapes and accepts extra escape sequences that Snowflake does not.
    config.string_escapes.push('\\');
    let tokens = Tokenizer::new(config)
        .tokenize(sql)
        .map_err(|error| SidemanticError::SqlParse(error.to_string()))?;
    // In 0.1.x spans count Unicode scalar positions despite their byte-offset
    // documentation. Convert them to byte offsets before slicing UTF-8 text.
    let offsets: Vec<usize> = sql
        .char_indices()
        .map(|(offset, _)| offset)
        .chain([sql.len()])
        .collect();
    let mut output = String::with_capacity(sql.len());
    let mut previous = 0;
    for token in tokens {
        if token.token_type != TokenType::String {
            continue;
        }
        let start = *offsets.get(token.span.start).ok_or_else(invalid_escape)?;
        let end = *offsets.get(token.span.end).ok_or_else(invalid_escape)?;
        let raw = sql.get(start..end).ok_or_else(invalid_escape)?;
        if !raw.starts_with('\'') || !raw.contains('\\') {
            continue;
        }
        let value = decode_single_quoted(raw)?;
        output.push_str(&sql[previous..start]);
        // The unmodified Snowflake library parser reads standard doubled quotes
        // and literal backslashes, so DuckDB literal emission is its input form.
        output.push_str(
            &polyglot_sql::generate(
                &Expression::Literal(Literal::String(value)),
                DialectType::DuckDB,
            )
            .map_err(|error| SidemanticError::SqlGeneration(error.to_string()))?,
        );
        previous = end;
    }
    if previous == 0 {
        return Ok(Cow::Borrowed(sql));
    }
    output.push_str(&sql[previous..]);
    Ok(Cow::Owned(output))
}

/// Prepare canonical DuckDB literal values for the Snowflake generator. Apply
/// exactly once before emitting the source SQL for the full transpile pipeline.
/// Other sources must first enter the canonical DuckDB representation.
pub(super) fn target_expression(expression: Expression, target: DialectType) -> Result<Expression> {
    if target != DialectType::Snowflake {
        return Ok(expression);
    }
    fn escape(value: &mut Value) -> std::result::Result<(), serde_json::Error> {
        // The pinned library traversal omits some typed-function children.
        // Walk the serialized AST exhaustively, matching complete literal nodes
        // and leaving identifier/metadata strings unchanged.
        match value {
            Value::Object(fields) if fields.len() == 1 && fields.contains_key("literal") => {
                let expression = serde_json::from_value::<Expression>(value.clone())?;
                let text = match expression {
                    Expression::Literal(Literal::String(text)) => Some(text),
                    Expression::Literal(Literal::DollarString(text)) => {
                        Some(polyglot_sql::tokens::parse_dollar_string_token(&text).1)
                    }
                    _ => None,
                };
                if let Some(text) = text {
                    *value = serde_json::to_value(Expression::Literal(Literal::String(
                        text.replace('\\', "\\\\"),
                    )))?;
                }
            }
            Value::Object(fields) => {
                for child in fields.values_mut() {
                    escape(child)?;
                }
            }
            Value::Array(values) => {
                for child in values {
                    escape(child)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    let convert_error =
        |error: serde_json::Error| SidemanticError::SqlGeneration(error.to_string());
    let mut value = serde_json::to_value(expression).map_err(convert_error)?;
    escape(&mut value).map_err(convert_error)?;
    serde_json::from_value(value).map_err(convert_error)
}

fn invalid_escape() -> SidemanticError {
    SidemanticError::SqlParse("Invalid Snowflake string escape".into())
}

/// Snowflake's documented escape table, including unknown-escape behavior:
/// https://docs.snowflake.com/en/sql-reference/data-types-text#escape-sequences-in-single-quoted-string-constants
fn decode_single_quoted(raw: &str) -> Result<String> {
    let mut characters = raw[1..raw.len() - 1].chars().peekable();
    let mut value = String::new();
    while let Some(character) = characters.next() {
        if character == '\'' && characters.peek() == Some(&'\'') {
            characters.next();
            value.push('\'');
            continue;
        }
        if character != '\\' {
            value.push(character);
            continue;
        }
        let escaped = characters.next().ok_or_else(invalid_escape)?;
        let decoded = match escaped {
            '\'' | '"' | '\\' => escaped,
            'b' => '\u{0008}',
            'f' => '\u{000c}',
            'n' => '\n',
            'r' => '\r',
            't' => '\t',
            '0'..='7' => {
                // A lone \\0 is NUL; a three-digit octal escape is ASCII.
                if escaped == '0' && !characters.peek().is_some_and(|c| matches!(c, '0'..='7')) {
                    '\0'
                } else {
                    let mut code = escaped.to_digit(8).unwrap();
                    for _ in 0..2 {
                        let digit = characters
                            .next()
                            .and_then(|c| c.to_digit(8))
                            .ok_or_else(invalid_escape)?;
                        code = code * 8 + digit;
                    }
                    char::from_u32(code).ok_or_else(invalid_escape)?
                }
            }
            'x' | 'u' => {
                let digits = if escaped == 'x' { 2 } else { 4 };
                let mut code = 0;
                for _ in 0..digits {
                    let digit = characters
                        .next()
                        .and_then(|c| c.to_digit(16))
                        .ok_or_else(invalid_escape)?;
                    code = code * 16 + digit;
                }
                char::from_u32(code).ok_or_else(invalid_escape)?
            }
            // Snowflake ignores the slash in unrecognized sequences, unlike
            // the library tokenizer's generic escape behavior.
            other => other,
        };
        value.push(decoded);
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn first_literal(sql: &str, dialect: DialectType) -> String {
        let Expression::Select(select) = polyglot_sql::parse_one(sql, dialect).unwrap() else {
            panic!("expected SELECT");
        };
        let Expression::Literal(Literal::String(value)) = &select.expressions[0] else {
            panic!("expected string literal");
        };
        value.clone()
    }

    #[test]
    fn authored_escape_sequences_have_documented_values() {
        for (source, expected) in [
            (r"'C:\\user'", r"C:\user"),
            (r"'can\'t'", "can't"),
            (r"'can''t\n'", "can't\n"),
            (r"'\b\f\n\r\t\0'", "\u{0008}\u{000c}\n\r\t\0"),
            (r"'-\041-\x21-\u26c4-'", "-!-!-⛄-"),
            (r"'\z\a\v\Z'", "zavZ"),
        ] {
            let sql = format!("SELECT {source}");
            let prepared = source_sql(&sql, DialectType::Snowflake).unwrap();
            let generated = polyglot_sql::Dialect::get(DialectType::Snowflake)
                .transpile_to(&prepared, DialectType::DuckDB)
                .unwrap();
            assert_eq!(first_literal(&generated[0], DialectType::DuckDB), expected);
        }
    }

    #[test]
    fn unicode_escapes_follow_snowflake_four_digit_contract() {
        // Snowflake explicitly does not support BigQuery's eight-digit escape:
        // https://docs.snowflake.com/en/migrations/aim-for-datawarehouses/code-conversion/issues-and-troubleshooting/conversion-issues/bigqueryEWI#ssc-ewi-bq0008
        // Unknown escapes drop only the backslash, per the string escape table.
        for (source, expected) in [
            (r"'\U0001F600'", "U0001F600"),
            (r"'\U00110000'", "U00110000"),
            (r"'\Uxyz'", "Uxyz"),
            (r"'\u0000'", "\0"),
            (r"'\uD7FF'", "\u{d7ff}"),
            (r"'\uE000'", "\u{e000}"),
            (r"'\uFFFF'", "\u{ffff}"),
            (r"'\u0041B'", "AB"),
        ] {
            let sql = format!("SELECT {source}");
            let prepared = source_sql(&sql, DialectType::Snowflake).unwrap();
            assert_eq!(first_literal(&prepared, DialectType::Snowflake), expected);
        }
    }

    #[test]
    fn preprocessing_preserves_unicode_offsets_identifiers_comments_and_dollars() {
        let sql = "SELECT 'é', '\\u26c4', $$raw\\n'$$, \"id\\n\" -- \\u1234\n";
        let prepared = source_sql(sql, DialectType::Snowflake).unwrap();
        assert_eq!(
            prepared,
            "SELECT 'é', '⛄', $$raw\\n'$$, \"id\\n\" -- \\u1234\n"
        );
    }

    #[test]
    fn canonical_values_survive_snowflake_output_and_input() {
        for value in [
            r"C:\user",
            "slash\\'quote",
            "\n\t\r\u{0008}\u{000c}\0",
            r"literal\u26c4",
            "é⛄$$",
        ] {
            let expression = Expression::Literal(Literal::String(value.into()));
            let prepared = target_expression(expression, DialectType::Snowflake).unwrap();
            let source = polyglot_sql::generate(&prepared, DialectType::DuckDB).unwrap();
            let snowflake = polyglot_sql::Dialect::get(DialectType::DuckDB)
                .transpile_to(&format!("SELECT {source}"), DialectType::Snowflake)
                .unwrap();
            let decoded = source_sql(&snowflake[0], DialectType::Snowflake).unwrap();
            assert_eq!(first_literal(&decoded, DialectType::Snowflake), value);
        }
    }

    #[test]
    fn malformed_escapes_fail_instead_of_changing_the_value() {
        for sql in [
            r"SELECT '\x2'",
            r"SELECT '\u123'",
            r"SELECT '\u12G4'",
            r"SELECT '\uD800'",
            r"SELECT '\uDFFF'",
            r"SELECT '\04'",
        ] {
            assert!(source_sql(sql, DialectType::Snowflake).is_err(), "{sql}");
        }
    }

    #[test]
    fn nested_typed_function_literals_receive_target_escaping() {
        use polyglot_sql::expressions::{BinaryOp, RegexpReplaceFunc};

        let literal = |text: &str| Expression::Literal(Literal::String(text.into()));
        let expression = Expression::RegexpReplace(Box::new(RegexpReplaceFunc {
            this: Expression::Concat(Box::new(BinaryOp::new(
                literal(r"root\"),
                literal(r"leaf\"),
            ))),
            pattern: literal(r"\w"),
            replacement: literal(r"\1"),
            flags: None,
        }));
        let escaped = target_expression(expression, DialectType::Snowflake).unwrap();
        let generated = polyglot_sql::generate(&escaped, DialectType::Snowflake).unwrap();
        for expected in [r"'root\\'", r"'leaf\\'", r"'\\w'", r"'\\1'"] {
            assert!(generated.contains(expected), "{generated}");
        }
        let query = format!("SELECT {generated}");
        let decoded = source_sql(&query, DialectType::Snowflake).unwrap();
        let Expression::Select(select) =
            polyglot_sql::parse_one(&decoded, DialectType::Snowflake).unwrap()
        else {
            panic!("expected SELECT");
        };
        let Expression::RegexpReplace(function) = &select.expressions[0] else {
            panic!("expected typed regexp_replace");
        };
        assert_eq!(function.pattern, literal(r"\w"));
        assert_eq!(function.replacement, literal(r"\1"));
        assert_eq!(
            function.this,
            Expression::Concat(Box::new(BinaryOp::new(
                literal(r"root\"),
                literal(r"leaf\")
            )))
        );
    }
}
