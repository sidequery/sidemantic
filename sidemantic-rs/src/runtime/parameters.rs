//! Parameter-aware rendering for query filters and selected segments.
//!
//! MiniJinja evaluates conditions against raw values. Emitted values become
//! opaque markers until the SQL lexer establishes their quoting context.
//! The generic trusted SQL template renderer retains its separate raw API.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use minijinja::machinery::{tokenize, Token as TemplateToken};
use minijinja::value::{Value, ValueKind};
use minijinja::{Environment, Error, ErrorKind};
use polyglot_sql::dialects::{DialectImpl, SnowflakeDialect};
use polyglot_sql::expressions::Literal;
use polyglot_sql::tokens::{TokenType, Tokenizer};
use polyglot_sql::{Dialect, DialectType, Expression};

use crate::core::{Parameter, ParameterType};
use crate::semantic_input::dialects;

struct Captured {
    marker: String,
    sql: String,
    text: String,
}

fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::InvalidOperation, message.into())
}

fn capture(
    value: &Value,
    parameter: Option<&Parameter>,
    dialect: DialectType,
    prefix: &str,
    outputs: &Mutex<Vec<Captured>>,
) -> std::result::Result<String, Error> {
    let (sql, text) = if let Some(parameter) = parameter {
        let value = serde_yaml::to_value(value).map_err(|error| invalid(error.to_string()))?;
        let sql = super::format_parameter_value_in_dialect(parameter, &value, dialect)
            .map_err(invalid)?;
        let text = if matches!(
            parameter.parameter_type,
            ParameterType::String | ParameterType::Date
        ) {
            super::yaml_value_to_python_str(&value)
        } else {
            sql.clone()
        };
        (sql, text)
    } else {
        match value.kind() {
            ValueKind::String => {
                let text = value.as_str().expect("string value").to_owned();
                (string_literal(&text, dialect).map_err(invalid)?, text)
            }
            ValueKind::Bool => {
                let sql = if value.is_true() { "TRUE" } else { "FALSE" }.to_owned();
                (sql.clone(), sql)
            }
            ValueKind::Number => {
                let number = f64::try_from(value.clone())?;
                if !number.is_finite() {
                    return Err(invalid("Invalid numeric template value"));
                }
                let sql = value.to_string();
                (sql.clone(), sql)
            }
            ValueKind::None => ("NULL".into(), "NULL".into()),
            _ => return Err(invalid("Template output must be a scalar SQL value")),
        }
    };
    let mut outputs = outputs
        .lock()
        .map_err(|_| invalid("Parameter rendering state poisoned"))?;
    let marker = format!("{prefix}value_{}_end", outputs.len());
    outputs.push(Captured {
        marker: marker.clone(),
        sql,
        text,
    });
    Ok(marker)
}

fn string_literal(value: &str, dialect: DialectType) -> std::result::Result<String, String> {
    dialects::emit(
        Expression::Literal(Literal::String(value.to_owned())),
        DialectType::DuckDB,
        dialect,
    )
    .map_err(|error| error.to_string())
}

/// Wrap bare-name output expressions so declared string/unquoted/yesno types
/// remain available to the formatter. Tokenization leaves conditions, complex
/// expressions, quoted Jinja strings, comments and raw blocks untouched.
fn named_outputs(sql: &str, helper: &str) -> std::result::Result<String, String> {
    let tokens = tokenize(sql, false, Default::default(), Default::default())
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    let mut rendered = sql.to_owned();
    for window in tokens.windows(3).rev() {
        if let [(TemplateToken::VariableStart, _), (TemplateToken::Ident(name), span), (TemplateToken::VariableEnd, _)] =
            window
        {
            let start = span.start_offset as usize;
            let end = span.end_offset as usize;
            if sql.get(start..end) != Some(*name) {
                return Err("Invalid template token span".into());
            }
            rendered.replace_range(start..end, &format!("{helper}({name}, '{name}')"));
        }
    }
    Ok(rendered)
}

pub(super) fn render(
    sql: &str,
    parameters: &HashMap<String, &Parameter>,
    values: &HashMap<String, serde_yaml::Value>,
    dialect: DialectType,
) -> std::result::Result<String, String> {
    let context = super::build_runtime_context(parameters, values);
    let mut prefix = "__sidemantic_parameter_".to_owned();
    let context_text = serde_json::to_string(&context).map_err(|error| error.to_string())?;
    while sql.contains(&prefix) || context_text.contains(&prefix) {
        prefix.push('_');
    }
    let helper = format!("{prefix}capture");
    let template = named_outputs(sql, &helper)?;
    let definitions: HashMap<String, Parameter> = parameters
        .iter()
        .map(|(name, parameter)| (name.clone(), (**parameter).clone()))
        .collect();
    let outputs = Arc::new(Mutex::new(Vec::<Captured>::new()));
    let mut environment = Environment::new();
    let function_outputs = outputs.clone();
    let function_prefix = prefix.clone();
    environment.add_function(helper, move |value: Value, name: String| {
        capture(
            &value,
            definitions.get(&name),
            dialect,
            &function_prefix,
            &function_outputs,
        )
        .map(Value::from_safe_string)
    });
    let formatter_outputs = outputs.clone();
    environment.set_formatter(move |output, _state, value| {
        // The named-output helper already captured this marker. All remaining
        // expressions use their evaluated scalar type, matching Python.
        if let Some(text) = value.as_str() {
            let captured = formatter_outputs
                .lock()
                .map_err(|_| invalid("Parameter rendering state poisoned"))?;
            if captured.iter().any(|captured| captured.marker == text) {
                output.write_str(text)?;
                return Ok(());
            }
        }
        let marker = capture(value, None, dialect, &prefix, &formatter_outputs)?;
        output.write_str(&marker)?;
        Ok(())
    });
    let rendered = environment
        .template_from_str(&template)
        .and_then(|template| template.render(context))
        .map_err(|error| error.to_string())?;
    let outputs = outputs
        .lock()
        .map_err(|_| "Parameter rendering state poisoned".to_owned())?;
    replace_outputs(&rendered, &outputs, dialect)
}

fn replace_outputs(
    sql: &str,
    outputs: &[Captured],
    dialect: DialectType,
) -> std::result::Result<String, String> {
    let tokens = if dialect == DialectType::Snowflake {
        // 0.1.x omits this Snowflake lexer setting; escaped quotes still need to
        // be recognized before replacing outputs in an authored string literal.
        let mut config = SnowflakeDialect.tokenizer_config();
        config.string_escapes.push('\\');
        Tokenizer::new(config).tokenize(sql)
    } else {
        Dialect::get(dialect).tokenize(sql)
    }
    .map_err(|error| format!("Invalid SQL quoting in parameter template: {error}"))?;
    // The pinned SQL tokenizer uses character offsets, not UTF-8 byte offsets.
    let offsets: Vec<usize> = sql
        .char_indices()
        .map(|(offset, _)| offset)
        .chain([sql.len()])
        .collect();
    let mut replacements = BTreeMap::new();
    let mut literals: BTreeMap<(usize, usize), String> = BTreeMap::new();
    for output in outputs {
        let mut occurrences = sql.match_indices(&output.marker);
        let Some((position, _)) = occurrences.next() else {
            return Err("Parameter output requires a SQL value context".into());
        };
        if occurrences.next().is_some() {
            return Err("Ambiguous parameter output marker".into());
        }
        let token = tokens
            .iter()
            .find(|token| {
                offsets
                    .get(token.span.start)
                    .is_some_and(|start| *start <= position)
                    && offsets
                        .get(token.span.end)
                        .is_some_and(|end| position < *end)
            })
            .ok_or("Parameter output requires a SQL value context")?;
        let start = *offsets
            .get(token.span.start)
            .ok_or("Invalid SQL token span")?;
        let end = *offsets
            .get(token.span.end)
            .ok_or("Invalid SQL token span")?;
        if matches!(
            token.token_type,
            TokenType::String | TokenType::DollarString | TokenType::ByteString
        ) {
            let raw = sql.get(start..end).ok_or("Invalid SQL token span")?;
            if token.token_type == TokenType::ByteString
                || matches!(
                    dialect,
                    DialectType::MySQL
                        | DialectType::BigQuery
                        | DialectType::Snowflake
                        | DialectType::Spark
                        | DialectType::Databricks
                        | DialectType::Hive
                )
            {
                let prefix = &sql[start..position];
                if (prefix.len() - prefix.trim_end_matches('\\').len()) % 2 != 0 {
                    return Err(
                        "Parameter output cannot follow an unpaired SQL escape character".into(),
                    );
                }
            }
            let value = match literals.entry((start, end)) {
                std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::btree_map::Entry::Vacant(entry) => {
                    let normalized = dialects::fragment(raw, dialect, dialects::Fragment::Scalar)
                        .map_err(|error| error.to_string())?;
                    let expression = crate::core::parse_semantic_expression(&normalized)
                        .map_err(|error| error.to_string())?;
                    let text = match expression {
                        Expression::Literal(Literal::String(text)) => text,
                        Expression::Literal(Literal::DollarString(text)) => {
                            polyglot_sql::tokens::parse_dollar_string_token(&text).1
                        }
                        _ => {
                            return Err(
                                "Parameter output requires an ordinary SQL string literal".into()
                            )
                        }
                    };
                    entry.insert(text)
                }
            };
            *value = value.replace(&output.marker, &output.text);
        } else if token.token_type == TokenType::Var && token.text == output.marker {
            replacements.insert((start, end), output.sql.clone());
        } else {
            return Err("Parameter output requires a SQL value context".into());
        }
    }
    for (span, value) in literals {
        replacements.insert(span, string_literal(&value, dialect)?);
    }
    let mut rendered = sql.to_owned();
    for ((start, end), replacement) in replacements.into_iter().rev() {
        rendered.replace_range(start..end, &replacement);
    }
    Ok(rendered)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parameter(name: &str, kind: &str) -> Parameter {
        serde_json::from_value(json!({"name":name, "type":kind})).unwrap()
    }

    #[test]
    fn output_literals_preserve_quotes_backslashes_and_quoted_context() {
        let parameter = parameter("value", "string");
        let definitions = HashMap::from([("value".to_owned(), &parameter)]);
        for dialect in [DialectType::DuckDB, DialectType::Snowflake] {
            for value in [
                "O'Reilly\\folder",
                "\\' OR 1=1 --",
                "__sidemantic_parameter_value_0_end",
            ] {
                let values =
                    HashMap::from([("value".to_owned(), serde_yaml::Value::String(value.into()))]);
                for (template, expected) in [
                    ("{# c #}{{ value }}", value.to_owned()),
                    ("{# c #}'{{ value }}'", value.to_owned()),
                    (
                        "{# c #}'é prefix {{ value }} suffix'",
                        format!("é prefix {value} suffix"),
                    ),
                ] {
                    let rendered = render(template, &definitions, &values, dialect).unwrap();
                    let normalized =
                        dialects::fragment(&rendered, dialect, dialects::Fragment::Scalar).unwrap();
                    assert_eq!(
                        crate::core::parse_semantic_expression(&normalized).unwrap(),
                        Expression::Literal(Literal::String(expected))
                    );
                }
            }
        }
    }

    #[test]
    fn declared_types_do_not_change_raw_conditions() {
        let text = parameter("text", "string");
        let number = parameter("number", "number");
        let enabled = parameter("enabled", "yesno");
        let field = parameter("field", "unquoted");
        let definitions = HashMap::from([
            ("text".into(), &text),
            ("number".into(), &number),
            ("enabled".into(), &enabled),
            ("field".into(), &field),
        ]);
        let values =
            serde_yaml::from_str("text: 123\nnumber: 20\nenabled: true\nfield: orders.amount\n")
                .unwrap();
        let rendered = render(
            "{% if enabled and number > 10 and text == 123 %}{{ text }}, {{ field }}, {{ number }}, {{ enabled }}{% else %}FALSE{% endif %}",
            &definitions, &values, DialectType::DuckDB,
        ).unwrap();
        assert_eq!(rendered, "'123', orders.amount, 20, TRUE");
    }

    #[test]
    fn tokenizer_preserves_raw_blocks_and_strings_inside_expressions() {
        let parameter = parameter("value", "string");
        let definitions = HashMap::from([("value".to_owned(), &parameter)]);
        let values = HashMap::from([("value".to_owned(), serde_yaml::Value::String("ok".into()))]);
        assert_eq!(
            render(
                "'{% raw %}{{ value }}{% endraw %}' || {{ value }}",
                &definitions,
                &values,
                DialectType::DuckDB
            )
            .unwrap(),
            "'{{ value }}' || 'ok'"
        );
        assert_eq!(
            render(
                "{# c #}{{ '{{ value }}' }}",
                &definitions,
                &values,
                DialectType::DuckDB
            )
            .unwrap(),
            "'{{ value }}'"
        );
    }

    #[test]
    fn emitted_values_cannot_become_quoted_identifiers_or_token_suffixes() {
        let parameter = parameter("value", "string");
        let definitions = HashMap::from([("value".to_owned(), &parameter)]);
        let values = HashMap::from([(
            "value".to_owned(),
            serde_yaml::Value::String("amount".into()),
        )]);
        for template in [
            "{# c #}\"{{ value }}\"",
            "{# c #}prefix_{{ value }}",
            "{# c #}-- {{ value }}",
        ] {
            assert!(render(template, &definitions, &values, DialectType::DuckDB).is_err());
        }
    }
}
