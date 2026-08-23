use dax_parser::{
    format_expression_with_options, format_expression_with_style, format_query_with_options,
    format_query_with_style, lex_with_dialect, parse_expression_lossless_with_dialect,
    parse_expression_with_dialect, parse_query_lossless_with_dialect, parse_query_with_dialect,
    recover_expression_with_dialect, recover_query_with_dialect, validate_expression,
    validate_expression_against_model, validate_query, validate_query_against_model, Dialect,
    FormatOptions, FormatStyle, ModelMetadata, ModelTable, TokenKind, ValidationOptions,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

fn to_json(value: impl serde::Serialize) -> PyResult<String> {
    serde_json::to_string(&value).map_err(|err| PyValueError::new_err(err.to_string()))
}

fn dialect(
    allow_semicolon_separators: bool,
    allow_decimal_comma: bool,
    allow_dash_dash_comments: bool,
    allow_double_slash_comments: bool,
    allow_block_comments: bool,
) -> Dialect {
    Dialect {
        allow_semicolon_separators,
        allow_decimal_comma,
        allow_dash_dash_comments,
        allow_double_slash_comments,
        allow_block_comments,
    }
}

fn format_options(localized: bool) -> FormatOptions {
    if localized {
        FormatOptions::localized()
    } else {
        FormatOptions::canonical()
    }
}

fn model_metadata(input: &str) -> PyResult<ModelMetadata> {
    let raw: serde_json::Value =
        serde_json::from_str(input).map_err(|err| PyValueError::new_err(err.to_string()))?;
    let tables = raw
        .get("tables")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| PyValueError::new_err("model metadata must contain a tables array"))?;
    let tables = tables
        .iter()
        .map(|table| {
            let name = table
                .get("name")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| PyValueError::new_err("model table name must be a string"))?;
            let string_list = |field: &str| -> PyResult<Vec<String>> {
                table
                    .get(field)
                    .and_then(serde_json::Value::as_array)
                    .ok_or_else(|| {
                        PyValueError::new_err(format!("model table {field} must be an array"))
                    })?
                    .iter()
                    .map(|value| {
                        value.as_str().map(str::to_owned).ok_or_else(|| {
                            PyValueError::new_err(format!(
                                "model table {field} values must be strings"
                            ))
                        })
                    })
                    .collect()
            };
            Ok(ModelTable {
                name: name.to_owned(),
                columns: string_list("columns")?,
                measures: string_list("measures")?,
            })
        })
        .collect::<PyResult<Vec<_>>>()?;
    Ok(ModelMetadata { tables })
}

#[pyfunction(
    name = "parse_expression",
    signature = (
        input,
        allow_semicolon_separators=true,
        allow_decimal_comma=false,
        allow_dash_dash_comments=true,
        allow_double_slash_comments=true,
        allow_block_comments=true
    )
)]
fn parse_expression_py(
    input: &str,
    allow_semicolon_separators: bool,
    allow_decimal_comma: bool,
    allow_dash_dash_comments: bool,
    allow_double_slash_comments: bool,
    allow_block_comments: bool,
) -> PyResult<String> {
    let expr = parse_expression_with_dialect(
        input,
        dialect(
            allow_semicolon_separators,
            allow_decimal_comma,
            allow_dash_dash_comments,
            allow_double_slash_comments,
            allow_block_comments,
        ),
    )
    .map_err(|err| PyValueError::new_err(err.to_string()))?;
    to_json(expr)
}

#[pyfunction(
    name = "parse_query",
    signature = (
        input,
        allow_semicolon_separators=true,
        allow_decimal_comma=false,
        allow_dash_dash_comments=true,
        allow_double_slash_comments=true,
        allow_block_comments=true
    )
)]
fn parse_query_py(
    input: &str,
    allow_semicolon_separators: bool,
    allow_decimal_comma: bool,
    allow_dash_dash_comments: bool,
    allow_double_slash_comments: bool,
    allow_block_comments: bool,
) -> PyResult<String> {
    let query = parse_query_with_dialect(
        input,
        dialect(
            allow_semicolon_separators,
            allow_decimal_comma,
            allow_dash_dash_comments,
            allow_double_slash_comments,
            allow_block_comments,
        ),
    )
    .map_err(|err| PyValueError::new_err(err.to_string()))?;
    to_json(query)
}

#[pyfunction(
    name = "lex",
    signature = (
        input,
        allow_semicolon_separators=true,
        allow_decimal_comma=false,
        allow_dash_dash_comments=true,
        allow_double_slash_comments=true,
        allow_block_comments=true
    )
)]
fn lex_py(
    input: &str,
    allow_semicolon_separators: bool,
    allow_decimal_comma: bool,
    allow_dash_dash_comments: bool,
    allow_double_slash_comments: bool,
    allow_block_comments: bool,
) -> PyResult<String> {
    let tokens = lex_with_dialect(
        input,
        dialect(
            allow_semicolon_separators,
            allow_decimal_comma,
            allow_dash_dash_comments,
            allow_double_slash_comments,
            allow_block_comments,
        ),
    )
    .map_err(|err| PyValueError::new_err(err.to_string()))?;
    to_json(tokens)
}

#[pyfunction(
    name = "validate_expression",
    signature = (
        input,
        allow_semicolon_separators=true,
        allow_decimal_comma=false,
        allow_dash_dash_comments=true,
        allow_double_slash_comments=true,
        allow_block_comments=true,
        report_unrecognized_functions=false
    )
)]
fn validate_expression_py(
    input: &str,
    allow_semicolon_separators: bool,
    allow_decimal_comma: bool,
    allow_dash_dash_comments: bool,
    allow_double_slash_comments: bool,
    allow_block_comments: bool,
    report_unrecognized_functions: bool,
) -> PyResult<String> {
    let expr = parse_expression_with_dialect(
        input,
        dialect(
            allow_semicolon_separators,
            allow_decimal_comma,
            allow_dash_dash_comments,
            allow_double_slash_comments,
            allow_block_comments,
        ),
    )
    .map_err(|err| PyValueError::new_err(err.to_string()))?;
    to_json(validate_expression(
        &expr,
        ValidationOptions {
            report_unrecognized_functions,
        },
    ))
}

#[pyfunction(
    name = "validate_query",
    signature = (
        input,
        allow_semicolon_separators=true,
        allow_decimal_comma=false,
        allow_dash_dash_comments=true,
        allow_double_slash_comments=true,
        allow_block_comments=true,
        report_unrecognized_functions=false
    )
)]
fn validate_query_py(
    input: &str,
    allow_semicolon_separators: bool,
    allow_decimal_comma: bool,
    allow_dash_dash_comments: bool,
    allow_double_slash_comments: bool,
    allow_block_comments: bool,
    report_unrecognized_functions: bool,
) -> PyResult<String> {
    let query = parse_query_with_dialect(
        input,
        dialect(
            allow_semicolon_separators,
            allow_decimal_comma,
            allow_dash_dash_comments,
            allow_double_slash_comments,
            allow_block_comments,
        ),
    )
    .map_err(|err| PyValueError::new_err(err.to_string()))?;
    to_json(validate_query(
        &query,
        ValidationOptions {
            report_unrecognized_functions,
        },
    ))
}

#[pyfunction(
    name = "format_expression",
    signature = (
        input,
        localized=false,
        allow_semicolon_separators=true,
        allow_decimal_comma=false,
        allow_dash_dash_comments=true,
        allow_double_slash_comments=true,
        allow_block_comments=true,
        sqlbi=false
    )
)]
#[allow(clippy::too_many_arguments)]
fn format_expression_py(
    input: &str,
    localized: bool,
    allow_semicolon_separators: bool,
    allow_decimal_comma: bool,
    allow_dash_dash_comments: bool,
    allow_double_slash_comments: bool,
    allow_block_comments: bool,
    sqlbi: bool,
) -> PyResult<String> {
    let dialect = dialect(
        allow_semicolon_separators,
        allow_decimal_comma,
        allow_dash_dash_comments,
        allow_double_slash_comments,
        allow_block_comments,
    );
    let has_formula_marker = sqlbi
        && lex_with_dialect(input, dialect)
            .map_err(|err| PyValueError::new_err(err.to_string()))?
            .iter()
            .find(|token| !matches!(token.kind, TokenKind::DocComment(_)))
            .is_some_and(|token| matches!(token.kind, TokenKind::Eq));
    let expr = parse_expression_with_dialect(input, dialect)
        .map_err(|err| PyValueError::new_err(err.to_string()))?;
    let options = format_options(localized);
    let formatted = if sqlbi {
        format_expression_with_style(&expr, options, FormatStyle::Sqlbi)
    } else {
        format_expression_with_options(&expr, options)
    };
    if has_formula_marker {
        Ok(format!("=\n{formatted}"))
    } else {
        Ok(formatted)
    }
}

#[pyfunction(
    name = "format_query",
    signature = (
        input,
        localized=false,
        allow_semicolon_separators=true,
        allow_decimal_comma=false,
        allow_dash_dash_comments=true,
        allow_double_slash_comments=true,
        allow_block_comments=true,
        sqlbi=false
    )
)]
#[allow(clippy::too_many_arguments)]
fn format_query_py(
    input: &str,
    localized: bool,
    allow_semicolon_separators: bool,
    allow_decimal_comma: bool,
    allow_dash_dash_comments: bool,
    allow_double_slash_comments: bool,
    allow_block_comments: bool,
    sqlbi: bool,
) -> PyResult<String> {
    let query = parse_query_with_dialect(
        input,
        dialect(
            allow_semicolon_separators,
            allow_decimal_comma,
            allow_dash_dash_comments,
            allow_double_slash_comments,
            allow_block_comments,
        ),
    )
    .map_err(|err| PyValueError::new_err(err.to_string()))?;
    let options = format_options(localized);
    Ok(if sqlbi {
        format_query_with_style(&query, options, FormatStyle::Sqlbi)
    } else {
        format_query_with_options(&query, options)
    })
}

#[pyfunction(
    name = "parse_expression_lossless",
    signature = (
        input,
        allow_semicolon_separators=true,
        allow_decimal_comma=false,
        allow_dash_dash_comments=true,
        allow_double_slash_comments=true,
        allow_block_comments=true
    )
)]
fn parse_expression_lossless_py(
    input: &str,
    allow_semicolon_separators: bool,
    allow_decimal_comma: bool,
    allow_dash_dash_comments: bool,
    allow_double_slash_comments: bool,
    allow_block_comments: bool,
) -> PyResult<String> {
    let result = parse_expression_lossless_with_dialect(
        input,
        dialect(
            allow_semicolon_separators,
            allow_decimal_comma,
            allow_dash_dash_comments,
            allow_double_slash_comments,
            allow_block_comments,
        ),
    )
    .map_err(|err| PyValueError::new_err(err.to_string()))?;
    to_json(result)
}

#[pyfunction(
    name = "parse_query_lossless",
    signature = (
        input,
        allow_semicolon_separators=true,
        allow_decimal_comma=false,
        allow_dash_dash_comments=true,
        allow_double_slash_comments=true,
        allow_block_comments=true
    )
)]
fn parse_query_lossless_py(
    input: &str,
    allow_semicolon_separators: bool,
    allow_decimal_comma: bool,
    allow_dash_dash_comments: bool,
    allow_double_slash_comments: bool,
    allow_block_comments: bool,
) -> PyResult<String> {
    let result = parse_query_lossless_with_dialect(
        input,
        dialect(
            allow_semicolon_separators,
            allow_decimal_comma,
            allow_dash_dash_comments,
            allow_double_slash_comments,
            allow_block_comments,
        ),
    )
    .map_err(|err| PyValueError::new_err(err.to_string()))?;
    to_json(result)
}

#[pyfunction(
    name = "recover_expression",
    signature = (
        input,
        allow_semicolon_separators=true,
        allow_decimal_comma=false,
        allow_dash_dash_comments=true,
        allow_double_slash_comments=true,
        allow_block_comments=true
    )
)]
fn recover_expression_py(
    input: &str,
    allow_semicolon_separators: bool,
    allow_decimal_comma: bool,
    allow_dash_dash_comments: bool,
    allow_double_slash_comments: bool,
    allow_block_comments: bool,
) -> PyResult<String> {
    to_json(recover_expression_with_dialect(
        input,
        dialect(
            allow_semicolon_separators,
            allow_decimal_comma,
            allow_dash_dash_comments,
            allow_double_slash_comments,
            allow_block_comments,
        ),
    ))
}

#[pyfunction(
    name = "recover_query",
    signature = (
        input,
        allow_semicolon_separators=true,
        allow_decimal_comma=false,
        allow_dash_dash_comments=true,
        allow_double_slash_comments=true,
        allow_block_comments=true
    )
)]
fn recover_query_py(
    input: &str,
    allow_semicolon_separators: bool,
    allow_decimal_comma: bool,
    allow_dash_dash_comments: bool,
    allow_double_slash_comments: bool,
    allow_block_comments: bool,
) -> PyResult<String> {
    to_json(recover_query_with_dialect(
        input,
        dialect(
            allow_semicolon_separators,
            allow_decimal_comma,
            allow_dash_dash_comments,
            allow_double_slash_comments,
            allow_block_comments,
        ),
    ))
}

#[pyfunction(
    name = "validate_expression_against_model",
    signature = (
        input,
        model_json,
        allow_semicolon_separators=true,
        allow_decimal_comma=false,
        allow_dash_dash_comments=true,
        allow_double_slash_comments=true,
        allow_block_comments=true
    )
)]
fn validate_expression_against_model_py(
    input: &str,
    model_json: &str,
    allow_semicolon_separators: bool,
    allow_decimal_comma: bool,
    allow_dash_dash_comments: bool,
    allow_double_slash_comments: bool,
    allow_block_comments: bool,
) -> PyResult<String> {
    let expr = parse_expression_with_dialect(
        input,
        dialect(
            allow_semicolon_separators,
            allow_decimal_comma,
            allow_dash_dash_comments,
            allow_double_slash_comments,
            allow_block_comments,
        ),
    )
    .map_err(|err| PyValueError::new_err(err.to_string()))?;
    to_json(validate_expression_against_model(
        &expr,
        &model_metadata(model_json)?,
    ))
}

#[pyfunction(
    name = "validate_query_against_model",
    signature = (
        input,
        model_json,
        allow_semicolon_separators=true,
        allow_decimal_comma=false,
        allow_dash_dash_comments=true,
        allow_double_slash_comments=true,
        allow_block_comments=true
    )
)]
fn validate_query_against_model_py(
    input: &str,
    model_json: &str,
    allow_semicolon_separators: bool,
    allow_decimal_comma: bool,
    allow_dash_dash_comments: bool,
    allow_double_slash_comments: bool,
    allow_block_comments: bool,
) -> PyResult<String> {
    let query = parse_query_with_dialect(
        input,
        dialect(
            allow_semicolon_separators,
            allow_decimal_comma,
            allow_dash_dash_comments,
            allow_double_slash_comments,
            allow_block_comments,
        ),
    )
    .map_err(|err| PyValueError::new_err(err.to_string()))?;
    to_json(validate_query_against_model(
        &query,
        &model_metadata(model_json)?,
    ))
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_expression_py, m)?)?;
    m.add_function(wrap_pyfunction!(parse_query_py, m)?)?;
    m.add_function(wrap_pyfunction!(lex_py, m)?)?;
    m.add_function(wrap_pyfunction!(validate_expression_py, m)?)?;
    m.add_function(wrap_pyfunction!(validate_query_py, m)?)?;
    m.add_function(wrap_pyfunction!(format_expression_py, m)?)?;
    m.add_function(wrap_pyfunction!(format_query_py, m)?)?;
    m.add_function(wrap_pyfunction!(parse_expression_lossless_py, m)?)?;
    m.add_function(wrap_pyfunction!(parse_query_lossless_py, m)?)?;
    m.add_function(wrap_pyfunction!(recover_expression_py, m)?)?;
    m.add_function(wrap_pyfunction!(recover_query_py, m)?)?;
    m.add_function(wrap_pyfunction!(validate_expression_against_model_py, m)?)?;
    m.add_function(wrap_pyfunction!(validate_query_against_model_py, m)?)?;
    Ok(())
}
