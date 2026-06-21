use dax_parser::{lex, parse_expression, parse_query};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

fn to_json(value: impl serde::Serialize) -> PyResult<String> {
    serde_json::to_string(&value).map_err(|err| PyValueError::new_err(err.to_string()))
}

#[pyfunction(name = "parse_expression")]
fn parse_expression_py(input: &str) -> PyResult<String> {
    let expr = parse_expression(input).map_err(|err| PyValueError::new_err(err.to_string()))?;
    to_json(expr)
}

#[pyfunction(name = "parse_query")]
fn parse_query_py(input: &str) -> PyResult<String> {
    let query = parse_query(input).map_err(|err| PyValueError::new_err(err.to_string()))?;
    to_json(query)
}

#[pyfunction(name = "lex")]
fn lex_py(input: &str) -> PyResult<String> {
    let tokens = lex(input).map_err(|err| PyValueError::new_err(err.to_string()))?;
    to_json(tokens)
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_expression_py, m)?)?;
    m.add_function(wrap_pyfunction!(parse_query_py, m)?)?;
    m.add_function(wrap_pyfunction!(lex_py, m)?)?;
    Ok(())
}
