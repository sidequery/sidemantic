use dax_parser::{lex, parse_expression, parse_query};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pythonize::pythonize;
use serde_json::to_value;

fn to_py_object(py: Python<'_>, value: impl serde::Serialize) -> PyResult<PyObject> {
    let json = to_value(value).map_err(|err| PyValueError::new_err(err.to_string()))?;
    pythonize(py, &json).map_err(|err| PyValueError::new_err(err.to_string()))
}

#[pyfunction(name = "parse_expression")]
fn parse_expression_py(py: Python<'_>, input: &str) -> PyResult<PyObject> {
    let expr = parse_expression(input).map_err(|err| PyValueError::new_err(err.to_string()))?;
    to_py_object(py, expr)
}

#[pyfunction(name = "parse_query")]
fn parse_query_py(py: Python<'_>, input: &str) -> PyResult<PyObject> {
    let query = parse_query(input).map_err(|err| PyValueError::new_err(err.to_string()))?;
    to_py_object(py, query)
}

#[pyfunction(name = "lex")]
fn lex_py(py: Python<'_>, input: &str) -> PyResult<PyObject> {
    let tokens = lex(input).map_err(|err| PyValueError::new_err(err.to_string()))?;
    to_py_object(py, tokens)
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_expression_py, m)?)?;
    m.add_function(wrap_pyfunction!(parse_query_py, m)?)?;
    m.add_function(wrap_pyfunction!(lex_py, m)?)?;
    Ok(())
}
