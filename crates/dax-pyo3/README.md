# sidemantic-dax

Python bindings for the `dax-parser` crate.

## Build

```bash
cd crates/dax-pyo3
maturin develop
```

This package is built with ABI3 for Python 3.11+, so a single wheel works for 3.11–3.13.

If your Python is newer than PyO3 supports (for example 3.14), set:

```bash
export PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1
```

## Usage

```python
import sidemantic_dax as dax

expr = dax.parse_expression("SUM('Sales'[Amount])")
query = dax.parse_query("evaluate 'Sales'")
```

`parse_expression` and `parse_query` return typed Python AST nodes. Raw JSON-style output is available as:

```python
expr_raw = dax.parse_expression_raw("SUM('Sales'[Amount])")
tokens_raw = dax.lex_raw("SUM('Sales'[Amount])")
```
