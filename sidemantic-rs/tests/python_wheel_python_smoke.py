# /// script
# requires-python = ">=3.11"
# ///

import importlib.metadata
import importlib.util

import sidemantic_rs

root_python_package = importlib.util.find_spec("sidemantic")
if root_python_package is not None:
    raise AssertionError("isolated sidemantic_rs python-feature wheel unexpectedly found root sidemantic package")

if importlib.metadata.version("sidemantic-rs") != "0.1.0":
    raise AssertionError("unexpected sidemantic-rs wheel version")

models_yaml = """
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: amount
"""

query_yaml = """
metrics: [orders.revenue]
dimensions: [orders.status]
"""

compiled = sidemantic_rs.compile_with_yaml(models_yaml, query_yaml)
if "SUM(" not in compiled or "GROUP BY" not in compiled:
    raise AssertionError(f"unexpected compiled SQL: {compiled}")

execute_with_adbc = getattr(sidemantic_rs, "execute_with_adbc", None)
if not callable(execute_with_adbc):
    raise AssertionError("python-feature wheel should expose execute_with_adbc disabled stub")

try:
    execute_with_adbc("adbc_driver_duckdb", "select 1")
except RuntimeError as exc:
    message = str(exc)
    if "python-adbc" not in message or "not enabled" not in message:
        raise AssertionError(f"unexpected disabled ADBC error: {message}") from exc
else:
    raise AssertionError("python-feature execute_with_adbc should fail with a feature guidance error")
