# /// script
# requires-python = ">=3.11"
# ///

import importlib.metadata
import importlib.util
import json

import sidemantic_rs


def assert_contains(text: str, needle: str) -> None:
    if needle not in text:
        raise AssertionError(f"expected {needle!r} in {text!r}")


def expect_raises(exc_type: type[BaseException], func, *args) -> str:
    try:
        func(*args)
    except exc_type as exc:
        return str(exc)
    raise AssertionError(f"expected {exc_type.__name__} from {func.__name__}")


root_python_package = importlib.util.find_spec("sidemantic")
if root_python_package is not None:
    raise AssertionError("isolated sidemantic_rs wheel smoke unexpectedly found root sidemantic package")

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
      - name: customer_id
        type: numeric
    metrics:
      - name: revenue
        agg: sum
        sql: amount
      - name: order_count
        agg: count
    relationships:
      - name: customers
        type: many_to_one
        foreign_key: customer_id
  - name: customers
    table: customers
    primary_key: id
    dimensions:
      - name: country
        type: categorical
metrics:
  - name: revenue_per_order
    type: ratio
    numerator: revenue
    denominator: order_count
"""

query_yaml = """
metrics: [revenue_per_order]
dimensions: [orders.status]
order_by: [revenue_per_order DESC]
limit: 5
"""

compiled = sidemantic_rs.compile_with_yaml(models_yaml, query_yaml)
assert_contains(compiled, "SUM(")
assert_contains(compiled, "COUNT(")
assert_contains(compiled, "ORDER BY")
assert_contains(compiled, "LIMIT 5")

rewritten = sidemantic_rs.rewrite_with_yaml(
    models_yaml,
    "SELECT orders.revenue, orders.status FROM orders ORDER BY orders.revenue DESC LIMIT 3",
)
assert_contains(rewritten, "SUM(")
assert_contains(rewritten, "ORDER BY")
assert_contains(rewritten, "LIMIT 3")

payload = json.loads(sidemantic_rs.load_graph_with_yaml(models_yaml))
assert "orders" in json.dumps(payload)
assert "revenue_per_order" in json.dumps(payload)

errors = sidemantic_rs.validate_query_with_yaml(models_yaml, query_yaml)
if errors:
    raise AssertionError(f"unexpected validation errors: {errors!r}")

reference_error = sidemantic_rs.validate_query_references(
    models_yaml,
    ["orders.missing_metric"],
    [],
)
if not reference_error:
    raise AssertionError("missing metric reference should produce validation errors")

reference_errors = sidemantic_rs.validate_query_references(
    models_yaml,
    ["revenue_per_order"],
    ["orders.status"],
)
if reference_errors:
    raise AssertionError(f"unexpected reference errors: {reference_errors!r}")

sql_payload = json.loads(sidemantic_rs.load_graph_with_sql("MODEL (name events, table events, primary_key event_id);"))
assert "events" in json.dumps(sql_payload)

statement_blocks = json.loads(
    sidemantic_rs.parse_sql_statement_blocks_payload("MODEL (name events, table events, primary_key event_id);")
)
if statement_blocks[0]["kind"] != "model":
    raise AssertionError(f"unexpected statement blocks: {statement_blocks!r}")

expect_raises(ValueError, sidemantic_rs.parse_sql_statement_blocks_payload, "MODEL (")
expect_raises(ValueError, sidemantic_rs.load_graph_with_yaml, "models: [")
expect_raises(ValueError, sidemantic_rs.compile_with_yaml, models_yaml, "metrics: [")

catalog = json.loads(sidemantic_rs.generate_catalog_metadata(models_yaml, "semantic"))
assert "orders" in json.dumps(catalog)

if sidemantic_rs.detect_adapter_kind("cube.yml", "cubes: []") != "cube":
    raise AssertionError("adapter detection failed for Cube YAML")

chart_x, chart_y = sidemantic_rs.chart_auto_detect_columns(["status", "revenue"], [True])
if chart_x != "status" or chart_y != ["revenue"]:
    raise AssertionError(f"unexpected chart columns: {(chart_x, chart_y)!r}")

expect_raises(
    ValueError,
    sidemantic_rs.chart_auto_detect_columns,
    ["status", "revenue"],
    [False, True],
)

if not sidemantic_rs.is_relative_date("last 7 days"):
    raise AssertionError("relative date detection failed")
assert_contains(sidemantic_rs.relative_date_to_range("last 7 days", "created_at"), "created_at")

models_for_query = sidemantic_rs.find_models_for_query(["orders.status"], ["orders.revenue"])
if models_for_query != ["orders"]:
    raise AssertionError(f"unexpected query models: {models_for_query!r}")

parsed_ref = sidemantic_rs.parse_reference_with_yaml(models_yaml, "orders.revenue")
if parsed_ref[:2] != ("orders", "revenue"):
    raise AssertionError(f"unexpected parsed reference: {parsed_ref!r}")

path = sidemantic_rs.find_relationship_path_with_yaml(models_yaml, "orders", "customers")
if not path or path[0][0] != "orders":
    raise AssertionError(f"unexpected relationship path: {path!r}")

expect_raises(
    KeyError,
    sidemantic_rs.find_relationship_path_with_yaml,
    models_yaml,
    "missing",
    "orders",
)

relationship_yaml = """
name: customers
type: many_to_one
foreign_key: customer_id
"""
if sidemantic_rs.relationship_foreign_key_columns(relationship_yaml) != ["customer_id"]:
    raise AssertionError("relationship foreign-key helper failed")

refresh_statements = sidemantic_rs.build_preaggregation_refresh_statements("full", "agg_orders", "SELECT 1 AS n")
if not refresh_statements or "agg_orders" not in "\n".join(refresh_statements):
    raise AssertionError(f"unexpected refresh statements: {refresh_statements!r}")

expect_raises(
    ValueError,
    sidemantic_rs.build_preaggregation_refresh_statements,
    "incremental",
    "agg_orders",
    "SELECT 1 AS n",
)

sidemantic_rs.registry_set_current_layer({"name": "wheel-smoke"})
if sidemantic_rs.registry_get_current_layer() != {"name": "wheel-smoke"}:
    raise AssertionError("registry ContextVar roundtrip failed")

if not callable(getattr(sidemantic_rs, "execute_with_adbc", None)):
    raise AssertionError("default python wheel should expose execute_with_adbc")
