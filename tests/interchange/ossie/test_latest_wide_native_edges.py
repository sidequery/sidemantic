"""Cross-route boundaries for logical naming and component grain in Layer 3."""

import pytest

from tests.interchange.ossie.test_layer2_conformance import ROUTES, assert_rows, model
from tests.interchange.ossie.test_layer3_conformance import assert_error, execute_wide


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("name", ["__ossie_row_id", "__ow_rid"])
def test_authored_field_cannot_capture_internal_home_row_identity(route, name):
    document = model()
    orders = next(dataset for dataset in document["datasets"] if dataset["name"] == "orders")
    orders["fields"].append({"name": name, "expression": {"sql": "amount"}})
    orders["metrics"].append({"name": "reserved_sum", "expression": {"sql": f"SUM({name})"}})
    query = {"dimensions": ["customers.region"], "metrics": ["orders.reserved_sum"]}
    assert_rows(execute_wide(route, query, document)[0], [["EAST", 60], ["WEST", 40], [None, 180]])


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("predicate", ["1", "'yes'", "1+2", "DATE '2026-01-01'"])
def test_nonboolean_filter_error_precedes_unknown_metric(route, predicate):
    assert_error(route, {"metrics": ["missing"], "filter": predicate}, "E_INVALID_QUERY")


@pytest.mark.parametrize("route", ROUTES)
def test_model_component_error_priority_is_independent_of_expression_order(route):
    document = model()
    document["metrics"].append(
        {"name": "bad_priority", "expression": {"sql": "SUM(orders.amount) OVER () + customers.id"}}
    )
    assert_error(route, {"metrics": ["bad_priority"]}, "E_UNAGGREGATED_FINER_GRAIN_REFERENCE", document)


@pytest.mark.parametrize("route", ROUTES)
def test_count_does_not_evaluate_unreferenced_physical_field(route):
    document = model()
    orders = next(dataset for dataset in document["datasets"] if dataset["name"] == "orders")
    orders["fields"].append({"name": "missing", "expression": {"sql": "no_such_source_column"}})
    assert_rows(execute_wide(route, {"metrics": ["orders.order_count"]}, document)[0], [[9]])


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("expression", ["1", "amount", "SUM(amount)+amount", "SUM(amount) OVER ()", "SUM(SUM(amount))"])
def test_invalid_dataset_measure_is_rejected_before_execution(route, expression):
    document = model()
    orders = next(dataset for dataset in document["datasets"] if dataset["name"] == "orders")
    orders["metrics"].append({"name": "invalid", "expression": {"sql": expression}})
    assert_error(route, {"metrics": ["orders.invalid"]}, "E_INVALID_QUERY", document)


@pytest.mark.parametrize("route", ROUTES)
def test_quoted_names_preserve_exact_identity(route):
    document = {
        "version": "0.2.0.dev0",
        "name": "commerce",
        "datasets": [
            {
                "name": '"Orders"',
                "source": "raw_orders",
                "fields": [{"name": '"Amount"', "expression": {"sql": "amount * 2"}}],
                "metrics": [{"name": '"Total"', "expression": {"sql": 'SUM("Amount")'}}],
            }
        ],
    }
    assert_rows(execute_wide(route, {"metrics": ['"Orders"."Total"']}, document)[0], [[560]])
    assert_error(route, {"metrics": ["orders.total"]}, "E_NAME_NOT_FOUND", document)


@pytest.mark.parametrize("route", ROUTES)
def test_alias_identifier_limit_is_enforced(route):
    query = {"metrics": [{"metric": "orders.revenue", "alias": "x" * 129}]}
    assert_error(route, query, "E_INVALID_QUERY")


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("shape", ["singleton", "empty_union", "real_branch"])
def test_constant_model_metric_uses_the_union_of_component_domains(route, shape):
    document = model()
    document["metrics"].append({"name": "constant", "expression": {"sql": "42"}})
    query = {"metrics": ["constant"]}
    expected = [[42]]
    if shape != "singleton":
        query["dimensions"] = ["customers.region"]
        expected = []
    if shape == "real_branch":
        query["metrics"].append("orders.revenue")
        expected = [["EAST", 42, 60], ["WEST", 42, 40], [None, 42, 180]]
    rows, columns = execute_wide(route, query, document)
    assert_rows(rows, expected)
    assert (
        columns
        == {
            "singleton": ["constant"],
            "empty_union": ["region", "constant"],
            "real_branch": ["region", "constant", "revenue"],
        }[shape]
    )


@pytest.mark.parametrize("route", ROUTES)
def test_nested_model_aggregate_rejected_before_execution(route):
    document = model()
    document["metrics"].append({"name": "nested", "expression": {"sql": "SUM(SUM(orders.amount))"}})
    assert_error(route, {"metrics": ["nested"]}, "E_INVALID_QUERY", document)


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize(("source", "target"), [("snowflake", "duckdb"), ("duckdb", "snowflake")])
def test_input_dialect_and_output_dialect_have_independent_array_index_semantics(route, source, target):
    import json

    import duckdb
    import sqlglot

    from sidemantic import SemanticLayer
    from sidemantic.interchange.ossie import OssieParseOptions, lower_ossie_document, parse_ossie_document
    from tests.interchange.ossie.test_layer2_conformance import LATEST
    from tests.rust_layer_adapter import _rust_request

    def variant(sql):
        return {"dialects": [{"dialect": "ANSI_SQL" if source == "duckdb" else source.upper(), "expression": sql}]}

    document = {
        "version": "0.2.0.dev0",
        "name": "arrays",
        "datasets": [
            {
                "name": "values",
                "source": "SELECT ARRAY_CONSTRUCT(10,20) AS xs" if source == "snowflake" else "SELECT [10,20] AS xs",
                "fields": [{"name": "first", "expression": variant("xs[0]" if source == "snowflake" else "xs[1]")}],
                "metrics": [
                    {
                        "name": "total",
                        "expression": variant(
                            "SUM(first) + SUM(ABS(source.xs[0]))"
                            if source == "snowflake"
                            else "SUM(first) + SUM(ABS(source.xs[1]))"
                        ),
                    }
                ],
            }
        ],
    }
    query = {"metrics": ["values.total"]}
    if route == "native_rust":
        response = _rust_request(
            {
                "action": "ossie_wide_compile",
                "content": json.dumps(document),
                "serialization": "json",
                "target": source.upper(),
                "dialect": target,
                "schema_revision": LATEST,
                "query": query,
            }
        )
        assert response["status"] == "ok", response
        sql = response["sql"]
    else:
        parsed = parse_ossie_document(
            json.dumps(document).encode(),
            options=OssieParseOptions(validate_schema=True, schema_revision=LATEST, target_dialect=source),
        )
        lowered = lower_ossie_document(parsed)
        assert lowered.valid, lowered.diagnostics
        layer = SemanticLayer.from_catalog(
            lowered.catalog,
            dialect=source,
            engine="rust" if route == "public_rust" else "python",
            fallback=False,
            auto_register=False,
        )
        try:
            sql = layer.compile_ossie_wide(query, dialect=target)
        finally:
            layer.adapter.close()
    with duckdb.connect() as connection:
        duckdb_sql = sqlglot.transpile(sql, read=target, write="duckdb")[0]
        assert connection.execute(duckdb_sql).fetchall() == [(20,)]
