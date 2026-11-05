"""Independent result and diagnostic oracles for Ossie wide-table semantics."""

from __future__ import annotations

import json
import re

import duckdb
import pytest

from sidemantic import SemanticLayer
from tests.interchange.ossie.test_layer2_conformance import (
    LATEST,
    ROOT,
    ROUTES,
    assert_rows,
    model,
    setup,
    shared_catalog,
)
from tests.rust_layer_adapter import _rust_request

CASES = json.loads((ROOT / "layer3-cases.json").read_text())
ERRORS = json.loads((ROOT / "layer3-errors.json").read_text())


def compile_wide(route, query, document=None):
    document = document or model()
    if route == "native_rust":
        response = _rust_request(
            {
                "action": "ossie_wide_compile",
                "content": json.dumps(document),
                "serialization": "json",
                "target": "DUCKDB",
                "dialect": "duckdb",
                "schema_revision": LATEST,
                "query": query,
            }
        )
        if response["status"] != "ok":
            raise ValueError(response["error"])
        return response["sql"]
    layer = SemanticLayer.from_catalog(
        shared_catalog(document), engine="rust" if route == "public_rust" else "python", fallback=False
    )
    try:
        return layer.compile_ossie_wide(query)
    finally:
        layer.adapter.close()


def execute_wide(route, query, document=None, extra_sql=""):
    document = document or model()
    if route == "native_rust":
        sql = compile_wide(route, query, document)
        with duckdb.connect() as connection:
            connection.execute(setup() + extra_sql)
            result = connection.execute(sql)
            return result.fetchall(), [column[0] for column in result.description]
    engine = "rust" if route == "public_rust" else "python"
    layer = SemanticLayer.from_catalog(shared_catalog(document), engine=engine, fallback=False, auto_register=False)
    try:
        layer.adapter.execute(setup() + extra_sql)
        result = layer.query_ossie_wide(query)
        rows = result.fetchall()
        assert layer.last_engine_selection == {"engine": engine, "reason": None}
        return rows, [column[0] for column in result.description]
    finally:
        layer.adapter.close()


def assert_error(route, query, code, document=None):
    # Reject at compilation, before any physical table exists.
    with pytest.raises(ValueError, match=re.escape(code)):
        compile_wide(route, query, document)


@pytest.mark.parametrize("case", CASES, ids=lambda case: case["id"])
@pytest.mark.parametrize("route", ROUTES)
def test_layer3_result_contract(route, case):
    rows, columns = execute_wide(route, case["query"], extra_sql=case.get("extra_sql", ""))
    assert_rows(rows, case["expected"])
    if case["id"] == "aliases_output_order":
        assert columns == ["Region", "Revenue", "Cost"]


@pytest.mark.parametrize("case", ERRORS, ids=lambda case: case["id"])
@pytest.mark.parametrize("route", ROUTES)
def test_layer3_analysis_errors_and_precedence(route, case):
    assert_error(route, case["query"], case["error"])


@pytest.mark.parametrize("route", ROUTES)
def test_existing_null_branch_differs_from_missing_branch(route):
    document = model()
    tickets = next(dataset for dataset in document["datasets"] if dataset["name"] == "tickets")
    tickets["metrics"].append(
        {"name": "empty_sensitive", "expression": {"sql": "CASE WHEN COUNT(*)=0 THEN 999 ELSE SUM(cost) END"}}
    )
    query = {"dimensions": ["customers.region"], "metrics": ["tickets.empty_sensitive", "visits.visit_count"]}
    rows, _ = execute_wide(route, query, document, "DELETE FROM raw_tickets WHERE customer_id IS NULL;")
    assert_rows(rows, [["EAST", 10, 0], ["WEST", 5, 0], ["NORTH", 11, 2], [None, None, 1], ["SOUTH", 999, 1]])


def relationship(name, source, target, from_column="id", to_column="id"):
    return {
        "name": name,
        "from": source,
        "to": target,
        "from_columns": [from_column],
        "to_columns": [to_column],
    }


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("parallel", [False, True], ids=["unequal_length_paths", "parallel_edges"])
def test_all_directed_paths_count_for_ambiguity(route, parallel):
    document = model()
    if parallel:
        document["relationships"].append(relationship("second_customer", "orders", "customers", "customer_id"))
    else:
        document["datasets"].append(
            {
                "name": "bridge",
                "source": "raw_customers",
                "primary_key": ["id"],
                "fields": [{"name": "id", "expression": {"sql": "id"}}],
            }
        )
        document["relationships"].extend(
            [
                relationship("orders_bridge", "orders", "bridge", "customer_id"),
                relationship("bridge_customer", "bridge", "customers"),
            ]
        )
    assert_error(
        route, {"dimensions": ["customers.region"], "metrics": ["orders.revenue"]}, "E_AMBIGUOUS_PATH", document
    )


@pytest.mark.parametrize("route", ROUTES)
def test_self_relationship_is_not_an_additional_path(route):
    document = model()
    document["relationships"].append(relationship("self_customer", "customers", "customers"))
    rows, _ = execute_wide(route, {"dimensions": ["customers.region"], "metrics": ["orders.revenue"]}, document)
    assert_rows(rows, [["EAST", 60], ["WEST", 40], [None, 180]])


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize(
    ("expression", "code"),
    [
        ("SUM(orders.amount) * customers.id", "E_UNAGGREGATED_FINER_GRAIN_REFERENCE"),
        ("SUM(orders.amount) OVER ()", "E_DEFERRED_WINDOWED_METRIC"),
        ("SUM(orders.amount * tickets.cost)", "E_AMBIGUOUS_MEASURE_GRAIN"),
        ("COUNT(*)", "E_AMBIGUOUS_MEASURE_GRAIN"),
        ("COUNT(*) FILTER (WHERE orders.status='paid')", "E_AMBIGUOUS_MEASURE_GRAIN"),
    ],
    ids=["finer_grain_scalar", "window", "multi_dataset_argument", "no_home_dataset", "filter_does_not_choose_home"],
)
def test_model_metric_component_scope_is_checked_at_query_time(route, expression, code):
    document = model()
    document["metrics"].append({"name": "unsupported", "expression": {"sql": expression}})
    assert_error(route, {"metrics": ["unsupported"]}, code, document)


@pytest.mark.parametrize("route", ROUTES)
def test_dimensions_only_multiple_valid_anchors_are_ambiguous(route):
    document = model()
    for name in ("a", "b"):
        document["datasets"].append(
            {
                "name": name,
                "source": "raw_customers",
                "primary_key": ["id"],
                "fields": [
                    {"name": "id", "expression": {"sql": "id"}},
                    {"name": "label", "expression": {"sql": "region"}},
                ],
            }
        )
    document["relationships"].extend([relationship("a_b", "a", "b"), relationship("b_a", "b", "a")])
    assert_error(
        route,
        {"dimensions": [{"field": "a.label", "alias": "a"}, {"field": "b.label", "alias": "b"}]},
        "E_AMBIGUOUS_PATH",
        document,
    )


@pytest.mark.parametrize("route", ROUTES)
def test_metric_filter_preserves_group_domain_while_query_filter_removes_it(route):
    document = model()
    orders = next(dataset for dataset in document["datasets"] if dataset["name"] == "orders")
    orders["metrics"].extend(
        [
            {"name": "open_amount", "expression": {"sql": "SUM(amount) FILTER (WHERE status='open')"}},
            {"name": "open_count", "expression": {"sql": "COUNT(*) FILTER (WHERE status='open')"}},
        ]
    )
    query = {"dimensions": ["customers.region"], "metrics": ["orders.open_amount", "orders.open_count"]}
    assert_rows(execute_wide(route, query, document)[0], [["EAST", 10, 1], ["WEST", None, 0], [None, 60, 1]])
    query["filter"] = "orders.status='open'"
    assert_rows(execute_wide(route, query, document)[0], [["EAST", 10, 1], [None, 60, 1]])


@pytest.mark.parametrize("route", ROUTES)
def test_ordered_set_and_holistic_components_use_original_rows(route):
    document = model()
    orders = next(dataset for dataset in document["datasets"] if dataset["name"] == "orders")
    orders["metrics"].extend(
        [
            {"name": "median_amount", "expression": {"sql": "MEDIAN(amount)"}},
            {"name": "p75", "expression": {"sql": "PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY amount)"}},
        ]
    )
    query = {
        "dimensions": ["customers.region"],
        "metrics": ["orders.median_amount", "orders.p75", "visits.visit_count"],
    }
    assert_rows(
        execute_wide(route, query, document)[0],
        [
            ["EAST", 10, 15, 0],
            ["WEST", 40, 40, 0],
            [None, 60, 65, 1],
            ["NORTH", None, None, 2],
            ["SOUTH", None, None, 1],
        ],
    )


@pytest.mark.parametrize("route", ROUTES)
def test_unrelated_sources_and_unused_field_expressions_are_not_bound(route):
    document = model()
    orders = next(dataset for dataset in document["datasets"] if dataset["name"] == "orders")
    orders["fields"].append({"name": "unused", "expression": {"sql": "absent_physical_column"}})
    rows, _ = execute_wide(
        route,
        {"metrics": ["orders.revenue", "orders.order_count"]},
        document,
        "DROP TABLE raw_unrelated; DROP TABLE raw_tickets; DROP TABLE raw_visits; DROP TABLE raw_customers;",
    )
    assert_rows(rows, [[280, 9]])
