"""Independent relational-query results for the pinned Ossie Layer 2 contract.

Expected rows are hand-derived or conventional SQL over physical fixture tables.
Neither Sidemantic engine is used as an oracle for another engine.
"""

from __future__ import annotations

import copy
import json
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from sidemantic import SemanticLayer
from sidemantic.interchange.ossie import OssieParseOptions, lower_ossie_document, parse_ossie_document
from sidemantic.sql.query_rewriter import QueryRewriter
from tests.rust_layer_adapter import _rust_request

ROOT = Path(__file__).resolve().parents[2] / "ossie-fixtures/layer23-172a3e5"
ROUTES = ("python", "public_rust", "native_rust")
CASES = json.loads((ROOT / "layer2-cases.json").read_text())
ERRORS = json.loads((ROOT / "layer2-errors.json").read_text())
LATEST = "172a3e566f2fc9c0a733ebbb470e97c33d2bd8b1"


def model():
    return json.loads((ROOT / "model.json").read_text())


def setup():
    return (ROOT / "setup.sql").read_text()


def shared_catalog(document):
    parsed = parse_ossie_document(
        json.dumps(document).encode(),
        options=OssieParseOptions(validate_schema=True, target_dialect="duckdb", schema_revision=LATEST),
    )
    lowered = lower_ossie_document(parsed)
    assert lowered.valid, lowered.diagnostics
    return lowered.catalog


def compile_query(route, query, document=None):
    document = document or model()
    if route == "native_rust":
        response = _rust_request(
            {
                "action": "ossie_compile",
                "content": json.dumps(document),
                "serialization": "json",
                "target": "DUCKDB",
                "dialect": "duckdb",
                "schema_revision": LATEST,
                "sql": query,
            }
        )
        if response["status"] != "ok":
            raise ValueError(response["error"])
        return response["sql"]
    graph = next(iter(shared_catalog(document).scopes.values())).graph
    return QueryRewriter(
        graph,
        dialect="duckdb",
        use_rust_rewriter=route == "public_rust",
        rust_no_fallback=True,
    ).rewrite(query)


def execute(route, query, document=None, ddl=None):
    document = document or model()
    if route == "native_rust":
        sql = compile_query(route, query, document)
        with duckdb.connect() as connection:
            connection.execute(setup() if ddl is None else ddl)
            result = connection.execute(sql)
            return result.fetchall(), [column[0] for column in result.description]
    engine = "rust" if route == "public_rust" else "python"
    layer = SemanticLayer.from_catalog(shared_catalog(document), engine=engine, fallback=False, auto_register=False)
    try:
        layer.adapter.execute(setup() if ddl is None else ddl)
        result = layer.sql(query)
        rows = result.fetchall()
        assert layer.last_engine_selection == {"engine": engine, "reason": None}
        return rows, [column[0] for column in result.description]
    finally:
        layer.adapter.close()


def assert_rows(actual, expected):
    """Compare result multisets with numeric tolerance and exact NULL/string values."""
    remaining = list(actual)

    def equal_row(left, right):
        if len(left) != len(right):
            return False
        for a, b in zip(left, right, strict=True):
            if isinstance(a, (int, float, Decimal)) and isinstance(b, (int, float, Decimal)):
                if float(a) != pytest.approx(float(b), rel=1e-10, abs=1e-10):
                    return False
            elif a != b:
                return False
        return True

    assert len(remaining) == len(expected), (actual, expected)
    for row in expected:
        match = next((i for i, actual_row in enumerate(remaining) if equal_row(actual_row, row)), None)
        assert match is not None, {"missing_expected_row": row, "remaining_actual": remaining, "expected": expected}
        remaining.pop(match)


@pytest.mark.parametrize("case", CASES, ids=lambda case: case["id"])
@pytest.mark.parametrize("route", ROUTES)
def test_layer2_result_contract(route, case):
    if "reference_sql" in case:
        with duckdb.connect() as connection:
            connection.execute(setup())
            expected = connection.execute(case["reference_sql"]).fetchall()
    else:
        expected = case["expected"]
    rows, _ = execute(route, case["query"])
    assert_rows(rows, expected)
    if case.get("ordered"):
        for actual_row, expected_row in zip(rows, expected, strict=True):
            assert_rows([actual_row], [expected_row])


@pytest.mark.parametrize("case", ERRORS, ids=lambda case: case["id"])
@pytest.mark.parametrize("route", ROUTES)
def test_layer2_type_errors_are_rejected_before_database_execution(route, case):
    # No database is created here: SQL engine execution errors do not satisfy C1.
    with pytest.raises(ValueError):
        compile_query(route, case["query"])


@pytest.mark.parametrize("route", ROUTES)
def test_layer2_projection_order_and_aliases_are_preserved(route):
    rows, columns = execute(
        route,
        'SELECT AGG(order_count) AS "Rows", AGG(revenue) AS "Total", AGG(order_count) + 1 AS "Next" FROM commerce.orders',
    )
    assert columns == ["Rows", "Total", "Next"]
    assert_rows(rows, [[9, 280, 10]])


@pytest.mark.parametrize("route", ROUTES)
def test_layer2_holistic_aggregates_consume_original_population(route):
    document = copy.deepcopy(model())
    orders = next(dataset for dataset in document["datasets"] if dataset["name"] == "orders")
    orders["metrics"].extend(
        [
            {"name": "median_amount", "expression": {"sql": "MEDIAN(amount)"}},
            {"name": "p75", "expression": {"sql": "PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY amount)"}},
        ]
    )
    query = "SELECT AGG(o.median_amount),AGG(o.p75),AGG(o.unique_customers) FROM commerce.orders o CROSS JOIN commerce.tickets t"
    with duckdb.connect() as connection:
        connection.execute(setup())
        expected = connection.execute(
            "SELECT MEDIAN(amount),PERCENTILE_CONT(0.75) WITHIN GROUP(ORDER BY amount),COUNT(DISTINCT customer_id) FROM raw_orders"
        ).fetchall()
    assert_rows(execute(route, query, document)[0], expected)


@pytest.mark.parametrize("route", ROUTES)
def test_using_star_emits_merged_key_once(route):
    query = (
        "SELECT * FROM (SELECT customer_id FROM commerce.orders) o "
        "FULL JOIN (SELECT customer_id FROM commerce.tickets) t USING(customer_id)"
    )
    rows, columns = execute(route, query)
    assert columns == ["customer_id"]
    assert_rows(rows, [[1]] * 8 + [[2], [3], [4], [5], [999], [None], [None]])
