"""Structured Ossie adapters preserve filters and output schemas across engines."""

import json

import duckdb
import pytest

from sidemantic import SemanticLayer
from tests.interchange.ossie.test_layer2_conformance import LATEST, ROUTES, shared_catalog
from tests.rust_layer_adapter import _rust_request


def _document():
    def dataset(name, source):
        return {
            "name": name,
            "source": source,
            "fields": [
                {"name": "amount", "expression": {"sql": "amount"}},
                {"name": "category", "expression": {"sql": "category"}},
            ],
            "metrics": [{"name": "total", "expression": {"sql": "SUM(amount)"}}],
        }

    return {
        "version": "0.2.0.dev0",
        "name": "commerce",
        "datasets": [
            dataset(
                "orders",
                "SELECT 1 AS amount, 'a' AS category UNION ALL SELECT 2, 'a' "
                "UNION ALL SELECT 3, 'b' UNION ALL SELECT 101, 'b'",
            ),
            dataset("returns", "SELECT 7 AS amount, 'a' AS category"),
        ],
    }


def _execute(route, query, *, strict=False):
    document = _document()
    if route == "native_rust":
        response = _rust_request(
            {
                "action": "ossie_wide_compile" if strict else "ossie_compile",
                "content": json.dumps(document),
                "serialization": "json",
                "target": "DUCKDB",
                "dialect": "duckdb",
                "schema_revision": LATEST,
                **({"query": query} if strict else query),
            }
        )
        assert response["status"] == "ok", response
        with duckdb.connect() as connection:
            result = connection.execute(response["sql"])
            return result.fetchall(), [column[0] for column in result.description]
    engine = "rust" if route == "public_rust" else "python"
    layer = SemanticLayer.from_catalog(shared_catalog(document), engine=engine, fallback=False, auto_register=False)
    try:
        result = layer.query_ossie_wide(query) if strict else layer.adapter.execute(layer.compile(**query))
        rows = result.fetchall()
        assert layer.last_engine_selection == {"engine": engine, "reason": None}
        return rows, [column[0] for column in result.description]
    finally:
        layer.adapter.close()


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize(
    "filters,expected",
    [
        (["orders.amount > 0", "orders.amount < 100"], 6),
        (["orders.amount = 1 OR orders.amount = 2", "orders.amount > 1"], 2),
    ],
)
def test_structured_filter_lists_preserve_conjunction_and_precedence(route, filters, expected):
    rows, columns = _execute(route, {"metrics": ["orders.total"], "filters": filters})
    assert rows == [(expected,)]
    assert columns == ["orders.total"]


@pytest.mark.parametrize("route", ROUTES)
def test_structured_metrics_with_same_local_name_have_distinct_default_outputs(route):
    rows, columns = _execute(route, {"metrics": ["orders.total", "returns.total"]})
    assert rows == [(107, 7)]
    assert columns == ["orders.total", "returns.total"]


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize(
    "order",
    ["orders.total DESC", "ORDERS.TOTAL DESC", "orders.total DESC NULLS FIRST", "orders.total desc nulls last"],
)
def test_structured_default_metric_name_remains_orderable(route, order):
    rows, columns = _execute(
        route,
        {"dimensions": ["orders.category"], "metrics": ["orders.total"], "order_by": [order]},
    )
    assert rows == [("b", 104), ("a", 3)]
    assert columns == ["category", "orders.total"]


@pytest.mark.parametrize("route", ROUTES)
def test_structured_aliases_are_literal_output_names_and_remain_orderable(route):
    rows, columns = _execute(
        route,
        {
            "dimensions": ["orders.category"],
            "metrics": ["orders.total"],
            "aliases": {"orders.category": "Order category", "orders.total": 'Order "total"'},
            "order_by": ["orders.total DESC"],
        },
    )
    assert rows == [("b", 104), ("a", 3)]
    assert columns == ["Order category", 'Order "total"']


@pytest.mark.parametrize("route", ROUTES)
def test_strict_interface_retains_local_member_output_defaults(route):
    rows, columns = _execute(route, {"metrics": ["orders.total"]}, strict=True)
    assert rows == [(107,)]
    assert columns == ["total"]


@pytest.mark.parametrize("route", ROUTES)
def test_strict_parenthesized_filter_preserves_boolean_grouping(route):
    rows, columns = _execute(
        route,
        {"metrics": ["orders.total"], "filter": "(orders.amount = 1 OR orders.amount = 2) AND (orders.amount > 1)"},
        strict=True,
    )
    assert rows == [(2,)]
    assert columns == ["total"]
