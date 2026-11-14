"""Aggregate FILTER binds logical and source fields exactly once."""

import pytest

from tests.interchange.ossie.test_ossie_engine_conformance import _query

ROUTES = ["python", "public_rust", "native_rust"]


def _document(field_sql="amount * 2"):
    return {
        "version": "0.2.0.dev0",
        "name": "filter_scope",
        "datasets": [
            {
                "name": "orders",
                "source": (
                    "SELECT 10 AS amount, 'a' AS category UNION ALL SELECT 20, 'a' "
                    "UNION ALL SELECT NULL, 'a' UNION ALL SELECT 5, 'b'"
                ),
                "fields": [
                    {"name": "amount", "expression": {"sql": field_sql}},
                    {"name": "category", "expression": {"sql": "category"}},
                ],
                "metrics": [
                    {"name": "filtered_sum", "expression": {"sql": "SUM(amount) FILTER (WHERE amount > 35)"}},
                    {"name": "filtered_count", "expression": {"sql": "COUNT(*) FILTER (WHERE amount > 35)"}},
                    {
                        "name": "source_sum",
                        "expression": {"sql": "SUM(source.amount) FILTER (WHERE source.amount > 15)"},
                    },
                    {"name": "source_count", "expression": {"sql": "COUNT(*) FILTER (WHERE source.amount > 15)"}},
                    {"name": "mixed_sum", "expression": {"sql": "SUM(amount) FILTER (WHERE source.amount > 15)"}},
                    {
                        "name": "mixed_source_sum",
                        "expression": {"sql": "SUM(source.amount) FILTER (WHERE amount > 35)"},
                    },
                ],
            }
        ],
    }


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("grouped", [False, True])
@pytest.mark.parametrize("field_sql", ["amount * 2", "ABS(amount) * 2"])
def test_filtered_aggregate_logical_fields_are_not_evaluated_twice(route, grouped, field_sql):
    query = {"metrics": ["orders.filtered_sum", "orders.filtered_count"]}
    if grouped:
        query.update(dimensions=["orders.category"], order_by=["orders.category"])
    document = _document(field_sql)
    if field_sql.startswith("ABS"):
        document["datasets"][0]["source"] = (
            "SELECT -10 AS amount, 'a' AS category UNION ALL SELECT -20, 'a' "
            "UNION ALL SELECT NULL, 'a' UNION ALL SELECT -5, 'b'"
        )
    rows = _query(document, route, query)
    assert rows == ([("a", 40, 1), ("b", None, 0)] if grouped else [(40, 1)])


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("grouped", [False, True])
def test_filtered_source_references_bypass_shadowing_logical_fields(route, grouped):
    query = {"metrics": ["orders.source_sum", "orders.source_count", "orders.mixed_sum", "orders.mixed_source_sum"]}
    if grouped:
        query.update(dimensions=["orders.category"], order_by=["orders.category"])
    rows = _query(_document(), route, query)
    assert rows == ([("a", 20, 1, 40, 20), ("b", None, 0, None, None)] if grouped else [(20, 1, 40, 20)])


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("grouped", [False, True])
def test_ordered_set_aggregate_calculated_input_is_evaluated_once(route, grouped):
    document = _document()
    document["datasets"][0]["metrics"] = [
        {"name": "median", "expression": {"sql": "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount)"}}
    ]
    query = {"metrics": ["orders.median"]}
    if grouped:
        query.update(dimensions=["orders.category"], order_by=["orders.category"])
    rows = _query(document, route, query)
    assert rows == ([("a", 30), ("b", 10)] if grouped else [(20,)])
