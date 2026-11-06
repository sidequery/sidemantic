"""Window modifiers do not turn the outer window operator into a grouped aggregate."""

import duckdb
import pytest
import sqlglot

from sidemantic.sql.ossie_relational import _has_aggregate
from tests.interchange.ossie.test_layer2_conformance import ROUTES, execute

DOCUMENT = {
    "version": "0.2.0.dev0",
    "name": "sales",
    "datasets": [
        {
            "name": "orders",
            "source": "raw_orders",
            "fields": [
                {"name": "id", "expression": {"sql": "id"}},
                {"name": "amount", "expression": {"sql": "amount * 2"}},
            ],
            "metrics": [{"name": "revenue", "expression": {"sql": "SUM(amount)"}}],
        }
    ],
}
DDL = "CREATE TABLE raw_orders(id INTEGER,amount INTEGER); INSERT INTO raw_orders VALUES(1,10),(2,-5),(3,NULL),(4,10)"
CASES = [
    "SELECT amount,SUM(amount) FILTER(WHERE amount>0) OVER() AS total FROM sales.orders ORDER BY id",
    "SELECT amount,COUNT(*) FILTER(WHERE amount>100) OVER() AS total FROM sales.orders ORDER BY id",
    "SELECT amount,SUM(amount) FILTER(WHERE amount>0) OVER(ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS total FROM sales.orders ORDER BY id",
    "SELECT amount,SUM(amount) FILTER(WHERE amount>0) OVER() AS total FROM sales.orders QUALIFY ROW_NUMBER() OVER(ORDER BY id)<=2 ORDER BY id",
    "SELECT amount,FIRST_VALUE(amount IGNORE NULLS) OVER(ORDER BY id) AS first_amount FROM sales.orders ORDER BY id",
    "SELECT SUM(SUM(amount)) FILTER(WHERE SUM(amount)>0) OVER() AS total FROM sales.orders",
    "SELECT SUM(SUM(amount)) FILTER(WHERE COUNT(*)>1) OVER() AS total FROM sales.orders GROUP BY amount ORDER BY amount",
    "SELECT FIRST_VALUE(SUM(amount) IGNORE NULLS) OVER(ORDER BY amount) AS total FROM sales.orders GROUP BY amount ORDER BY amount",
]


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("query", CASES)
def test_window_modifiers_match_logical_field_oracle(route, query):
    reference = query.replace("sales.orders", "(SELECT id,amount*2 AS amount FROM raw_orders) logical_orders")
    with duckdb.connect() as connection:
        connection.execute(DDL)
        expected = connection.execute(reference).fetchall()
    actual, _ = execute(route, query, DOCUMENT, DDL)
    assert actual == expected


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize(
    "query",
    [
        "SELECT amount,SUM(SUM(amount)) FILTER(WHERE SUM(amount)>0) OVER() FROM sales.orders",
        "SELECT SUM(amount) FILTER(WHERE SUM(amount)>0) OVER() FROM sales.orders",
        "SELECT SUM(revenue) FILTER(WHERE amount>0) OVER() FROM sales.orders",
    ],
)
def test_window_modifiers_do_not_hide_invalid_aggregate_inputs(route, query):
    with pytest.raises(ValueError, match="group|aggregate|measure"):
        execute(route, query, DOCUMENT, DDL)


@pytest.mark.parametrize(
    "expression,grouped",
    [
        ("SUM(amount) FILTER(WHERE amount>0) OVER()", False),
        ("PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY amount) OVER()", False),
        ("FIRST_VALUE(amount IGNORE NULLS) OVER()", False),
        ("SUM(SUM(amount)) FILTER(WHERE amount>0) OVER()", True),
        ("SUM(amount) FILTER(WHERE SUM(amount)>0) OVER()", True),
        ("PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY SUM(amount)) OVER()", True),
        ("FIRST_VALUE(SUM(amount) IGNORE NULLS) OVER()", True),
    ],
)
def test_group_detection_sees_inputs_through_window_modifiers(expression, grouped):
    parsed = sqlglot.parse_one(f"SELECT {expression}", read="duckdb")
    assert _has_aggregate(parsed.expressions[0]) is grouped
