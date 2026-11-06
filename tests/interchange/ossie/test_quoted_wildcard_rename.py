"""Quoted wildcard names survive native parsing with their authored identity."""

import duckdb
import pytest

from tests.interchange.ossie.test_layer2_conformance import ROUTES, execute
from tests.interchange.ossie.test_review_relational_boundaries import _document

DDL = """CREATE SCHEMA shop;
CREATE TABLE shop.orders AS
SELECT 1 AS id, 10 AS amount, 10 AS revenue, 1 AS n
UNION ALL SELECT 2, 20, 20, 1;"""


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize(
    "query",
    [
        'SELECT AGG("total.pay") FROM (SELECT * RENAME(revenue AS "total.pay") FROM shop.orders) q',
        'SELECT AGG("MiXeD") FROM (SELECT * RENAME(revenue AS "MiXeD") FROM shop.orders) q',
        'SELECT AGG("pay""roll") FROM (SELECT * RENAME(revenue AS "pay""roll") FROM shop.orders) q',
        'SELECT AGG("new.pay") FROM (SELECT * RENAME("old.pay" AS "new.pay") FROM '
        '(SELECT revenue AS "old.pay" FROM shop.orders) p) q',
        'SELECT AGG("New") FROM (SELECT * RENAME("Old""Pay" AS "New") FROM '
        '(SELECT revenue AS "Old""Pay" FROM shop.orders) p) q',
        'SELECT AGG(pay) FROM (SELECT * RENAME("REVENUE" AS pay) FROM shop.orders) q',
        'SELECT * EXCLUDE(revenue,n) RENAME(id AS "Order.Id", amount AS "net.amount") FROM shop.orders ORDER BY 1',
        'SELECT AGG("total.pay") FROM (SELECT * RENAME revenue AS "total.pay" FROM shop.orders) q',
        'SELECT AGG("total.pay") FROM (SELECT shop.orders.* RENAME(revenue AS "total.pay") FROM shop.orders) q',
        'SELECT AGG("total.pay") FROM (SELECT "SHOP"."ORDERS".* RENAME(revenue AS "total.pay") FROM shop.orders) q',
        'SELECT AGG("total.pay") FROM (SELECT "o.dot".* RENAME(revenue AS "total.pay") FROM shop.orders "o.dot") q',
        'SELECT * RENAME("Old.Pay" AS "New""Pay") FROM (SELECT amount AS "Old.Pay" FROM shop.orders) s ORDER BY 1',
        'SELECT AGG("total.pay") FROM (SELECT * RENAME("__ossie_quoted_rename_0" AS "total.pay") FROM '
        '(SELECT revenue AS "__ossie_quoted_rename_0" FROM shop.orders) p) q',
        'SELECT "é", AGG("π.pay") FROM (SELECT \'é\' AS "é", * '
        'RENAME /* operand quote */ (revenue AS "π.pay") FROM shop.orders) q GROUP BY "é"',
        'SELECT id, \'RENAME(revenue AS "fake")\' AS note, AGG("total.pay") FROM '
        '(SELECT * RENAME(revenue AS "total.pay") FROM shop.orders /* RENAME(n AS "unused") */) q GROUP BY id',
    ],
    ids=[
        "dotted_new",
        "mixed_case_new",
        "escaped_new",
        "dotted_old",
        "escaped_old",
        "quoted_old",
        "multiple",
        "without_parentheses",
        "schema_star",
        "quoted_schema_star",
        "quoted_dotted_alias",
        "escaped_output_name",
        "placeholder_collision",
        "unicode_and_comments",
        "literal_and_comment",
    ],
)
def test_quoted_rename_matches_ordinary_sql(route, query):
    with duckdb.connect() as connection:
        connection.execute(DDL)
        reference = query.replace("AGG(", "SUM(")
        # DuckDB accepts relation.*, but its parser does not accept schema.relation.*.
        # These sources are unaliased and unique, so the ordinary SQL equivalent
        # uses the same relation's local name.
        reference = reference.replace("shop.orders.*", "orders.*").replace('"SHOP"."ORDERS".*', "orders.*")
        result = connection.execute(reference)
        expected_rows = result.fetchall()
        # Aggregate-generated labels differ between AGG and SUM; named/scalar
        # projections still need to preserve the exact quoted output spelling.
        expected_names = [column[0] for column in result.description]
    rows, names = execute(route, query, document=_document(), ddl="SELECT 1")
    assert sorted(rows, key=repr) == sorted(expected_rows, key=repr)
    if "AGG(" not in query:
        assert names == expected_names


@pytest.mark.parametrize("engine", ["python", "rust"])
def test_persistent_view_definition_supports_quoted_rename(engine):
    from sidemantic import SemanticLayer
    from tests.interchange.ossie.test_layer2_conformance import shared_catalog

    layer = SemanticLayer.from_catalog(shared_catalog(_document()), engine=engine, fallback=False, auto_register=False)
    try:
        layer.sql('CREATE VIEW renamed AS SELECT * RENAME(revenue AS "pay""roll") FROM shop.orders')
        assert layer.sql('SELECT AGG("pay""roll") FROM renamed').fetchall() == [(30,)]
    finally:
        layer.adapter.close()
