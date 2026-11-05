"""Stored views isolate name binding; wildcard modifiers preserve typed columns."""

import duckdb
import pytest
import yaml

from sidemantic.sql.query_rewriter import QueryRewriter
from tests.interchange.ossie.test_layer2_conformance import ROUTES, execute, shared_catalog
from tests.rust_layer_adapter import _graph_yaml_from_graph, _rust_request


def _document():
    return {
        "version": "0.2.0.dev0",
        "name": "shop",
        "datasets": [
            {
                "name": "orders",
                "source": "SELECT 1 AS id,10 AS amount UNION ALL SELECT 2,20",
                "fields": [{"name": name, "expression": {"sql": name}} for name in ("id", "amount")],
                "metrics": [
                    {"name": "revenue", "expression": {"sql": "SUM(amount)"}},
                    {"name": "n", "expression": {"sql": "COUNT(*)"}},
                ],
            }
        ],
    }


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize(
    "views,query,expected",
    [
        (
            {"summary": "SELECT TO_MEASURE(SUM(amount)) AS total FROM orders"},
            "WITH orders AS(SELECT 999 AS amount) SELECT AGG(total) FROM summary",
            [(30,)],
        ),
        (
            {
                "base_view": "SELECT TO_MEASURE(SUM(amount)) AS total FROM orders",
                "summary": "SELECT total FROM base_view",
            },
            "WITH base_view AS(SELECT TO_MEASURE(SUM(999)) AS total) SELECT AGG(total) FROM summary",
            [(30,)],
        ),
        (
            {"summary": "WITH orders AS(SELECT 7 AS amount) SELECT TO_MEASURE(SUM(amount)) AS total FROM orders"},
            "WITH orders AS(SELECT 999 AS amount) SELECT AGG(total) FROM summary",
            [(7,)],
        ),
        (
            {"summary": "SELECT TO_MEASURE(SUM(amount)) AS total FROM orders"},
            "WITH summary AS(SELECT TO_MEASURE(SUM(999)) AS total) SELECT AGG(total) FROM summary",
            [(999,)],
        ),
    ],
    ids=["dataset_shadow", "nested_view_shadow", "view_local_cte", "caller_cte_still_shadows_view"],
)
def test_stored_view_has_its_own_cte_scope(route, views, query, expected):
    graph = next(iter(shared_catalog(_document()).scopes.values())).graph
    graph.metadata["ossie_views"] = views
    if route == "native_rust":
        payload = yaml.safe_load(_graph_yaml_from_graph(graph))
        payload["metadata"] = graph.metadata
        response = _rust_request({"action": "rewrite_sql", "models_yaml": yaml.safe_dump(payload), "sql": query})
        assert response["status"] == "ok", response
        compiled = response["sql"]
    else:
        compiled = QueryRewriter(graph, use_rust_rewriter=route == "public_rust", rust_no_fallback=True).rewrite(query)
    with duckdb.connect() as connection:
        assert connection.execute(compiled).fetchall() == expected


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize(
    "query,expected,names",
    [
        (
            "SELECT * EXCLUDE(revenue,n) FROM shop.orders ORDER BY id",
            [(1, 10), (2, 20)],
            ["id", "amount"],
        ),
        (
            "SELECT o.* EXCLUDE(revenue,n) FROM shop.orders o ORDER BY id",
            [(1, 10), (2, 20)],
            ["id", "amount"],
        ),
        (
            "SELECT amount,AGG(revenue) AS revenue,AGG(n) AS n FROM(SELECT * REPLACE(amount*2 AS amount) FROM shop.orders) q GROUP BY amount ORDER BY amount",
            [(20, 10, 1), (40, 20, 1)],
            ["amount", "revenue", "n"],
        ),
        (
            "SELECT AGG(pay) AS pay,AGG(n) AS n FROM(SELECT * RENAME(revenue AS pay) FROM shop.orders) q",
            [(30, 2)],
            ["pay", "n"],
        ),
        (
            "SELECT * EXCLUDE(n) REPLACE(99 AS revenue) FROM shop.orders ORDER BY id",
            [(1, 10, 99), (2, 20, 99)],
            ["id", "amount", "revenue"],
        ),
        (
            "SELECT AGG(revenue) AS revenue FROM(SELECT * REPLACE(TO_MEASURE(SUM(amount*2)) AS revenue) FROM shop.orders) q",
            [(60,)],
            ["revenue"],
        ),
        (
            "SELECT amount,AGG(pay) AS pay FROM(SELECT o.* EXCLUDE(n) REPLACE(amount*2 AS amount) RENAME(revenue AS pay) FROM shop.orders o) q GROUP BY amount ORDER BY amount",
            [(20, 10), (40, 20)],
            ["amount", "pay"],
        ),
    ],
    ids=[
        "exclude",
        "qualified_exclude",
        "replace_field",
        "rename_measure",
        "replace_measure_scalar",
        "replace_measure_recipe",
        "combined",
    ],
)
def test_wildcard_modifiers_preserve_projection_and_measure_lineage(route, query, expected, names):
    rows, columns = execute(route, query, document=_document(), ddl="SELECT 1")
    assert rows == expected
    assert columns == names


@pytest.mark.parametrize("route", ROUTES)
def test_quoted_wildcard_rename_never_silently_changes_identifier_identity(route):
    query = 'SELECT AGG("total.pay") AS total FROM(SELECT * RENAME(revenue AS "total.pay") FROM shop.orders) q'
    if route == "python":
        assert execute(route, query, document=_document(), ddl="SELECT 1") == ([(30,)], ["total"])
    else:
        with pytest.raises(ValueError, match="quoted wildcard RENAME"):
            execute(route, query, document=_document(), ddl="SELECT 1")


@pytest.mark.parametrize("route", ROUTES)
def test_rename_text_in_literals_and_comments_is_not_a_modifier(route):
    query = """SELECT id,'RENAME(revenue AS "x")' AS note FROM shop.orders
        /* RENAME(revenue AS "x") */ ORDER BY id"""
    assert execute(route, query, document=_document(), ddl="SELECT 1")[0] == [
        (1, 'RENAME(revenue AS "x")'),
        (2, 'RENAME(revenue AS "x")'),
    ]


@pytest.mark.parametrize("route", ROUTES)
def test_physical_wildcard_rename_remains_ordinary_sql(route):
    graph = next(iter(shared_catalog(_document()).scopes.values())).graph
    query = 'SELECT * RENAME(amount AS "x") FROM(VALUES(10)) r(amount)'
    if route == "native_rust":
        response = _rust_request({"action": "rewrite_sql", "models_yaml": _graph_yaml_from_graph(graph), "sql": query})
        assert response["status"] == "ok", response
        compiled = response["sql"]
    else:
        compiled = QueryRewriter(graph, use_rust_rewriter=route == "public_rust", rust_no_fallback=True).rewrite(query)
    with duckdb.connect() as connection:
        assert connection.execute(compiled).fetchall() == [(10,)]
