"""Case-sensitive PostgreSQL execution for complete metric filter lowering."""

import os

import pytest

from sidemantic import Metric, Model
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.rust_bridge import compile_semantic_input


@pytest.mark.parametrize(
    "expression,predicate,expected",
    [
        ("SUM(orders.Amount)", "orders.Amount > 0", 5),
        ('SUM(orders."Amount")', 'orders."Amount" > 0', 30),
        (
            "SUM(orders.Amount)",
            """orders.Amount > 0 AND orders."Amount" = 10 AND orders.label = 'orders.Amount'""",
            2,
        ),
    ],
)
def test_complete_filters_preserve_identifier_identity(expression, predicate, expected):
    dsn = os.environ.get("SIDEMANTIC_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("PostgreSQL execution requires SIDEMANTIC_TEST_POSTGRES_DSN")
    import psycopg
    import sidemantic_rs

    assert callable(sidemantic_rs.compile_with_semantic_input)
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="filter_case_population",
            primary_key="id",
            metrics=[Metric(name="total", sql=expression, sql_is_complete=True, filters=[predicate])],
        )
    )
    sql = compile_semantic_input(graph, {"metrics": ["orders.total"], "dialect": "postgres"})
    with psycopg.connect(dsn, autocommit=True) as connection:
        connection.execute(
            'create temporary table filter_case_population (id integer, amount integer, "Amount" integer, label text)'
        )
        connection.execute(
            "insert into filter_case_population values "
            "(1, 2, 10, 'orders.Amount'), (2, 3, 20, 'different'), (3, -5, -30, 'orders.Amount')"
        )
        assert connection.execute(sql).fetchall() == [(expected,)]
