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
        ("AVG(orders.Amount)", "orders.Amount > 0", 2.5),
        ('AVG(orders."Amount")', 'orders."Amount" > 0', 15),
        ('COUNT(DISTINCT orders."Amount")', 'orders."Amount" > 0', 2),
        ("COUNT(DISTINCT orders.Amount)", "orders.Amount > 0", 2),
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


@pytest.mark.parametrize("having", ["none", "average", "sum"])
def test_postgres_filtered_float_fanout_average_preserves_values(having):
    dsn = os.environ.get("SIDEMANTIC_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("PostgreSQL execution requires SIDEMANTIC_TEST_POSTGRES_DSN")
    import psycopg
    import sidemantic_rs

    from sidemantic import Dimension, Relationship

    assert callable(sidemantic_rs.compile_with_semantic_input)
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="float_orders",
            primary_key="id",
            metrics=[
                Metric(name="average", sql='AVG("Amount")', sql_is_complete=True, filters=["paid"]),
                Metric(name="first_average", sql='AVG("Amount")', sql_is_complete=True, filters=["paid", "id != 2"]),
                Metric(name="total", sql='SUM("Amount")', sql_is_complete=True, filters=["paid"]),
                Metric(name="first_total", sql='SUM("Amount")', sql_is_complete=True, filters=["paid", "id != 2"]),
                Metric(name="row_count", agg="count"),
            ],
            relationships=[Relationship(name="items", type="one_to_many", foreign_key="order_id")],
        )
    )
    graph.add_model(
        Model(
            name="items",
            table="float_items",
            primary_key="id",
            dimensions=[Dimension(name="category", type="categorical")],
        )
    )
    filters = ["items.category IS NOT NULL"]
    expected = [
        ("a", pytest.approx(5 / 24), 0.125, 0.625, 0.25, 4),
        ("b", 0.375, None, 0.375, None, 2),
        ("orphan", None, None, None, None, 0),
    ]
    if having == "average":
        filters.append("orders.average > 0.3")
        expected = [("b", 0.375, None, 0.375, None, 2)]
    elif having == "sum":
        filters.append("orders.total > 0.5")
        expected = [("a", pytest.approx(5 / 24), 0.125, 0.625, 0.25, 4)]
    sql = compile_semantic_input(
        graph,
        {
            "metrics": [
                "orders.average",
                "orders.first_average",
                "orders.total",
                "orders.first_total",
                "orders.row_count",
            ],
            "dimensions": ["items.category"],
            "filters": filters,
            "order_by": ["items.category"],
            "dialect": "postgres",
        },
    )
    with psycopg.connect(dsn, autocommit=True) as connection:
        connection.execute('create temporary table float_orders(id integer, "Amount" double precision, paid boolean)')
        connection.execute("insert into float_orders values (1,0.125,true),(2,0.375,true),(3,null,true),(7,0.125,true)")
        connection.execute("create temporary table float_items(id integer, order_id integer, category text)")
        connection.execute(
            "insert into float_items values (1,1,'a'),(2,1,'a'),(3,1,'a'),(4,2,'a'),(5,3,'a'),(6,7,'a'),(7,7,'a'),(8,2,'b'),(9,2,'b'),(10,3,'b'),(11,999,'orphan')"
        )
        assert connection.execute(sql).fetchall() == expected


@pytest.mark.parametrize("population", ["grouped", "no_matches", "empty"])
def test_postgres_filtered_complete_row_count(population):
    dsn = os.environ.get("SIDEMANTIC_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("PostgreSQL execution requires SIDEMANTIC_TEST_POSTGRES_DSN")
    import psycopg
    import sidemantic_rs

    from sidemantic import Dimension, SecurityPolicy

    assert callable(sidemantic_rs.compile_with_semantic_input)
    graph = SemanticGraph()
    predicate = 'orders."Amount" IS NULL' if population != "no_matches" else 'orders."Amount" > 1000'
    graph.add_model(
        Model(
            name="orders",
            table="star_population",
            primary_key="id",
            dimensions=[Dimension(name="region", type="categorical")],
            metrics=[
                Metric(name="rows", sql="COUNT(*)", sql_is_complete=True, filters=[predicate]),
                Metric(name="ordinary", agg="count", filters=[predicate]),
            ],
            security=SecurityPolicy(row_filters=["id <= {{ user.max_id }}"]),
            invariant_filters=["id != 4"],
        )
    )
    query = {"metrics": ["orders.rows", "orders.ordinary"], "dialect": "postgres", "user_attributes": {"max_id": 4}}
    expected = [(0, 0)]
    if population == "grouped":
        query.update(dimensions=["orders.region"], order_by=["orders.region"])
        expected = [("a", 1, 1), ("b", 0, 0)]
    sql = compile_semantic_input(graph, query)
    with psycopg.connect(dsn, autocommit=True) as connection:
        connection.execute('create temporary table star_population(id integer, "Amount" integer, region text)')
        if population != "empty":
            connection.execute(
                "insert into star_population values (1,null,'a'),(2,2,'a'),(3,3,'b'),(4,null,'a'),(5,null,'a')"
            )
        assert connection.execute(sql).fetchall() == expected


@pytest.mark.parametrize("declaration", ["model", "graph"])
@pytest.mark.parametrize("population", ["grouped", "empty", "no_matches", "fanout"])
def test_postgres_filtered_owned_count_family(declaration, population):
    dsn = os.environ.get("SIDEMANTIC_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("PostgreSQL execution requires SIDEMANTIC_TEST_POSTGRES_DSN")
    from copy import deepcopy

    import psycopg
    import sidemantic_rs

    from sidemantic import Dimension, Relationship, SecurityPolicy
    from sidemantic.semantic_handoff import graph_to_semantic_input

    assert callable(sidemantic_rs.compile_with_semantic_input)
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="owned_count_population",
            primary_key="id",
            dimensions=[Dimension(name="region", type="categorical")],
            security=SecurityPolicy(row_filters=["tenant = {{ user.tenant }}"]),
            invariant_filters=["active"],
        )
    )
    references = []
    for name, expression in [("rows", "COUNT(*)"), ("ones", "COUNT(1)"), ("nulls", "COUNT(NULL)")]:
        predicate = "orders.status = 'paid'" if population != "no_matches" else "orders.status = 'missing'"
        metric = Metric(name=name, sql=expression, sql_is_complete=True, filters=[predicate], fill_nulls_with=99)
        if declaration == "graph":
            graph.add_metric(metric, model_name="orders")
            references.append(name)
        else:
            graph.models["orders"].metrics.append(metric)
            references.append(f"orders.{name}")
    query = {"metrics": references, "dialect": "postgres", "user_attributes": {"tenant": 1}}
    expected = [(0, 0, 0)]
    if population == "grouped":
        query.update(dimensions=["orders.region"], order_by=["orders.region"])
        expected = [("a", 2, 2, 0), ("b", 0, 0, 0)]
    elif population == "fanout":
        graph.models["orders"].relationships.append(
            Relationship(name="items", type="one_to_many", foreign_key="order_id")
        )
        graph.add_model(
            Model(
                name="items",
                table="owned_count_items",
                primary_key="id",
                dimensions=[Dimension(name="category", type="categorical")],
            )
        )
        query.update(dimensions=["items.category"], order_by=["items.category"])
        expected = [("a", 2, 2, 0), ("b", 0, 0, 0)]
    source = deepcopy(graph_to_semantic_input(graph))
    sql = compile_semantic_input(graph, query)
    assert graph_to_semantic_input(graph) == source
    with psycopg.connect(dsn, autocommit=True) as connection:
        connection.execute(
            "create temporary table owned_count_population(id integer,value integer,status text,region text,tenant integer,active boolean)"
        )
        if population != "empty":
            connection.execute(
                "insert into owned_count_population values (1,null,'paid','a',1,true),(2,2,'paid','a',1,true),(3,null,'unpaid','b',1,true),(4,null,'paid','a',1,false),(5,null,'paid','a',2,true)"
            )
        if population == "fanout":
            connection.execute("create temporary table owned_count_items(id integer,order_id integer,category text)")
            connection.execute(
                "insert into owned_count_items values (1,1,'a'),(2,1,'a'),(3,2,'a'),(4,3,'b'),(5,4,'a'),(6,5,'a')"
            )
        assert connection.execute(sql).fetchall() == expected
