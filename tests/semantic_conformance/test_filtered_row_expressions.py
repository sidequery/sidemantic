"""Whole-row filtered aggregate inputs with independently specified populations."""

from copy import deepcopy

import pytest

from sidemantic import Dimension, Metric, Model, Relationship, SecurityPolicy, SemanticLayer
from sidemantic.semantic_handoff import graph_to_semantic_input

INPUTS = [
    (
        "price * qty - COALESCE(rebate, 0)",
        {"sum": 4.0, "avg": 4 / 3, "count": 3, "count_distinct": 2, "min": -2.0, "max": 3.0},
    ),
    ("COALESCE(price * qty, 7)", {"sum": 18.0, "avg": 3.6, "count": 5, "count_distinct": 3, "min": -2.0, "max": 7.0}),
    (
        "CASE WHEN qty > 0 THEN price * qty ELSE 9 END",
        {"sum": 13.0, "avg": 3.25, "count": 4, "count_distinct": 3, "min": -2.0, "max": 9.0},
    ),
]


@pytest.fixture(params=["python", "rust"])
def engine(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs", reason="Row-expression acceptance requires the real Rust extension")
    return request.param


@pytest.fixture(params=["model", "graph"])
def layer(request, engine):
    layer = SemanticLayer(engine=engine, fallback=False, auto_register=False)
    layer.add_model(
        Model(
            name="orders",
            table="filtered_row_orders",
            primary_key="id",
            dimensions=[
                Dimension(name="price", type="numeric", sql="price * 100"),
                Dimension(name="qty", type="numeric", sql="qty + 10"),
                Dimension(name="region", type="categorical"),
            ],
            security=SecurityPolicy(row_filters=["tenant = {{ user.tenant }}"]),
            invariant_filters=["active"],
        )
    )
    layer.adapter.execute("""
        create table filtered_row_orders(id integer,price double,qty integer,rebate double,paid boolean,status varchar,region varchar,tenant integer,active boolean);
        insert into filtered_row_orders values
          (1,1.5,2,null,true,'orders.paid','a',1,true), (2,2,null,0.5,true,'other','a',1,true),
          (3,null,1,null,true,'other','b',1,true), (4,9,0,null,false,'other','a',1,true),
          (5,100,2,null,true,'other','b',2,true), (6,100,2,null,true,'other','a',1,false),
          (7,-1,2,0,true,'other','b',1,true), (8,1.5,2,null,true,'other','b',1,true),
          (9,4,1,null,null,'other','c',1,true);
    """)
    try:
        yield layer, request.param
    finally:
        layer.adapter.close()


def add_metric(layer, expression, aggregation, name="measure", filters=None):
    semantic, declaration = layer
    sql = f"COUNT(DISTINCT {expression})" if aggregation == "count_distinct" else f"{aggregation.upper()}({expression})"
    metric = Metric(name=name, sql=sql, sql_is_complete=True, filters=filters if filters is not None else ["paid"])
    if declaration == "graph":
        semantic.graph.add_metric(metric, model_name="orders")
        return name
    semantic.graph.models["orders"].metrics.append(metric)
    return f"orders.{name}"


def result(layer, metrics, **query):
    semantic, _ = layer
    query.setdefault("user_attributes", {"tenant": 1})
    source = deepcopy(graph_to_semantic_input(semantic.graph))
    sql = semantic.compile(metrics=metrics, **query)
    assert graph_to_semantic_input(semantic.graph) == source
    if semantic.engine == "rust":
        assert semantic.last_engine_selection["engine"] == "rust"
    cursor = semantic.adapter.execute(sql)
    return [field[0] for field in cursor.description], cursor.fetchall()


@pytest.mark.parametrize("expression,expected", INPUTS)
@pytest.mark.parametrize("aggregation", ["sum", "avg", "count", "count_distinct", "min", "max"])
def test_filtered_input_is_evaluated_before_metric_filter(layer, expression, expected, aggregation):
    reference = add_metric(layer, expression, aggregation)
    columns, rows = result(layer, [reference])
    assert columns == ["measure"]
    assert rows == [(pytest.approx(expected[aggregation]),)]


@pytest.mark.parametrize("expression,expected", INPUTS)
def test_filtered_expressions_survive_unequal_keyed_fanout(layer, expression, expected):
    semantic, _ = layer
    semantic.graph.models["orders"].relationships.append(
        Relationship(name="items", type="one_to_many", foreign_key="order_id")
    )
    semantic.add_model(
        Model(
            name="items",
            table="filtered_row_items",
            primary_key="id",
            dimensions=[Dimension(name="category", type="categorical")],
        )
    )
    semantic.adapter.execute("""
        create table filtered_row_items(id integer,order_id integer,category varchar);
        insert into filtered_row_items values (1,1,'all'),(2,1,'all'),(3,1,'all'),(4,2,'all'),
          (5,3,'all'),(6,3,'all'),(7,4,'all'),(8,5,'all'),(9,6,'all'),(10,7,'all'),
          (11,8,'all'),(12,8,'all'),(13,9,'empty'),(14,99,'orphan');
    """)
    aggregations = ["sum", "avg", "count", "count_distinct", "min", "max"]
    refs = [add_metric(layer, expression, agg, name=agg) for agg in aggregations]
    columns, rows = result(layer, refs, dimensions=["items.category"], order_by=["items.category"])
    assert columns == ["category", *aggregations]
    assert rows == [
        tuple(["all", *[pytest.approx(expected[agg]) for agg in aggregations]]),
        ("empty", None, None, 0, 0, None, None),
    ]


def test_excluded_groups_do_not_get_case_or_coalesce_fallback_values(layer):
    refs = [
        add_metric(layer, "COALESCE(price * qty, 7)", agg, name=agg)
        for agg in ["sum", "avg", "count", "count_distinct"]
    ]
    assert result(layer, refs, dimensions=["orders.region"], order_by=["orders.region"]) == (
        ["region", "sum", "avg", "count", "count_distinct"],
        [("a", 10.0, 5.0, 2, 2), ("b", 8.0, pytest.approx(8 / 3), 3, 3), ("c", None, None, 0, 0)],
    )


def test_simple_case_and_qualified_columns_preserve_literals(layer):
    expression = "CASE orders.status WHEN 'orders.paid' THEN orders.price + 1 ELSE COALESCE(orders.price, 0) END"
    reference = add_metric(layer, expression, "sum")
    assert result(layer, [reference]) == (["measure"], [(5.0,)])


@pytest.mark.parametrize("population", ["empty", "no_matches"])
def test_empty_filtered_row_expression_populations(layer, population):
    semantic, _ = layer
    if population == "empty":
        semantic.adapter.execute("delete from filtered_row_orders")
    aggregations = ["sum", "avg", "count", "count_distinct", "min", "max"]
    refs = [add_metric(layer, "COALESCE(price, 7)", agg, name=agg, filters=["id < 0"]) for agg in aggregations]
    assert result(layer, refs) == (aggregations, [(None, None, 0, 0, None, None)])


def test_independent_whole_expression_filters_and_having(layer):
    refs = [
        add_metric(layer, "COALESCE(price * qty, 7)", "sum", name="paid"),
        add_metric(layer, "CASE WHEN price > 0 THEN price ELSE 9 END", "sum", name="unpaid", filters=["NOT paid"]),
        add_metric(layer, "*", "count", name="rows"),
    ]
    assert result(layer, refs) == (["paid", "unpaid", "rows"], [(18.0, 9.0, 5)])
    assert result(layer, refs, filters=[f"{refs[0]} > 17"]) == (["paid", "unpaid", "rows"], [(18.0, 9.0, 5)])
    assert result(layer, refs, filters=[f"{refs[0]} > 18"]) == (["paid", "unpaid", "rows"], [])


def test_predicates_modulo_and_negation_in_case(layer):
    expression = (
        "CASE WHEN NOT (qty IS NULL) AND qty BETWEEN 1 AND 2 "
        "AND status IN ('orders.paid', 'other') AND (price >= 0 OR price < 0) "
        "THEN -(qty % 2) + COALESCE(price, 0) ELSE 9 END"
    )
    reference = add_metric(layer, expression, "sum")
    # Qualified inputs are 1.5, 9, 9, -1, 1.5 for ids 1, 2, 3, 7, 8.
    # A null price makes the comparison unknown, selecting the ELSE branch.
    assert result(layer, [reference]) == (["measure"], [(20.0,)])


def test_distinct_case_strings_keep_physical_literal_values(layer):
    reference = add_metric(
        layer, "CASE WHEN qty IS NULL THEN 'orders.price' ELSE COALESCE(status, 'missing') END", "count_distinct"
    )
    assert result(layer, [reference]) == (["measure"], [(3,)])


@pytest.mark.parametrize("declaration", ["model", "graph"])
@pytest.mark.parametrize("expression,expected", INPUTS)
@pytest.mark.parametrize("fanout", [False, True])
def test_postgres_filtered_row_expression_oracles(declaration, expression, expected, fanout):
    import os

    dsn = os.environ.get("SIDEMANTIC_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("PostgreSQL execution requires SIDEMANTIC_TEST_POSTGRES_DSN")
    import psycopg
    import sidemantic_rs

    from sidemantic.core.semantic_graph import SemanticGraph
    from sidemantic.rust_bridge import compile_semantic_input

    assert callable(sidemantic_rs.compile_with_semantic_input)
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="row_population",
            primary_key="id",
            security=SecurityPolicy(row_filters=["tenant = {{ user.tenant }}"]),
            invariant_filters=["active"],
        )
    )
    aggregations = ["sum", "avg", "count", "count_distinct", "min", "max"]
    references = []
    for agg in aggregations:
        sql = f"COUNT(DISTINCT {expression})" if agg == "count_distinct" else f"{agg.upper()}({expression})"
        metric = Metric(name=agg, sql=sql, sql_is_complete=True, filters=["paid"])
        if declaration == "graph":
            graph.add_metric(metric, model_name="orders")
            references.append(agg)
        else:
            graph.models["orders"].metrics.append(metric)
            references.append(f"orders.{agg}")
    query = {"metrics": references, "dialect": "postgres", "user_attributes": {"tenant": 1}}
    expected_rows = [tuple(pytest.approx(expected[agg]) for agg in aggregations)]
    if fanout:
        graph.models["orders"].relationships.append(
            Relationship(name="items", type="one_to_many", foreign_key="order_id")
        )
        graph.add_model(
            Model(
                name="items",
                table="row_items",
                primary_key="id",
                dimensions=[Dimension(name="category", type="categorical")],
            )
        )
        query.update(dimensions=["items.category"], order_by=["items.category"])
        expected_rows = [("all", *expected_rows[0]), ("empty", None, None, 0, 0, None, None)]
    source = deepcopy(graph_to_semantic_input(graph))
    sql = compile_semantic_input(graph, query)
    assert graph_to_semantic_input(graph) == source
    with psycopg.connect(dsn, autocommit=True) as connection:
        connection.execute(
            "create temporary table row_population(id integer,price double precision,qty integer,rebate double precision,paid boolean,tenant integer,active boolean)"
        )
        connection.execute(
            "insert into row_population values (1,1.5,2,null,true,1,true),(2,2,null,0.5,true,1,true),(3,null,1,null,true,1,true),(4,9,0,null,false,1,true),(5,100,2,null,true,2,true),(6,100,2,null,true,1,false),(7,-1,2,0,true,1,true),(8,1.5,2,null,true,1,true),(9,4,1,null,null,1,true)"
        )
        if fanout:
            connection.execute("create temporary table row_items(id integer,order_id integer,category text)")
            connection.execute(
                "insert into row_items values (1,1,'all'),(2,1,'all'),(3,1,'all'),(4,2,'all'),(5,3,'all'),(6,3,'all'),(7,4,'all'),(8,5,'all'),(9,6,'all'),(10,7,'all'),(11,8,'all'),(12,8,'all'),(13,9,'empty'),(14,99,'orphan')"
            )
        assert connection.execute(sql).fetchall() == expected_rows


def test_filtered_expression_cross_source_leaf_keeps_count_zero(layer):
    semantic, _ = layer
    # Permit the unmatched region to exercise the absent count leaf; policy
    # enforcement is covered separately by the shared fixture's other queries.
    semantic.graph.models["orders"].security = None
    semantic.graph.models["orders"].invariant_filters = []
    semantic.graph.models["orders"].relationships.append(
        Relationship(name="regions", type="many_to_one", foreign_key="region", primary_key="region")
    )
    semantic.add_model(
        Model(
            name="regions",
            table="row_regions",
            primary_key="region",
            dimensions=[Dimension(name="region", type="categorical")],
            metrics=[Metric(name="quota", agg="sum", sql="quota")],
        )
    )
    reference = add_metric(layer, "CASE WHEN qty > 0 THEN price ELSE 9 END", "count")
    semantic.add_metric(Metric(name="combined", type="derived", sql=f"{reference} + regions.quota"))
    semantic.adapter.execute(
        "create table row_regions(region varchar,quota integer); insert into row_regions values ('a',10),('b',20),('c',30),('d',40)"
    )
    assert result(layer, ["combined"], dimensions=["regions.region"], order_by=["regions.region"]) == (
        ["region", "combined"],
        [("a", 13), ("b", 23), ("c", 30), ("d", 40)],
    )
