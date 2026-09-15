"""Declared dialects cross the same inert boundary without Python binding."""

import json

import pytest
import sqlglot

from sidemantic import Dimension, Metric, Model, Parameter, SecurityPolicy, SemanticLayer
from sidemantic.rust_bridge import compile_semantic_input, rewrite_semantic_input
from sidemantic.semantic_handoff import graph_to_semantic_input


@pytest.fixture(params=["python", "rust"])
def layer(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs", reason="Requires matching native extension")
    layer = SemanticLayer(engine=request.param, fallback=False, auto_register=False)
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[Dimension(name="label", type="categorical"), Dimension(name="tenant", type="numeric")],
            metrics=[Metric(name="value", agg="sum", sql="coalesce(amount, 0)")],
        )
    )
    layer.adapter.execute("create table orders(id integer, label varchar, tenant integer, amount integer)")
    layer.adapter.execute("insert into orders values (1,'a',1,10),(2,'a',1,NULL),(3,'b',2,100),(4,NULL,1,5)")
    yield layer
    layer.adapter.close()


@pytest.mark.parametrize("dialect", ["duckdb", "postgres", "bigquery", "snowflake", "mysql", "spark", "tsql"])
def test_target_dialect_policy_population(layer, dialect):
    model = layer.graph.models["orders"]
    model.security = SecurityPolicy(row_filters=["tenant = {{ user.tenant }}"])
    model.invariant_filters = ["id <> 2"]
    sql = layer.compile(metrics=["orders.value"], dialect=dialect, user_attributes={"tenant": 1})
    # SQLGlot is only the test execution adapter for unavailable warehouses;
    # expected rows are independent of either semantic compiler's SQL shape.
    executable = sqlglot.transpile(sql, read=dialect, write="duckdb")[0]
    assert layer.adapter.execute(executable).fetchall() == [(15,)]


@pytest.mark.parametrize(
    "dialect,expression,filter_sql,order_sql",
    [
        ("bigquery", "IFNULL(`amount`, 0)", "`orders`.`tenant` = 1", "`orders`.`label` DESC NULLS LAST"),
        ("mysql", "IFNULL(`amount`, 0)", "`orders`.`tenant` = 1", "`orders`.`label` DESC"),
        ("tsql", "ISNULL([amount], 0)", "[orders].[tenant] = 1", "[orders].[label] DESC"),
        ("snowflake", 'COALESCE("amount", 0)', '"orders"."tenant" = 1', '"orders"."label" DESC NULLS LAST'),
        ("postgres", 'COALESCE("amount", 0)', '"orders"."tenant" = 1', '"orders"."label" DESC NULLS LAST'),
    ],
)
def test_native_declared_graph_and_query_syntax(layer, dialect, expression, filter_sql, order_sql):
    if layer.engine != "rust":
        pytest.skip("Native boundary syntax contract; target-dialect result oracle is shared above")
    layer.graph.models["orders"].metrics[0].sql = expression
    before = graph_to_semantic_input(layer.graph, input_dialect=dialect)
    query = {
        "metrics": ["orders.value"],
        "dimensions": ["orders.label"],
        "filters": [filter_sql],
        "order_by": [order_sql],
        "dialect": "duckdb",
    }
    sql = compile_semantic_input(layer.graph, query, input_dialect=dialect)
    assert layer.adapter.execute(sql).fetchall() == [("a", 10), (None, 5)]
    # Quoted metric references must also resolve to the aggregate output,
    # retaining explicit ordering rather than leaking the physical qualifier.
    query["order_by"] = [order_sql.replace("label", "value").replace("DESC", "ASC")]
    sql = compile_semantic_input(layer.graph, query, input_dialect=dialect)
    assert layer.adapter.execute(sql).fetchall() == [(None, 5), ("a", 10)]
    assert graph_to_semantic_input(layer.graph, input_dialect=dialect) == before


@pytest.mark.parametrize("dialect,quoted", [("bigquery", "`"), ("mysql", "`"), ("postgres", '"')])
def test_native_rewrite_syntax_and_policy_context(layer, dialect, quoted):
    if layer.engine != "rust":
        pytest.skip("Native rewrite boundary contract")
    layer.graph.models["orders"].security = SecurityPolicy(row_filters=["tenant = {{ user.tenant }}"])
    sql = f"select orders.{quoted}value{quoted} from metrics where orders.{quoted}tenant{quoted} = 1"
    rewritten = rewrite_semantic_input(
        layer.graph,
        sql,
        sql_dialect=dialect,
        output_dialect="duckdb",
        user_attributes={"tenant": 1},
    )
    assert layer.adapter.execute(rewritten).fetchall() == [(15,)]


def test_native_fragment_framing_and_source_snapshot(layer):
    if layer.engine != "rust":
        pytest.skip("Native input validation contract")
    import sidemantic_rs

    source = graph_to_semantic_input(layer.graph)
    for field, sql in [("filters", "1 = 1 LIMIT 1"), ("order_by", "orders.value DESC LIMIT 1")]:
        query = {"metrics": ["orders.value"], "query_dialect": "bigquery", field: [sql]}
        with pytest.raises(Exception, match="extra clauses"):
            sidemantic_rs.compile_with_semantic_input(json.dumps(source), json.dumps(query))
    source["models"][0]["metrics"][0]["metadata"] = {"ossie_target_dialect": "bigquery"}
    source["models"][0]["metrics"][0]["sql"] = "IFNULL(`amount`, 0)"
    sql = sidemantic_rs.compile_with_semantic_input(json.dumps(source), '{"metrics":["orders.value"]}')
    assert layer.adapter.execute(sql).fetchall() == [(115,)]


@pytest.mark.parametrize("dialect", ["mysql", "bigquery", "snowflake", "spark"])
@pytest.mark.parametrize("value", [r"a\b", r"a\' OR 1=1 --", "O'Brien"])
def test_source_dialect_preserves_typed_policy_and_parameter_values(layer, dialect, value):
    if layer.engine != "rust":
        pytest.skip("Native source-dialect literal contract")
    layer.adapter.executemany("insert into orders values (?, ?, ?, ?)", [(5, value, 1, 7)])
    model = layer.graph.models["orders"]
    model.security = SecurityPolicy(row_filters=["label = {{ user.label }}"])
    sql = compile_semantic_input(
        layer.graph,
        {"metrics": ["orders.value"], "user_attributes": {"label": value}},
        input_dialect=dialect,
    )
    assert layer.adapter.execute(sql).fetchall() == [(7,)]
    model.security = None
    layer.graph.add_parameter(Parameter(name="label_value", type="string"))
    sql = compile_semantic_input(
        layer.graph,
        {
            "metrics": ["orders.value"],
            "filters": ["orders.label = {{ label_value }}"],
            "parameter_values": {"label_value": value},
        },
        query_dialect=dialect,
    )
    assert layer.adapter.execute(sql).fetchall() == [(7,)]


@pytest.mark.parametrize("dialect", ["mysql", "bigquery", "snowflake", "spark"])
@pytest.mark.parametrize("value", [r"a\b", r"a\' OR 1=1 --", "O'Brien"])
def test_target_policy_literals_survive_alias_wrapping(layer, dialect, value):
    layer.adapter.executemany("insert into orders values (?, ?, ?, ?)", [(5, value, 1, 7)])
    layer.graph.models["orders"].security = SecurityPolicy(row_filters=["label = {{ user.label }}"])
    sql = layer.compile(
        metrics=["orders.value"],
        aliases={"orders.value": "Gross value"},
        dialect=dialect,
        user_attributes={"label": value},
    )
    executable = sqlglot.transpile(sql, read=dialect, write="duckdb")[0]
    cursor = layer.adapter.execute(executable)
    assert cursor.fetchall() == [(7,)]
    assert cursor.description[0][0] == "Gross value"
