"""Result contracts for SQL dependency binding, populations and lexical scopes."""

from copy import deepcopy

import pytest

from sidemantic import Dimension, Metric, Model, Relationship, SemanticLayer
from sidemantic.semantic_handoff import graph_to_semantic_input


@pytest.fixture(params=["python", "rust"])
def layer(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs", reason="Requires the built Rust extension")
    layer = SemanticLayer(engine=request.param, fallback=False, auto_register=False)
    layer.add_model(
        Model(
            name="orders",
            table="binding_orders",
            primary_key="id",
            dimensions=[
                Dimension(name="id", type="numeric"),
                Dimension(name="status", type="categorical"),
                Dimension(name="amount", type="numeric", sql="amount * 10"),
            ],
            metrics=[Metric(name="revenue", agg="sum", sql="amount"), Metric(name="count", agg="count")],
            relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
        )
    )
    layer.add_model(
        Model(
            name="customers",
            table="binding_customers",
            primary_key="id",
            dimensions=[Dimension(name="region", type="categorical")],
            metrics=[Metric(name="quota", agg="sum", sql="quota")],
        )
    )
    layer.add_metric(
        Metric(name="revenue_per_quota", type="ratio", numerator="orders.revenue", denominator="customers.quota")
    )
    layer.adapter.execute("""
        create table binding_orders(id integer, customer_id integer, status varchar, amount double);
        insert into binding_orders values (1,1,'done',100),(2,1,'done',150),(3,2,'pending',200);
        create table binding_customers(id integer, region varchar, quota double);
        insert into binding_customers values (1,'west',2),(2,'east',3);
    """)
    try:
        yield layer
    finally:
        layer.adapter.close()


def result(layer, sql):
    before = deepcopy(graph_to_semantic_input(layer.graph))
    cursor = layer.sql(sql)
    assert graph_to_semantic_input(layer.graph) == before
    if layer.engine == "rust":
        assert layer.last_engine_selection["engine"] == "rust"
    return [column[0] for column in cursor.description], cursor.fetchall()


def test_scalar_functions_bind_measure_outputs_before_arithmetic(layer):
    assert result(
        layer,
        "select orders.status, round(orders.revenue / orders.count, 2) as average from orders order by average desc",
    ) == (["status", "average"], [("pending", 200), ("done", 125)])


def test_ad_hoc_aggregate_inputs_are_physical_and_do_not_mutate_model(layer):
    # The declared amount dimension is amount * 10; aggregate inputs retain the
    # source-column meaning used by Python's ad hoc aggregate contract.
    assert result(layer, "select sum(orders.amount) as total, count(*) as n from orders") == (
        ["total", "n"],
        [(450, 3)],
    )
    assert result(layer, "select sum(o.amount) as total from orders o where o.status = 'missing'") == (
        ["total"],
        [(None,)],
    )


def test_scalar_graph_metric_keeps_independent_populations(layer):
    assert result(layer, "select round(revenue_per_quota, 2) as ratio from metrics") == (["ratio"], [(90,)])


def test_joined_scalar_measures_keep_independent_populations(layer):
    assert result(layer, "select orders.revenue / customers.quota as ratio from orders") == (["ratio"], [(90,)])


@pytest.mark.parametrize("join,expected", [("join", 450), ("left join", 500)])
def test_explicit_relationship_join_retains_row_existence(layer, join, expected):
    layer.adapter.execute("insert into binding_orders values (4,999,'orphan',50)")
    assert result(
        layer,
        f"select o.revenue as total from orders o {join} customers c on (c.id = o.customer_id)",
    ) == (["total"], [(expected,)])


def test_filter_cte_scope_preserves_nested_qualifiers_and_literal_text(layer):
    assert result(
        layer,
        """
        with allowed(status, note) as (select 'done', 'orders.status AND orders.revenue')
        select orders.revenue from orders
        where orders.status in (select status from allowed where note = 'orders.status AND orders.revenue')
        """,
    ) == (["revenue"], [(250,)])


@pytest.mark.parametrize(
    "operation,expected", [("union all", [(250,), (450,)]), ("intersect", []), ("except", [(450,)])]
)
def test_set_operations_compile_both_semantic_operands(layer, operation, expected):
    assert result(
        layer,
        f"select orders.revenue as total from orders {operation} "
        "select orders.revenue as total from orders where orders.status = 'done' order by total",
    ) == (["total"], expected)


def test_semantic_query_in_join_source_is_compiled_without_binding_outer_columns(layer):
    layer.adapter.execute("create table labels(status varchar); insert into labels values ('done'),('pending')")
    assert result(
        layer,
        "select l.status, q.total from labels l join "
        "(select orders.status, orders.revenue as total from orders) q on l.status = q.status order by l.status",
    ) == (["status", "total"], [("done", 250), ("pending", 200)])


def test_expression_dependencies_do_not_escape_requested_projection(layer):
    assert result(layer, "select orders.status, orders.revenue * 2 as doubled from orders order by doubled desc") == (
        ["status", "doubled"],
        [("done", 500), ("pending", 400)],
    )
