"""Executed contracts for ad hoc references and dimension-only grouping."""

import pytest

from sidemantic import Dimension, Metric, Model, SemanticLayer


@pytest.fixture(params=["python", "rust"])
def layer(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs")
    layer = SemanticLayer(engine=request.param, fallback=False, auto_register=False)
    layer.add_model(
        Model(
            name="orders",
            table="rewrite_ci_orders",
            primary_key="id",
            dimensions=[Dimension(name="status", type="categorical"), Dimension(name="amount", type="numeric")],
            metrics=[Metric(name="sd_adhoc_metric_0", agg="sum", sql="id")],
        )
    )
    layer.adapter.execute("create table rewrite_ci_orders(id integer, status varchar, amount integer)")
    layer.adapter.execute("insert into rewrite_ci_orders values (1, 'done', 10), (2, 'done', 20), (3, 'pending', 40)")
    try:
        yield layer
    finally:
        layer.adapter.close()


@pytest.mark.parametrize(
    "sql,expected",
    [
        ("select count(*) as n from orders", [(3,)]),
        ("select sum(amount) + count(*) as total from orders", [(73,)]),
        (
            "select status, sum(amount) as total from orders group by status order by status",
            [("done", 30), ("pending", 40)],
        ),
        ("select status from orders order by status", [("done",), ("pending",)]),
    ],
)
def test_rewritten_result_contract(layer, sql, expected):
    assert layer.sql(sql).fetchall() == expected


@pytest.mark.parametrize(
    "ungrouped,expected", [(False, [("done",), ("pending",)]), (True, [("done",), ("done",), ("pending",)])]
)
def test_dimension_only_grouping_preserves_explicit_ungrouped_mode(layer, ungrouped, expected):
    query = layer.compile(dimensions=["orders.status"], order_by=["orders.status"], ungrouped=ungrouped)
    assert layer.adapter.execute(query).fetchall() == expected
