"""Executed contracts for ad hoc references and dimension-only grouping."""

import pytest

from sqlctx import Dimension, Metric, Model, SemanticLayer


@pytest.fixture(params=["python", "rust"])
def layer(request):
    if request.param == "rust":
        pytest.importorskip("sqlctx_rs")
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


@pytest.mark.parametrize(
    "sql",
    [
        "select orders.sd_adhoc_metric_0 from orders group by orders.status",
        "select orders.status, orders.amount from orders group by orders.status",
        "select orders.status from orders group by 1",
        "select orders.status from orders group by lower(orders.status)",
    ],
)
def test_group_by_cannot_silently_change_selected_grain(layer, sql):
    with pytest.raises(ValueError, match="GROUP BY"):
        layer.sql(sql)


@pytest.mark.parametrize("group", ["orders.status", "status", "state"])
def test_group_by_accepts_exact_selected_dimensions_and_aliases(layer, group):
    assert layer.sql(
        f"select orders.status as state, orders.sd_adhoc_metric_0 as total from orders group by {group} order by state"
    ).fetchall() == [("done", 3), ("pending", 3)]


@pytest.mark.parametrize(
    "sql",
    [
        "select orders.status, (select count(*) from labels l where l.status = orders.status) as n from orders",
        "select o.status, (select count(*) from labels l where l.status = o.status) as n from orders o",
        "select orders.status from orders where exists (select 1 from labels l where l.status = orders.status)",
        "select orders.status, (select (select count(*) from labels l where l.status = orders.status)) as n from orders",
        "select o.status, (with counts as (select count(*) as n from labels l where l.status = o.status) "
        "select n from counts) as n from orders o",
        "with result as (select o.status, (select count(*) from labels l where l.status = o.status) as n "
        "from orders o) select * from result",
        # A sibling's local alias cannot hide a true reference in another scope.
        "select o.status, (select count(*) from labels o), "
        "(select count(*) from labels l where l.status = o.status) from orders o",
    ],
)
def test_correlated_semantic_sources_fail_before_execution(layer, sql, monkeypatch):
    # The physical table really exists: these are unsupported semantic shapes,
    # not failures caused by an absent subquery source.
    layer.adapter.execute("create table labels(status varchar); insert into labels values ('done'), ('done')")

    def unexpected_execution(*args, **kwargs):
        pytest.fail("A correlated semantic query reached database execution")

    monkeypatch.setattr(layer.adapter, "execute", unexpected_execution)
    with pytest.raises(ValueError, match="Correlated subquery references semantic source"):
        layer.sql(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "select orders.status, (select count(*) from labels orders where orders.status = 'done') as n "
        "from orders order by orders.status",
        "select o.status, (select count(*) from labels o where o.status = 'done') as n from orders o order by o.status",
        "select o.status, (select (select count(*) from labels o where o.status = 'done')) as n "
        "from orders o order by o.status",
        "select o.status, (with counts as (select count(*) as n from labels o where o.status = 'done') "
        "select n from counts) as n from orders o order by o.status",
        "select o.status, (select count(*) from labels o where exists "
        "(select 1 where o.status = 'done')) as n from orders o order by o.status",
    ],
)
def test_subquery_local_alias_can_shadow_outer_semantic_source(layer, sql):
    layer.adapter.execute("create table labels(status varchar); insert into labels values ('done'), ('done')")
    assert layer.sql(sql).fetchall() == [("done", 2), ("pending", 2)]
