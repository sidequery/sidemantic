"""Cartesian relationship populations use the same exact grains in both compilers."""

import pytest

from sidemantic import Dimension, Explore, Metric, Model, Relationship, SecurityPolicy, SemanticLayer


@pytest.fixture(params=["python", "rust"])
def layer(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs", reason="Cross relationship parity requires the real extension")
    layer = SemanticLayer(engine=request.param, fallback=False, auto_register=False)
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[Dimension(name="status", type="categorical")],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
            relationships=[Relationship(name="calendar", type="cross")],
        )
    )
    layer.add_model(
        Model(
            name="calendar",
            table="calendar",
            primary_key="id",
            dimensions=[Dimension(name="label", type="categorical")],
            metrics=[Metric(name="days", agg="count")],
            security=SecurityPolicy(row_filters=["tenant = {{ user.tenant }}"]),
        )
    )
    layer.graph.add_explore(Explore(name="sales", model="orders"))
    layer.adapter.execute("""
        create table orders(id integer, status varchar, amount integer);
        insert into orders values (1,'paid',10),(2,'paid',20),(3,null,30);
        create table calendar(id integer, label varchar, tenant integer);
        insert into calendar values (1,'weekday',1),(2,'weekday',1),(3,null,1),(4,'hidden',2);
    """)
    try:
        yield layer
    finally:
        layer.adapter.close()


def rows(layer, **query):
    sql = layer.compile(user_attributes={"tenant": 1}, **query)
    assert layer.last_engine_selection == {"engine": layer.engine, "reason": None}
    return layer.adapter.execute(sql).fetchall()


def test_cross_dimensions_need_no_join_keys_and_preserve_nulls(layer):
    layer.graph.models["orders"].primary_key = []
    layer.graph.models["calendar"].primary_key = []
    assert set(rows(layer, dimensions=["orders.status", "calendar.label"])) == {
        ("paid", "weekday"),
        ("paid", None),
        (None, "weekday"),
        (None, None),
    }


def test_cross_measure_deduplicates_source_identity(layer):
    assert set(rows(layer, metrics=["orders.revenue"], dimensions=["calendar.label"])) == {("weekday", 60), (None, 60)}
    assert set(rows(layer, metrics=["calendar.days"], dimensions=["orders.status"])) == {("paid", 3), (None, 3)}


def test_cross_explore_empty_secured_target_removes_anchor_rows(layer):
    assert rows(layer, explore="sales", metrics=["revenue"], filters=["calendar.label = 'absent'"]) == [(None,)]
    assert rows(layer, metrics=["orders.revenue", "calendar.days"], filters=["calendar.label = 'absent'"]) == [
        (None, 0)
    ]


def test_cross_both_measure_populations(layer):
    assert rows(layer, metrics=["orders.revenue", "calendar.days"]) == [(60, 3)]


def test_cross_empty_unfiltered_sibling_preserves_empty_product(layer):
    layer.graph.models["calendar"].security = None
    layer.adapter.execute("delete from calendar")
    assert rows(layer, metrics=["orders.revenue", "calendar.days"]) == [(None, 0)]
