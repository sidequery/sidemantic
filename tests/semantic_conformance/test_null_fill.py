"""Null defaults apply to results and dependencies without adding source rows."""

import pytest

from sidemantic import Dimension, Metric, Model, Relationship, SecurityPolicy, SemanticLayer


@pytest.fixture(params=["python", "rust"])
def layer(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs", reason="Null-fill acceptance requires the matching Rust extension")
    layer = SemanticLayer(engine=request.param, fallback=False, auto_register=False)
    layer.add_model(
        Model(
            name="events",
            table="fill_events",
            primary_key="id",
            dimensions=[Dimension(name="category"), Dimension(name="amount", type="numeric")],
            metrics=[
                Metric(name="filled", agg="sum", sql="amount", fill_nulls_with=0),
                Metric(name="raw", agg="sum", sql="amount"),
                Metric(name="fractional", agg="sum", sql="amount", fill_nulls_with=-1.5),
                Metric(name="label", agg="min", sql="label", fill_nulls_with="missing's label"),
                Metric(name="incremented", type="derived", sql="filled + 1"),
                Metric(name="own_default", type="derived", sql="raw + 1", fill_nulls_with=-3),
                Metric(name="zero", agg="sum", sql="0"),
                Metric(name="ratio", type="ratio", numerator="raw", denominator="zero", fill_nulls_with=9),
            ],
        )
    )
    layer.adapter.execute("""
        create table fill_events(id integer, category varchar, amount integer, label varchar);
        insert into fill_events values (1, 'a', 10, null), (2, 'b', null, null), (3, 'a', -10, 'present');
    """)
    try:
        yield layer
    finally:
        layer.adapter.close()


@pytest.mark.parametrize(
    "metrics,filters,expected",
    [
        (["filled", "raw"], [], [(0, 0)]),
        (["filled", "raw", "fractional"], ["events.amount > 99"], [(0, None, -1.5)]),
        (["label"], ["events.amount > 99"], [("missing's label",)]),
        (["label"], [], [("present",)]),
        (["incremented", "own_default"], ["events.amount > 99"], [(1, -3)]),
        (["ratio"], [], [(9,)]),
    ],
)
def test_metric_result_and_dependency_defaults(layer, metrics, filters, expected):
    sql = layer.compile(metrics=[f"events.{metric}" for metric in metrics], filters=filters)
    assert layer.adapter.execute(sql).fetchall() == expected


def test_filled_leaf_participates_in_cross_source_calculation(layer):
    layer.add_model(
        Model(
            name="groups",
            table="fill_groups",
            primary_key="category",
            dimensions=[Dimension(name="category")],
            metrics=[Metric(name="quota", agg="sum", sql="quota")],
        )
    )
    layer.graph.models["events"].relationships = [
        Relationship(name="groups", type="many_to_one", foreign_key="category", primary_key="category")
    ]
    layer.add_metric(Metric(name="total", type="derived", sql="events.filled + groups.quota"))
    layer.adapter.execute(
        "create table fill_groups(category varchar, quota integer); insert into fill_groups values ('a', 2), ('b', 4), ('c', 6)"
    )
    sql = layer.compile(metrics=["total"], dimensions=["groups.category"], order_by=["groups.category"])
    assert layer.adapter.execute(sql).fetchall() == [("a", 2), ("b", 4), ("c", 6)]


def test_policy_excluded_groups_are_not_created_by_fill(layer):
    layer.graph.models["events"].security = SecurityPolicy(row_filters=["category = {{ user.category }}"])
    sql = layer.compile(
        metrics=["events.filled"], dimensions=["events.category"], user_attributes={"category": "missing"}
    )
    assert layer.adapter.execute(sql).fetchall() == []
