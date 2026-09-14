"""Special aggregate routes preserve Explore populations and computed join keys."""

from datetime import date

import pytest

from sidemantic import Dimension, Explore, Metric, Model, Relationship, SecurityPolicy, SemanticLayer


@pytest.fixture(params=["python", "rust"])
def layer(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs", reason="Population parity requires the real Rust extension")
    layer = SemanticLayer(engine=request.param, fallback=False, auto_register=False)
    layer.add_model(
        Model(
            name="accounts",
            table="accounts",
            primary_key="key",
            dimensions=[
                Dimension(name="key", type="numeric", sql="tenant * 100 + id"),
                Dimension(name="tier", type="categorical"),
            ],
            metrics=[Metric(name="quota", agg="sum", sql="quota")],
            relationships=[Relationship(name="events", type="one_to_many", foreign_key="account_key")],
            security=SecurityPolicy(row_filters=["tenant = {{ user.tenant }}"]),
            invariant_filters=["active"],
        )
    )
    layer.add_model(
        Model(
            name="events",
            table="events",
            primary_key="id",
            dimensions=[
                Dimension(name="account_key", type="numeric", sql="tenant * 100 + account_id"),
                Dimension(name="day", type="time", granularity="day"),
            ],
            metrics=[
                Metric(name="revenue", agg="sum", sql="amount"),
                Metric(name="running", type="cumulative", sql="events.revenue"),
                Metric(
                    name="closing",
                    agg="sum",
                    sql="amount",
                    non_additive_dimension="day",
                    non_additive_window_groupings=["account_key"],
                ),
            ],
        )
    )
    layer.graph.add_explore(Explore(name="sales", model="accounts"))
    layer.adapter.execute("""
        create table accounts(id integer, tenant integer, key integer, tier varchar, quota integer, active boolean);
        insert into accounts values (1,1,101,'a',10,true),(2,1,102,'b',20,true),(3,1,103,'empty',30,true),
            (4,1,104,'inactive',40,false),(1,2,201,'hidden',50,true);
        create table events(id integer, tenant integer, account_id integer, account_key integer, day date, amount integer);
        insert into events values (1,1,1,101,'2024-01-01',3),(2,1,1,101,'2024-01-02',4),
            (3,1,2,102,'2024-01-02',8),(4,1,4,104,'2024-01-03',100),
            (5,2,1,201,'2024-01-03',200),(6,1,99,199,'2024-01-03',1000);
    """)
    try:
        yield layer
    finally:
        layer.adapter.close()


def rows(layer, **query):
    sql = layer.compile(explore="sales", user_attributes={"tenant": 1}, use_preaggregations=False, **query)
    assert layer.last_engine_selection == {"engine": layer.engine, "reason": None}
    return layer.adapter.execute(sql).fetchall()


def test_independent_computed_populations_include_empty_anchor(layer):
    assert set(rows(layer, metrics=["quota", "events.revenue"], dimensions=["tier"])) == {
        ("a", 10, 7),
        ("b", 20, 8),
        ("empty", 30, None),
    }


def test_cumulative_computed_population_excludes_orphans_and_hidden_rows(layer):
    result = rows(layer, metrics=["events.running"], dimensions=["events.day"], order_by=["events.day"])
    assert result == [(date(2024, 1, 1), 3, 3), (date(2024, 1, 2), 12, 15), (None, None, 15)]


def test_snapshot_computed_population_and_null_unmatched_group(layer):
    assert set(rows(layer, metrics=["events.closing"], dimensions=["tier"])) == {
        ("a", 4),
        ("b", 8),
        ("empty", None),
    }


@pytest.mark.parametrize("layer", ["rust"], indirect=True)
def test_special_routes_evaluate_computed_keys_instead_of_shadow_columns(layer):
    # Python currently projects relationship keys as physical columns. The
    # shared fixtures above qualify population parity; this Rust contract also
    # proves that computed keys survive special-route recursion.
    layer.adapter.execute("update accounts set key = 999; update events set account_key = 999")
    test_independent_computed_populations_include_empty_anchor(layer)
    test_cumulative_computed_population_excludes_orphans_and_hidden_rows(layer)
    test_snapshot_computed_population_and_null_unmatched_group(layer)
