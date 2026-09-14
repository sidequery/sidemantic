"""Explore population and authoring contracts executed by both compilers."""

import pytest

from sidemantic import (
    Dimension,
    Explore,
    Metric,
    Model,
    Parameter,
    Relationship,
    SavedQuery,
    SecurityPolicy,
    SemanticLayer,
)
from sidemantic.core.semantic_layer import SecurityError
from sidemantic.semantic_handoff import graph_to_semantic_input


@pytest.fixture(params=["python", "rust"])
def layer(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs", reason="Explore acceptance requires the real extension")
    layer = SemanticLayer(engine=request.param, fallback=False, auto_register=False, enforce_visibility=True)
    layer.add_model(
        Model(
            name="accounts",
            table="accounts",
            primary_key="id",
            dimensions=[Dimension(name="tier", type="categorical"), Dimension(name="id", type="numeric")],
            metrics=[Metric(name="count", agg="count"), Metric(name="quota", agg="sum", sql="quota")],
            relationships=[Relationship(name="orders", type="one_to_many", foreign_key="account_id")],
            security=SecurityPolicy(row_filters=["tenant = {{ user.tenant }}"]),
            invariant_filters=["active"],
        )
    )
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[Dimension(name="status", type="categorical")],
            metrics=[
                Metric(name="revenue", agg="sum", sql="amount"),
                Metric(name="count", agg="count"),
                Metric(name="average", type="ratio", numerator="revenue", denominator="count"),
                Metric(name="doubled", type="derived", sql="revenue * 2"),
            ],
        )
    )
    with pytest.warns(DeprecationWarning):
        layer.graph.add_parameter(Parameter(name="minimum", type="number", default_value=0))
    layer.graph.add_explore(
        Explore(
            name="account_sales",
            model="accounts",
            default_metrics=["orders.revenue"],
            allowed_metrics=["orders.revenue", "orders.count", "orders.average", "orders.doubled", "count", "quota"],
            allowed_dimensions=["tier", "orders.status"],
            allowed_filter_fields=["tier"],
            allowed_order_by=["tier", "orders.revenue"],
            filters=["id > {{ minimum }}"],
            default_filters=["tier = 'business'"],
            default_limit=10,
            max_limit=20,
        )
    )
    layer.graph.add_saved_query(
        SavedQuery(
            name="all_sales",
            explore="account_sales",
            metrics=["orders.revenue"],
            parameters={"minimum": 0},
        )
    )
    layer.adapter.execute(
        "create table accounts(id integer, tier varchar, quota integer, tenant varchar, active boolean)"
    )
    layer.adapter.execute(
        "insert into accounts values (1,'business',10,'a',true),(2,'retail',20,'a',true),"
        "(3,'empty',30,'a',true),(4,'hidden',40,'b',true),(5,'inactive',50,'a',false)"
    )
    layer.adapter.execute("create table orders(id integer, account_id integer, status varchar, amount integer)")
    layer.adapter.execute(
        "insert into orders values (1,1,'paid',10),(2,1,'paid',20),(3,2,'pending',40),"
        "(4,4,'hidden',100),(5,5,'inactive',200),(6,99,'orphan',1000)"
    )
    try:
        yield layer
    finally:
        layer.adapter.close()


def rows(layer, **query):
    before = graph_to_semantic_input(layer.graph)
    sql = layer.compile(user_attributes={"tenant": "a"}, **query)
    assert layer.last_engine_selection == {"engine": layer.engine, "reason": None}
    assert graph_to_semantic_input(layer.graph) == before
    return layer.adapter.execute(sql).fetchall()


def test_off_base_metrics_keep_authorized_base_population(layer):
    assert rows(layer, explore="account_sales") == [(30,)]
    assert rows(layer, explore="account_sales", filters=[]) == [(70,)]
    assert rows(layer, saved_query="all_sales") == [(70,)]
    assert rows(layer, explore="account_sales", filters=[], parameters={"minimum": 1}) == [(40,)]


def test_dimensions_only_keep_unmatched_base_and_exclude_orphan_children(layer):
    result = rows(layer, explore="account_sales", metrics=[], dimensions=["orders.status"], filters=[])
    assert set(result) == {("paid",), ("pending",), (None,)}


def test_join_fanout_does_not_repeat_base_measure(layer):
    result = rows(layer, explore="account_sales", metrics=["quota"], dimensions=["orders.status"], filters=[])
    assert set(result) == {("paid", 10), ("pending", 20), (None, 30)}


def test_same_owner_ratio_and_derived_metrics_keep_anchor(layer):
    assert rows(layer, explore="account_sales", metrics=["orders.average", "orders.doubled"], filters=[]) == [
        (pytest.approx(70 / 3), 140)
    ]


def test_policy_is_required_even_without_selected_anchor_fields(layer):
    with pytest.raises(SecurityError):
        layer.compile(explore="account_sales", filters=[])
    layer.graph.models["accounts"].visibility = "private"
    with pytest.raises(SecurityError, match="not public"):
        layer.compile(explore="account_sales", user_attributes={"tenant": "a"})


@pytest.mark.parametrize(
    "query,match",
    [
        ({"metrics": ["missing"]}, "does not allow metric"),
        ({"dimensions": ["id"]}, "does not allow dimension"),
        ({"filters": ["id > 1"]}, "does not allow filter"),
        ({"order_by": ["id"]}, "does not allow ordering"),
        ({"order_by": ["tier"]}, "must be selected"),
        ({"limit": 21}, "exceeds max_limit"),
    ],
)
def test_contract_rejections_precede_dispatch(layer, query, match):
    with pytest.raises(ValueError, match=match):
        rows(layer, explore="account_sales", **query)


def test_order_defaults_and_limit_overrides(layer):
    layer.graph.explores["account_sales"].default_order_by = ["orders.revenue desc"]
    assert rows(layer, explore="account_sales", dimensions=["tier"], filters=[], limit=1) == [("retail", 40)]
    assert rows(
        layer, explore="account_sales", metrics=[], dimensions=["tier"], filters=[], order_by=["tier"], limit=2
    ) == [("business",), ("empty",)]


def test_saved_query_cannot_override_resolved_contract(layer):
    with pytest.raises(ValueError, match="immutable"):
        rows(layer, saved_query="all_sales", filters=[])


def test_unjoinable_explore_base_is_rejected(layer):
    layer.graph.models["accounts"].relationships.clear()
    # Rebuild the graph's cached relationship index after changing the fixture.
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    for model in layer.graph.models.values():
        graph.add_model(model)
    graph.add_explore(Explore(name="unjoinable", model="accounts", default_metrics=["orders.revenue"]))
    layer.graph = graph
    with pytest.raises(ValueError):
        rows(layer, explore="unjoinable")


def test_independent_aggregate_route_never_discards_explore_population(layer):
    assert rows(layer, explore="account_sales", metrics=["quota", "orders.revenue"], filters=[]) == [(60, 70)]
    assert set(
        rows(layer, explore="account_sales", metrics=["quota", "orders.revenue"], dimensions=["tier"], filters=[])
    ) == {("business", 10, 30), ("retail", 20, 40), ("empty", 30, None)}


def test_intermediate_policy_constrains_related_only_metric(layer):
    layer.graph.models["orders"].invariant_filters = ["status = 'paid'"]
    layer.graph.models["orders"].relationships.append(
        Relationship(name="details", type="one_to_many", foreign_key="order_id")
    )
    layer.add_model(
        Model(name="details", table="details", primary_key="id", metrics=[Metric(name="value", agg="sum", sql="value")])
    )
    layer.graph.explores["account_sales"].allowed_metrics.append("details.value")
    layer.adapter.execute("create table details(id integer, order_id integer, value integer)")
    layer.adapter.execute("insert into details values (1,1,3),(2,3,40),(3,4,100),(4,99,1000)")
    assert rows(layer, explore="account_sales", metrics=["details.value"], filters=[]) == [(3,)]
