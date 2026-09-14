"""Named catalogs remain inert until a caller selects their execution contract."""

import json

import pytest

from sidemantic import Dimension, Explore, Metric, Model, SavedQuery, SecurityPolicy, SemanticLayer
from sidemantic.core.semantic_layer import SecurityError
from sidemantic.core.table_calculation import TableCalculation
from sidemantic.rust_bridge import compile_semantic_input, rewrite_semantic_input
from sidemantic.semantic_handoff import UnsupportedSemanticFeaturesError, graph_to_semantic_input


@pytest.fixture(params=["python", "rust"])
def layer(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs", reason="Catalog acceptance requires the real extension")
    layer = SemanticLayer(engine=request.param, fallback=False, auto_register=False, enforce_visibility=True)
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[Dimension(name="status", type="categorical"), Dimension(name="amount", type="numeric")],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
            security=SecurityPolicy(row_filters=["tenant = {{ user.tenant }}"]),
            invariant_filters=["not deleted"],
        )
    )
    layer.graph.add_explore(
        Explore(
            name="large_orders",
            model="orders",
            default_metrics=["revenue"],
            allowed_metrics=["revenue"],
            filters=["amount > 15"],
            max_limit=10,
        )
    )
    layer.graph.add_saved_query(
        SavedQuery(name="paid", metrics=["orders.revenue"], filters=["orders.status = 'paid'"], limit=1)
    )
    layer.graph.add_saved_query(SavedQuery(name="large", explore="large_orders", metrics=["revenue"]))
    layer.graph.add_table_calculation(TableCalculation(name="doubled", type="formula", expression="${revenue} * 2"))
    layer.adapter.execute(
        "create table orders(id integer, amount integer, status varchar, tenant varchar, deleted boolean)"
    )
    layer.adapter.execute(
        "insert into orders values (1,10,'paid','a',false), (2,20,'paid','a',false), "
        "(3,50,'paid','a',true), (4,100,'paid','b',false), (5,7,'pending','a',false)"
    )
    try:
        yield layer
    finally:
        layer.adapter.close()


def rows(layer, **query):
    sql = layer.compile(user_attributes={"tenant": "a"}, **query)
    assert layer.last_engine_selection["engine"] == layer.engine
    return layer.adapter.execute(sql).fetchall()


def test_unused_catalogs_do_not_block_strict_query_or_mutate_source(layer):
    before = graph_to_semantic_input(layer.graph)
    assert rows(layer, metrics=["orders.revenue"]) == [(37,)]
    assert graph_to_semantic_input(layer.graph) == before
    assert all(before[name] for name in ("table_calculations", "explores", "saved_queries"))


def test_resolved_saved_query_keeps_filters_and_immutability(layer):
    before = graph_to_semantic_input(layer.graph)
    assert rows(layer, saved_query="paid") == [(30,)]
    with pytest.raises(ValueError, match="immutable"):
        rows(layer, saved_query="paid", filters=[])
    assert graph_to_semantic_input(layer.graph) == before


def test_catalogs_do_not_disable_mandatory_policy(layer):
    with pytest.raises(SecurityError):
        layer.compile(metrics=["orders.revenue"])


def test_saved_query_visibility_is_checked_before_dispatch(layer):
    layer.graph.saved_queries["paid"].visibility = "private"
    with pytest.raises(ValueError, match="not public"):
        rows(layer, saved_query="paid")


def test_invalid_active_saved_query_is_not_treated_as_unused(layer):
    layer.graph.saved_queries["paid"].metrics = ["orders.missing"]
    with pytest.raises(ValueError):
        rows(layer, saved_query="paid")


@pytest.mark.parametrize("query", [{"explore": "large_orders"}, {"saved_query": "large"}])
def test_active_explore_is_enforced(layer, query):
    assert rows(layer, **query) == [(20,)]


@pytest.mark.parametrize("layer", ["rust"], indirect=True)
def test_rewrite_preserves_policies_with_unused_catalogs(layer):
    before = graph_to_semantic_input(layer.graph)
    sql = rewrite_semantic_input(
        layer.graph, "select orders.revenue from metrics", user_attributes={"tenant": "a"}, enforce_visibility=True
    )
    assert layer.adapter.execute(sql).fetchall() == [(37,)]
    assert graph_to_semantic_input(layer.graph) == before


@pytest.mark.parametrize("layer", ["rust"], indirect=True)
@pytest.mark.parametrize(
    "field,value", [("explore", "large_orders"), ("saved_query", "paid"), ("table_calculations", ["doubled"])]
)
def test_raw_active_catalog_requests_are_never_ignored(layer, field, value):
    with pytest.raises(UnsupportedSemanticFeaturesError, match=f"query.{field}"):
        compile_semantic_input(
            layer.graph, {"metrics": ["orders.revenue"], "user_attributes": {"tenant": "a"}, field: value}
        )
    runtime = pytest.importorskip("sidemantic_rs")
    with pytest.raises(ValueError, match=f"rewrite.context.{field}"):
        runtime.rewrite_with_semantic_input_context(
            json.dumps(graph_to_semantic_input(layer.graph)),
            "select orders.revenue from metrics",
            json.dumps({"user_attributes": {"tenant": "a"}, field: value}),
        )


@pytest.mark.parametrize("layer", ["rust"], indirect=True)
@pytest.mark.parametrize("definitions", [[42], [{}], [{"name": ""}], [{"name": "same"}, {"name": "same"}]])
def test_invalid_catalog_identity_is_rejected(layer, definitions):
    runtime = pytest.importorskip("sidemantic_rs")
    source = graph_to_semantic_input(layer.graph)
    source["explores"] = definitions
    with pytest.raises(ValueError):
        runtime.compile_with_semantic_input(
            json.dumps(source), json.dumps({"metrics": ["orders.revenue"], "user_attributes": {"tenant": "a"}})
        )
