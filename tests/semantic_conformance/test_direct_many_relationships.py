"""Direct and custom many-to-many contracts already implemented by Python."""

import pytest

from sidemantic import Dimension, Explore, Metric, Model, Relationship, SemanticLayer


@pytest.fixture(params=["python", "rust"])
def layer(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs", reason="Direct relationship parity requires the real extension")
    layer = SemanticLayer(engine=request.param, fallback=False, auto_register=False)
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[Dimension(name="id", type="numeric")],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
            relationships=[Relationship(name="tags", type="many_to_many", foreign_key="code", primary_key="join_code")],
        )
    )
    layer.add_model(
        Model(
            name="tags",
            table="tags",
            primary_key="id",
            dimensions=[Dimension(name="label", type="categorical")],
        )
    )
    layer.graph.add_explore(Explore(name="sales", model="orders"))
    layer.adapter.execute("""
        create table orders(id integer, code varchar, amount integer);
        insert into orders values (1,'a',10),(2,'b',20),(3,'n',30),(4,null,40);
        create table tags(id integer, join_code varchar, label varchar, threshold integer);
        insert into tags values (1,'a','x',5),(2,'a','x',5),(3,'b','y',15),(4,'c','z',50);
    """)
    try:
        yield layer
    finally:
        layer.adapter.close()


def rows(layer, **query):
    sql = layer.compile(explore="sales", metrics=["revenue"], **query)
    assert layer.last_engine_selection == {"engine": layer.engine, "reason": None}
    return layer.adapter.execute(sql).fetchall()


def test_direct_alternate_keys_keep_source_grain_and_unmatched_rows(layer):
    assert set(rows(layer, dimensions=["tags.label"])) == {("x", 10), ("y", 20), (None, 70)}


def test_direct_custom_predicate_overrides_recorded_keys(layer):
    layer.graph.models["orders"].relationships[0].foreign_key = ["code", "id"]
    layer.graph.models["orders"].relationships[0].sql = "{from}.amount >= {to}.threshold"
    assert set(rows(layer, dimensions=["tags.label"])) == {("x", 100), ("y", 90)}


def test_custom_predicate_remains_physical_with_computed_entity_identity(layer):
    layer.graph.models["orders"].dimensions[0].sql = "id + 100"
    layer.graph.models["orders"].relationships[0].sql = "{from}.amount >= {to}.threshold"
    assert set(rows(layer, dimensions=["tags.label"])) == {("x", 100), ("y", 90)}


def test_direct_role_keeps_custom_predicate_and_source_population(layer):
    relationship = layer.graph.models["orders"].relationships[0]
    relationship.name = "labels"
    relationship.target_model = "tags"
    relationship.sql = "{from}.amount >= {to}.threshold"
    assert set(rows(layer, dimensions=["labels.label"])) == {("x", 100), ("y", 90)}


def test_legacy_remote_foreign_key_joins_source_primary_key(layer):
    relationship = layer.graph.models["orders"].relationships[0]
    relationship.primary_key = None
    relationship.foreign_key = "id"
    assert set(rows(layer, dimensions=["tags.label"])) == {("x", 30), ("y", 30), ("z", 40)}


def test_bridge_keys_override_direct_only_custom_sql(layer):
    relationship = layer.graph.models["orders"].relationships[0]
    relationship.through = "links"
    relationship.through_foreign_key = "order_id"
    relationship.related_foreign_key = "tag_id"
    relationship.primary_key = None
    relationship.sql = "{from}.amount >= {to}.threshold"
    layer.add_model(Model(name="links", table="links", primary_key="id"))
    layer.adapter.execute("""
        create table links(id integer, order_id integer, tag_id integer);
        insert into links values (1,1,1),(2,1,2),(3,2,3);
    """)
    assert set(rows(layer, dimensions=["tags.label"])) == {("x", 10), ("y", 20), (None, 70)}
