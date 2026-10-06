"""Measures retain their source identity across sibling one-to-many joins."""

import pytest

from sidemantic import Dimension, Metric, Model, Relationship, SemanticLayer


@pytest.fixture(params=["python", "rust"])
def layer(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs", reason="Sibling fanout acceptance requires the real Rust extension")
    layer = SemanticLayer(engine=request.param, fallback=False, auto_register=False)
    layer.add_model(
        Model(
            name="facts",
            table="sibling_facts",
            primary_key="id",
            metrics=[Metric(name="total", agg="sum", sql="amount")],
            relationships=[
                Relationship(name="items", type="one_to_many", foreign_key="fact_id"),
                Relationship(name="refunds", type="one_to_many", foreign_key="fact_id"),
            ],
        )
    )
    layer.add_model(
        Model(
            name="items",
            table="sibling_items",
            primary_key="id",
            metrics=[
                Metric(name="items_value", agg="sum", sql="value"),
                Metric(name="average", agg="avg", sql="value"),
                Metric(name="values", agg="count", sql="value"),
                Metric(name="rows", agg="count"),
            ],
        )
    )
    layer.add_model(
        Model(
            name="refunds",
            table="sibling_refunds",
            primary_key="id",
            dimensions=[Dimension(name="label", type="categorical")],
            relationships=[Relationship(name="events", type="one_to_many", foreign_key="refund_id")],
        )
    )
    layer.add_model(
        Model(
            name="events",
            table="sibling_events",
            primary_key="id",
            dimensions=[Dimension(name="tag", type="categorical")],
        )
    )
    layer.adapter.execute("""
        create table sibling_facts(id integer, amount integer);
        create table sibling_items(id integer, fact_id integer, value integer);
        create table sibling_refunds(id integer, fact_id integer, label varchar);
        create table sibling_events(id integer, refund_id integer, tag varchar);
    """)
    try:
        yield layer
    finally:
        layer.adapter.close()


def execute(layer, **query):
    sql = layer.compile(**query)
    assert layer.last_engine_selection["engine"] == layer.engine
    return layer.adapter.execute(sql).fetchall()


def test_minimized_generated_sibling_fanout(layer):
    """Seed 20261005, case 8: one item worth 3 was incorrectly summed to 6."""
    layer.adapter.execute("""
        insert into sibling_facts values (1, 1);
        insert into sibling_items values (101, 1, 3);
        insert into sibling_refunds values (9, 1, 'x'), (11, 1, 'x');
    """)
    expected = layer.adapter.execute("""
        select refund_groups.label, sum(items.value)
        from (select distinct fact_id, label from sibling_refunds) as refund_groups
        join sibling_facts as facts on refund_groups.fact_id = facts.id
        join sibling_items as items on facts.id = items.fact_id
        group by refund_groups.label
    """).fetchall()
    assert expected == [("x", 3)]
    assert execute(layer, metrics=["items.items_value"], dimensions=["refunds.label"]) == expected


@pytest.mark.parametrize("include_parent", [False, True])
def test_sibling_groups_preserve_each_item_and_null_population(layer, include_parent):
    layer.adapter.execute("""
        insert into sibling_facts values (1, 10), (2, 20), (3, 30), (4, 40);
        insert into sibling_items values
            (101, 1, 3), (102, 1, 3), (103, 1, null),
            (201, 2, 9), (202, 2, -2), (301, 3, null);
        insert into sibling_refunds values
            (1, 1, 'x'), (2, 1, 'x'), (3, 1, 'y'),
            (4, 2, 'x'), (5, 2, 'x'), (6, 2, 'x'),
            (7, 3, 'z'), (8, 3, 'z'),
            (9, 4, 'empty'), (10, 4, 'empty');
    """)
    # Deduplicate group membership before joining measure rows. Equal values
    # from different item identities still contribute separately.
    expected = layer.adapter.execute("""
        with refund_groups as (
            select distinct fact_id, label from sibling_refunds
        )
        select refund_groups.label, sum(items.value), avg(items.value), count(items.value), count(items.id)
        from refund_groups
        left join sibling_facts as facts on refund_groups.fact_id = facts.id
        left join sibling_items as items on facts.id = items.fact_id
        group by refund_groups.label
        order by refund_groups.label
    """).fetchall()
    assert expected == [
        ("empty", None, None, 0, 0),
        ("x", 13, 3.25, 4, 5),
        ("y", 6, 3.0, 2, 3),
        ("z", None, None, 0, 1),
    ]
    metrics = ["items.items_value", "items.average", "items.values", "items.rows"]
    if include_parent:
        metrics.append("facts.total")
        expected = [(*row, total) for row, total in zip(expected, [40, 30, 10, 30], strict=True)]
    assert execute(layer, metrics=metrics, dimensions=["refunds.label"], order_by=["refunds.label"]) == expected


def test_empty_sibling_population_has_no_groups(layer):
    layer.adapter.execute("""
        insert into sibling_facts values (1, 1);
        insert into sibling_items values (101, 1, 3);
    """)
    assert execute(layer, metrics=["items.items_value", "items.rows"], dimensions=["refunds.label"]) == []


def test_source_identity_survives_three_edge_fanout_path(layer):
    """Items -> facts -> refunds -> events must retain each item's grain."""
    layer.adapter.execute("""
        insert into sibling_facts values (1, 1);
        insert into sibling_items values (101, 1, 3), (102, 1, 7);
        insert into sibling_refunds values (9, 1, 'unused'), (11, 1, 'unused');
        insert into sibling_events values
            (1, 9, 'x'), (2, 9, 'x'), (3, 11, 'x'), (4, 11, 'y');
    """)
    expected = layer.adapter.execute("""
        with fact_tags as (
            select distinct refunds.fact_id, events.tag
            from sibling_refunds as refunds
            join sibling_events as events on refunds.id = events.refund_id
        )
        select fact_tags.tag, sum(items.value), count(items.id)
        from fact_tags
        join sibling_facts as facts on fact_tags.fact_id = facts.id
        join sibling_items as items on facts.id = items.fact_id
        group by fact_tags.tag
        order by fact_tags.tag
    """).fetchall()
    assert expected == [("x", 10, 2), ("y", 10, 2)]
    assert (
        execute(layer, metrics=["items.items_value", "items.rows"], dimensions=["events.tag"], order_by=["events.tag"])
        == expected
    )
