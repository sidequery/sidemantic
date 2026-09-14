"""Executed rewrite contracts independent of CTE projection and aggregate spelling."""

import pytest

from sidemantic import Dimension, Metric, Model, Relationship, SemanticLayer


@pytest.fixture(params=["python", "rust"])
def layer(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs")
    layer = SemanticLayer(engine=request.param, fallback=False, auto_register=False)
    try:
        yield layer
    finally:
        layer.adapter.close()


@pytest.mark.parametrize(
    "predicate,expected", [("", 3), (" where orders.status = 'done'", 2), (" where orders.status = 'missing'", 0)]
)
def test_count_without_sql_counts_rows_including_null_payloads(layer, predicate, expected):
    layer.add_model(
        Model(
            name="orders",
            table="rewrite_count_orders",
            primary_key="order_id",
            dimensions=[Dimension(name="status", type="categorical")],
            metrics=[Metric(name="order_count", agg="count", sql=None)],
        )
    )
    # No physical order_count column exists. Counting any nullable payload would
    # drop rows; COUNT(*) and a projected non-null sentinel must both count all.
    layer.adapter.execute("""
        create table rewrite_count_orders(order_id integer, status varchar, amount integer);
        insert into rewrite_count_orders values (1, 'done', null), (2, 'done', 10), (3, null, null);
    """)
    assert layer.sql(f"select orders.order_count from orders{predicate}").fetchall() == [(expected,)]


@pytest.fixture
def composite_items(layer):
    layer.add_model(
        Model(
            name="order_items",
            table="rewrite_key_items",
            primary_key=["order_id", "item_id"],
            dimensions=[Dimension(name="sku", type="categorical")],
            metrics=[Metric(name="item_revenue", agg="sum", sql="amount")],
        )
    )
    # Neither individual key component is unique. Repeated SKUs also ensure
    # aggregation happens at the selected dimension grain, not the source key.
    layer.adapter.execute("""
        create table rewrite_key_items(order_id integer, item_id integer, sku varchar, amount integer);
        insert into rewrite_key_items values (1, 1, 'a', 10), (1, 2, 'a', 20), (2, 1, 'b', 40), (2, 2, 'c', 80);
    """)
    return layer


def test_composite_primary_keys_preserve_selected_aggregate_grain(composite_items):
    assert composite_items.sql(
        "select order_items.sku, order_items.item_revenue from order_items order by order_items.sku"
    ).fetchall() == [("a", 30), ("b", 40), ("c", 80)]


def test_composite_relationship_rewrite_uses_both_key_components(composite_items):
    composite_items.add_model(
        Model(
            name="shipments",
            table="rewrite_key_shipments",
            primary_key="shipment_id",
            metrics=[Metric(name="shipment_count", agg="count")],
            relationships=[
                Relationship(
                    name="order_items",
                    type="many_to_one",
                    foreign_key=["order_id", "item_id"],
                    primary_key=["order_id", "item_id"],
                )
            ],
        )
    )
    composite_items.adapter.execute("""
        create table rewrite_key_shipments(shipment_id integer, order_id integer, item_id integer);
        insert into rewrite_key_shipments values (1, 1, 2), (2, 1, 2), (3, 2, 1), (4, 2, 2);
    """)
    assert composite_items.sql(
        "select order_items.sku, shipments.shipment_count from shipments order by order_items.sku"
    ).fetchall() == [("a", 2), ("b", 1), ("c", 1)]
