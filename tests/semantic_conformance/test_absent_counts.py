"""Empty source populations retain COUNT zero before defaults and calculations."""

import pytest

from sidemantic import Dimension, Metric, Model, Relationship, SecurityPolicy, SemanticLayer


@pytest.fixture(params=["python", "rust"])
def layer(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs", reason="Absent count acceptance requires the installed Rust extension")
    layer = SemanticLayer(engine=request.param, fallback=False, auto_register=False)
    layer.add_model(
        Model(
            name="orders",
            table="count_orders",
            primary_key="id",
            dimensions=[Dimension(name="region", type="categorical")],
            metrics=[
                Metric(name="rows", agg="count"),
                Metric(name="people", agg="count_distinct", sql="customer_id"),
                Metric(name="defaulted", agg="count", fill_nulls_with=99),
                Metric(name="amount", agg="sum", sql="amount"),
                Metric(name="minimum", agg="min", sql="amount"),
            ],
            relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
        )
    )
    layer.add_model(
        Model(
            name="customers",
            table="count_customers",
            primary_key="id",
            dimensions=[Dimension(name="region", type="categorical")],
            metrics=[Metric(name="customers", agg="count")],
        )
    )
    for metric in [
        Metric(name="combined", type="derived", sql="orders.rows + customers.customers"),
        Metric(
            name="fraction",
            type="ratio",
            numerator="orders.rows",
            denominator="customers.customers",
            fill_nulls_with=-1,
        ),
        Metric(
            name="reverse_fraction",
            type="ratio",
            numerator="customers.customers",
            denominator="orders.rows",
            fill_nulls_with=-2,
        ),
    ]:
        layer.add_metric(metric)
    layer.adapter.execute("""
        create table count_customers(id integer, region varchar);
        insert into count_customers values (1, 'matched'), (2, 'empty');
        create table count_orders(id integer, customer_id integer, region varchar, amount integer);
        insert into count_orders values (1, 1, 'matched', 10), (2, 1, 'matched', 20), (3, 9, 'orphan', 30);
    """)
    try:
        yield layer
    finally:
        layer.adapter.close()


@pytest.mark.parametrize("reverse", [False, True])
def test_absent_counts_are_zero_and_other_aggregates_remain_null(layer, reverse):
    metrics = [
        "orders.rows",
        "orders.people",
        "orders.defaulted",
        "orders.amount",
        "orders.minimum",
        "customers.customers",
    ]
    if reverse:
        metrics.reverse()
    cursor = layer.adapter.execute(
        layer.compile(
            metrics=metrics,
            dimensions=["customers.region"],
            order_by=["customers.region"],
            filters=["customers.region IS NOT NULL"],
        )
    )
    assert [column[0] for column in cursor.description] == ["region"] + [metric.split(".")[1] for metric in metrics]
    rows = cursor.fetchall()
    expected = [("empty", 0, 0, 0, None, None, 1), ("matched", 2, 1, 2, 30, 10, 1)]
    if reverse:
        expected = [(row[0], *reversed(row[1:])) for row in expected]
    assert rows == expected


def test_count_zero_flows_into_derived_and_ratio_before_outer_defaults(layer):
    cursor = layer.adapter.execute(
        layer.compile(
            metrics=["combined", "fraction", "reverse_fraction"],
            dimensions=["customers.region"],
            order_by=["customers.region"],
            filters=["customers.region IS NOT NULL"],
        )
    )
    assert cursor.fetchall() == [("empty", 1, 0, -2), ("matched", 3, 2, 0.5)]


def test_source_populations_preserve_orphans_with_zero_customer_count(layer):
    cursor = layer.adapter.execute(
        layer.compile(
            metrics=["orders.rows", "orders.people", "customers.customers", "combined", "fraction", "reverse_fraction"],
            dimensions=["customers.region"],
            order_by=["customers.region"],
        )
    )
    assert cursor.fetchall() == [
        ("empty", 0, 0, 1, 1, 0, -2),
        ("matched", 2, 1, 1, 3, 2, 0.5),
        (None, 1, 1, 0, 1, -1, 0),
    ]


def test_policy_does_not_restore_unauthorized_groups(layer):
    layer.graph.models["customers"].security = SecurityPolicy(row_filters=["region = {{ user.region }}"])
    cursor = layer.adapter.execute(
        layer.compile(
            metrics=["orders.rows", "customers.customers"],
            dimensions=["customers.region"],
            user_attributes={"region": "empty"},
        )
    )
    assert cursor.fetchall() == [("empty", 0, 1)]


def test_three_source_groups_merge_when_the_first_source_has_no_row(layer):
    layer.add_model(
        Model(
            name="refunds",
            table="count_refunds",
            primary_key="id",
            metrics=[Metric(name="refunds", agg="count")],
            relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
        )
    )
    layer.adapter.execute(
        "create table count_refunds(id integer, customer_id integer); insert into count_refunds values (1, 2)"
    )
    cursor = layer.adapter.execute(
        layer.compile(
            metrics=["orders.rows", "customers.customers", "refunds.refunds"],
            dimensions=["customers.region"],
            order_by=["customers.region"],
        )
    )
    # The empty group exists in the second and third sources, but not the first.
    # Joining every later source only against the first would split it into two rows.
    assert cursor.fetchall() == [("empty", 0, 1, 1), ("matched", 2, 1, 0), (None, 1, 0, 0)]


def test_absent_count_zero_is_used_by_aggregate_filter_and_order(layer):
    cursor = layer.adapter.execute(
        layer.compile(
            metrics=["orders.rows", "customers.customers"],
            dimensions=["customers.region"],
            filters=["orders.rows = 0"],
            order_by=["orders.rows", "customers.region"],
        )
    )
    assert cursor.fetchall() == [("empty", 0, 1)]
