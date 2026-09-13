"""Physical input populations for checked complete aggregate filter lowering."""

from copy import deepcopy

import pytest

from sidemantic import Dimension, Metric, Model, SemanticLayer
from sidemantic.semantic_handoff import UnsupportedSemanticFeaturesError, graph_to_semantic_input


@pytest.fixture(params=["python", "rust"])
def layer(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs", reason="Complete filter acceptance requires the real Rust extension")
    layer = SemanticLayer(engine=request.param, fallback=False, auto_register=False)
    layer.add_model(
        Model(
            name="orders",
            table="complete_orders",
            primary_key="id",
            dimensions=[
                Dimension(name="amount", type="numeric", sql="amount * 10"),
                Dimension(name="region", type="categorical"),
            ],
            metrics=[
                Metric(name="paid_sum", sql="SUM(amount)", sql_is_complete=True, filters=["status = 'paid'"]),
                Metric(name="paid_count", sql="COUNT(amount)", sql_is_complete=True, filters=["status = 'paid'"]),
                Metric(name="paid_min", sql="MIN(amount)", sql_is_complete=True, filters=["status = 'paid'"]),
                Metric(name="paid_max", sql="MAX(amount)", sql_is_complete=True, filters=["status = 'paid'"]),
                Metric(name="canceled", sql="SUM(amount)", sql_is_complete=True, filters=["status = 'canceled'"]),
                Metric(name="physical_two", sql="SUM(amount)", sql_is_complete=True, filters=["amount = 2"]),
                Metric(name="total", agg="sum", sql="amount"),
            ],
        )
    )
    layer.adapter.execute("""
        create table complete_orders(id integer, amount integer, status varchar, region varchar);
        insert into complete_orders values
            (1, 1, 'paid', 'north'), (2, 2, 'paid', 'north'), (3, null, 'paid', 'north'),
            (4, 9, 'canceled', 'south'), (5, 4, 'orders.vip', 'south'), (6, -3, 'shipped', 'south');
    """)
    try:
        yield layer
    finally:
        layer.adapter.close()


def assert_result(layer, query, columns, expected):
    source = deepcopy(graph_to_semantic_input(layer.graph))
    sql = layer.compile(**query)
    assert graph_to_semantic_input(layer.graph) == source
    result = layer.adapter.execute(sql)
    assert [column[0] for column in result.description] == columns
    assert result.fetchall() == expected


def test_independent_filters_preserve_each_measure_population(layer):
    assert_result(
        layer,
        {
            "metrics": [
                "orders.paid_sum",
                "orders.paid_count",
                "orders.paid_min",
                "orders.paid_max",
                "orders.canceled",
                "orders.total",
            ]
        },
        ["paid_sum", "paid_count", "paid_min", "paid_max", "canceled", "total"],
        [(3, 2, 1, 2, 9, 13)],
    )


def test_complete_filters_use_physical_values_under_semantic_name_collision(layer):
    assert_result(layer, {"metrics": ["orders.physical_two"]}, ["physical_two"], [(2,)])


def test_groups_without_matching_measure_rows_keep_other_measures(layer):
    assert_result(
        layer,
        {
            "metrics": ["orders.paid_sum", "orders.paid_count", "orders.total"],
            "dimensions": ["orders.region"],
            "order_by": ["orders.region"],
        },
        ["region", "paid_sum", "paid_count", "total"],
        [("north", 3, 2, 3), ("south", None, 0, 10)],
    )


def test_invariant_restriction_still_scopes_every_filtered_measure(layer):
    layer.graph.models["orders"].invariant_filters = ["id != 2"]
    assert_result(
        layer,
        {"metrics": ["orders.paid_sum", "orders.paid_count", "orders.total"]},
        ["paid_sum", "paid_count", "total"],
        [(1, 1, 11)],
    )


@pytest.mark.parametrize("layer", ["rust"], indirect=True)
def test_qualified_physical_columns_do_not_rewrite_string_literals(layer):
    layer.graph.models["orders"].metrics.append(
        Metric(name="vip", sql="SUM(orders.amount)", sql_is_complete=True, filters=["orders.status = 'orders.vip'"])
    )
    assert_result(layer, {"metrics": ["orders.vip"]}, ["vip"], [(4,)])


@pytest.mark.parametrize("layer", ["rust"], indirect=True)
def test_filter_conjunction_preserves_disjunction_grouping(layer):
    layer.graph.models["orders"].metrics.append(
        Metric(
            name="positive",
            sql="SUM(amount)",
            sql_is_complete=True,
            filters=["status = 'paid' OR status = 'shipped'", "amount > 1"],
        )
    )
    assert_result(layer, {"metrics": ["orders.positive"]}, ["positive"], [(2,)])


@pytest.mark.parametrize(
    "expression",
    [
        "COUNT(*)",
        "COUNT(1)",
        "SUM(1)",
        "SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END)",
        "SUM(amount) + COUNT(amount)",
        "SUM(amount) OVER ()",
        "SUM((SELECT amount))",
        "SUM(other.amount)",
        "COUNT(DISTINCT amount)",
    ],
)
def test_unproven_complete_filter_shapes_fail_explicitly(expression):
    pytest.importorskip("sidemantic_rs", reason="Complete filter rejection requires the real Rust extension")
    layer = SemanticLayer(engine="rust", fallback=False, auto_register=False)
    try:
        layer.add_model(
            Model(
                name="orders",
                table="orders",
                primary_key="id",
                metrics=[Metric(name="value", sql=expression, sql_is_complete=True, filters=["amount > 0"])],
            )
        )
        with pytest.raises(UnsupportedSemanticFeaturesError) as caught:
            layer.compile(metrics=["orders.value"])
        assert "metric.complete_filters" in caught.value.capabilities
    finally:
        layer.adapter.close()


def test_foreign_filter_population_fails_explicitly():
    pytest.importorskip("sidemantic_rs", reason="Complete filter rejection requires the real Rust extension")
    layer = SemanticLayer(engine="rust", fallback=False, auto_register=False)
    try:
        layer.add_model(
            Model(
                name="orders",
                table="orders",
                primary_key="id",
                metrics=[Metric(name="value", sql="SUM(amount)", sql_is_complete=True, filters=["other.amount > 0"])],
            )
        )
        with pytest.raises(UnsupportedSemanticFeaturesError) as caught:
            layer.compile(metrics=["orders.value"])
        assert "metric.complete_filters" in caught.value.capabilities
    finally:
        layer.adapter.close()
