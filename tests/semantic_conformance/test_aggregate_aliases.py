"""Independent result contracts for cross-grain child output normalization."""

import pytest

from sidemantic import Dimension, Metric, Model, Relationship, SemanticLayer
from sidemantic.semantic_handoff import UnsupportedSemanticFeaturesError


@pytest.fixture(params=["python", "rust"])
def layer(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs", reason="Alias acceptance requires the real Rust extension")
    layer = SemanticLayer(engine=request.param, auto_register=False)
    layer.add_model(
        Model(
            name="customers",
            table="alias_customers",
            primary_key="id",
            dimensions=[
                Dimension(name="region", type="categorical"),
                Dimension(name="revenue", type="categorical", sql="category"),
            ],
            metrics=[Metric(name="quota", agg="sum", sql="quota"), Metric(name="customer_count", agg="count")],
        )
    )
    layer.add_model(
        Model(
            name="orders",
            table="alias_orders",
            primary_key="id",
            dimensions=[Dimension(name="region", type="categorical")],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
            relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
        )
    )
    layer.add_metric(
        Metric(name="revenue_per_quota", type="ratio", numerator="orders.revenue", denominator="customers.quota")
    )
    layer.adapter.execute("""
        create table alias_customers(id integer, region varchar, category varchar, quota integer);
        insert into alias_customers values
            (1, 'north', 'retail', 2), (2, 'south', 'business', 4),
            (3, null, 'unknown', 5), (4, 'west', 'empty', 7);
        create table alias_orders(id integer, customer_id integer, region varchar, amount integer);
        insert into alias_orders values (10, 1, 'east', 10), (11, 1, 'east', 20), (12, 2, 'west', 40);
    """)
    try:
        yield layer
    finally:
        layer.adapter.close()


def assert_result(layer, query, columns, expected):
    sql = layer.compile(**query)
    assert layer.last_engine_selection["engine"] == layer.engine
    result = layer.adapter.execute(sql)
    assert [column[0] for column in result.description] == columns
    assert result.fetchall() == expected


def test_dimension_and_metric_names_from_different_owners(layer):
    # Customer quota is counted once despite customer 1 having two orders.
    assert_result(
        layer,
        {
            "metrics": ["orders.revenue", "customers.quota"],
            "dimensions": ["customers.revenue"],
            "order_by": ["customers.revenue"],
        },
        ["customers_revenue", "orders_revenue", "quota"],
        [("business", 40, 4), ("empty", None, 7), ("retail", 30, 2), ("unknown", None, 5)],
    )


def test_hidden_leaf_collision_does_not_rename_public_dimension(layer):
    assert_result(
        layer,
        {
            "metrics": ["revenue_per_quota"],
            "dimensions": ["customers.revenue"],
            "order_by": ["customers.revenue"],
        },
        ["revenue", "revenue_per_quota"],
        [("business", 10.0), ("empty", None), ("retail", 15.0), ("unknown", None)],
    )


@pytest.mark.parametrize("reverse_dimensions", [False, True])
@pytest.mark.parametrize("reverse_metrics", [False, True])
def test_two_grouping_dimensions_share_basename(layer, reverse_dimensions, reverse_metrics):
    dimensions = ["orders.region", "customers.region"]
    metrics = ["orders.revenue", "customers.quota"]
    columns = ["orders_region", "customers_region", "revenue", "quota"]
    expected = [("east", "north", 30, 2), ("west", "south", 40, 4), (None, "west", None, 7), (None, None, None, 5)]
    if reverse_dimensions:
        dimensions.reverse()
        columns[:2] = reversed(columns[:2])
        expected = [(customer, order, revenue, quota) for order, customer, revenue, quota in expected]
    if reverse_metrics:
        metrics.reverse()
        columns[2:] = reversed(columns[2:])
        expected = [(first, second, quota, revenue) for first, second, revenue, quota in expected]
    assert_result(
        layer,
        {
            "metrics": metrics,
            "dimensions": dimensions,
            "order_by": ["orders.region", "customers.region"],
        },
        columns,
        expected,
    )


def test_qualified_metric_filter_and_order_bind_selected_outputs(layer):
    assert_result(
        layer,
        {
            "metrics": ["orders.revenue", "customers.quota"],
            "dimensions": ["customers.revenue"],
            "filters": ["orders.revenue > 25", "customers.quota < 5"],
            "order_by": ["orders.revenue DESC"],
        },
        ["customers_revenue", "orders_revenue", "quota"],
        [("business", 40, 4), ("retail", 30, 2)],
    )


def test_unselected_metric_filter_uses_normalized_child_output(layer):
    assert_result(
        layer,
        {
            "metrics": ["revenue_per_quota"],
            "dimensions": ["customers.revenue"],
            "filters": ["orders.revenue > 35"],
            "order_by": ["customers.revenue"],
        },
        ["revenue", "revenue_per_quota"],
        [("business", 10.0)],
    )


def test_same_owner_duplicate_child_output_remains_explicitly_unsupported():
    pytest.importorskip("sidemantic_rs", reason="Alias rejection requires the real Rust extension")
    layer = SemanticLayer(engine="rust", auto_register=False)
    try:
        layer.add_model(
            Model(
                name="orders",
                table="orders",
                primary_key="id",
                dimensions=[Dimension(name="revenue", type="numeric")],
                metrics=[Metric(name="revenue", agg="sum", sql="amount")],
                relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
            )
        )
        layer.add_model(
            Model(
                name="customers",
                table="customers",
                primary_key="id",
                metrics=[Metric(name="quota", agg="sum", sql="quota")],
            )
        )
        with pytest.raises(UnsupportedSemanticFeaturesError) as caught:
            layer.compile(metrics=["orders.revenue", "customers.quota"], dimensions=["orders.revenue"])
        assert "aggregation.child_output_alias_collision" in caught.value.capabilities
    finally:
        layer.adapter.close()
