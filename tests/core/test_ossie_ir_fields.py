"""Additive core IR fields used by loss-aware interchange lowering."""

import pytest

from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import AmbiguousJoinPathError, JoinPath, SemanticGraph


def _graph_with_models(*models: Model) -> SemanticGraph:
    graph = SemanticGraph()
    for model in models:
        graph.add_model(model)
    return graph


def test_dimension_preserves_logical_type_and_declared_temporal_role():
    dimension = Dimension(
        name="recorded_at",
        type="categorical",
        logical_data_type="DateTimeTz",
        declared_is_time=False,
    )

    assert dimension.type == "categorical"
    assert dimension.logical_data_type == "DateTimeTz"
    assert dimension.declared_is_time is False


def test_dimension_native_defaults_remain_unchanged():
    dimension = Dimension(name="created_at", type="time", granularity="day")

    assert dimension.type == "time"
    assert dimension.logical_data_type is None
    assert dimension.declared_is_time is None


def test_metric_preserves_logical_result_type():
    metric = Metric(name="revenue", agg="sum", sql="amount", logical_data_type="Decimal")

    assert metric.logical_data_type == "Decimal"
    assert metric.agg == "sum"
    assert metric.sql == "amount"


def test_interchange_fields_do_not_leak_into_existing_model_dumps():
    dimension = Dimension(
        name="recorded_at",
        type="categorical",
        logical_data_type="DateTimeTz",
        declared_is_time=False,
    )
    metric = Metric(name="revenue", agg="sum", sql="amount", logical_data_type="Decimal")
    relationship = Relationship(
        name="customers",
        type="many_to_one",
        foreign_key="customer_id",
        edge_id="orders_customer",
    )
    model = Model(
        name="orders",
        table="orders",
        dimensions=[dimension],
        metrics=[metric],
        relationships=[relationship],
    )

    assert "logical_data_type" not in dimension.model_dump()
    assert "declared_is_time" not in dimension.model_dump()
    assert "logical_data_type" not in metric.model_dump()
    assert "edge_id" not in relationship.model_dump()

    model_dump = model.model_dump(exclude_none=True)
    assert "logical_data_type" not in model_dump["dimensions"][0]
    assert "declared_is_time" not in model_dump["dimensions"][0]
    assert "logical_data_type" not in model_dump["metrics"][0]
    assert "edge_id" not in model_dump["relationships"][0]


def test_relationship_edge_identity_does_not_redefine_target_name():
    relationship = Relationship(
        name="customers",
        type="many_to_one",
        foreign_key="customer_id",
        edge_id="orders_customer",
    )

    assert relationship.name == "customers"
    assert relationship.edge_id == "orders_customer"


def test_join_path_edge_identity_is_optional_for_backwards_compatibility():
    path = JoinPath(
        from_model="orders",
        to_model="customers",
        from_columns=["customer_id"],
        to_columns=["id"],
        relationship="many_to_one",
    )

    assert path.edge_id is None


@pytest.mark.parametrize(
    ("relationship", "source_primary_key", "target_primary_key"),
    [
        (
            Relationship(
                name="target",
                type="many_to_one",
                foreign_key="target_id",
                edge_id="many_to_one_edge",
            ),
            "source_id",
            "target_id",
        ),
        (
            Relationship(
                name="target",
                type="one_to_many",
                foreign_key="source_id",
                edge_id="one_to_many_edge",
            ),
            "source_id",
            "target_id",
        ),
        (
            Relationship(
                name="target",
                type="one_to_one",
                foreign_key="source_id",
                edge_id="one_to_one_edge",
            ),
            "source_id",
            "target_id",
        ),
        (
            Relationship(
                name="target",
                type="many_to_one",
                sql="{from}.target_code = {to}.target_code",
                edge_id="custom_condition_edge",
            ),
            None,
            None,
        ),
        (
            Relationship(name="target", type="cross", edge_id="cross_edge"),
            None,
            None,
        ),
    ],
)
@pytest.mark.parametrize("use_role", [False, True])
def test_edge_identity_survives_direct_and_reverse_graph_edges(
    relationship: Relationship,
    source_primary_key: str | None,
    target_primary_key: str | None,
    use_role: bool,
):
    relationship = relationship.model_copy(deep=True)
    target_instance = "target"
    if use_role:
        relationship.target_model = "target"
        relationship.name = target_instance = "target_role"
    graph = _graph_with_models(
        Model(
            name="source",
            table="source",
            primary_key=source_primary_key,
            relationships=[relationship],
        ),
        Model(name="target", table="target", primary_key=target_primary_key),
    )

    forward = graph.find_relationship_path("source", target_instance)
    reverse = graph.find_relationship_path(target_instance, "source")

    assert [hop.edge_id for hop in forward] == [relationship.edge_id]
    assert [hop.edge_id for hop in reverse] == [relationship.edge_id]
    assert forward[0].to_target_model == "target"
    assert graph.instance_has_keyed_relationship("source", {target_instance}) is (relationship.type != "cross")


def test_nested_role_paths_preserve_declared_edge_identities():
    graph = _graph_with_models(
        Model(
            name="orders",
            table="orders",
            relationships=[
                Relationship(
                    name="billing_customer",
                    target_model="customers",
                    type="many_to_one",
                    foreign_key="customer_id",
                    edge_id="billing_edge",
                )
            ],
        ),
        Model(
            name="customers",
            table="customers",
            primary_key="id",
            relationships=[
                Relationship(
                    name="countries",
                    type="many_to_one",
                    foreign_key="country_id",
                    edge_id="country_edge",
                )
            ],
        ),
        Model(name="countries", table="countries", primary_key="id"),
    )

    path = graph.find_relationship_path("orders", "billing_customer$countries")

    assert [(hop.to_instance, hop.to_target_model, hop.edge_id) for hop in path] == [
        ("billing_customer", "customers", "billing_edge"),
        ("billing_customer$countries", "countries", "country_edge"),
    ]
    assert [hop.edge_id for hop in graph.find_relationship_path("billing_customer$countries", "orders")] == [
        "country_edge",
        "billing_edge",
    ]


def test_parallel_named_edges_do_not_collapse_into_one_path():
    graph = _graph_with_models(
        Model(
            name="orders",
            table="orders",
            relationships=[
                Relationship(
                    name="customers",
                    type="many_to_one",
                    foreign_key="customer_id",
                    edge_id="billing_customer",
                ),
                Relationship(
                    name="customers",
                    type="many_to_one",
                    foreign_key="customer_id",
                    edge_id="shipping_customer",
                ),
            ],
        ),
        Model(name="customers", table="customers", primary_key="id"),
    )

    with pytest.raises(AmbiguousJoinPathError) as exc_info:
        graph.find_relationship_path("orders", "customers")

    assert "billing_customer" in str(exc_info.value)
    assert "shipping_customer" in str(exc_info.value)


def test_unidentified_duplicate_edges_keep_existing_deduplication_behavior():
    graph = _graph_with_models(
        Model(
            name="orders",
            table="orders",
            relationships=[
                Relationship(name="customers", type="many_to_one", foreign_key="customer_id"),
                Relationship(name="customers", type="many_to_one", foreign_key="customer_id"),
            ],
        ),
        Model(name="customers", table="customers", primary_key="id"),
    )

    path = graph.find_relationship_path("orders", "customers")

    assert len(path) == 1
    assert path[0].edge_id is None


def test_edge_identity_survives_direct_many_to_many_graph_edges():
    graph = _graph_with_models(
        Model(
            name="orders",
            table="orders",
            primary_key="order_id",
            relationships=[
                Relationship(
                    name="products",
                    type="many_to_many",
                    foreign_key="product_id",
                    primary_key="product_id",
                    edge_id="orders_products",
                )
            ],
        ),
        Model(name="products", table="products", primary_key="product_id"),
    )

    path = graph.find_relationship_path("orders", "products")

    assert [hop.edge_id for hop in path] == ["orders_products"]


def test_edge_identity_survives_junction_many_to_many_graph_edges():
    graph = _graph_with_models(
        Model(
            name="orders",
            table="orders",
            primary_key="order_id",
            relationships=[
                Relationship(
                    name="products",
                    type="many_to_many",
                    through="order_items",
                    through_foreign_key="order_id",
                    related_foreign_key="product_id",
                    edge_id="orders_products",
                )
            ],
        ),
        Model(name="products", table="products", primary_key="product_id"),
        Model(name="order_items", table="order_items", primary_key="id"),
    )

    path = graph.find_relationship_path("orders", "products")

    assert [(hop.from_model, hop.to_model, hop.edge_id) for hop in path] == [
        ("orders", "order_items", "orders_products"),
        ("order_items", "products", "orders_products"),
    ]
