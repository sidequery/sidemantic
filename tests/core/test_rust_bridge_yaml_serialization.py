"""Regression coverage for Python->Rust YAML bridge serialization fidelity."""

import yaml

from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.pre_aggregation import Index, PreAggregation, RefreshKey
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.rust_bridge import find_relationship_path_with_rust, graph_to_rust_yaml, models_to_rust_yaml
from tests.rust_layer_adapter import _dimension_to_rust_dict, _metric_to_rust_dict, _relationship_to_rust_dict


def test_models_to_rust_yaml_preserves_extended_core_metadata():
    model = Model(
        name="orders",
        table="orders",
        primary_key=["order_id", "tenant_id"],
        source_uri="s3://warehouse/orders",
        extends="base_orders",
        unique_keys=[["order_id", "tenant_id"]],
        default_time_dimension="order_date",
        default_grain="day",
        relationships=[
            Relationship(
                name="customers",
                type="many_to_one",
                foreign_key=["customer_id", "tenant_id"],
                primary_key=["customer_id", "tenant_id"],
            ),
            Relationship(
                name="products",
                type="many_to_many",
                through="order_products",
                through_foreign_key_columns=["order_id", "tenant_id"],
                related_foreign_key_columns=["product_id", "tenant_id"],
            ),
        ],
        dimensions=[
            Dimension(
                name="order_date",
                type="time",
                sql="order_date",
                granularity="day",
                supported_granularities=["day", "week", "month"],
                format="yyyy-mm-dd",
                value_format_name="iso_date",
                parent="order_month",
            )
        ],
        metrics=[
            Metric(
                name="revenue",
                agg="sum",
                sql="amount",
                value_format_name="usd",
                drill_fields=["order_id"],
                non_additive_dimension="order_date",
            )
        ],
        pre_aggregations=[
            PreAggregation(
                name="daily_rollup",
                measures=["revenue"],
                dimensions=["status"],
                time_dimension="order_date",
                granularity="day",
                refresh_key=RefreshKey(every="1 hour", incremental=True, update_window="7 day"),
                indexes=[Index(name="idx_status", columns=["status"], type="regular")],
            )
        ],
    )

    payload = yaml.safe_load(models_to_rust_yaml([model], include_extends=True))
    model_payload = payload["models"][0]
    relationship_payload = model_payload["relationships"][0]
    many_to_many_payload = model_payload["relationships"][1]
    dimension_payload = model_payload["dimensions"][0]
    metric_payload = model_payload["metrics"][0]
    preagg_payload = model_payload["pre_aggregations"][0]

    assert model_payload["source_uri"] == "s3://warehouse/orders"
    assert model_payload["extends"] == "base_orders"
    assert model_payload["primary_key_columns"] == ["order_id", "tenant_id"]
    assert model_payload["unique_keys"] == [["order_id", "tenant_id"]]
    assert relationship_payload["foreign_key_columns"] == ["customer_id", "tenant_id"]
    assert relationship_payload["primary_key_columns"] == ["customer_id", "tenant_id"]
    assert many_to_many_payload["through"] == "order_products"
    assert many_to_many_payload["through_foreign_key_columns"] == ["order_id", "tenant_id"]
    assert many_to_many_payload["related_foreign_key_columns"] == ["product_id", "tenant_id"]

    assert dimension_payload["supported_granularities"] == ["day", "week", "month"]
    assert dimension_payload["format"] == "yyyy-mm-dd"
    assert dimension_payload["value_format_name"] == "iso_date"
    assert dimension_payload["parent"] == "order_month"

    assert metric_payload["value_format_name"] == "usd"
    assert metric_payload["drill_fields"] == ["order_id"]
    assert metric_payload["non_additive_dimension"] == "order_date"

    assert preagg_payload["refresh_key"]["every"] == "1 hour"
    assert preagg_payload["refresh_key"]["incremental"] is True
    assert preagg_payload["indexes"] == [{"name": "idx_status", "columns": ["status"], "type": "regular"}]


def test_models_to_rust_yaml_does_not_invent_table_for_source_uri_model():
    model = Model(
        name="events",
        source_uri="s3://warehouse/events.parquet",
        primary_key="event_id",
        metrics=[Metric(name="event_count", agg="count")],
    )

    payload = yaml.safe_load(models_to_rust_yaml([model]))
    model_payload = payload["models"][0]

    assert model_payload["source_uri"] == "s3://warehouse/events.parquet"
    assert model_payload["table"] is None


def test_models_to_rust_yaml_preserves_optional_parity_fields_and_omits_none():
    model = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        dimensions=[
            Dimension(
                name="occurred_at",
                type="categorical",
                logical_data_type="DateTimeTz",
                declared_is_time=False,
            ),
            Dimension(name="status", type="categorical"),
        ],
        metrics=[Metric(name="revenue", agg="sum", sql="amount", logical_data_type="Decimal")],
        relationships=[
            Relationship(
                name="customers",
                edge_id="orders_customer",
                type="many_to_one",
                foreign_key="customer_id",
            )
        ],
    )

    model_payload = yaml.safe_load(models_to_rust_yaml([model]))["models"][0]
    occurred_at, status = model_payload["dimensions"]
    assert occurred_at["logical_data_type"] == "DateTimeTz"
    assert occurred_at["declared_is_time"] is False
    assert "logical_data_type" not in status
    assert "declared_is_time" not in status
    assert model_payload["metrics"][0]["logical_data_type"] == "Decimal"
    assert model_payload["relationships"][0]["edge_id"] == "orders_customer"


def test_pure_rust_adapter_serializers_preserve_false_and_omit_none():
    explicit_dimension = _dimension_to_rust_dict(
        Dimension(
            name="occurred_at",
            type="categorical",
            logical_data_type="DateTimeTz",
            declared_is_time=False,
        )
    )
    omitted_dimension = _dimension_to_rust_dict(Dimension(name="status", type="categorical"))
    metric = _metric_to_rust_dict(Metric(name="revenue", agg="sum", logical_data_type="Decimal"))
    relationship = _relationship_to_rust_dict(
        Relationship(name="customers", edge_id="orders_customer", type="many_to_one")
    )

    assert explicit_dimension["declared_is_time"] is False
    assert explicit_dimension["logical_data_type"] == "DateTimeTz"
    assert "declared_is_time" not in omitted_dimension
    assert "logical_data_type" not in omitted_dimension
    assert metric["logical_data_type"] == "Decimal"
    assert relationship["edge_id"] == "orders_customer"


def test_find_relationship_path_prefers_edge_aware_payload_and_preserves_legacy_fallback(monkeypatch):
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="order_id",
            relationships=[
                Relationship(
                    name="customers",
                    edge_id="orders_customer",
                    type="many_to_one",
                    foreign_key="customer_id",
                )
            ],
        )
    )
    graph.add_model(Model(name="customers", table="customers", primary_key="id"))

    class EdgeAwareRustModule:
        @staticmethod
        def find_relationship_path_payload_with_yaml(_yaml, _from_model, _to_model):
            return (
                '[{"from_model":"orders","to_model":"customers","from_columns":["customer_id"],'
                '"to_columns":["id"],"relationship":"many_to_one","edge_id":"orders_customer"}]'
            )

    monkeypatch.setattr("sidemantic.rust_bridge.get_rust_module", lambda: EdgeAwareRustModule())
    path = find_relationship_path_with_rust(graph, "orders", "customers")
    assert path[0].edge_id == "orders_customer"

    class LegacyRustModule:
        @staticmethod
        def find_relationship_path_with_yaml(_yaml, _from_model, _to_model):
            return [("orders", "customers", ["customer_id"], ["id"], "many_to_one")]

    monkeypatch.setattr("sidemantic.rust_bridge.get_rust_module", lambda: LegacyRustModule())
    legacy_path = find_relationship_path_with_rust(graph, "orders", "customers")
    assert legacy_path[0].edge_id is None


def test_graph_to_rust_yaml_assigns_complex_metrics_by_entity_dimension():
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="events",
            table="events",
            primary_key="event_id",
            dimensions=[
                Dimension(name="user_id", type="categorical"),
                Dimension(name="event_type", type="categorical"),
                Dimension(name="platform", type="categorical"),
            ],
        )
    )
    graph.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="order_id",
            dimensions=[Dimension(name="order_id", type="categorical")],
        )
    )
    graph.add_metric(
        Metric(
            name="signup_conversion",
            type="conversion",
            entity="user_id",
            base_event="event_type = 'signup'",
            conversion_event="event_type = 'purchase'",
            conversion_window="7 days",
        )
    )
    graph.add_metric(
        Metric(
            name="signup_retention",
            type="retention",
            entity="user_id",
            cohort_event="event_type = 'signup'",
        )
    )
    graph.add_metric(
        Metric(
            name="multi_platform_users",
            type="cohort",
            entity="user_id",
            inner_metrics=[{"name": "platform_count", "agg": "count_distinct", "sql": "platform"}],
            having="platform_count >= 2",
            agg="count",
        )
    )

    payload = yaml.safe_load(graph_to_rust_yaml(graph))
    models = {model["name"]: model for model in payload["models"]}
    event_metric_names = {metric["name"] for metric in models["events"]["metrics"]}
    order_metric_names = {metric["name"] for metric in models["orders"].get("metrics", [])}

    assert {"signup_conversion", "signup_retention", "multi_platform_users"} <= event_metric_names
    assert not {"signup_conversion", "signup_retention", "multi_platform_users"} & order_metric_names
    assert payload.get("metrics") in (None, [])
