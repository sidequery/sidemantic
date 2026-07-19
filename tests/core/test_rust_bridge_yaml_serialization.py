"""Regression coverage for Python->Rust YAML bridge serialization fidelity."""

import yaml

from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.pre_aggregation import Index, PreAggregation, RefreshKey
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.rust_bridge import _graph_from_loaded_payload, graph_to_rust_yaml, models_to_rust_yaml


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


def test_models_to_rust_yaml_preserves_explicit_keyless_model():
    payload = yaml.safe_load(models_to_rust_yaml([Model(name="events", table="events")]))

    assert payload["models"][0]["primary_key"] is None
    assert payload["models"][0]["primary_key_columns"] == []


def test_rust_payload_restores_explicit_keyless_model_as_none():
    graph = _graph_from_loaded_payload(
        {
            "models": [
                {
                    "name": "events",
                    "table": "events",
                    "primary_key": "",
                    "primary_key_columns": [],
                }
            ]
        }
    )

    assert graph.get_model("events").primary_key is None


def test_models_to_rust_yaml_uses_source_key_for_reverse_relationship():
    users = Model(
        name="users",
        table="users",
        primary_key="user_id",
        relationships=[Relationship(name="profiles", type="one_to_many", foreign_key="user_id")],
    )
    profiles = Model(name="profiles", table="profiles", primary_key="profile_id")

    payload = yaml.safe_load(models_to_rust_yaml([users, profiles]))
    relationship = payload["models"][0]["relationships"][0]

    assert relationship["primary_key"] == "user_id"
    assert relationship["primary_key_columns"] == ["user_id"]


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
