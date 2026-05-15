"""Regression coverage for Python->Rust YAML bridge serialization fidelity."""

import yaml

from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.pre_aggregation import Index, PreAggregation, RefreshKey
from sidemantic.rust_bridge import models_to_rust_yaml


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
    dimension_payload = model_payload["dimensions"][0]
    metric_payload = model_payload["metrics"][0]
    preagg_payload = model_payload["pre_aggregations"][0]

    assert model_payload["source_uri"] == "s3://warehouse/orders"
    assert model_payload["extends"] == "base_orders"
    assert model_payload["unique_keys"] == [["order_id", "tenant_id"]]

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
