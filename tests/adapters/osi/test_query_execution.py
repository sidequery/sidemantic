"""Execution tests for OSI-imported metrics."""

from pathlib import Path

import duckdb
import pytest

from sidemantic import SemanticLayer
from sidemantic.adapters.osi import OSIAdapter


def _load_layer_from_graph(graph, conn: duckdb.DuckDBPyConnection) -> SemanticLayer:
    layer = SemanticLayer()
    layer.conn = conn
    for model in graph.models.values():
        layer.add_model(model)
    for metric in graph.metrics.values():
        layer.add_metric(metric)
    return layer


def test_osi_ecommerce_metrics_execute():
    conn = duckdb.connect(":memory:")
    conn.execute("""
        create table orders (
            order_id integer,
            customer_id integer,
            order_date date,
            status varchar,
            amount double
        )
    """)
    conn.execute("""
        create table customers (
            customer_id integer,
            name varchar,
            email varchar,
            created_at date
        )
    """)
    conn.execute("""
        insert into orders values
            (1, 1, '2025-01-01', 'completed', 100.0),
            (2, 2, '2025-01-01', 'completed', 50.0)
    """)
    conn.execute("""
        insert into customers values
            (1, 'Alice', 'alice@example.com', '2024-01-01'),
            (2, 'Bob', 'bob@example.com', '2024-01-01')
    """)

    graph = OSIAdapter().parse("tests/fixtures/osi/ecommerce.yaml")
    graph.models["orders"].table = "orders"
    graph.models["customers"].table = "customers"
    layer = _load_layer_from_graph(graph, conn)

    total = layer.query(metrics=["total_revenue"]).fetchone()[0]
    assert total == pytest.approx(150.0)

    result = layer.query(metrics=["total_revenue"], dimensions=["customers.name"]).fetchall()
    assert sorted(result) == [("Alice", 100.0), ("Bob", 50.0)]


def test_osi_adtech_example_cross_model_metrics_execute():
    conn = duckdb.connect(":memory:")
    conn.execute("create schema adtech")

    conn.execute("""
        create table adtech.campaigns (
            campaign_id integer,
            advertiser_id integer,
            campaign_name varchar,
            objective varchar,
            status varchar,
            start_date date,
            end_date date,
            budget_usd double
        )
    """)
    conn.execute("""
        insert into adtech.campaigns values
            (100, 1, 'Campaign A', 'acquisition', 'active', '2025-01-01', '2025-02-01', 10000)
    """)

    conn.execute("""
        create table adtech.impressions (
            impression_id integer,
            impression_time timestamp,
            campaign_id integer,
            ad_group_id integer,
            creative_id integer,
            publisher_id integer,
            geo_id integer,
            device_id integer,
            user_id integer,
            is_viewable integer,
            video_start integer,
            video_complete integer
        )
    """)
    conn.execute("""
        insert into adtech.impressions values
            (1, '2025-01-01 10:00:00', 100, 10, 1000, 11, 1, 1, 501, 1, 1, 1),
            (2, '2025-01-01 10:01:00', 100, 10, 1000, 11, 1, 1, 502, 1, 1, 0)
    """)

    conn.execute("""
        create table adtech.clicks (
            click_id integer,
            click_time timestamp,
            impression_id integer,
            campaign_id integer,
            user_id integer
        )
    """)
    conn.execute("""
        insert into adtech.clicks values
            (10, '2025-01-01 10:05:00', 1, 100, 501)
    """)

    conn.execute("""
        create table adtech.conversions (
            conversion_id integer,
            conversion_time timestamp,
            click_id integer,
            campaign_id integer,
            conversion_type varchar,
            revenue_usd double,
            is_post_view integer
        )
    """)
    conn.execute("""
        insert into adtech.conversions values
            (900, '2025-01-01 11:00:00', 10, 100, 'purchase', 200.0, 0)
    """)

    conn.execute("""
        create table adtech.spend (
            spend_id integer,
            spend_date date,
            campaign_id integer,
            publisher_id integer,
            media_cost_usd double,
            platform_fee_usd double,
            data_fee_usd double
        )
    """)
    conn.execute("""
        insert into adtech.spend values
            (1, '2025-01-01', 100, 11, 100.0, 10.0, 5.0)
    """)

    graph = OSIAdapter().parse(Path("examples/osi_demo/adtech_semantic_model.yaml"))
    layer = _load_layer_from_graph(graph, conn)

    result = layer.query(
        metrics=[
            "impression_count",
            "click_count",
            "ctr",
            "spend_usd",
            "conversion_revenue_usd",
            "roas",
        ],
        dimensions=["campaigns.campaign_name"],
    ).fetchone()

    assert result is not None
    campaign_name, impressions, clicks, ctr, spend_usd, revenue_usd, roas = result
    assert campaign_name == "Campaign A"
    assert impressions == 2
    assert clicks == 1
    assert ctr == pytest.approx(0.5)
    assert spend_usd > 0
    assert revenue_usd > 0
    assert roas > 0
