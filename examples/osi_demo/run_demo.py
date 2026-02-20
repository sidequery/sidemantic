#!/usr/bin/env python3
# /// script
# dependencies = ["sidemantic", "duckdb", "pandas"]
# ///
"""OSI demo with a complex adtech semantic layer.

This demo:
1. Loads a complex OSI semantic model (10 datasets, 25 metrics)
2. Creates realistic adtech sample data in DuckDB
3. Runs multi-hop and cross-model metric queries

Run with:
    uv run examples/osi_demo/run_demo.py
"""

import sys
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb

# Add project root to path for local development
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

if TYPE_CHECKING:
    from sidemantic import SemanticLayer


def create_demo_data(conn: duckdb.DuckDBPyConnection) -> None:
    """Create adtech demo tables and seed data."""
    conn.execute("create schema if not exists adtech")

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
            (101, 1, 'Prospecting US', 'acquisition', 'active', '2025-01-01', '2025-03-31', 50000),
            (202, 2, 'Retargeting EU', 'retention', 'active', '2025-01-01', '2025-03-31', 30000)
    """)

    conn.execute("""
        create table adtech.ad_groups (
            ad_group_id integer,
            campaign_id integer,
            ad_group_name varchar,
            bid_strategy varchar
        )
    """)
    conn.execute("""
        insert into adtech.ad_groups values
            (1001, 101, 'US Prospecting Display', 'tCPA'),
            (1002, 101, 'US Prospecting Video', 'tROAS'),
            (2001, 202, 'EU Retargeting Display', 'Max Conversions')
    """)

    conn.execute("""
        create table adtech.creatives (
            creative_id integer,
            ad_group_id integer,
            creative_name varchar,
            creative_format varchar,
            video_length_sec integer
        )
    """)
    conn.execute("""
        insert into adtech.creatives values
            (5001, 1001, 'Prospect Banner A', 'banner', 0),
            (5002, 1002, 'Prospect Video A', 'video', 15),
            (5003, 2001, 'Retarget Banner A', 'banner', 0),
            (5004, 2001, 'Retarget Video A', 'video', 6)
    """)

    conn.execute("""
        create table adtech.publishers (
            publisher_id integer,
            publisher_name varchar,
            supply_channel varchar
        )
    """)
    conn.execute("""
        insert into adtech.publishers values
            (11, 'NewsHub', 'open_exchange'),
            (22, 'StreamPrime', 'private_marketplace')
    """)

    conn.execute("""
        create table adtech.geos (
            geo_id integer,
            country varchar,
            region varchar,
            market_tier varchar
        )
    """)
    conn.execute("""
        insert into adtech.geos values
            (1, 'US', 'North America', 'tier_1'),
            (2, 'DE', 'EMEA', 'tier_1')
    """)

    conn.execute("""
        create table adtech.devices (
            device_id integer,
            device_type varchar,
            os_family varchar,
            browser_family varchar
        )
    """)
    conn.execute("""
        insert into adtech.devices values
            (1, 'mobile', 'iOS', 'Safari'),
            (2, 'desktop', 'Windows', 'Chrome')
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
            (1, '2025-01-01 10:01:00', 101, 1001, 5001, 11, 1, 1, 10001, 1, 1, 1),
            (2, '2025-01-01 10:02:00', 101, 1001, 5001, 22, 1, 2, 10002, 0, 1, 0),
            (3, '2025-01-01 10:03:00', 101, 1002, 5002, 11, 1, 1, 10001, 1, 1, 1),
            (4, '2025-01-01 10:04:00', 101, 1002, 5002, 22, 2, 2, 10003, 1, 0, 0),
            (5, '2025-01-01 10:05:00', 202, 2001, 5003, 11, 2, 1, 20001, 1, 1, 1),
            (6, '2025-01-01 10:06:00', 202, 2001, 5004, 22, 2, 2, 20002, 1, 1, 1),
            (7, '2025-01-01 10:07:00', 202, 2001, 5003, 11, 2, 1, 20001, 0, 1, 0),
            (8, '2025-01-01 10:08:00', 202, 2001, 5004, 22, 1, 2, 20003, 1, 0, 0)
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
            (10101, '2025-01-01 10:10:00', 1, 101, 10001),
            (10102, '2025-01-01 10:11:00', 2, 101, 10002),
            (20201, '2025-01-01 10:12:00', 5, 202, 20001),
            (20202, '2025-01-01 10:13:00', 6, 202, 20002)
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
            (9001, '2025-01-01 12:00:00', 10101, 101, 'purchase', 120.0, 0),
            (9002, '2025-01-01 12:30:00', 20201, 202, 'signup', 50.0, 1),
            (9003, '2025-01-01 13:00:00', 20202, 202, 'purchase', 180.0, 0)
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
            (1, '2025-01-01', 101, 11, 60.0, 6.0, 2.0),
            (2, '2025-01-01', 101, 22, 40.0, 4.0, 1.0),
            (3, '2025-01-01', 202, 11, 70.0, 7.0, 3.0),
            (4, '2025-01-01', 202, 22, 50.0, 5.0, 2.0)
    """)


def load_osi_layer(conn: duckdb.DuckDBPyConnection) -> "SemanticLayer":
    """Load OSI file and build a SemanticLayer."""
    from sidemantic import SemanticLayer
    from sidemantic.adapters.osi import OSIAdapter

    model_path = Path(__file__).parent / "adtech_semantic_model.yaml"
    graph = OSIAdapter().parse(model_path)

    layer = SemanticLayer()
    layer.conn = conn

    for model in graph.models.values():
        layer.add_model(model)
    for metric in graph.metrics.values():
        layer.add_metric(metric)

    return layer


def run_query(
    layer: "SemanticLayer",
    title: str,
    metrics: list[str],
    dimensions: list[str],
    order_by: list[str] | None = None,
) -> None:
    """Compile and run a semantic query."""
    print("=" * 90)
    print(title)
    print("=" * 90)
    sql = layer.compile(metrics=metrics, dimensions=dimensions, order_by=order_by)
    print("\nGenerated SQL:")
    print(sql)
    print("\nResults:")
    df = layer.conn.execute(sql).fetchdf()
    if df.empty:
        raise RuntimeError(f"Query returned no rows: {title}")
    print(df.to_string(index=False))
    print()


def main() -> None:
    conn = duckdb.connect(":memory:")
    create_demo_data(conn)
    layer = load_osi_layer(conn)

    print("Loaded OSI semantic layer")
    print(f"Models: {len(layer.graph.models)}")
    print(f"Metrics: {len(layer.graph.metrics)}")
    print()

    run_query(
        layer=layer,
        title="Query 1: Campaign performance with cross-model KPIs",
        metrics=[
            "impression_count",
            "click_count",
            "conversion_count",
            "spend_usd",
            "ctr",
            "cvr",
            "cpm",
            "cpc",
            "cpa",
            "roas",
        ],
        dimensions=["campaigns.campaign_name"],
        order_by=["campaigns.campaign_name"],
    )

    run_query(
        layer=layer,
        title="Query 2: Publisher x device quality diagnostics",
        metrics=[
            "impression_count",
            "viewable_impression_count",
            "viewability_rate",
            "video_start_count",
            "video_complete_count",
            "video_completion_rate",
        ],
        dimensions=["publishers.publisher_name", "devices.device_type"],
        order_by=["publishers.publisher_name", "devices.device_type"],
    )

    run_query(
        layer=layer,
        title="Query 3: Geo and objective profitability (multi-hop joins)",
        metrics=[
            "unique_reach",
            "frequency",
            "total_cost_usd",
            "conversion_revenue_usd",
            "gross_profit_usd",
            "margin_pct",
            "effective_cost_per_mille",
            "post_view_conversion_count",
        ],
        dimensions=["geos.country", "campaigns.objective"],
        order_by=["geos.country", "campaigns.objective"],
    )

    print("Done: OSI adtech example executed successfully.")


if __name__ == "__main__":
    main()
