#!/usr/bin/env python3
# ruff: noqa: E402
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "sidemantic[widget]",
#   "duckdb",
#   "pandas",
# ]
# ///
"""OSI adtech widget notebook in percent-cell format.

Run as script:
    uv run examples/osi_demo/osi_widget_notebook.py

Use as notebook:
    Open this file in an editor that supports `# %%` notebook cells.
"""

# %% [markdown]
# # OSI Adtech Widget Demo
#
# This is a self-contained percent-format notebook script:
# - defines an OSI semantic model inline
# - creates in-memory adtech demo tables in DuckDB
# - loads the model with `OSIAdapter`
# - renders a `MetricsExplorer` widget

# %%
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import duckdb

# Add project root to path for local development
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# %% [markdown]
# ## 1) Define an OSI semantic model (inline YAML)

# %%
OSI_MODEL_YAML = """
semantic_model:
  - name: osi_adtech_notebook
    description: Self-contained OSI adtech notebook model

    datasets:
      - name: campaigns
        source: adtech.campaigns
        primary_key: [campaign_id]
        fields:
          - name: campaign_id
            expression:
              dialects: [{ dialect: ANSI_SQL, expression: campaign_id }]
          - name: campaign_name
            expression:
              dialects: [{ dialect: ANSI_SQL, expression: campaign_name }]
          - name: objective
            expression:
              dialects: [{ dialect: ANSI_SQL, expression: objective }]

      - name: impressions
        source: adtech.impressions
        primary_key: [impression_id]
        fields:
          - name: impression_id
            expression:
              dialects: [{ dialect: ANSI_SQL, expression: impression_id }]
          - name: impression_time
            expression:
              dialects: [{ dialect: ANSI_SQL, expression: impression_time }]
            dimension:
              is_time: true
          - name: campaign_id
            expression:
              dialects: [{ dialect: ANSI_SQL, expression: campaign_id }]
          - name: user_id
            expression:
              dialects: [{ dialect: ANSI_SQL, expression: user_id }]
          - name: is_viewable
            expression:
              dialects: [{ dialect: ANSI_SQL, expression: is_viewable }]

      - name: clicks
        source: adtech.clicks
        primary_key: [click_id]
        fields:
          - name: click_id
            expression:
              dialects: [{ dialect: ANSI_SQL, expression: click_id }]
          - name: impression_id
            expression:
              dialects: [{ dialect: ANSI_SQL, expression: impression_id }]
          - name: click_time
            expression:
              dialects: [{ dialect: ANSI_SQL, expression: click_time }]
            dimension:
              is_time: true

      - name: conversions
        source: adtech.conversions
        primary_key: [conversion_id]
        fields:
          - name: conversion_id
            expression:
              dialects: [{ dialect: ANSI_SQL, expression: conversion_id }]
          - name: click_id
            expression:
              dialects: [{ dialect: ANSI_SQL, expression: click_id }]
          - name: revenue_usd
            expression:
              dialects: [{ dialect: ANSI_SQL, expression: revenue_usd }]
          - name: is_post_view
            expression:
              dialects: [{ dialect: ANSI_SQL, expression: is_post_view }]

      - name: spend
        source: adtech.spend
        primary_key: [spend_id]
        fields:
          - name: spend_id
            expression:
              dialects: [{ dialect: ANSI_SQL, expression: spend_id }]
          - name: spend_date
            expression:
              dialects: [{ dialect: ANSI_SQL, expression: spend_date }]
            dimension:
              is_time: true
          - name: campaign_id
            expression:
              dialects: [{ dialect: ANSI_SQL, expression: campaign_id }]
          - name: media_cost_usd
            expression:
              dialects: [{ dialect: ANSI_SQL, expression: media_cost_usd }]

    relationships:
      - name: impressions_to_campaigns
        from: impressions
        to: campaigns
        from_columns: [campaign_id]
        to_columns: [campaign_id]
      - name: clicks_to_impressions
        from: clicks
        to: impressions
        from_columns: [impression_id]
        to_columns: [impression_id]
      - name: conversions_to_clicks
        from: conversions
        to: clicks
        from_columns: [click_id]
        to_columns: [click_id]
      - name: spend_to_campaigns
        from: spend
        to: campaigns
        from_columns: [campaign_id]
        to_columns: [campaign_id]

    metrics:
      - name: impression_count
        expression:
          dialects:
            - dialect: ANSI_SQL
              expression: COUNT(impressions.impression_id)
      - name: unique_reach
        expression:
          dialects:
            - dialect: ANSI_SQL
              expression: COUNT(DISTINCT impressions.user_id)
      - name: click_count
        expression:
          dialects:
            - dialect: ANSI_SQL
              expression: COUNT(clicks.click_id)
      - name: conversion_count
        expression:
          dialects:
            - dialect: ANSI_SQL
              expression: COUNT(conversions.conversion_id)
      - name: spend_usd
        expression:
          dialects:
            - dialect: ANSI_SQL
              expression: SUM(spend.media_cost_usd)
      - name: conversion_revenue_usd
        expression:
          dialects:
            - dialect: ANSI_SQL
              expression: SUM(conversions.revenue_usd)
      - name: ctr
        expression:
          dialects:
            - dialect: ANSI_SQL
              expression: COUNT(clicks.click_id) * 1.0 / NULLIF(COUNT(impressions.impression_id), 0)
      - name: cvr
        expression:
          dialects:
            - dialect: ANSI_SQL
              expression: COUNT(conversions.conversion_id) * 1.0 / NULLIF(COUNT(clicks.click_id), 0)
      - name: cpm
        expression:
          dialects:
            - dialect: ANSI_SQL
              expression: SUM(spend.media_cost_usd) * 1000.0 / NULLIF(COUNT(impressions.impression_id), 0)
      - name: roas
        expression:
          dialects:
            - dialect: ANSI_SQL
              expression: SUM(conversions.revenue_usd) * 1.0 / NULLIF(SUM(spend.media_cost_usd), 0)
"""

# %% [markdown]
# ## 2) Create adtech demo data

# %%
conn = duckdb.connect(":memory:")
conn.execute("create schema adtech")

conn.execute("""
    create table adtech.campaigns (
        campaign_id integer,
        campaign_name varchar,
        objective varchar
    )
""")
conn.execute("""
    insert into adtech.campaigns values
        (101, 'Prospecting US', 'acquisition'),
        (202, 'Retargeting EU', 'retention')
""")

conn.execute("""
    create table adtech.impressions (
        impression_id integer,
        impression_time timestamp,
        campaign_id integer,
        user_id integer,
        is_viewable integer
    )
""")
conn.execute("""
    insert into adtech.impressions values
        (1, '2025-01-01 10:01:00', 101, 10001, 1),
        (2, '2025-01-01 10:02:00', 101, 10002, 0),
        (3, '2025-01-01 10:03:00', 101, 10001, 1),
        (4, '2025-01-01 10:04:00', 202, 20001, 1),
        (5, '2025-01-01 10:05:00', 202, 20002, 1),
        (6, '2025-01-01 10:06:00', 202, 20001, 0)
""")

conn.execute("""
    create table adtech.clicks (
        click_id integer,
        impression_id integer,
        click_time timestamp
    )
""")
conn.execute("""
    insert into adtech.clicks values
        (10101, 1, '2025-01-01 10:10:00'),
        (10102, 2, '2025-01-01 10:11:00'),
        (20201, 4, '2025-01-01 10:12:00'),
        (20202, 5, '2025-01-01 10:13:00')
""")

conn.execute("""
    create table adtech.conversions (
        conversion_id integer,
        click_id integer,
        revenue_usd double,
        is_post_view integer
    )
""")
conn.execute("""
    insert into adtech.conversions values
        (9001, 10101, 120.0, 0),
        (9002, 20201, 50.0, 1),
        (9003, 20202, 180.0, 0)
""")

conn.execute("""
    create table adtech.spend (
        spend_id integer,
        spend_date date,
        campaign_id integer,
        media_cost_usd double
    )
""")
conn.execute("""
    insert into adtech.spend values
        (1, '2025-01-01', 101, 100.0),
        (2, '2025-01-01', 202, 120.0)
""")

# %% [markdown]
# ## 3) Load OSI model into Sidemantic

# %%
with tempfile.TemporaryDirectory() as tmp_dir:
    tmp_path = Path(tmp_dir) / "model.yaml"
    tmp_path.write_text(OSI_MODEL_YAML, encoding="utf-8")
    from sidemantic.adapters.osi import OSIAdapter

    graph = OSIAdapter().parse(tmp_path)

from sidemantic import SemanticLayer

layer = SemanticLayer()
layer.conn = conn
for model in graph.models.values():
    layer.add_model(model)
for metric in graph.metrics.values():
    layer.add_metric(metric)

print(f"Loaded models: {len(layer.graph.models)}")
print(f"Loaded metrics: {len(layer.graph.metrics)}")

# %% [markdown]
# ## 4) Validate with one cross-model query

# %%
sql = layer.compile(
    metrics=["impression_count", "click_count", "conversion_count", "spend_usd", "ctr", "roas"],
    dimensions=["campaigns.campaign_name"],
    order_by=["campaigns.campaign_name"],
)
print(sql)
conn.execute(sql).fetchdf()

# %% [markdown]
# ## 5) Launch Sidemantic widget
#
# In notebook mode, render `widget` to explore metrics interactively.

# %%
from sidemantic.widget import MetricsExplorer

widget = MetricsExplorer(
    layer,
    metrics=[
        "impression_count",
        "unique_reach",
        "click_count",
        "conversion_count",
        "spend_usd",
        "conversion_revenue_usd",
        "ctr",
        "cvr",
        "cpm",
        "roas",
    ],
    dimensions=[
        "campaigns.campaign_name",
        "campaigns.objective",
        "impressions.impression_time",
    ],
)
widget
