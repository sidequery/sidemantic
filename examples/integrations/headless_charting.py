#!/usr/bin/env python
"""Headless charting integration smoke/performance example.

For the user-facing declarative dashboard authoring example, see
``examples/headless_dashboard``. This script intentionally builds synthetic
data and renderer fixtures programmatically so it can exercise the full runtime
surface in one place.

Run:
  uv run examples/integrations/headless_charting.py --output-dir /tmp/sidemantic-charts
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import yaml

from sidemantic import Dimension, Metric, Model, SemanticLayer
from sidemantic.dashboard import DASHBOARD_SCHEMA, generate_dashboard_typescript
from sidemantic.viz import ChartBuilder, CrossfilterDashboard, CrossfilterTab


def build_layer(
    connection: str = "duckdb:///:memory:",
    large_records: int = 200_000,
    huge_records: int | None = None,
    massive_records: int | None = None,
    extreme_records: int | None = None,
    large_materialized: bool = True,
    huge_materialized: bool = True,
) -> SemanticLayer:
    layer = SemanticLayer(connection=connection, auto_register=False)
    _drop_relation(layer, "orders")
    layer.adapter.execute("""
        CREATE TABLE orders (
            id INTEGER,
            created_at DATE,
            region VARCHAR,
            channel VARCHAR,
            customer_tier VARCHAR,
            product_line VARCHAR,
            gross_margin DOUBLE,
            amount DOUBLE
        )
    """)
    rows = []
    order_id = 1
    regions = {"North": 1.08, "South": 0.88, "West": 1.22, "Central": 0.98}
    channels = {"Web": 1.15, "Sales": 1.35, "Partner": 0.92}
    tiers = {"Enterprise": 1.55, "Mid-Market": 1.05, "SMB": 0.72}
    product_lines = {"Platform": 1.28, "Analytics": 1.12, "Services": 0.85}
    month_curve = [0.86, 1.02, 0.93, 1.14, 1.38, 1.22, 1.48, 1.31, 1.58]

    for month_index, month in enumerate(("01", "02", "03", "04", "05", "06", "07", "08", "09"), start=1):
        for region, region_factor in regions.items():
            for channel, channel_factor in channels.items():
                for tier, tier_factor in tiers.items():
                    for product_line, product_factor in product_lines.items():
                        repeat_count = 1 + (
                            (month_index + len(region) + len(channel) + len(tier) + len(product_line)) % 3
                        )
                        for repeat in range(repeat_count):
                            day = 2 + ((repeat * 9 + month_index + len(channel)) % 24)
                            base = 210 + month_index * 19 + repeat * 13
                            amount = (
                                base
                                * month_curve[month_index - 1]
                                * region_factor
                                * channel_factor
                                * tier_factor
                                * product_factor
                            )
                            margin_rate = (
                                0.34 + (0.04 if channel == "Web" else 0) + (0.03 if product_line == "Platform" else 0)
                            )
                            margin = amount * margin_rate
                            rows.append(
                                (
                                    order_id,
                                    f"2024-{month}-{day:02d}",
                                    region,
                                    channel,
                                    tier,
                                    product_line,
                                    round(margin, 2),
                                    round(amount, 2),
                                )
                            )
                            order_id += 1
    layer.adapter.executemany("INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
    _add_orders_model(layer, "orders")
    _create_synthetic_orders_table(layer, "orders_200k", large_records, materialized=large_materialized)
    _add_orders_model(layer, "orders_200k")
    if huge_records:
        _create_synthetic_orders_table(layer, "orders_2m", huge_records, materialized=huge_materialized)
        _add_orders_model(layer, "orders_2m")
    if massive_records:
        _create_synthetic_orders_table(layer, "orders_20m", massive_records, materialized=False)
        _add_orders_model(layer, "orders_20m")
    if extreme_records:
        _create_synthetic_orders_table(layer, "orders_100m", extreme_records, materialized=False)
        _add_orders_model(layer, "orders_100m")
    return layer


def _create_synthetic_orders_table(
    layer: SemanticLayer,
    table_name: str,
    records: int,
    *,
    materialized: bool = True,
) -> None:
    relation_kind = "TABLE" if materialized else "VIEW"
    _drop_relation(layer, table_name)
    layer.adapter.execute(f"""
        CREATE {relation_kind} {table_name} AS
        WITH source AS (
            SELECT i::BIGINT AS id
            FROM range({records}) AS rows(i)
        ),
        shaped AS (
            SELECT
                id + 1 AS id,
                DATE '2023-01-01' + CAST(id % 730 AS INTEGER) AS created_at,
                CASE id % 5
                    WHEN 0 THEN 'North'
                    WHEN 1 THEN 'South'
                    WHEN 2 THEN 'West'
                    WHEN 3 THEN 'Central'
                    ELSE 'East'
                END AS region,
                CASE id % 4
                    WHEN 0 THEN 'Web'
                    WHEN 1 THEN 'Sales'
                    WHEN 2 THEN 'Partner'
                    ELSE 'Marketplace'
                END AS channel,
                CASE id % 4
                    WHEN 0 THEN 'Enterprise'
                    WHEN 1 THEN 'Strategic'
                    WHEN 2 THEN 'Mid-Market'
                    ELSE 'SMB'
                END AS customer_tier,
                CASE id % 5
                    WHEN 0 THEN 'Platform'
                    WHEN 1 THEN 'Analytics'
                    WHEN 2 THEN 'Services'
                    WHEN 3 THEN 'Data Cloud'
                    ELSE 'AI Apps'
                END AS product_line,
                ROUND(
                    (80 + (id % 900) * 1.7)
                    * (1 + (id % 5) * 0.06)
                    * (1 + (id % 4) * 0.08)
                    * (1 + (id % 6) * 0.03),
                    2
                ) AS amount
            FROM source
        )
        SELECT
            id,
            created_at,
            region,
            channel,
            customer_tier,
            product_line,
            ROUND(amount * (0.28 + (id % 7) * 0.015), 2) AS gross_margin,
            amount
        FROM shaped
    """)


def _drop_relation(layer: SemanticLayer, name: str) -> None:
    for relation_kind in ("VIEW", "TABLE"):
        try:
            layer.adapter.execute(f"DROP {relation_kind} IF EXISTS {name}")
        except Exception:
            pass


def _add_orders_model(layer: SemanticLayer, model_name: str) -> None:
    layer.add_model(
        Model(
            name=model_name,
            table=model_name,
            primary_key="id",
            dimensions=[
                Dimension(name="created_at", type="time", granularity="day"),
                Dimension(name="region", type="categorical"),
                Dimension(name="channel", type="categorical"),
                Dimension(name="customer_tier", type="categorical"),
                Dimension(name="product_line", type="categorical"),
            ],
            metrics=[
                Metric(name="revenue", agg="sum", sql="amount"),
                Metric(name="gross_margin", agg="sum", sql="gross_margin"),
                Metric(name="order_count", agg="count"),
            ],
        )
    )


def build_crossfilter_dashboard(
    *,
    connection: str = "duckdb:///:memory:",
    dashboard_renderer: str = "vega-lite",
    large_records: int = 200_000,
    huge_records: int | None = 2_000_000,
    massive_records: int | None = 20_000_000,
    extreme_records: int | None = 100_000_000,
    large_materialized: bool = True,
    huge_materialized: bool = True,
) -> CrossfilterDashboard:
    """Build the database-backed demo dashboard used by local and hosted examples.

    Interaction pre-aggregations are enabled but not warmed here. They are
    created lazily on the first relevant brush/select interaction.
    """
    layer = build_layer(
        connection=connection,
        large_records=large_records,
        huge_records=huge_records,
        massive_records=massive_records,
        extreme_records=extreme_records,
        large_materialized=large_materialized,
        huge_materialized=huge_materialized,
    )
    chart = _build_chart(layer, "orders", "Revenue Performance Explorer")
    large_chart = _build_chart(layer, "orders_200k", "Revenue Performance Explorer (200k)")
    huge_chart = _build_chart(layer, "orders_2m", "Revenue Performance Explorer (2M)") if huge_records else None
    massive_chart = _build_chart(layer, "orders_20m", "Revenue Performance Explorer (20M)") if massive_records else None
    extreme_chart = (
        _build_chart(layer, "orders_100m", "Revenue Performance Explorer (100M)") if extreme_records else None
    )

    dashboard_tabs = [
        CrossfilterTab(
            "standard",
            "1.9k records",
            chart.crossfilter(interaction_preaggregations=True, renderer=dashboard_renderer),
        ),
        CrossfilterTab(
            "large",
            f"{large_records:,} records",
            large_chart.crossfilter(
                source_record_count=large_records,
                interaction_preaggregations=True,
                renderer=dashboard_renderer,
            ),
            source_record_count=large_records,
        ),
    ]
    if huge_chart is not None:
        dashboard_tabs.append(
            CrossfilterTab(
                "huge",
                f"{huge_records:,} records",
                huge_chart.crossfilter(
                    source_record_count=huge_records,
                    interaction_preaggregations=True,
                    renderer=dashboard_renderer,
                ),
                source_record_count=huge_records,
            )
        )
    if massive_chart is not None:
        dashboard_tabs.append(
            CrossfilterTab(
                "massive",
                f"{massive_records:,} records",
                massive_chart.crossfilter(
                    source_record_count=massive_records,
                    interaction_preaggregations=True,
                    renderer=dashboard_renderer,
                ),
                source_record_count=massive_records,
            )
        )
    if extreme_chart is not None:
        dashboard_tabs.append(
            CrossfilterTab(
                "extreme",
                f"{extreme_records:,} records",
                extreme_chart.crossfilter(
                    source_record_count=extreme_records,
                    interaction_preaggregations=True,
                    renderer=dashboard_renderer,
                ),
                source_record_count=extreme_records,
            )
        )
    return CrossfilterDashboard("Revenue Performance Explorer", dashboard_tabs)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("/tmp/sidemantic-headless-charting"),
        help="Directory to write renderer specs and preview HTML",
    )
    parser.add_argument("--large-records", type=int, default=200_000, help="Rows to generate for the large tab")
    parser.add_argument("--huge-records", type=int, default=2_000_000, help="Rows to generate for the 2M tab")
    parser.add_argument("--massive-records", type=int, default=20_000_000, help="Rows to generate for the 20M tab")
    parser.add_argument("--extreme-records", type=int, default=100_000_000, help="Rows to generate for the 100M tab")
    parser.add_argument("--serve", action="store_true", help="Serve the example with a database-backed query endpoint")
    parser.add_argument("--host", default="127.0.0.1", help="Host for --serve")
    parser.add_argument("--port", type=int, default=8877, help="Port for --serve")
    parser.add_argument(
        "--dashboard-renderer",
        default="vega-lite",
        choices=["vega-lite", "plotly", "observable-plot", "d3"],
        help="Chart library adapter used inside the live crossfilter dashboard",
    )
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    layer = build_layer(
        large_records=args.large_records,
        huge_records=args.huge_records if args.huge_records else None,
        massive_records=args.massive_records if args.massive_records else None,
        extreme_records=args.extreme_records if args.extreme_records else None,
    )
    chart = _build_chart(layer, "orders", "Revenue Performance Explorer")
    large_chart = _build_chart(layer, "orders_200k", "Revenue Performance Explorer (200k)")
    huge_chart = _build_chart(layer, "orders_2m", "Revenue Performance Explorer (2M)") if args.huge_records else None
    massive_chart = (
        _build_chart(layer, "orders_20m", "Revenue Performance Explorer (20M)") if args.massive_records else None
    )
    extreme_chart = (
        _build_chart(layer, "orders_100m", "Revenue Performance Explorer (100M)") if args.extreme_records else None
    )
    dashboard_tabs = [
        CrossfilterTab(
            "standard",
            "1.9k records",
            chart.crossfilter(interaction_preaggregations=True, renderer=args.dashboard_renderer),
        ),
        CrossfilterTab(
            "large",
            f"{args.large_records:,} records",
            large_chart.crossfilter(interaction_preaggregations=True, renderer=args.dashboard_renderer),
        ),
    ]
    if huge_chart is not None:
        dashboard_tabs.append(
            CrossfilterTab(
                "huge",
                f"{args.huge_records:,} records",
                huge_chart.crossfilter(interaction_preaggregations=True, renderer=args.dashboard_renderer),
            )
        )
    if massive_chart is not None:
        dashboard_tabs.append(
            CrossfilterTab(
                "massive",
                f"{args.massive_records:,} records",
                massive_chart.crossfilter(interaction_preaggregations=True, renderer=args.dashboard_renderer),
            )
        )
    if extreme_chart is not None:
        dashboard_tabs.append(
            CrossfilterTab(
                "extreme",
                f"{args.extreme_records:,} records",
                extreme_chart.crossfilter(interaction_preaggregations=True, renderer=args.dashboard_renderer),
            )
        )
    dashboard = CrossfilterDashboard("Revenue Performance Explorer", dashboard_tabs)
    specs = {tab.id: tab.session.to_spec(query_endpoint=dashboard.query_endpoint) for tab in dashboard.tabs}

    outputs = {
        "vegalite.json": chart.to_vegalite(),
        "plotly.json": chart.to_plotly(),
        "observable_plot.json": chart.to_observable_plot(),
        "d3.json": chart.to_d3(),
        "crossfilter.json": dashboard.to_spec(),
        "crossfilter_200k.json": specs["large"],
    }
    if "huge" in specs:
        outputs["crossfilter_2m.json"] = specs["huge"]
    if "massive" in specs:
        outputs["crossfilter_20m.json"] = specs["massive"]
    if "extreme" in specs:
        outputs["crossfilter_100m.json"] = specs["extreme"]
    for filename, spec in outputs.items():
        (args.output_dir / filename).write_text(json.dumps(spec, indent=2, default=str))

    html_outputs = {
        "vegalite.html": chart.to_html("vega-lite"),
        "plotly.html": chart.to_html("plotly"),
        "observable_plot.html": chart.to_html("observable-plot"),
        "d3.html": chart.to_html("d3"),
        "crossfilter.html": dashboard.to_html(),
        "crossfilter_200k.html": large_chart.to_html("crossfilter"),
    }
    if huge_chart is not None:
        html_outputs["crossfilter_2m.html"] = huge_chart.to_html("crossfilter")
    if massive_chart is not None:
        html_outputs["crossfilter_20m.html"] = massive_chart.to_html("crossfilter")
    if extreme_chart is not None:
        html_outputs["crossfilter_100m.html"] = extreme_chart.to_html("crossfilter")
    for filename, html in html_outputs.items():
        (args.output_dir / filename).write_text(html)

    dashboard_spec = _dashboard_spec(
        renderer=args.dashboard_renderer,
        large_records=args.large_records,
        huge_records=args.huge_records if huge_chart is not None else None,
        massive_records=args.massive_records if massive_chart is not None else None,
        extreme_records=args.extreme_records if extreme_chart is not None else None,
    )
    (args.output_dir / "dashboard.yml").write_text(yaml.safe_dump(dashboard_spec, sort_keys=False))
    (args.output_dir / "dashboard.ts").write_text(_dashboard_ts_example(dashboard_spec))
    (args.output_dir / "sidemantic.generated.ts").write_text(generate_dashboard_typescript(layer))

    (args.output_dir / "interactive_gallery.html").write_text(_gallery_html(html_outputs))

    print(f"Wrote {len(outputs)} renderer specs and {len(html_outputs)} interactive HTML previews to {args.output_dir}")
    print(chart.sql)
    if args.serve:
        dashboard.serve(args.output_dir, host=args.host, port=args.port)


def _build_chart(layer: SemanticLayer, model_name: str, title: str) -> ChartBuilder:
    return (
        layer.chart(
            [f"{model_name}.revenue", f"{model_name}.gross_margin", f"{model_name}.order_count"],
            by=[
                f"{model_name}.created_at__month",
                f"{model_name}.region",
                f"{model_name}.channel",
                f"{model_name}.customer_tier",
                f"{model_name}.product_line",
            ],
            title=title,
        )
        .line()
        .brush("x")
    )


def _dashboard_spec(
    *,
    renderer: str,
    large_records: int,
    huge_records: int | None,
    massive_records: int | None,
    extreme_records: int | None,
) -> dict:
    tabs = [
        _dashboard_tab("standard", "1.9k records", "orders", "Revenue Explorer"),
        _dashboard_tab("large", f"{large_records:,} records", "orders_200k", "Revenue Explorer (200k)"),
    ]
    if huge_records is not None:
        tabs.append(_dashboard_tab("huge", f"{huge_records:,} records", "orders_2m", "Revenue Explorer (2M)"))
    if massive_records is not None:
        tabs.append(_dashboard_tab("massive", f"{massive_records:,} records", "orders_20m", "Revenue Explorer (20M)"))
    if extreme_records is not None:
        tabs.append(_dashboard_tab("extreme", f"{extreme_records:,} records", "orders_100m", "Revenue Explorer (100M)"))
    return {
        "schema": DASHBOARD_SCHEMA,
        "title": "Revenue Performance Explorer",
        "defaults": {
            "renderer": renderer,
            "query": {"interaction_preaggregations": True},
            "interactions": {"scope": "tab"},
        },
        "tabs": tabs,
    }


def _dashboard_tab(tab_id: str, label: str, model_name: str, title: str) -> dict:
    month = f"{model_name}.created_at__month"
    region = f"{model_name}.region"
    channel = f"{model_name}.channel"
    customer_tier = f"{model_name}.customer_tier"
    product_line = f"{model_name}.product_line"
    return {
        "id": tab_id,
        "label": label,
        "charts": [
            {
                "id": f"{model_name}_revenue_explorer",
                "title": title,
                "type": "line",
                "query": {
                    "metrics": [
                        f"{model_name}.revenue",
                        f"{model_name}.gross_margin",
                        f"{model_name}.order_count",
                    ],
                    "dimensions": [month, region, channel, customer_tier, product_line],
                    "order_by": [month],
                    "interaction_preaggregations": True,
                },
                "encoding": {"x": month, "y": f"{model_name}.revenue", "color": region},
                "interactions": {
                    "brush": {"fields": [month], "channel": "x"},
                    "select": {"fields": [region, channel, customer_tier, product_line]},
                },
            }
        ],
    }


def _dashboard_ts_example(spec: dict) -> str:
    renderer = spec["defaults"]["renderer"]
    tab_calls = []
    model_names = []
    for tab in spec["tabs"]:
        chart = tab["charts"][0]
        first_metric = chart["query"]["metrics"][0]
        model_name = first_metric.split(".", 1)[0]
        model_names.append(model_name)
        tab_calls.append(f'    revenueExplorerTab("{tab["id"]}", "{tab["label"]}", "{model_name}", "{chart["title"]}")')
    model_union = " | ".join(f'"{model_name}"' for model_name in model_names)
    tab_calls_source = ",\n".join(tab_calls)
    return f"""import {{ defineDashboard, type DashboardRenderer }} from "./sidemantic.generated";

type OrdersModel = {model_union};

const renderer: DashboardRenderer = "{renderer}";

function revenueExplorerTab<const M extends OrdersModel>(
  id: string,
  label: string,
  model: M,
  title: string,
) {{
  const month = `${{model}}.created_at__month` as `${{M}}.created_at__month`;
  const region = `${{model}}.region` as `${{M}}.region`;
  const channel = `${{model}}.channel` as `${{M}}.channel`;
  const customerTier = `${{model}}.customer_tier` as `${{M}}.customer_tier`;
  const productLine = `${{model}}.product_line` as `${{M}}.product_line`;
  const revenue = `${{model}}.revenue` as `${{M}}.revenue`;
  const grossMargin = `${{model}}.gross_margin` as `${{M}}.gross_margin`;
  const orderCount = `${{model}}.order_count` as `${{M}}.order_count`;

  return {{
    id,
    label,
    charts: [{{
      id: `${{model}}_revenue_explorer`,
      title,
      type: "line",
      query: {{
        metrics: [revenue, grossMargin, orderCount],
        dimensions: [month, region, channel, customerTier, productLine],
        orderBy: [month],
        interactionPreaggregations: true,
      }},
      encoding: {{
        x: month,
        y: revenue,
        color: region,
      }},
      interactions: {{
        brush: {{ fields: [month], channel: "x" }},
        select: {{ fields: [region, channel, customerTier, productLine] }},
      }},
    }}],
  }} as const;
}}

export default defineDashboard({{
  schema: "sidemantic.dashboard.v1",
  title: "{spec["title"]}",
  defaults: {{
    renderer,
    query: {{ interactionPreaggregations: true }},
    interactions: {{ scope: "tab" }},
  }},
  tabs: [
{tab_calls_source}
  ],
}});
"""


def _gallery_html(html_outputs: dict[str, str]) -> str:
    frames = "\n".join(
        f"""
        <section>
          <h2>{filename.removesuffix(".html").replace("_", " ").title()}</h2>
          <iframe src="{filename}" title="{filename}"></iframe>
        </section>
        """
        for filename in html_outputs
    )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Sidemantic Headless Charting Gallery</title>
  <style>
    body {{ margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }}
    main {{ width: min(1180px, calc(100vw - 32px)); margin: 24px auto; }}
    h1 {{ font-size: 24px; margin: 0 0 16px; }}
    h2 {{ font-size: 16px; margin: 0 0 8px; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(520px, 1fr)); gap: 18px; }}
    section {{ border: 1px solid #d5dbe5; border-radius: 8px; padding: 12px; }}
    iframe {{ width: 100%; height: 560px; border: 0; }}
  </style>
</head>
<body>
  <main>
    <h1>Sidemantic Headless Charting Gallery</h1>
    <div class="grid">{frames}</div>
  </main>
</body>
</html>
"""


if __name__ == "__main__":
    main()
