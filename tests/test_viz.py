import io
import json
import re

import pytest

from examples.integrations.headless_charting import _build_chart, build_layer
from sidemantic import Dimension, Freshness, Metric, Model, Relationship, SemanticLayer
from sidemantic.viz import (
    CrossfilterDashboard,
    CrossfilterTab,
    DimensionEquals,
    MetricRange,
    TimeRange,
    _freshness_datetime,
)


def _strip_vendored_script_bodies(html: str) -> str:
    return re.sub(
        r'<script data-sidemantic-vendor="[^"]+">.*?</script>',
        '<script data-sidemantic-vendor=""></script>',
        html,
        flags=re.DOTALL,
    )


def _build_layer() -> SemanticLayer:
    layer = SemanticLayer(connection="duckdb:///:memory:", auto_register=False)
    layer.adapter.execute("""
        CREATE TABLE orders (
            id INTEGER,
            created_at DATE,
            region VARCHAR,
            amount DOUBLE
        )
    """)
    layer.adapter.execute("""
        INSERT INTO orders VALUES
            (1, DATE '2024-01-03', 'North', 120.0),
            (2, DATE '2024-01-18', 'South', 80.0),
            (3, DATE '2024-02-02', 'North', 140.0),
            (4, DATE '2024-02-21', 'West', 170.0),
            (5, DATE '2024-03-11', 'South', 110.0),
            (6, DATE '2024-03-29', 'West', 190.0)
    """)
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[
                Dimension(name="created_at", type="time", granularity="day"),
                Dimension(name="region", type="categorical"),
            ],
            metrics=[
                Metric(name="revenue", agg="sum", sql="amount"),
                Metric(name="order_count", agg="count"),
            ],
        )
    )
    return layer


def test_layer_chart_emits_renderer_specs():
    layer = _build_layer()

    chart = (
        layer.chart(
            "orders.revenue",
            by=["orders.created_at__month", "orders.region"],
            title="Monthly Revenue by Region",
        )
        .line()
        .brush("x")
    )

    vega = chart.to_vegalite()
    assert vega["mark"]["type"] == "line"
    assert vega["params"][0]["select"]["encodings"] == ["x"]
    assert vega["encoding"]["color"]["field"] == "region"
    assert vega["encoding"]["opacity"]["condition"]["param"] == "brush"
    assert len(vega["data"]["values"]) == 6
    assert "DATE_TRUNC" in chart.sql

    plotly = chart.to_plotly()
    assert {trace["name"] for trace in plotly["data"]} == {"North", "South", "West"}
    assert plotly["layout"]["title"]["text"] == "Monthly Revenue by Region"

    observable = chart.to_observable_plot()
    assert observable["renderer"] == "observable-plot"
    assert observable["marks"][0]["type"] == "lineY"
    assert observable["marks"][0]["options"]["stroke"] == "region"

    d3 = chart.to_d3()
    assert d3["renderer"] == "d3"
    assert d3["fields"]["y"] == ["revenue"]
    assert d3["fields"]["series"] == "region"

    crossfilter = chart.to_crossfilter()
    assert crossfilter["renderer"] == "sidemantic-crossfilter"
    assert crossfilter["fields"]["series"] == "region"
    assert crossfilter["fields"]["metric_aggs"] == {"revenue": "sum"}
    assert crossfilter["field_plan"]["protocol"] == "sidemantic-field-plan-v1"
    assert crossfilter["field_plan"]["aliases"] == {
        "created_at__month": "orders.created_at__month",
        "region": "orders.region",
        "revenue": "orders.revenue",
    }
    assert crossfilter["field_plan"]["encodings"]["x"]["id"] == "orders.created_at__month"
    assert crossfilter["interaction_plan"]["brush"]["fields"][0]["id"] == "orders.created_at__month"
    assert [view["id"] for view in crossfilter["views"]] == ["trend", "scatter", "breakdown_region", "rows"]


def test_crossfilter_static_renderer_uses_metric_aggregation_metadata():
    layer = _build_layer()
    model = layer.get_model("orders")
    model.metrics.append(Metric(name="avg_revenue", agg="avg", sql="amount"))

    chart = layer.chart(
        ["orders.avg_revenue", "orders.order_count"],
        by=["orders.created_at__month", "orders.region"],
    )
    spec = chart.to_crossfilter()
    html = chart.to_html("crossfilter")

    assert spec["fields"]["metric_aggs"] == {"avg_revenue": "avg", "order_count": "count"}
    assert "function aggregateMetric(values, metric)" in html
    assert "if (agg === 'min') return d3.min(numbers);" in html
    assert "if (agg === 'max') return d3.max(numbers);" in html
    assert "if (agg === 'sum' || agg === 'count') return d3.sum(numbers);" in html
    assert "return numbers.length === 1 ? numbers[0] : null;" in html
    assert "d3.sum(values, row => Number(row[yField]))" not in html
    assert "d3.sum(data, row => Number(row[metric]))" not in html


def test_crossfilter_source_record_count_uses_count_metric_metadata():
    layer = _build_layer()
    model = layer.get_model("orders")
    model.metrics.append(Metric(name="discount_amount", agg="sum", sql="amount"))

    count_chart = layer.chart("orders.order_count", by="orders.region")
    count_spec = count_chart.to_crossfilter()
    assert count_spec["fields"]["metric_aggs"] == {"order_count": "count"}
    assert count_chart.crossfilter().source_record_count == 6

    discount_chart = layer.chart("orders.discount_amount", by="orders.region")
    discount_spec = discount_chart.to_crossfilter()
    assert discount_spec["fields"]["metric_aggs"] == {"discount_amount": "sum"}
    assert discount_chart.crossfilter().source_record_count == len(discount_spec["data"])

    html = discount_chart.to_html("crossfilter")
    assert "function countMetricForSpec(overrideSpec = null, candidateMetrics = null)" in html
    assert "return metrics.find(metric => metric.toLowerCase().includes('count'));" not in html


def test_crossfilter_metadata_spec_does_not_pollute_full_spec_cache():
    layer = _build_layer()
    session = layer.chart(
        ["orders.revenue", "orders.order_count"],
        by=["orders.created_at__month", "orders.region"],
    ).crossfilter()

    metadata = session.to_metadata_spec()

    assert metadata["data_deferred"] is True
    assert metadata["data"] == []
    assert session._spec is None

    assert session.source_record_count == 6
    full_spec = session.to_spec()
    assert full_spec.get("data_deferred") is not True
    assert len(full_spec["data"]) == 6


def test_crossfilter_metadata_compiles_duplicate_bare_aliases_to_unique_fields():
    layer = _build_layer()
    layer.add_model(
        Model(
            name="customers",
            table="customers",
            primary_key="id",
            dimensions=[Dimension(name="region", type="categorical")],
            metrics=[Metric(name="customer_count", agg="count")],
        )
    )
    session = layer.chart(
        "orders.revenue",
        by=["orders.region", "customers.region"],
    ).crossfilter()

    spec = session.to_metadata_spec()

    assert spec["fields"]["dimensions"] == ["orders_region", "customers_region"]
    assert spec["field_plan"]["aliases"] == {
        "orders_region": "orders.region",
        "customers_region": "customers.region",
        "revenue": "orders.revenue",
    }


def test_chart_sql_uses_stable_aliases_for_same_named_join_fields():
    layer = SemanticLayer(connection="duckdb:///:memory:", auto_register=False)
    layer.adapter.execute("""
        CREATE TABLE customers (
            id INTEGER,
            region VARCHAR
        )
    """)
    layer.adapter.execute("""
        CREATE TABLE orders (
            id INTEGER,
            customer_id INTEGER,
            region VARCHAR,
            amount DOUBLE
        )
    """)
    layer.adapter.execute("INSERT INTO customers VALUES (1, 'Enterprise'), (2, 'Self Serve')")
    layer.adapter.execute("INSERT INTO orders VALUES (1, 1, 'West', 100.0), (2, 2, 'East', 50.0)")
    layer.add_model(
        Model(
            name="customers",
            table="customers",
            primary_key="id",
            dimensions=[Dimension(name="region", type="categorical")],
        )
    )
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[Dimension(name="region", type="categorical")],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
            relationships=[
                Relationship(name="customers", type="many_to_one", foreign_key="customer_id", primary_key="id")
            ],
        )
    )

    spec = layer.chart("orders.revenue", by=["orders.region", "customers.region"]).to_crossfilter()

    assert spec["fields"]["dimensions"] == ["orders_region", "customers_region"]
    assert "AS orders_region" in spec["sidemantic"]["sql"]
    assert "AS customers_region" in spec["sidemantic"]["sql"]
    assert set(spec["data"][0]) == {"orders_region", "customers_region", "revenue"}


def test_crossfilter_live_query_counts_records_without_materializing_lazy_spec():
    layer = _build_layer()
    session = layer.chart(
        ["orders.revenue", "orders.order_count"],
        by=["orders.created_at__month", "orders.region"],
    ).crossfilter()

    metadata = session.to_metadata_spec()
    assert metadata["data_deferred"] is True
    assert session._spec is None

    response = session.query([DimensionEquals("region", "North")], event="category:region:North")

    assert session._spec is None
    assert response["total_source_rows"] == 6
    assert response["views"]["kpis"]["order_count"] == 2


def test_layer_chart_html_contains_vega_embed():
    layer = _build_layer()

    html = layer.chart("orders.revenue", by="orders.created_at__month").line().to_html("vega-lite")

    assert "<!doctype html>" in html
    assert "vega-embed" in html
    assert "__SIDEMANTIC_CHART_READY__" in html
    assert "interaction-status" in html
    assert "orders.revenue" in html

    crossfilter_html = layer.chart(
        "orders.revenue",
        by=["orders.created_at__month", "orders.region"],
    ).to_html("crossfilter")
    assert "__SIDEMANTIC_CROSSFILTER__" in crossfilter_html
    assert "cf-summary" in crossfilter_html
    assert "metricRange" in crossfilter_html
    assert "cf-scatter" in crossfilter_html
    assert "paddedDomain" in crossfilter_html
    assert "brushPad" in crossfilter_html
    assert "cf-active-filters" in crossfilter_html
    assert "cf-filter-pill" in crossfilter_html
    assert "clearFilter" in crossfilter_html
    assert "refreshFromServer" in crossfilter_html
    assert "sidemanticCrossfilterMode" in crossfilter_html
    assert "sidemanticCrossfilterSqlHasHaving" in crossfilter_html
    assert "Interaction preagg" in crossfilter_html
    assert "activatePreagg" in crossfilter_html
    assert "cf-preagg-toggle" in crossfilter_html
    assert "interaction_preaggregations" in crossfilter_html
    assert "chart_renderer" in crossfilter_html
    assert "chart_renderer_options" in crossfilter_html
    assert "cf-renderers" in crossfilter_html
    assert "cf-renderer-button" in crossfilter_html
    assert "Vega-Lite" in crossfilter_html
    assert "Plotly" in crossfilter_html
    assert "Observable Plot" in crossfilter_html
    assert "changeRenderer" in crossfilter_html
    assert "renderVegaLiteLine" in crossfilter_html
    assert "renderPlotlyLine" in crossfilter_html
    assert "renderObservablePlotLine" in crossfilter_html
    assert "renderObservableXBrushLayer" in crossfilter_html
    assert "renderObservableMetricBrushLayer" in crossfilter_html
    assert "cf-selection-overlay" in crossfilter_html
    assert "renderXRangeOverlay" in crossfilter_html
    assert "domainSourceRows" in crossfilter_html
    assert "refreshDomainsFromLiveViews" in crossfilter_html
    assert "__SIDEMANTIC_CROSSFILTER_API__" in crossfilter_html


def test_chart_html_uses_vendored_renderer_assets():
    layer = _build_layer()
    chart = layer.chart("orders.revenue", by="orders.created_at__month").line()
    forbidden_loaders = [
        "<script src=",
        'src="https://',
        "src='https://",
        'type="module"',
        "import embed from",
        "cdn.jsdelivr",
        "esm.sh",
    ]

    for renderer, expected_vendors in [
        ("vega-lite", ["vega", "vega_lite", "vega_embed"]),
        ("plotly", ["plotly"]),
        ("observable-plot", ["d3", "observable_plot"]),
        ("d3", ["d3"]),
        ("crossfilter", ["d3", "vega", "vega_lite", "vega_embed", "plotly", "observable_plot"]),
    ]:
        html = chart.to_html(renderer)
        shell_html = _strip_vendored_script_bodies(html)
        for vendor in expected_vendors:
            assert f'data-sidemantic-vendor="{vendor}"' in html
        for forbidden in forbidden_loaders:
            assert forbidden not in shell_html


def test_layer_chart_crossfilter_session_api():
    layer = _build_layer()
    session = layer.chart(
        ["orders.revenue", "orders.order_count"],
        by=["orders.created_at__month", "orders.region"],
    ).crossfilter()

    spec = session.to_spec(query_endpoint="/query")
    assert spec["protocol"] == "sidemantic-crossfilter-v1"
    assert spec["query_endpoint"] == "/query"
    assert spec["chart_renderer"] == "d3"
    assert spec["chart_renderer_options"] == ["vega-lite", "plotly", "observable-plot", "d3"]

    plotly_session = layer.chart(
        ["orders.revenue", "orders.order_count"],
        by=["orders.created_at__month", "orders.region"],
    ).crossfilter(renderer="plotly")
    assert plotly_session.to_spec()["chart_renderer"] == "plotly"
    assert plotly_session.to_spec()["sidemantic"]["chart_renderer_options"] == [
        "vega-lite",
        "plotly",
        "observable-plot",
        "d3",
    ]

    response = session.query([DimensionEquals("orders.region", "North")])
    assert response["protocol"] == "sidemantic-crossfilter-v1"
    assert response["diagnostics"]["mode"] == "database"
    assert response["freshness"]["protocol"] == "sidemantic-freshness-v1"
    assert response["freshness"]["status"] == "direct"
    assert response["freshness"]["stale"] is False
    assert response["diagnostics"]["freshness"] == response["freshness"]
    assert "orders.region = 'North'" in response["filter_expressions"]
    assert "WHERE region = 'North'" in response["sql"]
    assert response["views"]["kpis"]["order_count"] == 2
    assert len(response["views"]["bars"]["region"]) == 3


def test_crossfilter_interaction_preagg_materializes_and_reuses_table():
    layer = _build_layer()
    session = layer.chart(
        ["orders.revenue", "orders.order_count"],
        by=["orders.created_at__month", "orders.region"],
    ).crossfilter(interaction_preaggregations=True)

    activated = session.query(event="activate", active={"type": "xRange", "field": "created_at"})

    assert activated["used_interaction_preagg"] is True
    preagg = activated["diagnostics"]["interaction_preagg"]
    assert preagg["used"] is True
    assert preagg["reused"] is False
    assert activated["freshness"]["status"] == "preaggregated"
    assert activated["freshness"]["stale"] is False
    assert activated["freshness"]["interaction_preagg"]["built_at"]
    assert preagg["table"]["table_name"].startswith("sidemantic_ipreagg_")
    assert preagg["table"]["built_at"]
    assert preagg["table"]["model_version"]
    assert preagg["table"]["row_count"] == 6
    assert f'FROM "{preagg["table"]["table_name"]}"' in activated["sql"]

    filtered = session.query([DimensionEquals("region", "North")], event="category:region:North")

    assert filtered["used_interaction_preagg"] is True
    reused = filtered["diagnostics"]["interaction_preagg"]
    assert reused["used"] is True
    assert reused["reused"] is True
    assert filtered["freshness"]["status"] == "preaggregated"
    assert filtered["freshness"]["stale"] is None
    assert "source watermark unavailable" in filtered["freshness"]["stale_reason"]
    assert reused["table"]["table_name"] == preagg["table"]["table_name"]
    assert '"region" = ' in filtered["sql"]
    assert "FROM orders" not in filtered["sql"]
    assert filtered["views"]["kpis"]["order_count"] == 2


def test_crossfilter_source_watermark_marks_reused_preagg_fresh_when_current():
    layer = _build_layer()
    session = layer.chart(
        ["orders.revenue", "orders.order_count"],
        by=["orders.created_at__month", "orders.region"],
    ).crossfilter(
        interaction_preaggregations=True,
        source_watermark_sql="SELECT MAX(created_at) FROM orders",
    )

    activated = session.query(event="activate", active={"type": "xRange", "field": "created_at"})
    filtered = session.query([DimensionEquals("region", "North")], event="category:region:North")

    assert activated["freshness"]["source_watermark"]["status"] == "available"
    assert activated["freshness"]["interaction_preagg"]["source_watermark"]["status"] == "available"
    assert filtered["used_interaction_preagg"] is True
    assert filtered["freshness"]["stale"] is False
    assert filtered["freshness"]["stale_reason"] is None
    assert filtered["freshness"]["source_watermark"]["value"] == "2024-03-29"
    assert filtered["freshness"]["policy"]["source_watermark_sql"] == "SELECT MAX(created_at) FROM orders"


def test_crossfilter_inherits_model_freshness_watermark_without_chart_sql():
    layer = SemanticLayer(connection="duckdb:///:memory:", auto_register=False)
    layer.adapter.execute("""
        CREATE TABLE orders (
            id INTEGER,
            updated_at DATE,
            region VARCHAR,
            amount DOUBLE
        )
    """)
    layer.adapter.execute("""
        INSERT INTO orders VALUES
            (1, DATE '2024-01-03', 'North', 120.0),
            (2, DATE '2024-03-29', 'South', 80.0)
    """)
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            freshness=Freshness(watermark="updated_at", ttl_seconds=86_400),
            dimensions=[
                Dimension(name="updated_at", type="time", granularity="day"),
                Dimension(name="region", type="categorical"),
            ],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        )
    )

    response = layer.chart("orders.revenue", by="orders.region").crossfilter().query(event="tab")

    assert response["freshness"]["policy"]["source"] == "model_freshness"
    assert response["freshness"]["policy"]["source_model"] == "orders"
    assert response["freshness"]["policy"]["watermark"] == "orders.updated_at"
    assert response["freshness"]["policy"]["ttl_seconds"] == 86_400
    assert response["freshness"]["source_watermark"]["status"] == "available"
    assert response["freshness"]["source_watermark"]["value"] == "2024-03-29"
    assert response["freshness"]["source_watermark"]["sql"] == "SELECT MAX(updated_at) FROM orders"


def test_crossfilter_infers_freshness_watermark_from_updated_at_dimension():
    layer = SemanticLayer(connection="duckdb:///:memory:", auto_register=False)
    layer.adapter.execute("""
        CREATE TABLE orders (
            id INTEGER,
            updated_at DATE,
            region VARCHAR,
            amount DOUBLE
        )
    """)
    layer.adapter.execute("""
        INSERT INTO orders VALUES
            (1, DATE '2024-01-03', 'North', 120.0),
            (2, DATE '2024-03-29', 'South', 80.0)
    """)
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[
                Dimension(name="updated_at", type="time", granularity="day"),
                Dimension(name="region", type="categorical"),
            ],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        )
    )

    response = layer.chart("orders.revenue", by="orders.region").crossfilter().query(event="tab")

    assert response["freshness"]["policy"]["source"] == "model_inferred_watermark"
    assert response["freshness"]["policy"]["watermark"] == "orders.updated_at"
    assert response["freshness"]["source_watermark"]["status"] == "available"
    assert response["freshness"]["source_watermark"]["value"] == "2024-03-29"


def test_crossfilter_freshness_ttl_marks_old_watermark_stale():
    layer = _build_layer()
    session = layer.chart("orders.revenue", by="orders.region").crossfilter(
        source_watermark_sql="SELECT DATE '2024-01-01'",
        freshness_ttl_seconds=1,
    )

    response = session.query(event="tab")

    assert response["freshness"]["source_watermark"]["status"] == "available"
    assert response["freshness"]["policy"]["ttl_seconds"] == 1
    assert response["freshness"]["stale"] is True
    assert "freshness TTL" in response["freshness"]["stale_reason"]


def test_crossfilter_freshness_null_watermark_is_unknown_not_fresh():
    layer = _build_layer()
    session = layer.chart("orders.revenue", by="orders.region").crossfilter(
        source_watermark_sql="SELECT NULL",
        freshness_ttl_seconds=60,
    )

    response = session.query(event="tab")

    assert response["freshness"]["source_watermark"]["status"] == "unavailable"
    assert response["freshness"]["source_watermark"]["error"] == "query returned NULL"
    assert response["freshness"]["data_as_of"] is None
    assert response["freshness"]["stale"] is None
    assert "source watermark unavailable" in response["freshness"]["stale_reason"]


def test_crossfilter_freshness_ttl_without_watermark_is_unknown_for_direct_data():
    layer = _build_layer()
    session = layer.chart("orders.revenue", by="orders.region").crossfilter(freshness_ttl_seconds=60)

    response = session.query(event="tab")

    assert response["freshness"]["source_watermark"]["status"] == "not_configured"
    assert response["freshness"]["data_as_of"] is None
    assert response["freshness"]["stale"] is None
    assert response["freshness"]["stale_reason"] == "freshness TTL could not be evaluated"


def test_freshness_datetime_normalizes_timezone_aware_values_to_utc():
    assert _freshness_datetime("2026-06-04T16:00:00-07:00").isoformat() == "2026-06-04T23:00:00+00:00"


def test_freshness_accepts_ttl_alias_and_rejects_unknown_fields():
    assert Freshness(ttlSeconds=60).ttl_seconds == 60
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        Freshness(watermark="updated_at", unexpected=True)


def test_crossfilter_interaction_preagg_reuses_persisted_table(tmp_path):
    db_path = tmp_path / "crossfilter.duckdb"
    layer = build_layer(
        connection=f"duckdb:///{db_path}",
        large_records=100,
        huge_records=None,
        massive_records=None,
        extreme_records=None,
    )
    session = _build_chart(layer, "orders", "Orders").crossfilter(interaction_preaggregations=True)

    built = session.ensure_interaction_preaggregation()
    table_name = built["table"]["table_name"]
    assert built["used"] is True
    assert built["reused"] is False
    assert built["table"]["row_count"] == 972
    layer.adapter.close()

    next_layer = build_layer(
        connection=f"duckdb:///{db_path}",
        large_records=100,
        huge_records=None,
        massive_records=None,
        extreme_records=None,
    )
    next_session = _build_chart(next_layer, "orders", "Orders").crossfilter(interaction_preaggregations=True)

    restored_query = next_session.query(event="tab")
    restored = restored_query["diagnostics"]["interaction_preagg"]

    assert restored_query["used_interaction_preagg"] is True
    assert restored["used"] is True
    assert restored["reused"] is True
    assert restored_query["freshness"]["status"] == "preaggregated"
    assert restored_query["freshness"]["interaction_preagg"]["built_at"]
    assert restored_query["freshness"]["stale"] is None
    assert restored["table"]["table_name"] == table_name
    assert restored["table"]["row_count"] == 972
    next_layer.adapter.close()


def test_crossfilter_interaction_preagg_rebuilds_schema_mismatched_table():
    layer = _build_layer()
    session = layer.chart(
        ["orders.revenue", "orders.order_count"],
        by=["orders.created_at__month", "orders.region"],
    ).crossfilter(interaction_preaggregations=True)
    cache = session._interaction_preagg_cache
    assert cache is not None
    candidate = cache._candidate_table()
    layer.adapter.execute(f'CREATE TABLE "{candidate["table_name"]}" AS SELECT 1 AS wrong_column')

    diagnostics = session.ensure_interaction_preaggregation()

    assert diagnostics["used"] is True
    assert diagnostics["reused"] is False
    assert diagnostics["table"]["row_count"] == 6
    result = layer.adapter.execute(f'SELECT * FROM "{candidate["table_name"]}" LIMIT 0')
    assert [desc[0] for desc in result.description] == ["created_at__month", "region", "revenue", "order_count"]


def test_crossfilter_interaction_preagg_falls_back_for_non_duckdb_dialect():
    layer = _build_layer()
    layer.dialect = "postgres"
    session = layer.chart(
        ["orders.revenue", "orders.order_count"],
        by=["orders.created_at__month", "orders.region"],
    ).crossfilter(interaction_preaggregations=True)

    diagnostics = session.ensure_interaction_preaggregation()

    assert diagnostics["used"] is False
    assert "DuckDB or MotherDuck" in diagnostics["reason"]


def test_crossfilter_interaction_preagg_can_be_disabled_via_session_toggle():
    layer = _build_layer()
    session = layer.chart(
        ["orders.revenue", "orders.order_count"],
        by=["orders.created_at__month", "orders.region"],
    ).crossfilter(interaction_preaggregations=True)

    session.query(event="activate", active={"type": "xRange", "field": "created_at"})
    disabled = session.query(
        [DimensionEquals("region", "North")],
        event="category:region:North",
        interaction_preaggregations=False,
    )

    assert disabled["used_interaction_preagg"] is False
    assert disabled["diagnostics"]["interaction_preagg"]["enabled"] is False
    assert disabled["diagnostics"]["interaction_preagg"]["reason"] == "disabled by session toggle"
    assert "FROM orders" in disabled["sql"]


def test_crossfilter_request_preagg_toggle_is_per_request():
    layer = _build_layer()
    session = layer.chart(
        ["orders.revenue", "orders.order_count"],
        by=["orders.created_at__month", "orders.region"],
    ).crossfilter(interaction_preaggregations=True)
    session.ensure_interaction_preaggregation()

    disabled = session.handle_request(
        {
            "event": "category:region:North",
            "interaction_preaggregations": False,
            "filters": [{"type": "category", "field": "region", "value": "North"}],
        }
    )
    enabled = session.handle_request(
        {
            "event": "category:region:North",
            "interaction_preaggregations": True,
            "filters": [{"type": "category", "field": "region", "value": "North"}],
        }
    )

    assert disabled["used_interaction_preagg"] is False
    assert enabled["used_interaction_preagg"] is True
    assert enabled["diagnostics"]["interaction_preagg"]["reused"] is True


def test_crossfilter_dashboard_routes_requests_and_emits_tabs(tmp_path):
    layer = _build_layer()
    standard = layer.chart(
        ["orders.revenue", "orders.order_count"],
        by=["orders.created_at__month", "orders.region"],
    ).crossfilter(interaction_preaggregations=True)
    comparison = layer.chart(
        ["orders.revenue", "orders.order_count"],
        by=["orders.created_at__month", "orders.region"],
    ).crossfilter(interaction_preaggregations=True)
    dashboard = CrossfilterDashboard(
        "Revenue Explorer",
        [
            CrossfilterTab("standard", "Standard", standard),
            CrossfilterTab("comparison", "Comparison", comparison),
        ],
    )

    spec = dashboard.to_spec()
    assert spec["renderer"] == "sidemantic-crossfilter-tabs"
    assert [tab["id"] for tab in spec["tabs"]] == ["standard", "comparison"]
    assert spec["tabs"][0]["query_endpoint"] == "/crossfilter/query"
    assert "cf-tabs" in dashboard.to_html()
    written = dashboard.write(tmp_path)
    assert written["html"].name == "crossfilter.html"
    assert written["spec"].name == "crossfilter.json"
    assert "sidemantic-crossfilter-tabs" in written["spec"].read_text()

    response = dashboard.handle_request(
        {
            "tab": "comparison",
            "event": "category:region:North",
            "filters": [{"type": "category", "field": "region", "value": "North"}],
        }
    )

    assert response["used_interaction_preagg"] is True
    assert response["views"]["kpis"]["order_count"] == 2

    with pytest.raises(ValueError, match="Unknown crossfilter tab"):
        dashboard.handle_request({"tab": "missing", "filters": []})


def test_crossfilter_dashboard_serve_uses_lazy_specs(monkeypatch, tmp_path):
    layer = _build_layer()
    first = layer.chart(
        ["orders.revenue", "orders.order_count"],
        by=["orders.created_at__month", "orders.region"],
    ).crossfilter(renderer="vega-lite")
    second = layer.chart(
        ["orders.revenue", "orders.order_count"],
        by=["orders.created_at__month", "orders.region"],
    ).crossfilter(renderer="vega-lite")

    def fail_if_eager(*args, **kwargs):
        raise AssertionError("non-initial tab spec should not be materialized for the shell")

    monkeypatch.setattr(second, "to_spec", fail_if_eager)
    monkeypatch.setattr(second.chart, "data", fail_if_eager)

    dashboard = CrossfilterDashboard(
        "Lazy Serve",
        [
            CrossfilterTab("first", "First", first, source_record_count=6),
            CrossfilterTab("second", "Second", second, source_record_count=6),
        ],
    )
    captured = {}

    class FakeServer:
        daemon_threads = False

        def __init__(self, server_address, handler_cls):
            captured["server_address"] = server_address
            captured["handler_cls"] = handler_cls

        def serve_forever(self):
            captured["served"] = True

    monkeypatch.setattr("http.server.ThreadingHTTPServer", FakeServer)

    dashboard.serve(tmp_path, port=0)

    shell = json.loads((tmp_path / "crossfilter.json").read_text())
    assert "spec" in shell["tabs"][0]
    assert "spec" not in shell["tabs"][1]
    assert shell["tabs"][1]["spec_endpoint"] == "/crossfilter/spec?tab=second"
    assert captured["served"] is True

    handler = captured["handler_cls"].__new__(captured["handler_cls"])
    handler.path = "/crossfilter/spec?tab=second"
    handler.wfile = io.BytesIO()
    handler.send_response = lambda status: captured.setdefault("status", status)
    handler.send_header = lambda key, value: captured.setdefault("headers", []).append((key, value))
    handler.end_headers = lambda: None

    handler.do_GET()

    tab_payload = json.loads(handler.wfile.getvalue())
    assert captured["status"] == 200
    assert tab_payload["id"] == "second"
    assert tab_payload["spec"]["data_deferred"] is True
    assert tab_payload["spec"]["data"] == []


def test_crossfilter_rejects_unknown_filter_fields():
    layer = _build_layer()
    session = layer.chart(
        ["orders.revenue", "orders.order_count"],
        by=["orders.created_at__month", "orders.region"],
    ).crossfilter()

    with pytest.raises(ValueError, match="Unknown crossfilter dimension field"):
        session.query([DimensionEquals("not_a_dimension", "North")])

    with pytest.raises(ValueError, match="Unknown crossfilter metric field"):
        session.query([MetricRange("not_a_metric", 0, 10)])


def test_crossfilter_session_can_warm_preaggregations_explicitly():
    layer = _build_layer()
    session = layer.chart(
        ["orders.revenue", "orders.order_count"],
        by=["orders.created_at__month", "orders.region"],
    ).crossfilter(interaction_preaggregations=True)

    diagnostics = session.ensure_interaction_preaggregation()
    response = session.query([DimensionEquals("region", "North")], event="category:region:North")

    assert diagnostics["used"] is True
    assert diagnostics["reused"] is False
    assert response["diagnostics"]["interaction_preagg"]["reused"] is True


def test_crossfilter_interaction_preagg_is_model_agnostic():
    layer = SemanticLayer(connection="duckdb:///:memory:", auto_register=False)
    layer.adapter.execute("""
        CREATE TABLE tickets (
            id INTEGER,
            opened_at DATE,
            priority VARCHAR,
            team VARCHAR,
            handle_minutes DOUBLE
        )
    """)
    layer.adapter.execute("""
        INSERT INTO tickets VALUES
            (1, DATE '2024-01-03', 'High', 'Support', 20.0),
            (2, DATE '2024-01-18', 'Low', 'Support', 8.0),
            (3, DATE '2024-02-02', 'High', 'Success', 13.0),
            (4, DATE '2024-02-21', 'Medium', 'Success', 16.0)
    """)
    layer.add_model(
        Model(
            name="tickets",
            table="tickets",
            primary_key="id",
            dimensions=[
                Dimension(name="opened_at", type="time", granularity="day"),
                Dimension(name="priority", type="categorical"),
                Dimension(name="team", type="categorical"),
            ],
            metrics=[
                Metric(name="ticket_count", agg="count"),
                Metric(name="handle_minutes", agg="sum", sql="handle_minutes"),
            ],
        )
    )
    session = layer.chart(
        ["tickets.ticket_count", "tickets.handle_minutes"],
        by=["tickets.opened_at__month", "tickets.priority", "tickets.team"],
    ).crossfilter(interaction_preaggregations=True)

    response = session.query([DimensionEquals("priority", "High")], event="category:priority:High")

    assert response["used_interaction_preagg"] is True
    preagg = response["diagnostics"]["interaction_preagg"]["table"]
    assert preagg["model"] == "tickets"
    assert preagg["dimensions"] == ["tickets.opened_at__month", "tickets.priority", "tickets.team"]
    assert "orders" not in response["sql"]
    assert f'FROM "{preagg["table_name"]}"' in response["sql"]
    assert response["views"]["kpis"]["ticket_count"] == 2


def test_crossfilter_interaction_preagg_falls_back_for_unsupported_metric():
    layer = _build_layer()
    model = layer.get_model("orders")
    model.metrics.append(Metric(name="avg_revenue", agg="avg", sql="amount"))
    session = layer.chart(
        ["orders.avg_revenue"],
        by=["orders.created_at__month", "orders.region"],
    ).crossfilter(interaction_preaggregations=True)

    response = session.query([DimensionEquals("region", "North")], event="category:region:North")

    assert response["used_interaction_preagg"] is False
    assert "unsupported aggregate 'avg'" in response["diagnostics"]["interaction_preagg"]["reason"]
    assert "FROM orders" in response["sql"]


def test_crossfilter_lazy_metadata_preserves_unfiltered_group_totals():
    layer = _build_layer()
    session = layer.chart(
        ["orders.revenue", "orders.order_count"],
        by=["orders.created_at__month", "orders.region"],
    ).crossfilter()
    metadata = session.to_metadata_spec()

    response = session.query([DimensionEquals("region", "North")], event="category:region:North")

    assert metadata["data_deferred"] is True
    assert response["row_count"] == 2
    assert response["total_groups"] == 6
    assert session.source_group_count == 6


def test_crossfilter_runtime_discards_stale_tab_responses():
    layer = _build_layer()
    html = (
        layer.chart(
            ["orders.revenue", "orders.order_count"],
            by=["orders.created_at__month", "orders.region"],
        )
        .crossfilter()
        .to_html()
    )

    assert "pendingRequestId += 1;" in html
    assert "const requestedTabId = currentTabId();" in html
    assert "requestedTabId !== currentTabId()" in html


def test_crossfilter_runtime_disables_xrange_requests_for_metric_x_fields():
    layer = _build_layer()
    html = layer.chart("orders.revenue").crossfilter().to_html()

    assert "function xRangeIsSemanticDimension()" in html
    assert "return (fields?.dimensions || []).includes(xField);" in html
    assert "if (type === 'xRange' && !xRangeIsSemanticDimension()) return;" in html
    assert "return xRangeIsSemanticDimension() ? { type: 'xRange', field: xField } : null;" in html
    assert "if (state.xRange && xRangeIsSemanticDimension())" in html


def test_crossfilter_fallback_recomputes_avg_metrics_at_target_grain():
    layer = SemanticLayer(connection="duckdb:///:memory:", auto_register=False)
    layer.adapter.execute("""
        CREATE TABLE orders (
            id INTEGER,
            created_at DATE,
            region VARCHAR,
            amount DOUBLE
        )
    """)
    layer.adapter.execute("""
        INSERT INTO orders VALUES
            (1, DATE '2024-01-03', 'North', 100.0),
            (2, DATE '2024-01-08', 'North', 200.0),
            (3, DATE '2024-01-12', 'South', 1000.0)
    """)
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[
                Dimension(name="created_at", type="time", granularity="day"),
                Dimension(name="region", type="categorical"),
            ],
            metrics=[
                Metric(name="avg_revenue", agg="avg", sql="amount"),
                Metric(name="order_count", agg="count"),
            ],
        )
    )
    session = layer.chart(
        ["orders.avg_revenue", "orders.order_count"],
        by=["orders.created_at__month", "orders.region"],
    ).crossfilter()

    response = session.query()

    assert response["views"]["kpis"]["avg_revenue"] == pytest.approx(1300.0 / 3.0)
    assert "WITH filtered_groups" not in response["sqls"]["kpis"]


@pytest.mark.parametrize(
    ("grain", "minimum", "maximum", "lower_expr", "upper_expr"),
    [
        (
            "second",
            "2024-01-03T00:00:00",
            "2024-01-03T00:00:00",
            "orders.created_at >= TIMESTAMP '2024-01-03 00:00:00'",
            "orders.created_at < TIMESTAMP '2024-01-03 00:00:01'",
        ),
        (
            "minute",
            "2024-01-03T00:00:00",
            "2024-01-03T00:00:00",
            "orders.created_at >= TIMESTAMP '2024-01-03 00:00:00'",
            "orders.created_at < TIMESTAMP '2024-01-03 00:01:00'",
        ),
        (
            "hour",
            "2024-01-03T00:00:00",
            "2024-01-03T00:00:00",
            "orders.created_at >= TIMESTAMP '2024-01-03 00:00:00'",
            "orders.created_at < TIMESTAMP '2024-01-03 01:00:00'",
        ),
        (
            "day",
            "2024-01-03",
            "2024-01-03",
            "orders.created_at >= DATE '2024-01-03'",
            "orders.created_at < DATE '2024-01-04'",
        ),
        (
            "week",
            "2024-01-01",
            "2024-01-01",
            "orders.created_at >= DATE '2024-01-01'",
            "orders.created_at < DATE '2024-01-08'",
        ),
        (
            "month",
            "2024-01-01",
            "2024-01-01",
            "orders.created_at >= DATE '2024-01-01'",
            "orders.created_at < DATE '2024-02-01'",
        ),
        (
            "quarter",
            "2024-01-01",
            "2024-01-01",
            "orders.created_at >= DATE '2024-01-01'",
            "orders.created_at < DATE '2024-04-01'",
        ),
        (
            "year",
            "2024-01-01",
            "2024-01-01",
            "orders.created_at >= DATE '2024-01-01'",
            "orders.created_at < DATE '2025-01-01'",
        ),
    ],
)
def test_crossfilter_live_time_range_filters_supported_time_grains(grain, minimum, maximum, lower_expr, upper_expr):
    layer = _build_layer()
    session = layer.chart(
        ["orders.revenue", "orders.order_count"],
        by=[f"orders.created_at__{grain}", "orders.region"],
    ).crossfilter()

    response = session.query([TimeRange(f"created_at__{grain}", minimum, maximum)], event="xRange")

    assert lower_expr in response["filter_expressions"]
    assert upper_expr in response["filter_expressions"]
    assert not any(f"orders.created_at__{grain}" in expr for expr in response["filter_expressions"])
    assert response["row_count"] > 0


def test_chart_library_outputs_json():
    layer = _build_layer()
    chart = layer.chart("orders.revenue", by="orders.created_at__month").line().interactive()

    payload = json.loads(json.dumps(chart.to_renderer("vega-lite")))
    assert payload["mark"]["type"] == "line"
    assert payload["params"][0]["name"] == "brush"
    assert len(payload["data"]["values"]) == 3


def test_chart_library_renders_html():
    layer = _build_layer()
    html = layer.chart("orders.revenue", by="orders.region").bar().to_html("vega-lite")

    assert "vega-embed" in html
    assert "Revenue by Region" in html


def test_crossfilter_example_pushes_filters_to_semantic_sql():
    layer = build_layer(large_records=1_000)
    chart = _build_chart(layer, "orders_200k", "Revenue Performance Explorer")
    session = chart.crossfilter()

    response = session.query(
        [
            DimensionEquals("channel", "Sales"),
            TimeRange("created_at__month", "2023-06-01", "2024-03-01"),
            MetricRange("order_count", 0, 999_999, "revenue", 0, 999_999_999),
        ],
        event="test",
    )

    assert "orders_200k.channel = 'Sales'" in response["filter_expressions"]
    assert "orders_200k.created_at >= DATE '2023-06-01'" in response["filter_expressions"]
    assert "orders_200k.revenue <= 999999999.0" in response["filter_expressions"]
    assert "WHERE channel = 'Sales'" in response["sql"]
    assert "HAVING" in response["sql"]
    assert response["row_count"] > 0
    assert response["views"]["kpis"]["order_count"] > 0
    assert response["views"]["kpis"]["revenue"] == response["views"]["kpis"]["revenue"]
    assert len(response["views"]["bars"]["region"]) > 0
    assert "filtered_groups" not in response["sqls"]["kpis"]
    assert "filtered_groups" not in response["sqls"]["trend"]
    assert response["diagnostics"]["has_where"] is True
    assert response["diagnostics"]["has_having"] is True


def test_crossfilter_example_supports_huge_semantic_model():
    layer = build_layer(large_records=1_000, huge_records=2_000)
    chart = _build_chart(layer, "orders_2m", "Revenue Performance Explorer")
    session = chart.crossfilter()

    response = session.query([DimensionEquals("channel", "Sales")], event="huge-test")

    assert session.source_record_count == 2_000
    assert "orders_2m.channel = 'Sales'" in response["filter_expressions"]
    assert "FROM orders_2m" in response["sql"]
    assert "WHERE channel = 'Sales'" in response["sql"]
    assert response["diagnostics"]["has_where"] is True
    assert response["views"]["kpis"]["order_count"] > 0


def test_crossfilter_example_supports_massive_semantic_model():
    layer = build_layer(large_records=1_000, massive_records=3_000)
    chart = _build_chart(layer, "orders_20m", "Revenue Performance Explorer")
    session = chart.crossfilter(interaction_preaggregations=True)

    response = session.query([DimensionEquals("region", "Central")], event="massive-test")

    assert session.source_record_count == 3_000
    assert "orders_20m.region = 'Central'" in response["filter_expressions"]
    assert response["diagnostics"]["used_interaction_preagg"] is True
    assert response["diagnostics"]["interaction_preagg"]["table"]["model"] == "orders_20m"
    assert response["views"]["kpis"]["order_count"] > 0


def test_crossfilter_preagg_empty_selection_keeps_kpi_fields():
    layer = build_layer(large_records=1_000, massive_records=3_000)
    chart = _build_chart(layer, "orders_20m", "Revenue Performance Explorer")
    session = chart.crossfilter(interaction_preaggregations=True)

    response = session.query([DimensionEquals("region", "__no_such_region__")], event="empty-test")

    assert response["diagnostics"]["used_interaction_preagg"] is True
    kpis = response["views"]["kpis"]
    # Deriving KPIs from an empty grid must still emit every metric field (NULL),
    # matching the aggregate-query path so the dashboard keeps stable KPI tiles.
    assert "order_count" in kpis
    assert kpis["order_count"] is None


def test_crossfilter_preagg_kpis_total_full_grid_despite_limit():
    layer = _build_layer()
    metrics = ["orders.revenue", "orders.order_count"]
    by = ["orders.created_at__month", "orders.region"]
    limited = layer.chart(metrics, by=by, limit=1).crossfilter(interaction_preaggregations=True)
    unlimited = layer.chart(metrics, by=by).crossfilter(interaction_preaggregations=True)

    # North spans two month groups; a filter + event makes the session build the preagg.
    selection = [DimensionEquals("region", "North")]
    limited_resp = limited.query(selection, event="limit-test")
    unlimited_resp = unlimited.query(selection, event="limit-test")

    assert limited_resp["diagnostics"]["used_interaction_preagg"] is True
    # LIMIT pages `current` to one group, but KPI totals must still reflect the
    # full filtered grid, not the single returned page.
    assert len(limited_resp["views"]["table"]) == 1
    assert limited_resp["views"]["kpis"]["order_count"] == unlimited_resp["views"]["kpis"]["order_count"] == 2


def test_crossfilter_example_supports_extreme_semantic_model():
    layer = build_layer(large_records=1_000, extreme_records=4_000)
    chart = _build_chart(layer, "orders_100m", "Revenue Performance Explorer")
    session = chart.crossfilter(interaction_preaggregations=True)

    response = session.query([DimensionEquals("channel", "Marketplace")], event="extreme-test")

    assert session.source_record_count == 4_000
    assert "orders_100m.channel = 'Marketplace'" in response["filter_expressions"]
    assert response["diagnostics"]["used_interaction_preagg"] is True
    assert response["diagnostics"]["interaction_preagg"]["table"]["model"] == "orders_100m"
    assert response["views"]["kpis"]["order_count"] > 0
