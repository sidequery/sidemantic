import json
from collections.abc import Iterator
from pathlib import Path

import pytest
from playwright.sync_api import Browser, Page, expect, sync_playwright

from sidemantic import DashboardDocument, Dimension, Metric, Model, SemanticLayer


def _build_layer() -> SemanticLayer:
    layer = SemanticLayer(connection="duckdb:///:memory:", auto_register=False)
    layer.adapter.execute("""
        CREATE TABLE orders (
            id INTEGER,
            created_at DATE,
            region VARCHAR,
            channel VARCHAR,
            amount DOUBLE
        )
    """)
    layer.adapter.execute("""
        INSERT INTO orders VALUES
            (1, DATE '2024-01-03', 'North', 'Sales', 120.0),
            (2, DATE '2024-01-18', 'South', 'Partner', 80.0),
            (3, DATE '2024-02-02', 'North', 'Sales', 140.0),
            (4, DATE '2024-02-21', 'West', 'Marketplace', 170.0),
            (5, DATE '2024-03-11', 'South', 'Partner', 110.0),
            (6, DATE '2024-03-29', 'West', 'Marketplace', 190.0)
    """)
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[
                Dimension(name="created_at", type="time", granularity="day"),
                Dimension(name="region", type="categorical"),
                Dimension(name="channel", type="categorical"),
            ],
            metrics=[
                Metric(name="revenue", agg="sum", sql="amount"),
                Metric(name="order_count", agg="count"),
            ],
        )
    )
    return layer


@pytest.fixture(scope="session")
def browser() -> Iterator[Browser]:
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        yield browser
        browser.close()


@pytest.fixture
def browser_page(browser: Browser) -> Iterator[tuple[Page, list[str]]]:
    page = browser.new_page(viewport={"width": 1280, "height": 900})
    errors: list[str] = []
    page.on("pageerror", lambda error: errors.append(f"pageerror: {error}"))
    page.on(
        "console", lambda message: errors.append(f"console error: {message.text}") if message.type == "error" else None
    )

    def block_unexpected_network(route, request):
        if request.url.startswith(("http://", "https://")) and not request.url.startswith("https://sidemantic.test/"):
            errors.append(f"unexpected external network request: {request.url}")
            route.abort()
            return
        route.fallback()

    page.route("**/*", block_unexpected_network)
    yield page, errors
    page.close()


def _write_html(tmp_path: Path, html: str, name: str) -> Path:
    path = tmp_path / name
    path.write_text(html, encoding="utf-8")
    return path


@pytest.mark.parametrize(
    ("renderer", "selector"),
    [
        ("vega-lite", "#chart canvas, #chart svg"),
        ("plotly", "#chart.js-plotly-plot, #chart .js-plotly-plot"),
        ("observable-plot", "#chart svg"),
        ("d3", "svg#chart path, svg#chart circle"),
    ],
)
def test_chart_html_renderer_integrations_render_in_browser(
    browser_page: tuple[Page, list[str]],
    tmp_path: Path,
    renderer: str,
    selector: str,
):
    page, errors = browser_page
    chart = (
        _build_layer()
        .chart(
            ["orders.revenue", "orders.order_count"],
            by=["orders.created_at__month", "orders.region"],
            title="Monthly Revenue by Region",
        )
        .line()
        .brush("x")
    )
    html_path = _write_html(tmp_path, chart.to_html(renderer), f"{renderer}.html")

    page.goto(html_path.as_uri(), wait_until="networkidle", timeout=90_000)
    page.wait_for_function("window.__SIDEMANTIC_CHART_READY__ === true", timeout=60_000)

    expect(page.locator(selector).first).to_be_visible(timeout=20_000)
    assert page.evaluate("document.documentElement.dataset.sidemanticReady") == "true"
    assert errors == []


def test_crossfilter_chart_renderer_integrations_switch_in_browser(
    browser_page: tuple[Page, list[str]],
    tmp_path: Path,
):
    page, errors = browser_page
    session = (
        _build_layer()
        .chart(
            ["orders.revenue", "orders.order_count"],
            by=["orders.created_at__month", "orders.region"],
            title="Crossfilter Revenue",
        )
        .line()
        .crossfilter(renderer="d3")
    )
    html_path = _write_html(tmp_path, session.to_html(query_endpoint=None), "crossfilter-renderers.html")

    page.goto(html_path.as_uri(), wait_until="networkidle", timeout=90_000)
    page.wait_for_function("window.__SIDEMANTIC_CROSSFILTER__?.renderer === 'd3'", timeout=60_000)

    renderers = [
        ("d3", "D3", "#cf-line svg path"),
        ("vega-lite", "Vega-Lite", "#cf-line canvas, #cf-line svg"),
        ("plotly", "Plotly", "#cf-line.js-plotly-plot, #cf-line .js-plotly-plot"),
        ("observable-plot", "Observable Plot", "#cf-line svg"),
    ]
    for renderer, label, selector in renderers:
        page.get_by_role("button", name=label).click()
        page.wait_for_function(
            "renderer => window.__SIDEMANTIC_CROSSFILTER__?.renderer === renderer",
            arg=renderer,
            timeout=60_000,
        )
        expect(page.locator(selector).first).to_be_visible(timeout=20_000)

    assert page.evaluate("document.documentElement.dataset.sidemanticCrossfilterFilters") == "0"
    assert errors == []


def test_crossfilter_single_dimension_line_renders_one_series_in_browser(
    browser_page: tuple[Page, list[str]],
    tmp_path: Path,
):
    page, errors = browser_page
    session = (
        _build_layer()
        .chart("orders.revenue", by="orders.created_at__month", title="Monthly Revenue")
        .line()
        .crossfilter(renderer="d3")
    )
    html_path = _write_html(tmp_path, session.to_html(query_endpoint=None), "single-dimension-crossfilter.html")

    page.goto(html_path.as_uri(), wait_until="networkidle", timeout=90_000)
    page.wait_for_function("window.__SIDEMANTIC_CROSSFILTER__?.renderer === 'd3'", timeout=60_000)

    line_paths = page.locator("#cf-line svg path[stroke-width='2.5']")
    expect(line_paths.first).to_be_visible(timeout=20_000)
    assert line_paths.count() == 1
    assert (line_paths.first.get_attribute("d") or "").count("L") >= 1
    assert errors == []


def test_crossfilter_live_endpoint_api_queries_in_browser(
    browser_page: tuple[Page, list[str]],
    tmp_path: Path,
):
    page, errors = browser_page
    session = (
        _build_layer()
        .chart(
            ["orders.revenue", "orders.order_count"],
            by=["orders.created_at__month", "orders.region"],
            title="Live Crossfilter Revenue",
        )
        .line()
        .crossfilter(renderer="d3")
    )
    endpoint = "https://sidemantic.test/crossfilter/query"
    requests: list[dict[str, object]] = []

    def handle_query(route, request):
        payload = request.post_data_json
        requests.append(payload)
        response = session.handle_request(payload)
        route.fulfill(status=200, content_type="application/json", body=json.dumps(response, default=str))

    page.route(endpoint, handle_query)
    html_path = _write_html(tmp_path, session.to_html(query_endpoint=endpoint), "live-crossfilter.html")

    page.goto(html_path.as_uri(), wait_until="networkidle", timeout=90_000)
    page.wait_for_function("window.__SIDEMANTIC_CROSSFILTER_API__ != null", timeout=60_000)
    response = page.evaluate(
        """async () => window.__SIDEMANTIC_CROSSFILTER_API__.query(
            [{ type: 'category', field: 'region', value: 'North' }],
            'category:region:North',
            { type: 'category', field: 'region', value: 'North' }
        )"""
    )

    assert requests == [
        {
            "tab": "default",
            "event": "category:region:North",
            "active": {"type": "category", "field": "region", "value": "North"},
            "filters": [{"type": "category", "field": "region", "value": "North"}],
            "interaction_preaggregations": False,
        }
    ]
    assert response["updated_at"]
    assert response["views"]["kpis"]["revenue"] == 260.0
    assert response["views"]["kpis"]["order_count"] == 2
    assert errors == []


@pytest.mark.parametrize("renderer", ["d3", "vega-lite", "plotly", "observable-plot"])
def test_crossfilter_live_endpoint_filters_update_each_renderer_in_browser(
    browser_page: tuple[Page, list[str]],
    tmp_path: Path,
    renderer: str,
):
    page, errors = browser_page
    session = (
        _build_layer()
        .chart(
            ["orders.revenue", "orders.order_count"],
            by=["orders.created_at__month", "orders.region"],
            title="Limited Live Crossfilter Revenue",
            limit=4,
        )
        .line()
        .crossfilter(renderer=renderer, table_limit=1)
    )
    endpoint = f"https://sidemantic.test/{renderer}/crossfilter/query"
    requests: list[dict[str, object]] = []

    def handle_query(route, request):
        payload = request.post_data_json
        requests.append(payload)
        response = session.handle_request(payload)
        route.fulfill(status=200, content_type="application/json", body=json.dumps(response, default=str))

    page.route(endpoint, handle_query)
    html_path = _write_html(tmp_path, session.to_html(query_endpoint=endpoint), f"live-{renderer}.html")

    page.goto(html_path.as_uri(), wait_until="networkidle", timeout=90_000)
    page.wait_for_function(
        "renderer => window.__SIDEMANTIC_CROSSFILTER__?.renderer === renderer",
        arg=renderer,
        timeout=60_000,
    )
    page.evaluate("window.__SIDEMANTIC_CROSSFILTER_API__.setXRange('2024-01-01', '2024-02-01')")
    page.wait_for_function(
        "document.documentElement.dataset.sidemanticCrossfilterSemanticFilters === '2'",
        timeout=60_000,
    )

    assert requests[-1]["filters"] == [
        {"type": "xRange", "field": "created_at__month", "min": "2024-01-01", "max": "2024-02-01"}
    ]
    assert requests[-1]["interaction_preaggregations"] is False
    assert page.evaluate("window.__SIDEMANTIC_CROSSFILTER__.totalRows") == 4
    assert page.evaluate("window.__SIDEMANTIC_CROSSFILTER__.freshness.status") == "direct"
    assert page.evaluate("window.__SIDEMANTIC_CROSSFILTER__.freshness.source_watermark.status") == ("not_configured")
    assert page.evaluate("window.__SIDEMANTIC_CROSSFILTER_API__.freshnessPolicy.protocol") == (
        "sidemantic-freshness-policy-v1"
    )
    assert page.evaluate("window.__SIDEMANTIC_CROSSFILTER__.fieldPlan.aliases.region") == "orders.region"
    assert page.evaluate("window.__SIDEMANTIC_CROSSFILTER__.interactionPlan.protocol") == (
        "sidemantic-interaction-plan-v1"
    )
    assert page.locator("#cf-table tbody tr").count() == 1
    assert page.locator("#cf-dataset-meta").inner_text().count("Updated") == 1
    assert page.evaluate("document.documentElement.dataset.sidemanticCrossfilterUpdatedAt") != ""
    assert page.evaluate("document.documentElement.dataset.sidemanticCrossfilterFreshnessStatus") == "direct"
    assert page.evaluate("document.documentElement.dataset.sidemanticCrossfilterSourceWatermarkStatus") == (
        "not_configured"
    )
    assert errors == []


def test_dashboard_select_fields_drive_browser_breakdowns(
    browser_page: tuple[Page, list[str]],
    tmp_path: Path,
):
    page, errors = browser_page
    layer = _build_layer()
    dashboard = DashboardDocument.from_dict(
        {
            "schema": "sidemantic.dashboard.v1",
            "title": "Channel Explorer",
            "tabs": [
                {
                    "id": "overview",
                    "charts": [
                        {
                            "id": "revenue",
                            "type": "line",
                            "query": {
                                "metrics": ["orders.revenue", "orders.order_count"],
                                "dimensions": [
                                    "orders.created_at__month",
                                    "orders.region",
                                    "orders.channel",
                                ],
                            },
                            "encoding": {
                                "x": "orders.created_at__month",
                                "y": "orders.revenue",
                                "color": "orders.region",
                            },
                            "interactions": {
                                "brush": {"fields": ["orders.created_at__month"], "channel": "x"},
                                "select": {"fields": ["orders.channel"]},
                            },
                        }
                    ],
                }
            ],
        }
    ).to_crossfilter_dashboard(layer)
    html_path = _write_html(tmp_path, dashboard.to_html(), "dashboard-select-fields.html")

    page.goto(html_path.as_uri(), wait_until="networkidle", timeout=90_000)
    page.wait_for_function("window.__SIDEMANTIC_CROSSFILTER__?.renderer === 'd3'", timeout=60_000)

    expect(page.locator("#cf-bars > div[data-field='channel']")).to_be_visible(timeout=20_000)
    assert page.locator("#cf-bars > div[data-field='region']").count() == 0
    assert page.evaluate("window.__SIDEMANTIC_CROSSFILTER_API__.spec.interactions.select.fields") == ["channel"]
    assert page.evaluate("window.__SIDEMANTIC_CROSSFILTER_API__.interactionPlan.select.fields[0].id") == (
        "orders.channel"
    )
    assert errors == []
