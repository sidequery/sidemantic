from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from sidemantic import (
    DashboardDocument,
    Dimension,
    Freshness,
    Metric,
    Model,
    Relationship,
    SemanticLayer,
    load_from_directory,
)
from sidemantic.cli import app
from sidemantic.dashboard import DashboardSpecError, generate_dashboard_typescript
from sidemantic.viz import CrossfilterDashboard, CrossfilterTab

runner = CliRunner()


def _build_layer() -> SemanticLayer:
    layer = SemanticLayer(connection="duckdb:///:memory:", auto_register=False)
    layer.adapter.execute("""
        CREATE TABLE orders (
            id INTEGER,
            created_at DATE,
            region VARCHAR,
            status VARCHAR,
            amount DOUBLE
        )
    """)
    layer.adapter.execute("""
        INSERT INTO orders VALUES
            (1, DATE '2024-01-03', 'North', 'completed', 120.0),
            (2, DATE '2024-02-03', 'South', 'pending', 80.0),
            (3, DATE '2024-02-13', 'North', 'completed', 140.0)
    """)
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[
                Dimension(name="created_at", type="time", granularity="day"),
                Dimension(name="region", type="categorical"),
                Dimension(name="status", type="categorical"),
            ],
            metrics=[
                Metric(name="revenue", agg="sum", sql="amount"),
                Metric(name="order_count", agg="count"),
            ],
        )
    )
    return layer


def _dashboard_payload() -> dict:
    return {
        "schema": "sidemantic.dashboard.v1",
        "title": "Revenue Explorer",
        "defaults": {"renderer": "vega-lite", "query": {"interaction_preaggregations": True}},
        "tabs": [
            {
                "id": "overview",
                "label": "Overview",
                "charts": [
                    {
                        "id": "revenue_trend",
                        "title": "Revenue Trend",
                        "type": "line",
                        "query": {
                            "metrics": ["orders.revenue", "orders.order_count"],
                            "dimensions": ["orders.created_at__month", "orders.region", "orders.status"],
                            "order_by": ["orders.created_at__month"],
                        },
                        "encoding": {
                            "x": "orders.created_at__month",
                            "y": "orders.revenue",
                            "color": "orders.region",
                        },
                        "interactions": {
                            "brush": {"fields": ["orders.created_at__month"], "channel": "x"},
                            "select": {"fields": ["orders.region", "orders.status"]},
                        },
                    }
                ],
            }
        ],
    }


def test_dashboard_document_builds_crossfilter_dashboard():
    layer = _build_layer()
    document = DashboardDocument.from_dict(_dashboard_payload())

    assert document.validate(layer) == []

    dashboard = document.to_crossfilter_dashboard(layer)
    spec = dashboard.to_spec()

    assert spec["renderer"] == "sidemantic-crossfilter-tabs"
    assert spec["tabs"][0]["id"] == "overview"
    assert spec["tabs"][0]["spec"]["chart_renderer"] == "vega-lite"
    assert dashboard.tabs[0].session.interaction_preaggregations is True
    assert dashboard.tabs[0].session.chart_renderer == "vega-lite"
    assert dashboard.tabs[0].session.chart.dimensions[:2] == ["orders.created_at__month", "orders.region"]
    assert spec["tabs"][0]["spec"]["interactions"]["select"]["fields"] == ["region", "status"]
    assert spec["tabs"][0]["spec"]["interaction_plan"]["select"]["fields"][0]["id"] == "orders.region"
    assert spec["tabs"][0]["spec"]["field_plan"]["aliases"]["status"] == "orders.status"


def test_dashboard_document_honors_encoding_y_as_primary_metric():
    layer = _build_layer()
    payload = _dashboard_payload()
    payload["tabs"][0]["charts"][0]["encoding"]["y"] = "orders.order_count"

    dashboard = DashboardDocument.from_dict(payload).to_crossfilter_dashboard(layer)
    session = dashboard.tabs[0].session
    spec = dashboard.to_spec()["tabs"][0]["spec"]

    assert session.chart.metrics == ["orders.order_count", "orders.revenue"]
    assert spec["fields"]["metrics"][0] == "order_count"


def test_dashboard_chart_interaction_preagg_setting_overrides_default():
    layer = _build_layer()
    payload = _dashboard_payload()
    payload["defaults"]["query"]["interaction_preaggregations"] = True
    payload["tabs"][0]["charts"][0]["query"]["interaction_preaggregations"] = False

    dashboard = DashboardDocument.from_dict(payload).to_crossfilter_dashboard(layer)

    assert dashboard.tabs[0].session.interaction_preaggregations is False


def test_crossfilter_dashboard_asgi_app_routes_requests_without_eager_preagg():
    pytest.importorskip("fastapi")
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient

    layer = _build_layer()
    dashboard = DashboardDocument.from_dict(_dashboard_payload()).to_crossfilter_dashboard(layer)
    session = dashboard.tabs[0].session
    assert session.interaction_preagg_diagnostics is None
    assert session._interaction_preagg_cache is not None
    assert session._interaction_preagg_cache.info is None

    client = TestClient(dashboard.to_asgi_app())

    health = client.get("/readyz")
    assert health.status_code == 200
    assert health.json()["tabs"] == ["overview"]

    html = client.get("/crossfilter.html")
    assert html.status_code == 200
    assert "__SIDEMANTIC_CROSSFILTER__" in html.text

    spec = client.get("/crossfilter.json")
    assert spec.status_code == 200
    assert spec.json()["renderer"] == "sidemantic-crossfilter-tabs"

    response = client.post("/crossfilter/query", json={"tab": "overview", "filters": []})
    assert response.status_code == 200
    assert response.json()["protocol"] == "sidemantic-crossfilter-v1"
    assert session._interaction_preagg_cache.info is None


def test_crossfilter_dashboard_asgi_app_loads_non_initial_tab_specs_lazily(monkeypatch):
    pytest.importorskip("fastapi")
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient

    layer = _build_layer()
    first = layer.chart(
        ["orders.revenue", "orders.order_count"],
        by=["orders.created_at__month", "orders.region"],
    ).crossfilter(renderer="vega-lite")
    second = layer.chart(
        ["orders.revenue", "orders.order_count"],
        by=["orders.created_at__month", "orders.status"],
    ).crossfilter(renderer="vega-lite")

    def fail_if_eager(*args, **kwargs):
        raise AssertionError("non-initial tab spec should not be materialized for the shell")

    monkeypatch.setattr(second, "to_spec", fail_if_eager)
    monkeypatch.setattr(second.chart, "data", fail_if_eager)
    dashboard = CrossfilterDashboard(
        "Lazy Tabs",
        [
            CrossfilterTab("first", "First", first, source_record_count=3),
            CrossfilterTab("second", "Second", second, source_record_count=3),
        ],
    )

    client = TestClient(dashboard.to_asgi_app())
    html = client.get("/crossfilter.html")
    assert html.status_code == 200
    payload = client.get("/crossfilter.json").json()
    assert "spec" in payload["tabs"][0]
    assert "spec" not in payload["tabs"][1]
    assert payload["tabs"][1]["spec_endpoint"] == "/crossfilter/spec?tab=second"

    query = client.post("/crossfilter/query", json={"tab": "second", "event": "tab", "filters": []})
    assert query.status_code == 200
    assert query.json()["protocol"] == "sidemantic-crossfilter-v1"


def test_crossfilter_metadata_spec_does_not_poison_full_spec_cache():
    layer = _build_layer()
    session = layer.chart(
        ["orders.revenue", "orders.order_count"],
        by=["orders.created_at__month", "orders.region"],
    ).crossfilter(renderer="vega-lite")

    metadata = session.to_metadata_spec()

    assert metadata["data_deferred"] is True
    assert metadata["data"] == []
    assert session._spec is None

    full = session.to_spec()

    assert len(full["data"]) == 3
    assert session.source_record_count == 3


def test_dashboard_document_rejects_unknown_fields():
    layer = _build_layer()
    payload = _dashboard_payload()
    payload["tabs"][0]["charts"][0]["query"]["metrics"] = ["orders.revneue"]

    errors = DashboardDocument.from_dict(payload).validate(layer)

    assert "unknown metric 'orders.revneue'" in errors[0]


def test_dashboard_document_supports_multiple_charts_per_tab():
    layer = _build_layer()
    payload = _dashboard_payload()
    second_chart = {
        "id": "orders_by_status",
        "title": "Orders by status",
        "type": "bar",
        "query": {
            "metrics": ["orders.order_count"],
            "dimensions": ["orders.status"],
            "order_by": ["orders.order_count DESC"],
        },
        "encoding": {"x": "orders.status", "y": "orders.order_count"},
    }
    payload["tabs"][0]["charts"].append(second_chart)

    assert DashboardDocument.from_dict(payload).validate(layer, execute_sql=True) == []


@pytest.mark.parametrize("chart_type", ["scatter", "point"])
def test_dashboard_document_rejects_chart_types_without_canonical_renderers(chart_type):
    layer = _build_layer()
    payload = _dashboard_payload()
    payload["tabs"][0]["charts"][0]["type"] = chart_type

    errors = DashboardDocument.from_dict(payload).validate(layer)

    assert any(".type must be one of: area, auto, bar, line" in error for error in errors)


def test_legacy_crossfilter_adapter_reports_multi_chart_limit():
    layer = _build_layer()
    payload = _dashboard_payload()
    payload["tabs"][0]["charts"].append(
        {
            "id": "orders_by_status",
            "query": {"metrics": ["orders.order_count"], "dimensions": ["orders.status"]},
        }
    )

    with pytest.raises(DashboardSpecError, match="dashboard serve.*multi-chart"):
        DashboardDocument.from_dict(payload).to_crossfilter_dashboard(layer)


def test_dashboard_document_requires_unique_nonempty_chart_ids():
    layer = _build_layer()
    payload = _dashboard_payload()
    duplicate = dict(payload["tabs"][0]["charts"][0])
    payload["tabs"][0]["charts"].append(duplicate)
    payload["tabs"][0]["charts"].append({**duplicate, "id": ""})

    errors = DashboardDocument.from_dict(payload).validate(layer)

    assert any("id duplicates 'revenue_trend'" in error for error in errors)
    assert any("id is required" in error for error in errors)


def test_dashboard_document_rejects_unknown_order_by_fields():
    layer = _build_layer()
    payload = _dashboard_payload()
    payload["tabs"][0]["charts"][0]["query"]["order_by"] = ["orders.revene DESC"]

    errors = DashboardDocument.from_dict(payload).validate(layer)

    assert "query.order_by contains unknown field 'orders.revene'" in errors[0]


def test_dashboard_document_validates_order_by_against_query_fields():
    layer = _build_layer()
    payload = _dashboard_payload()
    payload["tabs"][0]["charts"][0]["query"]["dimensions"] = ["orders.created_at__month", "orders.region"]
    payload["tabs"][0]["charts"][0]["query"]["order_by"] = ["orders.status ASC"]

    errors = DashboardDocument.from_dict(payload).validate(layer)

    assert "query.order_by field 'orders.status' must also appear in query.metrics or query.dimensions" in errors[0]


def test_dashboard_document_compiles_duplicate_output_aliases():
    layer = _build_layer()
    layer.adapter.execute("CREATE TABLE customers (id INTEGER, status VARCHAR)")
    layer.adapter.execute("INSERT INTO customers VALUES (1, 'enterprise'), (2, 'self_serve'), (3, 'enterprise')")
    layer.get_model("orders").relationships.append(
        Relationship(name="customers", type="many_to_one", foreign_key="id", primary_key="id")
    )
    layer.add_model(
        Model(
            name="customers",
            table="customers",
            primary_key="id",
            dimensions=[Dimension(name="status", type="categorical")],
            metrics=[Metric(name="customer_count", agg="count")],
        )
    )
    payload = _dashboard_payload()
    chart = payload["tabs"][0]["charts"][0]
    chart["query"]["dimensions"] = ["orders.created_at__month", "orders.status", "customers.status"]
    chart["encoding"]["color"] = "orders.status"
    chart["interactions"]["select"]["fields"] = ["orders.status", "customers.status"]

    errors = DashboardDocument.from_dict(payload).validate(layer)

    assert errors == []

    dashboard = DashboardDocument.from_dict(payload).to_crossfilter_dashboard(layer)
    spec = dashboard.tab_spec("overview", include_data=False)["spec"]

    assert spec["fields"]["dimensions"] == ["created_at__month", "orders_status", "customers_status"]
    assert spec["field_plan"]["aliases"]["orders_status"] == "orders.status"
    assert spec["field_plan"]["aliases"]["customers_status"] == "customers.status"


def test_dashboard_document_validates_interaction_fields_are_real_behavior():
    layer = _build_layer()
    payload = _dashboard_payload()
    payload["tabs"][0]["charts"][0]["interactions"]["select"]["fields"] = ["orders.revenue"]
    payload["tabs"][0]["charts"][0]["interactions"]["brush"]["fields"] = ["orders.status"]

    errors = DashboardDocument.from_dict(payload).validate(layer, execute_sql=True)

    assert any(
        "interactions.select.fields field 'orders.revenue' must be a query dimension" in error for error in errors
    )
    assert any(
        "interactions.brush.fields must target the x field 'orders.created_at__month'" in error for error in errors
    )


def test_dashboard_document_validate_executes_sql_when_database_is_available():
    layer = _build_layer()
    payload = _dashboard_payload()
    layer.adapter.execute("DROP TABLE orders")

    errors = DashboardDocument.from_dict(payload).validate(layer, execute_sql=True)

    assert any("Table with name orders does not exist" in error for error in errors)


def test_dashboard_select_fields_drive_live_breakdown_queries():
    layer = _build_layer()
    payload = _dashboard_payload()
    payload["tabs"][0]["charts"][0]["interactions"]["select"]["fields"] = ["orders.status"]

    dashboard = DashboardDocument.from_dict(payload).to_crossfilter_dashboard(layer)
    response = dashboard.tabs[0].session.query(event="tab")

    assert set(response["views"]["bars"]) == {"status"}
    assert "bar:status" in response["sqls"]
    assert "bar:region" not in response["sqls"]


def test_dashboard_freshness_policy_is_propagated_to_live_session():
    layer = _build_layer()
    layer.get_model("orders").freshness = Freshness(watermark="created_at", ttl_seconds=86_400)
    payload = _dashboard_payload()

    document = DashboardDocument.from_dict(payload)
    assert document.validate(layer, execute_sql=True) == []

    dashboard = document.to_crossfilter_dashboard(layer)
    spec = dashboard.tab_spec("overview", include_data=False)["spec"]
    response = dashboard.handle_request({"tab": "overview", "event": "tab", "filters": []})

    assert spec["freshness_policy"]["source_watermark_configured"] is True
    assert spec["freshness_policy"]["source"] == "model_freshness"
    assert spec["freshness_policy"]["watermark"] == "orders.created_at"
    assert spec["freshness_policy"]["ttl_seconds"] == 86_400
    assert response["freshness"]["source_watermark"]["status"] == "available"
    assert response["freshness"]["policy"]["ttl_seconds"] == 86_400


def test_dashboard_validate_checks_default_freshness_sql_with_execute_sql():
    layer = _build_layer()
    payload = _dashboard_payload()
    payload["defaults"]["query"]["source_watermark_sql"] = "SELECT MAX(missing_at) FROM missing_orders"

    errors = DashboardDocument.from_dict(payload).validate(layer, execute_sql=True)

    assert any("source watermark unavailable" in error or "missing_orders" in error for error in errors)


def test_dashboard_validate_checks_chart_level_freshness_ttl():
    layer = _build_layer()
    payload = _dashboard_payload()
    payload["tabs"][0]["charts"][0]["freshness_ttl_seconds"] = "tomorrow"

    errors = DashboardDocument.from_dict(payload).validate(layer)

    assert any("freshness_ttl_seconds must be an integer" in error for error in errors)


def test_dashboard_typescript_is_generated_from_semantic_layer():
    ts = generate_dashboard_typescript(_build_layer())

    assert '"orders.revenue"' in ts
    assert '"orders.created_at__month"' in ts
    assert "export type Metric = typeof sidemanticSchema.metrics[number];" in ts
    assert "export function defineDashboard" in ts
    assert '"orders.region": string;' in ts
    assert '"orders.created_at__month": string | Date;' in ts


def test_dashboard_cli_validate_and_types(monkeypatch, tmp_path):
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "models.yml").write_text(
        """
models:
  - name: orders
    table: orders
    primary_key: id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: order_count
        agg: count
"""
    )
    db_path = tmp_path / "orders.db"
    layer = SemanticLayer(connection=f"duckdb:///{db_path}", auto_register=False)
    layer.adapter.execute("CREATE TABLE orders (id INTEGER, status VARCHAR)")
    layer.adapter.execute("INSERT INTO orders VALUES (1, 'completed'), (2, 'pending')")

    spec_path = tmp_path / "dashboard.yml"
    spec_path.write_text(
        yaml.safe_dump(
            {
                "schema": "sidemantic.dashboard.v1",
                "title": "Orders",
                "defaults": {"renderer": "plotly"},
                "tabs": [
                    {
                        "id": "overview",
                        "charts": [
                            {
                                "id": "orders_by_status",
                                "type": "bar",
                                "query": {
                                    "metrics": ["orders.order_count"],
                                    "dimensions": ["orders.status"],
                                    "interaction_preaggregations": True,
                                },
                                "encoding": {"x": "orders.status", "y": "orders.order_count"},
                                "interactions": {"select": {"fields": ["orders.status"]}},
                            }
                        ],
                    }
                ],
            },
            sort_keys=False,
        )
    )

    validate_result = runner.invoke(
        app,
        ["dashboard", "validate", str(spec_path), "--models", str(models_dir), "--db", str(db_path)],
    )
    assert validate_result.exit_code == 0
    assert "Dashboard spec is valid" in validate_result.output

    called = {}

    def fake_start_api_server(layer, **kwargs):
        called["layer"] = layer
        called.update(kwargs)

    monkeypatch.setattr("sidemantic.api_server.start_api_server", fake_start_api_server)
    serve_result = runner.invoke(
        app,
        [
            "dashboard",
            "serve",
            str(spec_path),
            "--models",
            str(models_dir),
            "--db",
            str(db_path),
            "--port",
            "9002",
        ],
    )
    assert serve_result.exit_code == 0
    assert called["port"] == 9002
    assert called["dashboard"].title == "Orders"

    help_result = runner.invoke(app, ["dashboard", "--help"])
    assert help_result.exit_code == 0
    assert "serve" in help_result.output

    types_path = tmp_path / "sidemantic.generated.ts"
    types_result = runner.invoke(app, ["dashboard", "types", "--models", str(models_dir), "--out", str(types_path)])
    assert types_result.exit_code == 0
    assert "orders.order_count" in types_path.read_text()


def test_committed_headless_dashboard_example_is_declarative():
    example_dir = Path(__file__).resolve().parents[1] / "examples" / "headless_dashboard"
    dashboard_yml = example_dir / "dashboard.yml"
    dashboard_ts = example_dir / "dashboard.ts"
    generated_ts = example_dir / "sidemantic.generated.ts"

    assert dashboard_yml.exists()
    assert dashboard_ts.exists()
    assert generated_ts.exists()

    layer = SemanticLayer(connection="duckdb:///:memory:", auto_register=False)
    load_from_directory(layer, example_dir)

    document = DashboardDocument.from_file(dashboard_yml)
    assert document.validate(layer) == []

    dashboard_source = dashboard_ts.read_text()
    generated_source = generated_ts.read_text()
    assert "defineDashboard" in dashboard_source
    assert '"orders.revenue"' in dashboard_source
    assert "export type Metric = typeof sidemanticSchema.metrics[number];" in generated_source
    assert '"orders.created_at__month"' in generated_source
