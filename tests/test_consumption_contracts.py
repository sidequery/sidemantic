"""Tests for curated consumption and semantic-governance contracts."""

from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from sidemantic import (
    Deprecation,
    Dimension,
    Explore,
    Metric,
    Model,
    Relationship,
    SavedQuery,
    SecurityError,
    Segment,
    SemanticLayer,
    View,
    load_from_directory,
)
from sidemantic.adapters.hex import HexAdapter
from sidemantic.adapters.lookml import LookMLAdapter
from sidemantic.adapters.metricflow import MetricFlowAdapter
from sidemantic.adapters.sidemantic import SidemanticAdapter
from sidemantic.api_server import create_app
from sidemantic.cli import app
from sidemantic.schema import generate_yaml_schema
from sidemantic.validation import validate_explore, validate_governance, validate_saved_query


def _layer() -> SemanticLayer:
    layer = SemanticLayer(auto_register=False)
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="order_id",
            owner="analytics",
            domain="commerce",
            category="sales",
            tags=["tier-1", "finance"],
            status="active",
            certification="certified",
            dimensions=[
                Dimension(name="status", type="categorical"),
                Dimension(name="created_at", type="time", granularity="day"),
            ],
            metrics=[
                Metric(name="revenue", agg="sum", sql="amount", owner="finance"),
                Metric(name="order_count", agg="count"),
            ],
        )
    )
    layer.graph.add_explore(
        Explore(
            name="revenue_overview",
            model="orders",
            label="Revenue overview",
            allowed_dimensions=["status", "created_at__month"],
            allowed_metrics=["revenue", "order_count"],
            allowed_filter_fields=["status"],
            allowed_order_by=["revenue"],
            default_dimensions=["status"],
            default_metrics=["revenue"],
            filters=["orders.status != 'deleted'"],
            default_filters=["orders.status = 'paid'"],
            default_order_by=["orders.revenue DESC"],
            default_limit=25,
            max_limit=100,
            owner="analytics",
            domain="commerce",
            certification="verified",
        )
    )
    layer.graph.add_saved_query(
        SavedQuery(
            name="paid_revenue",
            explore="revenue_overview",
            dimensions=["status"],
            metrics=["revenue"],
            filters=["orders.status = 'paid'"],
            order_by=["orders.revenue DESC"],
            limit=10,
        )
    )
    return layer


def test_view_is_public_alias_for_explore():
    assert View is Explore


def test_explore_defaults_compile_and_mandatory_filters_apply():
    sql = _layer().compile(explore="revenue_overview")

    assert "SUM(orders_cte.revenue_raw)" in sql
    assert "status <> 'deleted'" in sql
    assert "status = 'paid'" in sql
    assert "ORDER BY" in sql
    assert "LIMIT 25" in sql


def test_explore_enforces_allowlists_and_max_limit():
    layer = _layer()

    with pytest.raises(ValueError, match="does not allow dimension"):
        layer.compile(explore="revenue_overview", dimensions=["orders.order_id"])
    with pytest.raises(ValueError, match="does not allow filter field"):
        layer.compile(explore="revenue_overview", filters=["orders.created_at > '2026-01-01'"])
    with pytest.raises(ValueError, match="does not allow ordering"):
        layer.compile(explore="revenue_overview", order_by=["orders.status"])
    with pytest.raises(ValueError, match="exceeds max_limit"):
        layer.compile(explore="revenue_overview", limit=101)


def test_saved_query_is_immutable_and_compiles_through_its_explore():
    layer = _layer()

    sql = layer.compile(saved_query="paid_revenue")
    assert "LIMIT 10" in sql
    assert "status <> 'deleted'" in sql
    with pytest.raises(ValueError, match="immutable"):
        layer.compile(saved_query="paid_revenue", metrics=["orders.order_count"])
    with pytest.raises(ValueError, match="offset"):
        layer.compile(saved_query="paid_revenue", offset=5)
    with pytest.raises(ValueError, match="ungrouped"):
        layer.compile(saved_query="paid_revenue", ungrouped=True)


def test_native_yaml_roundtrip_preserves_contracts_and_governance(tmp_path: Path):
    source = tmp_path / "models.yml"
    output = tmp_path / "roundtrip.yml"
    source.write_text(
        """
version: 1
models:
  - name: orders
    table: orders
    owner: analytics
    domain: commerce
    category: sales
    tags: [tier-1]
    status: deprecated
    certification: certified
    deprecation:
      message: Use completed_orders
      deprecated_at: 2026-01-01
      sunset_at: 2026-12-31
      replaced_by: completed_orders
    visibility: internal
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: amount
        owner: finance
        visibility: internal
        freshness:
          watermark: updated_at
          ttl_seconds: 3600
explores:
  - name: revenue_overview
    model: orders
    allowed_dimensions: [status]
    allowed_metrics: [revenue]
    allowed_filter_fields: [status]
    allowed_order_by: [revenue]
    default_metrics: [revenue]
    owner: analytics
saved_queries:
  - name: revenue_by_status
    explore: revenue_overview
    dimensions: [status]
    metrics: [revenue]
    limit: 20
""".strip()
    )

    graph = SidemanticAdapter().parse(source)
    assert graph.models["orders"].deprecation == Deprecation(
        message="Use completed_orders",
        deprecated_at="2026-01-01",
        sunset_at="2026-12-31",
        replaced_by="completed_orders",
    )
    assert graph.models["orders"].visibility == "internal"
    assert graph.models["orders"].metrics[0].freshness.ttl_seconds == 3600
    assert graph.explores["revenue_overview"].owner == "analytics"
    assert graph.saved_queries["revenue_by_status"].limit == 20

    SidemanticAdapter().export(graph, output)
    exported = yaml.safe_load(output.read_text())
    assert exported["models"][0]["owner"] == "analytics"
    assert exported["models"][0]["deprecation"]["replaced_by"] == "completed_orders"
    exported_metric = exported["models"][0]["metrics"][0]
    assert exported_metric["owner"] == "finance"
    assert exported_metric["visibility"] == "internal"
    assert exported_metric["freshness"] == {"watermark": "updated_at", "ttl_seconds": 3600}
    assert exported["explores"][0]["allowed_metrics"] == ["revenue"]
    assert exported["explores"][0]["allowed_filter_fields"] == ["status"]
    assert exported["saved_queries"][0]["name"] == "revenue_by_status"


def test_validation_catalog_and_description_include_consumption_contracts():
    layer = _layer()
    errors, warnings = validate_explore(layer.graph.explores["revenue_overview"], layer.graph)
    assert errors == []
    assert warnings == []

    catalog = layer.get_catalog_metadata()
    assert catalog["explores"][0]["name"] == "revenue_overview"
    assert catalog["saved_queries"][0]["name"] == "paid_revenue"
    assert catalog["tables"][0]["owner"] == "analytics"

    description = layer.describe_models()
    assert description["models"][0]["governance"]["certification"] == "certified"
    assert description["explores"][0]["name"] == "revenue_overview"

    errors, _warnings = validate_governance(Metric(name="legacy", agg="count", status="deprecated"), "Metric 'legacy'")
    assert errors == ["Metric 'legacy' is deprecated but has no deprecation lifecycle/message"]


def test_validation_accepts_metric_filters_and_preflights_saved_query_explore_constraints():
    layer = _layer()
    layer.graph.models["orders"].metrics.append(Metric(name="cost", agg="sum", sql="cost"))
    explore = layer.graph.explores["revenue_overview"]
    explore.allowed_filter_fields = ["status", "revenue"]
    errors, _warnings = validate_explore(explore, layer.graph)
    assert errors == []

    invalid = SavedQuery(
        name="invalid_contract",
        explore="revenue_overview",
        metrics=["cost"],
        dimensions=["created_at__month"],
        filters=["orders.created_at > '2026-01-01'"],
        order_by=["orders.status"],
        limit=101,
    )
    errors, _warnings = validate_saved_query(invalid, layer.graph)
    assert any("metric(s) not allowed" in error for error in errors)
    assert any("filters on field(s) not allowed" in error for error in errors)
    assert any("orders by field(s) not allowed" in error for error in errors)
    assert any("exceeds Explore" in error for error in errors)


def test_validation_preflights_saved_query_segments():
    layer = _layer()
    layer.graph.models["orders"].segments.append(Segment(name="paid", sql="{model}.status = 'paid'"))

    valid = SavedQuery(
        name="valid_segment",
        explore="revenue_overview",
        metrics=["revenue"],
        segments=["paid"],
    )
    errors, _warnings = validate_saved_query(valid, layer.graph)
    assert errors == []

    invalid = valid.model_copy(update={"name": "invalid_segment", "segments": ["orders.missing"]})
    errors, _warnings = validate_saved_query(invalid, layer.graph)
    assert errors == [
        "Saved query 'invalid_segment' references segment 'missing' which doesn't exist on model 'orders'"
    ]


def test_visibility_enforcement_covers_models_metrics_and_explores():
    layer = _layer()
    layer.enforce_visibility = True
    layer.graph.saved_queries["paid_revenue"].visibility = "private"
    with pytest.raises(ValueError, match="Saved query 'paid_revenue' is not public"):
        layer.compile(saved_query="paid_revenue")
    layer.graph.saved_queries["paid_revenue"].visibility = "public"
    layer.graph.models["orders"].visibility = "internal"
    with pytest.raises(SecurityError, match="not public"):
        layer.compile(explore="revenue_overview")
    layer.graph.models["orders"].visibility = "public"
    layer.graph.models["orders"].metrics[0].visibility = "internal"
    with pytest.raises(SecurityError, match="not public"):
        layer.compile(explore="revenue_overview")


def test_meta_api_exposes_consumption_contracts():
    client = TestClient(create_app(_layer()))

    assert client.get("/explores").json()[0]["name"] == "revenue_overview"
    assert client.get("/saved-queries").json()[0]["name"] == "paid_revenue"
    graph = client.get("/graph").json()
    assert graph["explores"][0]["model"] == "orders"
    compiled = client.post("/compile", json={"saved_query": "paid_revenue"})
    assert compiled.status_code == 200, compiled.text
    assert "LIMIT 10" in compiled.json()["sql"]

    explicit_empty = client.post(
        "/compile",
        json={
            "explore": "revenue_overview",
            "dimensions": ["orders.status"],
            "metrics": [],
            "filters": [],
        },
    )
    assert explicit_empty.status_code == 200, explicit_empty.text
    explicit_sql = explicit_empty.json()["sql"]
    assert "SUM(" not in explicit_sql
    assert "status = 'paid'" not in explicit_sql
    assert "status <> 'deleted'" in explicit_sql


def test_meta_api_does_not_leak_private_relationship_targets():
    layer = _layer()
    layer.enforce_visibility = True
    layer.graph.models["orders"].relationships.append(
        Relationship(name="private_customers", type="many_to_one", foreign_key="customer_id")
    )
    layer.add_model(
        Model(
            name="private_customers",
            table="private_customers",
            primary_key="customer_id",
            visibility="private",
            dimensions=[Dimension(name="customer_id", type="numeric")],
        )
    )

    graph = TestClient(create_app(layer)).get("/graph").json()
    assert [model["name"] for model in graph["models"]] == ["orders"]
    assert graph["models"][0]["relationships"] == []
    assert graph["joinable_pairs"] == []

    catalog = layer.get_catalog_metadata()
    assert all(item.get("referenced_table_name") != "private_customers" for item in catalog["key_column_usage"])

    description = layer.describe_models()
    assert not description["models"][0].get("relationships")


def test_visibility_enforcement_hides_contracts_that_expose_private_fields():
    layer = _layer()
    layer.enforce_visibility = True
    layer.graph.models["orders"].metrics.append(
        Metric(name="secret_margin", agg="sum", sql="margin", visibility="private")
    )
    layer.graph.add_explore(
        Explore(
            name="leaky_explore",
            model="orders",
            allowed_metrics=["secret_margin"],
            default_metrics=["secret_margin"],
            metadata={"source_fields": ["secret_margin"]},
        )
    )
    layer.graph.add_saved_query(SavedQuery(name="leaky_query", explore="leaky_explore", metrics=["secret_margin"]))

    client = TestClient(create_app(layer))
    assert {value["name"] for value in client.get("/explores").json()} == {"revenue_overview"}
    assert {value["name"] for value in client.get("/saved-queries").json()} == {"paid_revenue"}
    assert {value["name"] for value in client.get("/graph").json()["explores"]} == {"revenue_overview"}
    assert {value["name"] for value in layer.get_catalog_metadata()["explores"]} == {"revenue_overview"}
    assert {value["name"] for value in layer.describe_models()["explores"]} == {"revenue_overview"}


def test_schema_and_cli_expose_contracts(tmp_path: Path):
    schema = generate_yaml_schema()
    assert "explores" in schema["properties"]
    assert "saved_queries" in schema["properties"]

    model_file = tmp_path / "models.yml"
    model_file.write_text(
        """
models:
  - name: orders
    table: orders
    dimensions:
      - name: status
    metrics:
      - name: revenue
        agg: sum
        sql: amount
explores:
  - name: revenue_overview
    model: orders
    default_dimensions: [status]
    default_metrics: [revenue]
saved_queries:
  - name: revenue_by_status
    explore: revenue_overview
    dimensions: [status]
    metrics: [revenue]
""".strip()
    )
    runner = CliRunner()

    info = runner.invoke(app, ["info", str(tmp_path), "--json"])
    assert info.exit_code == 0, info.output
    assert '"revenue_overview"' in info.output
    assert '"revenue_by_status"' in info.output

    dry_run = runner.invoke(
        app,
        ["query", "--models", str(tmp_path), "--saved-query", "revenue_by_status", "--dry-run"],
    )
    assert dry_run.exit_code == 0, dry_run.output
    assert "SUM" in dry_run.output


def test_directory_loader_recognizes_contract_only_native_yaml(tmp_path: Path):
    (tmp_path / "models.yml").write_text(
        """
models:
  - name: orders
    table: orders
    dimensions: [{name: status}]
    metrics: [{name: revenue, agg: sum, sql: amount}]
""".strip()
    )
    (tmp_path / "contracts.yml").write_text(
        """
version: 1
explores:
  - name: revenue_overview
    model: orders
    default_metrics: [revenue]
saved_queries:
  - name: revenue_by_status
    explore: revenue_overview
    dimensions: [status]
    metrics: [revenue]
""".strip()
    )

    layer = SemanticLayer(auto_register=False)
    load_from_directory(layer, tmp_path)
    assert set(layer.graph.explores) == {"revenue_overview"}
    assert set(layer.graph.saved_queries) == {"revenue_by_status"}


def test_lossless_adapter_bridges_create_typed_contracts():
    hex_graph = HexAdapter().parse(Path("tests/fixtures/hex/subscriptions_project.yml"))
    assert hex_graph.explores["revenue_overview"].model == "subscriptions"
    assert hex_graph.explores["revenue_overview"].allowed_metrics == [
        "subscriptions.total_mrr",
        "subscriptions.current_mrr",
    ]

    metricflow_graph = MetricFlowAdapter().parse(Path("tests/fixtures/metricflow/simple_manifest_saved_queries.yaml"))
    assert metricflow_graph.saved_queries["p0_booking_with_order_by_and_limit"].limit == 10
    assert "metricflow" in metricflow_graph.saved_queries["p0_booking"].metadata
    errors, warnings = validate_saved_query(metricflow_graph.saved_queries["p0_booking"], metricflow_graph)
    assert errors == []
    assert any("not executable" in warning for warning in warnings)

    lookml_graph = LookMLAdapter().parse(Path("tests/fixtures/lookml/edge_cases_explores.lkml"))
    assert lookml_graph.explores["orders"].model == "fact_orders"
    assert lookml_graph.explores["orders"].category == "Sales"
    assert lookml_graph.explores["completed_orders"].filters == ["fact_orders.status = 'completed'"]

    lookml_layer = SemanticLayer(auto_register=False)
    lookml_layer.graph = lookml_graph
    sql = lookml_layer.compile(explore="completed_orders", dimensions=["fact_orders.status"])
    assert "{'model': model}" not in sql
    assert "status = 'completed'" in sql


def test_lookml_joined_always_filter_preserves_join_target(tmp_path: Path):
    source = tmp_path / "joined_filter.lkml"
    source.write_text(
        """
view: orders {
  sql_table_name: orders ;;
  dimension: id { primary_key: yes sql: ${TABLE}.id ;; }
  dimension: customer_id { sql: ${TABLE}.customer_id ;; }
  measure: count { type: count }
}
view: customers {
  sql_table_name: customers ;;
  dimension: id { primary_key: yes sql: ${TABLE}.id ;; }
  dimension: region { sql: ${TABLE}.region ;; }
}
explore: orders {
  sql_always_where: ${TABLE}.id > 0 ;;
  always_filter: { filters: [customers.region: "West"] }
  join: customers {
    relationship: many_to_one
    sql_on: ${orders.customer_id} = ${customers.id} ;;
  }
}
""".strip()
    )

    graph = LookMLAdapter().parse(source)
    assert graph.explores["orders"].filters == ["orders.id > 0", "customers.region = 'West'"]

    layer = SemanticLayer(auto_register=False)
    layer.graph = graph
    sql = layer.compile(explore="orders", metrics=["orders.count"])
    assert "FROM customers" in sql
    assert "WHERE region = 'West'" in sql
    assert "WHERE id > 0" in sql
    assert "orders_cte.customer_id = customers_cte.id" in sql


def test_metricflow_metric_ordering_is_preserved_but_not_executable(tmp_path: Path):
    source = tmp_path / "metricflow.yml"
    source.write_text(
        """
semantic_models:
  - name: bookings
    model: ref('bookings')
    measures:
      - name: booking_count
        expr: "1"
        agg: sum
saved_queries:
  - name: ordered_bookings
    query_params:
      metrics: [booking_count]
      order_by:
        - Metric('booking_count').descending(True)
""".strip()
    )

    graph = MetricFlowAdapter().parse(source)
    saved_query = graph.saved_queries["ordered_bookings"]
    metadata = saved_query.metadata["metricflow"]
    assert metadata["executable"] is False
    assert "Metric()" in metadata["compatibility_message"]

    layer = SemanticLayer(auto_register=False)
    layer.graph = graph
    with pytest.raises(ValueError, match="cannot execute"):
        layer.compile(saved_query="ordered_bookings")
