"""Test Sidemantic native YAML adapter parsing and export."""

import tempfile
from pathlib import Path

import pytest
import yaml

from sidemantic.adapters.sidemantic import SidemanticAdapter
from sidemantic.core.semantic_layer import SemanticLayer


def test_parse_native_yaml():
    """Test parsing native Sidemantic YAML."""
    adapter = SidemanticAdapter()
    graph = adapter.parse("tests/fixtures/sidemantic/orders.yml")

    # Verify models
    assert len(graph.models) == 2
    assert "orders" in graph.models
    assert "customers" in graph.models

    # Verify orders model
    orders = graph.models["orders"]
    assert orders.table == "public.orders"
    # Note: Example YAML still uses old format, will be updated in task #10
    assert len(orders.dimensions) == 3
    assert len(orders.metrics) == 3

    # Verify metrics
    assert len(graph.metrics) == 3
    assert "total_revenue" in graph.metrics
    assert "conversion_rate" in graph.metrics
    assert "revenue_per_order" in graph.metrics

    # Verify metric types
    assert graph.metrics["total_revenue"].type is None  # Untyped (was simple)
    assert graph.metrics["conversion_rate"].type == "ratio"
    assert graph.metrics["revenue_per_order"].type == "derived"


def test_parse_time_comparison_and_conversion_fields(tmp_path):
    """Test parsing time_comparison and conversion metric fields from YAML."""
    adapter = SidemanticAdapter()
    yaml_path = tmp_path / "metrics.yml"
    yaml_path.write_text(
        """
metrics:
  - name: revenue_yoy
    type: time_comparison
    base_metric: total_revenue
    comparison_type: yoy
    calculation: percent_change
    time_offset: "1 year"

  - name: signup_conversion
    type: conversion
    entity: user_id
    base_event: "event = 'signup'"
    conversion_event: "event = 'purchase'"
    conversion_window: "7 days"
"""
    )

    graph = adapter.parse(yaml_path)

    revenue_yoy = graph.metrics["revenue_yoy"]
    assert revenue_yoy.type == "time_comparison"
    assert revenue_yoy.base_metric == "total_revenue"
    assert revenue_yoy.comparison_type == "yoy"
    assert revenue_yoy.calculation == "percent_change"
    assert revenue_yoy.time_offset == "1 year"

    signup_conversion = graph.metrics["signup_conversion"]
    assert signup_conversion.type == "conversion"
    assert signup_conversion.entity == "user_id"
    assert signup_conversion.base_event == "event = 'signup'"
    assert signup_conversion.conversion_event == "event = 'purchase'"
    assert signup_conversion.conversion_window == "7 days"


def test_parse_native_yaml_accepts_version_one(tmp_path):
    """Test native YAML version 1 is accepted."""
    adapter = SidemanticAdapter()
    yaml_path = tmp_path / "orders.yml"
    yaml_path.write_text(
        """
version: 1
models:
  - name: orders
    table: public.orders
    primary_key: order_id
    metrics:
      - name: order_count
        agg: count
"""
    )

    graph = adapter.parse(yaml_path)

    assert "orders" in graph.models


def test_parse_native_yaml_accepts_compatibility_aliases(tmp_path):
    """Python compatibility aliases are accepted as native input."""
    adapter = SidemanticAdapter()
    yaml_path = tmp_path / "orders.yml"
    yaml_path.write_text(
        """
version: 1
models:
  - name: orders
    table: orders
    auto_dimensions: false
    dimensions:
      - name: status
        type: categorical
        expr: order_status
    measures:
      - name: total_revenue
        agg: sum
        expr: amount
      - name: revenue_per_order
        type: derived
        measure: total_revenue / order_count
      - name: order_count
        agg: count
"""
    )

    graph = adapter.parse(yaml_path)
    orders = graph.models["orders"]

    assert orders.auto_dimensions is False
    assert orders.dimensions[0].sql == "order_status"
    assert [metric.name for metric in orders.metrics] == ["total_revenue", "revenue_per_order", "order_count"]
    assert orders.metrics[0].sql == "amount"
    assert orders.metrics[1].sql == "total_revenue / order_count"


def test_parse_native_yaml_accepts_legacy_metric_dependencies(tmp_path):
    """Legacy exported derived metrics used `metrics` for dependency hints."""
    adapter = SidemanticAdapter()
    yaml_path = tmp_path / "metrics.yml"
    yaml_path.write_text(
        """
version: 1
metrics:
  - name: revenue_per_order
    type: derived
    sql: total_revenue / order_count
    metrics:
      - total_revenue
      - order_count
"""
    )

    graph = adapter.parse(yaml_path)

    metric = graph.metrics["revenue_per_order"]
    assert metric.type == "derived"
    assert metric.sql == "total_revenue / order_count"


@pytest.mark.parametrize(
    ("yaml_body", "error_text"),
    [
        (
            """
models:
  - name: orders
    table: orders
    metrcs: []
""",
            "unknown native field(s) in model: metrcs",
        ),
        (
            """
models:
  - name: orders
    table: orders
    dimensions:
      - name: status
        type: categorical
        sqll: status
""",
            "unknown native field(s) in model 'orders' dimension: sqll",
        ),
        (
            """
models:
  - name: orders
    table: orders
    metrics:
      - name: total_revenue
        agg: sum
        sqll: amount
""",
            "unknown native field(s) in model 'orders' metric: sqll",
        ),
        (
            """
models:
  - name: orders
    table: orders
    relationships:
      - name: customers
        type: many_to_one
        foreign_keys: customer_id
""",
            "unknown native field(s) in model 'orders' relationship: foreign_keys",
        ),
        (
            """
models:
  - name: orders
    table: orders
    pre_aggregations:
      - name: daily
        measures: [total_revenue]
        time_dimensions: created_at
""",
            "unknown native field(s) in model 'orders' pre_aggregation: time_dimensions",
        ),
    ],
)
def test_parse_native_yaml_rejects_unknown_nested_fields(tmp_path, yaml_body, error_text):
    adapter = SidemanticAdapter()
    yaml_path = tmp_path / "orders.yml"
    yaml_path.write_text(f"version: 1\n{yaml_body}")

    with pytest.raises(ValueError) as exc_info:
        adapter.parse(yaml_path)
    assert error_text in str(exc_info.value)


def test_parse_export_preserves_native_metadata_visibility_and_granularity(tmp_path):
    adapter = SidemanticAdapter()
    yaml_path = tmp_path / "orders.yml"
    yaml_path.write_text(
        """
version: 1
models:
  - name: orders
    table: orders
    meta:
      owner: analytics
    dimensions:
      - name: created_at
        type: time
        sql: created_at
        granularity: day
        supported_granularities: [day, week, month]
        meta:
          role: event_time
        public: false
    metrics:
      - name: total_revenue
        agg: sum
        sql: amount
        meta:
          unit: usd
        public: false
metrics:
  - name: revenue_per_order
    type: derived
    sql: orders.total_revenue / orders.order_count
    meta:
      owner: finance
    public: false
"""
    )

    graph = adapter.parse(yaml_path)
    orders = graph.models["orders"]
    created_at = orders.dimensions[0]
    total_revenue = orders.metrics[0]
    revenue_per_order = graph.metrics["revenue_per_order"]

    assert orders.meta == {"owner": "analytics"}
    assert created_at.supported_granularities == ["day", "week", "month"]
    assert created_at.meta == {"role": "event_time"}
    assert created_at.public is False
    assert total_revenue.meta == {"unit": "usd"}
    assert total_revenue.public is False
    assert revenue_per_order.meta == {"owner": "finance"}
    assert revenue_per_order.public is False

    export_path = tmp_path / "exported.yml"
    adapter.export(graph, export_path)
    exported = yaml.safe_load(export_path.read_text())
    exported_model = exported["models"][0]
    exported_dimension = exported_model["dimensions"][0]
    exported_metric = exported_model["metrics"][0]
    exported_graph_metric = exported["metrics"][0]

    assert exported_model["meta"] == {"owner": "analytics"}
    assert exported_dimension["supported_granularities"] == ["day", "week", "month"]
    assert exported_dimension["meta"] == {"role": "event_time"}
    assert exported_dimension["public"] is False
    assert exported_metric["meta"] == {"unit": "usd"}
    assert exported_metric["public"] is False
    assert exported_graph_metric["meta"] == {"owner": "finance"}
    assert exported_graph_metric["public"] is False

    graph2 = adapter.parse(export_path)
    assert graph2.models["orders"].dimensions[0].supported_granularities == ["day", "week", "month"]
    assert graph2.models["orders"].dimensions[0].meta == {"role": "event_time"}
    assert graph2.models["orders"].dimensions[0].public is False
    assert graph2.models["orders"].metrics[0].meta == {"unit": "usd"}
    assert graph2.models["orders"].metrics[0].public is False
    assert graph2.metrics["revenue_per_order"].meta == {"owner": "finance"}
    assert graph2.metrics["revenue_per_order"].public is False


def test_parse_export_preserves_top_level_parameters(tmp_path):
    adapter = SidemanticAdapter()
    yaml_path = tmp_path / "orders.yml"
    yaml_path.write_text(
        """
version: 1
parameters:
  - name: status
    type: string
    description: Order status
    label: Status
    default_value: paid
    allowed_values: [paid, refunded]
  - name: report_date
    type: date
    default_to_today: true
models:
  - name: orders
    table: orders
"""
    )

    graph = adapter.parse(yaml_path)
    assert graph.parameters["status"].default_value == "paid"
    assert graph.parameters["status"].allowed_values == ["paid", "refunded"]
    assert graph.parameters["report_date"].default_to_today is True

    export_path = tmp_path / "exported.yml"
    adapter.export(graph, export_path)
    exported = yaml.safe_load(export_path.read_text())

    assert exported["parameters"] == [
        {
            "name": "status",
            "type": "string",
            "description": "Order status",
            "label": "Status",
            "default_value": "paid",
            "allowed_values": ["paid", "refunded"],
        },
        {
            "name": "report_date",
            "type": "date",
            "default_to_today": True,
        },
    ]

    graph2 = adapter.parse(export_path)
    assert graph2.parameters["status"].default_value == "paid"
    assert graph2.parameters["status"].allowed_values == ["paid", "refunded"]
    assert graph2.parameters["report_date"].default_to_today is True


def test_parse_export_preserves_relationship_custom_sql(tmp_path):
    adapter = SidemanticAdapter()
    yaml_path = tmp_path / "orders.yml"
    yaml_path.write_text(
        """
version: 1
models:
  - name: orders
    table: orders
    relationships:
      - name: customers
        type: many_to_one
        foreign_key_columns: [customer_id, tenant_id]
        primary_key_columns: [customer_id, tenant_id]
        sql: "{from}.customer_id = {to}.customer_id AND {from}.tenant_id IS NOT DISTINCT FROM {to}.tenant_id"
  - name: customers
    table: customers
    primary_key_columns: [customer_id, tenant_id]
"""
    )

    graph = adapter.parse(yaml_path)
    relationship = graph.models["orders"].relationships[0]
    assert relationship.sql == (
        "{from}.customer_id = {to}.customer_id AND {from}.tenant_id IS NOT DISTINCT FROM {to}.tenant_id"
    )

    export_path = tmp_path / "exported.yml"
    adapter.export(graph, export_path)
    exported = yaml.safe_load(export_path.read_text())

    assert exported["models"][0]["relationships"][0]["sql"] == relationship.sql

    graph2 = adapter.parse(export_path)
    assert graph2.models["orders"].relationships[0].sql == relationship.sql


def test_parse_native_yaml_rejects_relationship_sql_without_placeholders(tmp_path):
    adapter = SidemanticAdapter()
    yaml_path = tmp_path / "orders.yml"
    yaml_path.write_text(
        """
version: 1
models:
  - name: orders
    table: orders
    relationships:
      - name: customers
        type: many_to_one
        foreign_key: customer_id
        sql: customer_id
"""
    )

    with pytest.raises(ValueError, match=r"relationship 'customers' sql must include both \{from\} and \{to\}"):
        adapter.parse(yaml_path)


def test_parse_export_preserves_pre_aggregations(tmp_path):
    adapter = SidemanticAdapter()
    yaml_path = tmp_path / "orders.yml"
    yaml_path.write_text(
        """
version: 1
models:
  - name: orders
    table: orders
    dimensions:
      - name: status
        type: categorical
      - name: created_at
        type: time
    metrics:
      - name: total_revenue
        agg: sum
        sql: amount
    pre_aggregations:
      - name: daily_revenue
        type: rollup
        sql: "select status, sum(amount) as total_revenue from orders group by 1"
        measures: [total_revenue]
        dimensions: [status]
        time_dimension: created_at
        granularity: day
        partition_granularity: month
        build_range_start: "date '2026-01-01'"
        build_range_end: current_date
        scheduled_refresh: false
        refresh_key:
          every: 1 hour
          sql: "select max(updated_at) from orders"
          incremental: true
          update_window: 7 days
        indexes:
          - name: by_status
            columns: [status]
            type: aggregate
        meta:
          owner: analytics
"""
    )

    graph = adapter.parse(yaml_path)
    preagg = graph.models["orders"].pre_aggregations[0]
    assert preagg.sql == "select status, sum(amount) as total_revenue from orders group by 1"
    assert preagg.partition_granularity == "month"
    assert preagg.scheduled_refresh is False
    assert preagg.refresh_key.every == "1 hour"
    assert preagg.refresh_key.sql == "select max(updated_at) from orders"
    assert preagg.refresh_key.incremental is True
    assert preagg.refresh_key.update_window == "7 days"
    assert preagg.indexes[0].name == "by_status"
    assert preagg.indexes[0].type == "aggregate"
    assert preagg.meta == {"owner": "analytics"}

    export_path = tmp_path / "exported.yml"
    adapter.export(graph, export_path)
    exported = yaml.safe_load(export_path.read_text())
    exported_preagg = exported["models"][0]["pre_aggregations"][0]

    assert exported_preagg == {
        "name": "daily_revenue",
        "type": "rollup",
        "sql": "select status, sum(amount) as total_revenue from orders group by 1",
        "measures": ["total_revenue"],
        "dimensions": ["status"],
        "time_dimension": "created_at",
        "granularity": "day",
        "partition_granularity": "month",
        "build_range_start": "date '2026-01-01'",
        "build_range_end": "current_date",
        "scheduled_refresh": False,
        "refresh_key": {
            "every": "1 hour",
            "sql": "select max(updated_at) from orders",
            "incremental": True,
            "update_window": "7 days",
        },
        "indexes": [{"name": "by_status", "columns": ["status"], "type": "aggregate"}],
        "meta": {"owner": "analytics"},
    }

    graph2 = adapter.parse(export_path)
    preagg2 = graph2.models["orders"].pre_aggregations[0]
    assert preagg2.sql == preagg.sql
    assert preagg2.refresh_key.incremental is True
    assert preagg2.indexes[0].type == "aggregate"
    assert preagg2.meta == {"owner": "analytics"}


def test_parse_native_yaml_explicit_key_columns(tmp_path):
    """Explicit *_columns key fields are part of the native YAML contract."""
    adapter = SidemanticAdapter()
    yaml_path = tmp_path / "orders.yml"
    yaml_path.write_text(
        """
version: 1
models:
  - name: order_items
    table: order_items
    primary_key_columns: [order_id, item_id]
    unique_keys:
      - [order_id, item_id]
    metrics:
      - name: count
        agg: count
  - name: shipments
    table: shipments
    primary_key: shipment_id
    dimensions:
      - name: carrier
        type: categorical
    relationships:
      - name: order_items
        type: many_to_one
        foreign_key_columns: [order_id, item_id]
        primary_key_columns: [order_id, item_id]
"""
    )

    graph = adapter.parse(yaml_path)

    order_items = graph.models["order_items"]
    assert order_items.primary_key == ["order_id", "item_id"]
    assert order_items.primary_key_columns == ["order_id", "item_id"]
    assert order_items.unique_keys == [["order_id", "item_id"]]

    relationship = graph.models["shipments"].relationships[0]
    assert relationship.foreign_key == ["order_id", "item_id"]
    assert relationship.foreign_key_columns == ["order_id", "item_id"]
    assert relationship.primary_key == ["order_id", "item_id"]
    assert relationship.primary_key_columns == ["order_id", "item_id"]

    layer = SemanticLayer(auto_register=False)
    for model in graph.models.values():
        layer.add_model(model)

    sql = layer.compile(metrics=["order_items.count"], dimensions=["shipments.carrier"])
    assert "shipments_cte.order_id = order_items_cte.order_id" in sql
    assert "shipments_cte.item_id = order_items_cte.item_id" in sql


def test_parse_native_yaml_resolves_model_and_metric_inheritance(tmp_path):
    adapter = SidemanticAdapter()
    yaml_path = tmp_path / "orders.yml"
    yaml_path.write_text(
        """
version: 1
models:
  - name: base_orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
        sql: status
    metrics:
      - name: revenue
        agg: sum
        sql: amount
  - name: completed_orders
    extends: base_orders
    dimensions:
      - name: completed_at
        type: time
        sql: completed_at
        granularity: day
    metrics:
      - name: completed_revenue
        extends: revenue
        filters:
          - status = 'completed'
metrics:
  - name: base_revenue
    sql: base_orders.revenue
  - name: display_revenue
    extends: base_revenue
    label: Revenue
"""
    )

    graph = adapter.parse(yaml_path)

    completed_orders = graph.models["completed_orders"]
    assert completed_orders.table == "orders"
    assert completed_orders.primary_key == "order_id"
    assert completed_orders.extends is None
    assert completed_orders.get_dimension("status") is not None
    assert completed_orders.get_dimension("completed_at") is not None

    completed_revenue = completed_orders.get_metric("completed_revenue")
    assert completed_revenue.agg == "sum"
    assert completed_revenue.sql == "amount"
    assert completed_revenue.filters == ["status = 'completed'"]
    assert completed_revenue.extends is None

    display_revenue = graph.metrics["display_revenue"]
    assert display_revenue.sql == "base_orders.revenue"
    assert display_revenue.label == "Revenue"
    assert display_revenue.extends is None


def test_parse_native_yaml_rejects_invalid_top_level_sql_metric_block(tmp_path):
    adapter = SidemanticAdapter()
    yaml_path = tmp_path / "orders.yml"
    yaml_path.write_text(
        """
version: 1
sql_metrics: |
  SELECT 1;
"""
    )

    with pytest.raises(ValueError, match=r"orders\.yml: invalid sql_metrics"):
        adapter.parse(yaml_path)


def test_parse_native_yaml_rejects_invalid_model_sql_metric_block(tmp_path):
    adapter = SidemanticAdapter()
    yaml_path = tmp_path / "orders.yml"
    yaml_path.write_text(
        """
version: 1
models:
  - name: orders
    table: orders
    sql_metrics: |
      SELECT 1;
"""
    )

    with pytest.raises(ValueError, match=r"orders\.yml: invalid model 'orders' sql_metrics"):
        adapter.parse(yaml_path)


def test_parse_native_yaml_rejects_invalid_top_level_sql_segment_block(tmp_path):
    adapter = SidemanticAdapter()
    yaml_path = tmp_path / "orders.yml"
    yaml_path.write_text(
        """
version: 1
sql_segments: |
  SELECT 1;
"""
    )

    with pytest.raises(ValueError, match=r"orders\.yml: invalid sql_segments"):
        adapter.parse(yaml_path)


def test_parse_native_yaml_rejects_invalid_model_sql_segment_block(tmp_path):
    adapter = SidemanticAdapter()
    yaml_path = tmp_path / "orders.yml"
    yaml_path.write_text(
        """
version: 1
models:
  - name: orders
    table: orders
    sql_segments: |
      SELECT 1;
"""
    )

    with pytest.raises(ValueError, match=r"orders\.yml: invalid model 'orders' sql_segments"):
        adapter.parse(yaml_path)


def test_parse_native_yaml_rejects_unsupported_version(tmp_path):
    """Test unsupported native YAML versions fail early."""
    adapter = SidemanticAdapter()
    yaml_path = tmp_path / "orders.yml"
    yaml_path.write_text(
        """
version: 2
models:
  - name: orders
    table: public.orders
"""
    )

    with pytest.raises(ValueError, match="Unsupported native Sidemantic format version 2"):
        adapter.parse(yaml_path)


def test_parse_native_sql_frontmatter_accepts_version_one(tmp_path):
    """Test native SQL frontmatter version 1 is accepted and not treated as model data."""
    adapter = SidemanticAdapter()
    sql_path = tmp_path / "orders.sql"
    sql_path.write_text(
        """
---
version: 1
name: orders
table: public.orders
primary_key: order_id
---

METRIC (
  name order_count,
  agg count
);
"""
    )

    graph = adapter.parse(sql_path)

    assert "orders" in graph.models
    assert [metric.name for metric in graph.models["orders"].metrics] == ["order_count"]
    assert "version" not in (graph.models["orders"].metadata or {})


def test_parse_native_sql_version_only_frontmatter_preserves_graph_metric(tmp_path):
    """Test version-only frontmatter does not swallow graph-level SQL metrics."""
    adapter = SidemanticAdapter()
    sql_path = tmp_path / "metrics.sql"
    sql_path.write_text(
        """
---
version: 1
---

METRIC (
  name order_count,
  agg count
);
"""
    )

    graph = adapter.parse(sql_path)

    assert len(graph.models) == 0
    assert "order_count" in graph.metrics
    assert graph.metrics["order_count"].agg == "count"


def test_parse_native_sql_version_only_frontmatter_preserves_graph_parameter(tmp_path):
    """Test version-only frontmatter does not swallow graph-level SQL parameters."""
    adapter = SidemanticAdapter()
    sql_path = tmp_path / "parameters.sql"
    sql_path.write_text(
        """
---
version: 1
---

PARAMETER (
  name status_filter,
  type string,
  default_value 'paid'
);
"""
    )

    graph = adapter.parse(sql_path)

    assert len(graph.models) == 0
    assert "status_filter" in graph.parameters
    assert graph.parameters["status_filter"].type == "string"


def test_parse_native_sql_frontmatter_rejects_unsupported_version(tmp_path):
    """Test unsupported native SQL frontmatter versions fail early."""
    adapter = SidemanticAdapter()
    sql_path = tmp_path / "orders.sql"
    sql_path.write_text(
        """
---
version: 2
name: orders
table: public.orders
---

METRIC (
  name order_count,
  agg count
);
"""
    )

    with pytest.raises(ValueError, match="Unsupported native Sidemantic format version 2"):
        adapter.parse(sql_path)


def test_export_native_yaml():
    """Test exporting to native Sidemantic YAML."""
    # Load example
    adapter = SidemanticAdapter()
    graph = adapter.parse("tests/fixtures/sidemantic/orders.yml")

    # Export to temp file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        adapter.export(graph, temp_path)

        # Verify file exists and is readable
        assert temp_path.exists()
        exported = yaml.safe_load(temp_path.read_text())
        assert exported["version"] == 1

        # Re-parse exported file
        graph2 = adapter.parse(temp_path)

        # Verify round-trip preserves structure
        assert len(graph2.models) == len(graph.models)
        assert len(graph2.metrics) == len(graph.metrics)
        assert set(graph2.models.keys()) == set(graph.models.keys())
        assert set(graph2.metrics.keys()) == set(graph.metrics.keys())

    finally:
        temp_path.unlink(missing_ok=True)


def test_semantic_layer_from_yaml():
    """Test SemanticLayer.from_yaml() convenience method."""
    sl = SemanticLayer.from_yaml("tests/fixtures/sidemantic/orders.yml")

    assert sl.list_models() == ["orders", "customers"]
    assert set(sl.list_metrics()) == {"total_revenue", "conversion_rate", "revenue_per_order"}


def test_semantic_layer_to_yaml():
    """Test SemanticLayer.to_yaml() convenience method."""
    sl = SemanticLayer.from_yaml("tests/fixtures/sidemantic/orders.yml")

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        sl.to_yaml(temp_path)
        assert temp_path.exists()

        # Verify round-trip
        sl2 = SemanticLayer.from_yaml(temp_path)
        assert sl2.list_models() == sl.list_models()
        assert set(sl2.list_metrics()) == set(sl.list_metrics())

    finally:
        temp_path.unlink(missing_ok=True)


def test_adapter_validation():
    """Test adapter validation catches errors."""
    adapter = SidemanticAdapter()
    graph = adapter.parse("tests/fixtures/sidemantic/orders.yml")

    # Should have no errors
    errors = adapter.validate(graph)
    assert len(errors) == 0


def test_dimension_window_field(tmp_path):
    """Test that dimensions with window expressions are parsed and use window as sql_expr."""
    adapter = SidemanticAdapter()
    yaml_path = tmp_path / "events.yml"
    yaml_path.write_text(
        """
models:
  - name: events
    table: public.events
    primary_key: event_id
    dimensions:
      - name: event
        type: categorical

      - name: next_event
        type: categorical
        sql: event
        window: "LEAD(event) OVER (PARTITION BY person_id ORDER BY timestamp)"
        description: The next event after this one for the same person

      - name: next_timestamp
        type: time
        sql: timestamp
        window: "LEAD(timestamp) OVER (PARTITION BY person_id ORDER BY timestamp)"
        description: Timestamp of the next event

      - name: plain_dim
        type: categorical
        sql: status

    metrics:
      - name: event_count
        agg: count
"""
    )

    graph = adapter.parse(yaml_path)
    model = graph.models["events"]

    # Dimension without window: sql_expr returns sql or name
    event_dim = model.get_dimension("event")
    assert event_dim.window is None
    assert event_dim.sql_expr == "event"

    plain_dim = model.get_dimension("plain_dim")
    assert plain_dim.window is None
    assert plain_dim.sql_expr == "status"

    # Dimension with window: sql_expr returns the base expression,
    # window_sql_expr returns the window function
    next_event = model.get_dimension("next_event")
    assert next_event.window == "LEAD(event) OVER (PARTITION BY person_id ORDER BY timestamp)"
    assert next_event.sql == "event"
    assert next_event.sql_expr == "event"
    assert next_event.window_sql_expr == "LEAD(event) OVER (PARTITION BY person_id ORDER BY timestamp)"

    next_ts = model.get_dimension("next_timestamp")
    assert next_ts.window == "LEAD(timestamp) OVER (PARTITION BY person_id ORDER BY timestamp)"
    assert next_ts.sql == "timestamp"
    assert next_ts.sql_expr == "timestamp"
    assert next_ts.window_sql_expr == next_ts.window


def test_dimension_window_roundtrip(tmp_path):
    """Test that window dimensions survive YAML export/import roundtrip."""
    adapter = SidemanticAdapter()
    yaml_path = tmp_path / "events.yml"
    yaml_path.write_text(
        """
models:
  - name: events
    table: public.events
    primary_key: event_id
    dimensions:
      - name: next_event
        type: categorical
        sql: event
        window: "LEAD(event) OVER (PARTITION BY person_id ORDER BY timestamp)"

    metrics:
      - name: event_count
        agg: count
"""
    )

    graph = adapter.parse(yaml_path)

    # Export
    export_path = tmp_path / "exported.yml"
    adapter.export(graph, export_path)

    # Re-import
    graph2 = adapter.parse(export_path)
    model2 = graph2.models["events"]
    dim = model2.get_dimension("next_event")
    assert dim.window == "LEAD(event) OVER (PARTITION BY person_id ORDER BY timestamp)"
    assert dim.sql == "event"
    assert dim.window_sql_expr == dim.window
    assert dim.sql_expr == "event"


def test_dimension_window_in_sql_generation():
    """Test that window dimensions produce correct SQL in generated queries."""
    from sidemantic.core.dimension import Dimension
    from sidemantic.core.metric import Metric
    from sidemantic.core.model import Model
    from sidemantic.core.semantic_graph import SemanticGraph
    from sidemantic.sql.generator import SQLGenerator

    graph = SemanticGraph()
    model = Model(
        name="events",
        table="public.events",
        primary_key="event_id",
        dimensions=[
            Dimension(
                name="event",
                type="categorical",
            ),
            Dimension(
                name="next_event",
                type="categorical",
                sql="event",
                window="LEAD(event) OVER (PARTITION BY person_id ORDER BY timestamp)",
            ),
        ],
        metrics=[
            Metric(name="event_count", agg="count"),
        ],
    )
    graph.add_model(model)

    gen = SQLGenerator(graph, dialect="duckdb")
    sql = gen.generate(
        metrics=["events.event_count"],
        dimensions=["events.next_event"],
    )

    # The window expression should appear in the generated SQL
    assert "LEAD(event) OVER (PARTITION BY person_id ORDER BY timestamp)" in sql


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
