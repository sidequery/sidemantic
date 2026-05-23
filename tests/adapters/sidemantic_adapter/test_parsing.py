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
