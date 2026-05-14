"""Tests for native Sidemantic YAML relationship metadata."""

import yaml

from sidemantic.adapters.sidemantic import SidemanticAdapter
from sidemantic.core.introspection import describe_graph


def test_native_adapter_round_trips_relationship_activity_and_metric_overrides(tmp_path):
    source = tmp_path / "models.yml"
    source.write_text(
        """
models:
  - name: Sales
    table: sales
    primary_key: SalesKey
    relationships:
      - name: Calendar
        type: many_to_one
        foreign_key: ShipDateKey
        primary_key: DateKey
        active: false
    metrics:
      - name: Ship Sales
        agg: sum
        sql: Amount
        required_models:
          - Calendar
        relationship_overrides:
          - from_model: Sales
            from_column: ShipDateKey
            to_model: Calendar
            to_column: DateKey
            join_type: inner
            direction: Both
  - name: Calendar
    table: calendar
    primary_key: DateKey
"""
    )

    graph = SidemanticAdapter(lower_dax=False).parse(source)
    sales = graph.models["Sales"]
    relationship = sales.relationships[0]
    metric = sales.get_metric("Ship Sales")

    assert relationship.active is False
    assert metric.required_models == ["Calendar"]
    assert len(metric.relationship_overrides) == 1
    assert metric.relationship_overrides[0].from_column == "ShipDateKey"

    description = describe_graph(graph, model_names=["Sales"])
    metric_info = description["models"][0]["metrics"][0]
    assert metric_info["relationship_overrides"] == [
        {
            "from_model": "Sales",
            "from_column": "ShipDateKey",
            "to_model": "Calendar",
            "to_column": "DateKey",
            "join_type": "inner",
            "direction": "Both",
        }
    ]

    exported_path = tmp_path / "exported.yml"
    SidemanticAdapter(lower_dax=False).export(graph, exported_path)
    exported = yaml.safe_load(exported_path.read_text())

    exported_sales = exported["models"][0]
    assert exported_sales["relationships"][0]["active"] is False
    exported_metric = exported_sales["metrics"][0]
    assert exported_metric["required_models"] == ["Calendar"]
    assert exported_metric["relationship_overrides"][0]["from_column"] == "ShipDateKey"
