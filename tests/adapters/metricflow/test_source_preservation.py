"""Exercise the distinction between queryable mappings and retained dbt source."""

import pytest
import yaml

from sidemantic.adapters.metricflow import MetricFlowAdapter
from sidemantic.fidelity import capture_import_report


def _roundtrip(tmp_path, source):
    path = tmp_path / "source.yml"
    path.write_text(yaml.safe_dump(source))
    adapter = MetricFlowAdapter()
    graph = adapter.parse(path)
    output = tmp_path / "out.yml"
    adapter.export(graph, output)
    return graph, yaml.safe_load(output.read_text()), adapter.parse(output)


def _model():
    return {
        "name": "orders",
        "model": "ref('orders')",
        "entities": [{"name": "order", "type": "primary", "expr": "order_id"}],
        "measures": [{"name": "amount", "agg": "sum", "expr": "amount"}],
    }


def test_saved_queries_and_conversion_definitions_roundtrip_without_execution(tmp_path):
    saved = {
        "name": "weekly",
        "label": "Weekly",
        "config": {"cache": {"enabled": True}},
        "query_params": {"metrics": ["total"], "group_by": ["metric_time__week"], "where": "country = 'US'"},
        "exports": [{"name": "export", "config": {"export_as": "table"}}],
    }
    conversion = {
        "name": "rate",
        "type": "conversion",
        "label": "Rate",
        "type_params": {
            "conversion_type_params": {
                "base_measure": {"name": "visits", "filter": "is_valid"},
                "conversion_measure": "orders",
                "entity": "user",
                "window": "7 days",
            }
        },
    }
    with capture_import_report() as report:
        graph, exported, reloaded = _roundtrip(
            tmp_path, {"semantic_models": [_model()], "saved_queries": [saved], "metrics": [conversion]}
        )
    assert exported["saved_queries"] == [saved]
    assert exported["metrics"] == [conversion]
    assert "rate" not in graph.metrics
    assert reloaded.metadata["metricflow_conversion_metrics"]["rate"]["source_definition"] == conversion
    assert report.is_blocked
    assert {note.construct for note in report.notes} >= {"saved_query", "conversion_metric"}


@pytest.mark.parametrize(
    "params",
    [
        {"measure": {"name": "amount", "filter": "amount > 0"}, "window": "7 days"},
        {"measure": "amount", "cumulative_type_params": {"grain_to_date": "month", "period_agg": "last"}},
        {"cumulative_type_params": {"metric": {"name": "total"}, "window": "7 days", "period_agg": "sum"}},
    ],
)
def test_cumulative_inputs_and_windows_survive(tmp_path, params):
    source = {
        "semantic_models": [_model()],
        "metrics": [{"name": "rolling", "type": "cumulative", "type_params": params}],
    }
    original, exported, reloaded = _roundtrip(tmp_path, source)
    before, after = original.metrics["rolling"], reloaded.metrics["rolling"]
    assert after.sql == before.sql
    assert (after.window, after.grain_to_date) == (before.window, before.grain_to_date)
    assert after.metadata.get("period_agg") == before.metadata.get("period_agg")
    assert after.metadata["metricflow_cumulative_input"] == before.metadata["metricflow_cumulative_input"]
    assert exported["metrics"][0]["type_params"]


def test_derived_aliases_offsets_and_filters_preserve_source_expression(tmp_path):
    inputs = [
        {"name": "total", "alias": "current"},
        {"name": "total", "alias": "prior", "offset_window": "7 days", "filter": "country = 'US'"},
    ]
    source = {
        "semantic_models": [_model()],
        "metrics": [
            {"name": "total", "type": "simple", "type_params": {"measure": "amount"}},
            {"name": "growth", "type": "derived", "type_params": {"expr": "current - prior", "metrics": inputs}},
        ],
    }
    with capture_import_report() as report:
        graph, exported, reloaded = _roundtrip(tmp_path, source)
    assert graph.metrics["growth"].sql == "total - prior"
    assert exported["metrics"][1]["type_params"] == source["metrics"][1]["type_params"]
    assert reloaded.metrics["growth"].metadata["input_metrics"] == inputs
    assert any(note.construct == "derived_input_modifiers" for note in report.notes)
    graph.metrics["growth"].sql = "total * 10"
    with pytest.raises(ValueError, match="reconcile its stored input aliases/modifiers"):
        MetricFlowAdapter().export(graph, tmp_path / "changed.yml")


def test_latest_conversion_lowers_to_generated_measure_and_average_spelling(tmp_path):
    source = {
        "models": [
            {
                "name": "orders",
                "semantic_model": {"enabled": True},
                "columns": [{"name": "id", "entity": "primary"}],
                "metrics": [
                    {"name": "mean_amount", "type": "simple", "agg": "average", "expr": "amount"},
                    {"name": "orders_count", "type": "simple", "agg": "count"},
                ],
            }
        ],
        "metrics": [
            {
                "name": "rate",
                "type": "conversion",
                "base_metric": "orders_count",
                "conversion_metric": "orders_count",
                "entity": "user",
            }
        ],
    }
    _, exported, _ = _roundtrip(tmp_path, source)
    measures = exported["semantic_models"][0]["measures"]
    assert measures[0]["agg"] == "average"
    assert exported["metrics"][-1]["type_params"]["conversion_type_params"]["base_measure"] == "orders_count_measure"


@pytest.mark.parametrize("entity_type", ["primary", "unique"])
def test_entity_names_resolve_arbitrary_models_and_backing_keys(tmp_path, entity_type):
    source = {
        "semantic_models": [
            {
                "name": "people_rollup",
                "model": "ref('people')",
                "entities": [{"name": "customer", "type": entity_type, "expr": "person_id"}],
            },
            {
                "name": "purchases",
                "model": "ref('orders')",
                "entities": [
                    {"name": "order", "type": "primary", "expr": "id"},
                    {"name": "customer", "type": "foreign", "expr": "buyer_id"},
                ],
            },
        ]
    }
    graph, exported, reloaded = _roundtrip(tmp_path, source)
    for candidate in (graph, reloaded):
        relation = candidate.models["purchases"].relationships[0]
        assert relation.related_model == "people_rollup"
        assert relation.primary_key == "person_id"
        assert relation.name == "customer"
    assert any(e["type"] == entity_type and e["name"] == "customer" for e in exported["semantic_models"][0]["entities"])


def test_ambiguous_entity_is_reported_instead_of_guessed(tmp_path):
    source = {
        "semantic_models": [
            {
                "name": name,
                "model": f"ref('{name}')",
                "entities": [{"name": "customer", "type": "primary", "expr": "id"}],
            }
            for name in ("person_one", "person_two")
        ]
        + [
            {
                "name": "orders",
                "model": "ref('orders')",
                "entities": [{"name": "customer", "type": "foreign", "expr": "buyer_id"}],
            }
        ]
    }
    path = tmp_path / "source.yml"
    path.write_text(yaml.safe_dump(source))
    with capture_import_report() as report:
        MetricFlowAdapter().parse(path)
    assert any(note.construct == "ambiguous_entity" for note in report.notes)
    assert report.is_blocked


def test_compiled_manifest_does_not_silently_lose_node_relation(tmp_path):
    path = tmp_path / "semantic_manifest.json"
    path.write_text(
        '{"semantic_models": [{"name": "orders", "node_relation": {"relation_name": "db.public.orders"}}], "project_configuration": {}}'
    )
    with pytest.raises(ValueError, match="expects dbt source YAML"):
        MetricFlowAdapter().parse(path)


def test_export_diagnoses_unrepresentable_native_metrics_and_relations(tmp_path):
    from sidemantic import Metric, Model
    from sidemantic.core.relationship import Relationship
    from sidemantic.core.semantic_graph import SemanticGraph
    from sidemantic.fidelity import capture_export_report

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            sql="SELECT * FROM raw_orders",
            metrics=[Metric(name="expression", sql="SUM(amount) + MAX(amount)")],
            relationships=[Relationship(name="lines", type="one_to_many", foreign_key="order_id")],
        )
    )
    with capture_export_report() as report:
        MetricFlowAdapter().export(graph, tmp_path / "out.yml")
    assert report.is_blocked
    assert {note.construct for note in report.notes} >= {"model_sql", "model_metric", "relationship"}


def test_saved_query_native_parameter_edit_keeps_source_configuration(tmp_path):
    source = {
        "semantic_models": [_model()],
        "saved_queries": [
            {
                "name": "report",
                "config": {"cache": {"enabled": True}},
                "query_params": {"metrics": ["total"], "limit": 10},
            }
        ],
    }
    graph, _, _ = _roundtrip(tmp_path, source)
    graph.metadata["saved_queries"]["report"]["limit"] = 20
    path = tmp_path / "edit.yml"
    MetricFlowAdapter().export(graph, path)
    exported = yaml.safe_load(path.read_text())["saved_queries"][0]
    assert exported["query_params"]["limit"] == 20
    assert exported["config"] == source["saved_queries"][0]["config"]
    graph.metadata["saved_queries"]["report"]["limit"] = None
    MetricFlowAdapter().export(graph, path)
    assert "limit" not in yaml.safe_load(path.read_text())["saved_queries"][0]["query_params"]


def test_legacy_conversion_edits_overlay_source_and_preserve_reference_modifiers(tmp_path):
    conversion = {
        "name": "rate",
        "type": "conversion",
        "description": "Original",
        "filter": "country = 'US'",
        "config": {"meta": {"owner": "growth"}},
        "type_params": {
            "conversion_type_params": {
                "base_measure": {"name": "visits", "filter": "is_valid"},
                "conversion_measure": "orders",
                "entity": "user",
                "window": "7 days",
                "constant_properties": [{"base_property": "country", "conversion_property": "country"}],
            }
        },
    }
    graph, _, _ = _roundtrip(tmp_path, {"semantic_models": [_model()], "metrics": [conversion]})
    spec = graph.metadata["metricflow_conversion_metrics"]["rate"]
    spec.update(
        name="new_rate",
        description=None,
        label="Edited",
        filter=None,
        window="30 days",
        base_measure="valid_visits",
        conversion_measure="paid_orders",
        entity="customer",
        calculation="conversions",
        constant_properties=None,
    )
    output = tmp_path / "edited.yml"
    MetricFlowAdapter().export(graph, output)
    exported = yaml.safe_load(output.read_text())["metrics"][0]
    assert exported["name"] == "new_rate"
    assert exported["label"] == "Edited"
    assert "description" not in exported and "filter" not in exported
    assert exported["config"] == conversion["config"]
    assert exported["type_params"]["conversion_type_params"] == {
        "base_measure": {"name": "valid_visits", "filter": "is_valid"},
        "conversion_measure": "paid_orders",
        "entity": "customer",
        "window": "30 days",
        "calculation": "conversions",
    }
    restored = MetricFlowAdapter().parse(output).metadata["metricflow_conversion_metrics"]["new_rate"]
    for key in (
        "base_measure",
        "conversion_measure",
        "entity",
        "window",
        "calculation",
        "constant_properties",
        "filter",
    ):
        assert restored[key] == spec[key]
    spec["window"] = None
    MetricFlowAdapter().export(graph, output)
    assert "window" not in yaml.safe_load(output.read_text())["metrics"][0]["type_params"]["conversion_type_params"]


@pytest.mark.parametrize(
    "field,value",
    [
        ("filter", None),
        ("description", None),
        ("label", None),
        ("filter", "country = 'CA'"),
        ("description", "Edited description"),
        ("label", "Edited label"),
    ],
)
def test_latest_conversion_optional_field_edits_preserve_other_source_properties(tmp_path, field, value):
    conversion = {
        "name": "rate",
        "type": "conversion",
        "description": "Original description",
        "label": "Original label",
        "filter": "country = 'US'",
        "config": {"meta": {"owner": "growth"}},
        "base_metric": {"name": "orders_count", "filter": "is_valid"},
        "conversion_metric": {"name": "orders_count", "filter": "is_paid"},
        "entity": "user",
        "window": "7 days",
    }
    source = {
        "models": [
            {
                "name": "orders",
                "semantic_model": {"enabled": True},
                "columns": [{"name": "id", "entity": "primary"}],
                "metrics": [{"name": "orders_count", "type": "simple", "agg": "count"}],
            }
        ],
        "metrics": [conversion],
    }
    graph, original_export, _ = _roundtrip(tmp_path, source)
    spec = graph.metadata["metricflow_conversion_metrics"]["rate"]
    spec[field] = value
    output = tmp_path / "edited.yml"
    MetricFlowAdapter().export(graph, output)
    exported = yaml.safe_load(output.read_text())["metrics"][-1]
    expected = original_export["metrics"][-1]
    if value is None:
        expected.pop(field)
    else:
        expected[field] = value
    assert exported == expected
    assert exported["config"] == conversion["config"]
    params = exported["type_params"]["conversion_type_params"]
    assert params["base_measure"] == {"name": "orders_count_measure", "filter": "is_valid"}
    assert params["conversion_measure"] == {"name": "orders_count_measure", "filter": "is_paid"}
    restored = MetricFlowAdapter().parse(output).metadata["metricflow_conversion_metrics"]["rate"]
    assert restored[field] == value
    assert spec["source_definition"] == conversion
