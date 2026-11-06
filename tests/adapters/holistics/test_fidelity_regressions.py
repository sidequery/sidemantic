"""Unsupported AQL must never silently become a different aggregate."""

import pytest

from sidemantic.adapters.holistics import HolisticsAdapter
from sidemantic.core.model import Model
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.fidelity import capture_import_report


@pytest.mark.parametrize(
    "expression,function",
    [
        ("orders | filter(orders.amount > 100) | count(orders.id)", "filter"),
        ("orders | where(orders.amount > 100) | sum(orders.amount)", "where"),
        ("orders | group(orders.status) | sum(orders.amount)", "group"),
        ("sum(orders.amount) | of_all()", "of_all"),
        ("sum(orders.amount) | running_total()", "running_total"),
        ("sum(orders.amount) | running_total ()", "running_total"),
        ("sum(of_all(orders.amount))", "of_all"),
        ("sum(orders.amount) | relative_period(orders.day, interval(-1 month))", "relative_period"),
    ],
)
def test_scope_changing_aql_rejected_at_import(tmp_path, expression, function):
    source = tmp_path / "orders.model.aml"
    source.write_text(
        "Model orders {\n type: 'table'\n table_name: 'orders'\n"
        " measure revenue {\n type: 'number'\n definition: @aql " + expression + " ;;\n }\n}\n"
    )
    with capture_import_report() as report:
        with pytest.raises(ValueError, match=f"AQL function '{function}'"):
            HolisticsAdapter().parse(source)
    assert any(note.construct == "aql_scope" and function in note.detail for note in report.notes)


def test_single_file_export_rejects_multiple_models(tmp_path):
    graph = SemanticGraph()
    graph.add_model(Model(name="orders", table="orders"))
    graph.add_model(Model(name="customers", table="customers"))
    destination = tmp_path / "out.aml"
    with pytest.raises(ValueError, match="use a directory"):
        HolisticsAdapter().export(graph, destination)
    assert not destination.exists()


@pytest.mark.parametrize("format_name", ["hex", "holistics", "omni"])
def test_unplaced_graph_metrics_cannot_disappear(tmp_path, format_name):
    from sidemantic.adapters.hex import HexAdapter
    from sidemantic.adapters.omni import OmniAdapter
    from sidemantic.core.metric import Metric
    from sidemantic.fidelity import capture_export_report

    adapter = {"hex": HexAdapter, "holistics": HolisticsAdapter, "omni": OmniAdapter}[format_name]()
    graph = SemanticGraph()
    graph.add_model(Model(name="orders", table="orders"))
    graph.add_metric(Metric(name="revenue", type="derived", sql="SUM(orders.amount)"))
    destination = tmp_path / "out"
    with capture_export_report() as report:
        with pytest.raises(ValueError, match="Graph-level metrics"):
            adapter.export(graph, destination)
    assert not destination.exists()
    assert any(feature.feature == "graph_metrics" and feature.status == "rejected" for feature in report.features)


@pytest.mark.parametrize(
    "expression", ["'filter(example)'", 'concat("filter(example)", "x")', "'it''s running_total()'"]
)
def test_aql_strings_are_not_function_calls(expression):
    from sidemantic.adapters.holistics import _translate_aql_to_sql

    assert _translate_aql_to_sql(expression) == expression
