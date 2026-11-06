"""Composite GoodData keys must constrain joins, not just survive metadata export."""

import json

import duckdb
import pytest

from sidemantic.adapters.gooddata import GoodDataAdapter, GoodDataParseError
from sidemantic.sql.generator import SQLGenerator


def model_payload():
    return {
        "ldm": {
            "datasets": [
                {
                    "id": "customers",
                    "dataSourceTableId": "customers",
                    "grain": [{"id": "region", "type": "attribute"}, {"id": "code", "type": "attribute"}],
                    "attributes": [
                        {"id": "region", "sourceColumn": "pk_region"},
                        {"id": "code", "sourceColumn": "pk_code"},
                        {"id": "customer_name", "sourceColumn": "display_name"},
                    ],
                },
                {
                    "id": "orders",
                    "dataSourceTableId": "orders",
                    "grain": [{"id": "order_id", "type": "attribute"}],
                    "attributes": [{"id": "order_id", "sourceColumn": "id"}],
                    "facts": [{"id": "revenue", "sourceColumn": "amount", "sourceColumnDataType": "NUMERIC"}],
                    "references": [
                        {
                            "identifier": {"id": "customers", "type": "dataset"},
                            "sources": [
                                {"column": "fk_code", "target": {"id": "code", "type": "attribute"}},
                                {"column": "fk_region", "target": {"id": "region", "type": "attribute"}},
                            ],
                        }
                    ],
                },
            ]
        }
    }


def test_compound_grain_and_reordered_targets_execute_correct_join(tmp_path):
    source = tmp_path / "ldm.json"
    source.write_text(json.dumps(model_payload()))
    adapter = GoodDataAdapter()
    graph = adapter.parse(source)
    assert graph.models["customers"].primary_key == ["pk_region", "pk_code"]
    relationship = graph.models["orders"].relationships[0]
    assert relationship.foreign_key == ["fk_code", "fk_region"]
    assert relationship.primary_key == ["pk_code", "pk_region"]
    sql = SQLGenerator(graph).generate(metrics=["orders.revenue"], dimensions=["customers.customer_name"])
    with duckdb.connect() as conn:
        conn.execute(
            "CREATE TABLE customers AS SELECT * FROM (VALUES ('north', 1, 'Alice'), ('south', 1, 'Bob'), ('north', 2, 'Carol')) t(pk_region, pk_code, display_name)"
        )
        conn.execute(
            "CREATE TABLE orders AS SELECT * FROM (VALUES (1, 'north', 1, 10), (2, 'south', 1, 20), (3, 'north', 2, 30)) t(id, fk_region, fk_code, amount)"
        )
        assert sorted(conn.execute(sql).fetchall()) == [("Alice", 10), ("Bob", 20), ("Carol", 30)]
    output = tmp_path / "exported.json"
    adapter.export(graph, output)
    exported = json.loads(output.read_text())["ldm"]["datasets"]
    assert [item["id"] for item in exported[0]["grain"]] == ["region", "code"]
    assert exported[1]["references"][0]["sources"] == model_payload()["ldm"]["datasets"][1]["references"][0]["sources"]


def test_unknown_explicit_target_is_rejected_instead_of_using_default_key(tmp_path):
    payload = model_payload()
    payload["ldm"]["datasets"][1]["references"][0]["sources"][0]["target"]["id"] = "missing"
    source = tmp_path / "ldm.json"
    source.write_text(json.dumps(payload))
    with pytest.raises(GoodDataParseError, match="target attribute 'missing' does not exist"):
        GoodDataAdapter().parse(source)


def test_key_uses_attribute_column_not_display_label_column(tmp_path):
    payload = model_payload()
    attribute = payload["ldm"]["datasets"][0]["attributes"][0]
    attribute["labels"] = [{"id": "region_label", "sourceColumn": "region_description"}]
    attribute["defaultView"] = {"id": "region_label", "type": "label"}
    source = tmp_path / "labels.json"
    source.write_text(json.dumps(payload))
    adapter = GoodDataAdapter()
    graph = adapter.parse(source)
    assert graph.models["customers"].get_dimension("region").sql == "region_description"
    assert graph.models["customers"].primary_key_columns == ["pk_region", "pk_code"]
    assert graph.models["orders"].relationships[0].primary_key_columns == ["pk_code", "pk_region"]
    output = tmp_path / "out.json"
    adapter.export(graph, output)
    exported = json.loads(output.read_text())["ldm"]["datasets"][0]["attributes"][0]
    assert exported["sourceColumn"] == "pk_region"
    assert exported["labels"][0]["sourceColumn"] == "region_description"


def test_graph_level_metrics_are_not_silently_dropped(tmp_path):
    from sidemantic.core.metric import Metric
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_metric(Metric(name="Total", agg="count"))
    with pytest.raises(ValueError, match="graph-level metrics"):
        GoodDataAdapter().export(graph, tmp_path / "out.json")


@pytest.mark.parametrize("edited_bindings", [("sql",), ("native_key",), ("model_key",), ("sql", "model_key")])
def test_unlabelled_attribute_rebinding_updates_key_and_generated_label(tmp_path, edited_bindings):
    source = tmp_path / "ldm.json"
    source.write_text(json.dumps(model_payload()))
    adapter = GoodDataAdapter()
    graph = adapter.parse(source)
    model = graph.models["customers"]
    dimension = model.get_dimension("region")
    if "sql" in edited_bindings:
        dimension.sql = "new_region"
    if "native_key" in edited_bindings:
        dimension.metadata["gooddata"]["key_source_column"] = "new_region"
    if "model_key" in edited_bindings:
        model.primary_key = ["new_region", "pk_code"]
    destination = tmp_path / "rebound.json"
    adapter.export(graph, destination)
    attribute = json.loads(destination.read_text())["ldm"]["datasets"][0]["attributes"][0]
    assert attribute["sourceColumn"] == "new_region"
    assert attribute["labels"][0]["sourceColumn"] == "new_region"
    imported = adapter.parse(destination)
    assert imported.models["customers"].get_dimension("region").sql == "new_region"
    assert imported.models["customers"].primary_key == ["new_region", "pk_code"]
    assert imported.models["orders"].relationships[0].primary_key == ["pk_code", "new_region"]


def test_unlabelled_attribute_rejects_conflicting_binding_edits_before_writing(tmp_path):
    source = tmp_path / "ldm.json"
    source.write_text(json.dumps(model_payload()))
    adapter = GoodDataAdapter()
    graph = adapter.parse(source)
    model = graph.models["customers"]
    model.get_dimension("region").sql = "new_region"
    model.primary_key = ["another_region", "pk_code"]
    destination = tmp_path / "output"
    with pytest.raises(ValueError, match="conflicting SQL and key source edits"):
        adapter.export(graph, destination)
    assert not destination.exists()
