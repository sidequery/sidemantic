"""Unsupported security must fail visibly; native type metadata must survive export."""

import pytest
import yaml

from sidemantic.adapters.thoughtspot import ThoughtSpotAdapter


@pytest.mark.parametrize("kind", ["table", "worksheet", "model"])
def test_security_rules_reject_import(tmp_path, kind):
    source = tmp_path / "restricted.tml"
    source.write_text(
        yaml.safe_dump({kind: {"name": "orders", "rls_rules": [{"name": "tenant", "expr": "tenant = 1"}]}})
    )
    with pytest.raises(ValueError, match="rls_rules.*import rejected"):
        ThoughtSpotAdapter().parse(source)


def test_sql_view_is_explicitly_rejected(tmp_path):
    source = tmp_path / "view.sql_view.tml"
    source.write_text("sql_view:\n  name: orders\n  sql_query: SELECT id FROM orders\n")
    with pytest.raises(ValueError, match="sql_view documents are not supported"):
        ThoughtSpotAdapter().parse(source)


@pytest.mark.parametrize("nested_type", [False, True])
def test_table_roundtrip_preserves_connection_and_exact_types(tmp_path, nested_type):
    columns = []
    for name, dtype, role in [("id", "INT64", "ATTRIBUTE"), ("amount", "DECIMAL(20,4)", "MEASURE")]:
        column = {"name": name, "db_column_name": name, "properties": {"column_type": role, "aggregation": "SUM"}}
        if nested_type:
            column["db_column_properties"] = {"data_type": dtype}
        else:
            column["data_type"] = dtype
        columns.append(column)
    source = tmp_path / "orders.table.tml"
    source.write_text(
        yaml.safe_dump(
            {"table": {"name": "orders", "db_table": "orders", "connection": {"name": "Warehouse"}, "columns": columns}}
        )
    )
    adapter = ThoughtSpotAdapter()
    graph = adapter.parse(source)
    destination = tmp_path / "out.tml"
    adapter.export(graph, destination)
    output = yaml.safe_load(destination.read_text())["table"]
    assert output["connection"] == {"name": "Warehouse"}
    for original, exported in zip(columns, output["columns"], strict=True):
        key = "db_column_properties" if nested_type else "data_type"
        assert exported[key] == original[key]
    graph.models["orders"].dimensions[0].type = "categorical"
    adapter.export(graph, destination)
    assert yaml.safe_load(destination.read_text())["table"]["columns"][0]["data_type"] == "VARCHAR"


def test_table_joins_remain_native_table_and_reject_stale_replay(tmp_path):
    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/table_joins.table.tml")
    destination = tmp_path / "out.tml"
    adapter.export(graph, destination)
    output = yaml.safe_load(destination.read_text())["table"]
    assert output["joins_with"][0]["on"] == "[orders::customer_id] = [customers::id]"
    graph.models["orders"].relationships[0].foreign_key = "other_key"
    with pytest.raises(ValueError, match="edited native joins_with"):
        adapter.export(graph, destination)


@pytest.mark.parametrize("field_kind", ["dimension", "metric"])
def test_native_table_expression_edits_reject_before_writing(tmp_path, field_kind):
    adapter = ThoughtSpotAdapter()
    graph = adapter.parse("tests/fixtures/thoughtspot/table_joins.table.tml")
    model = graph.models["orders"]
    field = model.dimensions[0] if field_kind == "dimension" else model.metrics[0]
    field.sql = f"{field.sql} * 2"
    destination = tmp_path / "output"
    with pytest.raises(ValueError, match="SQL expression.*native table column"):
        adapter.export(graph, destination)
    assert not destination.exists()


def test_multimodel_file_output_rejects_instead_of_dropping_models(tmp_path):
    from sidemantic.core.model import Model
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(Model(name="orders", table="orders"))
    graph.add_model(Model(name="customers", table="customers"))
    destination = tmp_path / "out.tml"
    with pytest.raises(ValueError, match="output directory for multiple models"):
        ThoughtSpotAdapter().export(graph, destination)
    assert not destination.exists()


def test_graph_level_metrics_are_not_silently_dropped(tmp_path):
    from sidemantic.core.metric import Metric
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_metric(Metric(name="Total", agg="count"))
    with pytest.raises(ValueError, match="graph-level metrics"):
        ThoughtSpotAdapter().export(graph, tmp_path / "out.tml")
