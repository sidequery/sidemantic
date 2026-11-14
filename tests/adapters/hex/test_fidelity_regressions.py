"""Regression checks for native semantics that graph-only comparisons miss."""

import duckdb
import pytest
import yaml

from sidemantic import SemanticLayer
from sidemantic.adapters.hex import HexAdapter
from sidemantic.core.model import Model
from sidemantic.core.semantic_graph import SemanticGraph


def test_native_types_and_statistical_aggregates_roundtrip(tmp_path):
    aggregates = ["median", "stddev", "stddev_pop", "variance", "variance_pop"]
    source = tmp_path / "source.yaml"
    source.write_text(
        yaml.safe_dump(
            {
                "id": "orders",
                "base_sql_table": "orders",
                "dimensions": [{"id": "flag", "type": "boolean"}, {"id": "created", "type": "timestamp_naive"}],
                "measures": [{"id": name, "func": name, "of": "amount"} for name in aggregates],
            }
        )
    )
    adapter = HexAdapter()
    graph = adapter.parse(source)
    adapter.export(graph, tmp_path / "export")
    actual = yaml.safe_load((tmp_path / "export/orders.yml").read_text())
    assert [dimension["type"] for dimension in actual["dimensions"]] == ["boolean", "timestamp_naive"]
    assert [metric["func"] for metric in actual["measures"]] == aggregates
    layer = SemanticLayer(auto_register=False)
    layer.graph = adapter.parse(tmp_path / "export")
    with duckdb.connect() as connection:
        connection.execute("CREATE TABLE orders AS SELECT * FROM (VALUES (1), (3), (9)) t(amount)")
        assert connection.execute(layer.compile(metrics=["orders.median"])).fetchone() == (3.0,)


def test_compound_non_equi_join_survives_query_and_roundtrip(tmp_path):
    source = tmp_path / "source.yaml"
    source.write_text(
        yaml.safe_dump_all(
            [
                {
                    "id": "orders",
                    "base_sql_table": "orders",
                    "dimensions": [{"id": "id", "unique": True}],
                    "measures": [{"id": "revenue", "func": "sum", "of": "amount"}],
                    "relations": [
                        {
                            "id": "tiers",
                            "type": "many_to_one",
                            "join_sql": "amount > ${tiers}.threshold AND region = ${tiers}.region",
                        }
                    ],
                },
                {"id": "tiers", "base_sql_table": "tiers", "dimensions": [{"id": "id", "unique": True}]},
            ]
        )
    )
    adapter = HexAdapter()
    graph = adapter.parse(source)
    adapter.export(graph, tmp_path / "export")
    for candidate in (graph, adapter.parse(tmp_path / "export")):
        layer = SemanticLayer(auto_register=False)
        layer.graph = candidate
        with duckdb.connect() as connection:
            connection.execute(
                "CREATE TABLE orders AS SELECT * FROM (VALUES (1,5,'e'),(2,15,'e'),(3,30,'w'),(4,25,'e')) t(id,amount,region)"
            )
            connection.execute(
                "CREATE TABLE tiers AS SELECT * FROM (VALUES (1,10,'e'),(2,20,'w')) t(id,threshold,region)"
            )
            rows = connection.execute(
                layer.compile(metrics=["orders.revenue"], dimensions=["orders.id", "tiers.id"])
            ).fetchall()
            assert set(rows) == {(1, None, 5), (2, 1, 15), (3, 2, 30), (4, 1, 25)}


def test_multiple_models_cannot_silently_export_one(tmp_path):
    graph = SemanticGraph()
    graph.add_model(Model(name="orders", table="orders"))
    graph.add_model(Model(name="customers", table="customers"))
    destination = tmp_path / "out.yaml"
    with pytest.raises(ValueError, match="use a directory"):
        HexAdapter().export(graph, destination)
    assert not destination.exists()


def test_unknown_aggregation_is_rejected(tmp_path):
    source = tmp_path / "source.yaml"
    source.write_text("id: orders\nbase_sql_table: orders\nmeasures:\n- id: bad\n  func: arbitrary\n")
    with pytest.raises(ValueError, match="Unsupported Hex aggregation"):
        HexAdapter().parse(source)


def test_join_literals_are_not_interpreted_as_references(tmp_path):
    source = tmp_path / "source.yml"
    source.write_text(
        yaml.safe_dump(
            {
                "id": "orders",
                "base_sql_table": "orders",
                "relations": [
                    {"id": "tiers", "join_sql": "amount > ${tiers.threshold} AND label = '${tiers}.label {from}.text'"}
                ],
            }
        )
    )
    adapter = HexAdapter()
    graph = adapter.parse(source)
    adapter.export(graph, tmp_path / "out.yml")
    restored = yaml.safe_load((tmp_path / "out.yml").read_text())
    assert (
        restored["relations"][0]["join_sql"] == "amount > ${tiers}.threshold AND label = '${tiers}.label {from}.text'"
    )


def test_native_edits_override_visibility_and_semi_additive_metadata(tmp_path):
    source = tmp_path / "source.yml"
    source.write_text(
        yaml.safe_dump(
            {
                "id": "orders",
                "base_sql_table": "orders",
                "dimensions": [{"id": "day", "type": "date", "visibility": "private"}],
                "measures": [
                    {
                        "id": "balance",
                        "func": "sum",
                        "of": "amount",
                        "visibility": "private",
                        "semi_additive": {"over": [{"dimension": "day", "pick": "min"}]},
                    }
                ],
            }
        )
    )
    adapter = HexAdapter()
    graph = adapter.parse(source)
    model = graph.models["orders"]
    model.dimensions[0].public = True
    model.metrics[0].public = True
    model.metrics[0].non_additive_dimension = None
    adapter.export(graph, tmp_path / "out.yml")
    restored = yaml.safe_load((tmp_path / "out.yml").read_text())
    assert "visibility" not in restored["dimensions"][0]
    assert "visibility" not in restored["measures"][0]
    assert "semi_additive" not in restored["measures"][0]


@pytest.mark.parametrize(
    "source_type,granularity,expected_type",
    [("date", "hour", "timestamp_tz"), ("timestamp_naive", "day", "date"), ("timestamp_tz", "day", "date")],
)
def test_time_granularity_edits_override_imported_type(tmp_path, source_type, granularity, expected_type):
    source = tmp_path / "source.yml"
    source.write_text(
        yaml.safe_dump(
            {"id": "events", "base_sql_table": "events", "dimensions": [{"id": "created", "type": source_type}]}
        )
    )
    adapter = HexAdapter()
    graph = adapter.parse(source)
    graph.models["events"].dimensions[0].granularity = granularity
    output = tmp_path / "out.yml"
    adapter.export(graph, output)
    assert yaml.safe_load(output.read_text())["dimensions"][0]["type"] == expected_type
    assert adapter.parse(output).models["events"].dimensions[0].granularity == granularity
