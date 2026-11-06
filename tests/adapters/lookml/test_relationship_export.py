"""LookML explore export preserves join predicates and independent scopes."""

import duckdb
import lkml
import pytest
import yaml

from sidemantic import Dimension, Metric, Model, SemanticLayer
from sidemantic.adapters.lookml import LookMLAdapter
from sidemantic.adapters.snowflake import SnowflakeAdapter
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.fidelity import capture_import_report


def test_native_compound_transitive_joins_execute_after_roundtrip(tmp_path):
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key=["tenant", "id"],
            metrics=[Metric(name="amount", agg="sum", sql="amount")],
            relationships=[
                Relationship(
                    name="customers",
                    type="many_to_one",
                    foreign_key=["tenant", "customer_id"],
                    metadata={"join_type": "inner"},
                )
            ],
        )
    )
    graph.add_model(
        Model(
            name="customers",
            table="customers",
            primary_key=["tenant", "id"],
            relationships=[Relationship(name="regions", type="many_to_one", foreign_key="region_id")],
        )
    )
    graph.add_model(
        Model(
            name="regions",
            table="regions",
            primary_key="id",
            dimensions=[Dimension(name="name", type="categorical", sql="name")],
        )
    )
    adapter = LookMLAdapter()
    output = tmp_path / "retail.lkml"
    adapter.export(graph, output)
    data = lkml.load(output.read_text())
    orders = next(explore for explore in data["explores"] if explore["name"] == "orders")
    assert len(orders["joins"]) == 2
    assert (
        orders["joins"][0]["sql_on"]
        == "${orders.tenant} = ${customers.tenant} AND ${orders.customer_id} = ${customers.id}"
    )
    assert orders["joins"][0]["type"] == "inner"
    assert orders["joins"][1]["sql_on"] == "${customers.region_id} = ${regions.id}"
    with capture_import_report() as report:
        restored = adapter.parse(output)
    assert report.readiness == "ready"
    assert restored.models["orders"].relationships[0].foreign_key_columns == ["tenant", "customer_id"]
    with duckdb.connect() as db:
        db.execute(
            "CREATE TABLE orders AS SELECT * FROM (VALUES (1,1,1,10), (2,1,1,20), (3,1,1,7)) t(tenant,id,customer_id,amount)"
        )
        db.execute("CREATE TABLE customers AS SELECT * FROM (VALUES (1,1,1), (2,1,2)) t(tenant,id,region_id)")
        db.execute("CREATE TABLE regions AS SELECT * FROM (VALUES (1,'north'), (2,'south')) t(id,name)")
        layer = SemanticLayer()
        layer.graph = restored
        assert sorted(db.execute(layer.compile(dimensions=["regions.name"], metrics=["orders.amount"])).fetchall()) == [
            ("north", 10),
            ("south", 20),
        ]
    adapter.export(restored, tmp_path / "again.lkml")
    assert lkml.load((tmp_path / "again.lkml").read_text())["explores"] == data["explores"]


def test_imported_explores_keep_distinct_filters_aliases_and_join_types(tmp_path):
    source = tmp_path / "scoped.lkml"
    source.write_text("""
view: orders { sql_table_name: orders ;; dimension: customer_id {sql: ${TABLE}.customer_id ;;} }
view: customers { sql_table_name: customers ;; dimension: id { primary_key: yes sql: ${TABLE}.id ;;} }
explore: complete { from: orders sql_always_where: ${complete.customer_id} > 0 ;;
  join: buyer { from: customers type: inner sql_on: ${complete.customer_id} = ${buyer.id} ;; } }
explore: all_orders { from: orders
  join: customer { from: customers type: left_outer sql_on: ${all_orders.customer_id} = ${customer.id} ;; } }
""")
    adapter = LookMLAdapter()
    graph = adapter.parse(source)
    output = tmp_path / "export.lkml"
    adapter.export(graph, output)
    assert lkml.load(output.read_text())["explores"] == lkml.load(source.read_text())["explores"]
    graph.models["orders"].relationships[0].foreign_key = "different_customer_id"
    with pytest.raises(ValueError, match="relationships changed"):
        adapter.export(graph, output)


@pytest.mark.parametrize(
    "predicate",
    ["${orders.customer_id} != ${customers.id}", "${orders.customer_id} = ${customers.id} AND ${customers.id} > 0"],
)
def test_unrepresentable_predicate_is_diagnosed_and_retained(tmp_path, predicate):
    source = tmp_path / "unsupported.lkml"
    source.write_text(
        "view: orders {}\nview: customers {}\nexplore: orders { join: customers { sql_on: " + predicate + " ;; } }"
    )
    adapter = LookMLAdapter()
    with capture_import_report() as report:
        graph = adapter.parse(source)
    assert report.is_blocked
    assert not graph.models["orders"].relationships
    output = tmp_path / "out.lkml"
    adapter.export(graph, output)
    assert lkml.load(output.read_text())["explores"][0]["joins"][0]["sql_on"] == predicate


def test_native_unsupported_join_rejected_before_writing(tmp_path):
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="orders",
            relationships=[
                Relationship(name="customers", type="many_to_one", foreign_key=["tenant", "cid"], primary_key="id")
            ],
        )
    )
    graph.add_model(Model(name="customers", table="customers"))
    target = tmp_path / "invalid.lkml"
    with pytest.raises(ValueError, match="mismatched join keys"):
        LookMLAdapter().export(graph, target)
    assert not target.exists()


def test_missing_join_target_rejected_before_writing(tmp_path):
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="orders",
            relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
        )
    )
    target = tmp_path / "incomplete.lkml"
    with pytest.raises(ValueError, match="targets a missing model"):
        LookMLAdapter().export(graph, target)
    assert not target.exists()


def test_composite_key_encoding_prevents_concatenation_collisions():
    expression = LookMLAdapter._composite_key_sql(["a", "b"]).replace("${a}", "a").replace("${b}", "b")
    with duckdb.connect() as db:
        rows = db.execute(
            "SELECT " + expression + " FROM (VALUES ('ab','c'), ('a','bc'), (NULL,''), ('','')) t(a,b)"
        ).fetchall()
    assert len(set(rows)) == 4


def test_graph_only_metric_rejects_with_diagnostic(tmp_path):
    from sidemantic.fidelity import capture_export_report

    graph = SemanticGraph()
    graph.add_model(Model(name="orders", table="orders", metrics=[Metric(name="amount", agg="sum", sql="amount")]))
    graph.add_metric(Metric(name="all_revenue", type="derived", sql="orders.amount"))
    output = tmp_path / "metrics.lkml"
    with capture_export_report() as report, pytest.raises(ValueError, match="graph-level metrics"):
        LookMLAdapter().export(graph, output)
    assert report.is_blocked
    assert report.notes[0].construct == "graph_metric"
    assert not output.exists()


def test_owned_metric_graph_alias_is_not_rejected(tmp_path):
    graph = SemanticGraph()
    metric = Metric(name="amount", agg="sum", sql="amount")
    graph.add_model(Model(name="orders", table="orders", metrics=[metric]))
    graph.metrics["orders.amount"] = metric
    output = tmp_path / "owned.lkml"
    LookMLAdapter().export(graph, output)
    assert lkml.load(output.read_text())["views"][0]["measures"][0]["name"] == "amount"


@pytest.mark.parametrize("edit", [None, "runtime", "declaration", "both", "conflict"])
def test_snowflake_join_edits_survive_lookml_conversion(tmp_path, edit):
    source = tmp_path / "source.yaml"
    source.write_text(
        yaml.safe_dump(
            {
                "name": "retail",
                "tables": [
                    {
                        "name": "orders",
                        "base_table": {"table": "orders"},
                        "primary_key": {"columns": ["id"]},
                        "dimensions": [{"name": "id", "expr": "id", "data_type": "NUMBER"}],
                        "facts": [{"name": "amount", "expr": "amount", "default_aggregation": "sum"}],
                    },
                    {
                        "name": "customers",
                        "base_table": {"table": "customers"},
                        "primary_key": {"columns": ["id"]},
                        "dimensions": [{"name": "region", "expr": "region", "data_type": "TEXT"}],
                    },
                ],
                "relationships": [
                    {
                        "left_table": "orders",
                        "right_table": "customers",
                        "join_type": "inner",
                        "relationship_type": "many_to_one",
                        "relationship_columns": [{"left_column": "customer_id", "right_column": "id"}],
                    }
                ],
            }
        )
    )
    graph = SnowflakeAdapter().parse(source)
    metadata = graph.models["orders"].relationships[0].metadata
    if edit in {"runtime", "both", "conflict"}:
        metadata["bsl_how"] = "left"
    if edit in {"declaration", "both", "conflict"}:
        metadata["join_type"] = "full_outer" if edit == "conflict" else "left_outer"
    output = tmp_path / "retail.lkml"
    adapter = LookMLAdapter()
    if edit == "conflict":
        with pytest.raises(ValueError, match="Conflicting LookML join types"):
            adapter.export(graph, output)
        assert not output.exists()
        return

    adapter.export(graph, output)
    explore = next(item for item in lkml.load(output.read_text())["explores"] if item["name"] == "orders")
    assert explore["joins"][0]["type"] == ("left_outer" if edit else "inner")
    restored = adapter.parse(output)
    expected = [(1, "north", 10), (2, None, 20)] if edit else [(1, "north", 10)]
    with duckdb.connect() as db:
        db.execute("CREATE TABLE orders AS SELECT * FROM (VALUES (1,1,10), (2,2,20)) t(id,customer_id,amount)")
        db.execute("CREATE TABLE customers AS SELECT 1 AS id, 'north' AS region")
        # Runtime-only edits must produce the same results before and after export.
        candidates = [restored] if edit == "declaration" else [graph, restored]
        for candidate in candidates:
            layer = SemanticLayer()
            layer.graph = candidate
            sql = layer.compile(dimensions=["orders.id", "customers.region"], metrics=["orders.amount"])
            assert sorted(db.execute(sql).fetchall()) == expected
