"""Regression coverage for Cortex/Ossie interoperability losses."""

import duckdb
import pytest
import yaml

from sidemantic import Dimension, Metric, Model, SemanticLayer
from sidemantic.adapters.snowflake import SnowflakeAdapter
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph


def roundtrip(tmp_path, document):
    source = tmp_path / "source.yaml"
    source.write_text(yaml.safe_dump(document))
    adapter = SnowflakeAdapter()
    graph = adapter.parse(source)
    output = tmp_path / "different_name.yaml"
    adapter.export(graph, output)
    return graph, adapter.parse(output), yaml.safe_load(output.read_text())


def test_compound_keys_types_sources_and_context(tmp_path):
    document = {
        "name": "retail",
        "description": "Original model",
        "tables": [
            {
                "name": "orders",
                "base_table": {"definition": "SELECT * FROM raw_orders"},
                "primary_key": {"columns": ["tenant", "id"]},
                "unique_keys": [{"columns": ["tenant", "external_id"]}],
                "synonyms": ["purchases"],
                "dimensions": [
                    {"name": "tenant", "expr": "tenant", "data_type": "NUMBER(38,0)"},
                    {"name": "id", "expr": "id", "data_type": "DECIMAL(12, 2)"},
                ],
                "time_dimensions": [{"name": "created", "expr": "created", "data_type": "TIMESTAMP_TZ(9)"}],
                "facts": [
                    {"name": "amount", "expr": "amount", "data_type": "NUMBER(12,2)", "default_aggregation": "sum"}
                ],
            }
        ],
    }
    before, after, exported = roundtrip(tmp_path, document)
    model = after.models["orders"]
    assert model.primary_key == ["tenant", "id"]
    assert model.unique_keys == [["tenant", "external_id"]]
    assert model.sql == "SELECT * FROM raw_orders"
    assert model.table is None
    assert [d.type for d in model.dimensions] == ["numeric", "numeric", "time"]
    assert exported["name"] == "retail"
    assert exported["description"] == "Original model"
    assert exported["tables"] == document["tables"]
    # Native edits must supersede retained type metadata.
    before.models["orders"].dimensions[0].type = "boolean"
    SnowflakeAdapter().export(before, tmp_path / "edited.yaml")
    assert (
        yaml.safe_load((tmp_path / "edited.yaml").read_text())["tables"][0]["dimensions"][0]["data_type"] == "BOOLEAN"
    )


def test_compound_inner_join_keeps_results(tmp_path):
    document = {
        "name": "retail",
        "tables": [
            {
                "name": "orders",
                "base_table": {"table": "orders"},
                "primary_key": {"columns": ["tenant", "id"]},
                "facts": [{"name": "amount", "expr": "amount", "default_aggregation": "sum"}],
            },
            {
                "name": "customers",
                "base_table": {"table": "customers"},
                "primary_key": {"columns": ["tenant", "id"]},
                "dimensions": [{"name": "region", "expr": "region", "data_type": "TEXT"}],
            },
        ],
        "relationships": [
            {
                "name": "customer",
                "left_table": "orders",
                "right_table": "customers",
                "relationship_type": "many_to_one",
                "join_type": "inner",
                "relationship_columns": [
                    {"left_column": "tenant", "right_column": "tenant"},
                    {"left_column": "customer_id", "right_column": "id"},
                ],
            }
        ],
    }
    before, after, exported = roundtrip(tmp_path, document)
    assert exported["relationships"] == document["relationships"]
    with duckdb.connect() as db:
        db.execute(
            "CREATE TABLE orders AS SELECT * FROM (VALUES (1,1,1,10), (2,1,1,20), (3,1,1,7)) t(tenant,id,customer_id,amount)"
        )
        db.execute("CREATE TABLE customers AS SELECT * FROM (VALUES (1,1,'north'), (2,1,'south')) t(tenant,id,region)")
        for graph in (before, after):
            layer = SemanticLayer()
            layer.graph = graph
            sql = layer.compile(dimensions=["customers.region"], metrics=["orders.amount"])
            assert sorted(db.execute(sql).fetchall()) == [("north", 10), ("south", 20)]


@pytest.mark.parametrize("kind", ["many_to_one", "one_to_one"])
def test_native_relationship_key_defaults_and_direction(tmp_path, kind):
    graph = SemanticGraph()
    customers = Model(
        name="customers",
        table="customers",
        primary_key=["tenant", "id"],
        dimensions=[Dimension(name="region", type="categorical")],
    )
    orders = Model(
        name="orders",
        table="orders",
        primary_key=["tenant", "id"],
        metrics=[Metric(name="amount", agg="sum", sql="amount")],
    )
    if kind == "many_to_one":
        orders.relationships = [Relationship(name="customers", type=kind, foreign_key=["tenant", "customer_id"])]
    else:
        customers.relationships = [Relationship(name="orders", type=kind, foreign_key=["tenant", "customer_id"])]
    graph.add_model(customers)
    graph.add_model(orders)
    adapter = SnowflakeAdapter()
    adapter.export(graph, tmp_path / "native.yaml")
    restored = adapter.parse(tmp_path / "native.yaml")
    source = "orders" if kind == "many_to_one" else "customers"
    rel = restored.models[source].relationships[0]
    assert rel.foreign_key_columns == ["tenant", "customer_id"]
    assert rel.primary_key_columns == ["tenant", "id"]


def test_temporal_dimension_role_and_quoted_source_survive(tmp_path):
    document = {
        "name": "quoted",
        "tables": [
            {
                "name": "audit",
                "base_table": {"database": '"warehouse.db"', "schema": '"Public"', "table": '"Audit.Events"'},
                "dimensions": [{"name": "recorded", "expr": "recorded", "data_type": "TIMESTAMP_NTZ(6)"}],
            }
        ],
    }
    before, after, exported = roundtrip(tmp_path, document)
    assert exported["tables"] == document["tables"]
    assert before.models["audit"].dimensions[0].type == "time"
    assert after.models["audit"].primary_key is None


@pytest.mark.parametrize(
    "kind,join_type",
    [
        ("one_to_many", "left_outer"),
        ("many_to_many", "left_outer"),
        ("many_to_one", "right_outer"),
        ("many_to_one", "full_outer"),
    ],
)
def test_unsupported_vendor_relationships_reject_before_write(tmp_path, kind, join_type):
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="a",
            table="a",
            primary_key="id",
            relationships=[
                Relationship(name="b", type=kind, foreign_key="id", primary_key="id", metadata={"join_type": join_type})
            ],
        )
    )
    graph.add_model(Model(name="b", table="b", primary_key="id"))
    output = tmp_path / "invalid.yaml"
    with pytest.raises(ValueError, match="[Uu]nsupported"):
        SnowflakeAdapter().export(graph, output)
    assert not output.exists()


def test_native_ossie_logical_types_export_precisely(tmp_path):
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="orders",
            dimensions=[
                Dimension(name="id", type="numeric", logical_data_type="Integer"),
                Dimension(name="date", type="time", logical_data_type="Date"),
                Dimension(name="ts", type="time", logical_data_type="DateTimeTz"),
            ],
        )
    )
    output = tmp_path / "typed.yaml"
    SnowflakeAdapter().export(graph, output)
    table = yaml.safe_load(output.read_text())["tables"][0]
    assert table["dimensions"][0]["data_type"] == "NUMBER(38,0)"
    assert [field["data_type"] for field in table["time_dimensions"]] == ["DATE", "TIMESTAMP_TZ"]


@pytest.mark.parametrize("source_type", [None, "left_outer", "inner"])
@pytest.mark.parametrize("edited_field", ["join_type", "bsl_how", "both"])
def test_edited_join_type_overrides_imported_default(tmp_path, source_type, edited_field):
    relationship = {
        "left_table": "orders",
        "right_table": "customers",
        "relationship_type": "many_to_one",
        "relationship_columns": [{"left_column": "customer_id", "right_column": "id"}],
    }
    if source_type:
        relationship["join_type"] = source_type
    document = {
        "name": "retail",
        "tables": [{"name": name, "base_table": {"table": name}} for name in ("orders", "customers")],
        "relationships": [relationship],
    }
    graph, _, untouched = roundtrip(tmp_path, document)
    assert untouched["relationships"] == [relationship]
    rel = graph.models["orders"].relationships[0]
    expected = "left_outer" if source_type == "inner" else "inner"
    for field in ("join_type", "bsl_how") if edited_field == "both" else [edited_field]:
        rel.metadata[field] = expected.removesuffix("_outer") if field == "bsl_how" else expected
    output = tmp_path / "edited.yaml"
    adapter = SnowflakeAdapter()
    adapter.export(graph, output)
    assert yaml.safe_load(output.read_text())["relationships"][0]["join_type"] == expected
    assert adapter.parse(output).models["orders"].relationships[0].metadata["bsl_how"] == expected.removesuffix(
        "_outer"
    )
