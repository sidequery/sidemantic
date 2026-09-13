"""Export contracts for relationship roles with canonical target models."""

import pytest
import yaml

from sidemantic import Dimension, Model, Relationship
from sidemantic.adapters.atscale_sml import AtScaleSMLAdapter
from sidemantic.adapters.bsl import BSLAdapter
from sidemantic.adapters.cube import CubeAdapter
from sidemantic.adapters.gooddata import GoodDataAdapter
from sidemantic.adapters.hex import HexAdapter
from sidemantic.adapters.holistics import HolisticsAdapter
from sidemantic.adapters.metricflow import MetricFlowAdapter
from sidemantic.adapters.omni import OmniAdapter
from sidemantic.adapters.osi import OSIAdapter
from sidemantic.adapters.ossie import OssieAdapter
from sidemantic.adapters.snowflake import SnowflakeAdapter
from sidemantic.adapters.thoughtspot import ThoughtSpotAdapter
from sidemantic.adapters.tmdl import TMDLAdapter
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.interchange.ossie import OssieSynthesisError


def _aliased_graph() -> SemanticGraph:
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="order_id",
            dimensions=[
                Dimension(name="order_id", type="numeric", sql="order_id"),
                Dimension(name="customer_id", type="numeric", sql="customer_id"),
            ],
            relationships=[
                Relationship(
                    name="billing_customer",
                    target_model="customers",
                    type="many_to_one",
                    foreign_key="customer_id",
                    primary_key="customer_key",
                )
            ],
        )
    )
    graph.add_model(
        Model(
            name="customers",
            table="customers",
            primary_key="customer_key",
            dimensions=[Dimension(name="customer_key", type="numeric", sql="customer_key")],
        )
    )
    return graph


def test_cube_export_rejects_role_alias_without_independent_target_slot(tmp_path):
    with pytest.raises(
        ValueError,
        match="Cube export cannot represent relationship role 'billing_customer'.*target cube 'customers'",
    ):
        CubeAdapter().export(_aliased_graph(), tmp_path / "cube.yml")


def test_tmdl_export_preserves_role_and_uses_canonical_target(tmp_path):
    TMDLAdapter().export(_aliased_graph(), tmp_path)

    relationships = (tmp_path / "definition" / "relationships.tmdl").read_text()
    model = (tmp_path / "definition" / "model.tmdl").read_text()
    assert "relationship orders_billing_customer" in relationships
    assert "fromColumn: orders[customer_id]" in relationships
    assert "toColumn: customers[customer_key]" in relationships
    assert "ref relationship orders_billing_customer" in model


def test_holistics_export_preserves_role_and_uses_canonical_target(tmp_path):
    HolisticsAdapter().export(_aliased_graph(), tmp_path)

    relationships = (tmp_path / "relationships.aml").read_text()
    assert "Relationship orders_billing_customer" in relationships
    assert "from: r(orders.customer_id)" in relationships
    assert "to: r(customers.customer_key)" in relationships


def test_atscale_export_preserves_role_and_uses_canonical_dimension(tmp_path):
    AtScaleSMLAdapter().export(_aliased_graph(), tmp_path)

    model = yaml.safe_load((tmp_path / "models" / "orders.yml").read_text())
    relationship = model["relationships"][0]
    assert relationship["unique_name"] == "orders_billing_customer"
    assert relationship["to"] == {"dimension": "customers", "level": "customer_key"}
    assert "customers" in model["dimensions"]
    assert "billing_customer" not in model["dimensions"]


@pytest.mark.parametrize("adapter_class", [OSIAdapter, OssieAdapter])
def test_ossie_export_preserves_role_name_and_uses_canonical_target(tmp_path, adapter_class):
    output = tmp_path / "osi.yaml"
    graph = _aliased_graph()
    adapter = adapter_class(export_scope_name="commerce", expression_dialect="ANSI_SQL")

    with pytest.raises(OssieSynthesisError) as error:
        adapter.export(graph, output, portable_only=True)
    assert any(d.code == "ossie.synthesis.relationship_semantics_unsupported" for d in error.value.diagnostics)
    assert not output.exists()

    with pytest.warns(UserWarning, match="requires Sidemantic runtime extension"):
        adapter.export(graph, output)
    restored = adapter.parse(output)
    relationship = restored.models["orders"].relationships[0]
    assert relationship == graph.models["orders"].relationships[0]
    assert relationship.name == "billing_customer"
    assert relationship.related_model == "customers"
    path = restored.find_relationship_path("orders", "billing_customer")
    assert len(path) == 1
    assert path[0].to_target_model == "customers"
    assert path[0].from_columns == ["customer_id"]
    assert path[0].to_columns == ["customer_key"]


def test_gooddata_export_rejects_role_alias_without_independent_target_slot(tmp_path):
    with pytest.raises(
        ValueError,
        match="GoodData export cannot represent relationship role 'billing_customer'.*target dataset 'customers'",
    ):
        GoodDataAdapter().export(_aliased_graph(), tmp_path / "ldm.json")


def test_metricflow_resolution_preserves_entity_role_and_canonical_model():
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="orders",
            relationships=[Relationship(name="customer", type="many_to_one", foreign_key="customer_id")],
        )
    )
    graph.add_model(Model(name="customers", table="customers"))
    adapter = MetricFlowAdapter()

    adapter._resolve_relationship_names(graph)

    relationship = graph.models["orders"].relationships[0]
    assert relationship.name == "customer"
    assert relationship.related_model == "customers"


def test_metricflow_entity_role_and_canonical_model_round_trip(tmp_path):
    adapter = MetricFlowAdapter()
    graph = adapter.parse("tests/fixtures/metricflow/semantic_models.yml")
    output = tmp_path / "semantic_models.yml"

    adapter.export(graph, output)

    exported = yaml.safe_load(output.read_text())["semantic_models"]
    orders = next(model for model in exported if model["name"] == "orders")
    customers = next(model for model in exported if model["name"] == "customers")
    assert {entity["name"] for entity in orders["entities"] if entity["type"] == "foreign"} == {"customer"}
    assert {entity["name"] for entity in customers["entities"] if entity["type"] == "primary"} == {"customer"}

    reparsed = adapter.parse(output)
    relationship = reparsed.models["orders"].relationships[0]
    assert relationship.name == "customer"
    assert relationship.related_model == "customers"


def test_bsl_export_preserves_join_role_and_uses_canonical_model(tmp_path):
    output = tmp_path / "models.yml"
    BSLAdapter().export(_aliased_graph(), output)

    exported = yaml.safe_load(output.read_text())
    assert exported["orders"]["joins"]["billing_customer"]["model"] == "customers"


def test_gooddata_primary_key_resolution_uses_canonical_target():
    graph = _aliased_graph()
    relationship = graph.models["orders"].relationships[0]
    relationship.primary_key = None

    GoodDataAdapter()._apply_reference_primary_keys(graph)

    assert relationship.primary_key == "customer_key"


def test_omni_topic_does_not_duplicate_existing_aliased_target(tmp_path):
    graph = _aliased_graph()
    graph.topics = []
    topic = tmp_path / "orders.topic.yaml"
    topic.write_text("base_view: orders\njoins:\n  customers: {}\n")

    OmniAdapter()._parse_topic(topic, graph)

    relationships = graph.models["orders"].relationships
    assert [(rel.name, rel.related_model) for rel in relationships] == [("billing_customer", "customers")]


def test_hex_relation_role_and_canonical_target_round_trip(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "orders.yml").write_text(
        """id: orders
type: model
base_sql_table: orders
dimensions:
  - id: order_id
    type: number
    unique: true
  - id: customer_id
    type: number
relations:
  - id: billing_customer
    target: customers
    type: many_to_one
    join_sql: customer_id = ${billing_customer}.customer_key
"""
    )
    (source / "customers.yml").write_text(
        """id: customers
type: model
base_sql_table: customers
dimensions:
  - id: customer_key
    type: number
    unique: true
"""
    )
    adapter = HexAdapter()

    graph = adapter.parse(source)

    relationship = graph.models["orders"].relationships[0]
    assert relationship.name == "billing_customer"
    assert relationship.related_model == "customers"
    relationship.primary_key = "customer_key"

    output = tmp_path / "exported"
    adapter.export(graph, output)
    exported = yaml.safe_load((output / "orders.yml").read_text())
    assert exported["relations"] == [
        {
            "id": "billing_customer",
            "target": "customers",
            "type": "many_to_one",
            "join_sql": "customer_id = ${billing_customer}.customer_key",
        }
    ]

    reparsed = adapter.parse(output)
    reparsed_relationship = reparsed.models["orders"].relationships[0]
    assert reparsed_relationship.name == "billing_customer"
    assert reparsed_relationship.related_model == "customers"


def test_omni_export_rejects_role_alias_without_independent_target_slot(tmp_path):
    output = tmp_path / "omni"
    with pytest.raises(
        ValueError,
        match="Omni export cannot represent relationship role 'billing_customer'.*target view 'customers'",
    ):
        OmniAdapter().export(_aliased_graph(), output)
    assert not output.exists()


def test_thoughtspot_export_preserves_path_role_and_uses_canonical_table():
    worksheet = ThoughtSpotAdapter()._export_worksheet(_aliased_graph().models["orders"])["worksheet"]

    assert {table["name"] for table in worksheet["tables"]} == {"orders", "customers"}
    join = worksheet["joins"][0]
    assert join["name"] == "orders_billing_customer"
    assert join["destination"] == "customers"
    assert "[customers::customer_key]" in join["on"]
    path = next(path for path in worksheet["table_paths"] if path["id"] == "billing_customer")
    assert path["table"] == "customers"


def test_snowflake_export_preserves_role_name_and_uses_canonical_table():
    graph = _aliased_graph()
    relationship = graph.models["orders"].relationships[0]

    exported = SnowflakeAdapter()._export_relationship(graph.models["orders"], relationship)

    assert exported["name"] == "billing_customer"
    assert exported["left_table"] == "orders"
    assert exported["right_table"] == "customers"
