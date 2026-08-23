from __future__ import annotations

import pytest

from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.interchange.ossie import (
    OssieConsumerProfile,
    OssieSynthesisError,
    require_synthesized_document,
    synthesize_ossie_document,
)


def _graph() -> SemanticGraph:
    graph = SemanticGraph()
    orders = Model(
        name="orders",
        table="analytics.orders",
        primary_key="id",
        dimensions=[
            Dimension(name="id", type="numeric", sql="id", logical_data_type="Integer"),
            Dimension(name="customer_id", type="numeric", sql="customer_id", logical_data_type="Integer"),
            Dimension(
                name="loaded_at",
                type="categorical",
                sql="loaded_at",
                logical_data_type="DateTime",
                declared_is_time=False,
            ),
        ],
        metrics=[Metric(name="revenue", agg="sum", sql="amount", logical_data_type="Decimal")],
    )
    customers = Model(
        name="customers",
        table="analytics.customers",
        primary_key="id",
        dimensions=[Dimension(name="id", type="numeric", sql="id", logical_data_type="Integer")],
    )
    orders.relationships.append(
        Relationship(
            name="customers",
            edge_id="order_customer",
            type="many_to_one",
            foreign_key="customer_id",
            primary_key="id",
        )
    )
    graph.add_model(orders)
    graph.add_model(customers)
    return graph


def test_synthesis_requires_explicit_scope_and_expression_dialect() -> None:
    graph = _graph()

    with pytest.raises(ValueError, match="scope_name"):
        synthesize_ossie_document(graph, scope_name="", expression_dialect="BIGQUERY")
    with pytest.raises(ValueError, match="supported"):
        synthesize_ossie_document(graph, scope_name="commerce", expression_dialect="duckdb")


def test_synthesis_preserves_types_time_false_keys_and_edge_identity() -> None:
    result = synthesize_ossie_document(_graph(), scope_name="commerce", expression_dialect="BIGQUERY")

    assert result.valid
    data = result.document.to_parsed_data()
    semantic_model = data["semantic_model"][0]
    orders = semantic_model["datasets"][0]
    loaded_at = orders["fields"][2]
    relationship = semantic_model["relationships"][0]
    metric = semantic_model["metrics"][0]

    assert orders["primary_key"] == ["id"]
    assert loaded_at["datatype"] == "DateTime"
    assert loaded_at["dimension"] == {"is_time": False}
    assert relationship["name"] == "order_customer"
    assert relationship["to_columns"] == ["id"]
    assert metric["datatype"] == "Decimal"
    assert metric["expression"]["dialects"] == [{"dialect": "BIGQUERY", "expression": "SUM(orders.amount)"}]


def test_dbt_alias_synthesis_requires_and_preserves_explicit_consumer_context() -> None:
    graph = SemanticGraph()
    graph.add_model(Model(name="orders", table="analytics.orders"))
    rejected = synthesize_ossie_document(
        graph,
        scope_name="commerce",
        expression_dialect="ANSI_SQL",
        schema_version="0.1.0",
    )
    accepted = synthesize_ossie_document(
        graph,
        scope_name="commerce",
        expression_dialect="ANSI_SQL",
        schema_version="0.1.0",
        consumer_profile=OssieConsumerProfile.DBT_1_12,
    )

    assert not rejected.valid
    assert [diagnostic.code for diagnostic in rejected.diagnostics] == ["ossie.synthesis.profile_unsupported"]
    assert accepted.valid
    assert accepted.document.version == "0.1.0"


def test_synthesis_refuses_to_invent_relationship_identity_or_keys() -> None:
    graph = _graph()
    graph.models["orders"].relationships = [
        Relationship(name="customers", type="many_to_one", foreign_key="customer_id", primary_key=None)
    ]

    result = synthesize_ossie_document(graph, scope_name="commerce", expression_dialect="ANSI_SQL")

    assert not result.valid
    assert result.document is None
    assert {diagnostic.code for diagnostic in result.diagnostics} == {"ossie.synthesis.relationship_identity_missing"}
    with pytest.raises(OssieSynthesisError):
        require_synthesized_document(result)


def test_synthesis_refuses_models_without_sources_instead_of_emitting_invalid_output() -> None:
    graph = SemanticGraph()
    graph.add_model(Model(name="orders"))

    result = synthesize_ossie_document(graph, scope_name="commerce", expression_dialect="SNOWFLAKE")

    assert not result.valid
    assert result.document is None
    assert any(diagnostic.code == "ossie.synthesis.source_missing" for diagnostic in result.diagnostics)
    assert any(diagnostic.code.startswith("ossie.schema") for diagnostic in result.diagnostics)


def test_synthesis_refuses_invalid_or_multiple_statement_expressions() -> None:
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="analytics.orders",
            dimensions=[Dimension(name="unsafe", type="categorical", sql="id; DROP TABLE orders")],
        )
    )

    result = synthesize_ossie_document(graph, scope_name="commerce", expression_dialect="ANSI_SQL")

    assert not result.valid
    assert result.document is None
    assert any(diagnostic.code == "ossie.synthesis.expression_invalid" for diagnostic in result.diagnostics)


def test_synthesis_closes_over_declared_key_and_relationship_field_semantics() -> None:
    graph = _graph()
    graph.models["orders"].primary_key = "missing_id"
    graph.models["orders"].relationships[0].foreign_key = "missing_customer_id"

    result = synthesize_ossie_document(graph, scope_name="commerce", expression_dialect="ANSI_SQL")

    assert not result.valid
    assert result.document is None
    assert {diagnostic.code for diagnostic in result.diagnostics} >= {
        "ossie.semantic.dataset.key_field_unknown",
        "ossie.semantic.relationship.from_key_field_unknown",
    }


def test_synthesis_closes_over_relationship_endpoint_semantics() -> None:
    graph = _graph()
    graph.models["orders"].relationships[0].name = "missing_customers"

    result = synthesize_ossie_document(graph, scope_name="commerce", expression_dialect="ANSI_SQL")

    assert not result.valid
    assert result.document is None
    assert any(
        diagnostic.code
        in {
            "ossie.synthesis.relationship_keys_unusable",
            "ossie.semantic.relationship.to_dataset_unknown",
        }
        for diagnostic in result.diagnostics
    )
