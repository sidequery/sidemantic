from __future__ import annotations

import json

import pytest

from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.security import SecurityPolicy
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.core.semantic_layer import SemanticLayer
from sidemantic.interchange.ossie import (
    OssieConsumerProfile,
    OssieSynthesisError,
    lower_ossie_document,
    parse_ossie_document,
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


@pytest.mark.parametrize(
    "options",
    [
        {"filters": ["status = 'paid'"]},
        {"fill_nulls_with": 0},
        {"non_additive_dimension": "loaded_at"},
        {"non_additive_window_groupings": ["customer_id"]},
        {"offset_window": "1 month"},
        {"window_expression": "SUM(amount)", "window_frame": "ROWS UNBOUNDED PRECEDING"},
        {"extends": "base_revenue"},
        {"public": False},
    ],
)
def test_synthesis_refuses_result_affecting_metric_options(options: dict) -> None:
    graph = _graph()
    graph.models["orders"].metrics = [Metric(name="revenue", agg="sum", sql="amount", **options)]

    result = synthesize_ossie_document(graph, scope_name="commerce", expression_dialect="ANSI_SQL")

    assert result.document is None
    assert any(d.code == "ossie.synthesis.metric_semantics_unsupported" for d in result.diagnostics)


@pytest.mark.parametrize("options", [{"sql": "{from}.customer_id = {to}.id AND {to}.active"}, {"active": False}])
def test_synthesis_refuses_relationship_behavior_not_expressed_by_keys(options: dict) -> None:
    graph = _graph()
    relationship = graph.models["orders"].relationships[0]
    for name, value in options.items():
        setattr(relationship, name, value)

    result = synthesize_ossie_document(graph, scope_name="commerce", expression_dialect="ANSI_SQL")

    assert result.document is None
    assert any(d.code == "ossie.synthesis.relationship_semantics_unsupported" for d in result.diagnostics)


@pytest.mark.parametrize("key_kind", ["primary_key", "unique_keys"])
def test_synthesis_accepts_reordered_unique_key_without_reordering_join_pairs(key_kind: str) -> None:
    graph = _graph()
    customers = graph.models["customers"]
    customers.dimensions.append(Dimension(name="region", type="categorical", sql="region"))
    orders = graph.models["orders"]
    orders.dimensions.append(Dimension(name="region", type="categorical", sql="region"))
    if key_kind == "primary_key":
        customers.primary_key = ["ID", "REGION"]
    else:
        customers.unique_keys = [["ID", "REGION"]]
    orders.relationships[0].foreign_key = ["region", "customer_id"]
    orders.relationships[0].primary_key = ["region", "id"]

    result = synthesize_ossie_document(graph, scope_name="commerce", expression_dialect="ANSI_SQL")

    assert result.valid, result.diagnostics
    relationship = result.document.to_parsed_data()["semantic_model"][0]["relationships"][0]
    assert relationship["from_columns"] == ["region", "customer_id"]
    assert relationship["to_columns"] == ["region", "id"]


@pytest.mark.parametrize(
    ("options", "expected"),
    [
        ({"agg": "sum", "sql": "coalesce(amount, 0)"}, 15),
        ({"agg": "sum", "sql": "amount * quantity"}, 40),
        ({"agg": "sum", "sql": "amount * 1.5"}, 22.5),
        ({"sql": "SUM(amount) / SUM(quantity)", "sql_is_complete": True}, 2.5),
        ({"sql": "SUM(amount) + SUM(quantity)"}, 21),
        ({"agg": "sum", "sql": 'coalesce("amount", 0) * orders.quantity'}, 40),
        ({"type": "derived", "agg": "sum", "sql": "amount * quantity"}, 40),
    ],
)
def test_synthesis_qualifies_expression_columns_and_preserves_numeric_results(options: dict, expected: float) -> None:
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            sql="SELECT * FROM (VALUES (10, 2), (5, 4), (NULL, 0)) AS t(amount, quantity)",
            dimensions=[Dimension(name="amount", type="numeric"), Dimension(name="quantity", type="numeric")],
            metrics=[Metric(name="value", **options)],
        )
    )
    result = synthesize_ossie_document(graph, scope_name="commerce", expression_dialect="ANSI_SQL")
    assert result.valid, result.diagnostics
    parsed = parse_ossie_document(json.dumps(result.document.to_parsed_data()).encode(), source_identifier="test.json")
    lowered = lower_ossie_document(parsed, target_dialect="duckdb")
    assert lowered.valid, lowered.diagnostics
    layer = SemanticLayer.from_catalog(lowered.catalog, auto_register=False)
    assert float(layer.query(metrics=["value"]).fetchone()[0]) == pytest.approx(expected)


def test_synthesis_preserves_quoted_column_case_and_existing_qualifiers() -> None:
    graph = _graph()
    graph.models["orders"].metrics[0].sql = 'coalesce("Amount", 0) * orders."Quantity"'

    result = synthesize_ossie_document(graph, scope_name="commerce", expression_dialect="ANSI_SQL")

    assert result.valid, result.diagnostics
    expression = result.document.to_parsed_data()["semantic_model"][0]["metrics"][0]["expression"]
    assert expression["dialects"][0]["expression"] == 'SUM(COALESCE(orders."Amount", 0) * orders."Quantity")'


@pytest.mark.parametrize("expression", ["revenue * 2", "orders.revenue * 2"])
def test_synthesis_preserves_model_metric_references_in_derived_formulas(expression: str) -> None:
    graph = _graph()
    graph.models["orders"].metrics.append(Metric(name="double_revenue", type="derived", sql=expression))

    result = synthesize_ossie_document(graph, scope_name="commerce", expression_dialect="ANSI_SQL")

    assert result.valid, result.diagnostics
    metrics = result.document.to_parsed_data()["semantic_model"][0]["metrics"]
    assert metrics[1]["expression"]["dialects"][0]["expression"] == "revenue * 2"


@pytest.mark.parametrize("options", [{"agg": "count"}, {"agg": "sum", "sql": "1"}])
def test_synthesis_refuses_to_lose_model_binding_for_columnless_aggregates(options: dict) -> None:
    graph = _graph()
    graph.models["orders"].metrics = [Metric(name="row_count", **options)]

    result = synthesize_ossie_document(graph, scope_name="commerce", expression_dialect="ANSI_SQL")

    assert result.document is None
    assert any(d.code == "ossie.synthesis.metric_owner_unrepresentable" for d in result.diagnostics)


@pytest.mark.parametrize(
    "options",
    [
        {"extends": "base_orders"},
        {"security": SecurityPolicy(access=False)},
        {"security": SecurityPolicy(row_filters=["customer_id = 1"])},
    ],
)
def test_synthesis_refuses_to_discard_model_security_or_inheritance(options: dict) -> None:
    graph = _graph()
    for name, value in options.items():
        setattr(graph.models["orders"], name, value)

    result = synthesize_ossie_document(graph, scope_name="commerce", expression_dialect="ANSI_SQL")

    assert result.document is None
    assert any(d.code == "ossie.synthesis.model_semantics_unsupported" for d in result.diagnostics)


def test_synthesis_accepts_noop_metric_and_security_defaults() -> None:
    graph = _graph()
    graph.models["orders"].security = SecurityPolicy()
    graph.models["orders"].metrics[0].filters = []

    result = synthesize_ossie_document(graph, scope_name="commerce", expression_dialect="ANSI_SQL")

    assert result.valid, result.diagnostics


def test_synthesis_refuses_to_expose_private_dimensions() -> None:
    graph = _graph()
    graph.models["orders"].dimensions[-1].public = False

    result = synthesize_ossie_document(graph, scope_name="commerce", expression_dialect="ANSI_SQL")

    assert result.document is None
    assert any(d.code == "ossie.synthesis.field_semantics_unsupported" for d in result.diagnostics)
