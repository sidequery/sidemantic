from __future__ import annotations

import json

import pytest

from sidemantic.core.consumption import Explore, SavedQuery
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.parameter import Parameter
from sidemantic.core.relationship import Relationship
from sidemantic.core.security import SecurityPolicy
from sidemantic.core.segment import Segment
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.core.semantic_layer import SemanticLayer
from sidemantic.core.table_calculation import TableCalculation
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

    result = synthesize_ossie_document(graph, scope_name="commerce", expression_dialect="ANSI_SQL", portable_only=True)

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

    result = synthesize_ossie_document(graph, scope_name="commerce", expression_dialect="ANSI_SQL", portable_only=True)

    assert result.document is None
    assert any(d.code == "ossie.synthesis.metric_semantics_unsupported" for d in result.diagnostics)


@pytest.mark.parametrize("options", [{"sql": "{from}.customer_id = {to}.id AND {to}.active"}, {"active": False}])
def test_synthesis_refuses_relationship_behavior_not_expressed_by_keys(options: dict) -> None:
    graph = _graph()
    relationship = graph.models["orders"].relationships[0]
    for name, value in options.items():
        setattr(relationship, name, value)

    result = synthesize_ossie_document(graph, scope_name="commerce", expression_dialect="ANSI_SQL", portable_only=True)

    assert result.document is None
    assert any(d.code == "ossie.synthesis.relationship_semantics_unsupported" for d in result.diagnostics)


def test_synthesis_refuses_to_erase_relationship_role_instances() -> None:
    graph = _graph()
    relationship = graph.models["orders"].relationships[0]
    relationship.name = "billing_customer"
    relationship.target_model = "customers"

    result = synthesize_ossie_document(graph, scope_name="commerce", expression_dialect="ANSI_SQL", portable_only=True)

    assert result.document is None
    assert any(
        d.code == "ossie.synthesis.relationship_semantics_unsupported"
        and "billing_customer" in d.message
        and "customers" in d.message
        for d in result.diagnostics
    )


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
        ({"agg": "sum", "sql": "amount", "filters": ["quantity = 2"]}, 10),
        ({"agg": "sum", "sql": "amount", "filters": ["quantity < 0"], "fill_nulls_with": 0}, 0),
        ({"agg": "count"}, 3),
        ({"agg": "sum", "sql": "1"}, 3),
        ({"agg": "count", "filters": ["quantity >= 0"]}, 3),
        ({"agg": "count", "sql": "amount", "filters": ["quantity >= 0"]}, 2),
        ({"agg": "count_distinct", "sql": "amount", "filters": ["quantity = 2"]}, 1),
    ],
)
def test_synthesis_qualifies_expression_columns_and_preserves_numeric_results(options: dict, expected: float) -> None:
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            sql="SELECT * FROM (VALUES (10, 2), (5, 4), (NULL, 0)) AS orders(amount, quantity)",
            dimensions=[Dimension(name="amount", type="numeric"), Dimension(name="quantity", type="numeric")],
            metrics=[Metric(name="value", **options)],
        )
    )
    native = SemanticLayer(auto_register=False)
    native.graph = graph
    assert float(native.query(metrics=["orders.value"]).fetchone()[0]) == pytest.approx(expected)
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
def test_synthesis_retains_model_binding_for_columnless_aggregates(options: dict) -> None:
    graph = _graph()
    graph.models["orders"].metrics = [Metric(name="row_count", **options)]

    result = synthesize_ossie_document(graph, scope_name="commerce", expression_dialect="ANSI_SQL")

    assert result.valid, result.diagnostics
    scope = result.document.to_parsed_data()["semantic_model"][0]
    assert "orders.__sidemantic_row" in scope["metrics"][0]["expression"]["dialects"][0]["expression"]


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

    result = synthesize_ossie_document(graph, scope_name="commerce", expression_dialect="ANSI_SQL", portable_only=True)

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

    result = synthesize_ossie_document(graph, scope_name="commerce", expression_dialect="ANSI_SQL", portable_only=True)

    assert result.document is None
    assert any(d.code == "ossie.synthesis.field_semantics_unsupported" for d in result.diagnostics)


@pytest.mark.parametrize(
    "options",
    [
        {"non_additive_dimension": "loaded_at"},
        {"offset_window": "1 month"},
        {"window_expression": "SUM(amount)", "window_frame": "ROWS UNBOUNDED PRECEDING"},
        {"public": False},
    ],
)
def test_synthesis_restores_native_metric_semantics_through_extension(options):
    graph = _graph()
    graph.models["orders"].metrics = [Metric(name="value", agg="sum", sql="amount", **options)]
    result = synthesize_ossie_document(graph, scope_name="commerce", expression_dialect="ANSI_SQL")
    assert result.valid, result.diagnostics
    assert any(d.code == "ossie.synthesis.runtime_extension_required" for d in result.diagnostics)
    document = result.document.to_parsed_data()
    assert all("analytics.orders" not in d["source"] for d in document["semantic_model"][0]["datasets"])
    lowered = lower_ossie_document(parse_ossie_document(json.dumps(document).encode()), target_dialect="duckdb")
    assert lowered.valid, lowered.diagnostics
    assert lowered.catalog["commerce"].graph.models["orders"].metrics[0] == graph.models["orders"].metrics[0]


def test_native_extension_does_not_hide_invalid_sql():
    graph = _graph()
    graph.models["orders"].metrics = [Metric(name="value", agg="sum", sql="amount; SELECT 1", public=False)]
    result = synthesize_ossie_document(graph, scope_name="commerce", expression_dialect="ANSI_SQL")
    assert not result.valid
    assert any(d.code == "ossie.synthesis.expression_invalid" for d in result.diagnostics)


def test_columnless_aggregate_uses_own_rows_and_avoids_declared_field_collision():
    graph = SemanticGraph()
    graph.add_model(Model(name="unrelated", sql="SELECT 1 AS id"))
    graph.add_model(
        Model(
            name="orders",
            sql="SELECT * FROM (VALUES (NULL), (NULL), (NULL)) AS t(id)",
            dimensions=[Dimension(name="__sidemantic_row", type="numeric", sql="id")],
            metrics=[Metric(name="rows", agg="count")],
        )
    )
    result = synthesize_ossie_document(graph, scope_name="commerce", expression_dialect="ANSI_SQL", portable_only=True)
    assert result.valid, result.diagnostics
    lowered = lower_ossie_document(
        parse_ossie_document(json.dumps(result.document.to_parsed_data()).encode()), target_dialect="duckdb"
    )
    layer = SemanticLayer.from_catalog(lowered.catalog, auto_register=False)
    assert layer.query(metrics=["rows"]).fetchall() == [(3,)]


@pytest.mark.parametrize("model_local", [True, False])
@pytest.mark.parametrize("filtered", [True, False])
def test_roundtrip_distinguishes_physical_measure_columns_from_semantic_fields(model_local, filtered):
    graph = SemanticGraph()
    model = Model(
        name="orders",
        sql="SELECT * FROM (VALUES (10), (5), (NULL)) AS t(amount)",
        dimensions=[Dimension(name="amount", type="numeric", sql="amount * 2")],
    )
    graph.add_model(model)
    metric = Metric(
        name="value",
        agg="sum",
        sql="amount" if model_local else "orders.amount",
        filters=["amount > 7" if model_local else "orders.amount > 7"] if filtered else None,
    )
    if model_local:
        model.metrics.append(metric)
    else:
        graph.add_metric(metric)
    native = SemanticLayer(auto_register=False)
    native.graph = graph
    expected = native.query(metrics=["orders.value" if model_local else "value"]).fetchall()
    result = synthesize_ossie_document(graph, scope_name="commerce", expression_dialect="ANSI_SQL", portable_only=True)
    assert result.valid, result.diagnostics
    lowered = lower_ossie_document(
        parse_ossie_document(json.dumps(result.document.to_parsed_data()).encode()), target_dialect="duckdb"
    )
    imported = SemanticLayer.from_catalog(lowered.catalog, auto_register=False)
    assert imported.query(metrics=["value"]).fetchall() == expected


@pytest.mark.parametrize("fill", ["", "can't"])
def test_null_fill_string_literals_execute_before_and_after_portable_export(fill):
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            sql="SELECT CAST(NULL AS VARCHAR) AS label",
            metrics=[Metric(name="label", agg="max", sql="label", fill_nulls_with=fill)],
        )
    )
    native = SemanticLayer(auto_register=False)
    native.graph = graph
    assert native.query(metrics=["orders.label"]).fetchall() == [(fill,)]
    result = synthesize_ossie_document(graph, scope_name="commerce", expression_dialect="ANSI_SQL", portable_only=True)
    assert result.valid, result.diagnostics
    lowered = lower_ossie_document(
        parse_ossie_document(json.dumps(result.document.to_parsed_data()).encode()), target_dialect="duckdb"
    )
    imported = SemanticLayer.from_catalog(lowered.catalog, auto_register=False)
    assert imported.query(metrics=["label"]).fetchall() == [(fill,)]


@pytest.mark.parametrize("feature", ["granularity", "segments", "window"])
def test_native_query_semantics_require_extension_and_execute_after_roundtrip(feature):
    graph = SemanticGraph()
    model = Model(
        name="orders",
        sql="SELECT * FROM (VALUES (TIMESTAMP '2026-01-02', 10), (TIMESTAMP '2026-01-03', 20)) AS t(ts, amount)",
        dimensions=[Dimension(name="ts", type="categorical")],
        metrics=[Metric(name="revenue", agg="sum", sql="amount")],
    )
    query = {"metrics": ["orders.revenue"], "dimensions": ["orders.ts"]}
    if feature == "granularity":
        model.dimensions[0].type = "time"
        model.dimensions[0].granularity = "month"
    elif feature == "segments":
        model.segments.append(Segment(name="large", sql="{model}.amount > 10"))
        query["segments"] = ["orders.large"]
    else:
        model.dimensions[0].window = "MIN(ts) OVER ()"
    graph.add_model(model)
    native = SemanticLayer(auto_register=False)
    native.graph = graph
    expected = native.query(**query).fetchall()
    assert len(expected) == 1

    portable = synthesize_ossie_document(
        graph, scope_name="commerce", expression_dialect="ANSI_SQL", portable_only=True
    )
    assert not portable.valid
    assert portable.document is None
    assert any(d.code == "ossie.synthesis.native_state_unrepresented" for d in portable.diagnostics)

    result = synthesize_ossie_document(graph, scope_name="commerce", expression_dialect="ANSI_SQL")
    assert result.valid, result.diagnostics
    assert any(d.code == "ossie.synthesis.runtime_extension_required" for d in result.diagnostics)
    lowered = lower_ossie_document(
        parse_ossie_document(json.dumps(result.document.to_parsed_data()).encode()), target_dialect="duckdb"
    )
    assert lowered.valid, lowered.diagnostics
    restored = lowered.catalog["commerce"].graph.models["orders"]
    assert restored.dimensions == model.dimensions
    assert restored.segments == model.segments
    imported = SemanticLayer.from_catalog(lowered.catalog, auto_register=False)
    assert imported.query(**query).fetchall() == expected


@pytest.mark.parametrize(
    ("owner", "field", "value"),
    [
        ("dimension", "supported_granularities", ["month"]),
        ("dimension", "parent", "customer_id"),
        ("dimension", "format", "yyyy-MM"),
        ("model", "default_grain", "month"),
        ("model", "auto_dimensions", True),
        ("model", "owner", "analytics"),
        ("model", "metadata", {"ossie_source_kind": "table", "ossie_pointer": "/datasets/0", "custom": "preserve"}),
        ("metric", "drill_fields", ["orders.id"]),
        ("metric", "label", "Revenue"),
        ("relationship", "metadata", {"custom": "preserve"}),
        ("graph", "metadata", {"custom": "preserve"}),
        ("graph", "parameters", {"region": Parameter(name="region", type="string", default_value="US")}),
        ("graph", "table_calculations", {"rank": TableCalculation(name="rank", type="rank")}),
        ("graph", "explores", {"orders": Explore(name="orders", model="orders")}),
        ("graph", "saved_queries", {"revenue": SavedQuery(name="revenue", metrics=["orders.revenue"])}),
        ("graph", "import_warnings", [{"message": "source warning"}]),
    ],
)
def test_unprojected_native_fields_are_preserved_or_refused(owner, field, value):
    graph = _graph()
    model = graph.models["orders"]
    item = {
        "graph": graph,
        "model": model,
        "dimension": model.dimensions[2],
        "metric": model.metrics[0],
        "relationship": model.relationships[0],
    }[owner]
    setattr(item, field, value)
    portable = synthesize_ossie_document(
        graph, scope_name="commerce", expression_dialect="ANSI_SQL", portable_only=True
    )
    assert not portable.valid
    assert any(field in d.message for d in portable.diagnostics)
    result = synthesize_ossie_document(graph, scope_name="commerce", expression_dialect="ANSI_SQL")
    assert result.valid, result.diagnostics
    lowered = lower_ossie_document(
        parse_ossie_document(json.dumps(result.document.to_parsed_data()).encode()), target_dialect="duckdb"
    )
    assert lowered.valid, lowered.diagnostics
    restored_graph = lowered.catalog["commerce"].graph
    restored_model = restored_graph.models["orders"]
    restored = {
        "graph": restored_graph,
        "model": restored_model,
        "dimension": restored_model.dimensions[2],
        "metric": restored_model.metrics[0],
        "relationship": restored_model.relationships[0],
    }[owner]
    assert getattr(restored, field) == value


@pytest.mark.parametrize("feature", ["segments", "window"])
def test_extension_selection_does_not_hide_invalid_native_expression(feature):
    graph = _graph()
    if feature == "segments":
        graph.models["orders"].segments.append(Segment(name="invalid", sql="amount > 0; SELECT 1"))
    else:
        graph.models["orders"].dimensions[2].window = "MIN(loaded_at) OVER (); SELECT 1"
    result = synthesize_ossie_document(graph, scope_name="commerce", expression_dialect="ANSI_SQL")
    assert not result.valid
    assert any(d.code == "ossie.synthesis.expression_invalid" for d in result.diagnostics)
