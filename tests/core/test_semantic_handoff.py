"""Behavioral contracts for inert snapshots and typed compiler failures."""

import json
from types import SimpleNamespace

import pytest

from sidemantic import Dimension, Metric, Model, PreAggregation, Relationship, SecurityPolicy
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.core.semantic_layer import SecurityError
from sidemantic.rust_bridge import (
    compile_semantic_input,
    generate_preaggregation_materialization_sql_with_rust,
    validate_semantic_input,
)
from sidemantic.semantic_handoff import (
    RustBackendUnavailableError,
    UnsupportedSemanticFeaturesError,
    graph_to_semantic_input,
    graph_to_semantic_json,
)
from sidemantic.validation import QueryValidationError


@pytest.fixture
def source_graph():
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="events",
            table="events",
            primary_key=None,
            dimensions=[
                Dimension(
                    name="created",
                    type="time",
                    sql="created_at",
                    granularity="day",
                    logical_data_type="timestamp_tz",
                    declared_is_time=False,
                )
            ],
            metrics=[
                Metric(
                    name="opaque",
                    sql="SUM(IF(`status` = 'paid', `amount`, 0))",
                    sql_is_complete=True,
                    metadata={"dialect": "bigquery", "source": {"line": 7}},
                )
            ],
            relationships=[
                Relationship(
                    name="buyer",
                    target_model="people",
                    edge_id="source-edge-7",
                    type="many_to_one",
                    foreign_key=["tenant", "buyer_id"],
                    primary_key=["tenant", "id"],
                )
            ],
            invariant_filters=["deleted = false"],
            security=SecurityPolicy(row_filters=["tenant = {{ user.tenant }}"]),
            metadata={"provenance": {"source": ["source.cube.yml"]}},
        )
    )
    graph.add_model(
        Model(
            name="people",
            table="people",
            primary_key=["tenant", "id"],
            metrics=[Metric(name="count", agg="count")],
        )
    )
    graph.add_metric(Metric(name="cross_ratio", type="ratio", numerator="events.opaque", denominator="people.count"))
    graph.add_metric(
        Metric(name="explicit_ratio", type="ratio", numerator="events.opaque", denominator="people.count"),
        model_name="events",
    )
    graph.metadata = {"provenance": {"files": ["source.cube.yml"]}}
    graph.import_warnings = ["An upstream feature needs review"]
    return graph


def test_snapshot_preserves_keys_scopes_sql_and_source_semantics(source_graph):
    payload = graph_to_semantic_input(source_graph, input_dialect="bigquery")
    events, people = payload["models"]
    assert events["primary_key"] is None
    assert people["primary_key"] == ["tenant", "id"]
    assert {metric["name"] for metric in payload["metrics"]} == {"cross_ratio", "explicit_ratio"}
    assert payload["metric_owners"] == {"explicit_ratio": "events"}
    assert [metric["name"] for metric in events["metrics"]] == ["opaque"]
    assert events["metrics"][0]["sql"] == "SUM(IF(`status` = 'paid', `amount`, 0))"
    assert events["metrics"][0]["sql_is_complete"] is True
    assert events["metrics"][0]["metadata"]["dialect"] == "bigquery"
    assert payload["input_dialect"] == "bigquery"
    assert events["security"]["row_filters"] == ["tenant = {{ user.tenant }}"]
    assert events["invariant_filters"] == ["deleted = false"]
    assert events["relationships"][0]["edge_id"] == "source-edge-7"
    assert events["relationships"][0]["target_model"] == "people"
    assert events["relationships"][0]["foreign_key"] == ["tenant", "buyer_id"]
    assert events["dimensions"][0]["logical_data_type"] == "timestamp_tz"
    assert events["dimensions"][0]["declared_is_time"] is False
    assert {"input_dialect.bigquery", "model.security", "model.invariant_filters", "relationship.roles"} <= set(
        payload["required_capabilities"]
    )
    assert json.loads(graph_to_semantic_json(source_graph, input_dialect="bigquery")) == payload


def test_snapshot_is_independent_of_nested_source_metadata(source_graph):
    first = graph_to_semantic_input(source_graph)
    source_graph.metadata["provenance"]["files"].append("later.yml")
    source_graph.models["events"].metadata["provenance"]["source"].append("later.yml")
    source_graph.import_warnings.append("later warning")
    assert first["metadata"]["provenance"]["files"] == ["source.cube.yml"]
    assert first["models"][0]["metadata"]["provenance"]["source"] == ["source.cube.yml"]
    assert first["import_warnings"] == ["An upstream feature needs review"]

    second = graph_to_semantic_input(source_graph)
    second["metadata"]["provenance"]["files"].append("payload-only.yml")
    assert "payload-only.yml" not in source_graph.metadata["provenance"]["files"]


def test_serialization_does_not_reenter_authoring(source_graph, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("Serialization reentered model construction or registration")

    for cls in (Model, Metric, Dimension, Relationship, SecurityPolicy):
        monkeypatch.setattr(cls, "__init__", forbidden)
    monkeypatch.setattr("sidemantic.core.registry.auto_register_model", forbidden)
    monkeypatch.setattr("sidemantic.core.registry.auto_register_metric", forbidden)
    monkeypatch.setattr(SemanticGraph, "add_model", forbidden)
    monkeypatch.setattr(SemanticGraph, "add_metric", forbidden)
    assert json.loads(graph_to_semantic_json(source_graph))["models"][0]["name"] == "events"


def test_bridge_sends_graph_and_query_without_inventing_owner(source_graph):
    received = []

    def compile_input(graph_json, query_json):
        received.append((json.loads(graph_json), json.loads(query_json)))
        return "select 1"

    query = {"metrics": ["cross_ratio"], "dialect": "duckdb"}
    module = SimpleNamespace(compile_with_semantic_input=compile_input)
    assert compile_semantic_input(source_graph, query, rust_module=module) == "select 1"
    payload, received_query = received[0]
    assert received_query == query
    assert "cross_ratio" not in payload["metric_owners"]
    assert payload["models"][0]["primary_key"] is None


class ExtensionUnsupportedError(Exception):
    def __init__(self, capabilities):
        self.capabilities = capabilities


class ExtensionSecurityError(Exception):
    pass


class ExtensionQueryValidationError(ValueError):
    pass


@pytest.mark.parametrize("operation", ["compile", "validate"])
@pytest.mark.parametrize(
    "failure_kind", ["unsupported", "missing", "unexpected", "invalid", "malformed", "security", "query_invalid"]
)
def test_bridge_distinguishes_failure_classes(source_graph, operation, failure_kind):
    failure = {
        "unsupported": ExtensionUnsupportedError(["relationship.roles"]),
        "unexpected": RuntimeError("compiler invariant failed"),
        "invalid": ValueError("unknown metric missing"),
        "malformed": ExtensionUnsupportedError([]),
        "security": ExtensionSecurityError("Access denied"),
        "query_invalid": ExtensionQueryValidationError("Model has no primary key"),
    }.get(failure_kind)

    def fail(*args):
        raise failure

    module = SimpleNamespace(
        UnsupportedSemanticFeaturesError=ExtensionUnsupportedError,
        SecurityError=ExtensionSecurityError,
        QueryValidationError=ExtensionQueryValidationError,
    )
    if failure_kind != "missing":
        setattr(module, f"{operation}_with_semantic_input", fail)

    expected_type = {
        "unsupported": UnsupportedSemanticFeaturesError,
        "missing": RustBackendUnavailableError,
        "unexpected": RuntimeError,
        "invalid": ValueError,
        "malformed": TypeError,
        "security": SecurityError,
        "query_invalid": QueryValidationError,
    }[failure_kind]
    with pytest.raises(expected_type) as caught:
        if operation == "compile":
            compile_semantic_input(source_graph, {"metrics": ["missing"]}, rust_module=module)
        else:
            validate_semantic_input(source_graph, ["missing"], [], rust_module=module)
    if failure_kind in {"unexpected", "invalid"}:
        assert caught.value is failure
    if failure_kind == "unsupported":
        assert caught.value.capabilities == ["relationship.roles"]


@pytest.mark.parametrize("input_dialect", ["", " ", None])
def test_missing_input_dialect_is_invalid(source_graph, input_dialect):
    with pytest.raises(ValueError, match="dialect"):
        graph_to_semantic_input(source_graph, input_dialect=input_dialect)


def test_legacy_materializer_cannot_discard_invariant_filters():
    model = Model(
        name="orders",
        table="orders",
        primary_key="id",
        invariant_filters=["deleted = false"],
        metrics=[Metric(name="revenue", agg="sum", sql="amount")],
    )
    rollup = PreAggregation(name="total", measures=["revenue"])
    with pytest.raises(UnsupportedSemanticFeaturesError) as caught:
        generate_preaggregation_materialization_sql_with_rust(model, rollup)
    assert caught.value.capabilities == ["preaggregation.invariant_filters"]
