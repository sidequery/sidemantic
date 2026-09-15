"""Behavioral contracts for inert snapshots and typed compiler failures."""

import json
from types import SimpleNamespace

import pytest

from sidemantic import Dimension, Metric, Model, PreAggregation, Relationship, SecurityPolicy
from sidemantic.core.inheritance import merge_model
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


@pytest.mark.parametrize(
    "overrides",
    [
        {},
        {"primary_key": None, "default_grain": None, "auto_dimensions": False, "metadata": {}, "meta": None},
        {"primary_key": "alternate_id", "unique_keys": []},
    ],
)
def test_unresolved_child_snapshot_preserves_inheritance_override_semantics(overrides):
    parent = Model(
        name="base",
        table="orders",
        primary_key="id",
        default_grain="month",
        auto_dimensions=True,
        metadata={"source": "parent"},
        meta={"label": "parent"},
        unique_keys=[["alternate_id"]],
    )
    child = Model(name="child", extends="base", **overrides)
    graph = SemanticGraph()
    graph.add_model(parent)
    graph.add_model(child)
    snapshot = graph_to_semantic_input(graph)["models"][1]
    assert ("primary_key" in snapshot) == ("primary_key" in overrides)
    transported = Model.model_validate(snapshot)
    expected = merge_model(child, parent)
    actual = merge_model(transported, parent)
    assert actual.primary_key_columns == expected.primary_key_columns
    assert actual.model_dump(exclude={"primary_key"}) == expected.model_dump(exclude={"primary_key"})
    assert child.model_fields_set == {"name", "extends", *overrides}


@pytest.mark.parametrize("kind", ["one_to_one", "one_to_many", "many_to_one"])
@pytest.mark.parametrize("explicit", [None, "explicit_key"])
def test_snapshot_preserves_tmdl_local_endpoint_without_changing_model_key(kind, explicit):
    graph = SemanticGraph()
    relationship = Relationship(name="metadata", type=kind, foreign_key="meta_key", primary_key=explicit)
    relationship._tmdl_from_column = "alt_key"
    graph.add_model(Model(name="orders", table="orders", primary_key="id", relationships=[relationship]))
    payload = graph_to_semantic_input(graph)
    expected = explicit or ("alt_key" if kind in ("one_to_one", "one_to_many") else None)
    assert payload["models"][0]["relationships"][0].get("primary_key") == expected
    assert payload["models"][0]["primary_key"] == ["id"]
    assert relationship.primary_key == explicit


@pytest.mark.parametrize(
    "definition",
    [
        {"type": "conversion", "entity": "id", "base_event": "signup", "conversion_event": "purchase"},
        {"type": "time_comparison", "base_metric": "events.count"},
    ],
)
def test_snapshot_does_not_duplicate_automatically_indexed_model_metrics(definition):
    graph = SemanticGraph()
    metric = Metric(name="special", **definition)
    graph.add_model(Model(name="events", table="events", metrics=[metric]))
    payload = graph_to_semantic_input(graph)
    assert payload["metrics"] == []
    assert [item["name"] for item in payload["models"][0]["metrics"]] == ["special"]
    assert graph.metrics["special"] is metric


def test_snapshot_keeps_explicit_graph_metric_even_when_model_uses_same_object():
    graph = SemanticGraph()
    metric = Metric(name="revenue", agg="sum", sql="amount")
    graph.add_model(Model(name="orders", table="orders", metrics=[metric]))
    graph.add_metric(metric, model_name="orders")
    payload = graph_to_semantic_input(graph)
    assert [item["name"] for item in payload["metrics"]] == ["revenue"]
    assert payload["metric_owners"] == {"revenue": "orders"}


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


def test_rewrite_output_dialect_keeps_input_sql_and_graph_dialect(source_graph):
    from sidemantic.rust_bridge import rewrite_semantic_input

    calls = []

    def rewrite(input_json, sql, context_json):
        calls.append((json.loads(input_json), sql, json.loads(context_json)))
        return "SELECT 1"

    sql = "select events.total from metrics"
    module = SimpleNamespace(rewrite_with_semantic_input_context=rewrite)
    assert rewrite_semantic_input(source_graph, sql, output_dialect="postgres", rust_module=module) == "SELECT 1"
    envelope, received_sql, context = calls[0]
    assert envelope["input_dialect"] == "duckdb"
    assert received_sql == sql
    assert context == {"output_dialect": "postgres", "user_attributes": None, "enforce_visibility": False}


def test_postgres_transport_sql_preserves_literals_identifiers_and_graph(source_graph):
    from sidemantic.rust_bridge import rewrite_semantic_input

    received = []

    def rewrite(graph_json, sql, context_json):
        received.append((json.loads(graph_json), sql, json.loads(context_json)))
        return "SELECT 1"

    original = graph_to_semantic_input(source_graph)
    sql = r"""SELECT "events"."Revenue" FROM metrics WHERE "events"."Region" = E'O\'Brien' """
    rewrite_semantic_input(
        source_graph,
        sql,
        sql_dialect="postgres",
        output_dialect="postgres",
        rust_module=SimpleNamespace(rewrite_with_semantic_input_context=rewrite),
    )
    envelope, normalized, context = received[0]
    assert envelope == original
    assert '"events"."Revenue"' in normalized
    assert "'O''Brien'" in normalized
    assert context["output_dialect"] == "postgres"
    assert graph_to_semantic_input(source_graph) == original


def test_postgres_structured_fragments_keep_input_dictionary_unchanged(source_graph):
    received = []

    def compile_input(graph_json, query_json):
        received.append(json.loads(query_json))
        return "SELECT 1"

    query = {
        "metrics": ["events.total"],
        "filters": [r"""events."Region" = E'O\'Brien' """],
        "order_by": ['events."Region" DESC NULLS LAST'],
        "dialect": "postgres",
    }
    compile_semantic_input(
        source_graph,
        query,
        query_dialect="postgres",
        rust_module=SimpleNamespace(compile_with_semantic_input=compile_input),
    )
    assert "'O''Brien'" in received[0]["filters"][0]
    assert received[0]["order_by"] == ['events."Region" DESC NULLS LAST']
    assert query["filters"] == [r"""events."Region" = E'O\'Brien' """]


def test_other_query_dialects_are_forwarded_without_python_translation(source_graph):
    received = []

    def compile_input(graph_json, query_json):
        received.append(json.loads(query_json))
        return "SELECT 1"

    query = {"metrics": ["events.total"], "filters": ["`events`.`Region` = 'west'"]}
    compile_semantic_input(
        source_graph,
        query,
        query_dialect="bigquery",
        rust_module=SimpleNamespace(compile_with_semantic_input=compile_input),
    )
    assert received == [{**query, "query_dialect": "bigquery"}]
    assert "query_dialect" not in query


@pytest.mark.parametrize(
    "field,fragment",
    [
        ("filters", "1 = 1; SELECT 2"),
        ("filters", "1 = 1 ORDER BY 1"),
        ("filters", "1 = 1 LIMIT 1"),
        ("order_by", "events.total; SELECT 2"),
        ("order_by", "events.total LIMIT 1"),
    ],
)
def test_postgres_fragment_normalization_rejects_extra_sql(source_graph, field, fragment):
    with pytest.raises(ValueError, match="single statement|extra SQL clauses"):
        compile_semantic_input(source_graph, {field: [fragment]}, query_dialect="postgres", rust_module=object())


def test_postgres_sql_normalization_rejects_multiple_statements(source_graph):
    from sidemantic.rust_bridge import rewrite_semantic_input

    with pytest.raises(ValueError, match="single statement"):
        rewrite_semantic_input(source_graph, "SELECT 1; SELECT 2", sql_dialect="postgres", rust_module=object())


@pytest.mark.parametrize("clause", [None, "ORDER BY"])
@pytest.mark.parametrize(
    "ordering,expected",
    [("x ASC", "x ASC NULLS LAST"), ("x DESC", "x DESC NULLS FIRST"), ("x DESC NULLS LAST", "x DESC NULLS LAST")],
)
def test_postgres_null_order_survives_intermediate_dialect(clause, ordering, expected):
    from sidemantic.rust_bridge import _postgres_query_sql

    sql = f"SELECT x FROM events ORDER BY {ordering}" if clause is None else ordering
    assert expected in _postgres_query_sql(sql, clause=clause)


def test_postgres_nested_window_null_order_survives_intermediate_dialect():
    from sidemantic.rust_bridge import _postgres_query_sql

    sql = "SELECT x FROM events ORDER BY SUM(x) OVER (ORDER BY y DESC)"
    assert _postgres_query_sql(sql) == (
        "SELECT x FROM events ORDER BY SUM(x) OVER (ORDER BY y DESC NULLS FIRST) ASC NULLS LAST"
    )
