"""Execution and immutable profile checks for Apache Ossie 172a3e5."""

import json
from copy import deepcopy

import pytest

from sidemantic import SemanticLayer
from sidemantic.interchange.ossie import (
    CURRENT_OSSIE_SCHEMA_COMMIT,
    LATEST_OSSIE_SCHEMA_COMMIT,
    OssieParseOptions,
    lower_ossie_document,
    parse_ossie_document,
    serialize_ossie_document,
)


def field(name, sql):
    return {"name": name, "expression": {"sql": sql}}


def document():
    return {
        "version": "0.2.0.dev0",
        "name": "sales",
        "datasets": [
            {
                "name": "orders",
                "source": "orders",
                "fields": [field("amount", "amount * 2")],
                "metrics": [
                    field("total", "SUM(amount)"),
                    field("raw", "SUM(source.amount)"),
                    field("count", "COUNT(*)"),
                ],
            }
        ],
    }


def lower(data, revision=None):
    parsed = parse_ossie_document(
        json.dumps(data).encode(),
        options=OssieParseOptions(validate_schema=True, target_dialect="duckdb", schema_revision=revision),
    )
    return lower_ossie_document(parsed)


def test_latest_dataset_metrics_preserve_fields_source_columns_and_count_rows():
    lowered = lower(document())
    assert lowered.valid, lowered.diagnostics
    assert lowered.parse_result.profile.schema_revision == LATEST_OSSIE_SCHEMA_COMMIT
    layer = SemanticLayer.from_catalog(lowered.catalog, engine="python", fallback=False, auto_register=False)
    try:
        layer.adapter.execute("CREATE TABLE orders(amount INT); INSERT INTO orders VALUES (10),(20),(NULL)")
        assert layer.query(metrics=["orders.total", "orders.raw", "orders.count"]).fetchall() == [(60, 30, 3)]
        layer.adapter.execute("DELETE FROM orders")
        assert layer.query(metrics=["orders.total", "orders.raw", "orders.count"]).fetchall() == [(None, None, 0)]
    finally:
        layer.adapter.close()


def test_dataset_metric_names_are_local_and_can_repeat_across_datasets():
    data = document()
    second = deepcopy(data["datasets"][0])
    second.update(name="returns", source="returns")
    data["datasets"].append(second)
    lowered = lower(data)
    assert lowered.valid, lowered.diagnostics
    layer = SemanticLayer.from_catalog(lowered.catalog, engine="python", fallback=False, auto_register=False)
    try:
        layer.adapter.execute(
            "CREATE TABLE orders(amount INT); INSERT INTO orders VALUES (10),(20);"
            "CREATE TABLE returns(amount INT); INSERT INTO returns VALUES (4)"
        )
        assert layer.query(metrics=["orders.total"]).fetchall() == [(60,)]
        assert layer.query(metrics=["returns.total"]).fetchall() == [(8,)]
    finally:
        layer.adapter.close()


@pytest.mark.parametrize("sql", ["SUM(missing)", "SUM(other.amount)", "total * 2"])
def test_latest_dataset_metric_references_fail_closed(sql):
    data = document()
    data["datasets"][0]["metrics"][0]["expression"]["sql"] = sql
    lowered = lower(data)
    assert not lowered.valid
    assert not lowered.executable
    assert any(d.code == "ossie.semantic.metric.reference_invalid" for d in lowered.diagnostics)


def test_latest_shared_field_metric_namespace_rejects_case_collision():
    data = document()
    data["datasets"][0]["metrics"][0]["name"] = "AMOUNT"
    lowered = lower(data)
    assert not lowered.valid
    assert any(d.code == "ossie.semantic.dataset.member_duplicate" for d in lowered.diagnostics)


def test_latest_sql_wins_over_portable_variant_and_opaque_variant_survives():
    data = document()
    expression = data["datasets"][0]["fields"][0]["expression"]
    expression["dialects"] = [
        {"dialect": "OSSIE_SQL_2026", "expression": "amount * 100"},
        {"dialect": "CUSTOM_EXPRESSION", "expression": "opaque$syntax"},
    ]
    lowered = lower(data)
    assert lowered.valid, lowered.diagnostics
    scope = next(iter(lowered.catalog.scopes.values()))
    assert scope.graph.models["orders"].get_dimension("amount").sql == "amount * 2"
    emitted = serialize_ossie_document(lowered.document, "json")
    assert json.loads(emitted.data) == data


def test_explicit_old_revision_rejects_new_features_and_retains_provenance():
    lowered = lower(document(), CURRENT_OSSIE_SCHEMA_COMMIT)
    assert not lowered.valid
    assert lowered.schema_validation.schema_commit == CURRENT_OSSIE_SCHEMA_COMMIT


def test_ambiguous_document_keeps_old_pin_but_can_select_latest():
    data = {"version": "0.2.0.dev0", "name": "simple", "datasets": [{"name": "t", "source": "t"}]}
    assert lower(data).parse_result.profile.schema_revision == CURRENT_OSSIE_SCHEMA_COMMIT
    assert lower(data, LATEST_OSSIE_SCHEMA_COMMIT).parse_result.profile.schema_revision == LATEST_OSSIE_SCHEMA_COMMIT


@pytest.mark.parametrize("sql", ["SUM(amount)", "SUM(orders.missing)", "other_metric * 2"])
def test_latest_model_metrics_require_declared_qualified_fields(sql):
    data = document()
    data["metrics"] = [field("model_total", sql)]
    lowered = lower(data)
    assert not lowered.valid
    assert any(d.code == "ossie.semantic.metric.reference_invalid" for d in lowered.diagnostics)


def test_latest_filter_rejects_cross_dataset_predicates():
    data = document()
    data["datasets"].append({"name": "returns", "source": "returns", "fields": [field("amount", "amount")]})
    data["metrics"] = [field("bad", "SUM(orders.amount) FILTER (WHERE returns.amount > 0)")]
    lowered = lower(data)
    assert not lowered.valid
    assert any(d.code == "ossie.semantic.metric.filter_scope_invalid" for d in lowered.diagnostics)


def test_latest_schema_rejects_empty_vendor_name():
    data = document()
    data["custom_extensions"] = [{"vendor_name": "", "data": ""}]
    lowered = lower(data)
    assert not lowered.parse_result.valid
    assert any(d.code == "ossie.schema.min_length" for d in lowered.diagnostics)


def test_latest_model_count_star_preserves_source():
    data = document()
    data["metrics"] = [field("unbound_count", "COUNT(*) FILTER (WHERE orders.amount > 0)")]
    lowered = lower(data)
    assert lowered.parse_result.valid
    assert lowered.semantic_validation.valid
    assert lowered.executable
    assert json.loads(serialize_ossie_document(lowered.document, "json").data) == data


def test_latest_model_name_is_a_source_scope_identity_not_an_expression_identifier():
    data = document()
    data["name"] = "Sales semantic model"
    lowered = lower(data)
    assert lowered.valid, lowered.diagnostics
    assert lowered.catalog.scope_ids == ("Sales semantic model",)


def test_latest_permissive_lowering_excludes_invalid_metrics_instead_of_rebinding():
    data = document()
    data["datasets"][0]["metrics"][0]["name"] = "amount"
    data["metrics"] = [field("unsafe_reference", "orders.raw * 2")]
    parsed = parse_ossie_document(
        json.dumps(data).encode(),
        options=OssieParseOptions(validate_schema=True, target_dialect="duckdb", import_policy="permissive"),
    )
    lowered = lower_ossie_document(parsed)
    assert not lowered.valid
    graph = next(iter(lowered.catalog.scopes.values())).graph
    assert "orders.amount" not in graph.metrics
    assert "unsafe_reference" not in graph.metrics
    assert "orders.raw" in graph.metrics


@pytest.mark.parametrize("function", ["AGG", "agg", "Agg"])
@pytest.mark.parametrize("location", ["field", "dataset_metric", "model_metric"])
def test_latest_model_declarations_reject_reserved_agg_calls(function, location):
    data = document()
    if location == "field":
        data["datasets"][0]["fields"][0]["expression"] = {"sql": f"{function}(amount)"}
    elif location == "dataset_metric":
        data["datasets"][0]["metrics"][0]["expression"] = {"sql": f"{function}(amount)"}
    else:
        data["metrics"] = [field("model_total", f"{function}(orders.amount)")]
    lowered = lower(data)
    assert not lowered.valid
    assert not lowered.executable
    assert any(d.code == "ossie.semantic.expression.agg_reserved" for d in lowered.diagnostics)


def test_agg_vendor_function_and_historical_portable_function_remain_valid():
    data = {
        "version": "0.2.0.dev0",
        "name": "sales",
        "datasets": [
            {
                "name": "orders",
                "source": "orders",
                "fields": [
                    {"name": "amount", "expression": {"dialects": [{"dialect": "ANSI_SQL", "expression": "amount"}]}}
                ],
            }
        ],
        "metrics": [
            {"name": "total", "expression": {"dialects": [{"dialect": "ANSI_SQL", "expression": "AGG(orders.amount)"}]}}
        ],
    }
    assert lower(data, LATEST_OSSIE_SCHEMA_COMMIT).valid
    data["metrics"][0]["expression"]["dialects"][0]["dialect"] = "OSSIE_SQL_2026"
    assert lower(data, CURRENT_OSSIE_SCHEMA_COMMIT).valid


def test_portable_agg_identifier_and_overridden_portable_variant_are_not_calls():
    data = document()
    data["datasets"][0]["fields"][0]["expression"] = {
        "sql": "agg",
        "dialects": [{"dialect": "OSSIE_SQL_2026", "expression": "AGG(amount)"}],
    }
    assert lower(data).valid


@pytest.mark.parametrize(
    "metadata", [{"dialect": "CUSTOM"}, {"source": "t", "metrics": []}, {"nested": {"expression": {"sql": "amount"}}}]
)
def test_opaque_ai_metadata_does_not_select_a_new_schema_revision(metadata):
    data = {
        "version": "0.2.0.dev0",
        "name": "sales",
        "datasets": [{"name": "orders", "source": "orders"}],
        "ai_context": metadata,
    }
    lowered = lower(data)
    assert lowered.valid, lowered.diagnostics
    assert lowered.parse_result.profile.schema_revision == CURRENT_OSSIE_SCHEMA_COMMIT
