"""Latest-profile validation shares identifier rules and respects opaque metadata."""

import json

import pytest

from sidemantic.interchange.ossie import (
    LATEST_OSSIE_SCHEMA_COMMIT,
    OssieParseOptions,
    lower_ossie_document,
    parse_ossie_document,
    serialize_ossie_document,
)
from tests.rust_layer_adapter import rust_ossie_select_scope, rust_ossie_validate


def _document():
    return {
        "version": "0.2.0.dev0",
        "name": "sales",
        "datasets": [
            {
                "name": "orders",
                "source": "SELECT 10 AS amount",
                "fields": [{"name": "amount", "expression": {"sql": "amount * 2"}}],
                "metrics": [{"name": "total", "expression": {"sql": "SUM(amount)"}}],
            }
        ],
        "metrics": [{"name": "model_total", "expression": {"sql": "SUM(orders.amount)"}}],
    }


def _lower(document, target="duckdb"):
    parsed = parse_ossie_document(
        json.dumps(document).encode(),
        options=OssieParseOptions(
            target_dialect=target, validate_schema=True, schema_revision=LATEST_OSSIE_SCHEMA_COMMIT
        ),
    )
    return lower_ossie_document(parsed, target_dialect=target)


def _declaration(document, location):
    return {
        "model": document,
        "dataset": document["datasets"][0],
        "field": document["datasets"][0]["fields"][0],
        "dataset_metric": document["datasets"][0]["metrics"][0],
        "model_metric": document["metrics"][0],
    }[location]


@pytest.mark.parametrize("dialect", ["BIGQUERY", "DATABRICKS"])
@pytest.mark.parametrize("route", ["shared", "native"])
def test_quoted_vendor_identifiers_follow_regular_logical_name_rules(dialect, route):
    document = _document()
    dataset = document["datasets"][0]
    dataset["metrics"][0]["expression"] = {"dialects": [{"dialect": dialect, "expression": "SUM(`AmOuNt`)"}]}
    dataset["metrics"].append(
        {
            "name": "physical_total",
            "expression": {"dialects": [{"dialect": dialect, "expression": "SUM(`source`.`amount`)"}]},
        }
    )
    document["metrics"][0]["expression"] = {"dialects": [{"dialect": dialect, "expression": "SUM(`OrDeRs`.`amount`)"}]}
    if route == "shared":
        result = _lower(document, dialect.lower())
        assert result.valid and result.executable, result.diagnostics
    else:
        assert rust_ossie_select_scope(json.dumps(document), "json", target=dialect)


def test_databricks_filter_uses_one_normalized_dataset_identity():
    document = _document()
    document["metrics"][0]["expression"] = {
        "dialects": [{"dialect": "DATABRICKS", "expression": "SUM(`orders`.`amount`) FILTER (WHERE orders.amount > 0)"}]
    }
    result = _lower(document, "databricks")
    assert result.valid and result.executable, result.diagnostics


@pytest.mark.parametrize("dialect", ["OSSIE_SQL_2026", "ANSI_SQL", "SNOWFLAKE"])
def test_ansi_delimited_identifier_case_remains_significant(dialect):
    document = _document()
    expression = 'SUM("amount")'
    document["datasets"][0]["metrics"][0]["expression"] = (
        {"sql": expression}
        if dialect == "OSSIE_SQL_2026"
        else {"dialects": [{"dialect": dialect, "expression": expression}]}
    )
    result = _lower(document, "snowflake")
    assert not result.valid
    assert any(item.code == "ossie.semantic.metric.reference_invalid" for item in result.diagnostics)


@pytest.mark.parametrize("location", ["model", "dataset", "field", "dataset_metric", "model_metric"])
@pytest.mark.parametrize("route", ["shared", "native"])
def test_agg_lookalikes_in_ai_context_remain_opaque(location, route):
    document = _document()
    _declaration(document, location)["ai_context"] = {
        "expression": {"sql": "AGG(amount)"},
        "nested": {"datasets": [{"fields": [{"expression": {"sql": "AGG(amount)"}}]}]},
    }
    if route == "shared":
        result = _lower(document)
        assert result.valid and result.executable, result.diagnostics
        assert json.loads(serialize_ossie_document(result.document, "json").data) == document
    else:
        result = rust_ossie_validate(json.dumps(document), "json")
        assert result["valid"], result
        assert rust_ossie_select_scope(json.dumps(document), "json", target="DUCKDB")


@pytest.mark.parametrize("location", ["field", "dataset_metric", "model_metric"])
@pytest.mark.parametrize("route", ["shared", "native"])
def test_actual_portable_agg_declarations_still_reject(location, route):
    document = _document()
    document["ai_context"] = {"expression": {"sql": "AGG(amount)"}}
    reference = "orders.amount" if location == "model_metric" else "amount"
    _declaration(document, location)["expression"] = {"sql": f"AGG({reference})"}
    if route == "shared":
        result = _lower(document)
        assert not result.valid
        assert any(item.code == "ossie.semantic.expression.agg_reserved" for item in result.diagnostics)
    else:
        result = rust_ossie_validate(json.dumps(document), "json")
        assert not result["valid"]
        assert any(item["code"] == "ossie.semantic.expression.reserved_function" for item in result["diagnostics"])
