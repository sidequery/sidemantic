"""Shared native fixture checks for Python runtime behavior."""

import json
import logging
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from sidemantic.core.semantic_layer import SemanticLayer
from sidemantic.core.table_calculation import TableCalculation
from sidemantic.loaders import load_from_directory
from sidemantic.sql.query_rewriter import QueryRewriter
from sidemantic.sql.table_calc_processor import TableCalculationProcessor

FIXTURE_SUITE_ROOT = Path(__file__).parents[1] / "native-fixtures"


def fixture_cases():
    manifest = yaml.safe_load((FIXTURE_SUITE_ROOT / "manifest.yml").read_text())
    return [pytest.param(fixture, id=fixture["name"]) for fixture in manifest["fixtures"]]


def fixture_query_cases():
    manifest = yaml.safe_load((FIXTURE_SUITE_ROOT / "manifest.yml").read_text())
    cases = []
    for fixture in manifest["fixtures"]:
        if not fixture.get("valid", True):
            continue
        for query in fixture.get("queries") or []:
            cases.append(
                pytest.param(
                    fixture,
                    query,
                    id=f"{fixture['name']}::{query['name']}",
                )
            )
    return cases


def fixture_rewrite_cases():
    manifest = yaml.safe_load((FIXTURE_SUITE_ROOT / "manifest.yml").read_text())
    cases = []
    for fixture in manifest["fixtures"]:
        if not fixture.get("valid", True):
            continue
        for rewrite in fixture.get("rewrite_queries") or []:
            cases.append(
                pytest.param(
                    fixture,
                    rewrite,
                    id=f"{fixture['name']}::{rewrite['name']}",
                )
            )
    return cases


def normalize_value(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    return value


def assert_expected_validation_contract(fixture_root, fixture):
    expected_validation = fixture.get("expected_validation")
    if not expected_validation:
        return
    expected = json.loads((fixture_root / expected_validation).read_text())
    assert expected["valid"] is fixture.get("valid", True)


@pytest.mark.parametrize("fixture", fixture_cases())
def test_native_fixture_validation(fixture, caplog):
    fixture_root = FIXTURE_SUITE_ROOT / fixture["name"]
    assert_expected_validation_contract(fixture_root, fixture)
    layer = SemanticLayer()

    if fixture.get("valid", True):
        load_from_directory(layer, fixture_root / "models")
        return

    error_text = ""
    with caplog.at_level(logging.WARNING):
        try:
            load_from_directory(layer, fixture_root / "models")
        except Exception as exc:
            error_text = str(exc)

    if not error_text:
        assert not layer.graph.models
        error_text = "\n".join(record.message for record in caplog.records)

    for token in fixture.get("error_contains") or []:
        assert token in error_text


@pytest.mark.parametrize(("fixture", "query_manifest"), fixture_query_cases())
def test_native_fixture_loads_compiles_and_executes(fixture, query_manifest):
    fixture_root = FIXTURE_SUITE_ROOT / fixture["name"]
    layer = SemanticLayer()
    load_from_directory(layer, fixture_root / "models")

    query = yaml.safe_load((fixture_root / query_manifest["file"]).read_text())
    query_kwargs = dict(query)
    parameter_values = query_kwargs.pop("parameter_values", None)
    if parameter_values is not None:
        query_kwargs["parameters"] = parameter_values
    table_calculation_defs = query_kwargs.pop("table_calculations", None)
    table_calculations = [
        value if isinstance(value, TableCalculation) else TableCalculation(**value)
        for value in (table_calculation_defs or [])
    ]

    compiled = layer.compile(**query_kwargs)

    for token in query_manifest.get("sql_contains") or []:
        assert token.lower() in compiled.lower()

    expected_result = query_manifest.get("expected_result")
    if query_manifest.get("rust_expected_result") and not expected_result:
        assert query_manifest.get("rust_only_reason"), "Rust-only expected results must document the divergence"
    if not expected_result:
        return

    layer.adapter.execute((fixture_root / fixture["seed"]).read_text())
    relation = layer.query(**query_kwargs)
    rows = relation.fetchall()
    if table_calculations:
        base_columns = list(getattr(relation, "columns", []) or [])
        if not base_columns:
            base_columns = [column[0] for column in getattr(relation, "description", []) or []]
        rows, result_columns = TableCalculationProcessor(table_calculations).process(rows, base_columns)
        assert result_columns == query_manifest["result_columns"]
    else:
        result_columns = query_manifest["result_columns"]
    actual = [
        {column: normalize_value(value) for column, value in zip(result_columns, row, strict=True)} for row in rows
    ]
    expected = json.loads((fixture_root / expected_result).read_text())

    assert actual == expected


@pytest.mark.parametrize(("fixture", "rewrite_manifest"), fixture_rewrite_cases())
def test_native_fixture_rewrites_semantic_sql(fixture, rewrite_manifest):
    fixture_root = FIXTURE_SUITE_ROOT / fixture["name"]
    layer = SemanticLayer()
    load_from_directory(layer, fixture_root / "models")

    rewritten = QueryRewriter(layer.graph).rewrite(rewrite_manifest["sql"])

    for token in rewrite_manifest.get("sql_contains") or []:
        assert token.lower() in rewritten.lower()
