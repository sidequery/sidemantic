"""Pinned schema assertions and declared-language semantics across importers."""

import json
from pathlib import Path

import pytest

from sidemantic.interchange.ossie import (
    LATEST_OSSIE_SCHEMA_COMMIT,
    OssieParseOptions,
    lower_ossie_document,
    parse_ossie_document,
)
from tests.interchange.ossie.test_review_import_boundaries import _document
from tests.rust_layer_adapter import rust_ossie_select_scope, rust_ossie_validate


def _shared(document, target="duckdb"):
    parsed = parse_ossie_document(
        json.dumps(document).encode(),
        options=OssieParseOptions(validate_schema=True, schema_revision=LATEST_OSSIE_SCHEMA_COMMIT),
    )
    return lower_ossie_document(parsed, target_dialect=target)


def _replace(document, path, value):
    cursor = document
    for part in path[:-1]:
        cursor = cursor[part]
    cursor[path[-1]] = value


@pytest.mark.parametrize(
    "path,value,code",
    [
        (("custom_extensions",), [{"vendor_name": "", "data": ""}], "min_length"),
        (("custom_extensions",), [{"vendor_name": "Acme", "data": None}], "type"),
        (("custom_extensions",), [{"vendor_name": "Acme"}], "required"),
        (("custom_extensions",), [{"vendor_name": "Acme", "data": "", "extra": 1}], "additional_properties"),
        (("datasets",), [], "min_items"),
        (("datasets",), None, "type"),
        (("datasets", 0, "fields", 0, "expression"), {}, "any_of"),
        (("datasets", 0, "fields", 0, "expression"), {"sql": None}, "type"),
        (("datasets", 0, "fields", 0, "expression"), {"sql": ""}, "min_length"),
        (("datasets", 0, "fields", 0, "expression"), {"dialects": []}, "min_items"),
        (("datasets", 0, "fields", 0, "expression"), {"sql": "amount", "unexpected": True}, "additional_properties"),
        (("datasets", 0, "fields", 0, "datatype"), "NotAType", "enum"),
        (("datasets", 0, "fields", 0, "dimension"), {"is_time": 1}, "type"),
        (("datasets", 0, "primary_key"), [], "min_items"),
        (("datasets", 0, "unique_keys"), [[]], "min_items"),
        (("datasets", 0, "unique_keys"), [], "min_items"),
        (("ai_context",), None, "one_of"),
        (("ai_context",), {"synonyms": [3]}, "one_of"),
        (("ai_context",), {"instructions": False}, "one_of"),
        (("description",), False, "type"),
    ],
)
def test_native_rejects_every_pinned_schema_assertion(path, value, code):
    document = _document()
    _replace(document, path, value)
    shared = _shared(document)
    native = rust_ossie_validate(json.dumps(document), "json")
    assert not shared.valid
    assert not native["valid"], native
    assert native["validation_mode"] == "pinned_json_schema"
    assert any(item["code"] == f"ossie.schema.{code}" for item in native["diagnostics"]), native
    with pytest.raises(ValueError, match="strict handoff rejected"):
        rust_ossie_select_scope(json.dumps(document), "json", target="DUCKDB")


@pytest.mark.parametrize("location", ["model", "dataset", "field", "dataset_metric", "model_metric"])
def test_valid_opaque_metadata_and_empty_extension_payloads(location):
    document = _document()
    declaration = {
        "model": document,
        "dataset": document["datasets"][0],
        "field": document["datasets"][0]["fields"][0],
        "dataset_metric": document["datasets"][0]["metrics"][0],
        "model_metric": document["metrics"][0],
    }[location]
    declaration["custom_extensions"] = [{"vendor_name": "An arbitrary vendor", "data": ""}]
    declaration["ai_context"] = {"examples": [""], "opaque": {"type": "not a schema", "AGG": [None]}}
    assert _shared(document).valid
    native = rust_ossie_validate(json.dumps(document), "json")
    assert native["valid"], native
    assert rust_ossie_select_scope(json.dumps(document), "json", target="DUCKDB")


@pytest.mark.parametrize(
    "label",
    [
        "OSSIE_SQL_2026",
        "ANSI_SQL",
        "POSTGRES",
        "SNOWFLAKE",
        "BIGQUERY",
        "DATABRICKS",
        "REDSHIFT",
        "TRINO",
        "CLICKHOUSE",
    ],
)
@pytest.mark.parametrize("owner", ["dataset", "model"])
def test_all_known_variants_resolve_declared_metric_fields(label, owner):
    document = _document()
    declaration = document["datasets"][0]["metrics"][0] if owner == "dataset" else document["metrics"][0]
    declaration["expression"] = {
        "dialects": [
            {"dialect": label, "expression": "SUM(missing)" if owner == "dataset" else "SUM(orders.missing)"},
            *(
                []
                if label == "ANSI_SQL"
                else [
                    {"dialect": "ANSI_SQL", "expression": "SUM(amount)" if owner == "dataset" else "SUM(orders.amount)"}
                ]
            ),
        ]
    }
    assert not _shared(document).valid
    native = rust_ossie_validate(json.dumps(document), "json")
    assert not native["valid"], native
    assert any(item["code"] == "ossie.semantic.metric.reference_invalid" for item in native["diagnostics"])


@pytest.mark.parametrize("sql", ["SUM(missing)", "SUM(orders.amount)", "SUM(db.source.amount)"])
def test_nonselected_variant_cannot_bypass_validation(sql):
    document = _document()
    document["datasets"][0]["metrics"][0]["expression"]["dialects"] = [{"dialect": "POSTGRES", "expression": sql}]
    assert not _shared(document).valid
    native = rust_ossie_validate(json.dumps(document), "json")
    assert not native["valid"], native


@pytest.mark.parametrize("target", ["clickhouse", "spark"])
@pytest.mark.parametrize("exact", [False, True])
def test_new_native_targets_import_portable_and_exact_variants(target, exact):
    document = _document()
    if exact or target == "spark":
        for declaration in [
            *document["datasets"][0]["fields"],
            *document["datasets"][0]["metrics"],
            *document["metrics"],
        ]:
            sql = declaration["expression"].pop("sql")
            declaration["expression"]["dialects"] = [
                {"dialect": target.upper() if exact else "ANSI_SQL", "expression": sql}
            ]
    shared = _shared(document, target)
    assert shared.valid and shared.executable, shared.diagnostics
    assert rust_ossie_select_scope(json.dumps(document), "json", target=target.upper())


def test_schema_assets_in_standalone_crate_are_exact_pinned_copies():
    root = Path(__file__).resolve().parents[3]
    for version in ["0.1.1", "0.2.0.dev0", "0.2.0.dev0-b6c702e", "0.2.0.dev0-172a3e5"]:
        assert (root / f"sidemantic-rs/schemas/ossie/{version}.json").read_bytes() == (
            root / f"sidemantic/interchange/ossie/schemas/logical/{version}/schema.json"
        ).read_bytes()


@pytest.mark.parametrize("sql", ["SUM(", "SUM(amount); SELECT 1", "SUM(amount) AS alias", "SUM(amount) FROM orders"])
@pytest.mark.parametrize("kind", ["field", "metric"])
def test_nonselected_known_sql_must_be_one_valid_declaration(sql, kind):
    document = _document()
    declaration = document["datasets"][0]["fields" if kind == "field" else "metrics"][0]
    declaration["expression"]["dialects"] = [{"dialect": "POSTGRES", "expression": sql}]
    shared = _shared(document)
    native = rust_ossie_validate(json.dumps(document), "json")
    assert not shared.valid, shared.diagnostics
    assert not native["valid"], native
    assert any(item.code == "ossie.semantic.expression.invalid_sql" for item in shared.diagnostics)
    assert any(item["code"] == "ossie.semantic.expression.invalid_sql" for item in native["diagnostics"])


def test_unknown_vendor_expression_is_opaque_even_when_it_is_not_sql():
    document = _document()
    document["datasets"][0]["metrics"][0]["expression"]["dialects"] = [
        {"dialect": "MyVendorLanguage", "expression": "not SQL; AGG( [ opaque data"}
    ]
    assert _shared(document).valid
    native = rust_ossie_validate(json.dumps(document), "json")
    assert native["valid"], native
    assert rust_ossie_select_scope(json.dumps(document), "json", target="DUCKDB")


@pytest.mark.parametrize("revision", ["0.1.1", "831f48e", "b6c702e", "172a3e5"])
@pytest.mark.parametrize("valid", [False, True])
def test_all_pinned_logical_schema_revisions_validate_native_and_shared(revision, valid):
    from sidemantic.interchange.ossie import resolve_ossie_profile
    from sidemantic.interchange.ossie.validation import validate_ossie_schema
    from tests.rust_layer_adapter import _rust_request

    scope = {
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
        "custom_extensions": [{"vendor_name": "COMMON", "data": "valid" if valid else None}],
    }
    version = "0.1.1" if revision == "0.1.1" else "0.2.0.dev0"
    document = (
        {"version": version, "semantic_model": [scope]}
        if revision in {"0.1.1", "831f48e"}
        else {"version": version, **scope}
    )
    pin = None if revision == "0.1.1" else revision
    profile = resolve_ossie_profile(version, "ossie-core", schema_revision=pin)
    shared = validate_ossie_schema(document, profile=profile)
    native = _rust_request(
        {"action": "ossie_validate", "content": json.dumps(document), "serialization": "json", "schema_revision": pin}
    )["value"]
    assert shared.valid is valid, shared.diagnostics
    assert native["valid"] is valid, native
    assert native["validation_mode"] == "pinned_json_schema"


def test_native_spark_preserves_ansi_fallback_and_portable_rejection():
    document = _document()
    with pytest.raises(ValueError, match="Unsupported OSSIE_SQL_2026 target SPARK"):
        rust_ossie_select_scope(json.dumps(document), "json", target="SPARK")
    for declaration in [*document["datasets"][0]["fields"], *document["datasets"][0]["metrics"], *document["metrics"]]:
        declaration["expression"]["dialects"] = [
            {"dialect": "ANSI_SQL", "expression": declaration["expression"]["sql"]}
        ]
    assert rust_ossie_select_scope(json.dumps(document), "json", target="SPARK")


def test_vendor_dialect_duplicates_use_trimmed_casefolded_identity():
    document = _document()
    document["datasets"][0]["fields"][0]["expression"]["dialects"] = [
        {"dialect": "Custom", "expression": "opaque"},
        {"dialect": " custom ", "expression": "also opaque"},
    ]
    assert not _shared(document).valid
    native = rust_ossie_validate(json.dumps(document), "json")
    assert not native["valid"], native
    assert any(item["code"] == "ossie.semantic.expression.dialect_duplicate" for item in native["diagnostics"])


@pytest.mark.parametrize("location", ["field", "dataset_metric", "model_metric"])
@pytest.mark.parametrize("form", ["sql", "dialects", "nonselected"])
def test_portable_typed_literals_use_the_portable_source_parser(location, form):
    document = _document()
    declaration = {
        "field": document["datasets"][0]["fields"][0],
        "dataset_metric": document["datasets"][0]["metrics"][0],
        "model_metric": document["metrics"][0],
    }[location]
    text = "HOUR(TIMESTAMP_NTZ '2024-03-01 12:34:56')"
    if location != "field":
        text = f"SUM({text})"
    declaration["expression"] = (
        {"sql": text} if form == "sql" else {"dialects": [{"dialect": "OSSIE_SQL_2026", "expression": text}]}
    )
    if form == "nonselected":
        declaration["expression"]["dialects"].append(
            {"dialect": "DUCKDB", "expression": "12" if location == "field" else "SUM(12)"}
        )
    shared = _shared(document)
    assert shared.valid and shared.executable, shared.diagnostics
    native = rust_ossie_validate(json.dumps(document), "json")
    assert native["valid"], native
    assert rust_ossie_select_scope(json.dumps(document), "json", target="DUCKDB")
