"""Regression coverage for both immutable 0.2 development schema snapshots."""

from __future__ import annotations

import json
from copy import deepcopy

import pytest

from sidemantic.interchange.ossie import (
    CURRENT_OSSIE_SCHEMA_COMMIT,
    LEGACY_OSSIE_SCHEMA_COMMIT,
    OssieParseOptions,
    is_logical_document_data,
    logical_model_entries,
    parse_ossie_document,
    serialize_ossie_document,
    validate_ossie_semantics,
)
from sidemantic.interchange.ossie.validation import validate_ossie_schema


def _model():
    return {
        "version": "0.2.0.dev0",
        "name": "sales",
        "datasets": [{"name": "orders", "source": "orders"}],
    }


def _ontology():
    return {
        "version": "0.2.0.dev0",
        "name": "business",
        "prefixes": {"biz": "https://example.org/business/"},
        "ontology": [{"concept": "Person", "type": "EntityType", "iri": "biz:Person"}],
        "ontology_mappings": [{"semantic_model": _model(), "concept_mappings": []}],
    }


@pytest.mark.parametrize("current", [False, True])
def test_snapshot_selection_preserves_shape_provenance_and_bytes(current):
    model = _model()
    document = model if current else {"version": model.pop("version"), "semantic_model": [model]}
    raw = json.dumps(document, indent=3).encode() + b"\n\n"
    parsed = parse_ossie_document(
        raw, options=OssieParseOptions(validate_schema=True, preservation_policy="source-bytes")
    )

    assert parsed.valid
    assert parsed.schema_validation.schema_commit == (
        CURRENT_OSSIE_SCHEMA_COMMIT if current else LEGACY_OSSIE_SCHEMA_COMMIT
    )
    assert parsed.document.to_parsed_data() == document
    assert parsed.document.semantic_model_entries[0][0] == ("" if current else "/semantic_model/0")
    assert len(parsed.document.semantic_models) == 1
    serialized = serialize_ossie_document(parsed.document, "json", exact_source=True)
    assert serialized.exact_source_reused
    assert serialized.data == raw
    assert is_logical_document_data(document)
    assert logical_model_entries(document)[0][0] == ("" if current else "/semantic_model/0")


@pytest.mark.parametrize("revision", [CURRENT_OSSIE_SCHEMA_COMMIT, LEGACY_OSSIE_SCHEMA_COMMIT])
def test_explicit_revision_selects_a_pin_instead_of_silently_reinterpreting_shape(revision):
    parsed = parse_ossie_document(
        json.dumps(_model()).encode(),
        options=OssieParseOptions(validate_schema=True, schema_revision=revision),
    )
    assert parsed.valid is (revision == CURRENT_OSSIE_SCHEMA_COMMIT)
    assert parsed.schema_validation.schema_commit == revision


def test_current_schema_has_distinct_profile_identity_and_root_diagnostics():
    model = _model()
    model["datasets"][0]["source"] = ""
    parsed = parse_ossie_document(json.dumps(model).encode(), options=OssieParseOptions(validate_schema=True))
    assert not parsed.valid
    assert parsed.profile.schema_revision == CURRENT_OSSIE_SCHEMA_COMMIT
    assert CURRENT_OSSIE_SCHEMA_COMMIT in parsed.profile.identifier
    assert any(d.json_pointer == "/datasets/0/source" for d in parsed.diagnostics)
    assert all(d.schema.commit == CURRENT_OSSIE_SCHEMA_COMMIT for d in parsed.diagnostics)


def test_flat_semantic_errors_keep_root_pointers():
    model = _model()
    model["datasets"][0]["primary_key"] = ["missing"]
    result = validate_ossie_semantics(model)
    assert not result.valid
    assert result.checked_scopes == ("sales",)
    assert any(d.json_pointer == "/datasets/0/primary_key/0" for d in result.diagnostics)
    assert all(d.profile.schema_revision == CURRENT_OSSIE_SCHEMA_COMMIT for d in result.diagnostics)


@pytest.mark.parametrize("dialect", ["DAX", "OSSIE_SQL_2026"])
def test_new_dialects_are_current_schema_only(dialect):
    model = _model()
    model["datasets"][0]["fields"] = [
        {"name": "amount", "expression": {"dialects": [{"dialect": dialect, "expression": "amount"}]}}
    ]
    assert validate_ossie_schema(model).valid
    legacy = {"version": model.pop("version"), "semantic_model": [model]}
    result = validate_ossie_schema(legacy)
    assert not result.valid
    assert any(d.code == "ossie.schema.enum" for d in result.diagnostics)


def test_current_ontology_resolves_complete_core_references_offline(monkeypatch):
    import socket

    def no_network(*args, **kwargs):
        pytest.fail("Ontology validation attempted network access")

    monkeypatch.setattr(socket, "create_connection", no_network)
    ontology = _ontology()
    result = validate_ossie_schema(ontology)
    assert result.valid
    assert result.schema_commit == CURRENT_OSSIE_SCHEMA_COMMIT
    assert validate_ossie_semantics(ontology).valid
    del ontology["ontology_mappings"][0]["semantic_model"]["version"]
    result = validate_ossie_schema(ontology)
    assert not result.valid
    assert any(d.json_pointer == "/ontology_mappings/0/semantic_model" for d in result.diagnostics)


def test_explicit_current_ontology_pin_survives_ambiguous_source_shape():
    ontology = _ontology()
    del ontology["prefixes"]
    del ontology["ontology"][0]["iri"]
    del ontology["ontology_mappings"]
    raw = json.dumps(ontology).encode()
    result = parse_ossie_document(
        raw,
        options=OssieParseOptions(
            schema_revision=CURRENT_OSSIE_SCHEMA_COMMIT, validate_schema=True, preservation_policy="source-bytes"
        ),
    )
    assert result.valid
    assert result.document.schema_revision == CURRENT_OSSIE_SCHEMA_COMMIT
    assert result.schema_validation.schema_commit == CURRENT_OSSIE_SCHEMA_COMMIT
    serialized = serialize_ossie_document(result.document, "json", exact_source=True)
    assert serialized.exact_source_reused
    assert serialized.data == raw


def test_invalid_flat_document_is_classified_then_schema_validated():
    result = parse_ossie_document(
        b'{"version":"0.2.0.dev0","name":"sales"}', options=OssieParseOptions(validate_schema=True)
    )
    assert not result.valid
    assert [d.code for d in result.diagnostics] == ["ossie.schema.required"]


def test_canonical_current_source_round_trip_keeps_unknown_ai_data():
    model = _model()
    model["ai_context"] = {"custom": {"flag": True, "label": "café"}}
    parsed = parse_ossie_document(json.dumps(model).encode())
    detached = deepcopy(parsed.document.to_parsed_data())
    detached["ai_context"]["custom"]["flag"] = False
    assert parsed.document.to_parsed_data() == model
    for serialization in ("json", "yaml"):
        emitted = serialize_ossie_document(parsed.document, serialization)
        reparsed = parse_ossie_document(
            emitted.data, options=OssieParseOptions(serialization=serialization, validate_schema=True)
        )
        assert reparsed.valid
        assert reparsed.document.to_parsed_data() == model
