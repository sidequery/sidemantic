"""Standalone mapping preservation and offline reference checks at 172a3e5."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest
import yaml

from sidemantic.interchange.ossie import (
    OssieParseOptions,
    lower_ossie_document,
    parse_ossie_document,
    serialize_ossie_document,
)
from sidemantic.interchange.ossie.mapping import validate_ossie_mapping

LATEST = "172a3e566f2fc9c0a733ebbb470e97c33d2bd8b1"
FIXTURES = Path(__file__).parents[2] / "ossie-fixtures/upstream/172a3e5/examples"


@pytest.fixture
def catalog():
    ontology = {
        "version": "0.2.0.dev0",
        "name": "business",
        "ontology": [
            {
                "concept": "Person",
                "type": "EntityType",
                "relationships": [
                    {"name": "name", "roles": [{"concept": "String"}], "verbalizes": ["Person has String"]}
                ],
            },
            {"concept": "Customer", "type": "EntityType", "extends": ["Person"]},
        ],
    }
    model = {
        "version": "0.2.0.dev0",
        "name": "sales",
        "datasets": [
            {"name": "customers", "source": "customers", "fields": [{"name": "name", "expression": {"sql": "name"}}]}
        ],
    }
    mapping = {
        "version": "0.2.0.dev0",
        "name": "customer_mapping",
        "ontology_ref": {"name": "business"},
        "semantic_model_ref": {"name": "sales"},
        "concept_mappings": [
            {
                "concept": "Customer",
                "object_mappings": [
                    {"referent_mappings": [{"relationship": "Person.name", "expression": "CUSTOMERS.name"}]}
                ],
            }
        ],
    }
    return mapping, {"business": ontology, "sales": model}


def _referent(mapping):
    return mapping["concept_mappings"][0]["object_mappings"][0]["referent_mappings"][0]


def test_latest_mapping_preserves_upstream_source_without_executable_scopes():
    raw = (FIXTURES / "flights.mapping.yaml").read_bytes()
    parsed = parse_ossie_document(
        raw, options=OssieParseOptions(validate_schema=True, preservation_policy="source-bytes")
    )
    assert parsed.valid, parsed.diagnostics
    assert parsed.profile.schema_revision == LATEST
    assert type(parsed.document).__name__ == "OssieMappingDocument"
    result = lower_ossie_document(parsed)
    assert result.valid, result.diagnostics
    assert not result.catalog.scopes
    assert {d.code for d in result.diagnostics} <= {
        "ossie.semantic.mapping.reference_unresolved",
        "ossie.semantic.mapping.expression_unchecked",
    }
    serialized = serialize_ossie_document(parsed.document, "yaml", exact_source=True)
    assert serialized.exact_source_reused
    assert serialized.data == raw
    canonical = serialize_ossie_document(parsed.document, "json")
    assert json.loads(canonical.data) == yaml.safe_load(raw)


def test_latest_mapping_checks_real_upstream_referenced_documents():
    documents = {
        name: yaml.safe_load((FIXTURES / f"flights.{kind}.yaml").read_text())
        for name, kind in [("./flights.ontology.yaml", "ontology"), ("./flights.semantic_model.yaml", "semantic_model")]
    }
    mapping = yaml.safe_load((FIXTURES / "flights.mapping.yaml").read_text())
    result = validate_ossie_mapping(mapping, references=documents)
    assert result.valid, result.diagnostics
    assert all(d.code == "ossie.semantic.mapping.expression_unchecked" for d in result.diagnostics)


def test_latest_upstream_logical_source_is_preserved_with_portable_sql():
    raw = (FIXTURES / "flights.semantic_model.yaml").read_bytes()
    parsed = parse_ossie_document(raw, options=OssieParseOptions(validate_schema=True))
    assert parsed.valid, parsed.diagnostics
    assert parsed.profile.schema_revision == LATEST
    serialized = serialize_ossie_document(parsed.document, "json")
    assert json.loads(serialized.data) == yaml.safe_load(raw)


def test_mapping_resolves_inherited_relationships_and_normalized_fields(catalog):
    mapping, references = catalog
    result = validate_ossie_mapping(mapping, references=references)
    assert result.valid, result.diagnostics
    assert not result.diagnostics


@pytest.mark.parametrize("kind", ["name", "schema", "concept", "field", "relationship", "owner", "empty"])
def test_mapping_rejects_invalid_references(catalog, kind):
    mapping, references = copy.deepcopy(catalog)
    if kind == "name":
        references["business"]["name"] = "different"
    elif kind == "schema":
        references["sales"]["datasets"][0]["source"] = 42
    elif kind == "concept":
        mapping["concept_mappings"][0]["concept"] = "Undeclared"
    elif kind == "field":
        _referent(mapping)["expression"] = "customers.missing"
    elif kind == "relationship":
        _referent(mapping)["relationship"] = "missing"
    elif kind == "owner":
        _referent(mapping)["relationship"] = "String.name"
    elif kind == "empty":
        _referent(mapping)["expression"] = "-- comment only"
    result = validate_ossie_mapping(mapping, references=references)
    assert not result.valid
    assert result.failure_stage == "semantic"
    assert result.diagnostics


def test_mapping_does_not_resolve_network_or_local_iris_implicitly(catalog, monkeypatch):
    import urllib.request

    def no_io(*args, **kwargs):
        pytest.fail("A preserved mapping must not fetch references implicitly")

    monkeypatch.setattr(urllib.request, "urlopen", no_io)
    mapping, _ = catalog
    mapping["ontology_ref"]["iri"] = "https://example.invalid/ontology.yaml"
    mapping["semantic_model_ref"]["iri"] = "/no-such-file.yaml"
    result = validate_ossie_mapping(mapping)
    assert result.valid
    assert len(result.diagnostics) == 2
    assert {d.code for d in result.diagnostics} == {"ossie.semantic.mapping.reference_unresolved"}


def test_mapping_subquery_bindings_do_not_hide_correlated_model_references(catalog):
    mapping, references = catalog
    _referent(mapping)["expression"] = "EXISTS(SELECT 1 FROM physical AS p WHERE p.x = customers.name)"
    assert validate_ossie_mapping(mapping, references=references).valid
    _referent(mapping)["expression"] = "EXISTS(SELECT 1 FROM physical AS p WHERE p.x = customers.missing)"
    result = validate_ossie_mapping(mapping, references=references)
    assert not result.valid
    assert any(d.code == "ossie.semantic.mapping.field_unknown" for d in result.diagnostics)


def test_mapping_reference_identifiers_respect_quoting(catalog):
    mapping, references = catalog
    references["sales"]["datasets"][0]["fields"][0]["name"] = '"lower"'
    _referent(mapping)["expression"] = 'customers."lower"'
    assert validate_ossie_mapping(mapping, references=references).valid
    _referent(mapping)["expression"] = "customers.lower"
    assert not validate_ossie_mapping(mapping, references=references).valid


def test_mapping_link_tuple_arity_is_checked(catalog):
    mapping, references = catalog
    mapping["concept_mappings"][0]["link_mappings"] = [
        {"relationship": "Person.name", "object_mapping": {"expression": "customers.name"}}
    ]
    result = validate_ossie_mapping(mapping, references=references)
    assert not result.valid
    assert any(d.code == "ossie.semantic.mapping.relationship_arity" for d in result.diagnostics)
