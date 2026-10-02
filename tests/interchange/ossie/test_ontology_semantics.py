from __future__ import annotations

from copy import deepcopy

import pytest

from sidemantic.interchange.ossie import validate_ossie_semantics
from sidemantic.interchange.ossie.validation import validate_ossie_schema


def _ontology():
    return {
        "version": "0.2.0.dev0",
        "name": "business",
        "ontology": [
            {
                "concept": "Person",
                "type": "EntityType",
                "identify_by": ["id"],
                "relationships": [
                    {"name": "id", "roles": [{"concept": "String"}], "multiplicity": "OneToOne", "verbalizes": []}
                ],
            },
            {"concept": "Employee", "type": "EntityType", "extends": ["Person"]},
            {"concept": "Salary", "type": "ValueType", "extends": ["Decimal"]},
        ],
        "ontology_mappings": [
            {
                "semantic_model": {"name": "sales", "datasets": [{"name": "orders", "source": "orders"}]},
                "concept_mappings": [
                    {"concept": "Person", "object_mappings": [{"concept": "String", "expression": "orders.id"}]}
                ],
            }
        ],
    }


def _codes(document):
    return {diagnostic.code for diagnostic in validate_ossie_semantics(document).diagnostics}


@pytest.mark.parametrize("builtin", ["Any", "Boolean", "Date", "DateTime", "Decimal", "Float", "Integer", "String"])
def test_builtin_concepts_need_no_declaration_in_mapping_or_roles(builtin):
    document = _ontology()
    document["ontology_mappings"][0]["concept_mappings"][0]["object_mappings"][0]["concept"] = builtin
    document["ontology"][0]["relationships"][0]["roles"][0]["concept"] = builtin
    assert validate_ossie_schema(document).valid
    assert validate_ossie_semantics(document).valid


def test_unknown_supertype_and_role_concepts_are_reported_with_source_pointers():
    document = _ontology()
    document["ontology"][1]["extends"] = ["Missing"]
    document["ontology"][0]["relationships"][0]["roles"][0]["concept"] = "Missing"
    result = validate_ossie_semantics(document)
    assert {d.json_pointer for d in result.diagnostics if d.code == "ossie.semantic.ontology.concept_unknown"} == {
        "/ontology/1/extends/0",
        "/ontology/0/relationships/0/roles/0/concept",
    }


def test_duplicate_concepts_and_relationships_are_rejected():
    document = _ontology()
    document["ontology"].append(deepcopy(document["ontology"][0]))
    document["ontology"][0]["relationships"].append(deepcopy(document["ontology"][0]["relationships"][0]))
    assert {"ossie.semantic.ontology.concept_duplicate", "ossie.semantic.ontology.relationship_duplicate"} <= _codes(
        document
    )


@pytest.mark.parametrize("parent", ["Person", "Missing"])
def test_value_concepts_require_a_reachable_builtin_value_base(parent):
    document = _ontology()
    document["ontology"][2]["extends"] = [parent]
    assert "ossie.semantic.ontology.value_base_missing" in _codes(document)


def test_indirect_value_ancestry_is_valid_and_cycles_terminate():
    document = _ontology()
    document["ontology"].append({"concept": "NetSalary", "type": "ValueType", "extends": ["Salary"]})
    assert validate_ossie_semantics(document).valid
    document["ontology"][2]["extends"] = ["NetSalary"]
    assert "ossie.semantic.ontology.value_base_missing" in _codes(document)


def test_entity_cannot_extend_value_concept():
    document = _ontology()
    document["ontology"][1]["extends"] = ["String"]
    assert "ossie.semantic.ontology.supertype_kind" in _codes(document)


def test_repeated_role_concepts_need_distinguishing_names():
    document = _ontology()
    relationship = document["ontology"][0]["relationships"][0]
    relationship["roles"] = [{"concept": "Person"}]
    assert "ossie.semantic.ontology.role_duplicate" in _codes(document)
    relationship["roles"][0]["name"] = "other"
    assert validate_ossie_semantics(document).valid


def test_identifying_and_one_to_one_relationships_must_be_binary():
    document = _ontology()
    document["ontology"][0]["relationships"][0]["roles"] = []
    assert {"ossie.semantic.ontology.identifier_arity", "ossie.semantic.ontology.multiplicity_arity"} <= _codes(
        document
    )


def test_unknown_identifying_relationship_is_rejected():
    document = _ontology()
    document["ontology"][0]["identify_by"] = ["missing"]
    assert "ossie.semantic.ontology.relationship_unknown" in _codes(document)


@pytest.mark.parametrize(
    "mapping",
    [
        {"object_mappings": [{}]},
        {"object_mappings": [{"referent_mappings": [{"relationship": "id"}]}]},
        {},
    ],
)
def test_mapping_requires_its_expression_or_nested_mapping_structure(mapping):
    document = _ontology()
    document["ontology_mappings"][0]["concept_mappings"] = [{"concept": "Person", **mapping}]
    assert _codes(document) & {"ossie.semantic.ontology.object_mapping_empty", "ossie.semantic.ontology.mapping_empty"}


def test_nested_referent_relationships_resolve_in_target_concept():
    document = _ontology()
    document["ontology"].append(
        {
            "concept": "Account",
            "type": "EntityType",
            "identify_by": ["owner"],
            "relationships": [{"name": "owner", "roles": [{"concept": "Person"}], "verbalizes": []}],
        }
    )
    mapping = {
        "concept": "Account",
        "object_mappings": [
            {
                "referent_mappings": [
                    {"relationship": "owner", "referent_mappings": [{"relationship": "id", "expression": "orders.id"}]}
                ]
            }
        ],
    }
    document["ontology_mappings"][0]["concept_mappings"] = [mapping]
    assert validate_ossie_semantics(document).valid
    mapping["object_mappings"][0]["referent_mappings"][0]["referent_mappings"][0]["relationship"] = "missing"
    assert "ossie.semantic.ontology.relationship_unknown" in _codes(document)


def test_link_mapping_relationship_and_arity_are_checked():
    document = _ontology()
    mapping = {
        "concept": "Person",
        "link_mappings": [
            {
                "object_mapping": {"expression": "orders.id"},
                "children": [
                    {"relationship": "id", "object_mapping": {"concept": "String", "expression": "orders.id"}}
                ],
            }
        ],
    }
    document["ontology_mappings"][0]["concept_mappings"] = [mapping]
    assert validate_ossie_semantics(document).valid
    mapping["link_mappings"][0]["relationship"] = "id"
    assert "ossie.semantic.ontology.link_arity" in _codes(document)
    mapping["link_mappings"][0]["relationship"] = "missing"
    assert "ossie.semantic.ontology.relationship_unknown" in _codes(document)


@pytest.mark.parametrize("iri", ["biz:Person", "https://example.org/人", "urn:business:Person", "custom-scheme:Person"])
def test_current_iri_metadata_accepts_qnames_and_absolute_schemes(iri):
    document = _ontology()
    document.pop("ontology_mappings")
    document["prefixes"] = {"biz": "https://example.org/business/"}
    document["ontology"][0]["iri"] = iri
    assert validate_ossie_schema(document).valid
    assert validate_ossie_semantics(document).valid


@pytest.mark.parametrize("iri", ["relative/path", "https://example.org/bad space", "https://example.org/%zz"])
def test_malformed_iri_metadata_is_rejected(iri):
    document = _ontology()
    document["ontology"][0]["iri"] = iri
    assert "ossie.semantic.ontology.iri_invalid" in _codes(document)


def test_intermediate_link_concept_is_inferred_from_descendant_relationship():
    document = _ontology()
    document["ontology"].append(
        {
            "concept": "Company",
            "type": "EntityType",
            "identify_by": ["name"],
            "relationships": [{"name": "name", "roles": [{"concept": "String"}], "verbalizes": []}],
        }
    )
    document["ontology"][0]["relationships"].append(
        {
            "name": "earns_at",
            "roles": [{"concept": "Company"}, {"concept": "Salary"}],
            "verbalizes": [],
        }
    )
    document["ontology_mappings"][0]["concept_mappings"] = [
        {
            "concept": "Person",
            "link_mappings": [
                {
                    "object_mapping": {"expression": "orders.id"},
                    "children": [
                        {
                            "object_mapping": {
                                "referent_mappings": [{"relationship": "name", "expression": "orders.company"}]
                            },
                            "children": [
                                {"relationship": "earns_at", "object_mapping": {"expression": "orders.salary"}}
                            ],
                        }
                    ],
                }
            ],
        }
    ]
    assert validate_ossie_semantics(document).valid
