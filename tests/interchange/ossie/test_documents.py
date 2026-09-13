from dataclasses import FrozenInstanceError

import pytest

from sidemantic.interchange.ossie import (
    FrozenJSONObject,
    OssieDocumentSource,
    OssieLogicalDocument,
    OssieOntologyDocument,
    OssieSerialization,
    UnsupportedOssieDocument,
)


def test_logical_document_deeply_preserves_canonical_data_and_presence():
    parsed = {
        "version": "0.2.0.dev0",
        "semantic_model": [
            {
                "name": "commerce",
                "datasets": [
                    {
                        "name": "orders",
                        "fields": [
                            {
                                "name": "ordered_at",
                                "dimension": {"is_time": False},
                                "x-field-extension": {"enabled": True},
                            }
                        ],
                    }
                ],
            }
        ],
        "x-root-extension": {"owner": "analytics"},
    }
    original_bytes = b"# retained verbatim when requested\nversion: 0.2.0.dev0\n"
    document = OssieLogicalDocument(
        canonical_data=parsed,
        serialization=OssieSerialization.YAML,
        source=OssieDocumentSource(identifier="models/commerce.ossie.yaml", original_bytes=original_bytes),
    )

    parsed["semantic_model"][0]["name"] = "mutated"
    parsed["x-root-extension"]["owner"] = "mutated"

    assert document.version == "0.2.0.dev0"
    assert document.semantic_models[0]["name"] == "commerce"
    assert document.unknown_data.to_dict() == {"x-root-extension": {"owner": "analytics"}}
    assert document.is_field_present("/semantic_model/0/datasets/0/fields/0/dimension/is_time")
    assert not document.is_field_present("/semantic_model/0/datasets/0/fields/0/dimension/granularity")
    assert document.source is not None
    assert document.source.original_bytes == original_bytes
    assert len(document.source.sha256) == 64


def test_document_data_is_deeply_immutable_but_can_be_thawed_for_serialization():
    document = OssieLogicalDocument(
        canonical_data={"version": "0.1.1", "semantic_model": [{"name": "sales"}]},
        serialization="json",
    )

    assert isinstance(document.canonical_data, FrozenJSONObject)
    with pytest.raises(TypeError):
        document.canonical_data["version"] = "changed"
    with pytest.raises(FrozenInstanceError):
        document.serialization = OssieSerialization.YAML

    thawed = document.to_parsed_data()
    thawed["semantic_model"][0]["name"] = "changed"
    assert document.semantic_models[0]["name"] == "sales"


def test_frozen_json_object_constructor_also_freezes_nested_values():
    source = {"nested": [1, 2]}
    frozen = FrozenJSONObject((("extension", source),))

    source["nested"].append(3)

    assert frozen.to_dict() == {"extension": {"nested": [1, 2]}}


def test_ontology_document_keeps_ontology_and_mapping_families_distinct():
    document = OssieOntologyDocument(
        canonical_data={
            "version": "0.2.0.dev0",
            "name": "commerce-ontology",
            "description": None,
            "ontology": [{"concept": "Order", "type": "EntityType"}],
            "ontology_mappings": [
                {
                    "name": "commerce-mapping",
                    "semantic_model": {"name": "commerce", "datasets": []},
                    "concept_mappings": [],
                }
            ],
            "x-ontology-extension": "preserved",
        },
        serialization=OssieSerialization.JSON,
    )

    assert document.ontology[0]["concept"] == "Order"
    assert document.ontology_mappings[0]["semantic_model"]["name"] == "commerce"
    assert document.is_field_present("/description")
    assert document.unknown_data.to_dict() == {"x-ontology-extension": "preserved"}


def test_unsupported_document_can_preserve_non_object_parsed_data():
    document = UnsupportedOssieDocument(
        canonical_data=["not", {"yet": "classifiable"}],
        serialization=OssieSerialization.YAML,
        reason="root is not an object",
    )

    assert document.to_parsed_data() == ["not", {"yet": "classifiable"}]
    assert document.version is None
    assert document.reason == "root is not an object"


@pytest.mark.parametrize("invalid", [{"value": float("nan")}, {"value": object()}])
def test_documents_reject_values_outside_the_json_compatible_data_model(invalid):
    with pytest.raises((TypeError, ValueError)):
        UnsupportedOssieDocument(canonical_data=invalid, serialization=OssieSerialization.JSON)


def test_source_bytes_are_optional_and_must_be_bytes():
    source = OssieDocumentSource(identifier="memory:logical")
    assert source.original_bytes is None
    assert source.sha256 is None

    with pytest.raises(TypeError, match="original_bytes must be bytes"):
        OssieDocumentSource(original_bytes=bytearray(b"mutable"))
