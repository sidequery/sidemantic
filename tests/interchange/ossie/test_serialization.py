from __future__ import annotations

import json
from dataclasses import FrozenInstanceError

import pytest
import yaml

from sidemantic.interchange.ossie import (
    DBT_1_12_0_1_0_ALIAS,
    OssieConsumerProfile,
    OssieDocumentSource,
    OssieLogicalDocument,
    OssieOntologyDocument,
    OssieParseOptions,
    OssiePreservationPolicy,
    OssieSerialization,
    OssieSerializationError,
    UnsupportedOssieDocument,
    parse_ossie_document,
    serialize_ossie_document,
)

LOGICAL_DATA = {
    "version": "0.2.0.dev0",
    "semantic_model": [
        {
            "name": "commerce",
            "datasets": [{"name": "orders", "source": "analytics.orders"}],
        }
    ],
}

ONTOLOGY_DATA = {
    "version": "0.2.0.dev0",
    "name": "commerce-ontology",
    "ai_context": {"instructions": "Use café concepts consistently."},
    "ontology": [{"concept": "Customer", "type": "EntityType"}],
    "ontology_mappings": [
        {
            "semantic_model": {
                "name": "commerce",
                "datasets": [{"name": "customers", "source": "analytics.customers"}],
            },
            "concept_mappings": [],
        }
    ],
}


def _logical_document(*, source: OssieDocumentSource | None = None) -> OssieLogicalDocument:
    return OssieLogicalDocument(
        canonical_data=LOGICAL_DATA,
        serialization=OssieSerialization.YAML,
        source=source,
    )


def _diagnostic_codes(error: OssieSerializationError) -> list[str]:
    return [diagnostic.code for diagnostic in error.diagnostics]


def test_explicit_exact_source_mode_reuses_equivalent_original_bytes() -> None:
    original = b"# exact comment\nversion: 0.2.0.dev0\nsemantic_model:\n- datasets:\n  - source: analytics.orders\n    name: orders\n  name: commerce\n"
    parsed = parse_ossie_document(
        original,
        source_identifier="commerce.ossie.yaml",
        options=OssieParseOptions(preservation_policy=OssiePreservationPolicy.SOURCE_BYTES),
    )

    result = serialize_ossie_document(parsed.document, "yaml", exact_source=True)

    assert result.data == original
    assert result.exact_source_reused
    assert result.serialization is OssieSerialization.YAML
    assert result.diagnostics == ()
    with pytest.raises(FrozenInstanceError):
        result.data = b"changed"


def test_cross_serialization_is_canonical_and_does_not_reuse_source() -> None:
    source = OssieDocumentSource(original_bytes=b"version: 0.2.0.dev0\nsemantic_model: []\n")
    result = serialize_ossie_document(_logical_document(source=source), OssieSerialization.JSON, exact_source=True)

    assert json.loads(result.data) == LOGICAL_DATA
    assert result.data.endswith(b"\n")
    assert not result.exact_source_reused


def test_canonical_json_and_yaml_are_deterministic_unicode_safe_and_tag_free() -> None:
    document = OssieOntologyDocument(canonical_data=ONTOLOGY_DATA, serialization="json")

    json_results = [serialize_ossie_document(document, "json").data for _ in range(2)]
    yaml_results = [serialize_ossie_document(document, "yaml").data for _ in range(2)]

    assert json_results[0] == json_results[1]
    assert yaml_results[0] == yaml_results[1]
    assert json_results[0].endswith(b"\n")
    assert yaml_results[0].endswith(b"\n")
    assert "café" in json_results[0].decode()
    assert "café" in yaml_results[0].decode()
    assert b"!!python" not in yaml_results[0]
    assert json.loads(json_results[0]) == ONTOLOGY_DATA
    assert yaml.safe_load(yaml_results[0]) == ONTOLOGY_DATA


def test_schema_invalid_document_is_refused_with_structured_diagnostics() -> None:
    document = OssieLogicalDocument(
        canonical_data={"version": "0.2.0.dev0", "semantic_model": {}},
        serialization="json",
    )

    with pytest.raises(OssieSerializationError) as raised:
        serialize_ossie_document(document, "json")

    assert "ossie.schema.type" in _diagnostic_codes(raised.value)
    assert raised.value.diagnostics[0].json_pointer == "/semantic_model"
    assert raised.value.diagnostics[0].schema is not None


@pytest.mark.parametrize(
    ("document", "expected_code"),
    [
        (
            OssieLogicalDocument(
                canonical_data={"version": "9.9", "semantic_model": []},
                serialization="json",
            ),
            "ossie.schema.profile_unknown",
        ),
        (
            UnsupportedOssieDocument(
                canonical_data={"version": "0.2.0.dev0", "name": "unknown"},
                serialization="json",
                reason="document family is unknown",
            ),
            "ossie.serialization.unsupported_document",
        ),
    ],
)
def test_unsupported_versions_and_documents_are_refused(document, expected_code) -> None:
    with pytest.raises(OssieSerializationError) as raised:
        serialize_ossie_document(document, "yaml")

    assert _diagnostic_codes(raised.value) == [expected_code]


def test_stale_retained_source_is_not_blindly_reused() -> None:
    stale_source = b"version: 0.2.0.dev0\nsemantic_model: []\n"
    document = _logical_document(source=OssieDocumentSource(original_bytes=stale_source))

    result = serialize_ossie_document(document, "yaml", exact_source=True)

    assert not result.exact_source_reused
    assert result.data != stale_source
    assert yaml.safe_load(result.data) == LOGICAL_DATA
    assert [diagnostic.code for diagnostic in result.diagnostics] == ["ossie.serialization.exact_source_mismatch"]


def test_dbt_alias_exact_and_canonical_json_retain_0_1_0() -> None:
    original = b'{"version":"0.1.0","semantic_model":[]}\n'
    document = OssieLogicalDocument(
        canonical_data=json.loads(original),
        serialization="json",
        source=OssieDocumentSource(identifier="dbt.json", original_bytes=original),
    )

    exact = serialize_ossie_document(document, "json", exact_source=True, profile=DBT_1_12_0_1_0_ALIAS)
    canonical = serialize_ossie_document(
        document,
        "json",
        consumer_profile=OssieConsumerProfile.DBT_1_12,
    )

    assert exact.data == original
    assert exact.exact_source_reused
    assert json.loads(canonical.data)["version"] == "0.1.0"
    assert b'"version": "0.1.0"' in canonical.data


def test_dbt_alias_serialization_rejects_missing_or_wrong_context() -> None:
    document = OssieLogicalDocument(
        canonical_data={"version": "0.1.0", "semantic_model": []},
        serialization="json",
    )

    with pytest.raises(OssieSerializationError) as missing:
        serialize_ossie_document(document, "json")
    with pytest.raises(OssieSerializationError) as wrong:
        serialize_ossie_document(
            document,
            "json",
            profile=DBT_1_12_0_1_0_ALIAS,
            consumer_profile=OssieConsumerProfile.OSSIE_CORE,
        )

    assert _diagnostic_codes(missing.value) == ["ossie.schema.profile_context_required"]
    assert _diagnostic_codes(wrong.value) == ["ossie.schema.profile_context_mismatch"]
