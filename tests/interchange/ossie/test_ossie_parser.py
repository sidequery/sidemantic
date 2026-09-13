from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from sidemantic.interchange.ossie import (
    OSSIE_CORE_0_1_1,
    OSSIE_CORE_0_2_0_DEV0,
    OssieConsumerProfile,
    OssieImportPolicy,
    OssieLogicalDocument,
    OssieOntologyDocument,
    OssieParseOptions,
    OssiePreservationPolicy,
    OssieSerialization,
    UnsupportedOssieDocument,
    parse_ossie_document,
)

LOGICAL_YAML = b"""version: 0.2.0.dev0
semantic_model:
  - name: commerce
    datasets: []
"""

LOGICAL_JSON = b'{"version":"0.1.1","semantic_model":[{"name":"commerce","datasets":[]}]}'


def diagnostic_codes(result) -> list[str]:
    return [diagnostic.code for diagnostic in result.diagnostics]


@pytest.mark.parametrize(
    ("source", "identifier", "expected_serialization", "expected_profile"),
    [
        (LOGICAL_YAML, "commerce.ossie.yaml", OssieSerialization.YAML, OSSIE_CORE_0_2_0_DEV0),
        (LOGICAL_JSON, "commerce.ossie.json", OssieSerialization.JSON, OSSIE_CORE_0_1_1),
        (LOGICAL_YAML, "memory:commerce", OssieSerialization.YAML, OSSIE_CORE_0_2_0_DEV0),
        (LOGICAL_JSON, "memory:commerce", OssieSerialization.JSON, OSSIE_CORE_0_1_1),
    ],
)
def test_parses_exact_bytes_and_infers_serialization_without_selecting_version(
    source,
    identifier,
    expected_serialization,
    expected_profile,
):
    result = parse_ossie_document(source, source_identifier=identifier)

    assert isinstance(result.document, OssieLogicalDocument)
    assert result.document.serialization is expected_serialization
    assert result.document.version == expected_profile.schema_version
    assert result.profile is expected_profile
    assert result.options is not None
    assert result.options.serialization is expected_serialization
    assert result.document.source.identifier == identifier
    assert result.document.source.media_type == f"application/{expected_serialization.value}"
    assert result.document.source.original_bytes is None
    assert result.valid


def test_explicit_serialization_wins_without_overriding_version_or_consumer_profile():
    result = parse_ossie_document(
        LOGICAL_YAML,
        source_identifier="misleading.json",
        options=OssieParseOptions(
            serialization="yaml",
            consumer_profile=OssieConsumerProfile.OSSIE_CORE,
        ),
    )

    assert result.document.serialization is OssieSerialization.YAML
    assert result.profile is OSSIE_CORE_0_2_0_DEV0


def test_source_bytes_are_retained_only_under_source_bytes_policy():
    retained = parse_ossie_document(
        LOGICAL_YAML,
        options=OssieParseOptions(preservation_policy=OssiePreservationPolicy.SOURCE_BYTES),
    )
    canonical_only = parse_ossie_document(LOGICAL_YAML)

    assert retained.document.source.original_bytes == LOGICAL_YAML
    assert retained.document.source.sha256 is not None
    assert canonical_only.document.source.original_bytes is None
    assert canonical_only.document.source.identifier == "<memory>"
    assert canonical_only.document.source.media_type == "application/yaml"


@pytest.mark.parametrize(
    ("root", "expected_type"),
    [
        (b'{"version":"0.2.0.dev0","semantic_model":[]}', OssieLogicalDocument),
        (b'{"version":"0.2.0.dev0","ontology":[]}', OssieOntologyDocument),
        (b'{"version":"0.2.0.dev0","ontology_mappings":[]}', OssieOntologyDocument),
    ],
)
def test_classifies_supported_document_families(root, expected_type):
    result = parse_ossie_document(root)

    assert isinstance(result.document, expected_type)
    assert "ossie.document.family_missing" not in diagnostic_codes(result)
    assert "ossie.document.family_mixed" not in diagnostic_codes(result)


@pytest.mark.parametrize(
    ("root", "expected_code", "reason_fragment"),
    [
        (
            b'{"version":"0.2.0.dev0","semantic_model":[],"ontology":[]}',
            "ossie.document.family_mixed",
            "mixes logical",
        ),
        (b'{"version":"0.2.0.dev0","name":"unknown"}', "ossie.document.family_missing", "none of"),
        (b'["version", "0.2.0.dev0"]', "ossie.document.root_type", "root must be an object"),
    ],
)
def test_mixed_missing_and_non_object_roots_are_focused_unsupported_documents(root, expected_code, reason_fragment):
    result = parse_ossie_document(root)

    assert isinstance(result.document, UnsupportedOssieDocument)
    assert reason_fragment in result.document.reason
    assert expected_code in diagnostic_codes(result)
    assert not result.valid
    assert result.blocks_lowering


def test_version_is_read_from_document_and_unsupported_profile_is_diagnostic():
    result = parse_ossie_document(b'{"version":"9.9","semantic_model":[]}')

    assert isinstance(result.document, OssieLogicalDocument)
    assert result.document.version == "9.9"
    assert result.options is None
    assert result.profile is None
    assert diagnostic_codes(result) == ["ossie.profile.unsupported"]


@pytest.mark.parametrize(
    ("source", "expected_code"),
    [
        (b'{"semantic_model":[]}', "ossie.profile.version_missing"),
        (b'{"version":2,"semantic_model":[]}', "ossie.profile.version_type"),
    ],
)
def test_missing_or_non_string_versions_are_diagnostics(source, expected_code):
    result = parse_ossie_document(source)

    assert result.options is None
    assert result.profile is None
    assert diagnostic_codes(result) == [expected_code]


@pytest.mark.parametrize(
    ("source", "identifier", "expected_line", "expected_column"),
    [
        (b'{"version":"0.2.0.dev0",', "broken.json", 1, 25),
        (b"version: [\nsemantic_model: []\n", "broken.yaml", 3, 1),
    ],
)
def test_syntax_errors_are_unsupported_results_not_parser_tracebacks(
    source,
    identifier,
    expected_line,
    expected_column,
):
    result = parse_ossie_document(source, source_identifier=identifier)

    assert isinstance(result.document, UnsupportedOssieDocument)
    assert diagnostic_codes(result) == ["ossie.parse.syntax"]
    diagnostic = result.diagnostics[0]
    assert diagnostic.source.identifier == identifier
    assert diagnostic.source.line == expected_line
    assert diagnostic.source.column == expected_column


@pytest.mark.parametrize(
    ("source", "identifier", "expected_line"),
    [
        (
            b"version: 0.2.0.dev0\nsemantic_model: []\nsemantic_model: []\n",
            "duplicate.yaml",
            3,
        ),
        (
            b'{"version":"0.2.0.dev0","semantic_model":[],"semantic_model":[]}',
            "duplicate.json",
            None,
        ),
    ],
)
def test_duplicate_keys_never_silently_overwrite(source, identifier, expected_line):
    result = parse_ossie_document(source, source_identifier=identifier)

    assert isinstance(result.document, UnsupportedOssieDocument)
    assert diagnostic_codes(result) == ["ossie.parse.duplicate_key"]
    assert result.diagnostics[0].source.line == expected_line


def test_non_json_yaml_values_are_rejected_with_a_structured_diagnostic():
    result = parse_ossie_document(
        b"version: 0.2.0.dev0\nsemantic_model: []\nloaded_at: 2026-08-23\n",
        source_identifier="typed.yaml",
    )

    assert isinstance(result.document, UnsupportedOssieDocument)
    assert diagnostic_codes(result) == ["ossie.parse.non_json_value"]


def test_optional_schema_validation_is_returned_and_strict_errors_are_visible():
    result = parse_ossie_document(
        b'{"version":"0.2.0.dev0","semantic_model":{}}',
        options=OssieParseOptions(validate_schema=True, import_policy=OssieImportPolicy.STRICT),
    )

    assert result.schema_validation is not None
    assert not result.schema_validation.valid
    assert result.schema_validation.failure_stage == "schema"
    assert "ossie.schema.type" in diagnostic_codes(result)
    assert result.blocks_lowering


def test_permissive_policy_preserves_errors_without_claiming_validity_or_blocking():
    result = parse_ossie_document(
        b'{"version":"0.2.0.dev0","semantic_model":{}}',
        options=OssieParseOptions(validate_schema=True, import_policy=OssieImportPolicy.PERMISSIVE),
    )

    assert not result.valid
    assert not result.blocks_lowering
    assert result.schema_validation is not None


def test_parse_result_and_options_are_immutable():
    result = parse_ossie_document(LOGICAL_YAML)

    with pytest.raises(FrozenInstanceError):
        result.document = UnsupportedOssieDocument(
            canonical_data=None,
            serialization=OssieSerialization.YAML,
        )
    with pytest.raises(FrozenInstanceError):
        result.parse_options.validate_schema = True


def test_dbt_compatibility_alias_comes_from_consumer_options_not_json_extension():
    result = parse_ossie_document(
        b'{"version":"0.1.0","semantic_model":[]}',
        source_identifier="alias.yaml",
        options=OssieParseOptions(
            serialization=OssieSerialization.JSON,
            consumer_profile=OssieConsumerProfile.DBT_1_12,
        ),
    )

    assert result.profile is not None
    assert result.profile.is_compatibility_alias
    assert result.profile.consumer_profile is OssieConsumerProfile.DBT_1_12


def test_dbt_compatibility_alias_validates_with_explicit_consumer_context():
    result = parse_ossie_document(
        b'{"version":"0.1.0","semantic_model":[]}',
        options=OssieParseOptions(
            serialization=OssieSerialization.JSON,
            consumer_profile=OssieConsumerProfile.DBT_1_12,
            validate_schema=True,
        ),
    )

    assert result.valid
    assert result.schema_validation is not None
    assert result.schema_validation.valid
    assert result.schema_validation.profile == "logical-0.1.1"


def test_exact_bytes_are_required():
    with pytest.raises(TypeError, match="exact immutable bytes"):
        parse_ossie_document(bytearray(LOGICAL_YAML))


def test_deeply_nested_json_returns_a_limit_diagnostic_instead_of_recursion_error():
    source = b"[" * 10_000 + b"]" * 10_000

    result = parse_ossie_document(
        source,
        source_identifier="deep.json",
        options=OssieParseOptions(serialization=OssieSerialization.JSON),
    )

    assert isinstance(result.document, UnsupportedOssieDocument)
    assert diagnostic_codes(result) == ["ossie.parse.limit"]


def test_parser_enforces_a_bounded_input_budget():
    result = parse_ossie_document(b" " * (16 * 1024 * 1024 + 1), source_identifier="large.yaml")

    assert isinstance(result.document, UnsupportedOssieDocument)
    assert diagnostic_codes(result) == ["ossie.parse.limit"]
