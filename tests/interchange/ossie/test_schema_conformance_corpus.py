"""Deterministic instance mutations for the vendored Apache Ossie schemas.

The corpus deliberately tests instance-observable constraints rather than
repeating every occurrence of a schema keyword.  ``properties``, ``items``,
and ``$defs`` are applicators: their behavior is observed through the child
constraints below.  ``$ref`` is also an applicator and cannot produce a
standalone instance diagnostic; the ontology cases exercise both an external
logical-schema reference and the ``oneOf`` reached through that reference.
Schema annotations such as ``description``, ``title``, ``examples``, ``$id``,
and ``$schema`` have no instance failure mode and are intentionally not
mutated.
"""

from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
import yaml

from sidemantic.interchange.ossie import (
    OssieImportPolicy,
    OssieParseOptions,
    OssieSerialization,
    lower_ossie_document,
    parse_ossie_document,
    validation,
)

TEST_ROOT = Path(__file__).parents[2]
FIXTURE_ROOT = TEST_ROOT / "ossie-fixtures" / "cases"
SCHEMA_ROOT = Path(validation.__file__).parent / "schemas"


@dataclass(frozen=True, slots=True)
class ProfileAsset:
    name: str
    schema_path: str
    fixture_path: str
    fixture_serialization: OssieSerialization


@dataclass(frozen=True, slots=True)
class MutationCase:
    profile: str
    name: str
    keyword: str
    pointer: str
    path: tuple[str | int, ...]
    value: Any = None
    remove: bool = False
    type_value_class: str | None = None

    @property
    def expected_code(self) -> str:
        return {
            "required": "ossie.schema.required",
            "enum": "ossie.schema.enum",
            "const": "ossie.schema.const",
            "minItems": "ossie.schema.min_items",
            "type": "ossie.schema.type",
            "additionalProperties": "ossie.schema.additional_properties",
            "oneOf": "ossie.schema.one_of",
            # A $ref delegates the observable failure to the referenced
            # schema keyword.  This case proves the ontology -> logical ref.
            "ref": "ossie.schema.type",
        }[self.keyword]


PROFILE_ASSETS = (
    ProfileAsset(
        name="logical-0.1.1",
        schema_path="logical/0.1.1/schema.json",
        fixture_path="logical-0.1.1-valid/document.json",
        fixture_serialization=OssieSerialization.JSON,
    ),
    ProfileAsset(
        name="logical-0.2.0.dev0",
        schema_path="logical/0.2.0.dev0/schema.json",
        fixture_path="logical-0.2.0.dev0-valid/document.json",
        fixture_serialization=OssieSerialization.JSON,
    ),
    ProfileAsset(
        name="ontology-0.2.0.dev0",
        schema_path="ontology/0.2.0.dev0/schema.json",
        fixture_path="ontology-0.2.0.dev0-valid/document.yaml",
        fixture_serialization=OssieSerialization.YAML,
    ),
)


def _case(
    profile: str,
    name: str,
    keyword: str,
    pointer: str,
    *path: str | int,
    value: Any = None,
    remove: bool = False,
    type_value_class: str | None = None,
) -> MutationCase:
    return MutationCase(
        profile=profile,
        name=name,
        keyword=keyword,
        pointer=pointer,
        path=path,
        value=value,
        remove=remove,
        type_value_class=type_value_class,
    )


MUTATION_CASES = (
    # OSI 0.1.1: enum is represented by the Dialect definition.
    _case(
        "logical-0.1.1",
        "required-dataset-name",
        "required",
        "/semantic_model/0/datasets/0",
        "semantic_model",
        0,
        "datasets",
        0,
        "name",
        remove=True,
    ),
    _case(
        "logical-0.1.1",
        "enum-dialect",
        "enum",
        "/dialects/0",
        "dialects",
        0,
        value="NOT_A_DIALECT",
    ),
    _case("logical-0.1.1", "const-version", "const", "/version", "version", value="9.9.9"),
    _case(
        "logical-0.1.1",
        "min-items-datasets",
        "minItems",
        "/semantic_model/0/datasets",
        "semantic_model",
        0,
        "datasets",
        value=[],
    ),
    _case(
        "logical-0.1.1",
        "type-primitive-name",
        "type",
        "/semantic_model/0/name",
        "semantic_model",
        0,
        "name",
        value=42,
        type_value_class="primitive",
    ),
    _case(
        "logical-0.1.1",
        "type-object-semantic-model",
        "type",
        "/semantic_model",
        "semantic_model",
        value={},
        type_value_class="object",
    ),
    _case(
        "logical-0.1.1",
        "type-array-name",
        "type",
        "/semantic_model/0/name",
        "semantic_model",
        0,
        "name",
        value=[],
        type_value_class="array",
    ),
    _case(
        "logical-0.1.1",
        "additional-properties-semantic-model",
        "additionalProperties",
        "/semantic_model/0",
        "semantic_model",
        0,
        "unexpected",
        value=True,
    ),
    _case(
        "logical-0.1.1",
        "one-of-ai-context",
        "oneOf",
        "/semantic_model/0/ai_context",
        "semantic_model",
        0,
        "ai_context",
        value=42,
    ),
    # Apache Ossie 0.2: enum is represented by the logical DataType.
    _case(
        "logical-0.2.0.dev0",
        "required-dataset-name",
        "required",
        "/semantic_model/0/datasets/0",
        "semantic_model",
        0,
        "datasets",
        0,
        "name",
        remove=True,
    ),
    _case(
        "logical-0.2.0.dev0",
        "enum-datatype",
        "enum",
        "/semantic_model/0/datasets/0/fields/0/datatype",
        "semantic_model",
        0,
        "datasets",
        0,
        "fields",
        0,
        "datatype",
        value="NOT_A_DATATYPE",
    ),
    _case("logical-0.2.0.dev0", "const-version", "const", "/version", "version", value="9.9.9"),
    _case(
        "logical-0.2.0.dev0",
        "min-items-datasets",
        "minItems",
        "/semantic_model/0/datasets",
        "semantic_model",
        0,
        "datasets",
        value=[],
    ),
    _case(
        "logical-0.2.0.dev0",
        "type-primitive-name",
        "type",
        "/semantic_model/0/name",
        "semantic_model",
        0,
        "name",
        value=42,
        type_value_class="primitive",
    ),
    _case(
        "logical-0.2.0.dev0",
        "type-object-semantic-model",
        "type",
        "/semantic_model",
        "semantic_model",
        value={},
        type_value_class="object",
    ),
    _case(
        "logical-0.2.0.dev0",
        "type-array-name",
        "type",
        "/semantic_model/0/name",
        "semantic_model",
        0,
        "name",
        value=[],
        type_value_class="array",
    ),
    _case(
        "logical-0.2.0.dev0",
        "additional-properties-semantic-model",
        "additionalProperties",
        "/semantic_model/0",
        "semantic_model",
        0,
        "unexpected",
        value=True,
    ),
    _case(
        "logical-0.2.0.dev0",
        "one-of-ai-context",
        "oneOf",
        "/semantic_model/0/ai_context",
        "semantic_model",
        0,
        "ai_context",
        value=42,
    ),
    # Apache Ossie ontology 0.2: component type and multiplicity are enums.
    _case(
        "ontology-0.2.0.dev0",
        "required-component-type",
        "required",
        "/ontology/0",
        "ontology",
        0,
        "type",
        remove=True,
    ),
    _case(
        "ontology-0.2.0.dev0",
        "enum-component-type",
        "enum",
        "/ontology/0/type",
        "ontology",
        0,
        "type",
        value="NOT_A_CONCEPT_TYPE",
    ),
    _case("ontology-0.2.0.dev0", "const-version", "const", "/version", "version", value="9.9.9"),
    _case("ontology-0.2.0.dev0", "min-items-ontology", "minItems", "/ontology", "ontology", value=[]),
    _case(
        "ontology-0.2.0.dev0",
        "type-primitive-name",
        "type",
        "/name",
        "name",
        value=42,
        type_value_class="primitive",
    ),
    _case(
        "ontology-0.2.0.dev0",
        "type-object-ontology",
        "type",
        "/ontology",
        "ontology",
        value={},
        type_value_class="object",
    ),
    _case(
        "ontology-0.2.0.dev0",
        "type-array-name",
        "type",
        "/name",
        "name",
        value=[],
        type_value_class="array",
    ),
    _case(
        "ontology-0.2.0.dev0",
        "additional-properties-component",
        "additionalProperties",
        "/ontology/0",
        "ontology",
        0,
        "unexpected",
        value=True,
    ),
    _case(
        "ontology-0.2.0.dev0",
        "one-of-external-ai-context-ref",
        "oneOf",
        "/ai_context",
        "ai_context",
        value=42,
    ),
    _case(
        "ontology-0.2.0.dev0",
        "external-logical-semantic-model-ref",
        "ref",
        "/ontology_mappings/0/semantic_model",
        "ontology_mappings",
        0,
        "semantic_model",
        value=[],
    ),
)

EXPECTED_CASE_KEYWORDS = {
    "logical-0.1.1": {"required", "enum", "const", "minItems", "type", "additionalProperties", "oneOf"},
    "logical-0.2.0.dev0": {"required", "enum", "const", "minItems", "type", "additionalProperties", "oneOf"},
    "ontology-0.2.0.dev0": {
        "required",
        "enum",
        "const",
        "minItems",
        "type",
        "additionalProperties",
        "oneOf",
        "ref",
    },
}


def _asset(name: str) -> ProfileAsset:
    return next(asset for asset in PROFILE_ASSETS if asset.name == name)


def _load_fixture(asset: ProfileAsset) -> dict[str, Any]:
    source = (FIXTURE_ROOT / asset.fixture_path).read_text(encoding="utf-8")
    value = json.loads(source) if asset.fixture_serialization is OssieSerialization.JSON else yaml.safe_load(source)
    assert isinstance(value, dict)
    return value


def _representative_document(profile: str) -> dict[str, Any]:
    """Return a valid fixture-derived document with ref/enum targets present."""

    document = _load_fixture(_asset(profile))
    if profile == "logical-0.1.1":
        document["dialects"] = ["ANSI_SQL"]

    if profile.startswith("logical-"):
        dataset = document["semantic_model"][0]["datasets"][0]
        dataset["fields"] = [
            {
                "name": "id",
                "expression": {
                    "dialects": [{"dialect": "ANSI_SQL", "expression": "id"}],
                },
            }
        ]
        if profile == "logical-0.2.0.dev0":
            dataset["fields"][0]["datatype"] = "String"
    return document


def _load_schema(asset: ProfileAsset) -> dict[str, Any]:
    schema = json.loads((SCHEMA_ROOT / asset.schema_path).read_text(encoding="utf-8"))
    assert isinstance(schema, dict)
    return schema


def _schema_keywords(value: Any) -> set[str]:
    keywords = {"type", "const", "enum", "minItems", "required", "additionalProperties", "oneOf", "$ref"}
    if isinstance(value, dict):
        return {key for key, child in value.items() if key in keywords} | {
            keyword for child in value.values() for keyword in _schema_keywords(child)
        }
    if isinstance(value, list):
        return {keyword for child in value for keyword in _schema_keywords(child)}
    return set()


def _apply_mutation(document: dict[str, Any], case: MutationCase) -> None:
    parent: Any = document
    for part in case.path[:-1]:
        parent = parent[part]
    leaf = case.path[-1]
    if case.remove:
        del parent[leaf]
    else:
        parent[leaf] = deepcopy(case.value)


def _diagnostic_contract(result: validation.SchemaValidationResult) -> list[tuple[str, str]]:
    return [(diagnostic.code, diagnostic.json_pointer) for diagnostic in result.diagnostics]


@pytest.mark.parametrize("asset", PROFILE_ASSETS, ids=lambda asset: asset.name)
def test_pinned_valid_fixtures_pass(asset: ProfileAsset) -> None:
    """Every corpus starts from the existing valid fixture for its asset."""

    result = validation.validate_ossie_schema(_load_fixture(asset), profile=asset.name)

    assert result.valid
    assert result.profile == asset.name


def test_pinned_schema_assets_expose_the_corpus_keyword_classes() -> None:
    expected = {"type", "const", "required", "additionalProperties", "minItems"}
    for asset in PROFILE_ASSETS:
        schema = _load_schema(asset)
        assert schema["type"] == "object"
        assert schema["properties"]["version"]["const"] in {"0.1.1", "0.2.0.dev0"}
        assert expected <= _schema_keywords(schema)

    assert {"enum", "oneOf"} <= _schema_keywords(_load_schema(_asset("logical-0.1.1")))
    assert {"enum", "oneOf"} <= _schema_keywords(_load_schema(_asset("logical-0.2.0.dev0")))
    # The ontology's AIContext ref reaches the logical schema's oneOf, while
    # the ontology asset itself declares the external $ref rather than a
    # duplicate oneOf.
    assert {"enum", "$ref"} <= _schema_keywords(_load_schema(_asset("ontology-0.2.0.dev0")))


def test_mutation_matrix_covers_each_observable_constraint_for_each_profile() -> None:
    for profile, expected_keywords in EXPECTED_CASE_KEYWORDS.items():
        profile_cases = [case for case in MUTATION_CASES if case.profile == profile]
        assert {case.keyword for case in profile_cases} == expected_keywords
        assert {case.type_value_class for case in profile_cases if case.keyword == "type"} == {
            "primitive",
            "object",
            "array",
        }


@pytest.mark.parametrize("case", MUTATION_CASES, ids=lambda case: f"{case.profile}-{case.name}")
def test_schema_conformance_mutation_fails_at_stable_code_and_pointer(case: MutationCase) -> None:
    baseline = _representative_document(case.profile)
    assert validation.validate_ossie_schema(baseline, profile=case.profile).valid

    mutated = deepcopy(baseline)
    _apply_mutation(mutated, case)
    result = validation.validate_ossie_schema(mutated, profile=case.profile)

    assert not result.valid
    assert _diagnostic_contract(result) == [(case.expected_code, case.pointer)]


def test_strict_parser_and_lowering_block_a_schema_mutation() -> None:
    document = _representative_document("logical-0.2.0.dev0")
    case = next(
        case
        for case in MUTATION_CASES
        if case.profile == "logical-0.2.0.dev0" and case.name == "additional-properties-semantic-model"
    )
    _apply_mutation(document, case)

    parsed = parse_ossie_document(
        json.dumps(document, separators=(",", ":")).encode(),
        source_identifier="schema-conformance.json",
        options=OssieParseOptions(
            serialization=OssieSerialization.JSON,
            import_policy=OssieImportPolicy.STRICT,
            validate_schema=True,
        ),
    )
    assert not parsed.valid
    assert parsed.blocks_lowering
    assert _diagnostic_contract(parsed.schema_validation) == [(case.expected_code, case.pointer)]

    lowered = lower_ossie_document(parsed, target_dialect="duckdb")
    assert not lowered.executable
    assert lowered.catalog.scope_ids == ()
    assert _diagnostic_contract(lowered.schema_validation) == [(case.expected_code, case.pointer)]


def test_permissive_parser_and_lowering_retain_a_schema_diagnostic() -> None:
    document = _representative_document("logical-0.2.0.dev0")
    case = next(
        case
        for case in MUTATION_CASES
        if case.profile == "logical-0.2.0.dev0" and case.name == "additional-properties-semantic-model"
    )
    _apply_mutation(document, case)

    parsed = parse_ossie_document(
        json.dumps(document, separators=(",", ":")).encode(),
        source_identifier="schema-conformance.json",
        options=OssieParseOptions(
            serialization=OssieSerialization.JSON,
            import_policy=OssieImportPolicy.PERMISSIVE,
            validate_schema=True,
        ),
    )
    assert not parsed.valid
    assert not parsed.blocks_lowering
    assert _diagnostic_contract(parsed.schema_validation) == [(case.expected_code, case.pointer)]

    lowered = lower_ossie_document(parsed, target_dialect="duckdb")
    assert lowered.executable
    assert lowered.catalog.scope_ids == ("commerce",)
    assert not lowered.valid
    assert _diagnostic_contract(lowered.schema_validation) == [(case.expected_code, case.pointer)]
    scope = lowered.catalog["commerce"]
    assert not scope.valid
    assert [
        (diagnostic.code, diagnostic.json_pointer)
        for diagnostic in scope.diagnostics
        if diagnostic.code == case.expected_code
    ] == [(case.expected_code, case.pointer)]
