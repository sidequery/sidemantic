from __future__ import annotations

import hashlib
import json
import shutil
import socket
import subprocess
import sys
import urllib.request
from pathlib import Path
from typing import Any

import pytest
import yaml

from sidemantic.interchange.ossie import validation
from sidemantic.interchange.ossie.profiles import DBT_1_12_0_1_0_ALIAS, OssieConsumerProfile

TEST_ROOT = Path(__file__).parents[2]
FIXTURE_ROOT = TEST_ROOT / "ossie-fixtures"
SCHEMA_ROOT = Path(validation.__file__).parent / "schemas"


def _load_manifest() -> dict[str, Any]:
    return yaml.safe_load((FIXTURE_ROOT / "manifest.yaml").read_text())


def _load_case(case: dict[str, Any]) -> Any:
    source = (FIXTURE_ROOT / case["input"]).read_text()
    if case["serialization"] == "json":
        return json.loads(source)
    return yaml.safe_load(source)


def _diagnostic_contract(result: validation.SchemaValidationResult) -> list[dict[str, Any]]:
    return [
        {
            "code": diagnostic.code,
            "instance_path": diagnostic.json_pointer,
        }
        for diagnostic in result.diagnostics
    ]


@pytest.mark.parametrize("case", _load_manifest()["cases"], ids=lambda case: case["id"])
def test_manifest_schema_case(case: dict[str, Any]) -> None:
    result = validation.validate_ossie_schema(
        _load_case(case),
        profile=case["profile"],
    )

    assert result.valid is case["expected"]["valid"]
    assert result.profile == case["expected"].get("profile", case["profile"])
    assert _diagnostic_contract(result) == case["expected"]["diagnostics"]


def test_yaml_and_json_are_equivalent_within_each_version() -> None:
    manifest = _load_manifest()
    cases = {case["id"]: case for case in manifest["cases"]}

    for case_ids in manifest["equivalence_groups"].values():
        documents = [_load_case(cases[case_id]) for case_id in case_ids]
        assert documents[1:] == documents[:-1]


@pytest.mark.parametrize(
    ("field", "value", "code", "pointer"),
    [
        ("dialects", "SIGMA", "ossie.schema.type", "/dialects"),
        ("dialects", ["NOT_A_DIALECT"], "ossie.schema.enum", "/dialects/0"),
        ("vendors", "Sigma", "ossie.schema.type", "/vendors"),
        ("vendors", [42], "ossie.schema.type", "/vendors/0"),
    ],
)
def test_current_root_dialect_and_vendor_constraints(field: str, value: Any, code: str, pointer: str) -> None:
    document = json.loads((FIXTURE_ROOT / "cases/logical-0.2-current-dialects-vendors/document.json").read_text())
    document[field] = value

    result = validation.validate_ossie_schema(document)

    assert not result.valid
    assert _diagnostic_contract(result) == [{"code": code, "instance_path": pointer}]


def test_schema_profiles_have_exact_pins_and_integrity() -> None:
    expected = {
        "logical-0.1.1": (
            "faf581054dcf7964d5fe0ceae7d6f415c8ce32a5",
            "c1e9adec39562786aa78809665fba568797b15f4c53a0847d9cbcf2dead1bc94",
            "c1e9adec39562786aa78809665fba568797b15f4c53a0847d9cbcf2dead1bc94",
        ),
        "logical-0.2.0.dev0": (
            "831f48e582731cf1ee2e65380ca5abf8157869c7",
            "22be177612ed665e0af244c586b9c0162f2a3706f8e9b061910f7a8e2a19b8e8",
            "22be177612ed665e0af244c586b9c0162f2a3706f8e9b061910f7a8e2a19b8e8",
        ),
        "ontology-0.2.0.dev0": (
            "831f48e582731cf1ee2e65380ca5abf8157869c7",
            "555820756a7d30bc6986ce1b57feaa9937ec4ddd3288878b8af0e3361d824a41",
            "43ed640d984dda250d2baa0bf11ffc7cc174fc87c71d4e0e943e5d127427f889",
        ),
    }

    profiles = {profile.name: profile for profile in validation.available_schema_profiles()}
    assert set(profiles) == set(expected)

    for name, (commit, runtime_checksum, upstream_checksum) in expected.items():
        profile = profiles[name]
        assert profile.source_commit == commit
        assert commit in profile.source_url
        assert profile.runtime_sha256 == runtime_checksum
        assert profile.upstream_sha256 == upstream_checksum
        schema_bytes = (SCHEMA_ROOT / profile.runtime_path).read_bytes()
        upstream_bytes = (SCHEMA_ROOT / profile.upstream_path).read_bytes()
        assert hashlib.sha256(schema_bytes).hexdigest() == runtime_checksum
        assert hashlib.sha256(upstream_bytes).hexdigest() == upstream_checksum


def test_ontology_runtime_schema_uses_only_pinned_local_refs() -> None:
    logical = json.loads((SCHEMA_ROOT / "logical/0.2.0.dev0/schema.json").read_text())
    upstream = json.loads((SCHEMA_ROOT / "ontology/0.2.0.dev0/upstream.json").read_text())
    runtime = json.loads((SCHEMA_ROOT / "ontology/0.2.0.dev0/schema.json").read_text())

    def refs(value: Any) -> list[str]:
        if isinstance(value, dict):
            return [
                *([value["$ref"]] if "$ref" in value else []),
                *(ref for child in value.values() for ref in refs(child)),
            ]
        if isinstance(value, list):
            return [ref for child in value for ref in refs(child)]
        return []

    assert upstream["$id"] == "https://github.com/apache/ossie/ontology/ontology.json"
    assert upstream["$id"] != logical["$id"]
    assert runtime["$id"] == "urn:sidemantic:ossie:schema:ontology:0.2.0.dev0"
    assert runtime["$id"] != logical["$id"]
    assert any("/apache/ossie/main/" in ref for ref in refs(upstream))
    assert not any(ref.startswith("http") for ref in refs(runtime))
    assert refs(runtime).count("urn:sidemantic:ossie:schema:logical:0.2.0.dev0#/$defs/AIContext") == 1
    assert refs(runtime).count("urn:sidemantic:ossie:schema:logical:0.2.0.dev0#/$defs/SemanticModel") == 1


def test_ontology_validation_cannot_open_network(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_network(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("schema validation attempted network access")

    monkeypatch.setattr(urllib.request, "urlopen", fail_network)
    monkeypatch.setattr(socket, "create_connection", fail_network)

    ontology_case = next(case for case in _load_manifest()["cases"] if case["id"] == "ontology-0.2.0.dev0-offline-refs")
    result = validation.validate_ossie_schema(
        _load_case(ontology_case),
        profile=ontology_case["profile"],
    )
    assert result.valid


def test_optional_validator_is_imported_lazily() -> None:
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; "
                "import sidemantic.interchange.ossie.validation; "
                "assert 'jsonschema' not in sys.modules; "
                "assert 'referencing' not in sys.modules"
            ),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr


def test_missing_optional_validator_fails_explicitly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unavailable() -> tuple[Any, ...]:
        raise ModuleNotFoundError("No module named 'jsonschema'")

    monkeypatch.setattr(validation, "_load_validator_runtime", unavailable)
    document = _load_case(_load_manifest()["cases"][0])

    result = validation.validate_ossie_schema(document, profile="logical-0.1.1")

    assert not result.valid
    assert result.diagnostics[0].code == "ossie.validator.unavailable"
    assert result.failure_stage == "availability"
    assert "sidemantic[ossie]" in result.diagnostics[0].message


def test_auto_detection_requires_an_unambiguous_upstream_profile() -> None:
    valid_document = _load_case(_load_manifest()["cases"][0])
    assert validation.validate_ossie_schema(valid_document).valid

    result = validation.validate_ossie_schema({"version": "0.2.0.dev0", "semantic_model": [], "ontology": []})
    assert not result.valid
    assert result.diagnostics[0].code == "ossie.schema.profile_undetected"
    assert result.failure_stage == "profile"


def test_validation_result_is_serialization_friendly() -> None:
    invalid_case = next(case for case in _load_manifest()["cases"] if case["id"] == "logical-0.1.0-not-upstream-valid")
    result = validation.validate_ossie_schema(
        _load_case(invalid_case),
        profile=invalid_case["profile"],
    )

    serialized = json.loads(json.dumps(result.to_dict()))
    assert serialized["stage"] == "schema"
    assert serialized["valid"] is False
    assert serialized["diagnostics"][0]["json_pointer"] == "/version"


def test_dbt_0_1_0_alias_requires_context_and_validates_as_pinned_0_1_1() -> None:
    document = json.loads((FIXTURE_ROOT / "cases/logical-0.1.1-valid/document.json").read_text())
    document["version"] = "0.1.0"

    without_context = validation.validate_ossie_schema(document)
    wrong_consumer = validation.validate_ossie_schema(
        document,
        consumer_profile=OssieConsumerProfile.OSSIE_CORE,
    )
    by_consumer = validation.validate_ossie_schema(
        document,
        consumer_profile=OssieConsumerProfile.DBT_1_12,
    )
    by_profile = validation.validate_ossie_schema(document, profile=DBT_1_12_0_1_0_ALIAS)

    assert without_context.diagnostics[0].code == "ossie.schema.profile_context_required"
    assert wrong_consumer.diagnostics[0].code == "ossie.schema.profile_context_mismatch"
    assert by_consumer.valid
    assert by_profile.valid
    assert by_profile.profile == "logical-0.1.1"
    assert document["version"] == "0.1.0"


@pytest.mark.parametrize("consumer", [OssieConsumerProfile.OSSIE_CORE, OssieConsumerProfile.DBT_1_12])
def test_ordinary_0_1_1_profiles_remain_valid(consumer: OssieConsumerProfile) -> None:
    document = json.loads((FIXTURE_ROOT / "cases/logical-0.1.1-valid/document.json").read_text())

    result = validation.validate_ossie_schema(document, consumer_profile=consumer)

    assert result.valid
    assert result.profile == "logical-0.1.1"


@pytest.fixture
def temporary_schema_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    schema_root = tmp_path / "schemas"
    shutil.copytree(SCHEMA_ROOT, schema_root)
    monkeypatch.setattr(validation, "_schema_root", lambda: schema_root)
    validation._manifest.cache_clear()
    validation.available_schema_profiles.cache_clear()
    validation._load_schema.cache_clear()
    yield schema_root
    validation._manifest.cache_clear()
    validation.available_schema_profiles.cache_clear()
    validation._load_schema.cache_clear()


@pytest.mark.parametrize(
    ("asset_path", "expected_code"),
    [
        ("ontology/0.2.0.dev0/schema.json", "ossie.schema.runtime_asset_integrity"),
        ("ontology/0.2.0.dev0/upstream.json", "ossie.schema.upstream_asset_integrity"),
    ],
)
def test_tampered_bundle_assets_fail_with_structured_diagnostics(
    temporary_schema_root: Path,
    asset_path: str,
    expected_code: str,
) -> None:
    target = temporary_schema_root / asset_path
    target.write_bytes(target.read_bytes() + b"\n")
    document = _load_case(
        next(case for case in _load_manifest()["cases"] if case["id"] == "ontology-0.2.0.dev0-offline-refs")
    )

    result = validation.validate_ossie_schema(document, profile="ontology-0.2.0.dev0")

    assert not result.valid
    assert result.failure_stage == "integrity"
    assert result.diagnostics[0].code == expected_code


def test_declared_transformations_must_reproduce_runtime_asset(temporary_schema_root: Path) -> None:
    manifest_path = temporary_schema_root / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["profiles"]["ontology-0.2.0.dev0"]["transformations"][1]["to"] = "urn:tampered"
    manifest_path.write_text(json.dumps(manifest))
    validation._manifest.cache_clear()
    validation.available_schema_profiles.cache_clear()
    document = _load_case(
        next(case for case in _load_manifest()["cases"] if case["id"] == "ontology-0.2.0.dev0-offline-refs")
    )

    result = validation.validate_ossie_schema(document, profile="ontology-0.2.0.dev0")

    assert result.diagnostics[0].code == "ossie.schema.transformation_integrity"
