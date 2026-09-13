"""Offline JSON Schema validation for pinned Apache Ossie profiles.

The optional ``jsonschema`` and ``referencing`` packages are imported only when
validation is requested. Importing Sidemantic or this module remains safe for
the core Pyodide installation.
"""

from __future__ import annotations

import hashlib
import importlib
import json
from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from functools import cache, lru_cache
from importlib import resources
from typing import Any, Literal

from sidemantic.interchange.ossie.diagnostics import (
    OssieDiagnostic,
    OssieDiagnosticSeverity,
    OssieSchemaProvenance,
    sort_diagnostics,
)
from sidemantic.interchange.ossie.profiles import (
    OssieConsumerProfile,
    OssieProfile,
    resolve_ossie_profile,
)

DocumentKind = Literal["logical", "ontology"]
DiagnosticStage = Literal["profile", "availability", "integrity", "schema"]
JsonPathPart = str | int

_INSTALL_GUIDANCE = (
    "Apache Ossie schema validation requires the optional 'ossie' extra. "
    "Install it with `uv add 'sidemantic[ossie]'`, or use "
    "`uv sync --extra ossie` in a Sidemantic checkout."
)


@dataclass(frozen=True, slots=True)
class SchemaProfile:
    """Identity and provenance for one pinned schema profile."""

    name: str
    document_kind: DocumentKind
    version: str
    schema_dialect: str
    resource_uri: str
    runtime_path: str
    runtime_sha256: str
    upstream_path: str
    upstream_sha256: str
    source_repository: str
    source_commit: str
    source_path: str
    source_url: str
    transformations: tuple[Mapping[str, str], ...]
    dependencies: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SchemaValidationResult:
    """Result of the schema stage; later semantic stages can compose with it."""

    valid: bool
    profile: str | None
    schema_commit: str | None
    diagnostics: tuple[OssieDiagnostic, ...]
    failure_stage: DiagnosticStage | None = None
    stage: Literal["schema"] = "schema"

    def to_dict(self) -> dict[str, Any]:
        diagnostics = []
        for diagnostic in self.diagnostics:
            schema = diagnostic.schema
            diagnostics.append(
                {
                    "code": diagnostic.code,
                    "severity": diagnostic.severity.value,
                    "message": diagnostic.message,
                    "json_pointer": diagnostic.json_pointer,
                    "profile": diagnostic.profile.identifier if diagnostic.profile else None,
                    "schema": (
                        {
                            "schema_id": schema.schema_id,
                            "version": schema.version,
                            "commit": schema.commit,
                            "sha256": schema.sha256,
                        }
                        if schema
                        else None
                    ),
                }
            )
        return {
            "stage": self.stage,
            "failure_stage": self.failure_stage,
            "valid": self.valid,
            "profile": self.profile,
            "schema_commit": self.schema_commit,
            "diagnostics": diagnostics,
        }


class SchemaAssetIntegrityError(RuntimeError):
    """A vendored schema does not match the checksum in its manifest."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)


def _schema_root():
    return resources.files("sidemantic").joinpath("interchange", "ossie", "schemas")


@lru_cache(maxsize=1)
def _manifest() -> Mapping[str, Any]:
    manifest_path = _schema_root().joinpath("manifest.json")
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def _profile_from_record(name: str, record: Mapping[str, Any]) -> SchemaProfile:
    source = record["source"]
    return SchemaProfile(
        name=name,
        document_kind=record["document_kind"],
        version=record["version"],
        schema_dialect=record["schema_dialect"],
        resource_uri=record["resource_uri"],
        runtime_path=record["runtime_path"],
        runtime_sha256=record["runtime_sha256"],
        upstream_path=record["upstream_path"],
        upstream_sha256=record["upstream_sha256"],
        source_repository=source["repository"],
        source_commit=source["commit"],
        source_path=source["path"],
        source_url=source["url"],
        transformations=tuple(record.get("transformations", ())),
        dependencies=tuple(record.get("dependencies", ())),
    )


def _replace_reference_base(value: Any, old: str, new: str) -> None:
    if isinstance(value, dict):
        reference = value.get("$ref")
        if isinstance(reference, str) and reference.startswith(old):
            value["$ref"] = f"{new}{reference[len(old) :]}"
        for child in value.values():
            _replace_reference_base(child, old, new)
    elif isinstance(value, list):
        for child in value:
            _replace_reference_base(child, old, new)


def _apply_declared_transformations(profile: SchemaProfile, upstream: Any) -> Any:
    transformed = deepcopy(upstream)
    for index, transformation in enumerate(profile.transformations):
        operation = transformation.get("operation")
        old = transformation.get("from")
        new = transformation.get("to")
        reason = transformation.get("reason")
        if set(transformation) != {"operation", "from", "to", "reason"} or not all(
            isinstance(value, str) and value for value in (operation, old, new, reason)
        ):
            raise SchemaAssetIntegrityError(
                "ossie.schema.provenance_integrity",
                f"Invalid transformation declaration {index} for {profile.name}",
            )
        if operation == "replace_id":
            if not isinstance(transformed, dict) or transformed.get("$id") != old:
                raise SchemaAssetIntegrityError(
                    "ossie.schema.transformation_integrity",
                    f"Transformation {index} for {profile.name} does not match the upstream $id",
                )
            transformed["$id"] = new
        elif operation == "replace_reference_base":
            _replace_reference_base(transformed, old, new)
        else:
            raise SchemaAssetIntegrityError(
                "ossie.schema.provenance_integrity",
                f"Unknown transformation operation {operation!r} for {profile.name}",
            )
    return transformed


def _read_verified_asset(profile: SchemaProfile, *, upstream: bool) -> tuple[bytes, Any]:
    path = profile.upstream_path if upstream else profile.runtime_path
    expected_sha256 = profile.upstream_sha256 if upstream else profile.runtime_sha256
    asset_kind = "upstream" if upstream else "runtime"
    asset_path = _schema_root().joinpath(*path.split("/"))
    asset_bytes = asset_path.read_bytes()
    actual_sha256 = hashlib.sha256(asset_bytes).hexdigest()
    if actual_sha256 != expected_sha256:
        raise SchemaAssetIntegrityError(
            f"ossie.schema.{asset_kind}_asset_integrity",
            f"Checksum mismatch for {asset_kind} asset {path}: expected {expected_sha256}, got {actual_sha256}",
        )
    try:
        parsed = json.loads(asset_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SchemaAssetIntegrityError(
            f"ossie.schema.{asset_kind}_asset_integrity",
            f"Invalid JSON in {asset_kind} asset {path}: {exc}",
        ) from exc
    return asset_bytes, parsed


def _verify_bundle_integrity(profile: SchemaProfile) -> Mapping[str, Any]:
    runtime_bytes, runtime = _read_verified_asset(profile, upstream=False)
    upstream_bytes, upstream = _read_verified_asset(profile, upstream=True)

    expected_name = f"{profile.document_kind}-{profile.version}"
    repository_prefix = "https://github.com/"
    repository_slug = profile.source_repository.removeprefix(repository_prefix).rstrip("/")
    expected_source_url = (
        f"https://raw.githubusercontent.com/{repository_slug}/{profile.source_commit}/{profile.source_path.lstrip('/')}"
    )
    version_schema = runtime.get("properties", {}).get("version", {}) if isinstance(runtime, dict) else {}
    manifest_profiles = _manifest().get("profiles", {})
    if (
        profile.name != expected_name
        or not profile.source_repository.startswith(repository_prefix)
        or not profile.source_commit
        or profile.source_url != expected_source_url
        or runtime.get("$schema") != profile.schema_dialect
        or not isinstance(runtime.get("$id"), str)
        or profile.resource_uri != f"urn:sidemantic:ossie:schema:{profile.document_kind}:{profile.version}"
        or version_schema.get("const") != profile.version
        or len(profile.dependencies) != len(set(profile.dependencies))
        or any(dependency == profile.name for dependency in profile.dependencies)
        or any(dependency not in manifest_profiles for dependency in profile.dependencies)
    ):
        raise SchemaAssetIntegrityError(
            "ossie.schema.provenance_integrity",
            f"Manifest provenance is inconsistent for {profile.name}",
        )

    if profile.transformations:
        if _apply_declared_transformations(profile, upstream) != runtime:
            raise SchemaAssetIntegrityError(
                "ossie.schema.transformation_integrity",
                f"Runtime asset for {profile.name} does not match its declared transformations",
            )
    elif runtime_bytes != upstream_bytes:
        raise SchemaAssetIntegrityError(
            "ossie.schema.transformation_integrity",
            f"Runtime asset for {profile.name} differs from upstream without declared transformations",
        )
    return runtime


@lru_cache(maxsize=1)
def available_schema_profiles() -> tuple[SchemaProfile, ...]:
    """Return every vendored profile in deterministic manifest order."""

    profiles = _manifest()["profiles"]
    return tuple(_profile_from_record(name, record) for name, record in profiles.items())


def get_schema_profile(name: str) -> SchemaProfile:
    """Return a pinned profile or raise ``KeyError`` for an unknown name."""

    for profile in available_schema_profiles():
        if profile.name == name:
            return profile
    raise KeyError(name)


def detect_schema_profile(document: Any) -> SchemaProfile | None:
    """Identify an unambiguous upstream profile from document shape and version."""

    if not isinstance(document, Mapping):
        return None

    version = document.get("version")
    if version == "0.1.1":
        return get_schema_profile("logical-0.1.1")
    if version != "0.2.0.dev0":
        return None

    has_logical_root = "semantic_model" in document
    has_ontology_root = "ontology" in document or "ontology_mappings" in document
    if has_logical_root == has_ontology_root:
        return None
    if has_logical_root:
        return get_schema_profile("logical-0.2.0.dev0")
    return get_schema_profile("ontology-0.2.0.dev0")


def _json_pointer(path: Sequence[JsonPathPart]) -> str:
    def escape(part: JsonPathPart) -> str:
        return str(part).replace("~", "~0").replace("/", "~1")

    return "" if not path else "/" + "/".join(escape(part) for part in path)


@cache
def _load_schema(profile_name: str) -> Mapping[str, Any]:
    profile = get_schema_profile(profile_name)
    return _verify_bundle_integrity(profile)


def _load_validator_runtime() -> tuple[Any, ...]:
    """Load optional validator dependencies without top-level imports."""

    jsonschema = importlib.import_module("jsonschema")
    referencing = importlib.import_module("referencing")
    referencing_exceptions = importlib.import_module("referencing.exceptions")
    return (
        jsonschema.Draft202012Validator,
        jsonschema.exceptions.SchemaError,
        referencing.Registry,
        referencing.Resource,
        (
            referencing_exceptions.Unresolvable,
            referencing_exceptions.NoSuchResource,
            referencing_exceptions.NoSuchAnchor,
            referencing_exceptions.PointerToNowhere,
        ),
    )


def _diagnostic(
    *,
    code: str,
    message: str,
    profile: SchemaProfile | None,
    ossie_profile: OssieProfile | None = None,
    instance_path: Sequence[JsonPathPart] = (),
) -> OssieDiagnostic:
    schema: OssieSchemaProvenance | None = None
    if profile is not None:
        if ossie_profile is None:
            ossie_profile = resolve_ossie_profile(profile.version, OssieConsumerProfile.OSSIE_CORE)
        schema = OssieSchemaProvenance(
            schema_id=profile.resource_uri,
            version=profile.version,
            commit=profile.source_commit,
            sha256=profile.runtime_sha256,
        )
    return OssieDiagnostic(
        code=code,
        severity=OssieDiagnosticSeverity.ERROR,
        message=message,
        json_pointer=_json_pointer(instance_path),
        profile=ossie_profile,
        schema=schema,
    )


def _result_with_diagnostic(
    diagnostic: OssieDiagnostic,
    profile: SchemaProfile | None,
    failure_stage: DiagnosticStage,
) -> SchemaValidationResult:
    return SchemaValidationResult(
        valid=False,
        profile=profile.name if profile else None,
        schema_commit=profile.source_commit if profile else None,
        diagnostics=(diagnostic,),
        failure_stage=failure_stage,
    )


def _validation_code(keyword: str | None) -> str:
    normalized = {
        "additionalProperties": "additional_properties",
        "maxItems": "max_items",
        "minItems": "min_items",
        "maxLength": "max_length",
        "minLength": "min_length",
        "oneOf": "one_of",
        "anyOf": "any_of",
        "allOf": "all_of",
    }.get(keyword, keyword)
    return f"ossie.schema.{normalized or 'invalid'}"


def _build_validator(profile: SchemaProfile, runtime: tuple[Any, ...]):
    validator_class, schema_error, registry_class, resource_class, _ = runtime
    registry = registry_class()

    for dependency_name in profile.dependencies:
        dependency = get_schema_profile(dependency_name)
        dependency_schema = _load_schema(dependency.name)
        validator_class.check_schema(dependency_schema)
        registry = registry.with_resource(
            dependency.resource_uri,
            resource_class.from_contents(dependency_schema),
        )

    schema = _load_schema(profile.name)
    validator_class.check_schema(schema)
    try:
        return validator_class(schema, registry=registry)
    except TypeError as exc:  # pragma: no cover - guards unsupported old jsonschema
        raise schema_error("Installed jsonschema does not support offline registry validation") from exc


def validate_ossie_schema(
    document: Any,
    *,
    profile: str | SchemaProfile | OssieProfile | None = None,
    consumer_profile: OssieConsumerProfile | str | None = None,
) -> SchemaValidationResult:
    """Validate a parsed Ossie document against one pinned, offline schema.

    Passing ``profile`` is recommended for malformed or migration inputs. Auto
    detection intentionally succeeds only when version and root shape identify
    exactly one upstream document family.
    """

    resolved_profile: SchemaProfile | None
    contract_profile: OssieProfile | None = profile if isinstance(profile, OssieProfile) else None
    try:
        explicit_consumer = OssieConsumerProfile(consumer_profile) if consumer_profile is not None else None
    except ValueError:
        diagnostic = _diagnostic(
            code="ossie.schema.consumer_profile_unknown",
            message=f"Unknown Apache Ossie consumer profile: {consumer_profile}",
            profile=None,
        )
        return _result_with_diagnostic(diagnostic, None, "profile")

    document_version = document.get("version") if isinstance(document, Mapping) else None
    if contract_profile is not None:
        if explicit_consumer is not None and explicit_consumer is not contract_profile.consumer_profile:
            diagnostic = _diagnostic(
                code="ossie.schema.profile_context_mismatch",
                message="Explicit consumer profile does not match the supplied Ossie profile",
                profile=None,
                ossie_profile=contract_profile,
            )
            return _result_with_diagnostic(diagnostic, None, "profile")
        if document_version != contract_profile.schema_version:
            diagnostic = _diagnostic(
                code="ossie.schema.profile_context_mismatch",
                message=(
                    f"Document version {document_version!r} does not match explicit profile "
                    f"{contract_profile.identifier}"
                ),
                profile=None,
                ossie_profile=contract_profile,
                instance_path=("version",),
            )
            return _result_with_diagnostic(diagnostic, None, "profile")
        try:
            if contract_profile.is_compatibility_alias:
                resolved_profile = get_schema_profile(f"logical-{contract_profile.validation_schema_version}")
            else:
                resolved_profile = detect_schema_profile(document) or get_schema_profile(
                    f"logical-{contract_profile.validation_schema_version}"
                )
        except KeyError:
            resolved_profile = None
    elif isinstance(profile, SchemaProfile):
        resolved_profile = profile
    elif isinstance(profile, str):
        try:
            resolved_profile = get_schema_profile(profile)
        except KeyError:
            diagnostic = _diagnostic(
                code="ossie.schema.profile_unknown",
                message=f"Unknown Apache Ossie schema profile: {profile}",
                profile=None,
            )
            return _result_with_diagnostic(diagnostic, None, "profile")
    else:
        resolved_profile = detect_schema_profile(document)

    if contract_profile is None and isinstance(document_version, str):
        if document_version == "0.1.0":
            if explicit_consumer is not OssieConsumerProfile.DBT_1_12:
                code = (
                    "ossie.schema.profile_context_required"
                    if explicit_consumer is None
                    else "ossie.schema.profile_context_mismatch"
                )
                diagnostic = _diagnostic(
                    code=code,
                    message=(
                        "Ossie version 0.1.0 is only a dbt-1.12 compatibility alias; "
                        "pass that consumer/profile context explicitly"
                    ),
                    profile=None,
                    instance_path=("version",),
                )
                return _result_with_diagnostic(diagnostic, None, "profile")
            contract_profile = resolve_ossie_profile(document_version, explicit_consumer)
            resolved_profile = get_schema_profile("logical-0.1.1")
        elif explicit_consumer is not None:
            try:
                contract_profile = resolve_ossie_profile(document_version, explicit_consumer)
            except ValueError as exc:
                diagnostic = _diagnostic(
                    code="ossie.schema.profile_context_mismatch",
                    message=str(exc),
                    profile=resolved_profile,
                    instance_path=("version",),
                )
                return _result_with_diagnostic(diagnostic, resolved_profile, "profile")

    if resolved_profile is None:
        diagnostic = _diagnostic(
            code="ossie.schema.profile_undetected",
            message=(
                "Document version and root shape do not identify exactly one "
                "pinned Apache Ossie schema profile; pass profile explicitly."
            ),
            profile=None,
        )
        return _result_with_diagnostic(diagnostic, None, "profile")

    try:
        _load_schema(resolved_profile.name)
        for dependency_name in resolved_profile.dependencies:
            _load_schema(dependency_name)
    except SchemaAssetIntegrityError as exc:
        diagnostic = _diagnostic(
            code=exc.code,
            message=str(exc),
            profile=resolved_profile,
            ossie_profile=contract_profile,
        )
        return _result_with_diagnostic(diagnostic, resolved_profile, "integrity")

    try:
        runtime = _load_validator_runtime()
    except ImportError:
        diagnostic = _diagnostic(
            code="ossie.validator.unavailable",
            message=_INSTALL_GUIDANCE,
            profile=resolved_profile,
        )
        return _result_with_diagnostic(diagnostic, resolved_profile, "availability")

    _, schema_error, _, _, reference_errors = runtime
    try:
        validator = _build_validator(resolved_profile, runtime)
        validation_document = document
        if contract_profile is not None and contract_profile.is_compatibility_alias:
            # dbt 1.12 emits logical 0.1.0 although its accepted shape is the
            # pinned upstream 0.1.1 schema. Normalize only the validation copy;
            # callers retain and serialize the original 0.1.0 declaration.
            validation_document = dict(document)
            validation_document["version"] = contract_profile.validation_schema_version
        errors = sorted(
            validator.iter_errors(validation_document),
            key=lambda error: (
                _json_pointer(tuple(error.absolute_path)),
                _json_pointer(tuple(error.absolute_schema_path)),
                str(error.validator),
                error.message,
            ),
        )
    except SchemaAssetIntegrityError as exc:
        diagnostic = _diagnostic(
            code=exc.code,
            message=str(exc),
            profile=resolved_profile,
            ossie_profile=contract_profile,
        )
        return _result_with_diagnostic(diagnostic, resolved_profile, "integrity")
    except (schema_error, *reference_errors) as exc:
        diagnostic = _diagnostic(
            code="ossie.schema.resource_error",
            message=f"Pinned schema bundle could not be resolved offline: {exc}",
            profile=resolved_profile,
            ossie_profile=contract_profile,
        )
        return _result_with_diagnostic(diagnostic, resolved_profile, "integrity")

    diagnostics = tuple(
        _diagnostic(
            code=_validation_code(str(error.validator)),
            message=error.message,
            profile=resolved_profile,
            ossie_profile=contract_profile,
            instance_path=tuple(error.absolute_path),
        )
        for error in errors
    )
    return SchemaValidationResult(
        valid=not diagnostics,
        profile=resolved_profile.name,
        schema_commit=resolved_profile.source_commit,
        diagnostics=sort_diagnostics(diagnostics),
        failure_stage="schema" if diagnostics else None,
    )
