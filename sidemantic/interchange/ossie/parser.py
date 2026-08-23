"""Exact-byte parsing and document-family classification for Apache Ossie."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import PurePosixPath
from urllib.parse import urlsplit

import yaml

from sidemantic.interchange.ossie.diagnostics import (
    OssieDiagnostic,
    OssieDiagnosticSeverity,
    OssieSourceLocation,
    sort_diagnostics,
)
from sidemantic.interchange.ossie.documents import (
    OssieDocument,
    OssieDocumentSource,
    OssieLogicalDocument,
    OssieOntologyDocument,
    UnsupportedOssieDocument,
)
from sidemantic.interchange.ossie.profiles import (
    OssieConsumerProfile,
    OssieImportPolicy,
    OssieOptions,
    OssiePreservationPolicy,
    OssieProfile,
    OssieProfileError,
    OssieSerialization,
)
from sidemantic.interchange.ossie.validation import SchemaValidationResult, validate_ossie_schema

_MAX_SOURCE_BYTES = 16 * 1024 * 1024
_MAX_NESTING_DEPTH = 256

try:  # pragma: no cover - depends on how PyYAML was built
    from yaml import CSafeLoader as _SafeLoader
except ImportError:  # pragma: no cover - Pyodide does not ship libyaml
    from yaml import SafeLoader as _SafeLoader


@dataclass(frozen=True, slots=True)
class OssieParseOptions:
    """Options known before the document supplies its schema version."""

    serialization: OssieSerialization | None = None
    consumer_profile: OssieConsumerProfile = OssieConsumerProfile.OSSIE_CORE
    import_policy: OssieImportPolicy = OssieImportPolicy.STRICT
    source_dialect: str | None = None
    target_dialect: str | None = None
    preservation_policy: OssiePreservationPolicy = OssiePreservationPolicy.CANONICAL_DATA
    validate_schema: bool = False

    def __post_init__(self) -> None:
        try:
            if self.serialization is not None:
                object.__setattr__(self, "serialization", OssieSerialization(self.serialization))
            object.__setattr__(self, "consumer_profile", OssieConsumerProfile(self.consumer_profile))
            object.__setattr__(self, "import_policy", OssieImportPolicy(self.import_policy))
            object.__setattr__(self, "preservation_policy", OssiePreservationPolicy(self.preservation_policy))
        except ValueError as exc:
            raise OssieProfileError(f"Invalid Ossie parse option: {exc}") from exc

        for field_name in ("source_dialect", "target_dialect"):
            value = getattr(self, field_name)
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise OssieProfileError(f"{field_name} must be a non-empty string when provided")
        if not isinstance(self.validate_schema, bool):
            raise OssieProfileError("validate_schema must be a boolean")


@dataclass(frozen=True, slots=True)
class OssieParseResult:
    """Immutable result of parsing, classifying, and optionally validating bytes."""

    document: OssieDocument
    parse_options: OssieParseOptions
    options: OssieOptions | None
    profile: OssieProfile | None
    parse_diagnostics: tuple[OssieDiagnostic, ...]
    schema_validation: SchemaValidationResult | None = None

    def __post_init__(self) -> None:
        if self.options is None and self.profile is not None:
            raise ValueError("profile cannot be resolved when options are unresolved")
        if self.options is not None and self.profile is not self.options.profile:
            raise ValueError("profile must match the resolved options")
        object.__setattr__(self, "parse_diagnostics", sort_diagnostics(self.parse_diagnostics))

    @property
    def diagnostics(self) -> tuple[OssieDiagnostic, ...]:
        """Return parser and schema diagnostics in stable presentation order."""

        schema_diagnostics = self.schema_validation.diagnostics if self.schema_validation else ()
        return sort_diagnostics((*self.parse_diagnostics, *schema_diagnostics))

    @property
    def valid(self) -> bool:
        return not any(diagnostic.severity is OssieDiagnosticSeverity.ERROR for diagnostic in self.diagnostics)

    @property
    def blocks_lowering(self) -> bool:
        """Whether the selected import policy would block a later lowering stage."""

        return self.parse_options.import_policy is OssieImportPolicy.STRICT and not self.valid


class _DuplicateKeyError(ValueError):
    def __init__(self, key: object, *, line: int | None = None, column: int | None = None) -> None:
        super().__init__(f"Duplicate mapping key {key!r}")
        self.key = key
        self.line = line
        self.column = column


class _NonFiniteJSONNumberError(ValueError):
    pass


class _UniqueKeySafeLoader(_SafeLoader):
    """Safe YAML loader that rejects duplicate mapping keys at every depth."""

    def construct_mapping(self, node: yaml.MappingNode, deep: bool = False) -> dict[object, object]:
        self.flatten_mapping(node)
        mapping: dict[object, object] = {}
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            try:
                duplicate = key in mapping
            except TypeError as exc:
                mark = key_node.start_mark
                raise _DuplicateKeyError(
                    "<unhashable key>",
                    line=mark.line + 1,
                    column=mark.column + 1,
                ) from exc
            if duplicate:
                mark = key_node.start_mark
                raise _DuplicateKeyError(key, line=mark.line + 1, column=mark.column + 1)
            mapping[key] = self.construct_object(value_node, deep=deep)
        return mapping


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise _DuplicateKeyError(key)
        value[key] = item
    return value


def _reject_non_finite_json_number(value: str) -> None:
    raise _NonFiniteJSONNumberError(f"JSON number {value!r} is not finite")


def _source_location(
    identifier: str,
    *,
    line: int | None = None,
    column: int | None = None,
) -> OssieSourceLocation:
    return OssieSourceLocation(identifier=identifier, line=line, column=column)


def _diagnostic(
    *,
    code: str,
    message: str,
    identifier: str,
    json_pointer: str = "",
    line: int | None = None,
    column: int | None = None,
) -> OssieDiagnostic:
    return OssieDiagnostic(
        severity=OssieDiagnosticSeverity.ERROR,
        code=code,
        message=message,
        json_pointer=json_pointer,
        source=_source_location(identifier, line=line, column=column),
    )


def _serialization_from_suffix(identifier: str) -> OssieSerialization | None:
    path = urlsplit(identifier).path
    suffix = PurePosixPath(path).suffix.lower()
    if suffix == ".json":
        return OssieSerialization.JSON
    if suffix in {".yaml", ".yml"}:
        return OssieSerialization.YAML
    return None


def _infer_serialization(source_bytes: bytes, identifier: str) -> OssieSerialization:
    from_suffix = _serialization_from_suffix(identifier)
    if from_suffix is not None:
        return from_suffix

    try:
        text = source_bytes.decode("utf-8-sig")
    except UnicodeDecodeError:
        return OssieSerialization.YAML

    significant = text.lstrip()
    if significant.startswith(("{", "[")):
        return OssieSerialization.JSON
    try:
        json.loads(text)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return OssieSerialization.YAML
    return OssieSerialization.JSON


def _parse_serialized_data(text: str, serialization: OssieSerialization) -> object:
    if serialization is OssieSerialization.JSON:
        return json.loads(
            text,
            object_pairs_hook=_unique_json_object,
            parse_constant=_reject_non_finite_json_number,
        )
    return yaml.load(text, Loader=_UniqueKeySafeLoader)


def _classify_document(
    parsed: object,
    *,
    serialization: OssieSerialization,
    source: OssieDocumentSource,
) -> tuple[OssieDocument, str | None]:
    if not isinstance(parsed, Mapping):
        reason = "The Ossie document root must be an object"
        return (
            UnsupportedOssieDocument(
                canonical_data=parsed,
                serialization=serialization,
                source=source,
                reason=reason,
            ),
            "ossie.document.root_type",
        )

    has_logical_root = "semantic_model" in parsed
    has_ontology_root = "ontology" in parsed or "ontology_mappings" in parsed
    if has_logical_root and has_ontology_root:
        reason = "The document mixes logical semantic_model and ontology root families"
        return (
            UnsupportedOssieDocument(
                canonical_data=parsed,
                serialization=serialization,
                source=source,
                reason=reason,
            ),
            "ossie.document.family_mixed",
        )
    if has_logical_root:
        return (
            OssieLogicalDocument(canonical_data=parsed, serialization=serialization, source=source),
            None,
        )
    if has_ontology_root:
        return (
            OssieOntologyDocument(canonical_data=parsed, serialization=serialization, source=source),
            None,
        )

    reason = "The document contains none of semantic_model, ontology, or ontology_mappings"
    return (
        UnsupportedOssieDocument(
            canonical_data=parsed,
            serialization=serialization,
            source=source,
            reason=reason,
        ),
        "ossie.document.family_missing",
    )


def _resolve_options(
    document: OssieDocument,
    *,
    serialization: OssieSerialization,
    parse_options: OssieParseOptions,
    identifier: str,
) -> tuple[OssieOptions | None, OssieDiagnostic | None]:
    version = document.version
    if version is None:
        canonical = document.canonical_data
        if isinstance(canonical, Mapping) and "version" in canonical:
            diagnostic = _diagnostic(
                code="ossie.profile.version_type",
                message="The Ossie document version must be a string",
                identifier=identifier,
                json_pointer="/version",
            )
        else:
            diagnostic = _diagnostic(
                code="ossie.profile.version_missing",
                message="The Ossie document does not declare a version",
                identifier=identifier,
            )
        return None, diagnostic

    try:
        options = OssieOptions(
            schema_version=version,
            serialization=serialization,
            consumer_profile=parse_options.consumer_profile,
            import_policy=parse_options.import_policy,
            source_dialect=parse_options.source_dialect,
            target_dialect=parse_options.target_dialect,
            preservation_policy=parse_options.preservation_policy,
        )
    except OssieProfileError as exc:
        return (
            None,
            _diagnostic(
                code="ossie.profile.unsupported",
                message=str(exc),
                identifier=identifier,
                json_pointer="/version",
            ),
        )
    return options, None


def _schema_profile_name(document: OssieDocument) -> str | None:
    version = document.version
    if version is None:
        return None
    if isinstance(document, OssieLogicalDocument):
        return f"logical-{version}"
    if isinstance(document, OssieOntologyDocument):
        return f"ontology-{version}"
    return None


def parse_ossie_document(
    source_bytes: bytes,
    *,
    source_identifier: str = "<memory>",
    options: OssieParseOptions | None = None,
) -> OssieParseResult:
    """Parse exact YAML or JSON bytes without lowering into runtime objects.

    Syntax, document-family, profile, and requested schema-validation failures
    are returned as diagnostics rather than escaping as parser tracebacks.
    """

    if not isinstance(source_bytes, bytes):
        raise TypeError("source_bytes must be exact immutable bytes")
    if not source_identifier:
        raise ValueError("source_identifier must not be empty")

    parse_options = options or OssieParseOptions()
    serialization = parse_options.serialization or _infer_serialization(source_bytes, source_identifier)
    media_type = "application/json" if serialization is OssieSerialization.JSON else "application/yaml"
    source = OssieDocumentSource(
        identifier=source_identifier,
        original_bytes=(
            source_bytes if parse_options.preservation_policy is OssiePreservationPolicy.SOURCE_BYTES else None
        ),
        media_type=media_type,
    )

    diagnostics: list[OssieDiagnostic] = []
    if len(source_bytes) > _MAX_SOURCE_BYTES:
        document = UnsupportedOssieDocument(
            canonical_data=None,
            serialization=serialization,
            source=source,
            reason="The source exceeds the parser input budget",
        )
        diagnostics.append(
            _diagnostic(
                code="ossie.parse.limit",
                message=f"Apache Ossie source exceeds the {_MAX_SOURCE_BYTES}-byte parser limit",
                identifier=source_identifier,
            )
        )
        return OssieParseResult(document, parse_options, None, None, tuple(diagnostics))
    try:
        text = source_bytes.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        document = UnsupportedOssieDocument(
            canonical_data=None,
            serialization=serialization,
            source=source,
            reason="The source is not valid UTF-8",
        )
        diagnostics.append(
            _diagnostic(
                code="ossie.parse.encoding",
                message=f"Apache Ossie sources must be UTF-8: {exc}",
                identifier=source_identifier,
            )
        )
        return OssieParseResult(document, parse_options, None, None, tuple(diagnostics))

    try:
        parsed = _parse_serialized_data(text, serialization)
    except _DuplicateKeyError as exc:
        document = UnsupportedOssieDocument(
            canonical_data=None,
            serialization=serialization,
            source=source,
            reason=str(exc),
        )
        diagnostics.append(
            _diagnostic(
                code="ossie.parse.duplicate_key",
                message=str(exc),
                identifier=source_identifier,
                line=exc.line,
                column=exc.column,
            )
        )
        return OssieParseResult(document, parse_options, None, None, tuple(diagnostics))
    except json.JSONDecodeError as exc:
        document = UnsupportedOssieDocument(
            canonical_data=None,
            serialization=serialization,
            source=source,
            reason="Invalid JSON syntax",
        )
        diagnostics.append(
            _diagnostic(
                code="ossie.parse.syntax",
                message=exc.msg,
                identifier=source_identifier,
                line=exc.lineno,
                column=exc.colno,
            )
        )
        return OssieParseResult(document, parse_options, None, None, tuple(diagnostics))
    except yaml.MarkedYAMLError as exc:
        mark = exc.problem_mark
        document = UnsupportedOssieDocument(
            canonical_data=None,
            serialization=serialization,
            source=source,
            reason="Invalid YAML syntax",
        )
        diagnostics.append(
            _diagnostic(
                code="ossie.parse.syntax",
                message=exc.problem or str(exc),
                identifier=source_identifier,
                line=mark.line + 1 if mark else None,
                column=mark.column + 1 if mark else None,
            )
        )
        return OssieParseResult(document, parse_options, None, None, tuple(diagnostics))
    except _NonFiniteJSONNumberError as exc:
        document = UnsupportedOssieDocument(
            canonical_data=None,
            serialization=serialization,
            source=source,
            reason=str(exc),
        )
        diagnostics.append(
            _diagnostic(
                code="ossie.parse.non_json_value",
                message=str(exc),
                identifier=source_identifier,
            )
        )
        return OssieParseResult(document, parse_options, None, None, tuple(diagnostics))
    except RecursionError:
        document = UnsupportedOssieDocument(
            canonical_data=None,
            serialization=serialization,
            source=source,
            reason="The source exceeds the parser nesting budget",
        )
        diagnostics.append(
            _diagnostic(
                code="ossie.parse.limit",
                message=f"Apache Ossie source exceeds the {_MAX_NESTING_DEPTH}-level nesting limit",
                identifier=source_identifier,
            )
        )
        return OssieParseResult(document, parse_options, None, None, tuple(diagnostics))

    stack = [(parsed, 0)]
    nesting_exceeded = False
    while stack:
        value, depth = stack.pop()
        if depth > _MAX_NESTING_DEPTH:
            nesting_exceeded = True
            break
        if isinstance(value, dict):
            stack.extend((child, depth + 1) for child in value.values())
        elif isinstance(value, (list, tuple)):
            stack.extend((child, depth + 1) for child in value)
    if nesting_exceeded:
        document = UnsupportedOssieDocument(
            canonical_data=None,
            serialization=serialization,
            source=source,
            reason="The source exceeds the parser nesting budget",
        )
        diagnostics.append(
            _diagnostic(
                code="ossie.parse.limit",
                message=f"Apache Ossie source exceeds the {_MAX_NESTING_DEPTH}-level nesting limit",
                identifier=source_identifier,
            )
        )
        return OssieParseResult(document, parse_options, None, None, tuple(diagnostics))

    try:
        document, family_diagnostic_code = _classify_document(
            parsed,
            serialization=serialization,
            source=source,
        )
    except (TypeError, ValueError, RecursionError) as exc:
        document = UnsupportedOssieDocument(
            canonical_data=None,
            serialization=serialization,
            source=source,
            reason="Parsed input is outside Ossie's JSON-compatible data model",
        )
        diagnostics.append(
            _diagnostic(
                code="ossie.parse.non_json_value",
                message=str(exc),
                identifier=source_identifier,
            )
        )
        return OssieParseResult(document, parse_options, None, None, tuple(diagnostics))

    if family_diagnostic_code is not None:
        diagnostics.append(
            _diagnostic(
                code=family_diagnostic_code,
                message=document.reason or "Unsupported Ossie document family",
                identifier=source_identifier,
            )
        )

    resolved_options, profile_diagnostic = _resolve_options(
        document,
        serialization=serialization,
        parse_options=parse_options,
        identifier=source_identifier,
    )
    if profile_diagnostic is not None:
        diagnostics.append(profile_diagnostic)

    schema_validation = None
    if parse_options.validate_schema:
        validation_profile = (
            resolved_options.profile if resolved_options is not None else _schema_profile_name(document)
        )
        schema_validation = validate_ossie_schema(
            document.to_parsed_data(),
            profile=validation_profile,
            consumer_profile=parse_options.consumer_profile,
        )

    profile = resolved_options.profile if resolved_options is not None else None
    return OssieParseResult(
        document=document,
        parse_options=parse_options,
        options=resolved_options,
        profile=profile,
        parse_diagnostics=tuple(diagnostics),
        schema_validation=schema_validation,
    )
