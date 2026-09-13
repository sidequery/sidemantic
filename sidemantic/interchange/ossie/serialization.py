"""Validated source serialization for Apache Ossie documents.

Canonical serialization is deterministic, but it is not a lexical YAML
round-trip: comments, anchors, aliases, quoting, scalar styles, and whitespace
are not preserved. Exact retained bytes are returned only through the explicit
exact-source mode and only after they parse to the document's canonical data.
"""

from __future__ import annotations

import importlib
import json
from dataclasses import dataclass

from sidemantic.interchange.ossie.diagnostics import (
    OssieDiagnostic,
    OssieDiagnosticSeverity,
    OssieSourceLocation,
    sort_diagnostics,
)
from sidemantic.interchange.ossie.documents import (
    OssieDocument,
    OssieLogicalDocument,
    OssieOntologyDocument,
    UnsupportedOssieDocument,
)
from sidemantic.interchange.ossie.profiles import OssieConsumerProfile, OssieProfile, OssieSerialization


@dataclass(frozen=True, slots=True)
class OssieSerializationResult:
    """Immutable serialized bytes and diagnostics produced without file I/O."""

    data: bytes
    serialization: OssieSerialization
    exact_source_reused: bool
    diagnostics: tuple[OssieDiagnostic, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.data, bytes):
            raise TypeError("serialized data must be bytes")
        object.__setattr__(self, "serialization", OssieSerialization(self.serialization))
        object.__setattr__(self, "diagnostics", sort_diagnostics(self.diagnostics))


class OssieSerializationError(ValueError):
    """Serialization refusal with stable, structured diagnostics."""

    def __init__(self, diagnostics: tuple[OssieDiagnostic, ...]) -> None:
        self.diagnostics = sort_diagnostics(diagnostics)
        message = "; ".join(diagnostic.message for diagnostic in self.diagnostics)
        super().__init__(message or "Apache Ossie serialization failed")


def _source_location(document: OssieDocument) -> OssieSourceLocation | None:
    source = document.source
    if source is None or source.identifier is None:
        return None
    return OssieSourceLocation(identifier=source.identifier)


def _diagnostic(
    document: OssieDocument,
    *,
    code: str,
    message: str,
    severity: OssieDiagnosticSeverity = OssieDiagnosticSeverity.ERROR,
) -> OssieDiagnostic:
    return OssieDiagnostic(
        code=code,
        severity=severity,
        message=message,
        source=_source_location(document),
    )


def _schema_profile_name(document: OssieDocument) -> str | None:
    if document.version is None:
        return None
    if isinstance(document, OssieLogicalDocument):
        return f"logical-{document.version}"
    if isinstance(document, OssieOntologyDocument):
        return f"ontology-{document.version}"
    return None


def _validate_canonical_data(
    document: OssieDocument,
    canonical_data: object,
    *,
    profile: OssieProfile | None,
    consumer_profile: OssieConsumerProfile | str | None,
) -> tuple[OssieDiagnostic, ...]:
    if isinstance(document, UnsupportedOssieDocument):
        reason = f": {document.reason}" if document.reason else ""
        raise OssieSerializationError(
            (
                _diagnostic(
                    document,
                    code="ossie.serialization.unsupported_document",
                    message=f"Unsupported Apache Ossie document cannot be serialized{reason}",
                ),
            )
        )

    profile_name = _schema_profile_name(document)
    if profile_name is None:
        raise OssieSerializationError(
            (
                _diagnostic(
                    document,
                    code="ossie.serialization.version_missing",
                    message="Apache Ossie document must declare a version before serialization",
                ),
            )
        )

    # Validation owns the pinned offline schema bundle and loads jsonschema only
    # when this operation is requested.
    from sidemantic.interchange.ossie.validation import validate_ossie_schema

    validation_profile = profile
    if validation_profile is None and document.version != "0.1.0":
        validation_profile = profile_name
    validation = validate_ossie_schema(
        canonical_data,
        profile=validation_profile,
        consumer_profile=consumer_profile,
    )
    if not validation.valid:
        raise OssieSerializationError(validation.diagnostics)
    return validation.diagnostics


def _exact_source_matches(
    document: OssieDocument,
    source_bytes: bytes,
    consumer_profile: OssieConsumerProfile | str | None,
) -> bool:
    # Reuse the source parser's duplicate-key and JSON-compatibility checks. The
    # import remains off the canonical JSON/YAML serialization path.
    from sidemantic.interchange.ossie.parser import OssieParseOptions, parse_ossie_document

    parsed = parse_ossie_document(
        source_bytes,
        source_identifier=document.source.identifier or "<memory>",
        options=OssieParseOptions(
            serialization=document.serialization,
            consumer_profile=consumer_profile or OssieConsumerProfile.OSSIE_CORE,
        ),
    )
    return parsed.valid and parsed.document.to_parsed_data() == document.to_parsed_data()


def _canonical_json(canonical_data: object) -> bytes:
    text = json.dumps(
        canonical_data,
        allow_nan=False,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    )
    return f"{text}\n".encode()


def _canonical_yaml(document: OssieDocument, canonical_data: object) -> bytes:
    try:
        yaml = importlib.import_module("yaml")
    except ImportError as exc:
        raise OssieSerializationError(
            (
                _diagnostic(
                    document,
                    code="ossie.serialization.yaml_unavailable",
                    message="Canonical YAML serialization requires PyYAML",
                ),
            )
        ) from exc

    text = yaml.safe_dump(
        canonical_data,
        allow_unicode=True,
        default_flow_style=False,
        sort_keys=True,
    )
    return f"{text.rstrip(chr(10))}\n".encode()


def serialize_ossie_document(
    document: OssieDocument,
    serialization: OssieSerialization | str,
    *,
    exact_source: bool = False,
    profile: OssieProfile | None = None,
    consumer_profile: OssieConsumerProfile | str | None = None,
) -> OssieSerializationResult:
    """Serialize a document without selecting or changing its Ossie profile.

    The document's existing family and version identify the pinned offline
    schema used for validation. ``serialization`` selects only YAML versus JSON;
    it never selects, infers, or rewrites a schema version. The dbt 1.12 0.1.0
    compatibility alias requires an explicit ``profile`` or
    ``consumer_profile`` context.
    """

    if not isinstance(document, (OssieLogicalDocument, OssieOntologyDocument, UnsupportedOssieDocument)):
        raise TypeError("document must be an OssieDocument")
    if not isinstance(exact_source, bool):
        raise TypeError("exact_source must be a boolean")

    output_serialization = OssieSerialization(serialization)
    if profile is not None and not isinstance(profile, OssieProfile):
        raise TypeError("profile must be an OssieProfile")
    canonical_data = document.to_parsed_data()
    diagnostics = list(
        _validate_canonical_data(
            document,
            canonical_data,
            profile=profile,
            consumer_profile=consumer_profile,
        )
    )

    source = document.source
    if (
        exact_source
        and output_serialization is document.serialization
        and source is not None
        and source.original_bytes is not None
    ):
        exact_source_consumer = profile.consumer_profile if profile is not None else consumer_profile
        if _exact_source_matches(document, source.original_bytes, exact_source_consumer):
            return OssieSerializationResult(
                data=source.original_bytes,
                serialization=output_serialization,
                exact_source_reused=True,
                diagnostics=tuple(diagnostics),
            )
        diagnostics.append(
            _diagnostic(
                document,
                code="ossie.serialization.exact_source_mismatch",
                message="Retained source bytes no longer match canonical data; canonical serialization was used",
                severity=OssieDiagnosticSeverity.WARNING,
            )
        )

    if output_serialization is OssieSerialization.JSON:
        data = _canonical_json(canonical_data)
    else:
        data = _canonical_yaml(document, canonical_data)

    return OssieSerializationResult(
        data=data,
        serialization=output_serialization,
        exact_source_reused=False,
        diagnostics=tuple(diagnostics),
    )
