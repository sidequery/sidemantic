"""Structured diagnostics for Apache Ossie parsing, validation, and lowering."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum

from sidemantic.interchange.ossie.profiles import OssieProfile


class OssieDiagnosticSeverity(str, Enum):
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass(frozen=True, slots=True)
class OssieSourceLocation:
    """Source identity and optional one-based text coordinates."""

    identifier: str
    line: int | None = None
    column: int | None = None
    end_line: int | None = None
    end_column: int | None = None

    def __post_init__(self) -> None:
        if not self.identifier:
            raise ValueError("source identifier must not be empty")
        for field_name in ("line", "column", "end_line", "end_column"):
            value = getattr(self, field_name)
            if value is not None and value < 1:
                raise ValueError(f"{field_name} must be one-based")
        if self.column is not None and self.line is None:
            raise ValueError("column requires line")
        if self.end_column is not None and self.end_line is None:
            raise ValueError("end_column requires end_line")


@dataclass(frozen=True, slots=True)
class OssieSchemaProvenance:
    """Identity of the pinned schema used to produce a diagnostic."""

    schema_id: str
    version: str | None = None
    commit: str | None = None
    sha256: str | None = None

    def __post_init__(self) -> None:
        if not self.schema_id:
            raise ValueError("schema_id must not be empty")


@dataclass(frozen=True, slots=True)
class OssieDiagnostic:
    """One stable, source-addressable Ossie diagnostic."""

    severity: OssieDiagnosticSeverity
    code: str
    message: str
    json_pointer: str = ""
    source: OssieSourceLocation | None = None
    scope: str | None = None
    profile: OssieProfile | None = None
    schema: OssieSchemaProvenance | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "severity", OssieDiagnosticSeverity(self.severity))
        if not self.code or any(character.isspace() for character in self.code):
            raise ValueError("diagnostic code must be non-empty and contain no whitespace")
        if not self.message:
            raise ValueError("diagnostic message must not be empty")
        if self.json_pointer and not self.json_pointer.startswith("/"):
            raise ValueError("json_pointer must be empty or start with '/'")


_SEVERITY_ORDER = {
    OssieDiagnosticSeverity.ERROR: 0,
    OssieDiagnosticSeverity.WARNING: 1,
    OssieDiagnosticSeverity.INFO: 2,
}


def diagnostic_sort_key(diagnostic: OssieDiagnostic) -> tuple[object, ...]:
    """Return a stable ordering by source position, object path, severity, and identity."""

    source = diagnostic.source
    schema = diagnostic.schema
    return (
        source.identifier if source else "",
        source.line if source and source.line is not None else 0,
        source.column if source and source.column is not None else 0,
        source.end_line if source and source.end_line is not None else 0,
        source.end_column if source and source.end_column is not None else 0,
        diagnostic.json_pointer,
        _SEVERITY_ORDER[diagnostic.severity],
        diagnostic.code,
        diagnostic.message,
        diagnostic.scope or "",
        diagnostic.profile.identifier if diagnostic.profile else "",
        schema.schema_id if schema else "",
        schema.version or "" if schema else "",
        schema.commit or "" if schema else "",
        schema.sha256 or "" if schema else "",
    )


def sort_diagnostics(diagnostics: Iterable[OssieDiagnostic]) -> tuple[OssieDiagnostic, ...]:
    """Materialize diagnostics in deterministic presentation order."""

    return tuple(sorted(diagnostics, key=diagnostic_sort_key))
