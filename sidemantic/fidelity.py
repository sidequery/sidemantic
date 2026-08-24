"""Import fidelity reporting.

Foreign-format importers (Cube, LookML, MetricFlow, ...) silently drop or
approximate constructs sidemantic cannot represent. This module lets those drop
sites record a structured note that a caller can surface, without changing what
actually gets imported.

Recording is opt-in: :func:`record_import_note` is a no-op unless a
:func:`capture_import_report` block is active, so instrumented drop sites carry
zero cost on the normal path. Captures nest -- a note recorded while several
captures are active is delivered to every one of them.

Only stdlib is imported here so the module stays importable in Pyodide/WASM.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

# Rendered/serialized in this order; any unknown severity is appended after these.
_SEVERITY_ORDER: tuple[str, ...] = ("dropped", "approximated", "unsupported")
_FEATURE_STATUS_ORDER: tuple[str, ...] = ("exact", "partial", "unsupported", "rejected")
_LEGACY_REVIEW_SEVERITIES: frozenset[str] = frozenset({"dropped", "approximated"})

FeatureStatus = Literal["exact", "partial", "unsupported", "rejected"]
ReadinessStatus = Literal["ready", "review_required", "blocked"]


@dataclass
class FidelityNote:
    """One construct that was dropped or approximated during import."""

    construct: str  # short slug, e.g. "derived_table", "duplicate_model"
    detail: str  # human-readable explanation of what was dropped/approximated
    severity: str  # one of: "dropped", "approximated", "unsupported"
    source: str | None = None  # originating file path or format name when known
    location: str | None = None  # "file:line" when known


@dataclass(frozen=True)
class FeatureDiagnostic:
    """Import outcome for one foreign-format feature.

    ``rejected`` is reserved for constructs that would be unsafe to approximate
    or ignore. ``unsupported`` also blocks readiness, but does not imply a
    security or correctness hazard in the source construct itself.
    """

    feature: str
    status: FeatureStatus
    detail: str | None = None
    source: str | None = None
    location: str | None = None

    def __post_init__(self) -> None:
        if self.status not in _FEATURE_STATUS_ORDER:
            allowed = ", ".join(_FEATURE_STATUS_ORDER)
            raise ValueError(f"Invalid import feature status {self.status!r}; expected one of: {allowed}")


@dataclass
class ImportReport:
    """Collected fidelity notes from one import."""

    notes: list[FidelityNote] = field(default_factory=list)
    features: list[FeatureDiagnostic] = field(default_factory=list)

    @property
    def has_losses(self) -> bool:
        return bool(self.notes) or any(feature.status != "exact" for feature in self.features)

    @property
    def readiness(self) -> ReadinessStatus:
        """Conservative readiness classification for the imported result.

        Unsupported and explicitly rejected features block use. Partial mappings
        and legacy loss notes require review. Only an exact/no-loss report is
        immediately ready.
        """
        if any(feature.status in {"unsupported", "rejected"} for feature in self.features):
            return "blocked"
        if any(note.severity not in _LEGACY_REVIEW_SEVERITIES for note in self.notes):
            return "blocked"
        if self.has_losses:
            return "review_required"
        return "ready"

    @property
    def is_ready(self) -> bool:
        """Whether the import can be used without review or an override."""
        return self.readiness == "ready"

    @property
    def is_blocked(self) -> bool:
        """Whether unsupported or rejected features require fail-closed handling."""
        return self.readiness == "blocked"

    def counts(self) -> dict[str, int]:
        """Number of notes per severity."""
        result: dict[str, int] = {}
        for note in self.notes:
            result[note.severity] = result.get(note.severity, 0) + 1
        return result

    def feature_counts(self) -> dict[str, int]:
        """Number of feature diagnostics per status in stable status order."""
        observed = {status: 0 for status in _FEATURE_STATUS_ORDER}
        for feature in self.features:
            observed[feature.status] += 1
        return {status: observed[status] for status in _FEATURE_STATUS_ORDER if observed[status]}

    def loss_counts(self) -> dict[str, int]:
        """Combined non-exact note/feature counts for user-facing summaries."""
        result = self.counts()
        for feature in self.features:
            if feature.status != "exact":
                result[feature.status] = result.get(feature.status, 0) + 1
        return result

    def add_feature(
        self,
        feature: str,
        status: FeatureStatus,
        *,
        detail: str | None = None,
        source: str | None = None,
        location: str | None = None,
    ) -> FeatureDiagnostic:
        """Append and return a validated feature diagnostic."""
        diagnostic = FeatureDiagnostic(
            feature=feature,
            status=status,
            detail=detail,
            source=source,
            location=location,
        )
        self.features.append(diagnostic)
        return diagnostic

    def readiness_summary(self) -> dict[str, Any]:
        """JSON-safe fail-closed summary for CLI and adapter integrations."""
        return {
            "status": self.readiness,
            "is_ready": self.is_ready,
            "is_blocked": self.is_blocked,
            "requires_review": self.readiness == "review_required",
            "blocking_features": [
                *(note.construct for note in self.notes if note.severity not in _LEGACY_REVIEW_SEVERITIES),
                *(feature.feature for feature in self.features if feature.status in {"unsupported", "rejected"}),
            ],
        }

    def _severity_rank(self, severity: str) -> tuple[int, str]:
        try:
            return (_SEVERITY_ORDER.index(severity), "")
        except ValueError:
            return (len(_SEVERITY_ORDER), severity)

    def summary_lines(self) -> list[str]:
        """Human-renderable lines, grouped by severity in a stable order.

        Notes keep their insertion order within a severity group, so repeated
        runs over the same import produce identical output.
        """
        severities = sorted({note.severity for note in self.notes}, key=self._severity_rank)
        lines: list[str] = []
        for severity in severities:
            group = [note for note in self.notes if note.severity == severity]
            lines.append(f"{severity} ({len(group)}):")
            for note in group:
                location = f" ({note.source}:{note.location})" if note.source and note.location else ""
                if not location and note.source:
                    location = f" ({note.source})"
                elif not location and note.location:
                    location = f" ({note.location})"
                lines.append(f"  {note.construct}: {note.detail}{location}")
        return lines

    def feature_summary_lines(self, *, include_exact: bool = False) -> list[str]:
        """Human-renderable feature diagnostics grouped in stable status order."""
        statuses = [
            status
            for status in _FEATURE_STATUS_ORDER
            if (include_exact or status != "exact") and any(feature.status == status for feature in self.features)
        ]
        lines: list[str] = []
        for status in statuses:
            group = [feature for feature in self.features if feature.status == status]
            lines.append(f"{status} ({len(group)}):")
            for feature in group:
                location = _format_location(feature.source, feature.location)
                detail = f": {feature.detail}" if feature.detail else ""
                lines.append(f"  {feature.feature}{detail}{location}")
        return lines

    def to_dict(self) -> dict:
        """JSON-safe representation of the report."""
        return {
            "schema_version": 1,
            "has_losses": self.has_losses,
            "counts": self.counts(),
            "feature_counts": self.feature_counts(),
            "readiness": self.readiness_summary(),
            "notes": [asdict(note) for note in self.notes],
            "features": [asdict(feature) for feature in self.features],
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> ImportReport:
        """Restore a report serialized by :meth:`to_dict`.

        Derived summary fields are intentionally ignored. Payloads produced by
        the original notes-only serializer remain valid.
        """
        schema_version = payload.get("schema_version")
        if schema_version is not None and (
            isinstance(schema_version, bool) or not isinstance(schema_version, int) or schema_version != 1
        ):
            raise ValueError(f"Unsupported import report schema_version: {schema_version!r}")
        notes = [FidelityNote(**note) for note in payload.get("notes", [])]
        features = [FeatureDiagnostic(**feature) for feature in payload.get("features", [])]
        return cls(notes=notes, features=features)


def _format_location(source: str | None, location: str | None) -> str:
    if source and location:
        return f" ({source}:{location})"
    if source:
        return f" ({source})"
    if location:
        return f" ({location})"
    return ""


# Stack of active reports. A tuple (immutable) so concurrent contexts/tasks each
# see their own snapshot; entering a capture pushes, leaving pops.
_active_reports: ContextVar[tuple[ImportReport, ...]] = ContextVar("_active_import_reports", default=())


@contextmanager
def capture_import_report() -> Iterator[ImportReport]:
    """Collect fidelity notes recorded while the block is active.

    Nesting is supported: a note recorded inside nested captures lands in every
    active report, so an outer capture still sees notes from an inner one.
    """
    report = ImportReport()
    token = _active_reports.set(_active_reports.get() + (report,))
    try:
        yield report
    finally:
        _active_reports.reset(token)


def record_import_note(
    construct: str,
    detail: str,
    *,
    severity: str = "dropped",
    source: str | None = None,
    location: str | None = None,
) -> None:
    """Record a note about a dropped/approximated construct.

    No-op when no :func:`capture_import_report` is active. Never raises -- drop
    sites call this in the middle of parsing and must not be destabilized by it.
    """
    try:
        reports = _active_reports.get()
        if not reports:
            return
        note = FidelityNote(
            construct=construct,
            detail=detail,
            severity=severity,
            source=source,
            location=location,
        )
        for report in reports:
            report.notes.append(note)
    except Exception:
        return


def record_import_feature(
    feature: str,
    status: FeatureStatus,
    *,
    detail: str | None = None,
    source: str | None = None,
    location: str | None = None,
) -> None:
    """Record one feature's import outcome in every active report.

    This is the adapter integration API for explicit compatibility accounting.
    It is a no-op outside a capture. Invalid statuses raise ``ValueError`` so a
    misspelled status cannot silently weaken fail-closed reporting.
    """
    reports = _active_reports.get()
    if not reports:
        return
    diagnostic = FeatureDiagnostic(
        feature=feature,
        status=status,
        detail=detail,
        source=source,
        location=location,
    )
    for report in reports:
        report.features.append(diagnostic)
