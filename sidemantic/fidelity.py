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

# Rendered/serialized in this order; any unknown severity is appended after these.
_SEVERITY_ORDER: tuple[str, ...] = ("dropped", "approximated", "unsupported")


@dataclass
class FidelityNote:
    """One construct that was dropped or approximated during import."""

    construct: str  # short slug, e.g. "derived_table", "duplicate_model"
    detail: str  # human-readable explanation of what was dropped/approximated
    severity: str  # one of: "dropped", "approximated", "unsupported"
    source: str | None = None  # originating file path or format name when known
    location: str | None = None  # "file:line" when known


@dataclass
class ImportReport:
    """Collected fidelity notes from one import."""

    notes: list[FidelityNote] = field(default_factory=list)

    @property
    def has_losses(self) -> bool:
        return bool(self.notes)

    def counts(self) -> dict[str, int]:
        """Number of notes per severity."""
        result: dict[str, int] = {}
        for note in self.notes:
            result[note.severity] = result.get(note.severity, 0) + 1
        return result

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

    def to_dict(self) -> dict:
        """JSON-safe representation of the report."""
        return {
            "has_losses": self.has_losses,
            "counts": self.counts(),
            "notes": [asdict(note) for note in self.notes],
        }


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
