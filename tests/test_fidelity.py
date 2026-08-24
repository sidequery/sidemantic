"""Unit tests for the import fidelity reporting module."""

import pytest

from sidemantic.fidelity import (
    FeatureDiagnostic,
    FidelityNote,
    ImportReport,
    capture_import_report,
    record_import_feature,
    record_import_note,
)


def test_record_note_is_noop_without_capture():
    """Recording outside a capture must not raise and must go nowhere."""
    record_import_note("x", "no active capture", severity="dropped")
    # Nothing to assert beyond "did not raise"; a fresh report stays empty.
    assert ImportReport().notes == []


def test_capture_collects_notes():
    with capture_import_report() as report:
        record_import_note("derived_table", "dropped a derived table", severity="dropped", source="a.lkml")
        record_import_note("unsupported_measure_type", "coerced to count", severity="approximated")

    assert report.has_losses
    assert len(report.notes) == 2
    assert isinstance(report.notes[0], FidelityNote)
    assert report.notes[0].construct == "derived_table"
    assert report.notes[0].source == "a.lkml"


def test_counts_per_severity():
    with capture_import_report() as report:
        record_import_note("a", "d", severity="dropped")
        record_import_note("b", "d", severity="dropped")
        record_import_note("c", "d", severity="approximated")

    assert report.counts() == {"dropped": 2, "approximated": 1}


def test_has_losses_false_when_empty():
    with capture_import_report() as report:
        pass
    assert report.has_losses is False
    assert report.counts() == {}
    assert report.summary_lines() == []


def test_summary_lines_grouped_and_stable():
    with capture_import_report() as report:
        record_import_note("approx_construct", "approx detail", severity="approximated", source="Cube")
        record_import_note("drop_a", "drop detail a", severity="dropped")
        record_import_note("drop_b", "drop detail b", severity="dropped", source="x.yml", location="12")

    lines = report.summary_lines()
    # "dropped" is rendered before "approximated" regardless of insertion order.
    assert lines[0] == "dropped (2):"
    assert lines[1] == "  drop_a: drop detail a"
    assert lines[2] == "  drop_b: drop detail b (x.yml:12)"
    assert lines[3] == "approximated (1):"
    assert lines[4] == "  approx_construct: approx detail (Cube)"
    # Deterministic across repeated calls.
    assert report.summary_lines() == lines


def test_to_dict_is_json_safe():
    import json

    with capture_import_report() as report:
        record_import_note("derived_table", "dropped", severity="dropped", source="a.yml", location="3")

    payload = report.to_dict()
    assert payload["has_losses"] is True
    assert payload["counts"] == {"dropped": 1}
    assert payload["notes"] == [
        {
            "construct": "derived_table",
            "detail": "dropped",
            "severity": "dropped",
            "source": "a.yml",
            "location": "3",
        }
    ]
    # Round-trips through JSON without error.
    assert json.loads(json.dumps(payload)) == payload


def test_nested_captures_deliver_to_all_active_reports():
    with capture_import_report() as outer:
        record_import_note("outer_only", "before inner", severity="dropped")
        with capture_import_report() as inner:
            record_import_note("both", "inside inner", severity="approximated")
        record_import_note("outer_again", "after inner", severity="dropped")

    # Inner sees only what was recorded while it was active.
    assert [n.construct for n in inner.notes] == ["both"]
    # Outer sees everything, including the note recorded during the inner capture.
    assert [n.construct for n in outer.notes] == ["outer_only", "both", "outer_again"]


def test_capture_resets_after_block():
    with capture_import_report() as report:
        record_import_note("a", "d")
    # After the block, recording is a no-op again.
    record_import_note("b", "d")
    assert len(report.notes) == 1


def test_feature_diagnostics_distinguish_all_compatibility_outcomes():
    with capture_import_report() as report:
        record_import_feature("dimension.label", "exact", detail="Preserved")
        record_import_feature("measure.percentile", "partial", detail="Approximate aggregate")
        record_import_feature("query.nesting", "unsupported")
        record_import_feature("source.filter", "rejected", detail="Cannot safely omit")

    assert report.feature_counts() == {
        "exact": 1,
        "partial": 1,
        "unsupported": 1,
        "rejected": 1,
    }
    assert all(isinstance(item, FeatureDiagnostic) for item in report.features)
    assert report.has_losses is True
    assert report.readiness == "blocked"
    assert report.is_ready is False
    assert report.is_blocked is True
    assert report.readiness_summary() == {
        "status": "blocked",
        "is_ready": False,
        "is_blocked": True,
        "requires_review": False,
        "blocking_features": ["query.nesting", "source.filter"],
    }


def test_partial_feature_requires_review_but_does_not_block():
    report = ImportReport()
    report.add_feature("dimension.expression", "exact")
    report.add_feature("measure.expression", "partial")

    assert report.readiness == "review_required"
    assert report.is_ready is False
    assert report.is_blocked is False


def test_exact_features_are_ready_and_not_losses():
    report = ImportReport()
    report.add_feature("dimension.label", "exact")

    assert report.has_losses is False
    assert report.readiness == "ready"
    assert report.is_ready is True


def test_legacy_notes_participate_in_conservative_readiness():
    review = ImportReport(notes=[FidelityNote("case", "approximated", "approximated")])
    blocked = ImportReport(notes=[FidelityNote("nest", "unsupported", "unsupported")])
    unknown = ImportReport(notes=[FidelityNote("future", "unknown status", "future_status")])

    assert review.readiness == "review_required"
    assert blocked.readiness == "blocked"
    assert unknown.readiness == "blocked"
    assert blocked.readiness_summary()["blocking_features"] == ["nest"]
    assert unknown.readiness_summary()["blocking_features"] == ["future"]


def test_feature_serialization_round_trip_and_old_payload_compatibility():
    import json

    report = ImportReport(notes=[FidelityNote("case", "dropped", "dropped")])
    report.add_feature("measure.percentile", "partial", detail="Approximate", source="model.malloy", location="4")

    payload = json.loads(json.dumps(report.to_dict()))
    restored = ImportReport.from_dict(payload)
    assert restored == report
    assert payload["schema_version"] == 1
    assert payload["readiness"]["status"] == "review_required"

    legacy = ImportReport.from_dict(
        {
            "has_losses": True,
            "counts": {"dropped": 1},
            "notes": [
                {
                    "construct": "case",
                    "detail": "dropped",
                    "severity": "dropped",
                    "source": None,
                    "location": None,
                }
            ],
        }
    )
    assert legacy.notes == report.notes
    assert legacy.features == []


def test_invalid_feature_status_fails_loudly_inside_capture():
    with capture_import_report():
        with pytest.raises(ValueError, match="Invalid import feature status"):
            record_import_feature("typo", "approximate")  # type: ignore[arg-type]


def test_feature_recording_is_noop_without_capture():
    record_import_feature("source.table", "exact")
    assert ImportReport().features == []


def test_feature_summary_and_combined_loss_counts_are_actionable():
    report = ImportReport(notes=[FidelityNote("legacy_case", "Dropped case", "dropped")])
    report.add_feature("measure.percentile", "partial", detail="Approximate", source="model.malloy", location="4")
    report.add_feature("query.nesting", "unsupported")
    report.add_feature("dimension.label", "exact")

    assert report.loss_counts() == {"dropped": 1, "partial": 1, "unsupported": 1}
    assert report.feature_summary_lines() == [
        "partial (1):",
        "  measure.percentile: Approximate (model.malloy:4)",
        "unsupported (1):",
        "  query.nesting",
    ]


@pytest.mark.parametrize("schema_version", [2, 0, "1", True])
def test_from_dict_rejects_unknown_or_invalid_schema_version(schema_version):
    with pytest.raises(ValueError, match="Unsupported import report schema_version"):
        ImportReport.from_dict({"schema_version": schema_version, "notes": [], "features": []})


def test_from_dict_accepts_absent_legacy_and_v1_schema_versions():
    assert ImportReport.from_dict({"notes": []}) == ImportReport()
    assert ImportReport.from_dict({"schema_version": 1, "notes": [], "features": []}) == ImportReport()
