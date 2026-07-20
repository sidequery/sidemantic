"""Unit tests for the import fidelity reporting module."""

from sidemantic.fidelity import (
    FidelityNote,
    ImportReport,
    capture_import_report,
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
