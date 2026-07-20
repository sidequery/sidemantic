"""LookML import records fidelity notes for silently approximated constructs."""

import os
import tempfile

from sidemantic.adapters.lookml import LookMLAdapter
from sidemantic.fidelity import capture_import_report


def _parse(lkml_str: str):
    d = tempfile.mkdtemp()
    path = os.path.join(d, "orders.view.lkml")
    with open(path, "w") as fh:
        fh.write(lkml_str)
    return LookMLAdapter().parse(path)


def test_note_for_unsupported_measure_type():
    lkml = """
view: orders {
  sql_table_name: public.orders ;;
  dimension: id {
    primary_key: yes
    sql: ${TABLE}.id ;;
  }
  measure: weird_measure {
    type: bogus_type
    sql: ${TABLE}.amount ;;
  }
}
"""
    with capture_import_report() as report:
        _parse(lkml)

    notes = [n for n in report.notes if n.construct == "unsupported_measure_type"]
    assert len(notes) == 1
    assert notes[0].severity == "approximated"
    assert "orders.weird_measure" in notes[0].detail
    assert "bogus_type" in notes[0].detail


def test_no_note_for_known_measure_type():
    lkml = """
view: orders {
  sql_table_name: public.orders ;;
  dimension: id {
    primary_key: yes
    sql: ${TABLE}.id ;;
  }
  measure: total {
    type: sum
    sql: ${TABLE}.amount ;;
  }
}
"""
    with capture_import_report() as report:
        _parse(lkml)

    assert [n for n in report.notes if n.construct == "unsupported_measure_type"] == []
