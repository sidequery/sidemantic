"""Cube import records fidelity notes for silently dropped/approximated constructs."""

import os
import tempfile

from sidemantic.adapters.cube import CubeAdapter
from sidemantic.fidelity import capture_import_report


def _parse(yaml_str: str):
    d = tempfile.mkdtemp()
    path = os.path.join(d, "model.yml")
    with open(path, "w") as fh:
        fh.write(yaml_str)
    return CubeAdapter().parse(path)


def test_note_for_unsupported_measure_type():
    cube_yaml = """
cubes:
  - name: orders
    sql_table: public.orders
    measures:
      - name: mystery
        type: totally_unknown_agg
        sql: "{CUBE}.amount"
"""
    with capture_import_report() as report:
        _parse(cube_yaml)

    notes = [n for n in report.notes if n.construct == "unsupported_measure_type"]
    assert len(notes) == 1
    assert notes[0].severity == "approximated"
    assert "orders.mystery" in notes[0].detail
    assert "totally_unknown_agg" in notes[0].detail


def test_note_for_unsupported_dimension_type():
    cube_yaml = """
cubes:
  - name: orders
    sql_table: public.orders
    dimensions:
      - name: region
        type: geo_polygon
        sql: "{CUBE}.region"
"""
    with capture_import_report() as report:
        _parse(cube_yaml)

    notes = [n for n in report.notes if n.construct == "unsupported_dimension_type"]
    assert len(notes) == 1
    assert notes[0].severity == "approximated"
    assert "orders.region" in notes[0].detail


def test_no_note_for_known_types():
    cube_yaml = """
cubes:
  - name: orders
    sql_table: public.orders
    dimensions:
      - name: status
        type: string
        sql: "{CUBE}.status"
    measures:
      - name: total
        type: sum
        sql: "{CUBE}.amount"
"""
    with capture_import_report() as report:
        _parse(cube_yaml)

    assert report.notes == []
