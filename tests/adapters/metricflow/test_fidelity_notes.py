"""MetricFlow import records fidelity notes for silently dropped constructs."""

import os
import tempfile

from sidemantic.adapters.metricflow import MetricFlowAdapter
from sidemantic.fidelity import capture_import_report


def _parse(yaml_str: str):
    d = tempfile.mkdtemp()
    path = os.path.join(d, "model.yml")
    with open(path, "w") as fh:
        fh.write(yaml_str)
    return MetricFlowAdapter().parse(path)


def test_note_for_unrepresentable_aggregation():
    mf_yaml = """
semantic_models:
  - name: orders
    model: ref('orders')
    entities:
      - name: order_id
        type: primary
    measures:
      - name: pct
        agg: percentile
      - name: good
        agg: sum
        expr: amount
"""
    with capture_import_report() as report:
        _parse(mf_yaml)

    notes = [n for n in report.notes if n.construct == "unsupported_aggregation"]
    assert len(notes) == 1
    assert notes[0].severity == "dropped"
    assert "pct" in notes[0].detail
    assert "percentile" in notes[0].detail


def test_note_for_unsupported_metric_type():
    mf_yaml = """
semantic_models:
  - name: orders
    model: ref('orders')
    entities:
      - name: order_id
        type: primary
    measures:
      - name: good
        agg: sum
        expr: amount
metrics:
  - name: weird
    type: totally_unknown_type
    label: Weird
"""
    with capture_import_report() as report:
        _parse(mf_yaml)

    notes = [n for n in report.notes if n.construct == "unsupported_metric_type"]
    assert len(notes) == 1
    assert notes[0].severity == "dropped"
    assert "weird" in notes[0].detail
    assert "totally_unknown_type" in notes[0].detail
