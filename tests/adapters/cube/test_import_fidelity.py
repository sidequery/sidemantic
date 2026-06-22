"""Cube import emits fidelity warnings for constructs it preserves but does not execute."""

import os
import tempfile

import pytest

from sidemantic.adapters.cube import CubeAdapter, CubeImportWarning


def _parse(yaml_str: str):
    d = tempfile.mkdtemp()
    path = os.path.join(d, "model.yml")
    with open(path, "w") as fh:
        fh.write(yaml_str)
    return CubeAdapter().parse(path)


def test_warns_on_inert_preaggregation_controls():
    cube_yaml = """
cubes:
  - name: orders
    sql_table: orders
    measures:
      - name: c
        type: count
    dimensions:
      - name: id
        type: number
        sql: id
        primary_key: true
      - name: created
        type: time
        sql: created_at
    pre_aggregations:
      - name: main
        measures:
          - c
        time_dimension: created
        granularity: day
        partition_granularity: month
        refresh_key:
          sql: SELECT MAX(created_at) FROM orders
        indexes:
          - name: idx
            columns:
              - created
"""
    with pytest.warns(CubeImportWarning, match="partition_granularity"):
        _parse(cube_yaml)


def test_warns_on_measure_case_and_preserves_it():
    cube_yaml = """
cubes:
  - name: orders
    sql_table: orders
    measures:
      - name: amt
        type: number
        case:
          switch: "{currency}"
          when:
            - value: EUR
              sql: "{amount_eur}"
          else:
            sql: "{amount_usd}"
    dimensions:
      - name: id
        type: number
        sql: id
        primary_key: true
"""
    with pytest.warns(CubeImportWarning, match="case/switch"):
        graph = _parse(cube_yaml)
    metric = graph.get_model("orders").get_metric("amt")
    # Previously the entire case block was dropped (empty metric). Now preserved on meta.
    assert metric.meta and "case" in metric.meta


def test_warns_on_subquery_dimension():
    cube_yaml = """
cubes:
  - name: users
    sql_table: users
    measures:
      - name: c
        type: count
    dimensions:
      - name: id
        type: number
        sql: id
        primary_key: true
      - name: total_orders
        type: number
        sub_query: true
        sql: "{orders.count}"
"""
    with pytest.warns(CubeImportWarning, match="sub_query"):
        _parse(cube_yaml)
