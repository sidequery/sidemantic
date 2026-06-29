"""Tests for the approx_count_distinct aggregation and its Cube import mapping."""

import os
import tempfile

import duckdb

from sidemantic import Dimension, Metric, Model, SemanticLayer
from sidemantic.adapters.cube import CubeAdapter


def _layer_with_events():
    conn = duckdb.connect(":memory:")
    conn.execute("create table events as select * from (values (1,'a'),(2,'a'),(2,'b'),(3,'b')) t(user_id, cat)")
    layer = SemanticLayer()
    layer.conn = conn
    layer.add_model(
        Model(
            name="events",
            table="events",
            primary_key="user_id",
            dimensions=[Dimension(name="cat", type="categorical", sql="cat")],
            metrics=[Metric(name="uusers", agg="approx_count_distinct", sql="user_id")],
        )
    )
    return layer


def test_approx_count_distinct_generates_and_executes():
    layer = _layer_with_events()
    sql = layer.compile(metrics=["events.uusers"], dimensions=["events.cat"])
    assert "APPROX_COUNT_DISTINCT" in sql.upper()
    assert "COUNT(DISTINCT" not in sql.upper()
    rows = sorted(layer.query(metrics=["events.uusers"], dimensions=["events.cat"]).fetchall())
    # cat 'a' -> users {1, 2}; cat 'b' -> users {2, 3}
    assert rows == [("a", 2), ("b", 2)]


def test_cube_count_distinct_approx_preserves_approximate_semantics():
    cube_yaml = """
cubes:
  - name: orders
    sql_table: orders
    measures:
      - name: uu
        type: count_distinct_approx
        sql: user_id
    dimensions:
      - name: id
        type: number
        sql: id
        primary_key: true
"""
    d = tempfile.mkdtemp()
    path = os.path.join(d, "orders.yml")
    with open(path, "w") as fh:
        fh.write(cube_yaml)
    graph = CubeAdapter().parse(path)
    metric = graph.get_model("orders").get_metric("uu")
    # Previously this collapsed to an exact count_distinct, losing the approximate intent.
    assert metric.agg == "approx_count_distinct"


def test_approx_count_distinct_not_rollup_derivable():
    """Plain-table materialization stores an integer, so approx distinct cannot be re-aggregated."""
    from sidemantic.core.pre_aggregation import PreAggregation
    from sidemantic.core.preagg_matcher import PreAggregationMatcher

    model = Model(
        name="events",
        table="events",
        primary_key="user_id",
        metrics=[Metric(name="uusers", agg="approx_count_distinct", sql="user_id")],
    )
    matcher = PreAggregationMatcher(model)
    metric = model.get_metric("uusers")
    preagg = PreAggregation(name="rollup", measures=["uusers"], dimensions=[])
    assert matcher._is_measure_derivable(metric, preagg) is False


def test_approx_count_distinct_cube_round_trip(tmp_path):
    """approx_count_distinct survives a full Cube export -> re-import round-trip."""
    from sidemantic.core.semantic_graph import SemanticGraph

    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            metrics=[Metric(name="uu", agg="approx_count_distinct", sql="user_id")],
        )
    )
    out = tmp_path / "out.yml"
    CubeAdapter().export(graph, str(out))
    # Export must emit Cube's count_distinct_approx, not silently degrade to a plain count.
    assert "count_distinct_approx" in out.read_text()
    reimported = CubeAdapter().parse(str(out))
    assert reimported.get_model("orders").get_metric("uu").agg == "approx_count_distinct"


def test_migrator_approx_agg_names_are_valid_metric_aggs():
    """Migrator approx agg names produce valid Metric aggregations, not crashes."""
    # The migrator maps approxdistinct -> approx_count_distinct and
    # approxquantile -> median. Both must be constructible Metric aggs.
    assert Metric(name="m1", agg="approx_count_distinct", sql="user_id").agg == "approx_count_distinct"
    assert Metric(name="m2", agg="median", sql="amount").agg == "median"
