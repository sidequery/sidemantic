"""Tests for the first-class approx_count_distinct aggregation.

Regression coverage for the bug where Cube imports silently downgraded
`count_distinct_approx` to exact `count_distinct`, and where the migrator emitted
agg names (`approx_distinct`/`approx_quantile`) that no Metric Literal accepted.
"""

import tempfile
from pathlib import Path

import duckdb

from sidemantic import Dimension, Metric, Model, SemanticLayer
from sidemantic.adapters.cube import CubeAdapter
from sidemantic.core.pre_aggregation import PreAggregation
from sidemantic.core.preagg_matcher import PreAggregationMatcher
from tests.utils import fetch_rows


def test_approx_count_distinct_generates_and_executes(layer):
    """approx_count_distinct compiles to APPROX_COUNT_DISTINCT and runs on DuckDB."""
    conn = duckdb.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE events (
            cat VARCHAR,
            user_id INTEGER
        )
        """
    )
    conn.execute(
        """
        INSERT INTO events VALUES
            ('a', 1), ('a', 1), ('a', 2),
            ('b', 3), ('b', 4), ('b', 4)
        """
    )
    layer.conn = conn

    events = Model(
        name="events",
        table="events",
        primary_key="user_id",
        dimensions=[Dimension(name="cat", type="categorical")],
        metrics=[Metric(name="uusers", agg="approx_count_distinct", sql="user_id")],
    )
    layer.add_model(events)

    sql = layer.compile(metrics=["events.uusers"], dimensions=["events.cat"])
    assert "APPROX_COUNT_DISTINCT" in sql
    assert "COUNT(DISTINCT" not in sql

    rows = fetch_rows(layer.query(metrics=["events.uusers"], dimensions=["events.cat"], order_by=["events.cat"]))
    assert rows == [("a", 2), ("b", 2)]


def test_cube_count_distinct_approx_preserves_approximate_semantics(tmp_path):
    """Importing a cube `count_distinct_approx` measure yields agg=approx_count_distinct."""
    cube_yaml = tmp_path / "cube.yml"
    cube_yaml.write_text(
        """
cubes:
  - name: events
    sql_table: events
    measures:
      - name: uusers
        type: count_distinct_approx
        sql: user_id
      - name: exact_users
        type: count_distinct
        sql: user_id
    dimensions:
      - name: id
        sql: user_id
        type: number
        primary_key: true
"""
    )

    graph = CubeAdapter().parse(cube_yaml)
    model = graph.models["events"]

    approx = model.get_metric("uusers")
    assert approx.agg == "approx_count_distinct"

    exact = model.get_metric("exact_users")
    assert exact.agg == "count_distinct"

    # Regression guard: approximate must NOT be downgraded to exact.
    assert approx.agg != exact.agg


def test_approx_count_distinct_not_rollup_derivable():
    """approx_count_distinct is not derivable from rolled-up pre-aggregated data."""
    model = Model(
        name="events",
        table="events",
        primary_key="user_id",
        dimensions=[Dimension(name="cat", type="categorical")],
        metrics=[Metric(name="uusers", agg="approx_count_distinct", sql="user_id")],
    )

    matcher = PreAggregationMatcher(model)
    metric = model.get_metric("uusers")
    preagg = PreAggregation(name="rollup", measures=["uusers"], dimensions=["cat"])

    assert matcher._is_measure_derivable(metric, preagg) is False


def test_approx_count_distinct_cube_round_trip():
    """approx_count_distinct survives a Cube export -> re-import round-trip."""
    adapter = CubeAdapter()
    layer = SemanticLayer(auto_register=False)
    layer.add_model(
        Model(
            name="events",
            table="events",
            primary_key="user_id",
            dimensions=[Dimension(name="cat", type="categorical")],
            metrics=[Metric(name="uusers", agg="approx_count_distinct", sql="user_id")],
        )
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        temp_path = Path(f.name)

    try:
        adapter.export(layer.graph, temp_path)
        exported_yaml = temp_path.read_text()
        assert "count_distinct_approx" in exported_yaml

        graph = adapter.parse(temp_path)
        assert graph.models["events"].get_metric("uusers").agg == "approx_count_distinct"
    finally:
        temp_path.unlink(missing_ok=True)


def test_migrator_approx_agg_names_are_valid_metric_aggs():
    """Migrator approx agg names produce valid Metric aggregations, not crashes."""
    # The migrator maps approxdistinct -> approx_count_distinct and
    # approxquantile -> median. Both must be constructible Metric aggs.
    assert Metric(name="m1", agg="approx_count_distinct", sql="user_id").agg == "approx_count_distinct"
    assert Metric(name="m2", agg="median", sql="amount").agg == "median"
