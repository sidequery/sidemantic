"""Execution regressions for materialization and Python rollup routing."""

from datetime import date

import duckdb
import pytest
from typer.testing import CliRunner

from sidemantic import Dimension, Metric, Model, SemanticLayer
from sidemantic.cli import app
from sidemantic.core.pre_aggregation import PreAggregation, RefreshKey
from sidemantic.core.preagg_matcher import PreAggregationMatcher


def rollup_layer(aggregation="sum", grain="day"):
    layer = SemanticLayer(auto_register=False, engine="python")
    layer.conn.execute("CREATE TABLE orders (dt TIMESTAMP, region VARCHAR, amount DOUBLE)")
    layer.conn.execute(
        "INSERT INTO orders VALUES "
        "('2026-10-01 10:00', 'A', 1), ('2026-10-01 15:00', 'A', 3), "
        "('2026-10-02 10:00', 'B', 10), ('2026-10-02 15:00', 'B', 20), "
        "('2026-10-02 20:00', 'B', 30)"
    )
    preagg = PreAggregation(
        name="daily", measures=["value"], dimensions=["region"], time_dimension="dt", granularity=grain
    )
    model = Model(
        name="orders",
        table="orders",
        dimensions=[Dimension(name="dt", type="time", granularity=grain), Dimension(name="region", type="categorical")],
        metrics=[Metric(name="value", agg=aggregation, sql="amount")],
        pre_aggregations=[preagg],
    )
    layer.add_model(model)
    layer.conn.execute(f"CREATE TABLE orders_preagg_daily AS {preagg.generate_materialization_sql(model)}")
    return layer, model, preagg


def query_with_routing(layer, *, dimensions=None, filters=None):
    kwargs = dict(metrics=["orders.value"], dimensions=dimensions or [], filters=filters or [])
    layer.use_preaggregations = False
    expected = layer.query(**kwargs).fetchall()
    layer.use_preaggregations = True
    sql = layer.compile(**kwargs)
    actual = layer.query(**kwargs).fetchall()
    assert sorted(actual) == sorted(expected)
    return sql, actual


@pytest.mark.parametrize("aggregation", ["median", "stddev", "stddev_pop", "variance", "variance_pop"])
def test_finalized_statistics_require_exact_grain(aggregation):
    layer, model, preagg = rollup_layer(aggregation)
    sql, _ = query_with_routing(layer)
    assert "orders_preagg_daily" not in sql
    sql, _ = query_with_routing(layer, dimensions=["orders.dt__month", "orders.region"])
    assert "orders_preagg_daily" not in sql
    sql, _ = query_with_routing(layer, dimensions=["orders.dt", "orders.region"])
    assert "orders_preagg_daily" in sql
    matcher = PreAggregationMatcher(model)
    assert not matcher.can_satisfy_query(preagg, ["value"], [])
    assert not matcher.explain_query(preagg, ["value"], []).matched
    assert matcher.explain_query(preagg, ["value"], ["dt", "region"], "day").matched
    assert matcher.explain_query(preagg, ["value"], ["dt", "region"]).matched


@pytest.mark.parametrize("reference", ["orders.dt", "orders.dt__day"])
def test_default_grain_reads_bucket_and_preserves_alias(reference):
    layer, _, _ = rollup_layer()
    sql, rows = query_with_routing(layer, dimensions=[reference])
    assert "orders_preagg_daily" in sql
    assert sorted(rows) == [(date(2026, 10, 1), 4), (date(2026, 10, 2), 60)]
    layer.use_preaggregations = False
    expected_names = [column[0] for column in layer.query(metrics=["orders.value"], dimensions=[reference]).description]
    layer.use_preaggregations = True
    assert [
        column[0] for column in layer.query(metrics=["orders.value"], dimensions=[reference]).description
    ] == expected_names


@pytest.mark.parametrize(
    "predicate, safe",
    [
        ("orders.dt >= TIMESTAMP '2026-10-01 12:00:00'", False),
        ("orders.dt < TIMESTAMP '2026-10-01 12:00:00'", False),
        ("orders.dt > TIMESTAMP '2026-10-01 00:00:00'", False),
        ("orders.dt <= TIMESTAMP '2026-10-01 00:00:00'", False),
        ("orders.dt = TIMESTAMP '2026-10-01 00:00:00'", False),
        ("orders.dt BETWEEN '2026-10-01' AND '2026-10-02'", False),
        ("orders.dt >= TIMESTAMP '2026-10-02 00:00:00'", True),
        ("orders.dt < DATE '2026-10-02'", True),
        ("TIMESTAMP '2026-10-02 00:00:00' <= orders.dt", True),
        ("TIMESTAMP '2026-10-02 00:00:00' > orders.dt", True),
        ("orders.dt IS NOT NULL", True),
        ("orders.dt >= '2026-10-01' AND orders.dt < '2026-10-03'", True),
        ("orders.dt >= '2026-10-01' AND orders.dt < '2026-10-02 12:00:00'", False),
    ],
)
def test_time_filters_only_route_complete_buckets(predicate, safe):
    layer, model, preagg = rollup_layer()
    sql, _ = query_with_routing(layer, filters=[predicate])
    assert ("orders_preagg_daily" in sql) is safe
    matcher = PreAggregationMatcher(model)
    assert matcher.explain_query(preagg, ["value"], [], filters=[predicate]).matched is safe


@pytest.mark.parametrize(
    "grain,boundary,safe",
    [
        ("month", "2026-10-01", True),
        ("month", "2026-10-02", False),
        ("week", "2026-10-05", True),
        ("week", "2026-10-01", False),
        ("quarter", "2026-10-01", True),
        ("quarter", "2026-11-01", False),
        ("hour", "2026-10-01 10:00:00", True),
        ("hour", "2026-10-01 10:30:00", False),
    ],
)
def test_temporal_boundaries_match_materialized_grain(grain, boundary, safe):
    layer, _, _ = rollup_layer(grain=grain)
    sql, _ = query_with_routing(layer, filters=[f"orders.dt >= TIMESTAMP '{boundary}'"])
    assert ("orders_preagg_daily" in sql) is safe


@pytest.mark.parametrize("model_sql", [False, True])
@pytest.mark.parametrize("partitioned", [False, True])
def test_materialization_resolves_placeholders_in_all_inputs(model_sql, partitioned):
    conn = duckdb.connect()
    conn.execute("CREATE TABLE source (dt TIMESTAMP, label VARCHAR, amount INT, active BOOLEAN)")
    conn.execute("INSERT INTO source VALUES ('2026-10-01', '{model}', 10, true), ('2026-10-01', 'other', 99, false)")
    model = Model(
        name="orders",
        table=None if model_sql else "source",
        sql="SELECT * FROM source" if model_sql else None,
        invariant_filters=["{model}.active"],
        dimensions=[
            Dimension(name="dt", type="time", sql="{model}.dt"),
            Dimension(name="label", type="categorical", sql="{model}.label"),
        ],
        metrics=[Metric(name="value", agg="sum", sql="{model}.amount", filters=["{model}.label = '{model}'"])],
    )
    preagg = PreAggregation(
        name="daily",
        measures=["value"],
        dimensions=["label"],
        time_dimension="dt",
        granularity="day",
        partition_granularity="day" if partitioned else None,
    )
    sql = preagg.generate_materialization_sql(model)
    assert "= '{model}'" in sql
    preagg.refresh(conn, sql, "orders_preagg_daily", mode="full", model=model)
    assert conn.execute("SELECT label, value_raw FROM orders_preagg_daily").fetchall() == [("{model}", 10)]


@pytest.mark.parametrize("lookback", [None, "7 days"])
def test_incremental_partitions_remove_disappeared_recent_buckets(lookback):
    conn = duckdb.connect()
    conn.execute(
        "CREATE TABLE orders AS SELECT CURRENT_DATE AS dt, 10 AS amount UNION ALL SELECT CURRENT_DATE - 30, 100"
    )
    model = Model(
        name="orders",
        table="orders",
        dimensions=[Dimension(name="dt", type="time")],
        metrics=[Metric(name="value", agg="sum", sql="amount")],
    )
    preagg = PreAggregation(
        name="daily",
        measures=["value"],
        time_dimension="dt",
        granularity="day",
        partition_granularity="day",
        refresh_key=RefreshKey(update_window="7 days"),
    )
    preagg.build_partitions(conn, model, full_rebuild=True)
    conn.execute("DELETE FROM orders")
    assert preagg.build_partitions(conn, model, lookback=lookback) == []
    assert conn.execute("SELECT value_raw FROM orders_preagg_daily").fetchall() == [(100,)]
    assert (
        conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE 'orders_preagg_daily_p%'"
        ).fetchone()[0]
        == 1
    )


@pytest.mark.parametrize(
    "mode, window",
    [
        ("auto", None),
        ("incremental", None),
        ("merge", None),
        ("auto", "1 day"),
        ("incremental", "12 hours"),
        ("merge", "1 day"),
    ],
)
def test_cli_refresh_bounds_materialized_buckets(tmp_path, mode, window):
    db = tmp_path / "data.duckdb"
    with duckdb.connect(str(db)) as conn:
        conn.execute("CREATE TABLE orders (dt TIMESTAMP, amount INT)")
        conn.execute("INSERT INTO orders VALUES ('2026-10-01 10:00', 10), ('2026-10-02 15:00', 20)")
    models = tmp_path / "models"
    models.mkdir()
    (models / "orders.yml").write_text(
        """models:
- name: orders
  table: orders
  dimensions:
  - {name: dt, type: time, granularity: day}
  metrics:
  - {name: value, agg: sum, sql: amount}
  pre_aggregations:
  - name: daily
    measures: [value]
    time_dimension: dt
    granularity: day
    refresh_key:
      incremental: true
"""
        + (f"      update_window: {window}\n" if window else "")
    )
    args = ["preagg", "refresh", str(models), "--db", str(db), "--mode", mode, "--json"]
    for _ in range(3):
        result = CliRunner().invoke(app, args)
        assert result.exit_code == 0, result.stdout + result.stderr
        with duckdb.connect(str(db)) as conn:
            assert conn.execute("SELECT COUNT(*), SUM(value_raw) FROM orders_preagg_daily").fetchone() == (2, 30)
    with duckdb.connect(str(db)) as conn:
        conn.execute("INSERT INTO orders VALUES ('2026-10-03 12:00', 30)")
        # Replacement modes also pick up changes in the current bucket.
        if window or mode == "merge":
            conn.execute("INSERT INTO orders VALUES ('2026-10-02 20:00', 5)")
    result = CliRunner().invoke(app, args)
    assert result.exit_code == 0, result.stdout + result.stderr
    with duckdb.connect(str(db)) as conn:
        expected = 65 if window or mode == "merge" else 60
        assert conn.execute("SELECT COUNT(*), SUM(value_raw) FROM orders_preagg_daily").fetchone() == (3, expected)


def test_default_grain_can_roll_up_daily_buckets_to_month():
    layer, model, _ = rollup_layer()
    model.get_dimension("dt").granularity = "month"
    sql, rows = query_with_routing(layer, dimensions=["orders.dt"])
    assert "orders_preagg_daily" in sql
    assert rows == [(date(2026, 10, 1), 64)]


def test_bucket_filter_rewrite_preserves_literals():
    layer, _, _ = rollup_layer()
    # A categorical value equal to the temporal field name must not be rewritten.
    layer.conn.execute("UPDATE orders SET region = 'dt'")
    layer.conn.execute("UPDATE orders_preagg_daily SET region = 'dt'")
    sql, rows = query_with_routing(layer, filters=["orders.region = 'dt'", "orders.dt >= '2026-10-01'"])
    assert "orders_preagg_daily" in sql
    assert rows == [(64,)]


def test_incremental_partition_refresh_removes_empty_covering_view():
    conn = duckdb.connect()
    conn.execute("CREATE TABLE orders AS SELECT CURRENT_DATE AS dt, 10 AS amount")
    model = Model(
        name="orders",
        table="orders",
        dimensions=[Dimension(name="dt", type="time")],
        metrics=[Metric(name="value", agg="sum", sql="amount")],
    )
    preagg = PreAggregation(
        name="daily", measures=["value"], time_dimension="dt", granularity="day", partition_granularity="day"
    )
    preagg.build_partitions(conn, model)
    conn.execute("DELETE FROM orders")
    preagg.build_partitions(conn, model, lookback="7 days")
    assert (
        conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE 'orders_preagg_daily%'"
        ).fetchone()[0]
        == 0
    )


def test_incremental_partition_refresh_preserves_buckets_outside_build_range():
    conn = duckdb.connect()
    conn.execute(
        "CREATE TABLE orders AS SELECT DATE '2026-10-01' AS dt, 10 AS amount UNION ALL SELECT DATE '2026-10-02', 20 UNION ALL SELECT DATE '2026-10-03', 30"
    )
    model = Model(
        name="orders",
        table="orders",
        dimensions=[Dimension(name="dt", type="time")],
        metrics=[Metric(name="value", agg="sum", sql="amount")],
    )
    preagg = PreAggregation(
        name="daily", measures=["value"], time_dimension="dt", granularity="day", partition_granularity="day"
    )
    preagg.build_partitions(conn, model)
    conn.execute("DELETE FROM orders")
    preagg.build_partitions(conn, model, build_range_start="2026-10-02", build_range_end="2026-10-03")
    assert conn.execute("SELECT value_raw FROM orders_preagg_daily ORDER BY 1").fetchall() == [(10,), (30,)]


@pytest.mark.parametrize("mode", ["incremental", "merge"])
def test_cli_rejects_incremental_rollup_without_watermark(tmp_path, mode):
    db = tmp_path / "data.duckdb"
    with duckdb.connect(str(db)) as conn:
        conn.execute("CREATE TABLE orders AS SELECT 10 AS amount")
    models = tmp_path / "models"
    models.mkdir()
    (models / "orders.yml").write_text("""models:
- name: orders
  table: orders
  metrics:
  - {name: value, agg: sum, sql: amount}
  pre_aggregations:
  - {name: total, measures: [value]}
""")
    result = CliRunner().invoke(app, ["preagg", "refresh", str(models), "--db", str(db), "--mode", mode, "--json"])
    assert result.exit_code != 0
    assert "requires a rollup with time_dimension and granularity" in result.stdout + result.stderr
    with duckdb.connect(str(db)) as conn:
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'orders_preagg_total'"
            ).fetchone()[0]
            == 0
        )


def test_time_dimension_without_default_grain_falls_back_to_source():
    layer, model, preagg = rollup_layer()
    model.get_dimension("dt").granularity = None
    sql, rows = query_with_routing(layer, dimensions=["orders.dt"])
    assert "orders_preagg_daily" not in sql
    assert len(rows) == 5
    assert not PreAggregationMatcher(model).explain_query(preagg, ["value"], ["dt"]).matched


@pytest.mark.parametrize("mode", ["auto", "incremental", "merge"])
def test_cli_fractional_update_window_replaces_whole_overlapping_bucket(tmp_path, mode):
    db = tmp_path / "data.duckdb"
    with duckdb.connect(str(db)) as conn:
        conn.execute("CREATE TABLE orders (dt TIMESTAMP, amount INT)")
        conn.execute("INSERT INTO orders VALUES ('2026-10-01 18:00', 10), ('2026-10-02 15:00', 20)")
    models = tmp_path / "models"
    models.mkdir()
    (models / "orders.yml").write_text("""models:
- name: orders
  table: orders
  dimensions:
  - {name: dt, type: time, granularity: day}
  metrics:
  - {name: value, agg: sum, sql: amount}
  pre_aggregations:
  - name: daily
    measures: [value]
    time_dimension: dt
    granularity: day
    refresh_key:
      incremental: true
      update_window: 12 hours
""")
    args = ["preagg", "refresh", str(models), "--db", str(db), "--mode", mode, "--json"]
    result = CliRunner().invoke(app, args)
    assert result.exit_code == 0, result.stdout + result.stderr
    with duckdb.connect(str(db)) as conn:
        conn.execute("INSERT INTO orders VALUES ('2026-10-01 19:00', 5)")
    for _ in range(2):
        result = CliRunner().invoke(app, args)
        assert result.exit_code == 0, result.stdout + result.stderr
        with duckdb.connect(str(db)) as conn:
            assert conn.execute("SELECT value_raw FROM orders_preagg_daily ORDER BY dt_day").fetchall() == [
                (15,),
                (20,),
            ]
    with duckdb.connect(str(db)) as conn:
        conn.execute("DELETE FROM orders WHERE dt < '2026-10-02'")
    result = CliRunner().invoke(app, args)
    assert result.exit_code == 0, result.stdout + result.stderr
    with duckdb.connect(str(db)) as conn:
        assert conn.execute("SELECT value_raw FROM orders_preagg_daily").fetchall() == [(20,)]


@pytest.mark.parametrize(
    "row_time,start,end",
    [
        ("2026-10-01 09:00", "2026-10-01 12:00", "2026-10-02"),
        ("2026-10-01 18:00", "2026-10-01", "2026-10-01 12:00"),
    ],
)
def test_partial_partition_ranges_refresh_whole_overlapping_buckets(row_time, start, end):
    conn = duckdb.connect()
    conn.execute(f"CREATE TABLE orders AS SELECT TIMESTAMP '{row_time}' dt, 10 amount")
    model = Model(
        name="orders",
        table="orders",
        dimensions=[Dimension(name="dt", type="time")],
        metrics=[Metric(name="value", agg="sum", sql="amount")],
    )
    preagg = PreAggregation(
        name="daily", measures=["value"], time_dimension="dt", granularity="day", partition_granularity="day"
    )
    preagg.build_partitions(conn, model)
    # The surviving source row is outside the requested subrange, but belongs to
    # the bucket being replaced. Both discovery and materialization must see it.
    conn.execute("UPDATE orders SET amount = 15")
    preagg.build_partitions(conn, model, build_range_start=start, build_range_end=end)
    assert conn.execute("SELECT value_raw FROM orders_preagg_daily").fetchall() == [(15,)]


@pytest.mark.parametrize("timezone", ["UTC", "America/New_York"])
@pytest.mark.parametrize("legacy_name", [False, True])
def test_deleted_timezone_partitions_are_removed(timezone, legacy_name):
    import re

    conn = duckdb.connect()
    conn.execute("SET TimeZone = ?", [timezone])
    conn.execute("CREATE TABLE orders AS SELECT TIMESTAMPTZ '2026-10-01 09:00:00+00' dt, 10 amount")
    model = Model(
        name="orders",
        table="orders",
        dimensions=[Dimension(name="dt", type="time")],
        metrics=[Metric(name="value", agg="sum", sql="amount")],
    )
    preagg = PreAggregation(
        name="daily", measures=["value"], time_dimension="dt", granularity="day", partition_granularity="day"
    )
    [partition] = preagg.build_partitions(conn, model)
    assert re.fullmatch(r"orders_preagg_daily_p\d{14}", partition)
    if legacy_name:
        # Previously the suffix accidentally included offset digits from str().
        [bucket] = conn.execute("SELECT dt_day FROM orders_preagg_daily").fetchone()
        legacy = "orders_preagg_daily_p" + re.sub(r"[^0-9]", "", str(bucket))
        conn.execute("DROP VIEW orders_preagg_daily")
        conn.execute(f"ALTER TABLE {partition} RENAME TO {legacy}")
        conn.execute(f"CREATE VIEW orders_preagg_daily AS SELECT * FROM {legacy}")
    conn.execute("DELETE FROM orders")
    preagg.build_partitions(conn, model)
    assert (
        conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'orders_preagg_daily%'"
        ).fetchall()
        == []
    )


@pytest.mark.parametrize("timezone", ["UTC", "America/New_York"])
def test_timezone_partition_lookback_preserves_only_older_history(timezone):
    conn = duckdb.connect()
    conn.execute("SET TimeZone = ?", [timezone])
    conn.execute(
        "CREATE TABLE orders AS SELECT CURRENT_TIMESTAMP dt, 10 amount "
        "UNION ALL SELECT CURRENT_TIMESTAMP - INTERVAL '30 days', 100"
    )
    model = Model(
        name="orders",
        table="orders",
        dimensions=[Dimension(name="dt", type="time")],
        metrics=[Metric(name="value", agg="sum", sql="amount")],
    )
    preagg = PreAggregation(
        name="daily", measures=["value"], time_dimension="dt", granularity="day", partition_granularity="day"
    )
    preagg.build_partitions(conn, model)
    # Rebuilding populated timezone-aware buckets must not compare an aware
    # Python datetime against the naive lookback cutoff.
    preagg.build_partitions(conn, model, lookback="7 days")
    conn.execute("DELETE FROM orders")
    preagg.build_partitions(conn, model, lookback="7 days")
    assert conn.execute("SELECT value_raw FROM orders_preagg_daily").fetchall() == [(100,)]


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("other_grain", [None, "day", "month"])
@pytest.mark.parametrize("requested_grain", ["day", "month"])
def test_rollup_grain_is_bound_to_its_own_time_dimension(reverse, other_grain, requested_grain):
    layer = SemanticLayer(auto_register=False, engine="python")
    layer.conn.execute("CREATE TABLE orders (dt TIMESTAMP, shipped_at TIMESTAMP, amount INT)")
    layer.conn.execute("INSERT INTO orders VALUES ('2026-10-01', '2026-11-01', 10), ('2026-10-02', '2026-11-01', 20)")
    preagg = PreAggregation(
        name="monthly", measures=["value"], dimensions=["shipped_at"], time_dimension="dt", granularity="month"
    )
    model = Model(
        name="orders",
        table="orders",
        dimensions=[
            Dimension(name="dt", type="time", granularity="month"),
            Dimension(name="shipped_at", type="time", granularity="day"),
        ],
        metrics=[Metric(name="value", agg="sum", sql="amount")],
        pre_aggregations=[preagg],
    )
    layer.add_model(model)
    model.get_dimension("shipped_at").granularity = None
    layer.conn.execute(f"CREATE TABLE orders_preagg_monthly AS {preagg.generate_materialization_sql(model)}")
    dimensions = [f"orders.dt__{requested_grain}", "orders.shipped_at" + (f"__{other_grain}" if other_grain else "")]
    if reverse:
        dimensions.reverse()
    sql, rows = query_with_routing(layer, dimensions=dimensions)
    matches = requested_grain == "month"
    assert len(rows) == (1 if matches else 2)
    assert ("orders_preagg_monthly" in sql) is matches
    plan = layer.explain(metrics=["orders.value"], dimensions=dimensions)
    assert plan.used_preaggregation is matches
    assert any(candidate.matched for candidate in plan.candidates) is matches


@pytest.mark.parametrize("mode", ["incremental", "merge"])
@pytest.mark.parametrize("initial_target", ["missing", "empty", "null_only"])
@pytest.mark.parametrize("window", [None, "1 day"])
def test_cli_bootstraps_incremental_rollups_without_epoch_cutoff(tmp_path, mode, initial_target, window):
    db = tmp_path / "bootstrap.duckdb"
    with duckdb.connect(str(db)) as conn:
        conn.execute("CREATE TABLE orders (dt TIMESTAMP, amount INT)")
        if initial_target == "null_only":
            conn.execute("INSERT INTO orders VALUES (NULL, 3)")
        else:
            conn.execute("INSERT INTO orders VALUES (NULL, 3), ('1960-01-01', 7), ('1970-01-01', 11)")
        if initial_target != "missing":
            conn.execute("CREATE TABLE orders_preagg_daily (dt_day DATE, value_raw HUGEINT)")
            if initial_target == "null_only":
                conn.execute("INSERT INTO orders_preagg_daily VALUES (NULL, 999)")
    models = tmp_path / "models"
    models.mkdir()
    (models / "orders.yml").write_text(
        """models:
- name: orders
  table: orders
  dimensions:
  - {name: dt, type: time, granularity: day}
  metrics:
  - {name: value, agg: sum, sql: amount}
  pre_aggregations:
  - name: daily
    measures: [value]
    time_dimension: dt
    granularity: day
    refresh_key:
      incremental: true
"""
        + (f"      update_window: {window}\n" if window else "")
    )
    args = ["preagg", "refresh", str(models), "--db", str(db), "--mode", mode, "--json"]
    for _ in range(2):
        result = CliRunner().invoke(app, args)
        assert result.exit_code == 0, result.stdout + result.stderr
        with duckdb.connect(str(db)) as conn:
            actual = conn.execute("SELECT * FROM orders_preagg_daily ORDER BY dt_day NULLS FIRST").fetchall()
            expected = conn.execute(
                "SELECT DATE_TRUNC('day', dt), SUM(amount) FROM orders GROUP BY 1 ORDER BY 1 NULLS FIRST"
            ).fetchall()
            assert actual == expected


@pytest.mark.parametrize("coarse_reference", ["orders.dt", "orders.dt__month"])
@pytest.mark.parametrize("reverse", [False, True])
def test_rollup_must_support_every_requested_grain_of_same_dimension(coarse_reference, reverse):
    layer, _, _ = rollup_layer(grain="month")
    dimensions = ["orders.dt__day", coarse_reference]
    if reverse:
        dimensions.reverse()
    sql, rows = query_with_routing(layer, dimensions=dimensions)
    assert len(rows) == 2
    assert "orders_preagg_daily" not in sql
    plan = layer.explain(metrics=["orders.value"], dimensions=dimensions)
    assert not plan.used_preaggregation
    assert not any(candidate.matched for candidate in plan.candidates)


@pytest.mark.parametrize("default_grain", [False, True])
def test_finalized_rollup_cannot_regroup_secondary_time_dimension(default_grain):
    layer = SemanticLayer(auto_register=False, engine="python")
    layer.conn.execute("CREATE TABLE orders (dt TIMESTAMP, shipped_at TIMESTAMP, amount INT)")
    layer.conn.execute("INSERT INTO orders VALUES ('2026-10-01', '2026-11-01', 1), ('2026-10-01', '2026-11-02', 2)")
    preagg = PreAggregation(
        name="daily", measures=["value"], dimensions=["shipped_at"], time_dimension="dt", granularity="day"
    )
    model = Model(
        name="orders",
        table="orders",
        dimensions=[
            Dimension(name="dt", type="time", granularity="day"),
            Dimension(name="shipped_at", type="time", granularity="month"),
        ],
        metrics=[Metric(name="value", agg="count_distinct", sql="amount")],
        pre_aggregations=[preagg],
    )
    layer.add_model(model)
    if not default_grain:
        model.get_dimension("shipped_at").granularity = None
    layer.conn.execute(f"CREATE TABLE orders_preagg_daily AS {preagg.generate_materialization_sql(model)}")
    dimensions = ["orders.dt", "orders.shipped_at" + ("" if default_grain else "__month")]
    sql, rows = query_with_routing(layer, dimensions=dimensions)
    assert rows == [(date(2026, 10, 1), date(2026, 11, 1), 2)]
    assert "orders_preagg_daily" not in sql
    plan = layer.explain(metrics=["orders.value"], dimensions=dimensions)
    assert not plan.used_preaggregation
    assert not any(candidate.matched for candidate in plan.candidates)
