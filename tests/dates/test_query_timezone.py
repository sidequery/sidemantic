"""Opt-in query-level timezone bucketing for time dimensions."""

import duckdb
import pytest

from sidemantic import Dimension, Metric, Model, SemanticLayer
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.sql.generator import SQLGenerator


def _layer():
    conn = duckdb.connect(":memory:")
    conn.execute("create table ev as select 1 as id, TIMESTAMP '2024-01-02 02:00:00' as ts, 5 as amt")
    layer = SemanticLayer()
    layer.conn = conn
    layer.add_model(
        Model(
            name="ev",
            table="ev",
            primary_key="id",
            dimensions=[Dimension(name="ts", type="time", sql="ts", granularity="day")],
            metrics=[Metric(name="total", agg="sum", sql="amt")],
        )
    )
    return layer


def test_no_timezone_is_unchanged():
    """Without a timezone, generation is identical to before (no tz conversion)."""
    layer = _layer()
    sql = layer.compile(metrics=["ev.total"], dimensions=["ev.ts__day"])
    assert "AT TIME ZONE" not in sql


def test_timezone_shifts_day_boundary():
    """2024-01-02 02:00 UTC buckets to Jan 2 in UTC but Jan 1 in America/New_York (UTC-5)."""
    layer = _layer()
    sql = layer.compile(metrics=["ev.total"], dimensions=["ev.ts__day"], timezone="America/New_York")
    assert "AT TIME ZONE" in sql

    utc = layer.query(metrics=["ev.total"], dimensions=["ev.ts__day"]).fetchall()
    ny = layer.query(metrics=["ev.total"], dimensions=["ev.ts__day"], timezone="America/New_York").fetchall()
    assert str(utc[0][0]).startswith("2024-01-02")
    assert str(ny[0][0]).startswith("2024-01-01")


def test_timezone_dialect_specific_sql():
    """Each supported dialect gets its own localization idiom; unsupported dialects raise."""
    g = SemanticGraph()
    assert "CONVERT_TIMEZONE('UTC', 'America/New_York'" in (
        SQLGenerator(g, dialect="snowflake", timezone="America/New_York")._date_trunc("day", "ts")
    )
    assert "DATETIME(ts, 'America/New_York')" in (
        SQLGenerator(g, dialect="bigquery", timezone="America/New_York")._date_trunc("day", "ts")
    )
    assert "from_utc_timestamp(ts, 'America/New_York')" in (
        SQLGenerator(g, dialect="spark", timezone="America/New_York")._date_trunc("day", "ts")
    )
    with pytest.raises(ValueError, match="not supported"):
        SQLGenerator(g, dialect="mysql", timezone="UTC")._date_trunc("day", "ts")
