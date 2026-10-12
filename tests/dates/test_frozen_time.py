"""Current-time functions are frozen to compile-time literals for partition pruning."""

from datetime import UTC, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import duckdb
import pytest

from sidemantic import Dimension, Metric, Model, SemanticLayer
from sidemantic.sql.frozen_time import freeze_current_time

NOW = datetime(2026, 10, 11, 18, 30, 15, 123456, tzinfo=UTC)
PACIFIC = timezone(timedelta(hours=-7))
NOW_TZ = "CAST('2026-10-11 18:30:15.123456+00:00' AS TIMESTAMPTZ)"


def freeze(sql: str) -> str:
    return freeze_current_time(sql, "duckdb", now=NOW, timezone=PACIFIC)


def test_current_date_interval_arithmetic_folds_to_timestamp_literal():
    sql = "SELECT 1 FROM events WHERE timestamp >= CURRENT_DATE - INTERVAL '30' DAY"

    assert freeze(sql) == "SELECT 1 FROM events WHERE timestamp >= CAST('2026-09-11 00:00:00' AS TIMESTAMP)"


def test_current_date_integer_arithmetic_folds_to_date_literal():
    assert freeze("SELECT 1 WHERE d >= current_date - 7 AND d <= today()") == (
        "SELECT 1 WHERE d >= CAST('2026-10-04' AS DATE) AND d <= CAST('2026-10-11' AS DATE)"
    )


def test_current_date_uses_session_timezone():
    # 02:00 UTC on Oct 12 is still Oct 11 in UTC-7.
    late = datetime(2026, 10, 12, 2, 0, tzinfo=UTC)

    assert freeze_current_time("SELECT CURRENT_DATE", "duckdb", now=late, timezone=PACIFIC) == (
        "SELECT CAST('2026-10-11' AS DATE)"
    )
    assert freeze_current_time("SELECT CURRENT_DATE", "duckdb", now=late, timezone=UTC) == (
        "SELECT CAST('2026-10-12' AS DATE)"
    )


def test_now_becomes_timestamptz_literal_and_folds_exact_intervals():
    assert freeze("SELECT 1 WHERE ts >= now() - INTERVAL 24 HOUR AND x = 1") == (
        "SELECT 1 WHERE ts >= CAST('2026-10-10 18:30:15.123456+00:00' AS TIMESTAMPTZ) AND x = 1"
    )
    # Day arithmetic on TIMESTAMPTZ follows session-timezone calendar days, so it stays in SQL.
    assert freeze("SELECT 1 WHERE ts >= now() - interval 30 day") == (
        f"SELECT 1 WHERE ts >= {NOW_TZ} - interval 30 day"
    )


def test_current_timestamp_becomes_timestamptz_literal():
    assert freeze("SELECT 1 WHERE ts < current_timestamp") == f"SELECT 1 WHERE ts < {NOW_TZ}"
    assert freeze("SELECT 1 WHERE ts < CURRENT_TIMESTAMP()") == f"SELECT 1 WHERE ts < {NOW_TZ}"


def test_one_instant_is_used_for_every_call():
    frozen = freeze("SELECT now() AS a, CURRENT_TIMESTAMP AS b, get_current_timestamp() AS c")

    assert frozen == f"SELECT {NOW_TZ} AS a, {NOW_TZ} AS b, {NOW_TZ} AS c"


def test_sql_without_time_functions_is_unchanged():
    sql = "SELECT status FROM orders WHERE created_at >= '2024-01-01' AND status = 'completed'"

    assert freeze(sql) == sql


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT t.current_date, 'now()' FROM t -- CURRENT_DATE",
        'SELECT "now" FROM t WHERE x = 1',
        "SELECT CURRENT_TIMESTAMP(3)",
        "CREATE VIEW v AS SELECT * FROM t WHERE d >= CURRENT_DATE",
        "SELECT 1; SELECT now()",
    ],
)
def test_non_calls_non_queries_and_arguments_are_left_alone(sql):
    assert freeze(sql) == sql


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        # (x - CURRENT_DATE) - 1: folding CURRENT_DATE - 1 would regroup the expression.
        (
            "SELECT 1 WHERE x - CURRENT_DATE - 1 > 0",
            "SELECT 1 WHERE x - CAST('2026-10-11' AS DATE) - 1 > 0",
        ),
        # CURRENT_DATE - (INTERVAL 1 DAY * 2): only the leading integer term may fold.
        (
            "SELECT 1 WHERE ts > CURRENT_DATE - 1 - INTERVAL 1 DAY * 2",
            "SELECT 1 WHERE ts > CAST('2026-10-10' AS DATE) - INTERVAL 1 DAY * 2",
        ),
        # Calendar units are not folded; the engine folds the constant expression.
        (
            "SELECT 1 WHERE d >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'",
            "SELECT 1 WHERE d >= DATE_TRUNC('month', CAST('2026-10-11' AS DATE)) - INTERVAL '1 month'",
        ),
    ],
)
def test_folding_respects_precedence_and_units(sql, expected):
    assert freeze(sql) == expected


@pytest.mark.parametrize(
    "expression",
    [
        "current_date - interval '30' day",
        "current_date - interval '30 days' - interval 2 hour",
        "current_date + 3",
        "today()",
        "now() - interval 24 hours",
        "now() - interval 30 day",
        "date_trunc('month', current_date) - interval '1 month'",
    ],
)
def test_frozen_sql_matches_duckdb_value_and_type(expression):
    zone = "America/Los_Angeles"
    con = duckdb.connect()
    con.execute(f"SET TimeZone = '{zone}'")
    con.execute("BEGIN")
    # DuckDB fixes now()/current_date at transaction start; freeze at that same instant.
    transaction_start = con.execute("SELECT get_current_timestamp()").fetchone()[0]
    frozen = freeze_current_time(f"SELECT {expression}", "duckdb", now=transaction_start, timezone=ZoneInfo(zone))
    frozen_expression = frozen.removeprefix("SELECT ")

    original = con.execute(f"SELECT {expression}, typeof({expression})").fetchone()
    replaced = con.execute(f"SELECT {frozen_expression}, typeof({frozen_expression})").fetchone()

    assert "current_date" not in frozen.lower() and "now()" not in frozen.lower()
    assert replaced == original


def _events_layer(**kwargs) -> SemanticLayer:
    layer = SemanticLayer(auto_register=False, **kwargs)
    layer.clock = lambda: NOW
    layer.add_model(
        Model(
            name="events",
            table="events",
            primary_key="event_id",
            dimensions=[Dimension(name="timestamp", type="time", granularity="day")],
            metrics=[Metric(name="event_count", agg="count")],
        )
    )
    return layer


def _local_date_literal(days_ago: int) -> str:
    return f"CAST('{(NOW.astimezone().date() - timedelta(days=days_ago)).isoformat()} 00:00:00' AS TIMESTAMP)"


def test_compile_freezes_pushed_down_filters_on_duckdb():
    layer = _events_layer()

    sql = layer.compile(
        metrics=["events.event_count"],
        filters=["events.timestamp >= current_date - interval 30 day", "events.timestamp < current_timestamp"],
    )

    assert f"timestamp >= {_local_date_literal(30)}" in sql
    assert f"timestamp < {NOW_TZ}" in sql
    assert "CURRENT_DATE" not in sql.upper()
    assert "-- sidemantic:" in sql


def test_compile_freezes_sidemantic_relative_date_range():
    layer = _events_layer()

    sql = layer.compile(metrics=["events.event_count"], filters=["events.timestamp = 'yesterday'"])

    yesterday = (NOW.astimezone().date() - timedelta(days=1)).isoformat()
    assert f"timestamp = CAST('{yesterday}' AS DATE)" in sql


def test_freezing_can_be_disabled_and_is_off_for_other_dialects():
    filters = ["events.timestamp >= current_date - interval 30 day"]

    disabled = _events_layer(freeze_current_time=False).compile(metrics=["events.event_count"], filters=filters)
    postgres = _events_layer().compile(metrics=["events.event_count"], filters=filters, dialect="postgres")
    forced = _events_layer(freeze_current_time=True).compile(
        metrics=["events.event_count"], filters=filters, dialect="postgres"
    )

    assert "CURRENT_DATE - INTERVAL '30' DAY" in disabled
    assert "CURRENT_DATE" in postgres.upper()
    assert "CURRENT_DATE" not in forced.upper()


def test_semantic_sql_freezes_each_execution_despite_rewrite_cache():
    layer = _events_layer()
    layer.adapter.execute(
        "CREATE TABLE events AS SELECT i AS event_id, TIMESTAMP '2026-01-01' + to_days(i) AS timestamp FROM range(60) t(i)"
    )
    query = "select events.event_count from events where events.timestamp >= current_date - interval 30 day"

    layer.clock = lambda: datetime(2026, 2, 15, 12, tzinfo=UTC)
    first = layer.sql(query).fetchone()[0]
    layer.clock = lambda: datetime(2026, 3, 1, 12, tzinfo=UTC)
    second = layer.sql(query).fetchone()[0]

    # Cutoffs 2026-01-16 and 2026-01-30 over events dated 2026-01-01 .. 2026-03-01.
    assert (first, second) == (45, 31)


def test_rust_engine_output_is_frozen():
    pytest.importorskip("sidemantic_rs", reason="Requires the built Rust extension")
    layer = _events_layer(engine="rust", fallback=False)

    sql = layer.compile(
        metrics=["events.event_count"],
        filters=["events.timestamp >= current_date - interval 30 day"],
    )

    assert layer.last_engine_selection == {"engine": "rust", "reason": None}
    assert _local_date_literal(30) in sql
    assert "CURRENT_DATE" not in sql.upper()
