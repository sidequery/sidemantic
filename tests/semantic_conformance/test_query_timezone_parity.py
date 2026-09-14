"""Both compilers bucket UTC timestamps in the requested local timezone."""

import pytest

from sidemantic import Dimension, Metric, Model, SemanticLayer
from sidemantic.core.pre_aggregation import PreAggregation


@pytest.fixture(params=["python", "rust"])
def layer(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs", reason="Requires the real Rust extension")
    layer = SemanticLayer(engine=request.param, fallback=False, auto_register=False)
    layer.add_model(
        Model(
            name="events",
            table="timezone_events",
            primary_key="id",
            dimensions=[
                Dimension(name="ts", type="time", granularity="day"),
                Dimension(name="computed_ts", type="time", sql="ts + INTERVAL '1 hour'", granularity="day"),
            ],
            metrics=[Metric(name="total", agg="sum", sql="amount")],
        )
    )
    layer.adapter.execute("""
        create table timezone_events(id integer, ts timestamp, amount integer);
        insert into timezone_events values
            (1, '2024-01-02 02:00:00', 5),
            (2, '2024-01-02 06:00:00', 7),
            (3, '2024-03-10 06:30:00', 11),
            (4, '2024-03-10 07:30:00', 13),
            (5, '2024-11-03 05:30:00', 17),
            (6, '2024-11-03 06:30:00', 19);
    """)
    try:
        yield layer
    finally:
        layer.adapter.close()


@pytest.mark.parametrize("grain", ["hour", "day", "month"])
@pytest.mark.parametrize("dimension", ["ts", "computed_ts"])
def test_local_time_buckets_and_dst(layer, grain, dimension):
    sql = layer.compile(
        metrics=["events.total"], dimensions=[f"events.{dimension}__{grain}"], timezone="America/New_York"
    )
    expression = "ts" if dimension == "ts" else "ts + INTERVAL '1 hour'"
    expected = layer.adapter.execute(f"""
        select date_trunc('{grain}', ({expression}) at time zone 'UTC' at time zone 'America/New_York'),
               sum(amount)
        from timezone_events group by 1 order by 1
    """).fetchall()
    assert sorted(layer.adapter.execute(sql).fetchall()) == expected
    if layer.engine == "rust":
        assert layer.last_engine_selection["engine"] == "rust"


@pytest.mark.parametrize(
    "dialect, localization",
    [
        ("duckdb", "AT TIME ZONE"),
        ("postgres", "AT TIME ZONE"),
        ("snowflake", "CONVERT_TIMEZONE"),
        ("bigquery", "DATETIME"),
        ("spark", "FROM_UTC_TIMESTAMP"),
        ("databricks", "FROM_UTC_TIMESTAMP"),
        ("clickhouse", "TOTIMEZONE"),
    ],
)
def test_supported_dialect_localization(layer, dialect, localization):
    sql = layer.compile(
        metrics=["events.total"], dimensions=["events.ts__day"], timezone="America/New_York", dialect=dialect
    )
    assert localization in sql.upper()
    assert "'America/New_York'" in sql
    if dialect in {"duckdb", "postgres", "snowflake"}:
        assert "'UTC'" in sql


@pytest.mark.parametrize("timezone", ["UTC'; --", "America/New York", "UTC\\", "UTC\n"])
def test_timezone_rejects_invalid_characters(layer, timezone):
    with pytest.raises(ValueError, match="Invalid timezone"):
        layer.compile(metrics=["events.total"], dimensions=["events.ts__day"], timezone=timezone)


@pytest.mark.parametrize("timezone", [None, ""])
def test_absent_timezone_keeps_utc_buckets(layer, timezone):
    sql = layer.compile(metrics=["events.total"], dimensions=["events.ts__day"], timezone=timezone)
    assert "AT TIME ZONE" not in sql.upper()
    assert (
        sorted(layer.adapter.execute(sql).fetchall())
        == layer.adapter.execute("""
        select date_trunc('day', ts), sum(amount)
        from timezone_events group by 1 order by 1
    """).fetchall()
    )


def test_unsupported_dialect_is_explicit(layer):
    with pytest.raises(ValueError, match="timezone is not supported"):
        layer.compile(metrics=["events.total"], dimensions=["events.ts__day"], timezone="UTC", dialect="mysql")


def test_timezone_bypasses_utc_rollup(layer):
    layer.graph.get_model("events").pre_aggregations.append(
        PreAggregation(name="daily", measures=["total"], time_dimension="ts", granularity="day")
    )
    sql = layer.compile(
        metrics=["events.total"],
        dimensions=["events.ts__day"],
        use_preaggregations=True,
        timezone="America/New_York",
    )
    assert "preagg" not in sql.lower()
    assert str(sorted(layer.adapter.execute(sql).fetchall())[0][0]).startswith("2024-01-01")
