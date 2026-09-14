"""Target-dialect temporal fallback contracts; execution uses transpiled DuckDB SQL."""

import pytest
import sqlglot
from sqlglot import exp

from sidemantic import Dimension, Metric, Model, SemanticLayer


@pytest.fixture(params=["python", "rust"])
def layer(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs", reason="Temporal dialect parity requires the Rust extension")
    layer = SemanticLayer(engine=request.param, fallback=False, auto_register=False)
    layer.adapter.execute("""
        create table sales(id integer, day date, amount integer);
        insert into sales values
          (1, '2024-01-01', 2), (2, '2024-04-01', 6), (3, '2024-10-01', 20);
    """)
    layer.add_model(
        Model(
            name="sales",
            table="sales",
            primary_key="id",
            dimensions=[Dimension(name="day", type="time", granularity="month")],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        )
    )
    yield layer
    layer.adapter.close()


@pytest.mark.parametrize("dialect", ["bigquery", "snowflake", "trino"])
@pytest.mark.parametrize(
    "grain,offset,rows",
    [
        ("month", "2 months", 2),
        ("month", "75 days", 2),
        ("month", "105 days", 4),
        (None, "2 months", 2),
        ("quarter", "1 year", 4),
    ],
)
def test_ratio_lag_scales_offset_to_result_grain(layer, dialect, grain, offset, rows):
    layer.add_metric(
        Metric(name="ratio", type="ratio", numerator="sales.revenue", denominator="sales.revenue", offset_window=offset)
    )
    if grain is None:
        layer.graph.models["sales"].dimensions[0].granularity = None
    dimension = "sales.day" + (f"__{grain}" if grain else "")
    sql = layer.compile(metrics=["ratio"], dimensions=[dimension], order_by=[dimension], dialect=dialect)
    parsed = sqlglot.parse_one(sql, read=dialect)
    assert [int(lag.args["offset"].this) for lag in parsed.find_all(exp.Lag)] == [rows]
    cursor = layer.adapter.execute(parsed.sql(dialect="duckdb"))
    values = [row[-1] for row in cursor.fetchall()]
    assert values == ([None, None, 10] if rows == 2 else [None, None, None])


@pytest.mark.parametrize("dialect", ["duckdb", "postgres"])
def test_ungranulated_ratio_uses_row_fallback(layer, dialect):
    layer.graph.models["sales"].dimensions[0].granularity = None
    layer.add_metric(
        Metric(
            name="ratio", type="ratio", numerator="sales.revenue", denominator="sales.revenue", offset_window="2 months"
        )
    )
    sql = layer.compile(metrics=["ratio"], dimensions=["sales.day"], order_by=["sales.day"], dialect=dialect)
    parsed = sqlglot.parse_one(sql, read=dialect)
    assert [int(lag.args["offset"].this) for lag in parsed.find_all(exp.Lag)] == [2]
    assert [row[-1] for row in layer.adapter.execute(parsed.sql(dialect="duckdb")).fetchall()] == [None, None, 10]


@pytest.mark.parametrize("dialect", ["bigquery", "snowflake", "trino"])
def test_comparison_falls_back_to_named_period_row_count(layer, dialect):
    layer.add_metric(
        Metric(
            name="change",
            type="time_comparison",
            base_metric="sales.revenue",
            comparison_type="qoq",
            calculation="difference",
        )
    )
    sql = layer.compile(metrics=["change"], dimensions=["sales.day__month"], dialect=dialect)
    parsed = sqlglot.parse_one(sql, read=dialect)
    assert [int(lag.args["offset"].this) for lag in parsed.find_all(exp.Lag)] == [3]


@pytest.mark.parametrize("dialect", ["duckdb", "postgres"])
def test_calendar_capable_dialects_preserve_exact_period_lookup(layer, dialect):
    layer.add_metric(
        Metric(
            name="ratio", type="ratio", numerator="sales.revenue", denominator="sales.revenue", offset_window="2 months"
        )
    )
    sql = layer.compile(metrics=["ratio"], dimensions=["sales.day__month"], dialect=dialect)
    parsed = sqlglot.parse_one(sql, read=dialect)
    assert not list(parsed.find_all(exp.Lag))
    assert [row[-1] for row in layer.adapter.execute(parsed.sql(dialect="duckdb")).fetchall()] == [None, None, None]
