"""Snowflake default-session date differences execute through the native boundary."""

import json

import duckdb
import pytest

from sidemantic import Metric, Model, SemanticLayer
from sidemantic.rust_bridge import compile_semantic_input


@pytest.mark.parametrize(
    "expression,expected",
    [
        ("DATEDIFF(day, DATE '2024-02-28', DATE '2024-03-01')", 2),
        ("DATEDIFF('DD', DATE '2024-03-01', DATE '2024-02-28')", -2),
        ("DATEDIFF(day, TIMESTAMP '2024-01-01 23:59:59', TIMESTAMP '2024-01-02 00:00:00')", 1),
        ("DATEDIFF(day, TIMESTAMP '2024-01-02 00:00:00', TIMESTAMP '2024-01-01 23:59:59')", -1),
        ("DATEDIFF(month, DATE '2024-01-31', DATE '2024-02-01')", 1),
        ("DATEDIFF('MM', DATE '2024-02-01', DATE '2024-01-31')", -1),
        ("DATEDIFF(month, DATE '2024-01-01', DATE '2024-01-31')", 0),
        # Snowflake WEEK_START=0/1 uses Monday calendar boundaries.
        ("DATEDIFF(week, DATE '2024-01-07', DATE '2024-01-08')", 1),
        ("DATEDIFF('WK', DATE '2024-01-08', DATE '2024-01-07')", -1),
        ("DATEDIFF(week, DATE '2024-01-01', DATE '2024-01-07')", 0),
        ("DATEDIFF(week, DATE '2023-12-31', DATE '2024-01-01')", 1),
        ("COALESCE(DATEDIFF(day, NULL, DATE '2024-01-01'), 42)", 42),
    ],
)
@pytest.mark.parametrize("mode", ["rewrite", "compile"])
def test_snowflake_date_differences(expression, expected, mode):
    native = pytest.importorskip("sidemantic_rs", reason="Requires matching native extension")
    if mode == "rewrite":
        source = {"version": 1, "input_dialect": "snowflake", "models": []}
        sql = native.rewrite_with_semantic_input(json.dumps(source), f"SELECT {expression}")
    else:
        layer = SemanticLayer(engine="rust", fallback=False, auto_register=False)
        try:
            layer.add_model(
                Model(
                    name="dates", sql="SELECT 1 AS id", metrics=[Metric(name="difference", agg="sum", sql=expression)]
                )
            )
            sql = compile_semantic_input(layer.graph, {"metrics": ["dates.difference"]}, input_dialect="snowflake")
        finally:
            layer.adapter.close()
    with duckdb.connect() as connection:
        assert connection.execute(sql).fetchall() == [(expected,)]
