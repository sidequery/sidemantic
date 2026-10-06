"""Minimized generated regression for nullable dates in cumulative queries."""

from datetime import date, datetime

import pytest

from sidemantic import Dimension, Metric, Model, SemanticLayer


@pytest.mark.parametrize("engine", ["python", "rust"])
@pytest.mark.parametrize("null_amount", [None, 7])
def test_cumulative_null_date_follows_known_periods(engine, null_amount):
    """DuckDB must keep NULL dates NULL and apply its NULLS LAST window order.

    Seed 20261005/index 27 exposed a DuckDB 1.3.2 optimizer discrepancy: the
    Python projection returned a NULL running total while the Rust projection
    returned zero, despite equivalent window clauses. DuckDB 1.4.0 additionally
    exposed the internal NULL date sentinel as a real date. This independent
    oracle guards against both compilers agreeing on an incorrect host result.
    """
    with SemanticLayer(engine=engine, fallback=False, auto_register=False) as layer:
        layer.adapter.execute("create table facts (id integer, day date, amount integer, status varchar)")
        layer.adapter.executemany(
            "insert into facts values (?, ?, ?, ?)",
            [(1, "2000-01-01", 0, "paid"), (2, None, null_amount, "paid")],
        )
        layer.add_model(
            Model(
                name="facts",
                table="facts",
                primary_key="id",
                dimensions=[
                    Dimension(name="day", type="time", granularity="day"),
                    Dimension(name="status", type="categorical"),
                ],
                metrics=[
                    Metric(name="total", agg="sum", sql="amount"),
                    Metric(name="running", type="cumulative", sql="total"),
                ],
            )
        )
        result = layer.query(
            metrics=["facts.running"],
            dimensions=["facts.day__month", "facts.status"],
            order_by=["facts.day__month"],
        )
        assert [column[0] for column in result.description] == ["day__month", "status", "total", "running"]
        rows = [
            (period.date() if isinstance(period, datetime) else period, status, total, running)
            for period, status, total, running in result.fetchall()
        ]
        assert rows == [(date(2000, 1, 1), "paid", 0, 0), (None, "paid", null_amount, null_amount or 0)]
        assert layer.last_engine_selection == {"engine": engine, "reason": None}
