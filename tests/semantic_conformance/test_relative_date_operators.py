"""Relative-date comparisons retain point and full-period semantics."""

import pytest

from sidemantic import Dimension, Metric, Model, SemanticLayer


@pytest.fixture(params=["python", "rust"])
def layer(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs", reason="Requires the built Rust extension")
    layer = SemanticLayer(engine=request.param, fallback=False, auto_register=False)
    layer.add_model(
        Model(
            name="events",
            table="date_rows",
            primary_key="id",
            dimensions=[Dimension(name="created_at", type="time", granularity="day")],
            metrics=[Metric(name="total", agg="sum", sql="amount")],
        )
    )
    layer.adapter.execute("""
        create table date_rows as
        select offset_days as id, current_date + offset_days::integer as created_at,
               row_number() over () as amount
        from (values (-60), (-1), (0), (1), (60)) offsets(offset_days)
    """)
    try:
        yield layer
    finally:
        layer.adapter.close()


@pytest.mark.parametrize("route", ["query", "sql"])
@pytest.mark.parametrize("operator", ["=", "!=", "<>", "<", "<=", ">", ">="])
@pytest.mark.parametrize("period", ["today", "this month"])
def test_relative_date_operator_result_matches_period_contract(layer, route, operator, period):
    if period == "today":
        start = "current_date"
        period_predicate = "created_at = current_date"
    else:
        start = "date_trunc('month', current_date)"
        period_predicate = f"created_at >= {start} and created_at < {start} + interval '1 month'"
    if operator == "=":
        predicate = period_predicate
    elif operator in {"!=", "<>"}:
        predicate = f"not ({period_predicate})"
    else:
        predicate = f"created_at {operator} {start}"
    expected = layer.adapter.execute(f"select sum(amount) from date_rows where {predicate}").fetchall()
    filter_sql = f"events.created_at {operator} '{period}'"
    if route == "query":
        actual = layer.query(metrics=["events.total"], filters=[filter_sql]).fetchall()
    else:
        actual = layer.sql(f"select events.total from events where {filter_sql}").fetchall()
    assert actual == expected
    if layer.engine == "rust":
        assert layer.last_engine_selection["engine"] == "rust"
