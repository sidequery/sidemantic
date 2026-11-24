"""Window binding respects exact graph identities beside model-local names."""

from datetime import date

import pytest

from sqlctx import Dimension, Metric, Model, SemanticLayer


@pytest.mark.parametrize("engine", ["python", "rust"])
@pytest.mark.parametrize("expression", [False, True])
def test_dotted_graph_window_shadows_local_metric(engine, expression):
    layer = SemanticLayer(engine=engine, fallback=False, auto_register=False)
    layer.add_model(
        Model(
            name="events",
            table="identity_events",
            primary_key="id",
            dimensions=[Dimension(name="day", type="time", granularity="day")],
            metrics=[
                Metric(name="total", agg="sum", sql="amount"),
                Metric(name="running", agg="sum", sql="shadow"),
            ],
        )
    )
    controls = {"window_expression": "SUM(base.total)"} if expression else {"sql": "total"}
    layer.graph.add_metric(Metric(name="events.running", type="cumulative", **controls), model_name="events")
    try:
        layer.adapter.execute("create table identity_events(id integer, day date, amount integer, shadow integer)")
        layer.adapter.execute(
            "insert into identity_events values (1, '2024-01-01', 10, 900), (2, '2024-01-02', 20, 800)"
        )
        result = layer.query(metrics=["events.running"], dimensions=["events.day"], order_by=["events.day"])
        assert [column[0] for column in result.description] == ["day", "total", "events.running"]
        assert result.fetchall() == [(date(2024, 1, 1), 10, 10), (date(2024, 1, 2), 20, 30)]
    finally:
        layer.adapter.close()


@pytest.mark.parametrize("engine", ["python", "rust"])
@pytest.mark.parametrize("metric_type", ["time_comparison", "ratio"])
def test_previous_period_metric_name_is_one_output_identifier(engine, metric_type):
    layer = SemanticLayer(engine=engine, fallback=False, auto_register=False)
    name = "analytics.weekly growth"
    layer.add_model(
        Model(
            name="sales",
            sql="select * from (values (date '2024-01-01', 10), (date '2024-01-08', 30)) s(day, amount)",
            primary_key="day",
            dimensions=[Dimension(name="day", type="time", granularity="week")],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        )
    )
    controls = (
        {"base_metric": "sales.revenue", "comparison_type": "wow", "calculation": "difference"}
        if metric_type == "time_comparison"
        else {"numerator": "sales.revenue", "denominator": "sales.revenue", "offset_window": "1 week"}
    )
    layer.add_metric(Metric(name=name, type=metric_type, **controls))
    try:
        cursor = layer.query(metrics=[name], dimensions=["sales.day__week"], order_by=[f"{name} DESC NULLS LAST"])
        assert [column[0] for column in cursor.description] == ["day__week", "revenue", name]
        growth = 20 if metric_type == "time_comparison" else 3
        assert cursor.fetchall() == [(date(2024, 1, 8), 30, growth), (date(2024, 1, 1), 10, None)]
    finally:
        layer.adapter.close()


@pytest.mark.parametrize("engine", ["python", "rust"])
@pytest.mark.parametrize("offset_ratio", [False, True])
def test_previous_period_column_does_not_shadow_dimension(engine, offset_ratio):
    group = "growth_prev_denom" if offset_ratio else "growth_prev_value"
    layer = SemanticLayer(engine=engine, fallback=False, auto_register=False)
    layer.add_model(
        Model(
            name="sales",
            sql="select * from (values (date '2024-01-01', 10), (date '2024-01-08', 30)) s(day, amount)",
            primary_key="day",
            dimensions=[
                Dimension(name="day", type="time", granularity="week"),
                Dimension(name=group, type="numeric", sql="7"),
            ],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        )
    )
    growth = (
        Metric(
            name="growth", type="ratio", numerator="sales.revenue", denominator="sales.revenue", offset_window="1 week"
        )
        if offset_ratio
        else Metric(
            name="growth",
            type="time_comparison",
            base_metric="sales.revenue",
            comparison_type="wow",
            calculation="difference",
        )
    )
    layer.add_metric(growth)
    try:
        cursor = layer.query(
            metrics=["growth"], dimensions=["sales.day__week", f"sales.{group}"], order_by=["sales.day__week"]
        )
        assert [column[0] for column in cursor.description] == ["day__week", group, "revenue", "growth"]
        assert cursor.fetchall() == [
            (date(2024, 1, 1), 7, 10, None),
            (date(2024, 1, 8), 7, 30, 3 if offset_ratio else 20),
        ]
    finally:
        layer.adapter.close()
