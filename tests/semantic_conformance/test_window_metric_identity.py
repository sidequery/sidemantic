"""Window binding respects exact graph identities beside model-local names."""

from datetime import date

import pytest

from sidemantic import Dimension, Metric, Model, SemanticLayer


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
