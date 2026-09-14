"""Time-key display buckets do not replace row identity for joins or fanout."""

from datetime import date

import pytest

from sidemantic import Dimension, Metric, Model, Relationship, SemanticLayer


@pytest.fixture(params=["python", "rust"])
def layer(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs")
    layer = SemanticLayer(engine=request.param, fallback=False, auto_register=False)
    layer.add_model(
        Model(
            name="events",
            table="time_key_events",
            primary_key="event_time",
            dimensions=[
                Dimension(name="event_time", type="time", granularity="day", sql="cast(raw_time as timestamp)"),
                Dimension(name="id", type="numeric", sql="raw_id + 100"),
            ],
            metrics=[Metric(name="amount", agg="sum", sql="amount"), Metric(name="people", agg="count_distinct")],
        )
    )
    layer.adapter.execute("""
        create table time_key_events(raw_id integer,raw_time varchar,amount integer);
        insert into time_key_events values
            (1,'2026-01-01 01:00:00',3),(2,'2026-01-01 02:00:00',4),(3,'2026-01-02 01:00:00',8);
    """)
    try:
        yield layer
    finally:
        layer.adapter.close()


def execute(layer, **query):
    sql = layer.compile(**query)
    if layer.engine == "rust":
        assert layer.last_engine_selection["engine"] == "rust"
    cursor = layer.adapter.execute(sql)
    return [column[0] for column in cursor.description], cursor.fetchall()


@pytest.mark.parametrize("dimension", ["events.event_time", "events.event_time__day"])
@pytest.mark.parametrize("timezone", [None, "America/Los_Angeles"])
def test_time_identity_projection_uses_requested_or_default_grain(layer, dimension, timezone):
    first, second = (date(2026, 1, 1), date(2026, 1, 2))
    if timezone:
        first, second = date(2025, 12, 31), date(2026, 1, 1)
    columns, rows = execute(
        layer, metrics=["events.amount"], dimensions=[dimension], order_by=[dimension], timezone=timezone
    )
    assert columns == [dimension.split(".")[1], "amount"]
    assert rows == [(first, 7), (second, 8)]


def test_numeric_computed_identity_keeps_unbucketed_projection(layer):
    layer.graph.models["events"].primary_key = "id"
    assert execute(layer, metrics=["events.amount"], dimensions=["events.id"], order_by=["events.id"]) == (
        ["id", "amount"],
        [(101, 3), (102, 4), (103, 8)],
    )


def test_time_identity_stays_unbucketed_for_join_and_fanout(layer):
    if layer.engine != "rust":
        pytest.skip("Python joins physical keys; native computed-key identity has its own independent oracle")
    layer.add_model(
        Model(
            name="logs",
            table="time_key_logs",
            primary_key="id",
            dimensions=[
                Dimension(name="event_ref", type="time", granularity="day", sql="cast(raw_time as timestamp)"),
                Dimension(name="category", type="categorical"),
            ],
            relationships=[Relationship(name="events", type="many_to_one", foreign_key="event_ref")],
        )
    )
    layer.adapter.execute("""
        create table time_key_logs(id integer,raw_time varchar,category varchar);
        insert into time_key_logs values
            (1,'2026-01-01 01:00:00','x'),(2,'2026-01-01 01:00:00','x'),
            (3,'2026-01-01 02:00:00','x'),(4,'2026-01-02 01:00:00','y');
    """)
    assert execute(
        layer,
        metrics=["events.amount", "events.people"],
        dimensions=["events.event_time__day", "logs.category"],
        order_by=["events.event_time__day", "logs.category"],
    ) == (
        ["event_time__day", "category", "amount", "people"],
        [(date(2026, 1, 1), "x", 7, 2), (date(2026, 1, 2), "y", 8, 1)],
    )
