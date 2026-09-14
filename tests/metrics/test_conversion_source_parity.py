"""Execute the same conversion source contracts through Python and strict Rust."""

import json

import duckdb
import pytest

from sidemantic import Dimension, Metric, Model
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.semantic_handoff import graph_to_semantic_input
from sidemantic.sql.generator import SQLGenerator


@pytest.mark.parametrize("engine", ["python", "rust"])
@pytest.mark.parametrize(
    "owner,entity",
    [("model", "person"), ("model", "events.uid"), ("model", "uid"), ("graph", "person"), ("graph", "uid")],
)
def test_conversion_sql_source_expressions(engine, owner, entity):
    metric = Metric(
        name="funnel",
        type="conversion",
        entity=entity,
        steps=["events.event = 'events.signup'", "{model}.event = 'purchase'"],
    )
    model = Model(
        name="events",
        primary_key="uid",
        sql="""
            select 1 as uid, 'events.signup' as event, '2024-01-01'::timestamp as ts, 'a' as region
            union all select 1, 'purchase', '2024-01-02'::timestamp, 'b'
            union all select 2, 'events.signup', '2024-01-01'::timestamp, 'a'
            union all select 2, 'purchase', '2023-12-31'::timestamp, 'b'
        """,
        dimensions=[
            Dimension(name="uid", type="categorical"),
            Dimension(name="person", type="categorical", sql="coalesce({model}.uid, 0)"),
            Dimension(name="event_time", type="time", sql="cast(events.ts as date)"),
            Dimension(name="region", type="categorical", sql="upper(events.region)"),
        ],
        metrics=[metric] if owner == "model" else [],
    )
    graph = SemanticGraph()
    graph.add_model(model)
    if owner == "graph":
        graph.add_metric(metric)
    reference = "events.funnel" if owner == "model" else "funnel"
    query = dict(metrics=[reference], dimensions=["events.region"], filters=["events.region = 'A'"])
    if engine == "python":
        sql = SQLGenerator(graph).generate(**query)
    else:
        rust = pytest.importorskip("sidemantic_rs")
        payload = graph_to_semantic_input(graph)
        if owner == "graph":
            payload["metric_owners"] = {"funnel": "events"}
        sql = rust.compile_with_semantic_input(json.dumps(payload), json.dumps(query))
    with duckdb.connect() as connection:
        rows = connection.execute(sql).fetchall()
    # Both purchase events are outside the filtered source population.
    assert rows == [("A", 2, 2, 0, 0)]


@pytest.mark.parametrize("empty", [False, True])
def test_rust_two_event_conversion_projects_source_aliases(empty):
    rust = pytest.importorskip("sidemantic_rs")
    model = Model(
        name="events",
        primary_key="uid",
        sql="""
            select 1 as uid, 'signup' as action, '2024-01-01'::timestamp as ts
            union all select 1, 'purchase', '2024-01-02'::timestamp
            union all select 2, 'signup', '2024-01-01'::timestamp
        """,
        dimensions=[
            Dimension(name="person", type="categorical", sql="coalesce({model}.uid, 0)"),
            Dimension(name="event_time", type="time", sql="cast(events.ts as date)"),
            Dimension(name="event_type", type="categorical", sql="lower(events.action)"),
        ],
    )
    graph = SemanticGraph()
    graph.add_model(model)
    graph.add_metric(
        Metric(
            name="conversion",
            type="conversion",
            entity="person",
            base_event="signup",
            conversion_event="purchase",
            fill_nulls_with=0,
        )
    )
    payload = graph_to_semantic_input(graph)
    payload["metric_owners"] = {"conversion": "events"}
    query = {"metrics": ["conversion"], "filters": ["events.person < 0"] if empty else []}
    sql = rust.compile_with_semantic_input(json.dumps(payload), json.dumps(query))
    with duckdb.connect() as connection:
        assert connection.execute(sql).fetchall() == [(None if empty else 0.5,)]


@pytest.mark.parametrize("engine", ["python", "rust"])
def test_conversion_fill_is_accepted_without_changing_empty_result(engine):
    model = Model(
        name="events",
        primary_key="uid",
        sql="select 1 as uid, 'browse' as event_type, '2024-01-01'::timestamp as ts",
        dimensions=[
            Dimension(name="uid", type="categorical"),
            Dimension(name="event_type", type="categorical"),
            Dimension(name="ts", type="time"),
        ],
        metrics=[
            Metric(
                name="conversion",
                type="conversion",
                entity="uid",
                base_event="signup",
                conversion_event="purchase",
                fill_nulls_with=7,
            )
        ],
    )
    graph = SemanticGraph()
    graph.add_model(model)
    query = {"metrics": ["events.conversion"]}
    if engine == "python":
        sql = SQLGenerator(graph).generate(**query)
    else:
        rust = pytest.importorskip("sidemantic_rs")
        sql = rust.compile_with_semantic_input(json.dumps(graph_to_semantic_input(graph)), json.dumps(query))
    with duckdb.connect() as connection:
        assert connection.execute(sql).fetchall() == [(None,)]


@pytest.mark.parametrize("engine", ["python", "rust"])
def test_two_event_conversion_uses_last_time_dimension_instead_of_default(engine):
    model = Model(
        name="events",
        primary_key="uid",
        default_time_dimension="created",
        sql="""
            select 1 as uid, 'signup' as event_type,
              timestamp '2024-01-01' as created, timestamp '2024-01-03' as occurred
            union all select 1, 'purchase', timestamp '2024-01-02', timestamp '2024-01-02'
        """,
        dimensions=[
            Dimension(name="event_type", type="categorical"),
            Dimension(name="created", type="time"),
            Dimension(name="occurred", type="time"),
        ],
        metrics=[
            Metric(name="conversion", type="conversion", entity="uid", base_event="signup", conversion_event="purchase")
        ],
    )
    graph = SemanticGraph()
    graph.add_model(model)
    query = {"metrics": ["events.conversion"]}
    if engine == "python":
        sql = SQLGenerator(graph).generate(**query)
    else:
        rust = pytest.importorskip("sidemantic_rs")
        sql = rust.compile_with_semantic_input(json.dumps(graph_to_semantic_input(graph)), json.dumps(query))
    with duckdb.connect() as connection:
        cursor = connection.execute(sql)
        columns = [field[0] for field in cursor.description]
        assert [row[columns.index("conversion")] for row in cursor.fetchall()] == [0.0]
