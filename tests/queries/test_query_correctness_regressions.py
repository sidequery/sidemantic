"""Execution oracles for fanout, distinct aggregates, windows and funnels."""

from datetime import date

import pytest

from sidemantic import Dimension, Metric, Model, Relationship, SemanticLayer


@pytest.fixture
def python_layer():
    return SemanticLayer(engine="python", auto_register=False)


@pytest.mark.parametrize("owner", ["a", "b"])
def test_direct_many_to_many_deduplicates_both_directions(python_layer, owner):
    layer = python_layer
    for name in ["a", "b"]:
        layer.add_model(
            Model(
                name=name,
                sql="SELECT * FROM (VALUES (1, 'shared', 'group', 100), (2, 'shared', 'group', 200)) t(id, code, label, amount)",
                primary_key="id",
                dimensions=[Dimension(name="label", type="categorical")],
                metrics=[Metric(name="revenue", agg="sum", sql="amount")],
                relationships=[Relationship(name="b", type="many_to_many", foreign_key="code", primary_key="code")]
                if name == "a"
                else [],
            )
        )
    other = "b" if owner == "a" else "a"
    assert layer.query(metrics=[f"{owner}.revenue"], dimensions=[f"{other}.label"]).fetchall() == [("group", 300)]


@pytest.mark.parametrize("aggregate,expected", [("SUM", 30), ("AVG", 15)])
@pytest.mark.parametrize("fanout", [False, True])
def test_sql_first_distinct_aggregate(python_layer, aggregate, expected, fanout):
    metric = Metric(name="value", sql=f"{aggregate}(DISTINCT amount)")
    assert metric.agg is None and metric.sql_is_complete
    layer = python_layer
    layer.add_model(
        Model(
            name="sales",
            sql="SELECT * FROM (VALUES (1, 10), (2, 10), (3, 20)) t(id, amount)",
            primary_key="id",
            metrics=[metric],
            relationships=[Relationship(name="details", type="one_to_many", foreign_key="sale_id")],
        )
    )
    layer.add_model(
        Model(
            name="details",
            sql="SELECT * FROM (VALUES (1, 1, 'a'), (2, 1, 'a'), (3, 2, 'a'), (4, 3, 'a')) t(id, sale_id, label)",
            primary_key="id",
            dimensions=[Dimension(name="label", type="categorical")],
        )
    )
    dimensions = ["details.label"] if fanout else []
    assert layer.query(metrics=["sales.value"], dimensions=dimensions).fetchall() == [
        ("a", expected) if fanout else (expected,)
    ]


@pytest.fixture
def sales(python_layer):
    python_layer.add_model(
        Model(
            name="sales",
            sql="""SELECT * FROM (VALUES
                (1, DATE '2024-01-01', DATE '2023-12-01', 'a', 10),
                (2, DATE '2024-01-01', DATE '2023-12-02', 'b', 100),
                (3, DATE '2024-01-02', DATE '2023-12-01', 'a', 20),
                (4, DATE '2024-01-02', DATE '2023-12-02', 'b', 200)
            ) t(id, day, cohort_day, category, amount)""",
            primary_key="id",
            dimensions=[
                Dimension(name="day", type="time", granularity="day"),
                Dimension(name="cohort_day", type="time", granularity="day"),
                Dimension(name="category", type="categorical"),
            ],
            metrics=[
                Metric(name="revenue", agg="sum", sql="amount"),
                Metric(name="running", type="cumulative", sql="revenue"),
            ],
        )
    )
    return python_layer


@pytest.mark.parametrize("options", [{}, {"window_order": "day"}, {"grain_to_date": "month"}, {"window": "2 days"}])
def test_cumulative_keeps_temporal_cohorts_separate(sales, options):
    for key, value in options.items():
        setattr(sales.graph.get_model("sales").get_metric("running"), key, value)
    result = sales.query(
        metrics=["sales.running"],
        dimensions=["sales.day", "sales.cohort_day"],
        order_by=["sales.day", "sales.cohort_day"],
    )
    assert [row[-1] for row in result.fetchall()] == [10, 100, 30, 300]


@pytest.mark.parametrize(
    "predicate",
    [
        "running > 15",
        "sales.running > 15",
        "running > 15 AND category = 'a'",
        "running > 15 OR day = DATE '2024-01-01'",
    ],
)
def test_selected_window_result_filters_run_after_window(sales, predicate):
    query = f"SELECT day, running FROM sales WHERE category = 'a' AND ({predicate}) ORDER BY day"
    result = sales.sql(query)
    assert [column[0] for column in result.description] == ["day", "running"]
    expected = [(date(2024, 1, 2), 30)]
    if " OR " in predicate:
        expected.insert(0, (date(2024, 1, 1), 10))
    assert result.fetchall() == expected


@pytest.mark.parametrize("order", ["d", "total DESC"])
def test_sql_window_order_by_caller_alias(sales, order):
    result = sales.sql(f"SELECT day AS d, running AS total FROM sales ORDER BY {order} LIMIT 1 OFFSET 1")
    assert [column[0] for column in result.description] == ["d", "total"]
    expected = [(date(2024, 1, 2), 330)] if order == "d" else [(date(2024, 1, 1), 110)]
    assert result.fetchall() == expected


def test_direct_window_aliases_and_result_filter(sales):
    sql = sales.compile(
        metrics=["sales.running"],
        dimensions=["sales.day"],
        aliases={"sales.day": "d", "sales.running": "total"},
        filters=["sales.running > 150"],
        order_by=["total DESC"],
    )
    result = sales.conn.execute(sql)
    assert [column[0] for column in result.description] == ["d", "revenue", "total"]
    assert result.fetchall() == [(date(2024, 1, 2), 220, 330)]


@pytest.mark.parametrize("owner", ["model", "graph"])
@pytest.mark.parametrize("base_kind", ["aggregate", "graph_aggregate", "derived", "ratio", "complete", "expression"])
def test_filtered_cumulative_scopes_population_without_mutating_graph(sales, owner, base_kind):
    model = sales.graph.get_model("sales")
    base = "revenue"
    if base_kind == "graph_aggregate":
        sales.add_metric(Metric(name="graph_revenue", agg="sum", sql="sales.amount"))
        base = "graph_revenue"
    if base_kind == "derived":
        model.metrics.append(Metric(name="double_revenue", type="derived", sql="revenue * 2"))
        base = "double_revenue"
    elif base_kind == "ratio":
        model.metrics.extend(
            [
                Metric(name="orders", agg="count"),
                Metric(name="average", type="ratio", numerator="revenue", denominator="orders"),
            ]
        )
        base = "average"
    elif base_kind == "complete":
        model.metrics.append(Metric(name="distinct_revenue", sql="SUM(DISTINCT amount)"))
        base = "distinct_revenue"
    running = Metric(
        name="paid_running",
        type="cumulative",
        sql=f"sales.{base}" if owner == "graph" and base_kind != "graph_aggregate" else base,
        filters=["sales.category = 'a'"],
        window_expression="SUM(base.revenue)" if base_kind == "expression" else None,
    )
    if owner == "graph":
        sales.add_metric(running)
        reference = "paid_running"
    else:
        model.metrics.append(running)
        reference = "sales.paid_running"
    before_models = {name: item.model_dump() for name, item in sales.graph.models.items()}
    before_metrics = {name: item.model_dump() for name, item in sales.graph.metrics.items()}
    result = sales.query(
        metrics=["sales.revenue", "sales.running", reference],
        dimensions=["sales.day"],
        order_by=["sales.day"],
    )
    columns = [column[0] for column in result.description]
    assert not any(column.startswith("__sidemantic") for column in columns)
    rows = [dict(zip(columns, row, strict=True)) for row in result.fetchall()]
    factor = 2 if base_kind == "derived" else 1
    assert [(row["revenue"], row["running"], row["paid_running"]) for row in rows] == [
        (110, 110, 10 * factor),
        (220, 330, 30 * factor),
    ]
    assert {name: item.model_dump() for name, item in sales.graph.models.items()} == before_models
    assert {name: item.model_dump() for name, item in sales.graph.metrics.items()} == before_metrics


def test_cumulative_combines_base_and_wrapper_filters(sales):
    model = sales.graph.get_model("sales")
    model.get_metric("revenue").filters = ["amount > 15"]
    model.get_metric("running").filters = ["category = 'a'"]
    result = sales.sql("SELECT day, revenue, running FROM sales ORDER BY day")
    assert result.fetchall() == [(date(2024, 1, 1), 100, None), (date(2024, 1, 2), 220, 20)]


@pytest.mark.parametrize("multistep", [False, True])
@pytest.mark.parametrize("table_backed", [False, True])
@pytest.mark.parametrize("limit", [None, 1])
def test_conversion_source_expressions_and_pagination(python_layer, multistep, table_backed, limit):
    layer = python_layer
    source = """SELECT * FROM (VALUES
        (1, 'signup', TIMESTAMP '2024-01-01', 'a'),
        (1, 'purchase', TIMESTAMP '2024-01-02', 'a'),
        (2, 'signup', TIMESTAMP '2024-01-01', 'b')
    ) t(uid, action, ts, region)"""
    if table_backed:
        layer.conn.execute(f"CREATE TABLE source_events AS {source}")
    layer.add_model(
        Model(
            name="events",
            table="source_events" if table_backed else None,
            sql=None if table_backed else source,
            primary_key="uid",
            dimensions=[
                Dimension(name="person", type="categorical", sql="coalesce({model}.uid, 0)"),
                Dimension(name="event_type", type="categorical", sql="lower(events.action)"),
                Dimension(name="event_time", type="time", granularity="day", sql="cast(events.ts as date)"),
                Dimension(name="region", type="categorical", sql="upper({model}.region)"),
            ],
            metrics=[
                Metric(
                    name="conversion",
                    type="conversion",
                    entity="person",
                    **(
                        {"steps": ["action = 'signup'", "action = 'purchase'"]}
                        if multistep
                        else {"base_event": "signup", "conversion_event": "purchase"}
                    ),
                )
            ],
        )
    )
    rows = layer.query(
        metrics=["events.conversion"], dimensions=["events.region"], order_by=["events.region"], limit=limit, offset=1
    ).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "B"
    assert rows[0][-1] == 0


@pytest.mark.parametrize("alias_reference", ["sales.day", "sales.running"])
@pytest.mark.parametrize("alias", ["__sidemantic_cumulative_0", "__sidemantic_cumulative_0_raw"])
def test_filtered_cumulative_private_inputs_do_not_collide_with_caller_aliases(sales, alias_reference, alias):
    sales.graph.get_model("sales").get_metric("running").filters = ["category = 'a'"]
    sql = sales.compile(
        metrics=["sales.running"],
        dimensions=["sales.day"],
        aliases={alias_reference: alias},
        order_by=["sales.day"],
    )
    result = sales.conn.execute(sql)
    expected_columns = [alias, "running"] if alias_reference == "sales.day" else ["day", alias]
    assert [column[0] for column in result.description] == expected_columns
    assert result.fetchall() == [(date(2024, 1, 1), 10), (date(2024, 1, 2), 30)]
