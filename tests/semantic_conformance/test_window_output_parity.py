"""Window expressions aggregate period outputs, with independent expected results."""

from datetime import date, datetime

import pytest

from sidemantic import Dimension, Metric, Model, SecurityPolicy, SemanticLayer


@pytest.fixture(params=["python", "rust"])
def layer(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs", reason="Window acceptance requires the Rust extension")
    layer = SemanticLayer(engine=request.param, fallback=False, auto_register=False)
    layer.adapter.execute("""
        create table events(id integer, day date, category varchar, amount integer,
                            person integer, tenant integer);
        insert into events values
          (1, '2024-01-01', 'a', 2, 1, 1), (2, '2024-01-01', 'a', 3, 2, 1),
          (3, '2024-01-03', 'a', 7, 1, 1), (4, '2024-01-04', 'a', null, 1, 1),
          (5, '2024-01-01', null, 10, 3, 1), (6, '2024-01-03', null, 20, 3, 1),
          (7, '2024-01-04', null, 40, 4, 1), (8, '2024-01-03', 'a', 1000, 9, 2);
    """)
    layer.add_model(
        Model(
            name="events",
            table="events",
            primary_key="id",
            dimensions=[
                Dimension(name="day", type="time", granularity="day"),
                Dimension(name="category", type="categorical"),
            ],
            metrics=[
                Metric(name="daily_amount", agg="sum", sql="amount"),
                Metric(name="daily_people", agg="count_distinct", sql="person"),
            ],
            security=SecurityPolicy(row_filters=["tenant = {{ user.tenant }}"]),
        )
    )
    try:
        yield layer
    finally:
        layer.adapter.close()


def run(
    layer, *, expression="SUM(base.daily_amount)", frame=None, order=None, filters=None, dimensions=None, execute=True
):
    layer.graph.models["events"].metrics.append(
        Metric(
            name="windowed",
            type="cumulative",
            window_expression=expression,
            window_frame=frame,
            window_order=order,
        )
    )
    sql = layer.compile(
        metrics=["events.windowed"],
        dimensions=dimensions or ["events.day", "events.category"],
        filters=filters or [],
        user_attributes={"tenant": 1},
        order_by=["events.category", "events.day"],
    )
    if not execute:
        return sql
    result = layer.adapter.execute(sql)
    dependency = "daily_people" if "daily_people" in expression else "daily_amount"
    assert [column[0] for column in result.description] == ["day", "category", dependency, "windowed"]
    return [
        tuple(
            value.date().isoformat()
            if isinstance(value, datetime)
            else value.isoformat()
            if isinstance(value, date)
            else value
            for value in (row[0], row[1], row[3])
        )
        for row in result.fetchall()
    ]


@pytest.mark.parametrize(
    "frame,expected",
    [
        (None, [5, 12, 12, 10, 30, 70]),
        ("ROWS BETWEEN 1 PRECEDING AND CURRENT ROW", [5, 12, 7, 10, 30, 60]),
        ("RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW", [5, 7, 7, 10, 20, 60]),
        ("RANGE BETWEEN INTERVAL '2 day' PRECEDING AND CURRENT ROW", [5, 12, 7, 10, 30, 60]),
    ],
)
def test_period_frames_preserve_sparse_boundaries_null_values_and_partitions(layer, frame, expected):
    rows = run(layer, frame=frame)
    assert rows == [
        (day, category, value)
        for (day, category), value in zip(
            [
                ("2024-01-01", "a"),
                ("2024-01-03", "a"),
                ("2024-01-04", "a"),
                ("2024-01-01", None),
                ("2024-01-03", None),
                ("2024-01-04", None),
            ],
            expected,
        )
    ]


def test_expression_alias_is_a_period_metric_and_not_a_physical_column(layer):
    assert run(layer, expression='AVG(base."daily_amount")', order="day") == [
        ("2024-01-01", "a", 5),
        ("2024-01-03", "a", 6),
        ("2024-01-04", "a", 6),
        ("2024-01-01", None, 10),
        ("2024-01-03", None, 15),
        ("2024-01-04", None, 70 / 3),
    ]


def test_explicit_metric_output_order_overrides_chronological_order(layer):
    layer.adapter.execute("update events set amount = amount * 10 where day = '2024-01-01' and category = 'a'")
    assert [row[2] for row in run(layer, order="daily_amount")] == [57, 7, 57, 10, 30, 70]


def test_distinct_period_counts_are_summed_not_combined_row_distincts(layer):
    assert [row[2] for row in run(layer, expression="SUM(base.daily_people)")] == [2, 3, 4, 1, 2, 3]


@pytest.mark.parametrize(
    "function,expected",
    [
        ("MIN", [5, 5, 5, 10, 10, 10]),
        ("MAX", [5, 7, 7, 10, 20, 40]),
        ("COUNT", [1, 2, 2, 1, 2, 3]),
    ],
)
def test_promoted_functions_consume_period_values(layer, function, expected):
    assert [row[2] for row in run(layer, expression=f"{function}(base.daily_amount)")] == expected


def test_query_filter_and_policy_apply_before_window(layer):
    assert run(layer, filters=["events.day >= '2024-01-03'"]) == [
        ("2024-01-03", "a", 7),
        ("2024-01-04", "a", 7),
        ("2024-01-03", None, 20),
        ("2024-01-04", None, 60),
    ]


@pytest.mark.parametrize("order", ["amount", "missing", "day DESC", "day) FROM events; --"])
def test_invalid_window_order_rejected_before_execution(layer, order):
    with pytest.raises(Exception, match="window_order"):
        run(layer, order=order, execute=False)


@pytest.mark.parametrize("frame", ["garbage", "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW); SELECT 1; --"])
def test_invalid_window_frame_rejected_before_execution(layer, frame):
    with pytest.raises(Exception):
        run(layer, frame=frame, execute=False)
