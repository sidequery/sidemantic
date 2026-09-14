"""Temporal defaults fill final results without altering period populations."""

import pytest

from sidemantic import Dimension, Metric, Model, SecurityPolicy, SemanticLayer


@pytest.fixture(params=["python", "rust"])
def layer(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs", reason="Temporal fill requires matching installed Rust extension")
    layer = SemanticLayer(engine=request.param, fallback=False, auto_register=False)
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
                Metric(name="amount", agg="sum", sql="amount"),
                Metric(name="base_filled", agg="sum", sql="amount", fill_nulls_with=10),
            ],
            security=SecurityPolicy(row_filters=["tenant = {{ user.tenant }}"]),
        )
    )
    layer.adapter.execute("""
        create table events(id integer, day date, category varchar, amount integer, tenant integer);
        insert into events values
        (1,'2024-01-01','a',null,1),(2,'2024-01-02','a',0,1),
        (3,'2024-01-03','a',6,1),(4,'2024-01-05','a',2,1),
        (5,'2024-01-01',null,null,1),(6,'2024-01-02',null,null,1),
        (7,'2024-01-03','a',100,2);
    """)
    try:
        yield layer
    finally:
        layer.adapter.close()


def values(layer, metric, **options):
    layer.add_metric(metric)
    sql = layer.compile(
        metrics=[metric.name],
        dimensions=["events.day", "events.category"],
        user_attributes={"tenant": 1},
        order_by=["events.category", "events.day"],
        **options,
    )
    result = layer.adapter.execute(sql)
    columns = [item[0] for item in result.description]
    index = columns.index(metric.name)
    return [row[index] for row in result.fetchall()]


@pytest.mark.parametrize(
    "controls,expected",
    [
        ({}, [-9, 0, 6, 8, -9, -9]),
        ({"window": "1 day"}, [-9, 0, 6, 2, -9, -9]),
        ({"grain_to_date": "day"}, [-9, 0, 6, 2, -9, -9]),
        ({"window_frame": "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW"}, [-9, 0, 6, 8, -9, -9]),
        ({"window_expression": "SUM(base.amount)"}, [-9, 0, 6, 8, -9, -9]),
    ],
)
def test_cumulative_final_default_keeps_null_base_and_partitions(layer, controls, expected):
    metric = Metric(
        name="filled",
        type="cumulative",
        sql=None if "window_expression" in controls else "events.amount",
        fill_nulls_with=-9,
        **controls,
    )
    assert values(layer, metric) == expected


@pytest.mark.parametrize(
    "calculation,expected",
    [
        ("difference", [-9, -9, 6, -9, -9, -9]),
        ("ratio", [-9, -9, -9, -9, -9, -9]),
        ("percent_change", [-9, -9, -9, -9, -9, -9]),
    ],
)
def test_missing_prior_and_zero_denominator_fill_after_calculation(layer, calculation, expected):
    metric = Metric(
        name="filled",
        type="time_comparison",
        base_metric="events.amount",
        comparison_type="dod",
        calculation=calculation,
        fill_nulls_with=-9,
    )
    assert values(layer, metric) == expected


def test_base_fill_and_temporal_fill_are_separate(layer):
    metric = Metric(name="filled", type="cumulative", sql="events.base_filled", fill_nulls_with=-9)
    assert values(layer, metric) == [10, 10, 16, 18, 10, 20]


@pytest.mark.parametrize("kind", ["cumulative", "time_comparison"])
def test_filters_and_policy_do_not_create_empty_period_groups(layer, kind):
    metric = Metric(
        name="filled",
        type=kind,
        sql="events.amount" if kind == "cumulative" else None,
        base_metric="events.amount" if kind == "time_comparison" else None,
        fill_nulls_with=0,
    )
    assert values(layer, metric, filters=["events.category = 'missing'"]) == []


def test_cumulative_explicit_order_and_post_window_pagination(layer):
    metric = Metric(name="filled", type="cumulative", sql="events.amount", window_order="day", fill_nulls_with=-9)
    assert values(layer, metric, limit=2, offset=1) == [0, 6]


def test_cumulative_string_default_is_a_literal(layer):
    layer.graph.models["events"].metrics.append(Metric(name="label", agg="min", sql="category"))
    metric = Metric(name="filled", type="cumulative", agg="min", sql="events.label", fill_nulls_with="missing's value")
    assert values(layer, metric) == ["a", "a", "a", "a", "missing's value", "missing's value"]


def test_explicit_comparison_offset_keeps_sparse_calendar_lookup(layer):
    metric = Metric(
        name="filled",
        type="time_comparison",
        base_metric="events.amount",
        comparison_type="dod",
        time_offset="2 days",
        calculation="difference",
        fill_nulls_with=-9,
    )
    assert values(layer, metric) == [-9, -9, -9, -4, -9, -9]


def test_mixed_temporal_outputs_retain_filled_cumulative_in_lag_cte(layer):
    layer.add_metric(Metric(name="running", type="cumulative", sql="events.amount", fill_nulls_with=-9))
    layer.add_metric(
        Metric(
            name="change",
            type="time_comparison",
            base_metric="events.amount",
            comparison_type="dod",
            calculation="difference",
            fill_nulls_with=-8,
        )
    )
    sql = layer.compile(
        metrics=["running", "change"],
        dimensions=["events.day", "events.category"],
        user_attributes={"tenant": 1},
        order_by=["events.category", "events.day"],
    )
    result = layer.adapter.execute(sql)
    columns = [column[0] for column in result.description]
    indices = [columns.index(name) for name in ("running", "change")]
    assert [tuple(row[index] for index in indices) for row in result.fetchall()] == [
        (-9, -8),
        (0, -8),
        (6, 6),
        (8, -8),
        (-9, -8),
        (-9, -8),
    ]


def test_offset_ratio_default_applies_after_sparse_prior_division(layer):
    metric = Metric(
        name="filled",
        type="ratio",
        numerator="events.amount",
        denominator="events.amount",
        offset_window="2 days",
        fill_nulls_with=-9,
    )
    assert values(layer, metric) == [-9, -9, -9, pytest.approx(1 / 3), -9, -9]
