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
    layer,
    *,
    expression="SUM(base.daily_amount)",
    frame=None,
    order=None,
    filters=None,
    dimensions=None,
    execute=True,
    dialect=None,
    aggregation=None,
):
    layer.graph.models["events"].metrics.append(
        Metric(
            name="windowed",
            type="cumulative",
            window_expression=expression if aggregation is None else None,
            agg=aggregation,
            sql="daily_amount" if aggregation is not None else None,
            window_frame=frame,
            window_order=order,
        )
    )
    sql = layer.compile(
        metrics=["events.windowed"],
        dimensions=dimensions or ["events.day", "events.category"],
        filters=filters or [],
        user_attributes={"tenant": 1},
        order_by=["events.category", dimensions[0] if dimensions else "events.day"],
        dialect=dialect,
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


@pytest.mark.parametrize("order", ["day", "events.day", "day__month"])
@pytest.mark.parametrize("aggregation", [None, "avg"])
def test_window_order_resolves_the_selected_dimension_grain(layer, order, aggregation):
    sql = run(
        layer,
        expression="AVG(base.daily_amount)",
        aggregation=aggregation,
        order=order,
        dimensions=["events.day__month", "events.category"],
        execute=False,
    )
    result = layer.adapter.execute(sql)
    assert [column[0] for column in result.description] == ["day__month", "category", "daily_amount", "windowed"]
    assert [row[1:] for row in result.fetchall()] == [("a", 12, 12), (None, 70, 70)]


def test_window_order_rejects_multiple_selected_grains(layer):
    with pytest.raises(Exception, match="window_order"):
        run(
            layer,
            order="day",
            dimensions=["events.day__day", "events.day__month", "events.category"],
            execute=False,
        )


def test_aggregate_reference_honors_explicit_range_frame(layer):
    rows = run(layer, aggregation="avg", frame="RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW")
    assert [row[2] for row in rows] == [5, 7, 7, 10, 20, 30]


@pytest.mark.parametrize("frame", ["garbage", "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW); SELECT 1; --"])
def test_aggregate_reference_rejects_invalid_frame(layer, frame):
    with pytest.raises(Exception):
        run(layer, aggregation="sum", frame=frame, execute=False)


@pytest.mark.parametrize("controls", [{"window": "7 days"}, {"grain_to_date": "month"}])
def test_aggregate_reference_rejects_conflicting_frame_controls(layer, controls):
    layer.graph.models["events"].metrics.append(
        Metric(
            name="windowed",
            type="cumulative",
            agg="sum",
            sql="daily_amount",
            window_frame="ROWS BETWEEN 1 PRECEDING AND CURRENT ROW",
            **controls,
        )
    )
    with pytest.raises(Exception, match="window_frame"):
        layer.compile(metrics=["events.windowed"], dimensions=["events.day"], user_attributes={"tenant": 1})


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


@pytest.fixture
def rust_layer(layer):
    if layer.engine != "rust":
        pytest.skip("Strict Rust compiler validation contract")
    return layer


@pytest.mark.parametrize("references", [["windowed"], ["second", "windowed"]])
def test_window_dependency_cycles_return_validation_errors(rust_layer, references):
    names = ["windowed", "second"]
    for name, reference in zip(names, references):
        rust_layer.graph.models["events"].metrics.append(
            Metric(
                name=name,
                type="cumulative",
                window_expression=f"SUM(base.{reference})",
            )
        )
    with pytest.raises(Exception, match="[Cc]ycl"):
        rust_layer.compile(metrics=["events.windowed"], dimensions=["events.day"], user_attributes={"tenant": 1})


@pytest.mark.parametrize("controls", [{"window": "7 days"}, {"grain_to_date": "month"}])
def test_window_expression_preserves_python_precedence(layer, controls):
    layer.graph.models["events"].metrics.append(
        Metric(
            name="windowed",
            type="cumulative",
            window_expression="SUM(base.daily_amount)",
            fill_nulls_with=0,
            **controls,
        )
    )
    cursor = layer.adapter.execute(
        layer.compile(
            metrics=["events.windowed"],
            dimensions=["events.day"],
            order_by=["events.day"],
            user_attributes={"tenant": 1},
        )
    )
    assert [row[-1] for row in cursor.fetchall()] == [15, 42, 82]


def test_graph_window_infers_unique_period_metric_owner(layer):
    layer.add_metric(Metric(name="graph_window", type="cumulative", window_expression="SUM(base.daily_amount)"))
    cursor = layer.adapter.execute(
        layer.compile(
            metrics=["graph_window"],
            dimensions=["events.day"],
            order_by=["events.day"],
            user_attributes={"tenant": 1},
        )
    )
    assert [column[0] for column in cursor.description] == ["day", "daily_amount", "graph_window"]
    assert [row[2] for row in cursor.fetchall()] == [15, 42, 82]


def test_quoted_output_expression_is_generated_for_bigquery(rust_layer):
    import sqlglot
    from sqlglot import exp

    # This is compilation and AST binding coverage, not live BigQuery execution.
    rust_layer.graph.models["events"].security = None
    sql = run(rust_layer, expression='AVG(base."daily_amount")', dialect="bigquery", execute=False)
    parsed = sqlglot.parse_one(sql, read="bigquery")
    window = next(parsed.find_all(exp.Window))
    columns = list(window.this.find_all(exp.Column))
    assert [(column.table, column.name) for column in columns] == [("base", "daily_amount")]
    assert not list(window.this.find_all(exp.Literal))
    assert 'base."daily_amount"' not in sql


@pytest.mark.parametrize("reference", ["missing", "daily_amount"])
def test_graph_window_rejects_missing_or_ambiguous_period_metric(rust_layer, reference):
    if reference == "daily_amount":
        rust_layer.add_model(
            Model(
                name="other",
                table="events",
                primary_key="id",
                metrics=[Metric(name="daily_amount", agg="sum", sql="amount")],
            )
        )
    rust_layer.add_metric(Metric(name="graph_window", type="cumulative", window_expression=f"SUM(base.{reference})"))
    with pytest.raises(Exception, match="possible definitions|[Aa]mbiguous"):
        rust_layer.compile(metrics=["graph_window"], dimensions=["events.day"], user_attributes={"tenant": 1})


@pytest.mark.parametrize("aggregation", ["count", "count_distinct"])
def test_cumulative_count_counts_nonnull_period_values(layer, aggregation):
    assert [row[2] for row in run(layer, aggregation=aggregation)] == [1, 2, 2, 1, 2, 3]


@pytest.mark.parametrize(
    "expression,expected",
    [
        ("SUM(base.daily_amount * 2) FILTER (WHERE base.daily_amount >= 7)", [None, 14, 14, 20, 60, 140]),
        ("COUNT(DISTINCT base.daily_amount)", [1, 2, 2, 1, 2, 3]),
        ("SUM(COALESCE(base.daily_amount, 1))", [5, 12, 13, 10, 30, 70]),
    ],
)
def test_window_expression_arithmetic_filter_and_null_arguments(layer, expression, expected):
    assert [row[2] for row in run(layer, expression=expression)] == expected


@pytest.mark.parametrize(
    "frame,expected",
    [
        ("ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING", [12, 7, None, 70, 60, 40]),
        ("ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW", [7, 5, 7, 20, 50, 20]),
    ],
)
def test_following_and_excluded_window_frames(layer, frame, expected):
    assert [row[2] for row in run(layer, frame=frame)] == expected


def test_window_expression_binds_multiple_grouped_dependencies(layer):
    layer.graph.models["events"].metrics.append(
        Metric(name="windowed", type="cumulative", window_expression="SUM(base.daily_amount + base.daily_people)")
    )
    cursor = layer.adapter.execute(
        layer.compile(
            metrics=["events.windowed"],
            dimensions=["events.day", "events.category"],
            order_by=["events.category", "events.day"],
            user_attributes={"tenant": 1},
        )
    )
    columns = [column[0] for column in cursor.description]
    assert set(columns) == {"day", "category", "daily_amount", "daily_people", "windowed"}
    assert [row[columns.index("windowed")] for row in cursor.fetchall()] == [7, 15, 15, 11, 32, 73]
