"""Approximate distinct preserves the DuckDB population and estimator."""

import pytest

from sidemantic import Dimension, Metric, Model, Relationship, SemanticLayer
from sidemantic.core.pre_aggregation import PreAggregation


@pytest.fixture(params=["python", "rust"])
def layer(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs", reason="Requires the real Rust extension")
    layer = SemanticLayer(engine=request.param, fallback=False, auto_register=False)
    layer.add_model(
        Model(
            name="events",
            table="approx_events",
            primary_key="id",
            dimensions=[Dimension(name="category", type="categorical"), Dimension(name="user_id", type="numeric")],
            metrics=[
                Metric(name="users", agg="approx_count_distinct", sql="user_id"),
                Metric(name="paid_users", agg="approx_count_distinct", sql="user_id", filters=["paid"]),
                Metric(name="no_users", agg="approx_count_distinct", sql="user_id", filters=["false"]),
                Metric(name="rows", agg="count"),
            ],
        )
    )
    layer.adapter.execute("""
        create table approx_events as
        select i as id, i % 10000 as user_id, 'large' as category, i % 3 = 0 as paid
        from range(30000) t(i);
        insert into approx_events values
            (30000, null, 'nulls', true), (30001, null, 'nulls', false),
            (30002, 1, 'small', true), (30003, 1, 'small', false), (30004, 2, 'small', false);
    """)
    try:
        yield layer
    finally:
        layer.adapter.close()


def test_estimator_and_filtered_populations(layer):
    query = dict(metrics=["events.users", "events.paid_users", "events.no_users"], dimensions=["events.category"])
    sql = layer.compile(**query)
    assert "APPROX_COUNT_DISTINCT" in sql.upper()
    assert "COUNT(DISTINCT" not in sql.upper()
    expected = layer.adapter.execute("""
        select category, approx_count_distinct(user_id),
            approx_count_distinct(case when paid then user_id end),
            approx_count_distinct(case when false then user_id end)
        from approx_events group by category order by category
    """).fetchall()
    actual = sorted(layer.adapter.execute(sql).fetchall())
    assert actual == expected
    assert expected[1] == ("nulls", 0, 0, 0)
    exact = layer.adapter.execute(
        "select count(distinct user_id) from approx_events where category = 'large'"
    ).fetchone()[0]
    assert expected[0][1] != exact, "Fixture must distinguish the approximate estimator from exact count"
    if layer.engine == "rust":
        assert layer.last_engine_selection["engine"] == "rust"


@pytest.mark.parametrize("filters", [["events.user_id < 200"], ["false"]])
@pytest.mark.parametrize("grouped", [False, True])
def test_query_filters_and_empty_populations(layer, filters, grouped):
    dimensions = ["events.category"] if grouped else []
    sql = layer.compile(metrics=["events.users", "events.rows"], dimensions=dimensions, filters=filters)
    where = filters[0].replace("events.", "")
    grouping = "category, " if grouped else ""
    group_by = " group by category" if grouped else ""
    expected = layer.adapter.execute(
        f"select {grouping}approx_count_distinct(user_id), count(*) from approx_events where {where}{group_by}"
    ).fetchall()
    assert sorted(layer.adapter.execute(sql).fetchall()) == sorted(expected)
    if not grouped and filters == ["false"]:
        assert expected == [(0, 0)]


def test_aggregate_filters_wrappers_and_stored_scalar_fallback(layer):
    expected = layer.adapter.execute("select approx_count_distinct(user_id) from approx_events").fetchone()[0]
    sql = layer.compile(metrics=["events.users", "events.rows"], filters=["events.users > 0"])
    assert layer.adapter.execute(sql).fetchall() == [(expected, 30005)]
    model = layer.graph.get_model("events")
    model.metrics.append(Metric(name="derived", type="derived", sql="users + 1"))
    sql = layer.compile(metrics=["events.derived"])
    assert layer.adapter.execute(sql).fetchall() == [(expected + 1,)]
    model.pre_aggregations.append(PreAggregation(name="stored", measures=["users"], dimensions=["category"]))
    sql = layer.compile(metrics=["events.users"], use_preaggregations=True)
    assert "APPROX_COUNT_DISTINCT" in sql.upper()
    assert layer.adapter.execute(sql).fetchall() == [(expected,)]


@pytest.mark.parametrize("relationship_type", ["many_to_one", "one_to_many"])
def test_joined_populations(layer, relationship_type):
    layer.adapter.execute("create table users as select i as id, 'region' as region from range(10000) t(i)")
    layer.add_model(
        Model(name="users", table="users", primary_key="id", dimensions=[Dimension(name="region", type="categorical")])
    )
    layer.graph.get_model("events").relationships.append(
        Relationship(name="users", type=relationship_type, sql="user_id", foreign_key="id")
    )
    sql = layer.compile(metrics=["events.users"], dimensions=["users.region"], filters=["events.user_id is not null"])
    expected = layer.adapter.execute("""
        select region, approx_count_distinct(user_id)
        from approx_events left join users on user_id = users.id where user_id is not null group by region
    """).fetchall()
    assert sorted(layer.adapter.execute(sql).fetchall(), key=str) == sorted(expected, key=str)


@pytest.mark.parametrize("window_expression", [None, "sum(base.users)"])
def test_cumulative_approximate_counts_use_grouped_outputs(layer, window_expression):
    layer.adapter.execute("alter table approx_events add column created_at date")
    layer.adapter.execute("update approx_events set created_at = date '2025-01-01' + cast(id // 10000 as integer)")
    model = layer.graph.get_model("events")
    model.dimensions.append(Dimension(name="created_at", type="time", granularity="day"))
    model.metrics.append(
        Metric(
            name="running_users",
            type="cumulative",
            window="7 days",
            sql="users" if window_expression is None else None,
            window_expression=window_expression,
        )
    )
    sql = layer.compile(metrics=["events.running_users"], dimensions=["events.created_at__day"])
    expected = layer.adapter.execute("""
        with daily as (
            select created_at, approx_count_distinct(user_id) as users
            from approx_events group by created_at
        )
        select sum(users) over (order by created_at rows between unbounded preceding and current row)
        from daily order by created_at
    """).fetchall()
    result = layer.adapter.execute(sql)
    index = [column[0] for column in result.description].index("running_users")
    assert [(row[index],) for row in sorted(result.fetchall())] == expected


APPROXIMATE_DIALECTS = [
    ("duckdb", "APPROX_COUNT_DISTINCT"),
    ("postgres", "APPROX_DISTINCT"),
    ("bigquery", "APPROX_COUNT_DISTINCT"),
    ("snowflake", "APPROX_COUNT_DISTINCT"),
    ("trino", "APPROX_DISTINCT"),
    ("spark", "APPROX_COUNT_DISTINCT"),
    ("databricks", "APPROX_COUNT_DISTINCT"),
    ("redshift", "APPROXIMATE COUNT"),
    ("clickhouse", "UNIQ"),
    ("mysql", "APPROX_DISTINCT"),
    ("sqlite", "APPROX_DISTINCT"),
]


@pytest.mark.parametrize("dialect,function", APPROXIMATE_DIALECTS)
@pytest.mark.parametrize(
    "shape",
    [
        "simple",
        "implicit",
        "fanout_implicit",
        "filtered",
        "wrapper",
        "fanout",
        "snapshot",
        "cumulative",
        "complete",
        "complete_filtered",
        "nested_complete",
        "window",
    ],
)
def test_approximate_dialect_rendering_and_population_results(layer, dialect, function, shape):
    import sqlglot

    if (
        layer.engine == "python"
        and shape in {"complete", "complete_filtered", "nested_complete"}
        and dialect == "redshift"
    ):
        pytest.skip("Python reparses completed Redshift aggregates with its generic SQL parser")
    model = layer.graph.get_model("events")
    metric = "events.users"
    dimensions = ["events.category"]
    expected_sql = "select approx_count_distinct(user_id) from approx_events group by category order by category"
    if shape == "implicit":
        model.metrics.append(Metric(name="implicit", agg="approx_count_distinct"))
        metric = "events.implicit"
        expected_sql = "select approx_count_distinct(id) from approx_events group by category order by category"
    elif shape == "filtered":
        metric = "events.paid_users"
        expected_sql = "select approx_count_distinct(case when paid then user_id end) from approx_events group by category order by category"
    elif shape == "wrapper":
        model.metrics.append(Metric(name="wrapped", type="derived", sql="users + 1"))
        metric = "events.wrapped"
        expected_sql = (
            "select approx_count_distinct(user_id) + 1 from approx_events group by category order by category"
        )
    elif shape in {"fanout", "fanout_implicit"}:
        layer.adapter.execute(
            "create table children as select id as event_id, 'joined' as label from approx_events cross join range(2)"
        )
        layer.add_model(
            Model(
                name="children",
                table="children",
                primary_key="event_id",
                dimensions=[Dimension(name="label", type="categorical")],
            )
        )
        model.relationships.append(Relationship(name="children", type="one_to_many", sql="id", foreign_key="event_id"))
        dimensions = ["children.label"]
        expected_sql = "select approx_count_distinct(user_id) from approx_events"
        if shape == "fanout_implicit":
            model.metrics.append(Metric(name="implicit", agg="approx_count_distinct"))
            metric = "events.implicit"
            expected_sql = "select count(*) from approx_events"
    elif shape in {"complete", "complete_filtered", "nested_complete"}:
        expression = "APPROX_COUNT_DISTINCT(user_id)"
        if shape == "nested_complete":
            expression = "ROUND(COALESCE(APPROX_COUNT_DISTINCT(user_id), 0), 0)"
        if shape == "complete_filtered":
            expression += " FILTER (WHERE paid)"
            expected_sql = "select approx_count_distinct(case when paid then user_id end) from approx_events group by category order by category"
        model.metrics.append(Metric(name="complete", type="derived", sql=expression, sql_is_complete=True))
        metric = "events.complete"
    elif shape in {"snapshot", "cumulative", "window"}:
        layer.adapter.execute("alter table approx_events add column created_at date")
        layer.adapter.execute("update approx_events set created_at = date '2025-01-01' + cast(id // 10000 as integer)")
        model.dimensions.append(Dimension(name="created_at", type="time", granularity="day"))
        if shape == "snapshot":
            model.metrics.append(
                Metric(name="latest", agg="approx_count_distinct", sql="user_id", non_additive_dimension="created_at")
            )
            metric = "events.latest"
            dimensions = []
            expected_sql = "select approx_count_distinct(user_id) from approx_events where created_at = (select max(created_at) from approx_events)"
        else:
            controls = (
                {"agg": "approx_count_distinct", "sql": "users"}
                if shape == "cumulative"
                else {"window_expression": "APPROX_COUNT_DISTINCT(base.users)"}
            )
            model.metrics.append(Metric(name="running", type="cumulative", **controls))
            metric = "events.running"
            dimensions = ["events.created_at__day"]
            expected_sql = "with daily as (select created_at, approx_count_distinct(user_id) as users from approx_events group by created_at) select approx_count_distinct(users) over (order by created_at rows between unbounded preceding and current row) from daily order by created_at"
    if layer.engine == "rust" and dialect in {"postgres", "mysql", "sqlite"}:
        with pytest.raises(Exception, match="metric.approx_count_distinct_output_dialect"):
            layer.compile(metrics=[metric], dimensions=dimensions, order_by=dimensions, dialect=dialect)
        return
    if layer.engine == "rust" and dialect == "redshift" and shape in {"cumulative", "window"}:
        with pytest.raises(Exception, match="metric.approx_count_distinct_window_redshift"):
            layer.compile(metrics=[metric], dimensions=dimensions, order_by=dimensions, dialect=dialect)
        return
    sql = layer.compile(metrics=[metric], dimensions=dimensions, order_by=dimensions, dialect=dialect)
    parsed = sqlglot.parse_one(sql, read=dialect)
    # Python's snapshot route leaves the aggregate name canonical until SQL
    # normalization; Rust must render the target name at construction time.
    emitted = sql if layer.engine == "rust" else parsed.sql(dialect=dialect)
    if shape != "fanout_implicit":
        assert function in emitted.upper(), sql
    # Execute target SQL translated to DuckDB to verify the population and SQL
    # expression. This does not qualify native warehouse function availability.
    actual = [row[-1] for row in layer.adapter.execute(parsed.sql(dialect="duckdb")).fetchall()]
    expected = [row[-1] for row in layer.adapter.execute(expected_sql).fetchall()]
    assert actual == expected


@pytest.mark.parametrize(
    "expression",
    [
        "ROUND(COALESCE(APPROX_COUNT_DISTINCT(user_id), 0), 0)",
        "GREATEST(APPROX_COUNT_DISTINCT(user_id), 0)",
        "NULLIF(APPROX_COUNT_DISTINCT(user_id), 0)",
    ],
)
def test_redshift_nested_approximate_expressions_use_independent_parser(layer, expression):
    import sqlglot
    from sqlglot import exp

    if layer.engine != "rust":
        pytest.skip("Rust target emission; Python reparses Redshift using its generic parser")
    layer.graph.get_model("events").metrics.append(
        Metric(name="nested_users", type="derived", sql=expression, sql_is_complete=True)
    )
    sql = layer.compile(
        metrics=["events.nested_users"],
        dimensions=["events.category"],
        order_by=["events.category"],
        dialect="redshift",
    )
    parsed = sqlglot.parse_one(sql, read="redshift")
    assert parsed.find(exp.ApproxDistinct) is not None
    actual = [row[-1] for row in layer.adapter.execute(parsed.sql(dialect="duckdb")).fetchall()]
    expected = [
        row[0]
        for row in layer.adapter.execute(
            f"select {expression} from approx_events group by category order by category"
        ).fetchall()
    ]
    assert actual == expected
