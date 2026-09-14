"""Approximate distinct preserves the DuckDB population and estimator."""

import pytest

from sidemantic import Dimension, Metric, Model, Relationship, SemanticLayer
from sidemantic.core.pre_aggregation import PreAggregation
from sidemantic.semantic_handoff import UnsupportedSemanticFeaturesError


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


def test_rust_rejects_unsupported_dialects(layer):
    if layer.engine != "rust":
        pytest.skip("Rust dialect qualification boundary")
    for dialect in ["postgres", "bigquery", "snowflake"]:
        with pytest.raises(UnsupportedSemanticFeaturesError, match="approx_count_distinct_dialect"):
            layer.compile(metrics=["events.users"], dialect=dialect)


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
