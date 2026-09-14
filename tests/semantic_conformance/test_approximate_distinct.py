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


def test_rust_rejects_unqualified_shapes(layer):
    if layer.engine != "rust":
        pytest.skip("Rust qualification boundary")
    for dialect in ["postgres", "bigquery", "snowflake"]:
        with pytest.raises(UnsupportedSemanticFeaturesError, match="approx_count_distinct_dialect"):
            layer.compile(metrics=["events.users"], dialect=dialect)
    with pytest.raises(UnsupportedSemanticFeaturesError, match="approx_count_distinct_query_shape"):
        layer.compile(metrics=["events.users"], ungrouped=True)
    with pytest.raises(UnsupportedSemanticFeaturesError, match="approx_count_distinct_metric_filter"):
        layer.compile(metrics=["events.rows"], filters=["events.users > 0"])
    model = layer.graph.get_model("events")
    model.metrics.append(Metric(name="derived", type="derived", sql="users + 1"))
    with pytest.raises(UnsupportedSemanticFeaturesError, match="approx_count_distinct_calculation_shape"):
        layer.compile(metrics=["events.derived"])
    model.pre_aggregations.append(PreAggregation(name="stored", measures=["users"], dimensions=["category"]))
    with pytest.raises(UnsupportedSemanticFeaturesError, match="approx_count_distinct_preaggregation"):
        layer.compile(metrics=["events.users"], use_preaggregations=True)
    # Raw-source execution is still available even with a stored scalar count.
    sql = layer.compile(metrics=["events.users"], use_preaggregations=False)
    assert "APPROX_COUNT_DISTINCT" in sql.upper()


@pytest.mark.parametrize("relationship_type", ["many_to_one", "one_to_many"])
def test_rust_rejects_joined_populations(layer, relationship_type):
    if layer.engine != "rust":
        pytest.skip("Rust qualification boundary")
    layer.add_model(
        Model(name="users", table="users", primary_key="id", dimensions=[Dimension(name="region", type="categorical")])
    )
    layer.graph.get_model("events").relationships.append(
        Relationship(name="users", type=relationship_type, foreign_key="user_id")
    )
    with pytest.raises(UnsupportedSemanticFeaturesError, match="approx_count_distinct_join"):
        layer.compile(metrics=["events.users"], dimensions=["users.region"])
