"""Actual rollup builds and routed execution with independent population contracts."""

from datetime import date, datetime

import pytest

from sidemantic import Dimension, Metric, Model, PreAggregation, SecurityPolicy, SemanticLayer
from sidemantic.core.semantic_layer import SecurityError
from sidemantic.rust_bridge import generate_preaggregation_materialization_sql_with_rust


@pytest.fixture
def layer():
    pytest.importorskip("sidemantic_rs", reason="Rollup acceptance requires the matching Rust extension")
    layer = SemanticLayer(engine="rust", fallback=False, auto_register=False)
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[
                Dimension(name="status", type="categorical"),
                Dimension(name="tenant", type="categorical"),
                Dimension(name="created", type="time"),
            ],
            metrics=[
                Metric(name="revenue", agg="sum", sql="amount"),
                Metric(name="count", agg="count"),
                Metric(name="minimum", agg="min", sql="amount"),
                Metric(name="maximum", agg="max", sql="amount"),
                Metric(name="average", agg="avg", sql="amount"),
                Metric(name="median", agg="median", sql="amount"),
                Metric(name="paid", agg="sum", sql="amount", filters=["status = 'paid'"]),
            ],
            pre_aggregations=[
                PreAggregation(
                    name="daily",
                    measures=["revenue", "count", "minimum", "maximum"],
                    dimensions=["status"],
                    time_dimension="created",
                    granularity="day",
                )
            ],
        )
    )
    layer.adapter.execute("""
        create table orders(id integer, status varchar, tenant integer, created timestamp, amount integer);
        insert into orders values
          (1, 'paid', 1, '2026-01-01 01:00:00', 10),
          (2, 'paid', 1, '2026-01-01 02:00:00', 20),
          (3, 'paid', 2, '2026-01-02 01:00:00', 900),
          (4, 'open', 1, '2026-01-02 01:00:00', 40),
          (5, 'open', 1, '2026-02-01 01:00:00', null);
    """)
    try:
        yield layer
    finally:
        layer.adapter.close()


def materialize(layer, *, rust=False):
    model = layer.graph.models["orders"]
    rollup = model.pre_aggregations[0]
    sql = (
        generate_preaggregation_materialization_sql_with_rust(model, rollup)
        if rust
        else rollup.generate_materialization_sql(model)
    )
    layer.adapter.execute(f"create table orders_preagg_daily as {sql}")


def assert_result(layer, query, rows, *, routed):
    raw = layer.compile(**query, use_preaggregations=False)
    compiled = layer.compile(**query, use_preaggregations=True)
    assert ("used_preagg=true" in compiled) is routed, compiled
    assert ("orders_preagg_daily" in compiled) is routed, compiled
    raw_result = layer.adapter.execute(raw)
    raw_columns = [column[0] for column in raw_result.description]
    assert raw_result.fetchall() == rows
    result = layer.adapter.execute(compiled)
    assert [column[0] for column in result.description] == raw_columns
    assert result.fetchall() == rows


@pytest.mark.parametrize("rust_build", [False, True], ids=["python-build", "rust-build"])
@pytest.mark.parametrize(
    "query,rows",
    [
        ({"metrics": ["orders.revenue", "orders.count", "orders.minimum", "orders.maximum"]}, [(970, 5, 10, 900)]),
        (
            {"metrics": ["orders.revenue"], "dimensions": ["orders.status"], "order_by": ["orders.status"]},
            [("open", 40), ("paid", 930)],
        ),
        (
            {
                "metrics": ["orders.revenue"],
                "dimensions": ["orders.created__month"],
                "order_by": ["orders.created__month"],
            },
            [(date(2026, 1, 1), 970), (date(2026, 2, 1), None)],
        ),
        ({"metrics": ["orders.revenue"], "filters": ["orders.status in ('paid')"]}, [(930,)]),
    ],
)
def test_compatible_rollup_executes(layer, rust_build, query, rows):
    materialize(layer, rust=rust_build)
    assert_result(layer, query, rows, routed=True)


@pytest.mark.parametrize(
    "query,rows",
    [
        ({"metrics": ["orders.average"]}, [(242.5,)]),
        ({"metrics": ["orders.median"]}, [(30.0,)]),
        ({"metrics": ["orders.paid"]}, [(930,)]),
        ({"metrics": ["orders.revenue"], "filters": ["upper(orders.status) = 'OPEN'"]}, [(40,)]),
        ({"metrics": ["orders.revenue"], "filters": ["orders.tenant in (1)"]}, [(70,)]),
        ({"metrics": ["orders.revenue"], "filters": ["orders.tenant between 1 and 1"]}, [(70,)]),
        ({"metrics": ["orders.revenue"], "filters": ["orders.tenant is null"]}, [(None,)]),
        ({"metrics": ["orders.revenue"], "filters": ["orders.created >= '2026-01-02'"]}, [(940,)]),
    ],
)
def test_incompatible_rollup_uses_raw_source(layer, query, rows):
    materialize(layer)
    assert_result(layer, query, rows, routed=False)


@pytest.mark.parametrize("restriction", ["invariant", "policy", "both"])
def test_mandatory_restrictions_bypass_unscoped_rollup(layer, restriction):
    materialize(layer)  # Deliberately contains tenant 2's 900; restrictions come later.
    model = layer.graph.models["orders"]
    query = {"metrics": ["orders.revenue"]}
    if restriction in {"invariant", "both"}:
        model.invariant_filters = ["tenant = 1"]
    if restriction in {"policy", "both"}:
        model.security = SecurityPolicy(row_filters=["tenant = {{ user.tenant }}"])
        query["user_attributes"] = {"tenant": 1}
    assert_result(layer, query, [(70,)], routed=False)


def test_invariant_materialization_keeps_restricted_population(layer):
    layer.graph.models["orders"].invariant_filters = ["tenant = 1"]
    materialize(layer)
    assert layer.adapter.execute("select sum(revenue_raw) from orders_preagg_daily").fetchall() == [(70,)]
    assert_result(layer, {"metrics": ["orders.revenue"]}, [(70,)], routed=False)


@pytest.mark.parametrize("policy,attributes", [(SecurityPolicy(access=False), {}), (SecurityPolicy(), None)])
def test_access_denied_before_rollup_access(layer, policy, attributes):
    layer.graph.models["orders"].security = policy
    with pytest.raises(SecurityError):
        layer.compile(metrics=["orders.revenue"], use_preaggregations=True, user_attributes=attributes)


def test_filtered_materialization_preserves_null_and_count_populations(layer):
    model = layer.graph.models["orders"]
    model.metrics = [
        Metric(name="revenue", agg="sum", sql="amount", filters=["status = 'open'"]),
        Metric(name="rows", agg="count", filters=["status = 'open'"]),
        Metric(name="values", agg="count", sql="amount", filters=["status = 'open'"]),
        Metric(name="minimum", agg="min", sql="amount", filters=["status = 'open'"]),
        Metric(name="maximum", agg="max", sql="amount", filters=["status = 'open'"]),
    ]
    model.pre_aggregations = [PreAggregation(name="daily", measures=[metric.name for metric in model.metrics])]
    materialize(layer, rust=True)
    assert layer.adapter.execute("select * from orders_preagg_daily").fetchall() == [(40, 2, 1, 40, 40)]


@pytest.mark.parametrize("dimension", ["orders.created", "orders.created__hour"])
def test_bucket_rollup_cannot_restore_finer_timestamps(layer, dimension):
    materialize(layer)
    assert_result(
        layer,
        {"metrics": ["orders.revenue"], "dimensions": [dimension], "order_by": [dimension]},
        [
            (datetime(2026, 1, 1, 1), 10),
            (datetime(2026, 1, 1, 2), 20),
            (datetime(2026, 1, 2, 1), 940),
            (datetime(2026, 2, 1, 1), None),
        ],
        routed=False,
    )


def test_week_rollup_cannot_serve_calendar_month(layer):
    layer.graph.models["orders"].pre_aggregations[0].granularity = "week"
    materialize(layer)
    assert_result(
        layer,
        {"metrics": ["orders.revenue"], "dimensions": ["orders.created__month"], "order_by": ["orders.created__month"]},
        [(date(2026, 1, 1), 970), (date(2026, 2, 1), None)],
        routed=False,
    )


def test_empty_rollup_retains_zero_count(layer):
    layer.adapter.execute("delete from orders")
    materialize(layer)
    assert_result(layer, {"metrics": ["orders.revenue", "orders.count"]}, [(None, 0)], routed=True)


def test_declared_grain_applies_to_rollup_raw_time_dimension(layer):
    model = layer.graph.models["orders"]
    model.get_dimension("created").granularity = "day"
    model.pre_aggregations = [PreAggregation(name="daily", measures=["revenue"], dimensions=["created"])]
    materialize(layer)
    raw = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.created"],
        order_by=["orders.created"],
        use_preaggregations=False,
    )
    routed = layer.compile(
        metrics=["orders.revenue"], dimensions=["orders.created"], order_by=["orders.created"], use_preaggregations=True
    )
    assert "used_preagg=true" in routed
    raw_rows = layer.adapter.execute(raw).fetchall()
    rows = layer.adapter.execute(routed).fetchall()
    assert rows == raw_rows
    assert [(str(row[0])[:10], row[1]) for row in rows] == [
        ("2026-01-01", 30),
        ("2026-01-02", 940),
        ("2026-02-01", None),
    ]


@pytest.mark.parametrize("rust_build", [False, True])
def test_colliding_state_and_dimension_aliases_cannot_materialize_or_route(layer, rust_build):
    model = layer.graph.models["orders"]
    model.dimensions.append(Dimension(name="revenue_raw", type="numeric", sql="amount"))
    model.pre_aggregations = [PreAggregation(name="daily", measures=["revenue"], dimensions=["revenue_raw"])]
    import sidemantic_rs

    error = sidemantic_rs.UnsupportedSemanticFeaturesError if rust_build else ValueError
    with pytest.raises(error, match="colliding|output_alias_collision"):
        materialize(layer, rust=rust_build)
    # Poison an external table with the colliding dimension value; it must not
    # become aggregate state even if someone created it outside either builder.
    layer.adapter.execute("create table orders_preagg_daily as select 99999 as revenue_raw")
    assert_result(layer, {"metrics": ["orders.revenue"]}, [(970,)], routed=False)
