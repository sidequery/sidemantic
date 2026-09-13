"""Cohort aggregation contracts over independently specified synthetic populations."""

from datetime import date, datetime

import pytest

from sidemantic import Dimension, Metric, Model, SecurityPolicy, SemanticLayer
from sidemantic.core.semantic_layer import SecurityError


@pytest.fixture(params=["python", "rust"])
def layer(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs", reason="Cohort acceptance requires the real extension")
    layer = SemanticLayer(engine=request.param, fallback=False, auto_register=False)
    layer.add_model(
        Model(
            name="events",
            table="events",
            primary_key="id",
            dimensions=[
                Dimension(name="user_id", type="categorical"),
                Dimension(name="platform", type="categorical"),
                Dimension(name="region", type="categorical"),
                Dimension(name="day", type="time", granularity="day"),
                Dimension(name="amount", sql="raw_amount", type="numeric"),
            ],
            metrics=[
                Metric(
                    name="qualified",
                    type="cohort",
                    entity="user_id",
                    agg="count",
                    inner_metrics=[
                        {"name": "platforms", "agg": "count_distinct", "sql": "platform"},
                        {"name": "amount", "agg": "sum", "sql": "raw_amount"},
                    ],
                    having="platforms >= 2",
                )
            ],
            security=SecurityPolicy(row_filters=["tenant = {{ user.tenant }}"]),
        )
    )
    layer.adapter.execute("""
        create table events(id integer, user_id varchar, platform varchar, region varchar, day date, raw_amount integer, tenant integer);
        insert into events values
          (1,'u1','web','US','2024-01-01',10,1), (2,'u1','mobile','US','2024-01-02',20,1),
          (3,'u2','web','US','2024-01-01',5,1), (4,'u2','web','US','2024-01-02',7,1),
          (5,'u3','mobile','EU','2024-02-01',30,1), (6,'u3','web','EU','2024-02-02',40,1),
          (7,null,'web','EU','2024-02-01',1,1), (8,null,'mobile','EU','2024-02-02',2,1),
          (9,'u4','web','US','2024-01-01',50,1), (10,'u4','mobile','US','2024-01-02',60,2),
          (11,'u9','web','US','2024-01-01',100,2), (12,'u9','mobile','US','2024-01-02',100,2);
    """)
    try:
        yield layer
    finally:
        layer.adapter.close()


def result(layer, **query):
    query.setdefault("metrics", ["events.qualified"])
    query.setdefault("user_attributes", {"tenant": 1})
    sql = layer.compile(**query)
    if layer.engine == "rust":
        assert layer.last_engine_selection["engine"] == "rust"
    cursor = layer.adapter.execute(sql)
    return [field[0] for field in cursor.description], [
        tuple(value.isoformat()[:10] if isinstance(value, (date, datetime)) else value for value in row)
        for row in cursor.fetchall()
    ]


def test_count_counts_qualified_inner_groups_including_null_entity(layer):
    assert result(layer) == (["qualified"], [(3,)])


def test_distinct_entity_count_excludes_null_entity(layer):
    layer.graph.models["events"].metrics[0].agg = "count_distinct"
    assert result(layer) == (["qualified"], [(2,)])


def test_outer_sum_binds_inner_measure_alias(layer):
    metric = layer.graph.models["events"].metrics[0]
    metric.agg = "sum"
    metric.sql = "amount"
    assert result(layer) == (["qualified"], [(103,)])


def test_having_uses_inner_alias_even_when_source_dimension_has_same_name(layer):
    layer.graph.models["events"].metrics[0].having = "platforms >= 2 and amount > 20"
    assert result(layer) == (["qualified"], [(2,)])


def test_query_grouping_changes_inner_and_outer_grain(layer):
    assert result(layer, dimensions=["events.region"], order_by=["events.region"]) == (
        ["region", "qualified"],
        [("EU", 2), ("US", 1)],
    )


def test_time_bucket_is_carried_through_to_outer_output(layer):
    assert result(layer, dimensions=["events.day__month"], order_by=["events.day__month"]) == (
        ["day__month", "qualified"],
        [("2024-01-01", 1), ("2024-02-01", 2)],
    )


def test_filter_applies_before_inner_distinct_aggregation(layer):
    assert result(layer, filters=["events.platform = 'web'"]) == (["qualified"], [(0,)])


def test_metric_filter_applies_before_inner_distinct_aggregation(layer):
    layer.graph.models["events"].metrics[0].filters = ["raw_amount >= 10"]
    assert result(layer) == (["qualified"], [(2,)])


def test_policy_prevents_cross_tenant_entity_qualification(layer):
    assert result(layer, user_attributes={"tenant": 2}) == (["qualified"], [(1,)])


def test_cohort_policy_requires_caller_context(layer):
    with pytest.raises(SecurityError):
        layer.compile(metrics=["events.qualified"])


def test_entity_dimensions_are_output_and_grouping_dimensions(layer):
    layer.graph.models["events"].metrics[0].entity_dimensions = ["region"]
    assert result(layer, order_by=["events.region"]) == (["region", "qualified"], [("EU", 2), ("US", 1)])


def test_outer_expression_reads_cohort_columns_not_source_rows(layer):
    metric = layer.graph.models["events"].metrics[0]
    metric.agg = "sum"
    metric.sql = "{model}.amount * 2"
    assert result(layer) == (["qualified"], [(206,)])


def test_entity_can_qualify_globally_but_not_within_each_region(layer):
    layer.adapter.execute("update events set region = 'EU' where id = 2")
    assert result(layer) == (["qualified"], [(3,)])
    assert result(layer, dimensions=["events.region"], order_by=["events.region"]) == (
        ["region", "qualified"],
        [("EU", 2)],
    )


def test_reserved_output_dimension_is_quoted(layer):
    layer.graph.models["events"].dimensions.append(Dimension(name="group", sql="region", type="categorical"))
    assert result(layer, dimensions=["events.group"], order_by=["events.group"]) == (
        ["group", "qualified"],
        [("EU", 2), ("US", 1)],
    )


def test_empty_cohort_outer_sum_is_null(layer):
    metric = layer.graph.models["events"].metrics[0]
    metric.agg = "sum"
    metric.sql = "amount"
    assert result(layer, filters=["events.platform = 'web'"]) == (["qualified"], [(None,)])


def test_or_predicate_cannot_bypass_metric_filter(layer):
    layer.graph.models["events"].metrics[0].filters = ["platform = 'web'"]
    assert result(layer, filters=["raw_amount > 0 or events.region = 'absent'"]) == (["qualified"], [(0,)])


@pytest.mark.parametrize("layer", ["rust"], indirect=True)
@pytest.mark.parametrize(
    "source", ["(select count(*) from events)", "sum(raw_amount) over ()", "count(*)", "other.raw_amount"]
)
def test_rust_inner_expression_cannot_change_source_scope(layer, source):
    layer.graph.models["events"].metrics[0].inner_metrics[1]["sql"] = source
    with pytest.raises(ValueError):
        layer.compile(metrics=["events.qualified"], user_attributes={"tenant": 1})


@pytest.mark.parametrize("layer", ["rust"], indirect=True)
@pytest.mark.parametrize("expression", ["hidden", "hidden + 1"])
def test_rust_declared_dimension_cannot_hide_a_subquery(layer, expression):
    model = layer.graph.models["events"]
    model.dimensions.append(Dimension(name="hidden", type="numeric", sql="(select count(*) from events)"))
    model.metrics[0].inner_metrics[1]["sql"] = expression
    with pytest.raises(ValueError):
        layer.compile(metrics=["events.qualified"], user_attributes={"tenant": 1})


@pytest.mark.parametrize("layer", ["rust"], indirect=True)
def test_rust_row_filter_cannot_hide_a_subquery(layer):
    with pytest.raises(ValueError):
        layer.compile(
            metrics=["events.qualified"], filters=["(select count(*) from events) > 0"], user_attributes={"tenant": 1}
        )


def test_inner_source_dimension_arithmetic_preserves_precedence(layer):
    model = layer.graph.models["events"]
    model.dimensions.append(Dimension(name="augmented", sql="raw_amount + 1", type="numeric"))
    metric = model.metrics[0]
    metric.inner_metrics[1]["sql"] = "augmented * 2"
    metric.agg = "sum"
    metric.sql = "amount"
    assert result(layer) == (["qualified"], [(218,)])


@pytest.mark.parametrize("layer", ["rust"], indirect=True)
@pytest.mark.parametrize("alias", ["AMOUNT", "USER_ID", "REGION"])
def test_rust_inner_alias_collisions_ignore_case(layer, alias):
    model = layer.graph.models["events"]
    model.metrics[0].inner_metrics.append({"name": alias, "agg": "count"})
    with pytest.raises(ValueError, match="inner_alias_collision"):
        layer.compile(metrics=["events.qualified"], dimensions=["events.region"], user_attributes={"tenant": 1})


@pytest.mark.parametrize("layer", ["rust"], indirect=True)
def test_rust_output_alias_collision_ignores_case(layer):
    layer.graph.models["events"].dimensions.append(Dimension(name="QUALIFIED", sql="region", type="categorical"))
    with pytest.raises(ValueError, match="output_alias_collision"):
        layer.compile(metrics=["events.qualified"], dimensions=["events.QUALIFIED"], user_attributes={"tenant": 1})
