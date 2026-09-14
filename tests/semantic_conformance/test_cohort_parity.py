"""Cohort aggregation contracts over independently specified synthetic populations."""

from datetime import date, datetime

import pytest

from sidemantic import Dimension, Explore, Metric, Model, SecurityPolicy, SemanticLayer
from sidemantic.core.semantic_layer import SecurityError
from sidemantic.semantic_handoff import graph_to_semantic_input


@pytest.fixture(params=["python", "rust", "python_owned", "rust_owned"])
def layer(request):
    engine = request.param.split("_")[0]
    if engine == "rust":
        pytest.importorskip("sidemantic_rs", reason="Cohort acceptance requires the real extension")
    layer = SemanticLayer(engine=engine, fallback=False, auto_register=False)
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
    if request.param.endswith("_owned"):
        cohort = layer.graph.models["events"].metrics.pop(0)
        layer.graph.add_metric(cohort, model_name="events")
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


def test_source_anchored_explore_preserves_cohort_population(layer):
    layer.graph.add_explore(Explore(name="cohort", model="events"))
    assert result(layer, explore="cohort") == (["qualified"], [(3,)])


def cohort_metric(layer):
    return layer.graph.metrics.get("qualified") or layer.graph.models["events"].metrics[0]


def cohort_reference(layer):
    return "qualified" if "qualified" in layer.graph.metrics else "events.qualified"


def result(layer, **query):
    query.setdefault("metrics", [cohort_reference(layer)])
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
    cohort_metric(layer).agg = "count_distinct"
    assert result(layer) == (["qualified"], [(2,)])


def test_outer_sum_binds_inner_measure_alias(layer):
    metric = cohort_metric(layer)
    metric.agg = "sum"
    metric.sql = "amount"
    assert result(layer) == (["qualified"], [(103,)])


def test_having_uses_inner_alias_even_when_source_dimension_has_same_name(layer):
    cohort_metric(layer).having = "platforms >= 2 and amount > 20"
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
    cohort_metric(layer).filters = ["raw_amount >= 10"]
    assert result(layer) == (["qualified"], [(2,)])


def test_policy_prevents_cross_tenant_entity_qualification(layer):
    assert result(layer, user_attributes={"tenant": 2}) == (["qualified"], [(1,)])


def test_cohort_policy_requires_caller_context(layer):
    with pytest.raises(SecurityError):
        layer.compile(metrics=[cohort_reference(layer)])


def test_entity_dimensions_are_output_and_grouping_dimensions(layer):
    cohort_metric(layer).entity_dimensions = ["region"]
    assert result(layer, order_by=["events.region"]) == (["region", "qualified"], [("EU", 2), ("US", 1)])


def test_outer_expression_reads_cohort_columns_not_source_rows(layer):
    metric = cohort_metric(layer)
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
    metric = cohort_metric(layer)
    metric.agg = "sum"
    metric.sql = "amount"
    assert result(layer, filters=["events.platform = 'web'"]) == (["qualified"], [(None,)])


def test_or_predicate_cannot_bypass_metric_filter(layer):
    cohort_metric(layer).filters = ["platform = 'web'"]
    assert result(layer, filters=["raw_amount > 0 or events.region = 'absent'"]) == (["qualified"], [(0,)])


@pytest.mark.parametrize("layer", ["rust", "rust_owned"], indirect=True)
@pytest.mark.parametrize(
    "source", ["(select count(*) from events)", "sum(raw_amount) over ()", "count(*)", "other.raw_amount"]
)
def test_rust_inner_expression_cannot_change_source_scope(layer, source):
    cohort_metric(layer).inner_metrics[1]["sql"] = source
    with pytest.raises(ValueError):
        layer.compile(metrics=[cohort_reference(layer)], user_attributes={"tenant": 1})


@pytest.mark.parametrize("layer", ["rust", "rust_owned"], indirect=True)
@pytest.mark.parametrize("expression", ["hidden", "hidden + 1"])
def test_rust_declared_dimension_cannot_hide_a_subquery(layer, expression):
    model = layer.graph.models["events"]
    model.dimensions.append(Dimension(name="hidden", type="numeric", sql="(select count(*) from events)"))
    cohort_metric(layer).inner_metrics[1]["sql"] = expression
    with pytest.raises(ValueError):
        layer.compile(metrics=[cohort_reference(layer)], user_attributes={"tenant": 1})


@pytest.mark.parametrize("layer", ["rust", "rust_owned"], indirect=True)
def test_rust_row_filter_cannot_hide_a_subquery(layer):
    with pytest.raises(ValueError):
        layer.compile(
            metrics=[cohort_reference(layer)],
            filters=["(select count(*) from events) > 0"],
            user_attributes={"tenant": 1},
        )


def test_inner_source_dimension_arithmetic_preserves_precedence(layer):
    model = layer.graph.models["events"]
    model.dimensions.append(Dimension(name="augmented", sql="raw_amount + 1", type="numeric"))
    metric = cohort_metric(layer)
    metric.inner_metrics[1]["sql"] = "augmented * 2"
    metric.agg = "sum"
    metric.sql = "amount"
    assert result(layer) == (["qualified"], [(218,)])


@pytest.mark.parametrize("expression", ["amount", '"amount"', "events.amount"])
def test_inner_source_dimension_root_resolves_physical_expression(layer, expression):
    metric = cohort_metric(layer)
    metric.inner_metrics[1]["sql"] = expression
    metric.agg = "sum"
    metric.sql = "amount"
    assert result(layer) == (["qualified"], [(103,)])


@pytest.mark.parametrize("layer", ["rust", "rust_owned"], indirect=True)
@pytest.mark.parametrize("alias", ["AMOUNT", "USER_ID", "REGION"])
def test_rust_inner_alias_collisions_ignore_case(layer, alias):
    cohort_metric(layer).inner_metrics.append({"name": alias, "agg": "count"})
    with pytest.raises(ValueError, match="inner_alias_collision"):
        layer.compile(metrics=[cohort_reference(layer)], dimensions=["events.region"], user_attributes={"tenant": 1})


@pytest.mark.parametrize("layer", ["rust", "rust_owned"], indirect=True)
def test_rust_output_alias_collision_ignores_case(layer):
    layer.graph.models["events"].dimensions.append(Dimension(name="QUALIFIED", sql="region", type="categorical"))
    with pytest.raises(ValueError, match="output_alias_collision"):
        layer.compile(metrics=[cohort_reference(layer)], dimensions=["events.QUALIFIED"], user_attributes={"tenant": 1})


@pytest.mark.parametrize("layer", ["rust", "rust_owned"], indirect=True)
@pytest.mark.parametrize("context", ["having", "sql"])
@pytest.mark.parametrize("expression", ["SUM(amount)", "SUM(amount) OVER ()", "(SELECT amount)"])
def test_rust_cohort_result_context_requires_scalar_expression(layer, context, expression):
    metric = cohort_metric(layer)
    if context == "having":
        metric.having = f"{expression} > 20"
    else:
        metric.agg = "sum"
        metric.sql = expression
    with pytest.raises(ValueError, match="metric.cohort_result_non_row_expression"):
        layer.compile(metrics=[cohort_reference(layer)], user_attributes={"tenant": 1})


def test_invariants_restrict_inner_population_before_having(layer):
    layer.graph.models["events"].invariant_filters = ["id != 2"]
    assert result(layer) == (["qualified"], [(2,)])


@pytest.mark.parametrize("aggregation,expected", [("min", 3), ("max", 70), ("avg", 103 / 3)])
def test_outer_aggregations_read_qualified_inner_values(layer, aggregation, expected):
    metric = cohort_metric(layer)
    metric.agg = aggregation
    metric.sql = "amount"
    columns, rows = result(layer)
    assert columns == ["qualified"]
    assert len(rows) == 1
    assert rows[0][0] == pytest.approx(expected)


def test_compilation_preserves_cohort_source_and_owner(layer):
    before = graph_to_semantic_input(layer.graph)
    assert result(layer) == (["qualified"], [(3,)])
    assert graph_to_semantic_input(layer.graph) == before


@pytest.mark.parametrize("layer", ["python_owned", "rust_owned"], indirect=True)
def test_explicit_owner_wins_over_other_entity_and_metric_names(layer):
    layer.add_model(
        Model(
            name="unrelated",
            table="must_not_read",
            primary_key="id",
            dimensions=[Dimension(name="user_id", type="categorical")],
            metrics=[cohort_metric(layer).model_copy(deep=True)],
        )
    )
    assert result(layer) == (["qualified"], [(3,)])


@pytest.mark.parametrize("layer", ["python_owned", "rust_owned"], indirect=True)
def test_nonexistent_explicit_owner_does_not_fall_back_to_entity_match(layer):
    layer.graph.metric_owners["qualified"] = "missing"
    with pytest.raises((ValueError, KeyError)):
        result(layer)


@pytest.mark.parametrize("layer", ["rust_owned"], indirect=True)
@pytest.mark.parametrize(
    "mutation", ["unowned", "unknown_metric", "filled_ignored_offset", "wrapper", "joined_dimension"]
)
def test_owned_graph_cohort_unsupported_shapes_remain_gated(layer, mutation):
    query = {}
    if mutation == "unowned":
        layer.graph.metric_owners.clear()
    elif mutation == "unknown_metric":
        layer.graph.metric_owners["ghost"] = "events"
    elif mutation == "filled_ignored_offset":
        cohort_metric(layer).fill_nulls_with = 0
        cohort_metric(layer).time_offset = "1 day"
    elif mutation == "wrapper":
        layer.graph.add_metric(Metric(name="wrapped", type="derived", sql="qualified * 2"))
        query["metrics"] = ["wrapped"]
    else:
        layer.add_model(
            Model(
                name="other",
                table="must_not_read",
                primary_key="id",
                dimensions=[Dimension(name="region", type="categorical")],
            )
        )
        query["dimensions"] = ["other.region"]
    with pytest.raises(ValueError):
        result(layer, **query)


@pytest.mark.parametrize("layer", ["python_owned", "rust_owned"], indirect=True)
def test_owned_graph_cohort_visibility_is_enforced(layer):
    cohort_metric(layer).public = False
    layer.enforce_visibility = True
    with pytest.raises(SecurityError):
        result(layer)


@pytest.mark.parametrize("grouped", [False, True])
def test_cohort_default_fills_outer_empty_aggregate_without_creating_groups(layer, grouped):
    metric = cohort_metric(layer)
    metric.agg = "sum"
    metric.sql = "amount"
    metric.fill_nulls_with = -9
    metric.having = "platforms > 100"
    dimensions = ["events.region"] if grouped else []
    _, rows = result(layer, dimensions=dimensions)
    assert rows == ([] if grouped else [(-9,)])


def test_cohort_default_does_not_fill_inner_values_before_having(layer):
    metric = cohort_metric(layer)
    metric.agg = "avg"
    metric.sql = "amount"
    metric.having = "amount IS NULL"
    metric.fill_nulls_with = -9
    layer.adapter.execute("update events set raw_amount = null where tenant = 1")
    _, rows = result(layer)
    assert rows == [(-9,)]


def test_cohort_count_zero_is_not_replaced_by_default(layer):
    metric = cohort_metric(layer)
    metric.fill_nulls_with = -9
    metric.having = "platforms > 100"
    assert result(layer)[1] == [(0,)]


def test_selected_calculations_preserve_implicit_entity_dimensions(layer):
    from sidemantic.core.table_calculation import TableCalculation
    from sidemantic.sql.table_calc_processor import TableCalculationProcessor

    cohort_metric(layer).entity_dimensions = ["region"]
    calculations = [
        TableCalculation(name="sequence", type="row_number"),
        TableCalculation(name="running", type="running_total", field="qualified"),
    ]
    for calculation in calculations:
        layer.graph.add_table_calculation(calculation)
    query = {"order_by": ["events.region"]}
    columns, rows = result(layer, **query)
    assert columns == ["region", "qualified"]
    expected_rows, expected_columns = TableCalculationProcessor(calculations).process(rows, columns)
    assert result(layer, **query, table_calculations=[c.name for c in calculations]) == (
        expected_columns,
        expected_rows,
    )
    if layer.engine == "rust":
        import json

        import sidemantic_rs

        payload = {**query, "metrics": [cohort_reference(layer)], "table_calculations": [c.name for c in calculations]}
        assert (
            json.loads(
                sidemantic_rs.validate_with_semantic_input(
                    json.dumps(graph_to_semantic_input(layer.graph)), json.dumps(payload)
                )
            )
            == []
        )
