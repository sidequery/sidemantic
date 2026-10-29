"""Cross-model cumulative filters retain their declared row population."""

from copy import deepcopy
from datetime import date

import pytest

from sidemantic import Dimension, Metric, Model, Relationship, SecurityPolicy


@pytest.fixture
def cross_model_layer(layer):
    layer.add_model(
        Model(
            name="facts",
            primary_key="id",
            sql="""SELECT * FROM (VALUES
                (1, DATE '2024-01-01', 1, 'b', DATE '2023-12-01', NULL),
                (2, DATE '2024-01-02', 2, 'a', DATE '2023-12-01', NULL),
                (3, DATE '2024-01-02', 2, 'a', DATE '2023-12-01', NULL),
                (4, DATE '2024-01-03', 3, 'b', DATE '2023-12-01', NULL),
                (5, DATE '2024-01-01', 4, 'a', DATE '2023-12-02', 'x'),
                (6, DATE '2024-01-02', 5, 'a', DATE '2023-12-02', 'x')
            ) t(id, day, other_id, owner_category, cohort_day, label)""",
            dimensions=[
                Dimension(name="day", type="time", granularity="day"),
                Dimension(name="cohort_day", type="time", granularity="day"),
                Dimension(name="category", type="categorical", sql="owner_category"),
                Dimension(name="label", type="categorical"),
            ],
            relationships=[Relationship(name="other", type="many_to_one", foreign_key="other_id")],
        )
    )
    layer.add_model(
        Model(
            name="other",
            primary_key="id",
            sql="""SELECT * FROM (VALUES
                (1, 'a', 100), (2, 'b', 5), (3, 'a', 200), (4, 'b', 10), (5, 'b', 20)
            ) t(id, category, amount)""",
            dimensions=[Dimension(name="category", type="categorical")],
            metrics=[
                Metric(name="revenue", agg="sum", sql="amount"),
                Metric(name="n", agg="count"),
                Metric(name="distinct_n", sql="COUNT(DISTINCT id)", sql_is_complete=True),
                Metric(name="plus_one", type="derived", sql="n + 1"),
                Metric(name="average", type="ratio", numerator="revenue", denominator="n"),
            ],
        )
    )
    return layer


def add_window(layer, base, predicate, *, graph_scope=False):
    metric = Metric(name="running", type="cumulative", sql=f"other.{base}", filters=[predicate])
    if graph_scope:
        layer.graph.add_metric(metric, model_name="facts")
        return "running"
    layer.graph.models["facts"].metrics.append(metric)
    return "facts.running"


def rows(layer, reference, **kwargs):
    sql = layer.compile(
        metrics=["other.revenue", reference],
        dimensions=["facts.day", "facts.cohort_day", "facts.label"],
        order_by=["facts.cohort_day", "facts.day"],
        **kwargs,
    )
    return layer.adapter.execute(sql).fetchall()


@pytest.mark.parametrize("predicate", ["category = 'a'", "facts.category = 'a'", "{model}.category = 'a'"])
@pytest.mark.parametrize("graph_scope", [False, True])
def test_foreign_base_retains_filter_owner_and_nullable_group_keys(cross_model_layer, predicate, graph_scope):
    layer = cross_model_layer
    reference = add_window(layer, "revenue", predicate, graph_scope=graph_scope)
    before = deepcopy(layer.graph.models)
    result = rows(layer, reference)
    assert [row[-2:] for row in result] == [(100, None), (5, 5), (200, 5), (10, 10), (20, 30)]
    assert [row[:3] for row in result] == [
        (date(2024, 1, 1), date(2023, 12, 1), None),
        (date(2024, 1, 2), date(2023, 12, 1), None),
        (date(2024, 1, 3), date(2023, 12, 1), None),
        (date(2024, 1, 1), date(2023, 12, 2), "x"),
        (date(2024, 1, 2), date(2023, 12, 2), "x"),
    ]
    assert layer.graph.models == before


@pytest.mark.parametrize(
    "base,expected",
    [
        ("n", [0, 1, 1, 1, 2]),
        ("distinct_n", [0, 1, 1, 1, 2]),
        ("plus_one", [1, 3, 4, 2, 4]),
        ("average", [None, 5, 5, 10, 30]),
    ],
)
def test_empty_foreign_populations_and_derived_inputs(cross_model_layer, base, expected):
    layer = cross_model_layer
    reference = add_window(layer, base, "category = 'a'")
    assert [row[-1] for row in rows(layer, reference)] == expected


def test_foreign_input_own_filters_and_caller_aliases(cross_model_layer):
    layer = cross_model_layer
    layer.graph.models["other"].get_metric("revenue").filters = ["amount > 5"]
    reference = add_window(layer, "revenue", "category = 'a'")
    result = rows(layer, reference, aliases={reference: "__sidemantic_cumulative_0"})
    assert [row[-2:] for row in result] == [(100, None), (None, None), (200, None), (10, 10), (20, 30)]


def test_foreign_input_uses_security_on_both_models(cross_model_layer):
    layer = cross_model_layer
    layer.graph.models["facts"].security = SecurityPolicy(row_filters=["id <> 5"])
    layer.graph.models["other"].security = SecurityPolicy(row_filters=["amount <= 100"])
    reference = add_window(layer, "revenue", "category = 'a'")
    result = rows(layer, reference, user_attributes={})
    assert [row[-2:] for row in result] == [(100, None), (5, 5), (20, 20)]


def test_cross_model_filter_matches_no_rows_without_erasing_domain(cross_model_layer):
    layer = cross_model_layer
    reference = add_window(layer, "n", "category = 'missing'")
    result = rows(layer, reference)
    assert [row[-2:] for row in result] == [(100, 0), (5, 0), (200, 0), (10, 0), (20, 0)]


@pytest.mark.parametrize("base,expected", [("revenue", [None, 5, 5, 10, 30]), ("n", [0, 1, 1, 1, 2])])
def test_cross_model_window_expression_preserves_population(cross_model_layer, base, expected):
    layer = cross_model_layer
    layer.graph.models["facts"].metrics.append(
        Metric(
            name="running",
            type="cumulative",
            window_expression=f'SUM(base."other.{base}")',
            filters=["category = 'a'"],
        )
    )
    assert [row[-1] for row in rows(layer, "facts.running")] == expected


@pytest.mark.parametrize("base", ["n", "distinct_n"])
def test_empty_count_population_precedes_fill_default(cross_model_layer, base):
    layer = cross_model_layer
    layer.graph.models["other"].get_metric(base).fill_nulls_with = 99
    reference = add_window(layer, base, "category = 'a'")
    assert [row[-1] for row in rows(layer, reference)] == [0, 1, 1, 1, 2]


@pytest.mark.parametrize("base", ["average", "derived_average", "nested_average"])
def test_filtered_computed_inputs_reapply_fill_defaults(cross_model_layer, base):
    layer = cross_model_layer
    model = layer.graph.models["other"]
    model.get_metric("average").fill_nulls_with = 99
    model.metrics.extend(
        [
            Metric(name="derived_average", type="derived", sql="revenue / NULLIF(n, 0)", fill_nulls_with=99),
            Metric(name="nested_average", type="derived", sql="average * 2"),
        ]
    )
    reference = add_window(layer, base, "category = 'a'")
    factor = 2 if base == "nested_average" else 1
    assert [row[-1] for row in rows(layer, reference)] == [value * factor for value in [99, 104, 203, 10, 30]]


@pytest.mark.parametrize("complete", [False, True])
@pytest.mark.parametrize(
    "expression,expected",
    [
        ("COUNT(*) FILTER (WHERE amount > 0)", [0, 1, 1, 1, 2]),
        ("COUNT(*) + 1", [1, 3, 4, 2, 4]),
        ("COALESCE(SUM(amount), 0) + COUNT(*)", [0, 6, 6, 11, 32]),
        ("SUM(amount) + COUNT(*)", [None, 6, 6, 11, 32]),
        ("NULLIF(COUNT(*), 1)", [0, 0, 0, None, None]),
    ],
)
def test_complete_foreign_aggregates_keep_empty_and_populated_null_semantics(
    cross_model_layer, complete, expression, expected
):
    layer = cross_model_layer
    layer.graph.models["other"].metrics.append(Metric(name="custom", sql=expression, sql_is_complete=complete))
    reference = add_window(layer, "custom", "category = 'a'")
    assert [row[-1] for row in rows(layer, reference)] == expected


def test_populated_null_is_not_replaced_by_empty_aggregate_value(cross_model_layer):
    layer = cross_model_layer
    layer.graph.models["other"].metrics.append(Metric(name="custom", sql="NULLIF(COUNT(*), 1)", sql_is_complete=True))
    reference = add_window(layer, "custom", "category = 'a'")
    layer.graph.models["facts"].get_metric("running").agg = "count"
    assert [row[-1] for row in rows(layer, reference)] == [1, 1, 2, 0, 0]


def test_filter_owner_placeholder_does_not_rewrite_string_literals(cross_model_layer):
    layer = cross_model_layer
    model = layer.graph.models["facts"]
    model.sql = model.sql.replace("'a'", "'{model}'")
    reference = add_window(layer, "revenue", "{model}.category = '{model}'")
    assert [row[-1] for row in rows(layer, reference)] == [None, 5, 5, 10, 30]
