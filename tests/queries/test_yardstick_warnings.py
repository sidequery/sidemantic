"""Context-loss warnings and modifier precedence from upstream Yardstick."""

import warnings

import pytest

from sidemantic import Dimension, Metric, Model, SemanticLayer
from sidemantic.sql.query_rewriter import QueryRewriter, YardstickBindingError, YardstickWarning


@pytest.fixture
def warning_layer():
    layer = SemanticLayer()
    layer.conn.execute("create table sales(year int, region varchar, amount double)")
    layer.conn.execute("insert into sales values (2022,'US',100),(2022,'EU',50),(2023,'US',150),(2023,'EU',75)")
    layer.add_model(
        Model(
            name="sales_v",
            table="sales",
            metadata={"yardstick": {}},
            dimensions=[Dimension(name="year", type="numeric"), Dimension(name="region", type="categorical")],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        )
    )
    yield layer
    layer.conn.close()


def query_with(modifiers, predicate="year = 2023"):
    return (
        f"select region, aggregate(revenue) {modifiers} as revenue from sales_v "
        f"where {predicate} group by region order by region"
    )


@pytest.mark.parametrize(
    "modifiers",
    [
        "at (all region)",
        "at (all)",
        "at (all) at (visible)",
        "at (where year=2023) at (all)",
        "at (all) at (where year=2023)",
        "at (all) at (set year=2023)",
        "at (set year=2023) at (all year)",
        "at (all region) at (where region='US') at (visible)",
        "at (all region visible set region=current(region))",
        "at (all region) at (where region='US') at (where year=2023)",
    ],
)
def test_lost_filter_warns_and_can_be_an_error(warning_layer, modifiers):
    rewriter = QueryRewriter(warning_layer.graph)
    query = query_with(modifiers)
    with pytest.warns(YardstickWarning, match="year"):
        rewriter.rewrite(query)
    with warnings.catch_warnings():
        warnings.simplefilter("error", YardstickWarning)
        with pytest.raises(YardstickWarning, match="year"):
            rewriter.rewrite(query)


@pytest.mark.parametrize(
    "modifiers,expected",
    [
        ("at (all region where year=2023)", [("EU", 225.0), ("US", 225.0)]),
        ("at (all region set year=2023)", [("EU", 225.0), ("US", 225.0)]),
        ("at (all year) at (set year=2023)", [("EU", 75.0), ("US", 150.0)]),
        ("at (all region visible)", [("EU", 225.0), ("US", 225.0)]),
        ("at (all region) at (where year=2023) at (where region='US')", [("EU", 225.0), ("US", 225.0)]),
    ],
)
def test_effective_context_preserves_filter(warning_layer, modifiers, expected):
    with warnings.catch_warnings():
        warnings.simplefilter("error", YardstickWarning)
        assert warning_layer.sql(query_with(modifiers)).fetchall() == expected


def test_grouped_filter_removed_by_all_warns(warning_layer):
    with pytest.warns(YardstickWarning, match="year"):
        rows = warning_layer.sql(
            "select year, region, aggregate(revenue) at (all year) from sales_v "
            "where year=2023 group by year,region order by region"
        ).fetchall()
    assert rows == [(2023, "EU", 125.0), (2023, "US", 250.0)]


@pytest.mark.parametrize(
    "predicate",
    [
        "exists (/* calendar */ select 1 from sales c where c.year=2023)",
        "1=1 /* year */",
        "'year' = 'year'",
    ],
)
def test_comments_literals_and_nested_sources_do_not_warn(warning_layer, predicate):
    with warnings.catch_warnings():
        warnings.simplefilter("error", YardstickWarning)
        assert warning_layer.sql(query_with("at (all region)", predicate)).fetchall() == [("EU", 375.0), ("US", 375.0)]


def test_other_joined_source_does_not_warn(warning_layer):
    with warnings.catch_warnings():
        warnings.simplefilter("error", YardstickWarning)
        assert warning_layer.sql(
            "select s.region, aggregate(s.revenue) at (all region) from sales_v s "
            "join sales c on true where c.year=2023 group by s.region order by s.region"
        ).fetchall() == [("EU", 375.0), ("US", 375.0)]


def test_repeated_layer_execution_observes_new_warning_policy(warning_layer):
    query = query_with("at (all region)")
    with pytest.warns(YardstickWarning):
        assert warning_layer.sql(query).fetchall() == [("EU", 375.0), ("US", 375.0)]
    with warnings.catch_warnings():
        warnings.simplefilter("error", YardstickWarning)
        with pytest.raises(YardstickWarning):
            warning_layer.sql(query)


def test_literal_yardstick_table_function_preserves_results_and_warnings(warning_layer):
    inner = query_with("at (all region)").replace("'", "''")
    with pytest.warns(YardstickWarning, match="year"):
        result = warning_layer.sql(f"select * from yardstick('{inner}') as report").fetchall()
    assert result == [("EU", 375.0), ("US", 375.0)]


@pytest.mark.parametrize("argument", ["'select 1; select 2'", "'delete from sales'", "'select 1 into sink'", "region"])
def test_yardstick_table_function_requires_single_literal_read(warning_layer, argument):
    with pytest.raises(ValueError):
        QueryRewriter(warning_layer.graph).rewrite(f"select * from yardstick({argument})")
    assert warning_layer.conn.execute("select count(*) from sales").fetchone() == (4,)


@pytest.mark.parametrize("attributes", [None, {"role": "admin"}])
def test_yardstick_wrapper_cannot_bypass_semantic_security(warning_layer, attributes):
    from sidemantic.core.security import SecurityPolicy

    warning_layer.graph.models["sales_v"].security = SecurityPolicy(access=False, row_filters=["year = 2023"])
    with pytest.raises(ValueError, match="security controls"):
        QueryRewriter(warning_layer.graph).rewrite(
            "select * from yardstick('SEMANTIC select aggregate(revenue) from sales_v')",
            user_attributes=attributes,
        )


def test_semantic_prefix_inside_yardstick_wrapper(warning_layer):
    assert warning_layer.sql(
        "select * from yardstick('SEMANTIC select region, aggregate(revenue) from sales_v order by region')"
    ).fetchall() == [("EU", 125.0), ("US", 250.0)]


def test_wrapper_explanation_matches_rewrite_and_emits_warning(warning_layer):
    sql = f"select * from yardstick('{query_with('at (all region)')}')"
    rewriter = QueryRewriter(warning_layer.graph)
    with pytest.warns(YardstickWarning):
        rewritten = rewriter.rewrite(sql)
    with pytest.warns(YardstickWarning):
        explanation = rewriter.explain(sql)
    assert explanation.input_sql == sql
    assert explanation.rewritten_sql == rewritten
    assert explanation.chosen_plan == "yardstick_semantic_sql"


def test_parenthesized_current_preserves_context_and_string_literals(warning_layer):
    assert warning_layer.sql(query_with("at (set region=coalesce(current(region), 'US'))")).fetchall() == [
        ("EU", 125.0),
        ("US", 250.0),
    ]


@pytest.mark.parametrize(
    "predicate,should_warn",
    [
        ("month(order_date)=2", False),
        ("order_date >= date '2023-02-01'", True),
        ("order_date >= date '2023-02-01' /* month(order_date) */", True),
        ("order_date >= date '2023-02-01' and 'month(order_date)' <> ''", True),
    ],
)
def test_set_expression_must_encode_actual_outer_filter(warning_layer, predicate, should_warn):
    warning_layer.conn.execute("create table daily_orders(order_date date, amount double)")
    warning_layer.conn.execute("insert into daily_orders values ('2023-01-01',100),('2023-02-01',200)")
    warning_layer.add_model(
        Model(
            name="daily_v",
            table="daily_orders",
            metadata={"yardstick": {}},
            dimensions=[Dimension(name="order_date", type="time", granularity="day")],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        )
    )
    sql = (
        "select month(order_date), aggregate(revenue) at (all month(order_date) set month(order_date)=2) "
        f"from daily_v where {predicate} group by month(order_date)"
    )
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", YardstickWarning)
        assert warning_layer.sql(sql).fetchall() == [(2, 200.0)]
    assert any(isinstance(item.message, YardstickWarning) for item in caught) == should_warn


@pytest.mark.parametrize(
    "query",
    [
        "select aggregate(missing) from sales_v",
        "select aggregate(sales_v.missing) from sales_v",
        "select aggregate(unknown.revenue) from sales_v",
        "select aggregate(revenue) from sales_v s join sales_v t on true",
        "select aggregate(revenue) from missing_source",
    ],
)
def test_unknown_or_ambiguous_measure_is_a_binding_error(warning_layer, query):
    with pytest.raises(YardstickBindingError):
        QueryRewriter(warning_layer.graph).rewrite(query)
