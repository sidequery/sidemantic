"""Entity-grain advanced and complete aggregates, through both real compilers."""

import pytest

from sidemantic import Dimension, Metric, Model, Relationship, SemanticLayer


@pytest.fixture(params=["python", "rust"])
def layer(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs", reason="Requires the real Rust extension")
    layer = SemanticLayer(engine=request.param, fallback=False, auto_register=False)
    layer.adapter.execute(
        "create table raw_orders as select * from (values (1, 10.0), (2, 20.0), (3, 60.0)) t(id, amount)"
    )
    layer.adapter.execute(
        "create table raw_items as select * from "
        "(values (1, 1, 'all'), (2, 1, 'all'), (3, 2, 'all'), (4, 3, 'all')) t(id, order_id, category)"
    )
    layer.add_model(
        Model(
            name="orders",
            table="raw_orders",
            primary_key="id",
            metrics=[
                *[
                    Metric(name=agg, agg=agg, sql="amount")
                    for agg in ["median", "stddev", "stddev_pop", "variance", "variance_pop"]
                ],
                Metric(name="users", agg="approx_count_distinct", sql="id"),
                Metric(name="implicit_users", agg="approx_count_distinct"),
                Metric(name="implicit_exact_users", agg="count_distinct"),
                Metric(name="average", sql="sum({model}.amount) / count(*)", sql_is_complete=True),
                Metric(name="opaque_count", sql="count(*)", sql_is_complete=True),
                Metric(name="qualified_average", sql="avg(orders_cte.amount)", sql_is_complete=True),
                Metric(name="filtered_average", sql="avg(amount)", sql_is_complete=True, filters=["amount >= 20"]),
                Metric(name="double_average", type="derived", sql="average * 2"),
            ],
            relationships=[Relationship(name="items", type="one_to_many", sql="id", foreign_key="order_id")],
        )
    )
    layer.add_model(
        Model(
            name="items",
            table="raw_items",
            primary_key="id",
            dimensions=[Dimension(name="category", type="categorical")],
            metrics=[Metric(name="item_count", agg="count")],
            relationships=[Relationship(name="orders", type="many_to_one", foreign_key="order_id")],
        )
    )
    try:
        yield layer
    finally:
        layer.adapter.close()


@pytest.mark.parametrize("aggregation", ["median", "stddev", "stddev_pop", "variance", "variance_pop"])
def test_advanced_aggregates_use_each_entity_once(layer, aggregation):
    function = "var_pop" if aggregation == "variance_pop" else aggregation
    expected = layer.adapter.execute(f"select {function}(amount) from raw_orders").fetchone()[0]
    sql = layer.compile(metrics=[f"orders.{aggregation}"], dimensions=["items.category"])
    rows = layer.adapter.execute(sql).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "all"
    assert rows[0][1] == pytest.approx(expected)


def test_complete_sql_and_wrappers_run_after_entity_dedup(layer):
    sql = layer.compile(
        metrics=[
            "orders.average",
            "orders.opaque_count",
            "orders.filtered_average",
            "orders.double_average",
            "orders.qualified_average",
        ],
        dimensions=["items.category"],
    )
    assert layer.adapter.execute(sql).fetchall() == [("all", 30.0, 3, 40.0, 60.0, 30.0)]


def test_approximate_distinct_supports_fanout_and_aggregate_filters(layer):
    sql = layer.compile(metrics=["orders.users"], dimensions=["items.category"], filters=["orders.users > 2"])
    assert "APPROX_COUNT_DISTINCT" in sql.upper()
    assert layer.adapter.execute(sql).fetchall() == [("all", 3)]


def test_complete_sql_and_advanced_independent_populations(layer):
    sql = layer.compile(
        metrics=["orders.average", "orders.median", "items.item_count"],
        dimensions=["items.category"],
    )
    assert layer.adapter.execute(sql).fetchall() == [("all", 30.0, 20.0, 4)]


@pytest.mark.parametrize("dimensions", [[], ["items.category"]])
def test_distinct_without_expression_uses_entity_identity(layer, dimensions):
    sql = layer.compile(metrics=["orders.implicit_users", "orders.implicit_exact_users"], dimensions=dimensions)
    expected = [("all", 3, 3)] if dimensions else [(3, 3)]
    assert layer.adapter.execute(sql).fetchall() == expected


@pytest.mark.parametrize(
    "formula,expected",
    [
        ("median(amount)", 40.0),
        ("sum(amount) / count(*)", 80.0 / 3),
        ("sum(amount) / count(amount)", 40.0),
        ("count(distinct abs(amount))", 2),
        ("sum(amount) filter (where amount > 30)", 60.0),
    ],
)
@pytest.mark.parametrize("fanout", [False, True])
def test_filtered_complete_formulas_keep_python_row_inputs(layer, formula, expected, fanout):
    layer.graph.models["orders"].metrics.append(
        Metric(name="formula", sql=formula, sql_is_complete=True, filters=["amount >= 20"])
    )
    rows = layer.query(metrics=["orders.formula"], dimensions=["items.category"] if fanout else []).fetchall()
    assert len(rows) == 1
    assert rows[0][-1] == pytest.approx(expected)


@pytest.mark.parametrize("fanout", [False, True])
@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("filtered", [False, True])
def test_complete_default_is_applied_at_selected_output_not_nested_formula(layer, fanout, reverse, filtered):
    layer.adapter.execute("update raw_orders set amount = null")
    layer.graph.models["orders"].metrics.extend(
        [
            Metric(name="filled_complete", sql="sum(amount)", sql_is_complete=True, fill_nulls_with=9),
            Metric(name="wrapper", type="derived", sql="filled_complete + 1", fill_nulls_with=7),
        ]
    )
    metrics = ["orders.filled_complete", "orders.wrapper"]
    if reverse:
        metrics.reverse()
    cursor = layer.query(
        metrics=metrics,
        dimensions=["items.category"] if fanout else [],
        filters=["orders.filled_complete = 9"] if filtered else [],
    )
    columns = [field[0] for field in cursor.description]
    rows = cursor.fetchall()
    assert len(rows) == 1
    assert rows[0][columns.index("filled_complete")] == 9
    assert rows[0][columns.index("wrapper")] == 7
