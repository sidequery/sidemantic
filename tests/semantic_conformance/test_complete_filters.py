"""Physical input populations for checked complete aggregate filter lowering."""

from copy import deepcopy

import pytest

from sidemantic import Dimension, Metric, Model, Relationship, SecurityPolicy, SemanticLayer
from sidemantic.semantic_handoff import UnsupportedSemanticFeaturesError, graph_to_semantic_input


@pytest.fixture(params=["python", "rust"])
def layer(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs", reason="Complete filter acceptance requires the real Rust extension")
    layer = SemanticLayer(engine=request.param, fallback=False, auto_register=False)
    layer.add_model(
        Model(
            name="orders",
            table="complete_orders",
            primary_key="id",
            dimensions=[
                Dimension(name="amount", type="numeric", sql="amount * 10"),
                Dimension(name="region", type="categorical"),
            ],
            metrics=[
                Metric(name="paid_sum", sql="SUM(amount)", sql_is_complete=True, filters=["status = 'paid'"]),
                Metric(name="paid_count", sql="COUNT(amount)", sql_is_complete=True, filters=["status = 'paid'"]),
                Metric(name="paid_min", sql="MIN(amount)", sql_is_complete=True, filters=["status = 'paid'"]),
                Metric(name="paid_max", sql="MAX(amount)", sql_is_complete=True, filters=["status = 'paid'"]),
                Metric(name="canceled", sql="SUM(amount)", sql_is_complete=True, filters=["status = 'canceled'"]),
                Metric(name="physical_two", sql="SUM(amount)", sql_is_complete=True, filters=["amount = 2"]),
                Metric(name="total", agg="sum", sql="amount"),
            ],
        )
    )
    layer.adapter.execute("""
        create table complete_orders(id integer, amount integer, status varchar, region varchar);
        insert into complete_orders values
            (1, 1, 'paid', 'north'), (2, 2, 'paid', 'north'), (3, null, 'paid', 'north'),
            (4, 9, 'canceled', 'south'), (5, 4, 'orders.vip', 'south'), (6, -3, 'shipped', 'south');
    """)
    try:
        yield layer
    finally:
        layer.adapter.close()


def assert_result(layer, query, columns, expected):
    source = deepcopy(graph_to_semantic_input(layer.graph))
    sql = layer.compile(**query)
    if layer.engine == "rust":
        assert layer.last_engine_selection["engine"] == "rust"
    assert graph_to_semantic_input(layer.graph) == source
    result = layer.adapter.execute(sql)
    assert [column[0] for column in result.description] == columns
    assert result.fetchall() == expected


def test_independent_filters_preserve_each_measure_population(layer):
    assert_result(
        layer,
        {
            "metrics": [
                "orders.paid_sum",
                "orders.paid_count",
                "orders.paid_min",
                "orders.paid_max",
                "orders.canceled",
                "orders.total",
            ]
        },
        ["paid_sum", "paid_count", "paid_min", "paid_max", "canceled", "total"],
        [(3, 2, 1, 2, 9, 13)],
    )


def test_complete_filters_use_physical_values_under_semantic_name_collision(layer):
    assert_result(layer, {"metrics": ["orders.physical_two"]}, ["physical_two"], [(2,)])


def test_groups_without_matching_measure_rows_keep_other_measures(layer):
    assert_result(
        layer,
        {
            "metrics": ["orders.paid_sum", "orders.paid_count", "orders.total"],
            "dimensions": ["orders.region"],
            "order_by": ["orders.region"],
        },
        ["region", "paid_sum", "paid_count", "total"],
        [("north", 3, 2, 3), ("south", None, 0, 10)],
    )


def test_invariant_restriction_still_scopes_every_filtered_measure(layer):
    layer.graph.models["orders"].invariant_filters = ["id != 2"]
    assert_result(
        layer,
        {"metrics": ["orders.paid_sum", "orders.paid_count", "orders.total"]},
        ["paid_sum", "paid_count", "total"],
        [(1, 1, 11)],
    )


@pytest.mark.parametrize("layer", ["rust"], indirect=True)
def test_qualified_physical_columns_do_not_rewrite_string_literals(layer):
    layer.graph.models["orders"].metrics.append(
        Metric(name="vip", sql="SUM(orders.amount)", sql_is_complete=True, filters=["orders.status = 'orders.vip'"])
    )
    assert_result(layer, {"metrics": ["orders.vip"]}, ["vip"], [(4,)])


@pytest.mark.parametrize("layer", ["rust"], indirect=True)
def test_filter_conjunction_preserves_disjunction_grouping(layer):
    layer.graph.models["orders"].metrics.append(
        Metric(
            name="positive",
            sql="SUM(amount)",
            sql_is_complete=True,
            filters=["status = 'paid' OR status = 'shipped'", "amount > 1"],
        )
    )
    assert_result(layer, {"metrics": ["orders.positive"]}, ["positive"], [(2,)])


@pytest.mark.parametrize(
    "expression",
    [
        "COUNT(*)",
        "COUNT(1)",
        "SUM(1)",
        "SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END)",
        "SUM(amount) + COUNT(amount)",
        "SUM(amount) OVER ()",
        "SUM((SELECT amount))",
        "SUM(other.amount)",
        "COUNT(DISTINCT amount + 1)",
        "AVG(DISTINCT amount)",
        "AVG(amount) OVER ()",
        "AVG(other.amount)",
    ],
)
def test_unproven_complete_filter_shapes_fail_explicitly(expression):
    pytest.importorskip("sidemantic_rs", reason="Complete filter rejection requires the real Rust extension")
    layer = SemanticLayer(engine="rust", fallback=False, auto_register=False)
    try:
        layer.add_model(
            Model(
                name="orders",
                table="orders",
                primary_key="id",
                metrics=[Metric(name="value", sql=expression, sql_is_complete=True, filters=["amount > 0"])],
            )
        )
        with pytest.raises(UnsupportedSemanticFeaturesError) as caught:
            layer.compile(metrics=["orders.value"])
        assert "metric.complete_filters" in caught.value.capabilities
    finally:
        layer.adapter.close()


def test_foreign_filter_population_fails_explicitly():
    pytest.importorskip("sidemantic_rs", reason="Complete filter rejection requires the real Rust extension")
    layer = SemanticLayer(engine="rust", fallback=False, auto_register=False)
    try:
        layer.add_model(
            Model(
                name="orders",
                table="orders",
                primary_key="id",
                metrics=[Metric(name="value", sql="SUM(amount)", sql_is_complete=True, filters=["other.amount > 0"])],
            )
        )
        with pytest.raises(UnsupportedSemanticFeaturesError) as caught:
            layer.compile(metrics=["orders.value"])
        assert "metric.complete_filters" in caught.value.capabilities
    finally:
        layer.adapter.close()


@pytest.fixture
def average_distinct_layer(layer):
    layer.graph.models["orders"].metrics.extend(
        [
            Metric(name="paid_avg", sql="AVG(amount)", sql_is_complete=True, filters=["status = 'paid'"]),
            Metric(
                name="paid_distinct", sql="COUNT(DISTINCT amount)", sql_is_complete=True, filters=["status = 'paid'"]
            ),
        ]
    )
    # Repeated values belong to different source rows. AVG keeps both rows;
    # COUNT DISTINCT collapses their values. NULLs enter neither denominator.
    layer.adapter.execute("insert into complete_orders values (7, 1, 'paid', 'north')")
    return layer


def test_filtered_average_and_distinct_keep_different_denominators(average_distinct_layer):
    assert_result(
        average_distinct_layer,
        {"metrics": ["orders.paid_avg", "orders.paid_distinct", "orders.total"]},
        ["paid_avg", "paid_distinct", "total"],
        [(pytest.approx(4 / 3), 2, 14)],
    )


def test_filtered_average_and_distinct_preserve_empty_measure_groups(average_distinct_layer):
    average_distinct_layer.adapter.execute("insert into complete_orders values (8, null, 'paid', 'west')")
    assert_result(
        average_distinct_layer,
        {
            "metrics": ["orders.paid_avg", "orders.paid_distinct"],
            "dimensions": ["orders.region"],
            "order_by": ["orders.region"],
        },
        ["region", "paid_avg", "paid_distinct"],
        [("north", pytest.approx(4 / 3), 2), ("south", None, 0), ("west", None, 0)],
    )


def test_filtered_average_and_distinct_keep_independent_populations(average_distinct_layer):
    layer = average_distinct_layer
    layer.graph.models["orders"].get_metric("paid_avg").filters = ["status = 'canceled'"]
    assert_result(
        layer,
        {"metrics": ["orders.paid_avg", "orders.paid_distinct"]},
        ["paid_avg", "paid_distinct"],
        [(9.0, 2)],
    )


def test_filtered_average_and_distinct_filters_use_physical_column_values(average_distinct_layer):
    layer = average_distinct_layer
    for name in ["paid_avg", "paid_distinct"]:
        layer.graph.models["orders"].get_metric(name).filters.append("amount = 2")
    assert_result(
        layer,
        {"metrics": ["orders.paid_avg", "orders.paid_distinct"]},
        ["paid_avg", "paid_distinct"],
        [(2.0, 1)],
    )


def test_filtered_average_and_distinct_obey_source_policies(average_distinct_layer):
    layer = average_distinct_layer
    layer.graph.models["orders"].security = SecurityPolicy(row_filters=["id <= {{ user.max_id }}"])
    layer.graph.models["orders"].invariant_filters = ["id != 1"]
    assert_result(
        layer,
        {"metrics": ["orders.paid_avg", "orders.paid_distinct"], "user_attributes": {"max_id": 3}},
        ["paid_avg", "paid_distinct"],
        [(2.0, 1)],
    )


def test_filtered_average_and_distinct_survive_unequal_join_fanout(average_distinct_layer):
    layer = average_distinct_layer
    layer.graph.models["orders"].relationships.append(
        Relationship(name="items", type="one_to_many", foreign_key="order_id")
    )
    layer.add_model(
        Model(
            name="items",
            table="complete_items",
            primary_key="id",
            dimensions=[Dimension(name="category", type="categorical")],
        )
    )
    layer.adapter.execute("""
        create table complete_items(id integer, order_id integer, category varchar);
        insert into complete_items values
            (1,1,'a'), (2,1,'a'), (3,1,'a'), (4,2,'a'),
            (5,3,'a'), (6,7,'a'), (7,7,'a'), (8,4,'b');
    """)
    assert_result(
        layer,
        {
            "metrics": ["orders.paid_avg", "orders.paid_distinct"],
            "dimensions": ["items.category"],
            "filters": ["items.category IS NOT NULL"],
            "order_by": ["items.category"],
        },
        ["category", "paid_avg", "paid_distinct"],
        [("a", pytest.approx(4 / 3), 2), ("b", None, 0)],
    )


@pytest.mark.parametrize("data_type", ["FLOAT", "DOUBLE"])
@pytest.mark.parametrize("having", ["none", "average", "sum"])
@pytest.mark.parametrize("collision", ["none", "outputs", "internal"])
def test_filtered_float_averages_deduplicate_keys_within_each_group(
    average_distinct_layer, data_type, having, collision
):
    layer = average_distinct_layer
    layer.adapter.execute(f"alter table complete_orders alter column amount type {data_type}")
    layer.adapter.execute(
        "update complete_orders set amount = case id when 1 then 0.125 when 2 then 0.375 when 7 then 0.125 else amount end"
    )
    model = layer.graph.models["orders"]
    model.metrics.extend(
        [
            Metric(name="first_avg", sql="AVG(amount)", sql_is_complete=True, filters=["status = 'paid'", "id != 2"]),
            Metric(name="first_sum", sql="SUM(amount)", sql_is_complete=True, filters=["status = 'paid'", "id != 2"]),
            Metric(name="row_count", agg="count"),
        ]
    )
    model.relationships.append(Relationship(name="items", type="one_to_many", foreign_key="order_id"))
    layer.add_model(
        Model(
            name="items",
            table="float_items",
            primary_key="id",
            dimensions=[Dimension(name="category", type="categorical")],
        )
    )
    layer.adapter.execute("""
        create table float_items(id integer, order_id integer, category varchar);
        insert into float_items values
            (1,1,'a'), (2,1,'a'), (3,1,'a'), (4,2,'a'), (5,3,'a'), (6,7,'a'), (7,7,'a'),
            (8,2,'b'), (9,2,'b'), (10,3,'b'), (11,999,'orphan'), (12,3,'null-only');
    """)
    # Independent source SQL establishes the three distinct source rows including
    # equal values on different keys. AVG(DISTINCT amount) would incorrectly be .25.
    assert layer.adapter.execute("select avg(amount) from complete_orders where status = 'paid'").fetchone()[
        0
    ] == pytest.approx(5 / 24)
    dimension = "category"
    columns = ["category", "paid_avg", "first_avg", "row_count"]
    average = "paid_avg"
    first_average = "first_avg"
    if collision == "outputs":
        layer.graph.models["items"].dimensions.append(Dimension(name="paid_avg", sql="category", type="categorical"))
        dimension = "paid_avg"
        columns = ["items_paid_avg", "orders_paid_avg", "first_avg", "row_count"]
    elif collision == "internal":
        average = "__fanout_rank_0"
        first_average = "__fanout_column_0"
        model.get_metric("paid_avg").name = average
        model.get_metric("first_avg").name = first_average
        columns = [dimension, average, first_average, "row_count"]
    columns = [*columns[:-1], "paid_sum", "first_sum", columns[-1]]
    query = {
        "metrics": [
            f"orders.{average}",
            f"orders.{first_average}",
            "orders.paid_sum",
            "orders.first_sum",
            "orders.row_count",
        ],
        "dimensions": [f"items.{dimension}"],
        "order_by": [f"items.{dimension}"],
        "filters": ["items.category IS NOT NULL"],
    }
    expected = [
        ("a", pytest.approx(5 / 24), 0.125, 0.625, 0.25, 4),
        ("b", 0.375, None, 0.375, None, 2),
        ("null-only", None, None, None, None, 1),
        ("orphan", None, None, None, None, 0),
    ]
    if having == "average":
        query["filters"].append(f"orders.{average} > 0.3")
        expected = [("b", 0.375, None, 0.375, None, 2)]
    elif having == "sum":
        query["filters"].append("orders.paid_sum > 0.5")
        expected = [("a", pytest.approx(5 / 24), 0.125, 0.625, 0.25, 4)]
    assert_result(layer, query, columns, expected)
