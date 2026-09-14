"""Owned filtered count populations, independent of their declaration location."""

from copy import deepcopy

import pytest

from sidemantic import Dimension, Metric, Model, Relationship, SecurityPolicy, SemanticLayer
from sidemantic.core.semantic_layer import SecurityError
from sidemantic.semantic_handoff import graph_to_semantic_input


@pytest.fixture(params=["python", "rust"])
def engine(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs", reason="Count family acceptance requires the real Rust extension")
    return request.param


@pytest.fixture(params=["model", "graph"])
def counts(request, engine):
    layer = SemanticLayer(engine=engine, fallback=False, auto_register=False)
    layer.add_model(
        Model(
            name="orders",
            table="count_family_orders",
            primary_key="id",
            dimensions=[
                Dimension(name="region", type="categorical"),
                Dimension(name="value", type="numeric", sql="value * 10"),
            ],
            metrics=[Metric(name="value_count", agg="count", sql="value", filters=["status = 'paid'"])],
            security=SecurityPolicy(row_filters=["tenant = {{ user.tenant }}"]),
            invariant_filters=["active"],
        )
    )
    metrics = [
        Metric(name=name, sql=sql, sql_is_complete=True, filters=["status = 'paid'"], fill_nulls_with=99)
        for name, sql in [("star", "COUNT(*)"), ("one", "COUNT(1)"), ("nulls", "COUNT(NULL)")]
    ] + [
        Metric(name=name, agg="count", sql=sql, filters=["status = 'paid'"])
        for name, sql in [("ordinary_star", None), ("ordinary_one", "1"), ("ordinary_null", "NULL")]
    ]
    refs = {}
    for metric in metrics:
        if request.param == "model":
            layer.graph.models["orders"].metrics.append(metric)
            refs[metric.name] = f"orders.{metric.name}"
        else:
            layer.graph.add_metric(metric, model_name="orders")
            refs[metric.name] = metric.name
    layer.adapter.execute("""
        create table count_family_orders(id integer, value integer, status varchar, region varchar, tenant integer, active boolean);
        insert into count_family_orders values
          (1,null,'paid','a',1,true), (2,5,'paid','a',1,true), (3,null,'unpaid','b',1,true),
          (4,null,'paid','b',2,true), (5,null,'paid','a',1,false), (6,null,'paid',null,1,true),
          (7,null,null,'b',1,true), (8,2,'orders.vip','b',1,true);
    """)
    try:
        yield layer, refs
    finally:
        layer.adapter.close()


def result(counts, names=None, **query):
    layer, refs = counts
    query.setdefault("metrics", [refs[name] for name in (names or refs)])
    query.setdefault("user_attributes", {"tenant": 1})
    source = deepcopy(graph_to_semantic_input(layer.graph))
    sql = layer.compile(**query)
    assert graph_to_semantic_input(layer.graph) == source
    if layer.engine == "rust":
        assert layer.last_engine_selection["engine"] == "rust"
    cursor = layer.adapter.execute(sql)
    return [column[0] for column in cursor.description], cursor.fetchall()


def metric(counts, name):
    layer, refs = counts
    return layer.graph.resolve_metric_reference(refs[name])[1]


def test_count_family_null_values_and_public_aliases(counts):
    assert result(counts) == (list(counts[1]), [(3, 3, 0, 3, 3, 0)])
    assert result(
        counts,
        names=["star", "one", "nulls"],
        metrics=[counts[1]["star"], counts[1]["one"], counts[1]["nulls"], "orders.value_count"],
    ) == (["star", "one", "nulls", "value_count"], [(3, 3, 0, 1)])


def test_count_family_grouped_zero_and_null_group(counts):
    columns, rows = result(counts, dimensions=["orders.region"])
    assert columns == ["region", *counts[1]]
    assert set(rows) == {("a", 2, 2, 0, 2, 2, 0), ("b", 0, 0, 0, 0, 0, 0), (None, 1, 1, 0, 1, 1, 0)}


@pytest.mark.parametrize("population", ["no_matches", "empty_source", "query_excluded"])
def test_count_family_empty_populations_are_zero(counts, population):
    layer, _ = counts
    query = {}
    if population == "empty_source":
        layer.adapter.execute("delete from count_family_orders")
    elif population == "no_matches":
        for name in counts[1]:
            metric(counts, name).filters = ["status = 'missing'"]
    else:
        query["filters"] = ["orders.region = 'missing'"]
    assert result(counts, **query) == (list(counts[1]), [(0, 0, 0, 0, 0, 0)])


def test_count_family_independent_predicates_use_physical_values(counts):
    metric(counts, "one").filters = ["orders.status = 'paid' OR orders.status = 'orders.vip'", 'orders."value" = 2']
    assert result(counts, ["star", "one", "nulls"]) == (["star", "one", "nulls"], [(3, 1, 0)])


def test_count_family_mandatory_policies_apply_to_constant_null(counts):
    layer, refs = counts
    assert result(counts, ["star", "one", "nulls"], user_attributes={"tenant": 2}) == (
        ["star", "one", "nulls"],
        [(1, 1, 0)],
    )
    for name in ["star", "one", "nulls"]:
        with pytest.raises(SecurityError):
            layer.compile(metrics=[refs[name]])
    assert result(counts, ["nulls"], user_attributes={"tenant": 9}, dimensions=["orders.region"]) == (
        ["region", "nulls"],
        [],
    )


@pytest.mark.parametrize("restricted", [False, True])
def test_count_family_keyed_fanout_and_orphan_group(counts, restricted):
    layer, _ = counts
    if not restricted:
        layer.graph.models["orders"].security = None
        layer.graph.models["orders"].invariant_filters = []
    layer.graph.models["orders"].relationships.append(
        Relationship(name="items", type="one_to_many", foreign_key="order_id")
    )
    layer.add_model(
        Model(
            name="items",
            table="count_family_items",
            primary_key="id",
            dimensions=[Dimension(name="category", type="categorical")],
        )
    )
    layer.adapter.execute("""
        create table count_family_items(id integer, order_id integer, category varchar);
        insert into count_family_items values (1,1,'x'),(2,1,'x'),(3,2,'x'),(4,2,'y'),
          (5,3,'y'),(6,4,'y'),(7,5,'x'),(8,6,'x'),(9,99,'orphan');
    """)
    # Source restrictions exclude ids4/5 and the orphan group. Without those
    # restrictions, the retained orphan still cannot invent a qualifying row.
    expected = (
        [("x", 3, 3, 0, 3, 3, 0), ("y", 1, 1, 0, 1, 1, 0)]
        if restricted
        else [("orphan", 0, 0, 0, 0, 0, 0), ("x", 4, 4, 0, 4, 4, 0), ("y", 2, 2, 0, 2, 2, 0)]
    )
    assert result(
        counts, dimensions=["items.category"], filters=["items.category IS NOT NULL"], order_by=["items.category"]
    ) == (["category", *counts[1]], expected)


@pytest.mark.parametrize("restricted", [False, True])
def test_count_family_cross_source_counts_restore_absent_zero(counts, restricted):
    layer, refs = counts
    if not restricted:
        layer.graph.models["orders"].security = None
        layer.graph.models["orders"].invariant_filters = []
    layer.graph.models["orders"].relationships.append(
        Relationship(name="regions", type="many_to_one", foreign_key="region", primary_key="region")
    )
    layer.add_model(
        Model(
            name="regions",
            table="count_family_regions",
            primary_key="region",
            dimensions=[Dimension(name="region", type="categorical")],
            metrics=[Metric(name="quota", agg="sum", sql="quota")],
        )
    )
    layer.add_metric(Metric(name="combined", type="derived", sql=f"{refs['one']} + {refs['nulls']} + regions.quota"))
    layer.adapter.execute(
        "create table count_family_regions(region varchar,quota integer); insert into count_family_regions values ('a',10),('b',20),('c',30)"
    )
    assert result(counts, metrics=["combined"], dimensions=["regions.region"], order_by=["regions.region"]) == (
        ["region", "combined"],
        [("a", 12), ("b", 20)] if restricted else [("a", 13), ("b", 21), ("c", 30)],
    )


def test_count_family_order_filter_limit_and_public_alias(counts):
    _, refs = counts
    assert result(
        counts,
        ["one"],
        dimensions=["orders.region"],
        filters=[f"{refs['one']} > 0"],
        order_by=[f"{refs['one']} DESC"],
        limit=1,
    ) == (["region", "one"], [("a", 2)])


@pytest.mark.parametrize("counts", ["graph"], indirect=True)
def test_owned_count_private_leaf_names_do_not_shadow_public_fields(counts):
    layer, refs = counts
    model = layer.graph.models["orders"]
    model.dimensions.extend(
        [
            Dimension(name="__SIDEMANTIC_FILTERED_0", sql="region", type="categorical"),
            Dimension(name="__sidemantic_filtered_1_raw", sql="region", type="categorical"),
        ]
    )
    model.metrics.append(Metric(name="__sidemantic_filtered_2", agg="count", sql="value"))
    layer.graph.add_metric(
        Metric(name="__sidemantic_filtered_3", sql="COUNT(1)", sql_is_complete=True, filters=["status = 'orders.vip'"]),
        model_name="orders",
    )
    assert result(counts, metrics=[refs["one"], "orders.__sidemantic_filtered_2", "__sidemantic_filtered_3"]) == (
        ["one", "__sidemantic_filtered_2", "__sidemantic_filtered_3"],
        [(3, 2, 1)],
    )


@pytest.mark.parametrize("counts", ["graph"], indirect=True)
@pytest.mark.parametrize("engine", ["python"])
def test_owned_count_generator_sees_live_graph_changes(counts):
    from sidemantic.sql.generator import SQLGenerator

    layer, refs = counts
    generator = SQLGenerator(layer.graph)
    source = deepcopy(graph_to_semantic_input(layer.graph))
    assert layer.adapter.execute(
        generator.generate(metrics=[refs["one"]], user_attributes={"tenant": 1})
    ).fetchall() == [(3,)]
    assert graph_to_semantic_input(layer.graph) == source
    metric(counts, "one").filters = ["status = 'orders.vip'"]
    assert layer.adapter.execute(
        generator.generate(metrics=[refs["one"]], user_attributes={"tenant": 1})
    ).fetchall() == [(1,)]
    layer.graph.add_metric(
        Metric(name="new_count", sql="COUNT(NULL)", sql_is_complete=True, filters=["true"]), model_name="orders"
    )
    assert layer.adapter.execute(
        generator.generate(metrics=["new_count"], user_attributes={"tenant": 1})
    ).fetchall() == [(0,)]
    assert generator.graph is layer.graph


@pytest.mark.parametrize("counts", ["graph"], indirect=True)
@pytest.mark.parametrize("name,expected", [("star", [2, 2, 3]), ("one", [2, 2, 3]), ("nulls", [0, 0, 0])])
def test_owned_count_as_cumulative_base_preserves_public_output(counts, name, expected):
    from datetime import date

    layer, refs = counts
    layer.adapter.execute(
        "alter table count_family_orders add column day date; update count_family_orders set day = case when id in (1,2,4,5) then date '2024-01-01' when id in (3,7,8) then date '2024-01-02' else date '2024-01-03' end"
    )
    layer.graph.models["orders"].dimensions.append(Dimension(name="day", type="time", granularity="day"))
    layer.add_metric(Metric(name="running", type="cumulative", sql=refs[name], fill_nulls_with=99))
    columns, rows = result(counts, metrics=["running"], dimensions=["orders.day"], order_by=["orders.day"])
    assert columns == ["day", name, "running"]
    daily = [2, 0, 1] if name != "nulls" else [0, 0, 0]
    assert rows == [(date(2024, 1, index + 1), daily[index], expected[index]) for index in range(3)]


@pytest.mark.parametrize("expression", ["COUNT(*)", "COUNT(1)", "COUNT(NULL)"])
@pytest.mark.parametrize("predicate", ["other.status = 1", "count(*) > 0", "(select count(*) from orders) > 0"])
def test_count_family_does_not_admit_other_filter_scopes(expression, predicate):
    pytest.importorskip("sidemantic_rs")
    from sidemantic.core.semantic_graph import SemanticGraph
    from sidemantic.rust_bridge import compile_semantic_input
    from sidemantic.semantic_handoff import UnsupportedSemanticFeaturesError

    graph = SemanticGraph()
    graph.add_model(Model(name="orders", table="orders", primary_key="id"))
    graph.add_metric(
        Metric(name="counted", sql=expression, sql_is_complete=True, filters=[predicate]), model_name="orders"
    )
    with pytest.raises(UnsupportedSemanticFeaturesError) as error:
        compile_semantic_input(graph, {"metrics": ["counted"]})
    assert "metric.complete_filters" in error.value.capabilities
