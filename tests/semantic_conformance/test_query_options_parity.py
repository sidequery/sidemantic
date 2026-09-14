"""Query options execute through both semantic compilers without fallback."""

import pytest

from sidemantic import Dimension, Metric, Model, Relationship, SemanticLayer
from sidemantic.core.table_calculation import TableCalculation
from sidemantic.semantic_handoff import UnsupportedSemanticFeaturesError


@pytest.fixture(params=["python", "rust"])
def layer(request):
    if request.param == "rust":
        pytest.importorskip("sidemantic_rs", reason="Requires the actual Rust extension")
    layer = SemanticLayer(engine=request.param, auto_register=False, default_limit=2)
    layer.adapter.execute(
        """create table orders as select * from (values
        (1, 'a', 'west', 1, 10), (2, 'a', 'east', 2, 20),
        (3, 'b', 'west', 1, 30), (4, null, 'east', 3, 40)
        ) as t(id, category, region, customer, amount)"""
    )
    layer.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[Dimension(name=name, type="categorical") for name in ("category", "region")],
            metrics=[
                Metric(name="revenue", agg="sum", sql="amount"),
                Metric(name="customers", agg="count_distinct", sql="customer"),
                Metric(name="average", agg="avg", sql="amount"),
            ],
        )
    )
    yield layer
    layer.adapter.close()


def execute(layer, **query):
    result = layer.adapter.execute(layer.compile(**query))
    return [column[0] for column in result.description], result.fetchall()


@pytest.mark.parametrize("order", ["orders.revenue desc", "revenue desc", "total desc"])
def test_aliases_keep_ordering_and_pagination(layer, order):
    columns, rows = execute(
        layer,
        metrics=["orders.revenue"],
        dimensions=["orders.category"],
        aliases={"orders.category": "Category label", "orders.revenue": "total"},
        order_by=[order, "orders.category"],
        limit=2,
        offset=1,
    )
    assert columns == ["Category label", "total"]
    assert rows == [("a", 30), ("b", 30)]


@pytest.mark.parametrize("alias", ['sum"; drop table orders; --', "select", "MixedCase"])
def test_quoted_aliases_are_identifiers(layer, alias):
    columns, rows = execute(
        layer,
        metrics=["orders.revenue"],
        aliases={"orders.revenue": alias},
    )
    assert columns == [alias]
    assert rows == [(100,)]
    assert layer.adapter.execute("select count(*) from orders").fetchone() == (4,)


@pytest.mark.parametrize("order", ["orders.revenue desc", "revenue desc", "total desc"])
def test_aliases_bind_selected_calculation_order(layer, order):
    layer.graph.add_table_calculation(TableCalculation(name="running", type="running_total", field="total"))
    columns, rows = execute(
        layer,
        metrics=["orders.revenue"],
        dimensions=["orders.category"],
        aliases={"orders.revenue": "total", "orders.category": "Category label"},
        order_by=[order, "orders.category"],
        table_calculations=["running"],
    )
    assert columns == ["Category label", "total", "running"]
    assert rows == [(None, 40, 40), ("a", 30, 70)]


def test_totals_recompute_nonadditive_aggregates_and_keep_real_null_group(layer):
    columns, rows = execute(
        layer,
        metrics=["orders.revenue", "orders.customers", "orders.average"],
        dimensions=["orders.category"],
        with_totals=True,
    )
    assert columns == ["category", "revenue", "customers", "average", "_is_total"]
    assert set(rows) == {("a", 30, 2, 15, 0), ("b", 30, 1, 30, 0), (None, 40, 1, 40, 0), (None, 100, 3, 25, 1)}


def test_totals_aliases_filters_and_multiple_dimensions(layer):
    columns, rows = execute(
        layer,
        metrics=["orders.revenue", "orders.customers"],
        dimensions=["orders.category", "orders.region"],
        filters=["orders.region = 'west'"],
        aliases={"orders.category": "Category label", "orders.customers": "unique buyers"},
        with_totals=True,
    )
    assert columns == ["Category label", "region", "revenue", "unique buyers", "_is_total"]
    assert set(rows) == {("a", "west", 10, 1, 0), ("b", "west", 30, 1, 0), (None, None, 40, 1, 1)}


def test_totals_with_dimension_only_and_without_dimensions(layer):
    columns, rows = execute(layer, dimensions=["orders.category"], with_totals=True)
    assert columns == ["category", "_is_total"]
    assert set(rows) == {("a", 0), ("b", 0), (None, 0), (None, 1)}
    columns, rows = execute(layer, metrics=["orders.customers"], with_totals=True)
    assert columns == ["customers"]
    assert rows == [(3,)]


@pytest.mark.parametrize("options", [{"limit": 1}, {"offset": 1}, {"ungrouped": True}])
def test_totals_invalid_combinations(layer, options):
    with pytest.raises(ValueError, match="with_totals cannot be combined"):
        layer.compile(metrics=["orders.revenue"], dimensions=["orders.category"], with_totals=True, **options)


def test_totals_preaggregation_is_unsupported(layer):
    with pytest.raises(
        (NotImplementedError, UnsupportedSemanticFeaturesError), match="pre-aggregated|query.totals.preaggregation"
    ):
        layer.compile(
            metrics=["orders.revenue"], dimensions=["orders.category"], with_totals=True, use_preaggregations=True
        )


def test_ungrouped_aliases(layer):
    columns, rows = execute(
        layer,
        metrics=["orders.revenue"],
        dimensions=["orders.category"],
        aliases={"orders.revenue": "amount", "orders.category": "label"},
        ungrouped=True,
        order_by=["amount desc"],
    )
    assert columns == ["label", "amount"]
    assert rows == [(None, 40), ("b", 30)]


def test_totals_one_to_one_sources_keep_null_detail_separate_from_total(layer):
    layer.adapter.execute("create table bonuses as select id, id * 2 as quota from orders")
    layer.add_model(
        Model(
            name="bonuses",
            table="bonuses",
            primary_key="id",
            metrics=[Metric(name="quota", agg="sum", sql="quota")],
            relationships=[Relationship(name="orders", type="one_to_one", foreign_key="id")],
        )
    )
    layer.add_metric(Metric(name="combined", type="derived", sql="orders.revenue + bonuses.quota"))
    columns, rows = execute(
        layer,
        metrics=["orders.revenue", "bonuses.quota", "combined"],
        dimensions=["orders.category"],
        with_totals=True,
        aliases={"combined": "combined value"},
    )
    assert columns == ["category", "revenue", "quota", "combined value", "_is_total"]
    assert set(rows) == {("a", 30, 6, 36, 0), ("b", 30, 6, 36, 0), (None, 40, 8, 48, 0), (None, 100, 20, 120, 1)}


@pytest.mark.parametrize("use_preaggregations", [False, True])
def test_snapshot_totals_retain_latest_per_entity_selection(layer, use_preaggregations):
    layer.adapter.execute("""create table balances as select * from (values
        (1, 'a', 'east', date '2024-01-01', 10), (2, 'a', 'east', date '2024-01-02', 30),
        (3, 'b', 'west', date '2024-01-01', 20), (4, 'b', 'west', date '2024-01-02', 60)
        ) as t(id, account, region, day, amount)""")
    layer.add_model(
        Model(
            name="balances",
            table="balances",
            primary_key="id",
            dimensions=[
                Dimension(name="account", type="categorical"),
                Dimension(name="region", type="categorical"),
                Dimension(name="day", type="time", granularity="day"),
            ],
            metrics=[
                Metric(
                    name="closing",
                    agg="sum",
                    sql="amount",
                    non_additive_dimension="day",
                    non_additive_window_groupings=["account"],
                ),
                Metric(name="activity", agg="sum", sql="amount"),
            ],
        )
    )
    columns, rows = execute(
        layer,
        metrics=["balances.closing", "balances.activity"],
        dimensions=["balances.region"],
        aliases={"balances.closing": "balance"},
        with_totals=True,
        use_preaggregations=use_preaggregations,
    )
    assert columns == ["region", "balance", "activity", "_is_total"]
    assert set(rows) == {("east", 30, 40, 0), ("west", 60, 80, 0), (None, 90, 120, 1)}


def test_totals_deduplicate_fanned_out_keys_across_detail_groups(layer):
    layer.adapter.execute("""create table lineitems as select * from (values
        (1, 1, 'x'), (2, 1, 'y'), (3, 2, 'x'), (4, 3, 'y'), (5, 4, 'x')
        ) as t(id, order_id, kind)""")
    layer.add_model(
        Model(
            name="lineitems",
            table="lineitems",
            primary_key="id",
            dimensions=[Dimension(name="kind", type="categorical")],
            relationships=[Relationship(name="orders", type="many_to_one", foreign_key="order_id")],
        )
    )
    columns, rows = execute(
        layer, metrics=["orders.revenue", "orders.average"], dimensions=["lineitems.kind"], with_totals=True
    )
    assert columns == ["kind", "revenue", "average", "_is_total"]
    totals = [row for row in rows if row[-1] == 1]
    assert totals == [(None, 100, 25, 1)]
    assert {kind: amount for kind, amount, average, marker in rows if marker == 0} == {"x": 70, "y": 40}
    assert {kind: average for kind, amount, average, marker in rows if marker == 0} == pytest.approx(
        {"x": 70 / 3, "y": 20}
    )


def test_totals_empty_population_keeps_the_grand_total(layer):
    columns, rows = execute(
        layer,
        metrics=["orders.revenue", "orders.customers"],
        dimensions=["orders.category"],
        filters=["orders.category = 'absent'"],
        with_totals=True,
    )
    assert columns == ["category", "revenue", "customers", "_is_total"]
    assert rows == [(None, None, 0, 1)]


@pytest.mark.parametrize("alias", ["Category label", "Category DESC", "Category NULLS FIRST"])
@pytest.mark.parametrize(
    "suffix,expected",
    [
        ("", [("a", 30), ("b", 30)]),
        (" desc", [("b", 30), ("a", 30)]),
        (" asc nulls first", [(None, 40), ("a", 30)]),
        ("\tDESC\tNULLS\tLAST", [("b", 30), ("a", 30)]),
    ],
)
@pytest.mark.parametrize("calculations", [False, True])
def test_spaced_output_alias_ordering_keeps_full_names(layer, alias, suffix, expected, calculations):
    if calculations:
        layer.graph.add_table_calculation(TableCalculation(name="running", type="running_total", field="revenue"))
    columns, rows = execute(
        layer,
        metrics=["orders.revenue"],
        dimensions=["orders.category"],
        aliases={"orders.category": alias},
        order_by=[alias + suffix],
        filters=["orders.category IS NOT NULL"] if not suffix else None,
        table_calculations=["running"] if calculations else None,
        limit=2,
    )
    if layer.engine == "rust":
        assert layer.last_engine_selection["engine"] == "rust"
    assert columns == [alias, "revenue", *(["running"] if calculations else [])]
    if calculations:
        first, second = expected
        assert rows == [(*first, first[1]), (*second, first[1] + second[1])]
    else:
        assert rows == expected
