"""Ossie logical fields and joined-row aggregate grains in the Rust runtime."""

import json

import pytest

from sidemantic import Dimension, Metric, Model, Relationship, SemanticLayer
from sidemantic.adapters.ossie import OssieAdapter
from sidemantic.rust_bridge import rewrite_semantic_input


def expression(sql):
    return {"dialects": [{"dialect": "ANSI_SQL", "expression": sql}]}


def field(name, sql=None):
    return {"name": name, "expression": expression(sql or name)}


def load_layer(tmp_path, datasets, metrics, relationships=()):
    pytest.importorskip("sidemantic_rs", reason="Requires the source-built Rust runtime")
    source = tmp_path / "aggregates.ossie.json"
    source.write_text(
        json.dumps(
            {
                "version": "0.2.0.dev0",
                "semantic_model": [
                    {
                        "name": "commerce",
                        "datasets": datasets,
                        "metrics": [{"name": name, "expression": expression(sql)} for name, sql in metrics.items()],
                        "relationships": list(relationships),
                    }
                ],
            }
        )
    )
    layer = SemanticLayer(engine="rust", fallback=False, auto_register=False)
    layer.graph = OssieAdapter().parse(source)
    return layer


@pytest.mark.parametrize("name,reference", [("amount", "amount"), ("Amount", "amount"), ("gross", "gross")])
@pytest.mark.parametrize("route", ["query", "rewrite"])
def test_computed_input_cannot_be_shadowed_by_physical_column(tmp_path, name, reference, route):
    layer = load_layer(
        tmp_path,
        [{"name": "orders", "source": "orders", "fields": [field(name, "amount * 2")]}],
        {"total": f"SUM(orders.{reference})"},
    )
    try:
        layer.adapter.execute("create table orders(amount int); insert into orders values (10),(20)")
        if route == "query":
            assert layer.query(metrics=["total"]).fetchall() == [(60,)]
            assert layer.last_engine_selection["engine"] == "rust"
        else:
            sql = rewrite_semantic_input(layer.graph, "select total from metrics")
            assert layer.adapter.execute(sql).fetchall() == [(60,)]
    finally:
        layer.adapter.close()


@pytest.mark.parametrize("dimensions", [[], ["orders.population"]])
def test_column_free_aggregate_uses_sole_dataset_population(tmp_path, dimensions):
    layer = load_layer(
        tmp_path,
        [{"name": "orders", "source": "orders", "fields": [field("population", "1")]}],
        {"rows": "COUNT(*)"},
    )
    try:
        layer.adapter.execute("create table orders(amount int); insert into orders values (10),(20),(null)")
        expected = [(1, 3)] if dimensions else [(3,)]
        assert layer.query(metrics=["rows"], dimensions=dimensions).fetchall() == expected
    finally:
        layer.adapter.close()


def test_imported_window_field_is_materialized_before_grouping_and_filtering(tmp_path):
    layer = load_layer(
        tmp_path,
        [
            {
                "name": "orders",
                "source": "orders",
                "fields": [field("id"), field("rank", "ROW_NUMBER() OVER (ORDER BY amount)")],
            }
        ],
        {},
    )
    try:
        layer.adapter.execute("create table orders(id int, amount int); insert into orders values (1,30),(2,10),(3,20)")
        assert layer.query(dimensions=["orders.id", "orders.rank"], order_by=["orders.id"]).fetchall() == [
            (1, 3),
            (2, 1),
            (3, 2),
        ]
        assert layer.query(
            dimensions=["orders.id", "orders.rank"], filters=["orders.rank <= 2"], order_by=["orders.id"]
        ).fetchall() == [(2, 1), (3, 2)]
    finally:
        layer.adapter.close()


@pytest.fixture
def joined(tmp_path):
    layer = load_layer(
        tmp_path,
        [
            {
                "name": "orders",
                "source": "orders",
                "primary_key": ["id"],
                "fields": [field("id"), field("customer_ref", "customer_id + 1"), field("amount")],
            },
            {
                "name": "customers",
                "source": "customers",
                "primary_key": ["id"],
                "fields": [field("id"), field("name"), field("budget")],
            },
        ],
        {
            "weighted": "SUM(orders.amount * customers.budget)",
            "revenue": "SUM(orders.amount)",
            "budget": "SUM(customers.budget)",
            "customers": "COUNT(customers.id)",
            "average": "AVG(customers.budget)",
            "ratio": "SUM(orders.amount) / SUM(customers.budget)",
            "combined": "weighted + budget",
        },
        [
            {
                "name": "customer",
                "from": "orders",
                "to": "customers",
                "from_columns": ["customer_ref"],
                "to_columns": ["id"],
            }
        ],
    )
    layer.adapter.execute("""
        create table orders(id int, customer_id int, amount int);
        insert into orders values (1,0,10),(2,0,20),(3,1,30);
        create table customers(id int, name varchar, budget int);
        insert into customers values (1,'A',100),(2,'B',200);
    """)
    try:
        yield layer
    finally:
        layer.adapter.close()


def test_joined_row_aggregate_and_independent_aggregates_keep_separate_grains(joined):
    assert joined.query(metrics=["weighted"]).fetchall() == [(9000,)]
    assert joined.query(
        metrics=["weighted", "revenue", "budget", "customers", "average", "ratio", "combined"]
    ).fetchall() == [(9000, 60, 300, 2, 150, 0.2, 9300)]
    assert joined.last_engine_selection["engine"] == "rust"


def test_joined_row_aggregate_groups_and_rewrites(joined):
    sql = rewrite_semantic_input(
        joined.graph, "select customers.name, weighted, budget, ratio from metrics order by customers.name"
    )
    assert joined.adapter.execute(sql).fetchall() == [("A", 3000, 100, 0.3), ("B", 6000, 200, 0.15)]


def test_joined_rows_preserve_unmatched_facts_and_equal_value_entities(joined):
    joined.adapter.execute("insert into orders values (4,0,10),(5,999,50)")
    assert joined.query(metrics=["weighted", "revenue", "budget"]).fetchall() == [(10000, 120, 300)]


def test_joined_row_aggregate_does_not_require_unused_entity_keys(joined):
    joined.graph.models["orders"].primary_key = None
    assert joined.query(metrics=["weighted"]).fetchall() == [(9000,)]


def test_joined_tuple_grain_survives_an_additional_fanout(joined):
    joined.add_model(
        Model(
            name="tags",
            table="tags",
            primary_key="id",
            dimensions=[Dimension(name="label", type="categorical")],
            relationships=[Relationship(name="orders", type="many_to_one", foreign_key="order_id")],
        )
    )
    joined.adapter.execute("""
        create table tags(id int, order_id int, label varchar);
        insert into tags values (1,1,'x'),(2,1,'x'),(3,2,'x'),(4,3,'x');
    """)
    assert joined.query(metrics=["weighted", "budget"], dimensions=["tags.label"]).fetchall() == [("x", 9000, 300)]


def test_native_complete_sql_keeps_physical_input_semantics(joined):
    joined.graph.models["orders"].get_dimension("amount").sql = "amount * 10"
    joined.graph.models["orders"].metrics.append(Metric(name="physical", sql="SUM(amount)", sql_is_complete=True))
    assert joined.query(metrics=["orders.physical", "weighted"]).fetchall() == [(60, 90000)]


@pytest.mark.parametrize("plan", ["ordinary", "independent", "fanout"])
@pytest.mark.parametrize("filtered", [False, True])
@pytest.mark.parametrize("logical_name", ["amount", "gross"])
@pytest.mark.parametrize("model_owned", [False, True])
def test_graph_measures_preserve_input_scope(plan, filtered, logical_name, model_owned):
    pytest.importorskip("sidemantic_rs")
    layer = SemanticLayer(engine="rust", fallback=False, auto_register=False)
    layer.add_model(
        Model(
            name="orders",
            sql="SELECT * FROM (VALUES (1, 10, 1), (2, 5, 1), (3, NULL, 1)) AS t(id, amount, customer_id)",
            primary_key="id",
            dimensions=[Dimension(name=logical_name, type="numeric", sql="orders.amount * 2")],
            metrics=[Metric(name="physical", agg="sum", sql="amount", filters=["amount > 7"] if filtered else None)],
        )
    )
    input_name = "amount" if model_owned else logical_name
    layer.graph.add_metric(
        Metric(
            name="value",
            agg="sum",
            sql=f"orders.{input_name}",
            filters=[f"orders.{input_name} > 7"] if filtered else None,
        ),
        model_name="orders" if model_owned else None,
    )
    metrics = ["value", "orders.physical"]
    dimensions = []
    physical_value = 10 if filtered else 15
    expected = (physical_value if model_owned else 30, physical_value)
    if plan == "independent":
        layer.add_model(
            Model(
                name="customers",
                sql="SELECT 1 AS id, 100 AS budget",
                primary_key="id",
                metrics=[Metric(name="budget", agg="sum", sql="budget")],
            )
        )
        layer.graph.models["orders"].relationships.append(
            Relationship(name="customers", type="many_to_one", foreign_key="customer_id")
        )
        metrics.append("customers.budget")
        expected += (100,)
    elif plan == "fanout":
        layer.add_model(
            Model(
                name="tags",
                sql="SELECT * FROM (VALUES (1, 1), (2, 1), (3, 2), (4, 3)) AS t(id, order_id)",
                primary_key="id",
                dimensions=[Dimension(name="label", type="categorical", sql="'all'")],
                relationships=[Relationship(name="orders", type="many_to_one", foreign_key="order_id")],
            )
        )
        dimensions = ["tags.label"]
        expected = ("all", *expected)
    try:
        assert layer.query(metrics=metrics, dimensions=dimensions).fetchall() == [expected]
        assert layer.last_engine_selection["engine"] == "rust"
    finally:
        layer.adapter.close()


def test_joined_aggregate_preserves_compound_computed_relationship_keys(joined):
    orders = joined.graph.models["orders"]
    customers = joined.graph.models["customers"]
    orders.dimensions.append(Dimension(name="tenant_ref", type="numeric", sql="tenant + 10"))
    customers.dimensions.append(Dimension(name="tenant_key", type="numeric", sql="tenant + 10"))
    customers.primary_key = ["tenant_key", "id"]
    orders.relationships = [
        Relationship(
            name="customers",
            type="many_to_one",
            foreign_key=["tenant_ref", "customer_ref"],
            primary_key=["tenant_key", "id"],
        )
    ]
    joined.adapter.execute("""
        alter table customers add column tenant int default 1;
        alter table orders add column tenant int default 1;
        insert into customers values (1,'C',500,2);
        insert into orders values (4,0,10,2);
    """)
    assert joined.query(metrics=["weighted", "budget", "ratio"]).fetchall() == [(14000, 800, 0.0875)]
