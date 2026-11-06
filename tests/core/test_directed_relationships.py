"""Native relationship direction must survive planning and engine handoff."""

import duckdb
import pytest

from sidemantic import Dimension, Metric, Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.semantic_handoff import graph_handoff_requirements
from sidemantic.sql.generator import SQLGenerator


@pytest.mark.parametrize("kind", ["many_to_one", "one_to_many", "one_to_one", "many_to_many", "cross"])
@pytest.mark.parametrize("reversible", [False, True])
def test_direction_controls_path_discovery(kind, reversible):
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            relationships=[
                Relationship(
                    name="customers",
                    type=kind,
                    foreign_key="id",
                    primary_key="id",
                    metadata={"reversible": reversible},
                )
            ],
        )
    )
    graph.add_model(Model(name="customers", table="customers", primary_key="id"))
    assert len(graph.find_relationship_path("orders", "customers")) == 1
    if reversible:
        assert len(graph.find_relationship_path("customers", "orders")) == 1
    else:
        with pytest.raises(ValueError, match="No.*path"):
            graph.find_relationship_path("customers", "orders")
        assert "relationship.forward_only" in graph_handoff_requirements(graph)


def test_directed_junction_and_nested_role_paths():
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            relationships=[
                Relationship(
                    name="buyer",
                    target_model="customers",
                    type="many_to_one",
                    foreign_key="customer_id",
                    metadata={"reversible": False},
                ),
                Relationship(
                    name="products",
                    type="many_to_many",
                    through="items",
                    through_foreign_key="order_id",
                    related_foreign_key="product_id",
                    metadata={"reversible": False},
                ),
            ],
        )
    )
    graph.add_model(
        Model(
            name="customers",
            table="customers",
            primary_key="id",
            relationships=[
                Relationship(
                    name="regions", type="many_to_one", foreign_key="region_id", metadata={"reversible": False}
                )
            ],
        )
    )
    for name in ("regions", "items", "products"):
        graph.add_model(Model(name=name, table=name, primary_key="id"))
    assert len(graph.find_relationship_path("orders", "products")) == 2
    assert len(graph.find_relationship_path("orders", "buyer$regions")) == 2
    for source in ("products", "buyer$regions", "buyer"):
        with pytest.raises(ValueError, match="No.*path"):
            graph.find_relationship_path(source, "orders")


def test_dimension_first_query_selects_permitted_anchor_and_keeps_inner_predicate():
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
            relationships=[
                Relationship(
                    name="tiers",
                    type="many_to_one",
                    foreign_key="amount",
                    primary_key="threshold",
                    sql="{from}.amount > {to}.threshold AND {from}.region = {to}.region",
                    metadata={"reversible": False, "bsl_how": "inner"},
                )
            ],
        )
    )
    graph.add_model(
        Model(name="tiers", table="tiers", primary_key="id", dimensions=[Dimension(name="id", type="numeric")])
    )
    sql = SQLGenerator(graph).generate(metrics=["orders.revenue"], dimensions=["tiers.id"])
    with duckdb.connect() as connection:
        connection.execute(
            "CREATE TABLE orders AS SELECT * FROM (VALUES (1,5,'e'),(2,15,'e'),(3,30,'w'),(4,25,'e')) t(id,amount,region)"
        )
        connection.execute("CREATE TABLE tiers AS SELECT * FROM (VALUES (1,10,'e'),(2,20,'w')) t(id,threshold,region)")
        assert set(connection.execute(sql).fetchall()) == {(1, 40), (2, 30)}
    with pytest.raises(ValueError, match="No.*path"):
        SQLGenerator(graph, base_model="tiers").generate(metrics=["orders.revenue"], dimensions=["tiers.id"])


@pytest.mark.parametrize("reversible", [False, True])
@pytest.mark.parametrize("kind", ["many_to_one", "many_to_many", "cross"])
def test_target_side_measure_is_not_duplicated_by_permitted_join(reversible, kind):
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[Dimension(name="region", type="categorical")],
            relationships=[
                Relationship(
                    name="customers",
                    type=kind,
                    foreign_key="customer_id",
                    primary_key="id",
                    metadata={"reversible": reversible},
                )
            ],
        )
    )
    graph.add_model(
        Model(
            name="customers",
            table="customers",
            primary_key="id",
            metrics=[Metric(name="credit", agg="sum", sql="credit")],
        )
    )
    sql = SQLGenerator(graph).generate(metrics=["customers.credit"], dimensions=["orders.region"])
    with duckdb.connect() as connection:
        connection.execute(
            "CREATE TABLE orders AS SELECT * FROM (VALUES (1,10,'east'),(2,10,'east'),(3,20,'east')) t(id,customer_id,region)"
        )
        connection.execute("CREATE TABLE customers AS SELECT * FROM (VALUES (10,100),(20,100)) t(id,credit)")
        # Equal-valued distinct customer rows must both survive; repeated orders
        # must not duplicate either customer, so SUM(DISTINCT credit) is wrong too.
        assert connection.execute(sql).fetchall() == [("east", 200)]


@pytest.mark.parametrize("filter_only", [False, True])
def test_directed_cycle_deduplicates_the_actual_join_target(filter_only):
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[Dimension(name="region", type="categorical")],
            relationships=[
                Relationship(
                    name="customers", type="many_to_one", foreign_key="customer_id", metadata={"reversible": False}
                )
            ],
        )
    )
    graph.add_model(
        Model(
            name="customers",
            table="customers",
            primary_key="id",
            metrics=[
                Metric(name="credit", agg="sum", sql="credit"),
                Metric(name="count", agg="count"),
                Metric(name="double_credit", type="derived", sql="credit * 2"),
            ],
            relationships=[
                Relationship(
                    name="accounts", type="many_to_one", foreign_key="account_id", metadata={"reversible": False}
                )
            ],
        )
    )
    graph.add_model(
        Model(
            name="accounts",
            table="accounts",
            primary_key="id",
            relationships=[
                Relationship(name="orders", type="many_to_one", foreign_key="order_id", metadata={"reversible": False})
            ],
        )
    )
    # The allowed return route has no one-to-many hops, but is not the inverse
    # of the orders -> customers join that actually determines this population.
    assert [hop.relationship for hop in graph.find_relationship_path("customers", "orders")] == [
        "many_to_one",
        "many_to_one",
    ]
    generator = SQLGenerator(graph, base_model="orders" if filter_only else None)
    sql = generator.generate(
        metrics=["customers.credit", "customers.count", "customers.double_credit"],
        dimensions=[] if filter_only else ["orders.region"],
        filters=["orders.region = 'east'"] if filter_only else None,
    )
    with duckdb.connect() as connection:
        connection.execute(
            "CREATE TABLE orders AS SELECT * FROM (VALUES (1,10,'east'),(2,10,'east'),(3,20,'east')) t(id,customer_id,region)"
        )
        connection.execute(
            "CREATE TABLE customers AS SELECT * FROM (VALUES (10,100,1),(20,100,1)) t(id,credit,account_id)"
        )
        # Equal-valued customer entities both count; only repeated keys collapse.
        expected = (200, 2, 400) if filter_only else ("east", 200, 2, 400)
        assert connection.execute(sql).fetchall() == [expected]


@pytest.mark.parametrize("metrics", [["orders.amount", "customers.credit"], ["customers.credit", "orders.amount"]])
def test_multiple_metric_grains_use_permitted_anchor_in_each_population(metrics):
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            dimensions=[Dimension(name="region", type="categorical")],
            metrics=[Metric(name="amount", agg="sum", sql="amount")],
            relationships=[
                Relationship(
                    name="customers", type="many_to_one", foreign_key="customer_id", metadata={"reversible": False}
                )
            ],
        )
    )
    graph.add_model(
        Model(
            name="customers",
            table="customers",
            primary_key="id",
            metrics=[Metric(name="credit", agg="sum", sql="credit")],
        )
    )
    sql = SQLGenerator(graph).generate(metrics=metrics, dimensions=["orders.region"])
    with duckdb.connect() as connection:
        connection.execute(
            "CREATE TABLE orders AS SELECT * FROM (VALUES (1,10,'east',4),(2,10,'east',5)) t(id,customer_id,region,amount)"
        )
        connection.execute("CREATE TABLE customers AS SELECT * FROM (VALUES (10,100)) t(id,credit)")
        expected = [9, 100] if metrics[0] == "orders.amount" else [100, 9]
        assert connection.execute(sql).fetchall() == [("east", *expected)]


@pytest.mark.parametrize("reversible", [False, True])
@pytest.mark.parametrize("reverse_metrics", [False, True])
def test_one_to_many_derived_metric_keeps_its_own_aggregate_grain(reversible, reverse_metrics):
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="customers",
            table="customers",
            primary_key="id",
            dimensions=[Dimension(name="region", type="categorical")],
            metrics=[
                Metric(name="credit", agg="sum", sql="credit"),
                Metric(name="double_credit", type="derived", sql="credit * 2"),
            ],
            relationships=[
                Relationship(
                    name="orders",
                    type="one_to_many",
                    foreign_key="customer_id",
                    metadata={"reversible": reversible},
                )
            ],
        )
    )
    graph.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            metrics=[Metric(name="amount", agg="sum", sql="amount")],
        )
    )
    metrics = ["customers.double_credit", "orders.amount"]
    if reverse_metrics:
        metrics.reverse()
    sql = SQLGenerator(graph).generate(metrics=metrics, dimensions=["customers.region"])
    with duckdb.connect() as connection:
        connection.execute("CREATE TABLE customers AS SELECT 10 AS id, 100 AS credit, 'east' AS region")
        connection.execute("CREATE TABLE orders AS SELECT * FROM (VALUES (1,10,4),(2,10,5)) t(id,customer_id,amount)")
        expected = [9, 200] if reverse_metrics else [200, 9]
        assert connection.execute(sql).fetchall() == [("east", *expected)]


@pytest.mark.parametrize("reversible", [False, True])
@pytest.mark.parametrize(
    ("kind", "target_key"),
    [("many_to_one", None), ("many_to_one", "region_key"), ("one_to_many", None), ("one_to_one", None)],
)
def test_nested_role_projects_incoming_relationship_keys(reversible, kind, target_key):
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="orders",
            table="orders",
            primary_key="id",
            metrics=[Metric(name="amount", agg="sum", sql="amount")],
            relationships=[
                Relationship(
                    name="buyer",
                    target_model="customers",
                    type="many_to_one",
                    foreign_key="customer_id",
                    metadata={"reversible": reversible},
                )
            ],
        )
    )
    graph.add_model(
        Model(
            name="customers",
            table="customers",
            primary_key="id",
            relationships=[
                Relationship(
                    name="regions",
                    type=kind,
                    foreign_key="region_id" if kind == "many_to_one" else "customer_id",
                    primary_key=target_key,
                    metadata={"reversible": reversible},
                )
            ],
        )
    )
    graph.add_model(
        Model(
            name="regions",
            table="regions",
            primary_key="id",
            dimensions=[Dimension(name="region", type="categorical")],
        )
    )
    sql = SQLGenerator(graph).generate(metrics=["orders.amount"], dimensions=["buyer$regions.region"])
    with duckdb.connect() as connection:
        connection.execute("CREATE TABLE orders AS SELECT 1 AS id, 10 AS customer_id, 5 AS amount")
        region_id = 1000 if target_key else 100
        connection.execute(f"CREATE TABLE customers AS SELECT 10 AS id, {region_id} AS region_id")
        connection.execute(
            "CREATE TABLE regions AS SELECT 100 AS id, 1000 AS region_key, 10 AS customer_id, 'east' AS region"
        )
        assert connection.execute(sql).fetchall() == [("east", 5)]
