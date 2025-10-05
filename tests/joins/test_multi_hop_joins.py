"""Test multi-hop join discovery and execution.

Multi-hop joins enable queries across models that aren't directly connected.
Example: orders -> customers -> regions (2 hops)
"""

import duckdb
import pytest

from sidemantic import Dimension, Metric, Model, Relationship, SemanticLayer


@pytest.fixture
def three_table_chain():
    """Create 3-table chain: orders -> customers -> regions."""
    conn = duckdb.connect(":memory:")

    conn.execute("""CREATE TABLE orders (order_id INTEGER, customer_id INTEGER, order_amount DECIMAL(10, 2))""")
    conn.execute("""CREATE TABLE customers (customer_id INTEGER, region_id INTEGER, customer_name VARCHAR)""")
    conn.execute("""CREATE TABLE regions (region_id INTEGER, region_name VARCHAR)""")

    conn.execute("""INSERT INTO orders VALUES (1, 101, 150.00), (2, 102, 200.00), (3, 101, 100.00), (4, 103, 300.00)""")
    conn.execute("""INSERT INTO customers VALUES (101, 1, 'Alice'), (102, 2, 'Bob'), (103, 1, 'Charlie')""")
    conn.execute("""INSERT INTO regions VALUES (1, 'North America'), (2, 'Europe')""")

    return conn


def test_two_hop_join(three_table_chain):
    """Test 2-hop join path discovery."""
    # Create semantic layer with 3-table chain
    layer = SemanticLayer()
    layer.conn = three_table_chain

    orders = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        dimensions=[Dimension(name="customer_id", type="numeric")],
        metrics=[Metric(name="revenue", agg="sum", sql="order_amount")],
        relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
    )

    customers = Model(
        name="customers",
        table="customers",
        primary_key="customer_id",
        dimensions=[
            Dimension(name="region_id", type="numeric"),
            Dimension(name="customer_name", type="categorical"),
        ],
        relationships=[Relationship(name="regions", type="many_to_one", foreign_key="region_id")],
    )

    regions = Model(
        name="regions",
        table="regions",
        primary_key="region_id",
        dimensions=[Dimension(name="region_name", type="categorical")],
    )

    layer.add_model(orders)
    layer.add_model(customers)
    layer.add_model(regions)

    # Query orders.revenue by regions.region_name (2-hop join)
    sql = layer.compile(metrics=["orders.revenue"], dimensions=["regions.region_name"])

    # Should have all 3 CTEs
    assert "orders_cte" in sql
    assert "customers_cte" in sql
    assert "regions_cte" in sql

    # Should have 2 LEFT JOINs
    assert sql.count("LEFT JOIN") == 2


def test_join_path_discovery(three_table_chain):
    """Test join path algorithm finds multi-hop paths."""
    layer = SemanticLayer()
    layer.conn = three_table_chain

    orders = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
    )

    customers = Model(
        name="customers",
        table="customers",
        primary_key="customer_id",
        relationships=[Relationship(name="regions", type="many_to_one", foreign_key="region_id")],
    )

    regions = Model(
        name="regions",
        table="regions",
        primary_key="region_id",
    )

    layer.add_model(orders)
    layer.add_model(customers)
    layer.add_model(regions)

    # Find 2-hop path from orders to regions
    path = layer.graph.find_relationship_path("orders", "regions")

    # Should return 2-element list of join paths
    assert len(path) == 2
    assert path[0].from_model == "orders"
    assert path[0].to_model == "customers"
    assert path[1].from_model == "customers"
    assert path[1].to_model == "regions"


def test_intermediate_model_included():
    """Test that intermediate models are included in CTEs."""
    layer = SemanticLayer()

    orders = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        metrics=[Metric(name="revenue", agg="sum", sql="order_amount")],
        relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
    )

    customers = Model(
        name="customers",
        table="customers",
        primary_key="customer_id",
        relationships=[Relationship(name="regions", type="many_to_one", foreign_key="region_id")],
    )

    regions = Model(
        name="regions",
        table="regions",
        primary_key="region_id",
        dimensions=[Dimension(name="region_name", type="categorical")],
    )

    layer.add_model(orders)
    layer.add_model(customers)
    layer.add_model(regions)

    # Query only orders and regions (customers is intermediate)
    sql = layer.compile(metrics=["orders.revenue"], dimensions=["regions.region_name"])

    # All 3 CTEs should be present, including intermediate customers
    assert "orders_cte" in sql
    assert "customers_cte" in sql
    assert "regions_cte" in sql


def test_query_execution(three_table_chain):
    """Test multi-hop query executes and returns correct results."""
    layer = SemanticLayer()
    layer.conn = three_table_chain

    orders = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        metrics=[Metric(name="revenue", agg="sum", sql="order_amount")],
        relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
    )

    customers = Model(
        name="customers",
        table="customers",
        primary_key="customer_id",
        relationships=[Relationship(name="regions", type="many_to_one", foreign_key="region_id")],
    )

    regions = Model(
        name="regions",
        table="regions",
        primary_key="region_id",
        dimensions=[Dimension(name="region_name", type="categorical")],
    )

    layer.add_model(orders)
    layer.add_model(customers)
    layer.add_model(regions)

    # Execute multi-hop query
    result = layer.query(metrics=["orders.revenue"], dimensions=["regions.region_name"])
    df = result.fetchdf()

    # Should have 2 regions with correct revenue
    assert len(df) == 2
    revenues = {row["region_name"]: row["revenue"] for _, row in df.iterrows()}
    # North America: orders 1,3,4 (Alice:150+100, Charlie:300) = 550
    # Europe: order 2 (Bob:200) = 200
    assert revenues["North America"] == 550.0
    assert revenues["Europe"] == 200.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
