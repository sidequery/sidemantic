"""Test Rails-like join syntax."""

import duckdb
import pytest

from sidemantic import Dimension, Measure, Model, SemanticLayer
from sidemantic.core.join import Join


def test_belongs_to_join():
    """Test belongs_to relationship (many-to-one)."""
    # Setup database
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount DECIMAL(10, 2))")
    conn.execute("CREATE TABLE customers (id INTEGER, name VARCHAR)")
    conn.execute("INSERT INTO orders VALUES (1, 101, 100.00), (2, 101, 150.00), (3, 102, 200.00)")
    conn.execute("INSERT INTO customers VALUES (101, 'Alice'), (102, 'Bob')")

    sl = SemanticLayer(connection="duckdb:///:memory:")
    sl.conn = conn

    # Define models with Rails-like joins
    orders = Model(
        name="orders",
        table="orders",
        primary_key="id",
        joins=[
            Join(name="customers", type="belongs_to", foreign_key="customer_id")
        ],
        dimensions=[],
        measures=[Measure(name="revenue", agg="sum", expr="amount")],
    )

    customers = Model(
        name="customers",
        table="customers",
        primary_key="id",
        dimensions=[Dimension(name="name", type="categorical")],
        measures=[],
    )

    sl.add_model(orders)
    sl.add_model(customers)

    # Query across the join
    result = sl.query(
        metrics=["orders.revenue"],
        dimensions=["customers.name"]
    )

    df = result.df()
    print("\nBelongs To Join Results:")
    print(df)

    assert len(df) == 2
    assert "name" in df.columns
    assert "revenue" in df.columns


def test_has_many_join():
    """Test has_many relationship (one-to-many)."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE customers (id INTEGER, name VARCHAR)")
    conn.execute("CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount DECIMAL(10, 2))")
    conn.execute("INSERT INTO customers VALUES (101, 'Alice'), (102, 'Bob')")
    conn.execute("INSERT INTO orders VALUES (1, 101, 100.00), (2, 101, 150.00), (3, 102, 200.00)")

    sl = SemanticLayer(connection="duckdb:///:memory:")
    sl.conn = conn

    # Define has_many from customers perspective
    customers = Model(
        name="customers",
        table="customers",
        primary_key="id",
        joins=[
            Join(name="orders", type="has_many", foreign_key="customer_id")
        ],
        dimensions=[Dimension(name="name", type="categorical")],
        measures=[],
    )

    orders = Model(
        name="orders",
        table="orders",
        primary_key="id",
        dimensions=[],
        measures=[Measure(name="revenue", agg="sum", expr="amount")],
    )

    sl.add_model(customers)
    sl.add_model(orders)

    # Query should work in either direction
    result = sl.query(
        metrics=["orders.revenue"],
        dimensions=["customers.name"]
    )

    df = result.df()
    print("\nHas Many Join Results:")
    print(df)

    assert len(df) == 2


def test_mixed_join_styles():
    """Test that old entity-based and new Rails-like joins work together."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE orders (id INTEGER, customer_id INTEGER, product_id INTEGER, amount DECIMAL(10, 2))")
    conn.execute("CREATE TABLE customers (id INTEGER, name VARCHAR)")
    conn.execute("CREATE TABLE products (product_id INTEGER, category VARCHAR)")
    conn.execute("INSERT INTO orders VALUES (1, 101, 1, 100.00)")
    conn.execute("INSERT INTO customers VALUES (101, 'Alice')")
    conn.execute("INSERT INTO products VALUES (1, 'Electronics')")

    sl = SemanticLayer(connection="duckdb:///:memory:")
    sl.conn = conn

    from sidemantic import Entity

    # Orders uses Rails-like join for customers
    orders = Model(
        name="orders",
        table="orders",
        primary_key="id",
        joins=[
            Join(name="customers", type="belongs_to", foreign_key="customer_id")
        ],
        # But entity-based join for products
        entities=[
            Entity(name="product", type="foreign", expr="product_id")
        ],
        dimensions=[],
        measures=[Measure(name="revenue", agg="sum", expr="amount")],
    )

    customers = Model(
        name="customers",
        table="customers",
        primary_key="id",
        dimensions=[Dimension(name="name", type="categorical")],
        measures=[],
    )

    products = Model(
        name="products",
        table="products",
        entities=[
            Entity(name="product", type="primary", expr="product_id")
        ],
        dimensions=[Dimension(name="category", type="categorical")],
        measures=[],
    )

    sl.add_model(orders)
    sl.add_model(customers)
    sl.add_model(products)

    # Query should work across both join styles
    result = sl.query(
        metrics=["orders.revenue"],
        dimensions=["customers.name", "products.category"]
    )

    df = result.df()
    print("\nMixed Join Styles Results:")
    print(df)

    assert len(df) == 1
    assert list(df.columns) == ["name", "category", "revenue"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
