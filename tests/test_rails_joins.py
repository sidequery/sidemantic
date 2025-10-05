"""Test Rails-like join syntax."""

import duckdb
import pytest

from sidemantic import Dimension, Metric, Model, Relationship, SemanticLayer


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
        relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
        dimensions=[],
        metrics=[Metric(name="revenue", agg="sum", sql="amount")],
    )

    customers = Model(
        name="customers",
        table="customers",
        primary_key="id",
        dimensions=[Dimension(name="name", type="categorical")],
        metrics=[],
    )

    sl.add_model(orders)
    sl.add_model(customers)

    # Query across the join
    result = sl.query(metrics=["orders.revenue"], dimensions=["customers.name"])

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
        relationships=[Relationship(name="orders", type="one_to_many", foreign_key="customer_id")],
        dimensions=[Dimension(name="name", type="categorical")],
        metrics=[],
    )

    orders = Model(
        name="orders",
        table="orders",
        primary_key="id",
        dimensions=[],
        metrics=[Metric(name="revenue", agg="sum", sql="amount")],
    )

    sl.add_model(customers)
    sl.add_model(orders)

    # Query should work in either direction
    result = sl.query(metrics=["orders.revenue"], dimensions=["customers.name"])

    df = result.df()
    print("\nHas Many Join Results:")
    print(df)

    assert len(df) == 2


def test_multi_relationship_join():
    """Test model with multiple relationships."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE orders (id INTEGER, customer_id INTEGER, product_id INTEGER, amount DECIMAL(10, 2))")
    conn.execute("CREATE TABLE customers (id INTEGER, name VARCHAR)")
    conn.execute("CREATE TABLE products (product_id INTEGER, category VARCHAR)")
    conn.execute("INSERT INTO orders VALUES (1, 101, 1, 100.00)")
    conn.execute("INSERT INTO customers VALUES (101, 'Alice')")
    conn.execute("INSERT INTO products VALUES (1, 'Electronics')")

    sl = SemanticLayer(connection="duckdb:///:memory:")
    sl.conn = conn

    # Orders has relationships to both customers and products
    orders = Model(
        name="orders",
        table="orders",
        primary_key="id",
        relationships=[
            Relationship(name="customers", type="many_to_one", foreign_key="customer_id"),
            Relationship(name="products", type="many_to_one", foreign_key="product_id"),
        ],
        dimensions=[],
        metrics=[Metric(name="revenue", agg="sum", sql="amount")],
    )

    customers = Model(
        name="customers",
        table="customers",
        primary_key="id",
        dimensions=[Dimension(name="name", type="categorical")],
        metrics=[],
    )

    products = Model(
        name="products",
        table="products",
        primary_key="product_id",
        dimensions=[Dimension(name="category", type="categorical")],
        metrics=[],
    )

    sl.add_model(orders)
    sl.add_model(customers)
    sl.add_model(products)

    # Query should work across multiple relationships
    result = sl.query(metrics=["orders.revenue"], dimensions=["customers.name", "products.category"])

    df = result.df()
    print("\nMulti-Relationship Results:")
    print(df)

    assert len(df) == 1
    assert list(df.columns) == ["name", "category", "revenue"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
