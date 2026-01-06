"""Test many-to-many join discovery via junction model."""

import duckdb

from sidemantic import Dimension, Metric, Model, Relationship


def test_many_to_many_join_path(layer):
    """Test join path uses junction model for many_to_many relationships."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE orders (order_id INTEGER, amount DECIMAL(10, 2))")
    conn.execute("CREATE TABLE products (product_id INTEGER, name VARCHAR)")
    conn.execute("CREATE TABLE order_items (order_id INTEGER, product_id INTEGER)")

    conn.execute(
        "INSERT INTO orders VALUES (1, 100.00), (2, 200.00)"
    )
    conn.execute(
        "INSERT INTO products VALUES (10, 'Widget'), (20, 'Gadget')"
    )
    conn.execute(
        "INSERT INTO order_items VALUES (1, 10), (1, 20), (2, 20)"
    )

    layer.conn = conn

    orders = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        relationships=[
            Relationship(
                name="products",
                type="many_to_many",
                through="order_items",
                through_foreign_key="order_id",
                related_foreign_key="product_id",
            )
        ],
    )

    products = Model(
        name="products",
        table="products",
        primary_key="product_id",
        dimensions=[Dimension(name="name", type="categorical")],
    )

    order_items = Model(
        name="order_items",
        table="order_items",
        primary_key="order_id",
        dimensions=[
            Dimension(name="order_id", type="numeric"),
            Dimension(name="product_id", type="numeric"),
        ],
    )

    layer.add_model(orders)
    layer.add_model(products)
    layer.add_model(order_items)

    path = layer.graph.find_relationship_path("orders", "products")
    assert len(path) == 2
    assert path[0].from_model == "orders"
    assert path[0].to_model == "order_items"
    assert path[1].from_model == "order_items"
    assert path[1].to_model == "products"

    sql = layer.compile(metrics=["orders.revenue"], dimensions=["products.name"])
    assert "orders_cte" in sql
    assert "order_items_cte" in sql
    assert "products_cte" in sql
    assert sql.count("LEFT JOIN") == 2
