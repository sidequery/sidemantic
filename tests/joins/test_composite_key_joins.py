"""Test composite (multi-column) key joins."""

import duckdb
import pytest

from sidemantic import Dimension, Metric, Model, Relationship
from sidemantic.core.semantic_graph import JoinPath, SemanticGraph

# =============================================================================
# JOINPATH TESTS
# =============================================================================


def test_joinpath_multi_column():
    """Test JoinPath with multiple columns."""
    jp = JoinPath(
        from_model="shipments",
        to_model="order_items",
        from_columns=["order_id", "item_id"],
        to_columns=["order_id", "item_id"],
        relationship="many_to_one",
    )

    assert jp.from_columns == ["order_id", "item_id"]
    assert jp.to_columns == ["order_id", "item_id"]


def test_joinpath_backwards_compat_properties():
    """Test JoinPath backwards compatibility properties."""
    jp = JoinPath(
        from_model="orders",
        to_model="customers",
        from_columns=["customer_id"],
        to_columns=["id"],
        relationship="many_to_one",
    )

    # from_entity and to_entity return first column for backwards compat
    assert jp.from_entity == "customer_id"
    assert jp.to_entity == "id"


def test_joinpath_backwards_compat_multi_column():
    """Test JoinPath backwards compat returns first column for multi-column keys."""
    jp = JoinPath(
        from_model="shipments",
        to_model="order_items",
        from_columns=["order_id", "item_id"],
        to_columns=["order_id", "item_id"],
        relationship="many_to_one",
    )

    # Should return first column only
    assert jp.from_entity == "order_id"
    assert jp.to_entity == "order_id"


def test_joinpath_empty_columns():
    """Test JoinPath with empty column lists."""
    jp = JoinPath(
        from_model="a",
        to_model="b",
        from_columns=[],
        to_columns=[],
        relationship="many_to_one",
    )

    assert jp.from_entity == ""
    assert jp.to_entity == ""


# =============================================================================
# SEMANTIC GRAPH TESTS
# =============================================================================


def test_semantic_graph_composite_pk_adjacency():
    """Test SemanticGraph builds adjacency with composite key columns."""
    order_items = Model(
        name="order_items",
        table="public.order_items",
        primary_key=["order_id", "item_id"],
        dimensions=[
            Dimension(name="order_id", type="categorical"),
            Dimension(name="item_id", type="categorical"),
        ],
    )

    shipments = Model(
        name="shipments",
        table="public.shipments",
        primary_key="shipment_id",
        relationships=[
            Relationship(
                name="order_items",
                type="many_to_one",
                foreign_key=["order_id", "item_id"],
                primary_key=["order_id", "item_id"],
            )
        ],
        dimensions=[
            Dimension(name="shipment_id", type="categorical"),
        ],
    )

    graph = SemanticGraph()
    graph.add_model(order_items)
    graph.add_model(shipments)
    graph.build_adjacency()

    # Find path from shipments to order_items
    path = graph.find_relationship_path("shipments", "order_items")
    assert len(path) == 1

    jp = path[0]
    assert jp.from_model == "shipments"
    assert jp.to_model == "order_items"
    assert jp.from_columns == ["order_id", "item_id"]
    assert jp.to_columns == ["order_id", "item_id"]


def test_semantic_graph_single_pk_still_works():
    """Test SemanticGraph still works with single-column primary keys."""
    orders = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
        dimensions=[],
    )

    customers = Model(
        name="customers",
        table="customers",
        primary_key="id",
        dimensions=[Dimension(name="name", type="categorical")],
    )

    graph = SemanticGraph()
    graph.add_model(orders)
    graph.add_model(customers)
    graph.build_adjacency()

    path = graph.find_relationship_path("orders", "customers")
    assert len(path) == 1

    jp = path[0]
    assert jp.from_columns == ["customer_id"]
    assert jp.to_columns == ["id"]


# =============================================================================
# SQL GENERATION TESTS
# =============================================================================


def test_composite_key_join_sql(layer):
    """Test SQL generation with composite key joins."""
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE order_items (
            order_id INTEGER,
            item_id INTEGER,
            quantity INTEGER,
            price DECIMAL(10, 2)
        )
    """)
    conn.execute("""
        CREATE TABLE shipments (
            shipment_id INTEGER,
            order_id INTEGER,
            item_id INTEGER,
            shipped_date DATE
        )
    """)
    conn.execute("INSERT INTO order_items VALUES (1, 100, 2, 50.00), (1, 101, 1, 30.00), (2, 100, 3, 50.00)")
    conn.execute("INSERT INTO shipments VALUES (1, 1, 100, '2024-01-01'), (2, 1, 101, '2024-01-02')")

    layer.conn = conn

    order_items = Model(
        name="order_items",
        table="order_items",
        primary_key=["order_id", "item_id"],
        dimensions=[
            Dimension(name="order_id", type="categorical"),
            Dimension(name="item_id", type="categorical"),
        ],
        metrics=[
            Metric(name="total_quantity", agg="sum", sql="quantity"),
            Metric(name="total_value", agg="sum", sql="quantity * price"),
        ],
    )

    shipments = Model(
        name="shipments",
        table="shipments",
        primary_key="shipment_id",
        relationships=[
            Relationship(
                name="order_items",
                type="many_to_one",
                foreign_key=["order_id", "item_id"],
                primary_key=["order_id", "item_id"],
            )
        ],
        dimensions=[
            Dimension(name="shipment_id", type="categorical"),
            Dimension(name="shipped_date", type="time", granularity="day"),
        ],
        metrics=[
            Metric(name="shipment_count", agg="count"),
        ],
    )

    layer.add_model(order_items)
    layer.add_model(shipments)

    # Query across composite key join
    result = layer.query(
        metrics=["shipments.shipment_count"],
        dimensions=["order_items.item_id"],
    )

    df = result.df()
    print("\nComposite Key Join Results:")
    print(df)

    # We should have 2 rows: item_id 100 and 101 (both shipped)
    assert len(df) == 2
    assert "item_id" in df.columns
    assert "shipment_count" in df.columns


def test_composite_key_join_condition_in_sql(layer):
    """Test that generated SQL contains AND condition for composite keys."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE order_items (order_id INTEGER, item_id INTEGER, quantity INTEGER)")
    conn.execute("CREATE TABLE shipments (shipment_id INTEGER, order_id INTEGER, item_id INTEGER)")

    layer.conn = conn

    order_items = Model(
        name="order_items",
        table="order_items",
        primary_key=["order_id", "item_id"],
        dimensions=[
            Dimension(name="order_id", type="categorical"),
            Dimension(name="item_id", type="categorical"),
        ],
        metrics=[Metric(name="qty", agg="sum", sql="quantity")],
    )

    shipments = Model(
        name="shipments",
        table="shipments",
        primary_key="shipment_id",
        relationships=[
            Relationship(
                name="order_items",
                type="many_to_one",
                foreign_key=["order_id", "item_id"],
                primary_key=["order_id", "item_id"],
            )
        ],
        dimensions=[Dimension(name="shipment_id", type="categorical")],
        metrics=[Metric(name="cnt", agg="count")],
    )

    layer.add_model(order_items)
    layer.add_model(shipments)

    # Get SQL without executing using compile()
    sql = layer.compile(
        metrics=["shipments.cnt"],
        dimensions=["order_items.item_id"],
    )

    print("\nGenerated SQL:")
    print(sql)

    # The SQL should contain AND for composite key join
    # Looking for pattern like: shipments.order_id = order_items.order_id AND shipments.item_id = order_items.item_id
    assert "order_id" in sql.lower()
    assert "item_id" in sql.lower()
    # Verify it's using both columns in the join (AND condition)
    assert " and " in sql.lower() or " AND " in sql


def test_count_distinct_with_composite_pk(layer):
    """Test COUNT DISTINCT with composite primary key concatenates columns."""
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE line_items (
            order_id INTEGER,
            line_number INTEGER,
            product_id INTEGER,
            amount DECIMAL(10, 2)
        )
    """)
    conn.execute("""
        INSERT INTO line_items VALUES
            (1, 1, 100, 50.00),
            (1, 2, 101, 30.00),
            (2, 1, 100, 50.00),
            (2, 2, 102, 40.00)
    """)

    layer.conn = conn

    line_items = Model(
        name="line_items",
        table="line_items",
        primary_key=["order_id", "line_number"],
        dimensions=[
            Dimension(name="order_id", type="categorical"),
            Dimension(name="line_number", type="categorical"),
            Dimension(name="product_id", type="categorical"),
        ],
        metrics=[
            Metric(
                name="line_count",
                agg="count_distinct",
                sql="CONCAT(CAST(order_id AS VARCHAR), '|', CAST(line_number AS VARCHAR))",
            ),
            Metric(name="total_amount", agg="sum", sql="amount"),
        ],
    )

    layer.add_model(line_items)

    # Query to count distinct line items per product
    result = layer.query(
        metrics=["line_items.line_count"],
        dimensions=["line_items.product_id"],
    )

    df = result.df()
    print("\nCount Distinct Composite Key Results:")
    print(df)

    # product_id 100 has 2 line items (order 1 line 1, order 2 line 1)
    # product_id 101 has 1 line item
    # product_id 102 has 1 line item
    assert len(df) == 3


def test_single_column_pk_still_generates_correct_sql(layer):
    """Regression test: single-column PK joins still work after multi-column changes."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount DECIMAL(10, 2))")
    conn.execute("CREATE TABLE customers (id INTEGER, name VARCHAR)")
    conn.execute("INSERT INTO orders VALUES (1, 101, 100.00), (2, 101, 150.00), (3, 102, 200.00)")
    conn.execute("INSERT INTO customers VALUES (101, 'Alice'), (102, 'Bob')")

    layer.conn = conn

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

    layer.add_model(orders)
    layer.add_model(customers)

    result = layer.query(metrics=["orders.revenue"], dimensions=["customers.name"])
    df = result.df()

    assert len(df) == 2
    assert set(df["name"].tolist()) == {"Alice", "Bob"}


# =============================================================================
# EDGE CASES
# =============================================================================


def test_model_primary_key_columns_single():
    """Test primary_key_columns property with single-column key."""
    model = Model(
        name="test",
        table="test",
        primary_key="id",
        dimensions=[],
    )
    assert model.primary_key_columns == ["id"]


def test_model_primary_key_columns_multi():
    """Test primary_key_columns property with multi-column key."""
    model = Model(
        name="test",
        table="test",
        primary_key=["col1", "col2", "col3"],
        dimensions=[],
    )
    assert model.primary_key_columns == ["col1", "col2", "col3"]


def test_relationship_foreign_key_columns_single():
    """Test foreign_key_columns property with single-column key."""
    rel = Relationship(name="customers", type="many_to_one", foreign_key="customer_id")
    assert rel.foreign_key_columns == ["customer_id"]


def test_relationship_foreign_key_columns_multi():
    """Test foreign_key_columns property with multi-column key."""
    rel = Relationship(
        name="order_items",
        type="many_to_one",
        foreign_key=["order_id", "item_id"],
    )
    assert rel.foreign_key_columns == ["order_id", "item_id"]


def test_relationship_foreign_key_columns_default():
    """Test foreign_key_columns defaults to {name}_id for many_to_one."""
    rel = Relationship(name="customers", type="many_to_one")
    assert rel.foreign_key_columns == ["customers_id"]


def test_relationship_primary_key_columns_single():
    """Test primary_key_columns property with single-column key."""
    rel = Relationship(name="customers", type="many_to_one", primary_key="cust_id")
    assert rel.primary_key_columns == ["cust_id"]


def test_relationship_primary_key_columns_multi():
    """Test primary_key_columns property with multi-column key."""
    rel = Relationship(
        name="order_items",
        type="many_to_one",
        primary_key=["order_id", "item_id"],
    )
    assert rel.primary_key_columns == ["order_id", "item_id"]


def test_relationship_primary_key_columns_default():
    """Test primary_key_columns defaults to [id]."""
    rel = Relationship(name="customers", type="many_to_one")
    assert rel.primary_key_columns == ["id"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
