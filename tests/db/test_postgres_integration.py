"""Integration tests for PostgreSQL adapter against real database.

Run with: docker compose up -d && pytest -m integration tests/db/test_postgres_integration.py -v
"""

import os

import pytest

from sidemantic import Dimension, Metric, Model, Relationship, SemanticLayer

# Mark all tests in this module as integration tests
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("POSTGRES_TEST") != "1",
        reason="Set POSTGRES_TEST=1 and run docker compose up -d to run Postgres integration tests",
    ),
]

# Use environment variable for URL (different in docker vs local)
POSTGRES_URL = os.getenv("POSTGRES_URL", "postgres://test:test@localhost:5433/sidemantic_test")


@pytest.fixture
def postgres_adapter():
    """Create PostgreSQL adapter connected to test database."""
    from sidemantic.db.postgres import PostgreSQLAdapter

    adapter = PostgreSQLAdapter.from_url(POSTGRES_URL)
    yield adapter
    adapter.close()


@pytest.fixture
def clean_postgres(postgres_adapter):
    """Clean database before each test."""
    # Drop all tables
    result = postgres_adapter.execute(
        """
        SELECT tablename FROM pg_tables
        WHERE schemaname = 'public'
    """
    )
    for row in result.fetchall():
        postgres_adapter.execute(f"DROP TABLE IF EXISTS {row[0]} CASCADE")
    yield postgres_adapter


def test_postgres_adapter_basic_query(postgres_adapter):
    """Test basic query execution."""
    result = postgres_adapter.execute("SELECT 1 as x, 2 as y")
    row = result.fetchone()
    assert row == (1, 2)


def test_postgres_adapter_create_insert_query(clean_postgres):
    """Test creating table, inserting data, and querying."""
    clean_postgres.execute("CREATE TABLE test (id INT, name VARCHAR(50))")
    clean_postgres.execute("INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob')")

    result = clean_postgres.execute("SELECT name FROM test ORDER BY id")
    rows = result.fetchall()
    assert rows == [("Alice",), ("Bob",)]


def test_postgres_adapter_executemany(clean_postgres):
    """Test executemany."""
    clean_postgres.execute("CREATE TABLE test (x INT, y INT)")
    clean_postgres.executemany("INSERT INTO test VALUES (%s, %s)", [(1, 2), (3, 4), (5, 6)])

    result = clean_postgres.execute("SELECT COUNT(*) FROM test")
    assert result.fetchone()[0] == 3


def test_postgres_adapter_get_tables(clean_postgres):
    """Test getting table list."""
    clean_postgres.execute("CREATE TABLE test1 (x INT)")
    clean_postgres.execute("CREATE TABLE test2 (x INT)")

    tables = clean_postgres.get_tables()
    table_names = {t["table_name"] for t in tables}
    assert "test1" in table_names
    assert "test2" in table_names


def test_postgres_adapter_get_columns(clean_postgres):
    """Test getting column list."""
    clean_postgres.execute("CREATE TABLE test (id INT, name VARCHAR(50), age INT)")

    columns = clean_postgres.get_columns("test")
    assert len(columns) == 3
    col_names = {c["column_name"] for c in columns}
    assert "id" in col_names
    assert "name" in col_names
    assert "age" in col_names


def test_semantic_layer_with_postgres_url(clean_postgres):
    """Test SemanticLayer with Postgres connection URL."""
    # Create test data
    clean_postgres.execute("CREATE TABLE orders (order_id INT, amount DECIMAL)")
    clean_postgres.execute("INSERT INTO orders VALUES (1, 100.0), (2, 200.0), (3, 300.0)")

    # Create semantic layer
    layer = SemanticLayer(connection=POSTGRES_URL)
    assert layer.dialect == "postgres"

    # Add model
    orders = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        metrics=[Metric(name="total_revenue", agg="sum", sql="amount")],
    )
    layer.add_model(orders)

    # Query
    result = layer.query(metrics=["orders.total_revenue"])
    row = result.fetchone()
    assert row[0] == 600.0


def test_semantic_layer_postgres_with_dimensions(clean_postgres):
    """Test querying with dimensions."""
    clean_postgres.execute(
        """
        CREATE TABLE orders (
            order_id INT,
            customer_id INT,
            status VARCHAR(20),
            amount DECIMAL
        )
    """
    )
    clean_postgres.execute(
        """
        INSERT INTO orders VALUES
            (1, 1, 'completed', 100.0),
            (2, 1, 'pending', 200.0),
            (3, 2, 'completed', 300.0),
            (4, 2, 'completed', 400.0)
    """
    )

    layer = SemanticLayer(connection=POSTGRES_URL)

    orders = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        dimensions=[Dimension(name="status", type="categorical")],
        metrics=[
            Metric(name="total_revenue", agg="sum", sql="amount"),
            Metric(name="order_count", agg="count", sql="order_id"),
        ],
    )
    layer.add_model(orders)

    result = layer.query(metrics=["orders.total_revenue", "orders.order_count"], dimensions=["orders.status"])

    rows = result.fetchall()
    # Should have 2 rows (completed, pending)
    assert len(rows) == 2

    results_dict = {row[0]: {"revenue": row[1], "count": row[2]} for row in rows}
    assert results_dict["completed"]["revenue"] == 800.0
    assert results_dict["completed"]["count"] == 3
    assert results_dict["pending"]["revenue"] == 200.0
    assert results_dict["pending"]["count"] == 1


def test_semantic_layer_postgres_with_joins(clean_postgres):
    """Test joins work with Postgres."""
    clean_postgres.execute(
        """
        CREATE TABLE orders (
            order_id INT PRIMARY KEY,
            customer_id INT,
            amount DECIMAL
        )
    """
    )
    clean_postgres.execute(
        """
        CREATE TABLE customers (
            customer_id INT PRIMARY KEY,
            name VARCHAR(50),
            region VARCHAR(50)
        )
    """
    )
    clean_postgres.execute("INSERT INTO customers VALUES (1, 'Alice', 'US'), (2, 'Bob', 'EU')")
    clean_postgres.execute(
        """
        INSERT INTO orders VALUES
            (1, 1, 100.0),
            (2, 1, 200.0),
            (3, 2, 300.0)
    """
    )

    layer = SemanticLayer(connection=POSTGRES_URL)

    orders = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        metrics=[Metric(name="total_revenue", agg="sum", sql="amount")],
        relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
    )

    customers = Model(
        name="customers",
        table="customers",
        primary_key="customer_id",
        dimensions=[Dimension(name="region", type="categorical")],
    )

    layer.add_model(orders)
    layer.add_model(customers)

    # Query across models
    result = layer.query(metrics=["orders.total_revenue"], dimensions=["customers.region"])

    rows = result.fetchall()
    results_dict = {row[0]: row[1] for row in rows}
    assert results_dict["US"] == 300.0
    assert results_dict["EU"] == 300.0


def test_semantic_layer_postgres_sql_method(clean_postgres):
    """Test SQL query rewriter with Postgres."""
    clean_postgres.execute("CREATE TABLE orders (order_id INT, amount DECIMAL, status VARCHAR(20))")
    clean_postgres.execute("INSERT INTO orders VALUES (1, 100.0, 'completed'), (2, 200.0, 'pending')")

    layer = SemanticLayer(connection=POSTGRES_URL)

    orders = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        dimensions=[Dimension(name="status", type="categorical")],
        metrics=[Metric(name="total_revenue", agg="sum", sql="amount")],
    )
    layer.add_model(orders)

    # Query using SQL method
    result = layer.sql("SELECT orders.total_revenue, orders.status FROM orders WHERE orders.status = 'completed'")

    row = result.fetchone()
    # Note: Column order might vary, check by description
    cols = [desc.name for desc in result.description]
    row_dict = dict(zip(cols, row))
    assert row_dict["total_revenue"] == 100.0
    assert row_dict["status"] == "completed"


def test_postgres_dialect_inference():
    """Test that Postgres URL correctly sets dialect."""
    layer = SemanticLayer(connection=POSTGRES_URL)
    assert layer.dialect == "postgres"
    assert layer.adapter.dialect == "postgres"


def test_semantic_layer_postgres_multiple_metrics(clean_postgres):
    """Test multiple metrics in single query."""
    clean_postgres.execute(
        """
        CREATE TABLE products (
            product_id INT PRIMARY KEY,
            price DECIMAL,
            quantity INT
        )
    """
    )
    clean_postgres.execute("INSERT INTO products VALUES (1, 100, 5), (2, 200, 3), (3, 150, 7)")

    layer = SemanticLayer(connection=POSTGRES_URL)
    products = Model(
        name="products",
        table="products",
        primary_key="product_id",
        metrics=[
            Metric(name="total_price", agg="sum", sql="price"),
            Metric(name="avg_price", agg="avg", sql="price"),
            Metric(name="total_quantity", agg="sum", sql="quantity"),
            Metric(name="product_count", agg="count", sql="product_id"),
        ],
    )
    layer.add_model(products)

    result = layer.query(
        metrics=["products.total_price", "products.avg_price", "products.total_quantity", "products.product_count"]
    )
    row = result.fetchone()
    cols = [desc.name for desc in result.description]
    row_dict = dict(zip(cols, row))

    assert row_dict["total_price"] == 450
    assert row_dict["avg_price"] == 150.0
    assert row_dict["total_quantity"] == 15
    assert row_dict["product_count"] == 3


def test_semantic_layer_postgres_filters(clean_postgres):
    """Test filters with WHERE clause through query method."""
    clean_postgres.execute(
        """
        CREATE TABLE inventory (
            item_id INT PRIMARY KEY,
            category VARCHAR(20),
            quantity INT
        )
    """
    )
    clean_postgres.execute(
        """
        INSERT INTO inventory VALUES
            (1, 'A', 100),
            (2, 'A', 200),
            (3, 'B', 150),
            (4, 'B', 250)
    """
    )

    layer = SemanticLayer(connection=POSTGRES_URL)
    inventory = Model(
        name="inventory",
        table="inventory",
        primary_key="item_id",
        dimensions=[Dimension(name="category", type="categorical")],
        metrics=[Metric(name="total_quantity", agg="sum", sql="quantity")],
    )
    layer.add_model(inventory)

    result = layer.query(
        metrics=["inventory.total_quantity"],
        dimensions=["inventory.category"],
        filters=["inventory.category = 'A'"],
    )
    rows = result.fetchall()
    assert len(rows) == 1
    assert rows[0][1] == 300


def test_semantic_layer_postgres_order_by(clean_postgres):
    """Test ORDER BY with SemanticLayer."""
    clean_postgres.execute(
        """
        CREATE TABLE scores (
            name VARCHAR(50) PRIMARY KEY,
            score INT
        )
    """
    )
    clean_postgres.execute(
        """
        INSERT INTO scores VALUES
            ('Alice', 85),
            ('Bob', 92),
            ('Charlie', 78),
            ('Diana', 95)
    """
    )

    layer = SemanticLayer(connection=POSTGRES_URL)
    scores = Model(
        name="scores",
        table="scores",
        primary_key="name",
        dimensions=[Dimension(name="name", type="categorical")],
        metrics=[Metric(name="avg_score", agg="avg", sql="score")],
    )
    layer.add_model(scores)

    result = layer.query(dimensions=["scores.name"], metrics=["scores.avg_score"], order_by=["scores.avg_score DESC"])
    rows = result.fetchall()
    # First row should have highest score
    assert rows[0][1] == 95  # Diana
    assert rows[-1][1] == 78  # Charlie


def test_semantic_layer_postgres_limit(clean_postgres):
    """Test LIMIT with SemanticLayer."""
    clean_postgres.execute(
        """
        CREATE TABLE items (
            id INT PRIMARY KEY,
            category VARCHAR(20)
        )
    """
    )
    clean_postgres.execute("INSERT INTO items VALUES (1, 'A'), (2, 'B'), (3, 'A'), (4, 'C'), (5, 'B')")

    layer = SemanticLayer(connection=POSTGRES_URL)
    items = Model(
        name="items",
        table="items",
        primary_key="id",
        dimensions=[Dimension(name="category", type="categorical")],
        metrics=[Metric(name="count", agg="count", sql="id")],
    )
    layer.add_model(items)

    result = layer.query(dimensions=["items.category"], metrics=["items.count"], limit=2)
    rows = result.fetchall()
    assert len(rows) == 2


@pytest.mark.skip(reason="Date functions test - may have issues with validation")
def test_semantic_layer_postgres_date_functions(clean_postgres):
    """Test date/time functions in dimensions."""
    clean_postgres.execute(
        """
        CREATE TABLE events (
            event_id INT PRIMARY KEY,
            event_date DATE
        )
    """
    )
    clean_postgres.execute(
        """
        INSERT INTO events VALUES
            (1, '2024-01-15'),
            (2, '2024-01-20'),
            (3, '2024-02-10'),
            (4, '2024-02-15')
    """
    )

    layer = SemanticLayer(connection=POSTGRES_URL)
    events = Model(
        name="events",
        table="events",
        primary_key="event_id",
        dimensions=[
            Dimension(name="month", type="time", sql="TO_CHAR(event_date, 'YYYY-MM')", granularity="month"),
            Dimension(name="year", type="time", sql="EXTRACT(YEAR FROM event_date)::TEXT", granularity="year"),
        ],
        metrics=[Metric(name="event_count", agg="count", sql="event_id")],
    )
    layer.add_model(events)

    result = layer.query(dimensions=["events.month"], metrics=["events.event_count"])
    rows = result.fetchall()
    results_dict = {row[0]: row[1] for row in rows}

    assert results_dict["2024-01"] == 2
    assert results_dict["2024-02"] == 2


def test_semantic_layer_postgres_symmetric_aggregates(clean_postgres):
    """Test symmetric aggregates handle fan-out joins correctly."""
    # Create a fan-out scenario: order has multiple line_items
    clean_postgres.execute(
        """
        CREATE TABLE orders_sym (
            order_id INT PRIMARY KEY,
            subtotal DECIMAL
        )
    """
    )
    clean_postgres.execute(
        """
        CREATE TABLE line_items_sym (
            item_id INT PRIMARY KEY,
            order_id INT,
            price DECIMAL
        )
    """
    )

    clean_postgres.execute("INSERT INTO orders_sym VALUES (1, 100), (2, 200)")
    clean_postgres.execute(
        """
        INSERT INTO line_items_sym VALUES
            (1, 1, 50),
            (2, 1, 30),
            (3, 1, 20),
            (4, 2, 100),
            (5, 2, 100)
    """
    )

    layer = SemanticLayer(connection=POSTGRES_URL)

    orders = Model(
        name="orders_sym",
        table="orders_sym",
        primary_key="order_id",
        metrics=[Metric(name="total_subtotal", agg="sum", sql="subtotal")],
    )

    line_items = Model(
        name="line_items_sym",
        table="line_items_sym",
        primary_key="item_id",
        metrics=[Metric(name="total_price", agg="sum", sql="price")],
        relationships=[Relationship(name="orders_sym", type="many_to_one", foreign_key="order_id")],
    )

    layer.add_model(orders)
    layer.add_model(line_items)

    # Query both metrics - should use symmetric aggregation to avoid fan-out
    result = layer.query(metrics=["orders_sym.total_subtotal", "line_items_sym.total_price"])
    row = result.fetchone()
    cols = [desc.name for desc in result.description]
    row_dict = dict(zip(cols, row))

    # Without symmetric aggregation, total_subtotal would be inflated
    assert row_dict["total_subtotal"] == 300  # 100 + 200, not inflated
    assert row_dict["total_price"] == 300  # 50+30+20+100+100


def test_semantic_layer_postgres_multiple_joins(clean_postgres):
    """Test joining 3+ models together."""
    clean_postgres.execute(
        """
        CREATE TABLE users_multi (
            user_id INT PRIMARY KEY,
            name VARCHAR(50)
        )
    """
    )
    clean_postgres.execute(
        """
        CREATE TABLE orders_multi (
            order_id INT PRIMARY KEY,
            user_id INT,
            product_id INT,
            amount DECIMAL
        )
    """
    )
    clean_postgres.execute(
        """
        CREATE TABLE products_multi (
            product_id INT PRIMARY KEY,
            product_name VARCHAR(50)
        )
    """
    )

    clean_postgres.execute("INSERT INTO users_multi VALUES (1, 'Alice'), (2, 'Bob')")
    clean_postgres.execute("INSERT INTO products_multi VALUES (1, 'Widget'), (2, 'Gadget')")
    clean_postgres.execute(
        """
        INSERT INTO orders_multi VALUES
            (1, 1, 1, 100),
            (2, 1, 2, 150),
            (3, 2, 1, 200)
    """
    )

    layer = SemanticLayer(connection=POSTGRES_URL)

    users = Model(
        name="users_multi",
        table="users_multi",
        primary_key="user_id",
        dimensions=[Dimension(name="name", type="categorical")],
    )

    products = Model(
        name="products_multi",
        table="products_multi",
        primary_key="product_id",
        dimensions=[Dimension(name="product_name", type="categorical")],
    )

    orders = Model(
        name="orders_multi",
        table="orders_multi",
        primary_key="order_id",
        metrics=[Metric(name="total", agg="sum", sql="amount")],
        relationships=[
            Relationship(name="users_multi", type="many_to_one", foreign_key="user_id"),
            Relationship(name="products_multi", type="many_to_one", foreign_key="product_id"),
        ],
    )

    layer.add_model(users)
    layer.add_model(products)
    layer.add_model(orders)

    result = layer.query(metrics=["orders_multi.total"], dimensions=["users_multi.name", "products_multi.product_name"])
    rows = result.fetchall()
    results_dict = {(row[0], row[1]): row[2] for row in rows}

    assert results_dict[("Alice", "Widget")] == 100
    assert results_dict[("Alice", "Gadget")] == 150
    assert results_dict[("Bob", "Widget")] == 200


def test_semantic_layer_postgres_compile_method(clean_postgres):
    """Test SQL compilation without executing."""
    clean_postgres.execute("CREATE TABLE compile_test (id INT PRIMARY KEY, value DECIMAL)")
    clean_postgres.execute("INSERT INTO compile_test VALUES (1, 100), (2, 200)")

    layer = SemanticLayer(connection=POSTGRES_URL)
    model = Model(
        name="compile_test",
        table="compile_test",
        primary_key="id",
        metrics=[Metric(name="total", agg="sum", sql="value")],
    )
    layer.add_model(model)

    sql = layer.compile(metrics=["compile_test.total"])
    assert "SELECT" in sql.upper()
    assert "SUM" in sql.upper()
    assert layer.dialect == "postgres"
