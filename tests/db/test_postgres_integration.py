"""Integration tests for PostgreSQL adapter against real database.

Run with: docker compose up -d && pytest -m integration tests/db/test_postgres_integration.py -v
"""

import os

import pytest

from sidemantic import Dimension, Metric, Model, SemanticLayer

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

    from sidemantic import Relationship

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
