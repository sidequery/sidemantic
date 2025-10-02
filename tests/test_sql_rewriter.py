"""Tests for SQL query rewriter."""

import duckdb
import pytest

from sidemantic.core.dimension import Dimension
from sidemantic.core.join import Join
from sidemantic.core.measure import Measure
from sidemantic.core.model import Model
from sidemantic.core.semantic_layer import SemanticLayer
from sidemantic.sql.query_rewriter import QueryRewriter


@pytest.fixture
def semantic_layer():
    """Create semantic layer with test data."""
    layer = SemanticLayer()

    # Create orders model
    orders = Model(
        name="orders",
        table="orders",
        primary_key="id",
        dimensions=[
            Dimension(name="status", type="categorical", sql="status"),
            Dimension(name="order_date", type="time", sql="order_date", granularity="day"),
        ],
        measures=[
            Measure(name="revenue", agg="sum", expr="amount"),
            Measure(name="count", agg="count"),
        ],
        joins=[Join(name="customers", type="belongs_to", foreign_key="customer_id")],
    )

    # Create customers model
    customers = Model(
        name="customers",
        table="customers",
        primary_key="id",
        dimensions=[
            Dimension(name="region", type="categorical", sql="region"),
            Dimension(name="tier", type="categorical", sql="tier"),
        ],
        measures=[Measure(name="count", agg="count")],
        joins=[Join(name="orders", type="has_many", foreign_key="customer_id")],
    )

    layer.add_model(orders)
    layer.add_model(customers)

    # Setup test data
    layer.conn.execute("""
        CREATE TABLE orders (
            id INTEGER,
            customer_id INTEGER,
            status VARCHAR,
            order_date DATE,
            amount DECIMAL(10, 2)
        )
    """)

    layer.conn.execute("""
        INSERT INTO orders VALUES
            (1, 1, 'completed', '2024-01-01', 100.00),
            (2, 1, 'completed', '2024-01-02', 150.00),
            (3, 2, 'pending', '2024-01-03', 200.00)
    """)

    layer.conn.execute("""
        CREATE TABLE customers (
            id INTEGER,
            region VARCHAR,
            tier VARCHAR
        )
    """)

    layer.conn.execute("""
        INSERT INTO customers VALUES
            (1, 'US', 'premium'),
            (2, 'EU', 'standard')
    """)

    return layer


def test_simple_metric_query(semantic_layer):
    """Test rewriting simple metric query."""
    sql = "SELECT orders.revenue FROM orders"

    result = semantic_layer.sql(sql)
    df = result.fetchdf()

    assert len(df) == 1
    assert df["revenue"][0] == 450.00


def test_metric_with_dimension(semantic_layer):
    """Test rewriting metric with dimension."""
    sql = "SELECT orders.revenue, orders.status FROM orders"

    result = semantic_layer.sql(sql)
    df = result.fetchdf()

    assert len(df) == 2
    completed = df[df["status"] == "completed"]
    assert completed["revenue"].values[0] == 250.00


def test_metric_with_filter(semantic_layer):
    """Test rewriting query with WHERE clause."""
    sql = "SELECT orders.revenue FROM orders WHERE orders.status = 'completed'"

    result = semantic_layer.sql(sql)
    df = result.fetchdf()

    assert len(df) == 1
    assert df["revenue"][0] == 250.00


def test_multiple_filters(semantic_layer):
    """Test rewriting query with multiple AND filters."""
    sql = """
        SELECT orders.revenue
        FROM orders
        WHERE orders.status = 'completed'
        AND orders.order_date >= '2024-01-01'
    """

    result = semantic_layer.sql(sql)
    df = result.fetchdf()

    assert len(df) == 1
    assert df["revenue"][0] == 250.00


def test_order_by(semantic_layer):
    """Test rewriting query with ORDER BY."""
    sql = "SELECT orders.revenue, orders.status FROM orders ORDER BY orders.status DESC"

    result = semantic_layer.sql(sql)
    df = result.fetchdf()

    assert len(df) == 2
    assert df["status"].tolist() == ["pending", "completed"]


def test_limit(semantic_layer):
    """Test rewriting query with LIMIT."""
    sql = "SELECT orders.revenue, orders.status FROM orders LIMIT 1"

    result = semantic_layer.sql(sql)
    df = result.fetchdf()

    assert len(df) == 1


def test_join_query(semantic_layer):
    """Test query that requires join."""
    sql = "SELECT orders.revenue, customers.region FROM orders"

    result = semantic_layer.sql(sql)
    df = result.fetchdf()

    assert len(df) == 2
    assert set(df["region"].tolist()) == {"US", "EU"}


def test_join_with_filter(semantic_layer):
    """Test query with filter on joined table."""
    sql = """
        SELECT orders.revenue, customers.region
        FROM orders
        WHERE customers.region = 'US'
    """

    result = semantic_layer.sql(sql)
    df = result.fetchdf()

    assert len(df) == 1
    assert df["revenue"][0] == 250.00
    assert df["region"][0] == "US"


def test_invalid_field(semantic_layer):
    """Test error for invalid field reference."""
    sql = "SELECT orders.invalid_field FROM orders"

    with pytest.raises(ValueError, match="not found"):
        semantic_layer.sql(sql)


def test_missing_table_prefix(semantic_layer):
    """Test that table prefix can be inferred from FROM clause."""
    sql = "SELECT revenue FROM orders"

    # Should work now with table inference
    result = semantic_layer.sql(sql)
    df = result.fetchdf()
    assert len(df) == 1  # Should aggregate all rows


def test_unsupported_aggregation(semantic_layer):
    """Test error for unsupported aggregation function."""
    sql = "SELECT COUNT(*) FROM orders"

    with pytest.raises(ValueError, match="must be defined as a metric"):
        semantic_layer.sql(sql)


def test_rewriter_directly():
    """Test QueryRewriter class directly."""
    layer = SemanticLayer()

    orders = Model(
        name="orders",
        table="orders",
        primary_key="id",
        dimensions=[Dimension(name="status", type="categorical", sql="status")],
        measures=[Measure(name="revenue", agg="sum", expr="amount")],
    )
    layer.add_model(orders)

    rewriter = QueryRewriter(layer.graph, dialect="duckdb")

    # Test rewriting
    sql = "SELECT orders.revenue, orders.status FROM orders WHERE orders.status = 'completed'"
    rewritten = rewriter.rewrite(sql)

    # Should contain semantic layer SQL structure
    assert "WITH orders_cte AS" in rewritten
    assert "SUM(orders_cte.revenue_raw) AS revenue" in rewritten
    assert "orders_cte.status = 'completed'" in rewritten


def test_dimension_only_query(semantic_layer):
    """Test query with only dimensions (no metrics)."""
    sql = "SELECT orders.status FROM orders"

    result = semantic_layer.sql(sql)
    df = result.fetchdf()

    assert len(df) == 2
    assert set(df["status"].tolist()) == {"completed", "pending"}
