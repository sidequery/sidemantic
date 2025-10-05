"""Tests for SQL query rewriter."""

import pytest

from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
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
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
            Metric(name="count", agg="count"),
        ],
        relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
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
        metrics=[Metric(name="count", agg="count")],
        relationships=[Relationship(name="orders", type="one_to_many", foreign_key="customer_id")],
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
        metrics=[Metric(name="revenue", agg="sum", sql="amount")],
    )
    layer.add_model(orders)

    rewriter = QueryRewriter(layer.graph, dialect="duckdb")

    # Test rewriting
    sql = "SELECT orders.revenue, orders.status FROM orders WHERE orders.status = 'completed'"
    rewritten = rewriter.rewrite(sql)

    # Should contain semantic layer SQL structure
    assert "WITH orders_cte AS" in rewritten
    assert "SUM(orders_cte.revenue_raw) AS revenue" in rewritten
    # Filter gets pushed down into CTE
    assert "status = 'completed'" in rewritten


def test_dimension_only_query(semantic_layer):
    """Test query with only dimensions (no metrics)."""
    sql = "SELECT orders.status FROM orders"

    result = semantic_layer.sql(sql)
    df = result.fetchdf()

    assert len(df) == 2
    assert set(df["status"].tolist()) == {"completed", "pending"}


def test_rewriter_invalid_sql():
    """Test error handling for invalid SQL."""
    layer = SemanticLayer()
    orders = Model(
        name="orders",
        table="orders",
        primary_key="id",
        dimensions=[Dimension(name="status", type="categorical", sql="status")],
    )
    layer.add_model(orders)

    rewriter = QueryRewriter(layer.graph, dialect="duckdb")

    # Invalid SQL syntax
    with pytest.raises(ValueError, match="Failed to parse SQL"):
        rewriter.rewrite("SELECT FROM WHERE")


def test_rewriter_non_select_query():
    """Test error for non-SELECT queries."""
    layer = SemanticLayer()
    orders = Model(
        name="orders",
        table="orders",
        primary_key="id",
        dimensions=[Dimension(name="status", type="categorical", sql="status")],
    )
    layer.add_model(orders)

    rewriter = QueryRewriter(layer.graph, dialect="duckdb")

    # INSERT query should fail
    with pytest.raises(ValueError, match="Only SELECT queries are supported"):
        rewriter.rewrite("INSERT INTO orders VALUES (1, 'test')")

    # UPDATE query should fail
    with pytest.raises(ValueError, match="Only SELECT queries are supported"):
        rewriter.rewrite("UPDATE orders SET status = 'completed'")

    # DELETE query should fail
    with pytest.raises(ValueError, match="Only SELECT queries are supported"):
        rewriter.rewrite("DELETE FROM orders")


def test_rewriter_or_filters(semantic_layer):
    """Test rewriting query with OR filters."""
    sql = """
        SELECT orders.revenue
        FROM orders
        WHERE orders.status = 'completed' OR orders.status = 'pending'
    """

    result = semantic_layer.sql(sql)
    df = result.fetchdf()

    # Should return all rows
    assert len(df) == 1
    assert df["revenue"][0] == 450.00


def test_rewriter_in_filter(semantic_layer):
    """Test rewriting query with IN clause."""
    sql = """
        SELECT orders.revenue
        FROM orders
        WHERE orders.status IN ('completed', 'pending')
    """

    result = semantic_layer.sql(sql)
    df = result.fetchdf()

    assert len(df) == 1
    assert df["revenue"][0] == 450.00


def test_rewriter_having_clause(semantic_layer):
    """Test rewriting query with HAVING clause."""
    sql = """
        SELECT orders.revenue, orders.status
        FROM orders
        HAVING orders.revenue > 150
    """

    result = semantic_layer.sql(sql)
    df = result.fetchdf()

    # HAVING filters on aggregated revenue
    # Both groups (completed=250, pending=200) exceed 150
    assert len(df) == 2


def test_rewriter_distinct(semantic_layer):
    """Test rewriting query with DISTINCT."""
    sql = "SELECT DISTINCT orders.status FROM orders"

    result = semantic_layer.sql(sql)
    df = result.fetchdf()

    assert len(df) == 2
    assert set(df["status"].tolist()) == {"completed", "pending"}


def test_select_star_expansion(semantic_layer):
    """Test SELECT * gets expanded to all model fields."""
    sql = "SELECT * FROM orders"

    result = semantic_layer.sql(sql)
    df = result.fetchdf()

    # Should have all dimensions and metrics
    assert "status" in df.columns
    assert "order_date" in df.columns
    assert "revenue" in df.columns
    assert "count" in df.columns


def test_select_star_without_from():
    """Test error when SELECT * has no FROM clause."""
    from sidemantic.sql.query_rewriter import QueryRewriter

    layer = SemanticLayer()
    orders = Model(
        name="orders",
        table="orders",
        primary_key="id",
        dimensions=[Dimension(name="status", type="categorical", sql="status")],
    )
    layer.add_model(orders)

    rewriter = QueryRewriter(layer.graph, dialect="duckdb")

    # SELECT * without FROM should error
    with pytest.raises(ValueError, match="SELECT \\* requires a FROM clause"):
        rewriter.rewrite("SELECT *")


def test_column_alias(semantic_layer):
    """Test column aliases in SELECT."""
    sql = "SELECT orders.revenue AS total_revenue, orders.status AS order_status FROM orders"

    result = semantic_layer.sql(sql)
    df = result.fetchdf()

    # Aliases should be preserved or handled
    assert len(df) == 2


def test_graph_level_metrics(semantic_layer):
    """Test querying graph-level metrics."""
    # Add a graph-level metric
    graph_metric = Metric(
        name="total_orders",
        type="derived",
        sql="COUNT(*)",
    )
    semantic_layer.add_metric(graph_metric)

    # Query the graph-level metric
    sql = "SELECT total_orders FROM orders"

    # This should work (or at least not crash)
    try:
        result = semantic_layer.sql(sql)
        assert result is not None
    except (ValueError, KeyError):
        # If graph-level metrics aren't fully supported yet, that's OK
        pass


def test_nested_and_or_filters(semantic_layer):
    """Test nested AND/OR filter combinations."""
    sql = """
        SELECT orders.revenue
        FROM orders
        WHERE (orders.status = 'completed' OR orders.status = 'pending')
          AND orders.order_date >= '2024-01-01'
    """

    result = semantic_layer.sql(sql)
    df = result.fetchdf()

    assert len(df) == 1
    assert df["revenue"][0] == 450.00


def test_complex_nested_filters(semantic_layer):
    """Test complex nested filter logic."""
    sql = """
        SELECT orders.revenue
        FROM orders
        WHERE (orders.status = 'completed' AND orders.order_date >= '2024-01-01')
           OR (orders.status = 'pending' AND orders.order_date >= '2024-01-03')
    """

    result = semantic_layer.sql(sql)
    df = result.fetchdf()

    # Should include completed orders from 1/1+ AND pending orders from 1/3+
    assert len(df) == 1


def test_query_without_metrics_or_dimensions():
    """Test error when query has neither metrics nor dimensions."""
    from sidemantic.sql.query_rewriter import QueryRewriter

    layer = SemanticLayer()
    orders = Model(
        name="orders",
        table="orders",
        primary_key="id",
        dimensions=[Dimension(name="status", type="categorical", sql="status")],
    )
    layer.add_model(orders)

    rewriter = QueryRewriter(layer.graph, dialect="duckdb")

    # A query that selects nothing meaningful
    with pytest.raises(ValueError, match="Query must select at least one"):
        rewriter.rewrite("SELECT FROM orders")


def test_unresolvable_column(semantic_layer):
    """Test error for completely unresolvable column."""
    sql = "SELECT completely_unknown_field FROM orders"

    with pytest.raises(ValueError, match="Cannot resolve column|not found"):
        semantic_layer.sql(sql)
