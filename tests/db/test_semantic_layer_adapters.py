"""Test SemanticLayer works with different database adapters."""

import pytest

from sidemantic import Dimension, Metric, Model, SemanticLayer
from sidemantic.db.duckdb import DuckDBAdapter


def test_semantic_layer_with_duckdb_url():
    """Test SemanticLayer with DuckDB URL."""
    layer = SemanticLayer(connection="duckdb:///:memory:")
    assert layer.dialect == "duckdb"
    assert layer.adapter.dialect == "duckdb"


def test_semantic_layer_with_duckdb_adapter():
    """Test SemanticLayer with DuckDB adapter instance."""
    adapter = DuckDBAdapter(":memory:")
    layer = SemanticLayer(connection=adapter)
    assert layer.dialect == "duckdb"
    assert layer.adapter is adapter


def test_semantic_layer_backward_compat_conn_property():
    """Test backward compatibility .conn property."""
    layer = SemanticLayer(connection="duckdb:///:memory:")

    # Should be able to access raw connection
    conn = layer.conn
    assert conn is not None

    # Should be able to use it directly (backward compat)
    result = conn.execute("SELECT 42")
    assert result.fetchone()[0] == 42


def test_semantic_layer_backward_compat_conn_setter():
    """Test backward compatibility .conn setter."""
    layer = SemanticLayer(connection="duckdb:///:memory:")

    # Create new connection with test data
    import duckdb

    new_conn = duckdb.connect(":memory:")
    new_conn.execute("CREATE TABLE test (x INT)")
    new_conn.execute("INSERT INTO test VALUES (123)")

    # Set the connection (for backward compat with tests)
    layer.conn = new_conn

    # Should use the new connection
    result = layer.adapter.execute("SELECT x FROM test")
    assert result.fetchone()[0] == 123


def test_semantic_layer_query_with_adapter():
    """Test querying through SemanticLayer using adapter."""
    layer = SemanticLayer()

    # Setup test data
    layer.adapter.execute("CREATE TABLE orders (order_id INT, amount DECIMAL)")
    layer.adapter.execute("INSERT INTO orders VALUES (1, 100.0), (2, 200.0)")

    # Create model
    orders = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        metrics=[Metric(name="total_revenue", agg="sum", sql="amount")],
    )
    layer.add_model(orders)

    # Query should work through adapter
    result = layer.query(metrics=["orders.total_revenue"])
    row = result.fetchone()
    assert row[0] == 300.0


def test_semantic_layer_invalid_connection_type():
    """Test error on invalid connection type."""
    with pytest.raises(ValueError, match="Unsupported connection URL"):
        SemanticLayer(connection="mysql://invalid")


def test_semantic_layer_invalid_connection_object():
    """Test error on invalid connection object."""
    with pytest.raises(TypeError, match="must be a string URL or BaseDatabaseAdapter"):
        SemanticLayer(connection=123)


def test_postgres_url_parsing():
    """Test PostgreSQL URL is recognized (without connecting)."""
    # This will fail on connection but should parse the URL correctly
    with pytest.raises(ImportError, match="psycopg"):
        SemanticLayer(connection="postgres://user:pass@localhost/db")
