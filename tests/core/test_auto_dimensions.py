"""Tests for auto_dimensions feature."""

import duckdb
import pytest

from sidemantic import Metric, Model, SemanticLayer


@pytest.fixture
def db():
    """Create a DuckDB connection with test tables."""
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            status VARCHAR,
            amount DECIMAL(10,2),
            is_returned BOOLEAN,
            created_at TIMESTAMP,
            order_date DATE
        )
    """)
    conn.execute("""
        INSERT INTO orders VALUES
        (1, 100, 'completed', 99.99, false, '2024-01-15 10:30:00', '2024-01-15'),
        (2, 101, 'pending', 49.50, false, '2024-01-16 14:00:00', '2024-01-16'),
        (3, 100, 'completed', 150.00, true, '2024-01-17 09:00:00', '2024-01-17')
    """)
    return conn


@pytest.fixture
def layer(db):
    """Create a SemanticLayer with the test DB connection."""
    from sidemantic.db.duckdb import DuckDBAdapter

    adapter = DuckDBAdapter()
    adapter.conn = db
    return SemanticLayer(connection=adapter, auto_register=False)


def test_auto_dimensions_from_table(layer):
    """auto_dimensions=True introspects all columns from a table."""
    model = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        auto_dimensions=True,
        metrics=[Metric(name="revenue", sql="SUM(amount)")],
    )
    layer.add_model(model)

    # Should have dimensions for all non-PK columns
    dim_names = {d.name for d in model.dimensions}
    assert "customer_id" in dim_names
    assert "status" in dim_names
    assert "amount" in dim_names
    assert "is_returned" in dim_names
    assert "created_at" in dim_names
    assert "order_date" in dim_names

    # PK should be excluded
    assert "order_id" not in dim_names


def test_auto_dimensions_type_mapping(layer):
    """Introspected dimensions get correct types from DB column types."""
    model = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        auto_dimensions=True,
        metrics=[Metric(name="count", sql="COUNT(*)")],
    )
    layer.add_model(model)

    status_dim = model.get_dimension("status")
    assert status_dim.type == "categorical"

    amount_dim = model.get_dimension("amount")
    assert amount_dim.type == "numeric"

    is_returned_dim = model.get_dimension("is_returned")
    assert is_returned_dim.type == "boolean"

    created_at_dim = model.get_dimension("created_at")
    assert created_at_dim.type == "time"
    assert created_at_dim.granularity == "second"

    order_date_dim = model.get_dimension("order_date")
    assert order_date_dim.type == "time"
    assert order_date_dim.granularity == "day"


def test_explicit_dimensions_take_precedence(layer):
    """Explicitly defined dimensions are preserved, introspection fills gaps."""
    from sidemantic.core.dimension import Dimension

    model = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        auto_dimensions=True,
        dimensions=[
            Dimension(name="status", type="categorical", label="Order Status"),
        ],
        metrics=[Metric(name="revenue", sql="SUM(amount)")],
    )
    layer.add_model(model)

    # Explicit dimension should keep its label
    status_dim = model.get_dimension("status")
    assert status_dim.label == "Order Status"

    # Other columns should be auto-introspected
    assert model.get_dimension("amount") is not None
    assert model.get_dimension("created_at") is not None


def test_auto_dimensions_false_no_introspection(layer):
    """auto_dimensions=False (default) doesn't introspect."""
    model = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        auto_dimensions=False,
        metrics=[Metric(name="revenue", sql="SUM(amount)")],
    )
    layer.add_model(model)

    assert len(model.dimensions) == 0


def test_auto_dimensions_default_is_false():
    """Default value of auto_dimensions is False."""
    model = Model(name="test", table="test", primary_key="id")
    assert model.auto_dimensions is False


def test_auto_dimensions_sql_model(layer):
    """auto_dimensions works for SQL-based (derived) models."""
    model = Model(
        name="order_summary",
        sql="SELECT order_id, status, amount, created_at FROM orders",
        primary_key="order_id",
        auto_dimensions=True,
        metrics=[Metric(name="total", sql="SUM(amount)")],
    )
    layer.add_model(model)

    dim_names = {d.name for d in model.dimensions}
    assert "status" in dim_names
    assert "amount" in dim_names
    assert "created_at" in dim_names
    assert "order_id" not in dim_names  # PK excluded


def test_auto_dimensions_composite_pk(layer):
    """Composite primary key columns are all excluded."""
    layer.adapter.execute("""
        CREATE TABLE line_items (
            order_id INTEGER,
            line_number INTEGER,
            product_name VARCHAR,
            quantity INTEGER,
            PRIMARY KEY (order_id, line_number)
        )
    """)

    model = Model(
        name="line_items",
        table="line_items",
        primary_key=["order_id", "line_number"],
        auto_dimensions=True,
        metrics=[Metric(name="total_qty", sql="SUM(quantity)")],
    )
    layer.add_model(model)

    dim_names = {d.name for d in model.dimensions}
    assert "order_id" not in dim_names
    assert "line_number" not in dim_names
    assert "product_name" in dim_names
    assert "quantity" in dim_names


def test_auto_dimensions_query_works(layer):
    """End-to-end: auto-introspected dimensions can be queried."""
    model = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        auto_dimensions=True,
        metrics=[Metric(name="revenue", sql="SUM(amount)")],
    )
    layer.add_model(model)

    result = layer.query(
        metrics=["orders.revenue"],
        dimensions=["orders.status"],
    )
    rows = result.fetchall()
    # completed: 99.99 + 150.00, pending: 49.50
    assert len(rows) == 2


def test_auto_dimensions_time_granularity_query(layer):
    """Auto-introspected time dimensions support granularity suffixes."""
    model = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        auto_dimensions=True,
        metrics=[Metric(name="revenue", sql="SUM(amount)")],
    )
    layer.add_model(model)

    result = layer.query(
        metrics=["orders.revenue"],
        dimensions=["orders.order_date__month"],
    )
    rows = result.fetchall()
    # All in January 2024
    assert len(rows) == 1


def test_map_db_type():
    """Test the type mapping function directly."""
    m = SemanticLayer._map_db_type

    assert m("VARCHAR") == ("categorical", None)
    assert m("VARCHAR(255)") == ("categorical", None)
    assert m("TEXT") == ("categorical", None)

    assert m("INTEGER") == ("numeric", None)
    assert m("BIGINT") == ("numeric", None)
    assert m("DECIMAL(10,2)") == ("numeric", None)
    assert m("FLOAT") == ("numeric", None)
    assert m("DOUBLE") == ("numeric", None)

    assert m("BOOLEAN") == ("boolean", None)
    assert m("BOOL") == ("boolean", None)

    assert m("DATE") == ("time", "day")
    assert m("TIMESTAMP") == ("time", "second")
    assert m("TIMESTAMPTZ") == ("time", "second")
    assert m("TIMESTAMP WITH TIME ZONE") == ("time", "second")
    assert m("DATETIME") == ("time", "second")

    # Unknown types default to categorical
    assert m("JSON") == ("categorical", None)
    assert m("BLOB") == ("categorical", None)
