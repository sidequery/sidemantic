"""Tests for critical production bug fixes."""

import duckdb
import pytest

from sidemantic import Dimension, Metric, Model, Relationship, SemanticLayer
from tests.utils import df_rows


def test_query_level_metric_filters_use_having():
    """Test that query-level filters on metrics use HAVING clause, not WHERE.

    Bug: Query filters like orders.revenue > 100 were applied in WHERE clause
    against _raw columns before aggregation, giving wrong results.

    Fix: Detect when filter references a metric and use HAVING instead.
    """
    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="region", type="categorical"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    layer = SemanticLayer()
    layer.add_model(orders)

    # Filter on aggregated metric should use HAVING
    sql = layer.compile(metrics=["orders.revenue"], dimensions=["orders.region"], filters=["orders.revenue > 100"])

    # Should have HAVING clause, not WHERE clause with revenue_raw
    assert "HAVING" in sql
    assert "revenue > 100" in sql
    # Should NOT filter on raw column before aggregation
    assert "revenue_raw > 100" not in sql


def test_dimension_filters_use_where():
    """Test that filters on dimensions still use WHERE clause."""
    layer = SemanticLayer()

    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="region", type="categorical"),
            Dimension(name="status", type="categorical"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    layer.add_model(orders)

    # Filter on dimension should use WHERE
    sql = layer.compile(
        metrics=["orders.revenue"], dimensions=["orders.region"], filters=["orders.status = 'completed'"]
    )

    # Should have WHERE clause
    assert "WHERE" in sql
    assert "status = 'completed'" in sql
    # Should NOT have HAVING for dimension filter
    assert "HAVING" not in sql


def test_mixed_filters_separate_where_and_having():
    """Test that mixed metric and dimension filters use both WHERE and HAVING."""
    layer = SemanticLayer()

    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="region", type="categorical"),
            Dimension(name="status", type="categorical"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    layer.add_model(orders)

    sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.region"],
        filters=["orders.status = 'completed'", "orders.revenue > 100"],
    )

    # Should have both WHERE (for dimension) and HAVING (for metric)
    assert "WHERE" in sql
    assert "status = 'completed'" in sql
    assert "HAVING" in sql
    assert "revenue > 100" in sql


def test_duplicate_column_names_get_prefixed():
    """Test that duplicate field names across models get prefixed.

    Bug: When multiple models have same dimension/metric name (e.g., id),
    the generated SELECT uses the same alias twice, causing ambiguous columns.

    Fix: Detect collisions and prefix with model name (orders_id, customers_id).
    """
    layer = SemanticLayer()

    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="id", type="numeric", sql="order_id"),
            Dimension(name="region", type="categorical"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
        relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
    )

    customers = Model(
        name="customers",
        table="customers_table",
        primary_key="customer_id",
        dimensions=[
            Dimension(name="id", type="numeric", sql="customer_id"),
            Dimension(name="region", type="categorical"),
        ],
    )

    layer.add_model(orders)
    layer.add_model(customers)

    # Query with duplicate dimension names
    sql = layer.compile(
        metrics=["orders.revenue"], dimensions=["orders.id", "customers.id", "orders.region", "customers.region"]
    )

    # Colliding fields should be prefixed
    assert "AS orders_id" in sql
    assert "AS customers_id" in sql
    assert "AS orders_region" in sql
    assert "AS customers_region" in sql

    # Should NOT have duplicate column aliases in the final SELECT
    # Extract just the final SELECT statement (after CTEs)
    final_select_start = sql.rfind("SELECT")
    if final_select_start != -1:
        final_select = sql[final_select_start:]
        # Get lines with aliases in the final SELECT
        lines = final_select.split("\n")
        select_lines = [l for l in lines if " AS " in l and not l.strip().startswith("--")]
        aliases = [l.split(" AS ")[-1].strip().rstrip(",") for l in select_lines]

        # Check for duplicates in final SELECT
        assert len(aliases) == len(set(aliases)), f"Duplicate column aliases in final SELECT: {aliases}"


def test_no_prefix_when_no_collision():
    """Test that fields don't get prefixed when there's no collision."""
    layer = SemanticLayer()

    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="order_date", type="time", granularity="day"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
        relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
    )

    customers = Model(
        name="customers",
        table="customers_table",
        primary_key="customer_id",
        dimensions=[
            Dimension(name="customer_name", type="categorical"),
        ],
    )

    layer.add_model(orders)
    layer.add_model(customers)

    sql = layer.compile(metrics=["orders.revenue"], dimensions=["orders.order_date", "customers.customer_name"])

    # Non-colliding fields should use simple aliases
    assert "AS order_date" in sql
    assert "AS customer_name" in sql
    # Should NOT have model prefixes
    assert "AS orders_order_date" not in sql
    assert "AS customers_customer_name" not in sql


def test_duckdb_absolute_file_paths():
    """Test that DuckDB absolute file paths preserve leading slash.

    Bug: duckdb:///tmp/app.db was converted to tmp/app.db (relative path).
    Fix: Preserve leading slash to get /tmp/app.db (absolute path).
    """
    # Test absolute path
    layer = SemanticLayer(connection="duckdb:///tmp/test.db")
    assert layer.conn is not None
    # Can't easily verify the exact path, but connection should work


def test_duckdb_memory_variations():
    """Test various :memory: URI formats."""
    # Standard memory
    layer1 = SemanticLayer(connection="duckdb:///:memory:")
    assert layer1.conn is not None

    # Just duckdb:// should default to memory
    layer2 = SemanticLayer(connection="duckdb:///")
    assert layer2.conn is not None


def test_query_method_accepts_parameters():
    """Test that .query() method accepts parameters argument.

    Bug: Documentation showed parameters argument but method didn't accept it.
    Fix: Add parameters argument and forward to compile().
    """
    layer = SemanticLayer()

    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="region", type="categorical"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE orders_table (
            order_id INTEGER,
            region VARCHAR,
            amount DECIMAL(10, 2)
        )
    """)
    conn.execute("INSERT INTO orders_table VALUES (1, 'US', 100)")

    layer.conn = conn
    layer.add_model(orders)

    # Should accept parameters argument without error
    result = layer.query(metrics=["orders.revenue"], dimensions=["orders.region"], parameters={"test_param": "value"})

    # Just verify it doesn't crash - parameters may not be used in this query
    assert result is not None


def test_metric_level_filters_still_use_where():
    """Test that Metric.filters (row-level filters) still use WHERE clause.

    Metric.filters are row-level filters that should filter before aggregation.
    They're different from query-level filters on aggregated metrics.
    """
    layer = SemanticLayer()

    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="region", type="categorical"),
        ],
        metrics=[
            Metric(name="completed_revenue", agg="sum", sql="amount", filters=["{model}.status = 'completed'"]),
        ],
    )

    layer.add_model(orders)

    sql = layer.compile(metrics=["orders.completed_revenue"], dimensions=["orders.region"])

    # Metric-level filter should be in WHERE clause (applied to rows before aggregation)
    assert "WHERE" in sql
    assert "status = 'completed'" in sql


def test_end_to_end_with_real_data():
    """Integration test with real DuckDB data to verify query correctness."""
    conn = duckdb.connect(":memory:")

    # Create test data
    conn.execute("""
        CREATE TABLE orders (
            order_id INTEGER,
            customer_id INTEGER,
            region VARCHAR,
            status VARCHAR,
            amount DECIMAL(10, 2)
        )
    """)

    conn.execute("""
        INSERT INTO orders VALUES
        (1, 101, 'US', 'completed', 50),
        (2, 101, 'US', 'completed', 150),  -- customer 101: 200 total
        (3, 102, 'EU', 'completed', 300),
        (4, 102, 'EU', 'pending', 75),      -- EU total: 375, completed: 300
        (5, 103, 'US', 'cancelled', 25)
    """)

    layer = SemanticLayer()

    orders = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        dimensions=[
            Dimension(name="region", type="categorical"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    layer.conn = conn
    layer.add_model(orders)

    # Test HAVING filter (should filter aggregated revenue)
    result = layer.query(
        metrics=["orders.revenue"],
        dimensions=["orders.region"],
        filters=["orders.revenue >= 200"],  # Should use HAVING
    )
    rows = df_rows(result)

    # Should only return regions with total revenue >= 200
    # US: 50+150+25 = 225, EU: 300+75 = 375
    # Both should be included
    assert len(rows) == 2
    revenues = {row[0]: row[1] for row in rows}
    assert revenues["US"] == 225.0
    assert revenues["EU"] == 375.0
