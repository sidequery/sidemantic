"""Basic tests for Sidemantic core functionality."""

import duckdb
import pytest

from sidemantic import Dimension, Metric, Model, Relationship, SemanticLayer
from tests.utils import df_rows


def test_create_model():
    """Test creating a basic model."""
    model = Model(
        name="orders",
        table="public.orders",
        primary_key="order_id",
        dimensions=[Dimension(name="status", type="categorical")],
        metrics=[Metric(name="count", agg="count")],
    )

    assert model.name == "orders"
    assert model.table == "public.orders"
    assert model.primary_key == "order_id"
    assert len(model.dimensions) == 1
    assert len(model.metrics) == 1


def test_semantic_layer(layer):
    """Test semantic layer basic operations."""
    orders = Model(
        name="orders",
        table="public.orders",
        primary_key="order_id",
        dimensions=[Dimension(name="status", type="categorical")],
        metrics=[Metric(name="revenue", agg="sum", sql="order_amount")],
    )

    layer.add_model(orders)

    assert "orders" in layer.list_models()
    assert layer.get_model("orders").name == "orders"


def test_join_path_discovery(layer):
    """Test automatic join path discovery."""
    orders = Model(
        name="orders",
        table="public.orders",
        primary_key="order_id",
        relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
    )

    customers = Model(
        name="customers",
        table="public.customers",
        primary_key="customer_id",
    )

    layer.add_model(orders)
    layer.add_model(customers)

    # Test join path finding
    join_path = layer.graph.find_relationship_path("orders", "customers")
    assert len(join_path) == 1
    assert join_path[0].from_model == "orders"
    assert join_path[0].to_model == "customers"


def test_time_dimension_granularity():
    """Test time dimension with granularity."""
    dim = Dimension(name="created_at", type="time", granularity="day", sql="created_at")

    # Test granularity SQL generation
    sql = dim.with_granularity("month")
    assert "DATE_TRUNC('month', created_at)" in sql


def test_time_dimension_bigquery_dialect(layer):
    """Test that time dimensions generate correct BigQuery DATE_TRUNC syntax.

    BigQuery requires: DATE_TRUNC(column, MONTH)
    PostgreSQL/DuckDB use: DATE_TRUNC('month', column)
    """
    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="created_at", type="time", sql="created_at", granularity="day"),
            Dimension(name="region", type="categorical"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    layer.add_model(orders)

    # Compile with BigQuery dialect
    sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.created_at__month"],
        dialect="bigquery",
    )

    print("BigQuery SQL:")
    print(sql)

    # BigQuery syntax: DATE_TRUNC(column, MONTH) - no quotes around granularity
    assert "DATE_TRUNC(created_at, MONTH)" in sql, f"Expected BigQuery DATE_TRUNC syntax. Got: {sql}"
    # Should NOT have PostgreSQL syntax
    assert "DATE_TRUNC('month'" not in sql, f"Should not have PostgreSQL syntax. Got: {sql}"


def test_time_dimension_duckdb_dialect(layer):
    """Test that time dimensions generate correct DuckDB DATE_TRUNC syntax."""
    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="created_at", type="time", sql="created_at", granularity="day"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    layer.add_model(orders)

    # Compile with DuckDB dialect (default)
    sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.created_at__month"],
        dialect="duckdb",
    )

    print("DuckDB SQL:")
    print(sql)

    # DuckDB syntax: DATE_TRUNC('month', column)
    assert "DATE_TRUNC('month'" in sql or "DATE_TRUNC('MONTH'" in sql, f"Expected DuckDB DATE_TRUNC syntax. Got: {sql}"


def test_measure_aggregation():
    """Test measure SQL generation."""
    measure = Metric(name="revenue", agg="sum", sql="order_amount")

    sql = measure.to_sql()
    assert sql == "SUM(order_amount)"


def test_simple_metric():
    """Test creating a simple metric (now untyped with sql)."""
    metric = Metric(name="total_revenue", sql="orders.revenue")

    assert metric.name == "total_revenue"
    assert metric.type is None  # Untyped metric with sql
    assert metric.sql == "orders.revenue"


def test_ratio_metric():
    """Test creating a ratio metric."""
    metric = Metric(
        name="conversion_rate",
        type="ratio",
        numerator="orders.completed_revenue",
        denominator="orders.revenue",
    )

    assert metric.name == "conversion_rate"
    assert metric.type == "ratio"
    assert metric.numerator == "orders.completed_revenue"
    assert metric.denominator == "orders.revenue"


def test_sql_compilation(layer):
    """Test SQL query compilation."""
    orders = Model(
        name="orders",
        table="public.orders",
        primary_key="order_id",
        dimensions=[Dimension(name="status", type="categorical")],
        metrics=[Metric(name="revenue", agg="sum", sql="order_amount")],
    )

    layer.add_model(orders)

    sql = layer.compile(metrics=["orders.revenue"], dimensions=["orders.status"])

    # Check that SQL contains expected components
    assert "WITH" in sql
    assert "orders_cte" in sql
    assert "SUM" in sql
    assert "GROUP BY" in sql


def test_multi_model_query(layer):
    """Test query across multiple models."""
    orders = Model(
        name="orders",
        table="public.orders",
        primary_key="order_id",
        relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
        dimensions=[Dimension(name="status", type="categorical")],
        metrics=[Metric(name="revenue", agg="sum", sql="order_amount")],
    )

    customers = Model(
        name="customers",
        table="public.customers",
        primary_key="customer_id",
        dimensions=[Dimension(name="region", type="categorical")],
    )

    layer.add_model(orders)
    layer.add_model(customers)

    sql = layer.compile(
        metrics=["orders.revenue"],
        dimensions=["customers.region"],
    )

    # Check for join
    assert "LEFT JOIN" in sql
    assert "customers_cte" in sql


def test_duplicate_column_names_get_prefixed(layer):
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
        select_lines = [line for line in lines if " AS " in line and not line.strip().startswith("--")]
        aliases = [line.split(" AS ")[-1].strip().rstrip(",") for line in select_lines]

        # Check for duplicates in final SELECT
        assert len(aliases) == len(set(aliases)), f"Duplicate column aliases in final SELECT: {aliases}"


def test_no_prefix_when_no_collision(layer):
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


def test_count_distinct_without_sql_uses_primary_key(layer):
    """Test that count_distinct without sql field uses primary key.

    Bug: count_distinct metrics without an explicit sql field generated
    SQL that referenced a non-existent column (the metric name).

    Fix: Use the model's primary key when count_distinct has no sql,
    which gives a count of distinct rows.
    """
    layer = SemanticLayer()

    location = Model(
        name="location",
        table="dim_location",
        primary_key="sk_location_id",
        dimensions=[
            Dimension(name="city", type="categorical"),
        ],
        metrics=[
            Metric(name="count", agg="count_distinct"),  # No sql field
        ],
    )

    layer.add_model(location)

    sql = layer.compile(metrics=["location.count"], dimensions=["location.city"])

    # Should use primary key (sk_location_id) instead of non-existent "count" column
    assert "sk_location_id AS count_raw" in sql
    # Should NOT use "count AS count_raw" (the metric name doesn't exist as a column)
    assert "count AS count_raw" not in sql
    # Final aggregation should be COUNT(DISTINCT ...)
    assert "COUNT(DISTINCT" in sql


def test_count_distinct_with_explicit_sql(layer):
    """Test that count_distinct with explicit sql uses that column."""
    layer = SemanticLayer()

    orders = Model(
        name="orders",
        table="orders_table",
        primary_key="order_id",
        dimensions=[
            Dimension(name="region", type="categorical"),
        ],
        metrics=[
            Metric(name="unique_customers", agg="count_distinct", sql="customer_id"),
        ],
    )

    layer.add_model(orders)

    sql = layer.compile(metrics=["orders.unique_customers"], dimensions=["orders.region"])

    # Should use the explicit sql field
    assert "customer_id AS unique_customers_raw" in sql
    assert "COUNT(DISTINCT" in sql


def test_count_distinct_with_actual_data(layer):
    """Test count_distinct with actual DuckDB data."""
    conn = duckdb.connect(":memory:")

    conn.execute("""
        CREATE TABLE dim_location (
            sk_location_id INTEGER PRIMARY KEY,
            city VARCHAR
        )
    """)

    conn.execute("""
        INSERT INTO dim_location VALUES
        (1, 'New York'),
        (2, 'New York'),
        (3, 'Los Angeles'),
        (4, 'Los Angeles'),
        (5, 'Los Angeles')
    """)

    layer = SemanticLayer()

    location = Model(
        name="location",
        table="dim_location",
        primary_key="sk_location_id",
        dimensions=[
            Dimension(name="city", type="categorical"),
        ],
        metrics=[
            Metric(name="location_count", agg="count_distinct"),  # Count distinct rows
        ],
    )

    layer.conn = conn
    layer.add_model(location)

    result = layer.query(metrics=["location.location_count"], dimensions=["location.city"])
    rows = df_rows(result)

    # New York: 2 distinct locations, Los Angeles: 3 distinct locations
    counts = {row[0]: row[1] for row in rows}
    assert counts["New York"] == 2
    assert counts["Los Angeles"] == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
