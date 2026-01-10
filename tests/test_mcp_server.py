"""Tests for MCP server functionality."""

import json
import tempfile
from decimal import Decimal
from pathlib import Path

import pytest

from sidemantic.mcp_server import create_chart, get_models, initialize_layer, list_models, run_query


@pytest.fixture
def demo_layer():
    """Create a demo semantic layer for testing."""
    # Create a temporary directory for testing
    tmpdir = tempfile.mkdtemp()
    tmpdir_path = Path(tmpdir)

    # Create a simple model file
    model_yaml = """
models:
  - name: orders
    table: orders_table
    dimensions:
      - name: order_id
        sql: order_id
        type: categorical
      - name: customer_name
        sql: customer_name
        type: categorical
      - name: order_date
        sql: order_date
        type: time
        granularity: day
    metrics:
      - name: total_revenue
        agg: sum
        sql: amount
      - name: order_count
        agg: count_distinct
        sql: order_id
"""
    model_file = tmpdir_path / "orders.yml"
    model_file.write_text(model_yaml)

    # Initialize the layer
    layer = initialize_layer(str(tmpdir_path), db_path=":memory:")

    # Create the table with some test data using adapter interface
    layer.adapter.execute("""
        CREATE TABLE orders_table (
            id INTEGER,
            order_id VARCHAR,
            customer_name VARCHAR,
            order_date DATE,
            amount DECIMAL
        )
    """)
    layer.adapter.execute("""
        INSERT INTO orders_table VALUES
            (1, '1', 'Alice', '2024-01-01', 100),
            (2, '2', 'Bob', '2024-01-02', 200),
            (3, '3', 'Alice', '2024-01-03', 150)
    """)

    yield layer

    # Cleanup
    import shutil

    shutil.rmtree(tmpdir)


def test_list_models(demo_layer):
    """Test listing all models."""
    models = list_models()

    assert len(models) == 1
    assert models[0]["name"] == "orders"
    assert models[0]["table"] == "orders_table"
    assert len(models[0]["dimensions"]) == 3
    assert len(models[0]["metrics"]) == 2
    assert "order_id" in models[0]["dimensions"]
    assert "customer_name" in models[0]["dimensions"]
    assert "order_date" in models[0]["dimensions"]
    assert "total_revenue" in models[0]["metrics"]
    assert "order_count" in models[0]["metrics"]


def test_get_models(demo_layer):
    """Test getting detailed model information."""
    models = get_models(["orders"])

    assert len(models) == 1
    model = models[0]
    assert model["name"] == "orders"
    assert model["table"] == "orders_table"

    # Check dimensions
    assert len(model["dimensions"]) == 3
    dim_names = [d["name"] for d in model["dimensions"]]
    assert "order_id" in dim_names
    assert "customer_name" in dim_names
    assert "order_date" in dim_names

    # Check metrics
    assert len(model["metrics"]) == 2
    metric_names = [m["name"] for m in model["metrics"]]
    assert "total_revenue" in metric_names
    assert "order_count" in metric_names

    # Check metric details
    revenue_metric = next(m for m in model["metrics"] if m["name"] == "total_revenue")
    assert revenue_metric["agg"] == "sum"
    assert revenue_metric["sql"] == "amount"


def test_get_models_nonexistent(demo_layer):
    """Test getting a model that doesn't exist."""
    models = get_models(["nonexistent"])
    assert len(models) == 0


def test_get_models_multiple(demo_layer):
    """Test getting multiple models (only one exists)."""
    models = get_models(["orders", "nonexistent"])
    assert len(models) == 1
    assert models[0]["name"] == "orders"


def test_run_query_basic(demo_layer):
    """Test running a basic query."""
    result = run_query(
        dimensions=["orders.customer_name"],
        metrics=["orders.total_revenue"],
    )

    assert result["sql"] is not None
    assert "SELECT" in result["sql"].upper()
    assert "customer_name" in result["sql"]
    assert "SUM" in result["sql"].upper()
    # Should have 2 rows (Alice and Bob)
    assert result["row_count"] == 2
    assert len(result["rows"]) == 2


def test_run_query_with_filter(demo_layer):
    """Test running a query with a WHERE clause."""
    result = run_query(
        dimensions=["orders.customer_name"],
        metrics=["orders.total_revenue"],
        where="orders.customer_name = 'Alice'",
    )

    assert result["sql"] is not None
    assert "WHERE" in result["sql"].upper()
    assert "Alice" in result["sql"]


def test_run_query_with_order_by(demo_layer):
    """Test running a query with ORDER BY."""
    result = run_query(
        dimensions=["orders.customer_name"],
        metrics=["orders.total_revenue"],
        order_by=["orders.total_revenue desc"],
    )

    assert result["sql"] is not None
    assert "ORDER BY" in result["sql"].upper()


def test_run_query_with_limit(demo_layer):
    """Test running a query with LIMIT."""
    result = run_query(
        dimensions=["orders.customer_name"],
        metrics=["orders.total_revenue"],
        limit=10,
    )

    assert result["sql"] is not None
    assert "LIMIT" in result["sql"].upper()
    assert "10" in result["sql"]


def test_run_query_dimensions_only(demo_layer):
    """Test running a query with only dimensions."""
    result = run_query(
        dimensions=["orders.customer_name", "orders.order_date"],
    )

    assert result["sql"] is not None
    assert "customer_name" in result["sql"]
    assert "order_date" in result["sql"]


def test_run_query_metrics_only(demo_layer):
    """Test running a query with only metrics."""
    result = run_query(
        metrics=["orders.total_revenue", "orders.order_count"],
    )

    assert result["sql"] is not None
    assert "SUM" in result["sql"].upper()
    assert "COUNT" in result["sql"].upper()


def test_run_query_decimal_conversion(demo_layer):
    """Test that Decimal values are converted to float for JSON serialization."""
    result = run_query(
        dimensions=["orders.customer_name"],
        metrics=["orders.total_revenue"],
    )

    # Verify we have results
    assert result["row_count"] > 0
    assert len(result["rows"]) > 0

    # Check that no Decimal objects are in the results
    for row in result["rows"]:
        for key, value in row.items():
            assert not isinstance(value, Decimal), f"Found Decimal value for {key}: {value}"
            # Revenue should be a float or int
            if "revenue" in key.lower():
                assert isinstance(value, (int, float)), f"Expected numeric type for {key}, got {type(value)}"

    # Verify the entire result is JSON serializable
    try:
        json.dumps(result)
    except TypeError as e:
        pytest.fail(f"Result is not JSON serializable: {e}")


def test_create_chart_basic(demo_layer):
    """Test creating a basic chart."""
    result = create_chart(
        dimensions=["orders.customer_name"],
        metrics=["orders.total_revenue"],
        chart_type="bar",
        title="Revenue by Customer",
    )

    # Check all expected keys are present
    assert "sql" in result
    assert "vega_spec" in result
    assert "png_base64" in result
    assert "row_count" in result

    # Verify SQL was generated
    assert result["sql"] is not None
    assert "SELECT" in result["sql"].upper()

    # Verify vega spec is a dict
    assert isinstance(result["vega_spec"], dict)
    assert "data" in result["vega_spec"]

    # Verify PNG is base64 encoded
    assert result["png_base64"].startswith("data:image/png;base64,")

    # Verify row count
    assert result["row_count"] == 2  # Alice and Bob


def test_create_chart_decimal_conversion(demo_layer):
    """Test that create_chart converts Decimals to floats for JSON serialization."""
    result = create_chart(
        dimensions=["orders.customer_name"],
        metrics=["orders.total_revenue"],
        chart_type="bar",
    )

    # The vega_spec contains the data embedded, so it must be JSON serializable
    vega_spec = result["vega_spec"]

    # Check that the embedded data has no Decimal values
    if "data" in vega_spec and "values" in vega_spec["data"]:
        for row in vega_spec["data"]["values"]:
            for key, value in row.items():
                assert not isinstance(value, Decimal), f"Found Decimal in vega_spec data for {key}: {value}"

    # Verify the entire result is JSON serializable
    try:
        json.dumps(result)
    except TypeError as e:
        pytest.fail(f"create_chart result is not JSON serializable: {e}")


def test_create_chart_with_filter(demo_layer):
    """Test creating a chart with a filter."""
    result = create_chart(
        dimensions=["orders.customer_name"],
        metrics=["orders.total_revenue"],
        where="orders.customer_name = 'Alice'",
        chart_type="bar",
    )

    assert result["sql"] is not None
    assert "WHERE" in result["sql"].upper()
    assert "Alice" in result["sql"]


def test_create_chart_time_series(demo_layer):
    """Test creating a time series chart."""
    result = create_chart(
        dimensions=["orders.order_date"],
        metrics=["orders.total_revenue"],
        chart_type="line",
        title="Revenue Over Time",
    )

    assert result["sql"] is not None
    assert result["row_count"] > 0

    # Verify it's JSON serializable
    try:
        json.dumps(result)
    except TypeError as e:
        pytest.fail(f"Time series chart result is not JSON serializable: {e}")
