"""Integration tests for Databricks SQL adapter.

NOTE: databricks-sql-connector ONLY works with real Databricks SQL warehouses, not vanilla Spark.
These tests require a real Databricks workspace and are skipped by default.

To run: DATABRICKS_TEST=1 DATABRICKS_URL="databricks://token@workspace.cloud.databricks.com/sql/1.0/warehouses/..." pytest -m integration tests/db/test_databricks_integration.py -v
"""

import os

import pytest

from sidemantic import Dimension, Metric, Model, SemanticLayer

# Mark all tests in this module as integration tests
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("DATABRICKS_TEST") != "1",
        reason="Set DATABRICKS_TEST=1 and DATABRICKS_URL to run Databricks integration tests (requires real Databricks workspace)",
    ),
]

# Requires real Databricks credentials
DATABRICKS_URL = os.getenv("DATABRICKS_URL")
if not DATABRICKS_URL and os.getenv("DATABRICKS_TEST") == "1":
    pytest.skip("DATABRICKS_URL must be set to run Databricks tests", allow_module_level=True)


@pytest.fixture(scope="module")
def databricks_layer():
    """Create SemanticLayer with Databricks connection to Spark Thrift server."""
    layer = SemanticLayer(connection=DATABRICKS_URL)
    yield layer
    layer.adapter.close()


def test_semantic_layer_basic_metric(databricks_layer):
    """Test basic metric query with SemanticLayer."""
    orders = Model(
        name="orders",
        table="(SELECT 1 as order_id, 100.0 as amount UNION ALL SELECT 2, 200.0 UNION ALL SELECT 3, 150.0)",
        primary_key="order_id",
        metrics=[
            Metric(name="total_revenue", agg="sum", sql="amount"),
            Metric(name="avg_revenue", agg="avg", sql="amount"),
            Metric(name="order_count", agg="count", sql="order_id"),
        ],
    )
    databricks_layer.add_model(orders)

    # Test sum
    result = databricks_layer.query(metrics=["orders.total_revenue"])
    row = result.fetchone()
    assert row[0] == 450.0

    # Test avg
    result = databricks_layer.query(metrics=["orders.avg_revenue"])
    row = result.fetchone()
    assert row[0] == 150.0

    # Test count
    result = databricks_layer.query(metrics=["orders.order_count"])
    row = result.fetchone()
    assert row[0] == 3


def test_semantic_layer_dimension_grouping(databricks_layer):
    """Test querying with dimensions and grouping."""
    sales = Model(
        name="sales",
        table="""(
            SELECT 'Electronics' as category, 'US' as region, 100 as amount UNION ALL
            SELECT 'Electronics', 'EU', 150 UNION ALL
            SELECT 'Clothing', 'US', 200 UNION ALL
            SELECT 'Clothing', 'EU', 250
        )""",
        primary_key="category",
        dimensions=[
            Dimension(name="category", type="categorical"),
            Dimension(name="region", type="categorical"),
        ],
        metrics=[Metric(name="total_sales", agg="sum", sql="amount")],
    )
    databricks_layer.add_model(sales)

    # Group by one dimension
    result = databricks_layer.query(metrics=["sales.total_sales"], dimensions=["sales.category"])
    rows = result.fetchall()
    cols = [desc[0] for desc in result.description]
    results = [dict(zip(cols, row)) for row in rows]
    results_dict = {r["category"]: r["total_sales"] for r in results}

    assert results_dict["Electronics"] == 250
    assert results_dict["Clothing"] == 450


def test_semantic_layer_sql_generation(databricks_layer):
    """Test SQL generation without executing."""
    metrics_model = Model(
        name="metrics_sql",
        table="(SELECT 1 as id, 100 as value)",
        primary_key="id",
        metrics=[Metric(name="total", agg="sum", sql="value")],
    )
    databricks_layer.add_model(metrics_model)

    sql = databricks_layer.compile(metrics=["metrics_sql.total"])
    assert "SELECT" in sql.upper()
    assert "SUM" in sql.upper()
    assert databricks_layer.dialect == "databricks"
