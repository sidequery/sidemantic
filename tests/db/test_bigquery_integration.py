"""Integration tests for BigQuery adapter against emulator.

Run with: docker compose up -d bigquery && pytest -m integration tests/db/test_bigquery_integration.py -v
"""

import os

import pytest

from sidemantic import Dimension, Metric, Model, Relationship, SemanticLayer

# Mark all tests in this module as integration tests
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("BIGQUERY_TEST") != "1",
        reason="Set BIGQUERY_TEST=1 and run docker compose up -d bigquery to run BigQuery integration tests",
    ),
]

# Use environment variable for URL (emulator endpoint)
BIGQUERY_PROJECT = os.getenv("BIGQUERY_PROJECT", "test-project")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "test_dataset")


@pytest.fixture(scope="module")
def bigquery_layer():
    """Create SemanticLayer with BigQuery connection."""
    # Set emulator host
    emulator_host = os.getenv("BIGQUERY_EMULATOR_HOST", "localhost:9050")
    os.environ["BIGQUERY_EMULATOR_HOST"] = emulator_host

    # Create layer with BigQuery URL
    layer = SemanticLayer(connection=f"bigquery://{BIGQUERY_PROJECT}/{BIGQUERY_DATASET}")
    yield layer
    layer.adapter.close()


def test_semantic_layer_basic_metric(bigquery_layer):
    """Test basic metric query with SemanticLayer."""
    # Define a model using a CTE as table
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
    bigquery_layer.add_model(orders)

    # Test sum
    result = bigquery_layer.query(metrics=["orders.total_revenue"])
    row = result.fetchone()
    assert row[0] == 450.0

    # Test avg
    result = bigquery_layer.query(metrics=["orders.avg_revenue"])
    row = result.fetchone()
    assert row[0] == 150.0

    # Test count
    result = bigquery_layer.query(metrics=["orders.order_count"])
    row = result.fetchone()
    assert row[0] == 3


def test_semantic_layer_dimension_grouping(bigquery_layer):
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
    bigquery_layer.add_model(sales)

    # Group by one dimension
    result = bigquery_layer.query(metrics=["sales.total_sales"], dimensions=["sales.category"])
    rows = result.fetchall()
    cols = [desc[0] for desc in result.description]
    results = [dict(zip(cols, row)) for row in rows]
    results_dict = {r["category"]: r["total_sales"] for r in results}

    assert results_dict["Electronics"] == 250
    assert results_dict["Clothing"] == 450


def test_semantic_layer_joins(bigquery_layer):
    """Test joins between models."""
    customers_join = Model(
        name="customers_join",
        table="""(
            SELECT 1 as customer_id, 'Alice' as name, 'US' as region UNION ALL
            SELECT 2, 'Bob', 'EU'
        )""",
        primary_key="customer_id",
        dimensions=[Dimension(name="region", type="categorical")],
    )

    orders_join = Model(
        name="orders_join",
        table="""(
            SELECT 1 as order_id, 1 as customer_id, 100.0 as amount UNION ALL
            SELECT 2, 1, 150.0 UNION ALL
            SELECT 3, 2, 200.0
        )""",
        primary_key="order_id",
        metrics=[Metric(name="total_revenue", agg="sum", sql="amount")],
        relationships=[Relationship(name="customers_join", type="many_to_one", foreign_key="customer_id")],
    )

    bigquery_layer.add_model(customers_join)
    bigquery_layer.add_model(orders_join)

    # Query with join
    result = bigquery_layer.query(metrics=["orders_join.total_revenue"], dimensions=["customers_join.region"])
    rows = result.fetchall()
    cols = [desc[0] for desc in result.description]
    results_dict = {dict(zip(cols, row))["region"]: dict(zip(cols, row))["total_revenue"] for row in rows}

    assert results_dict["US"] == 250.0
    assert results_dict["EU"] == 200.0


def test_semantic_layer_multiple_metrics(bigquery_layer):
    """Test multiple metrics in one query."""
    products = Model(
        name="products",
        table="""(
            SELECT 1 as product_id, 100 as price, 5 as quantity UNION ALL
            SELECT 2, 200, 3 UNION ALL
            SELECT 3, 150, 7
        )""",
        primary_key="product_id",
        metrics=[
            Metric(name="total_price", agg="sum", sql="price"),
            Metric(name="avg_price", agg="avg", sql="price"),
            Metric(name="total_quantity", agg="sum", sql="quantity"),
            Metric(name="product_count", agg="count", sql="product_id"),
        ],
    )
    bigquery_layer.add_model(products)

    result = bigquery_layer.query(
        metrics=["products.total_price", "products.avg_price", "products.total_quantity", "products.product_count"]
    )
    row = result.fetchone()
    cols = [desc[0] for desc in result.description]
    row_dict = dict(zip(cols, row))

    assert row_dict["total_price"] == 450
    assert row_dict["avg_price"] == 150.0
    assert row_dict["total_quantity"] == 15
    assert row_dict["product_count"] == 3


def test_semantic_layer_filters(bigquery_layer):
    """Test filtering with WHERE clause."""
    inventory = Model(
        name="inventory",
        table="""(
            SELECT 1 as item_id, 'A' as category, 100 as quantity UNION ALL
            SELECT 2, 'A', 200 UNION ALL
            SELECT 3, 'B', 150 UNION ALL
            SELECT 4, 'B', 250
        )""",
        primary_key="item_id",
        dimensions=[Dimension(name="category", type="categorical")],
        metrics=[Metric(name="total_quantity", agg="sum", sql="quantity")],
    )
    bigquery_layer.add_model(inventory)

    # Test with filter in SQL
    result = bigquery_layer.query(
        metrics=["inventory.total_quantity"],
        dimensions=["inventory.category"],
        filters=["inventory.category = 'A'"],
    )
    rows = result.fetchall()
    assert len(rows) == 1
    assert rows[0][1] == 300  # Total for category A


def test_semantic_layer_sql_generation(bigquery_layer):
    """Test SQL generation without executing."""
    metrics_model = Model(
        name="metrics_sql",
        table="(SELECT 1 as id, 100 as value)",
        primary_key="id",
        metrics=[Metric(name="total", agg="sum", sql="value")],
    )
    bigquery_layer.add_model(metrics_model)

    sql = bigquery_layer.compile(metrics=["metrics_sql.total"])
    assert "SELECT" in sql.upper()
    assert "SUM" in sql.upper()
    assert bigquery_layer.dialect == "bigquery"
