"""Integration tests for BigQuery adapter against emulator.

Run with: docker compose up -d bigquery && pytest -m integration tests/db/test_bigquery_integration.py -v
"""

import os

import pytest

from sidemantic import Dimension, Metric, Model, SemanticLayer

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


@pytest.fixture
def bigquery_adapter():
    """Create BigQuery adapter connected to emulator."""
    from sidemantic.db.bigquery import BigQueryAdapter

    # For emulator, we need to set BIGQUERY_EMULATOR_HOST
    emulator_host = os.getenv("BIGQUERY_EMULATOR_HOST", "localhost:9050")
    os.environ["BIGQUERY_EMULATOR_HOST"] = emulator_host

    adapter = BigQueryAdapter(project_id=BIGQUERY_PROJECT, dataset_id=BIGQUERY_DATASET)

    # Create dataset if it doesn't exist
    try:
        adapter.client.create_dataset(BIGQUERY_DATASET, exists_ok=True)
    except Exception:
        pass  # May already exist

    yield adapter
    adapter.close()


@pytest.fixture
def clean_bigquery(bigquery_adapter):
    """Clean BigQuery emulator by dropping all tables in dataset."""
    # Drop all tables in the test dataset
    try:
        dataset_ref = bigquery_adapter.client.dataset(BIGQUERY_DATASET)
        tables = list(bigquery_adapter.client.list_tables(dataset_ref))
        for table in tables:
            bigquery_adapter.client.delete_table(f"{BIGQUERY_DATASET}.{table.table_id}")
    except Exception:
        pass  # Dataset might not exist yet

    yield bigquery_adapter


def test_bigquery_adapter_execute(clean_bigquery):
    """Test basic query execution."""
    result = clean_bigquery.execute("SELECT 1 as x, 2 as y")
    row = result.fetchone()
    assert row == (1, 2)


def test_bigquery_adapter_create_table(clean_bigquery):
    """Test creating and querying a table."""
    clean_bigquery.execute(f"CREATE TABLE {BIGQUERY_DATASET}.test (id INT64, value STRING)")
    clean_bigquery.execute(f"INSERT INTO {BIGQUERY_DATASET}.test VALUES (1, 'foo'), (2, 'bar')")

    result = clean_bigquery.execute(f"SELECT id, value FROM {BIGQUERY_DATASET}.test ORDER BY id")
    rows = result.fetchall()
    assert len(rows) == 2
    assert rows[0] == (1, "foo")
    assert rows[1] == (2, "bar")


def test_bigquery_adapter_get_tables(clean_bigquery):
    """Test listing tables."""
    clean_bigquery.execute(f"CREATE TABLE {BIGQUERY_DATASET}.table1 (id INT64)")
    clean_bigquery.execute(f"CREATE TABLE {BIGQUERY_DATASET}.table2 (id INT64)")

    tables = clean_bigquery.get_tables()
    table_names = {t["table_name"] for t in tables}
    assert "table1" in table_names
    assert "table2" in table_names


def test_bigquery_adapter_get_columns(clean_bigquery):
    """Test getting column information."""
    clean_bigquery.execute(f"CREATE TABLE {BIGQUERY_DATASET}.test (id INT64, name STRING, amount FLOAT64)")

    columns = clean_bigquery.get_columns("test", schema=BIGQUERY_DATASET)
    column_names = {c["column_name"] for c in columns}
    assert column_names == {"id", "name", "amount"}


def test_semantic_layer_bigquery_basic(clean_bigquery):
    """Test SemanticLayer with BigQuery adapter."""
    # Create test table
    clean_bigquery.execute(f"CREATE TABLE {BIGQUERY_DATASET}.orders (order_id INT64, amount FLOAT64)")
    clean_bigquery.execute(f"INSERT INTO {BIGQUERY_DATASET}.orders VALUES (1, 100.0), (2, 200.0)")

    # Create semantic layer with BigQuery adapter
    layer = SemanticLayer(connection=clean_bigquery)

    # Define model
    orders = Model(
        name="orders",
        table=f"{BIGQUERY_DATASET}.orders",
        primary_key="order_id",
        metrics=[Metric(name="total_revenue", agg="sum", sql="amount")],
    )
    layer.add_model(orders)

    # Query
    result = layer.query(metrics=["orders.total_revenue"])
    row = result.fetchone()
    assert row[0] == 300.0


def test_semantic_layer_bigquery_with_url(clean_bigquery):
    """Test SemanticLayer with BigQuery URL."""
    # Create test table
    clean_bigquery.execute(f"CREATE TABLE {BIGQUERY_DATASET}.orders (order_id INT64, amount FLOAT64)")
    clean_bigquery.execute(f"INSERT INTO {BIGQUERY_DATASET}.orders VALUES (1, 150.0), (2, 250.0)")

    # Create semantic layer with URL
    # Set emulator host for new connection
    os.environ["BIGQUERY_EMULATOR_HOST"] = os.getenv("BIGQUERY_EMULATOR_HOST", "localhost:9050")

    layer = SemanticLayer(connection=f"bigquery://{BIGQUERY_PROJECT}/{BIGQUERY_DATASET}")

    # Define model
    orders = Model(
        name="orders",
        table=f"{BIGQUERY_DATASET}.orders",
        primary_key="order_id",
        metrics=[Metric(name="total_revenue", agg="sum", sql="amount")],
    )
    layer.add_model(orders)

    # Query
    result = layer.query(metrics=["orders.total_revenue"])
    row = result.fetchone()
    assert row[0] == 400.0


def test_semantic_layer_bigquery_with_joins(clean_bigquery):
    """Test SemanticLayer with BigQuery and joins."""
    # Create tables
    clean_bigquery.execute(
        f"CREATE TABLE {BIGQUERY_DATASET}.orders (order_id INT64, customer_id INT64, amount FLOAT64)"
    )
    clean_bigquery.execute(f"CREATE TABLE {BIGQUERY_DATASET}.customers (customer_id INT64, name STRING, region STRING)")

    # Insert data
    clean_bigquery.execute(f"INSERT INTO {BIGQUERY_DATASET}.orders VALUES (1, 1, 100.0), (2, 1, 150.0), (3, 2, 200.0)")
    clean_bigquery.execute(f"INSERT INTO {BIGQUERY_DATASET}.customers VALUES (1, 'Alice', 'US'), (2, 'Bob', 'EU')")

    # Create semantic layer
    layer = SemanticLayer(connection=clean_bigquery)

    # Define models with relationships
    from sidemantic import Relationship

    customers = Model(
        name="customers",
        table=f"{BIGQUERY_DATASET}.customers",
        primary_key="customer_id",
        dimensions=[Dimension(name="region", type="categorical")],
    )

    orders = Model(
        name="orders",
        table=f"{BIGQUERY_DATASET}.orders",
        primary_key="order_id",
        metrics=[Metric(name="total_revenue", agg="sum", sql="amount")],
        relationships=[Relationship(name="customers", type="many_to_one", foreign_key="customer_id")],
    )

    layer.add_model(customers)
    layer.add_model(orders)

    # Query with join
    result = layer.query(metrics=["orders.total_revenue"], dimensions=["customers.region"])

    # Collect results
    rows = result.fetchall()
    cols = [desc[0] for desc in result.description]
    results_dict = {dict(zip(cols, row))["region"]: dict(zip(cols, row))["total_revenue"] for row in rows}

    assert results_dict["US"] == 250.0  # Alice: 100 + 150
    assert results_dict["EU"] == 200.0  # Bob: 200


def test_semantic_layer_bigquery_aggregations(clean_bigquery):
    """Test different aggregation types."""
    # Create test table
    clean_bigquery.execute(f"CREATE TABLE {BIGQUERY_DATASET}.sales (id INT64, amount FLOAT64, quantity INT64)")
    clean_bigquery.execute(f"INSERT INTO {BIGQUERY_DATASET}.sales VALUES (1, 100.0, 5), (2, 200.0, 10), (3, 150.0, 7)")

    # Create semantic layer
    layer = SemanticLayer(connection=clean_bigquery)

    # Define model with different aggregations
    sales = Model(
        name="sales",
        table=f"{BIGQUERY_DATASET}.sales",
        primary_key="id",
        metrics=[
            Metric(name="total_amount", agg="sum", sql="amount"),
            Metric(name="avg_amount", agg="avg", sql="amount"),
            Metric(name="max_amount", agg="max", sql="amount"),
            Metric(name="min_amount", agg="min", sql="amount"),
            Metric(name="count_sales", agg="count", sql="id"),
        ],
    )
    layer.add_model(sales)

    # Query
    result = layer.query(
        metrics=[
            "sales.total_amount",
            "sales.avg_amount",
            "sales.max_amount",
            "sales.min_amount",
            "sales.count_sales",
        ]
    )
    row = result.fetchone()
    cols = [desc[0] for desc in result.description]
    row_dict = dict(zip(cols, row))

    assert row_dict["total_amount"] == 450.0
    assert abs(row_dict["avg_amount"] - 150.0) < 0.01
    assert row_dict["max_amount"] == 200.0
    assert row_dict["min_amount"] == 100.0
    assert row_dict["count_sales"] == 3
