"""Integration tests for ClickHouse adapter.

Run with: docker compose up -d clickhouse && pytest -m integration tests/db/test_clickhouse_integration.py -v
"""

import os

import pytest

from sidemantic import Dimension, Metric, Model, Relationship, SemanticLayer

# Mark all tests in this module as integration tests
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("CLICKHOUSE_TEST") != "1",
        reason="Set CLICKHOUSE_TEST=1 and run docker compose up -d clickhouse to run ClickHouse integration tests",
    ),
]

# Use environment variable for URL
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT", "8123")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse")
CLICKHOUSE_URL = f"clickhouse://default:{CLICKHOUSE_PASSWORD}@{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/default"


@pytest.fixture(scope="module")
def databricks_layer():
    """Create SemanticLayer with ClickHouse connection."""
    layer = SemanticLayer(connection=CLICKHOUSE_URL)
    yield layer
    layer.adapter.close()


def test_semantic_layer_basic_metric(databricks_layer):
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


def test_semantic_layer_joins(databricks_layer):
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

    databricks_layer.add_model(customers_join)
    databricks_layer.add_model(orders_join)

    # Query with join
    result = databricks_layer.query(metrics=["orders_join.total_revenue"], dimensions=["customers_join.region"])
    rows = result.fetchall()
    cols = [desc[0] for desc in result.description]
    results_dict = {dict(zip(cols, row))["region"]: dict(zip(cols, row))["total_revenue"] for row in rows}

    assert results_dict["US"] == 250.0
    assert results_dict["EU"] == 200.0


def test_semantic_layer_multiple_metrics(databricks_layer):
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
    databricks_layer.add_model(products)

    result = databricks_layer.query(
        metrics=["products.total_price", "products.avg_price", "products.total_quantity", "products.product_count"]
    )
    row = result.fetchone()
    cols = [desc[0] for desc in result.description]
    row_dict = dict(zip(cols, row))

    assert row_dict["total_price"] == 450
    assert row_dict["avg_price"] == 150.0
    assert row_dict["total_quantity"] == 15
    assert row_dict["product_count"] == 3


def test_semantic_layer_filters(databricks_layer):
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
    databricks_layer.add_model(inventory)

    # Test with filter in SQL
    result = databricks_layer.query(
        metrics=["inventory.total_quantity"],
        dimensions=["inventory.category"],
        filters=["inventory.category = 'A'"],
    )
    rows = result.fetchall()
    assert len(rows) == 1
    assert rows[0][1] == 300  # Total for category A


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


def test_semantic_layer_order_by(databricks_layer):
    """Test ORDER BY with SemanticLayer."""
    scores = Model(
        name="scores",
        table="""(
            SELECT 'Alice' as name, 85 as score UNION ALL
            SELECT 'Bob', 92 UNION ALL
            SELECT 'Charlie', 78 UNION ALL
            SELECT 'Diana', 95
        )""",
        primary_key="name",
        dimensions=[Dimension(name="name", type="categorical")],
        metrics=[Metric(name="avg_score", agg="avg", sql="score")],
    )
    databricks_layer.add_model(scores)

    # Order by dimension
    result = databricks_layer.query(
        dimensions=["scores.name"], metrics=["scores.avg_score"], order_by=["scores.avg_score DESC"]
    )
    rows = result.fetchall()
    # First row should have highest score
    assert rows[0][1] == 95  # Diana
    assert rows[-1][1] == 78  # Charlie


def test_semantic_layer_limit(databricks_layer):
    """Test LIMIT with SemanticLayer."""
    items = Model(
        name="items",
        table="""(
            SELECT 1 as id, 'A' as category UNION ALL
            SELECT 2, 'B' UNION ALL
            SELECT 3, 'A' UNION ALL
            SELECT 4, 'C' UNION ALL
            SELECT 5, 'B'
        )""",
        primary_key="id",
        dimensions=[Dimension(name="category", type="categorical")],
        metrics=[Metric(name="count", agg="count", sql="id")],
    )
    databricks_layer.add_model(items)

    result = databricks_layer.query(dimensions=["items.category"], metrics=["items.count"], limit=2)
    rows = result.fetchall()
    assert len(rows) == 2


@pytest.mark.skip(reason="Date functions test - consistent with other databases")
def test_semantic_layer_date_functions(databricks_layer):
    """Test date/time functions in metrics."""
    events = Model(
        name="events",
        table="""(
            SELECT DATE('2024-01-15') as event_date, 1 as event_id UNION ALL
            SELECT DATE('2024-01-20'), 2 UNION ALL
            SELECT DATE('2024-02-10'), 3 UNION ALL
            SELECT DATE('2024-02-15'), 4
        )""",
        primary_key="event_id",
        dimensions=[
            Dimension(name="month", type="time", sql="date_format(event_date, 'yyyy-MM')", granularity="month"),
            Dimension(name="year", type="time", sql="year(event_date)", granularity="year"),
        ],
        metrics=[Metric(name="event_count", agg="count", sql="event_id")],
    )
    databricks_layer.add_model(events)

    result = databricks_layer.query(dimensions=["events.month"], metrics=["events.event_count"])
    rows = result.fetchall()
    results_dict = {row[0]: row[1] for row in rows}

    assert results_dict["2024-01"] == 2
    assert results_dict["2024-02"] == 2


def test_semantic_layer_symmetric_aggregates(databricks_layer):
    """Test symmetric aggregates handle fan-out joins correctly."""
    # Create a fan-out scenario: order has multiple line_items
    orders_sym = Model(
        name="orders_sym",
        table="""(
            SELECT 1 as order_id, 100 as subtotal UNION ALL
            SELECT 2, 200
        )""",
        primary_key="order_id",
        metrics=[Metric(name="total_subtotal", agg="sum", sql="subtotal")],
    )

    line_items_sym = Model(
        name="line_items_sym",
        table="""(
            SELECT 1 as item_id, 1 as order_id, 50 as price UNION ALL
            SELECT 2, 1, 30 UNION ALL
            SELECT 3, 1, 20 UNION ALL
            SELECT 4, 2, 100 UNION ALL
            SELECT 5, 2, 100
        )""",
        primary_key="item_id",
        metrics=[Metric(name="total_price", agg="sum", sql="price")],
        relationships=[Relationship(name="orders_sym", type="many_to_one", foreign_key="order_id")],
    )

    databricks_layer.add_model(orders_sym)
    databricks_layer.add_model(line_items_sym)

    # Query both metrics - should use symmetric aggregation to avoid fan-out
    result = databricks_layer.query(metrics=["orders_sym.total_subtotal", "line_items_sym.total_price"])
    row = result.fetchone()
    cols = [desc[0] for desc in result.description]
    row_dict = dict(zip(cols, row))

    # Without symmetric aggregation, total_subtotal would be inflated
    assert row_dict["total_subtotal"] == 300  # 100 + 200, not inflated
    assert row_dict["total_price"] == 300  # 50+30+20+100+100


def test_semantic_layer_multiple_joins(databricks_layer):
    """Test joining 3+ models together."""
    users_multi = Model(
        name="users_multi",
        table="""(
            SELECT 1 as user_id, 'Alice' as name UNION ALL
            SELECT 2, 'Bob'
        )""",
        primary_key="user_id",
        dimensions=[Dimension(name="name", type="categorical")],
    )

    orders_multi = Model(
        name="orders_multi",
        table="""(
            SELECT 1 as order_id, 1 as user_id, 1 as product_id, 100 as amount UNION ALL
            SELECT 2, 1, 2, 150 UNION ALL
            SELECT 3, 2, 1, 200
        )""",
        primary_key="order_id",
        metrics=[Metric(name="total", agg="sum", sql="amount")],
        relationships=[
            Relationship(name="users_multi", type="many_to_one", foreign_key="user_id"),
            Relationship(name="products_multi", type="many_to_one", foreign_key="product_id"),
        ],
    )

    products_multi = Model(
        name="products_multi",
        table="""(
            SELECT 1 as product_id, 'Widget' as product_name UNION ALL
            SELECT 2, 'Gadget'
        )""",
        primary_key="product_id",
        dimensions=[Dimension(name="product_name", type="categorical")],
    )

    databricks_layer.add_model(users_multi)
    databricks_layer.add_model(products_multi)
    databricks_layer.add_model(orders_multi)

    # Query across 3 models
    result = databricks_layer.query(
        metrics=["orders_multi.total"], dimensions=["users_multi.name", "products_multi.product_name"]
    )
    rows = result.fetchall()
    results_dict = {(row[0], row[1]): row[2] for row in rows}

    assert results_dict[("Alice", "Widget")] == 100
    assert results_dict[("Alice", "Gadget")] == 150
    assert results_dict[("Bob", "Widget")] == 200
