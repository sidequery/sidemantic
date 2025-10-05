"""Test VIEW generation for reusable semantic queries."""

import duckdb

from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.sql.generator_v2 import SQLGenerator
from tests.utils import fetch_rows


def test_generate_view_creates_valid_sql():
    """Test that generate_view creates valid CREATE VIEW statement."""
    # Setup models
    customers = Model(
        name="customers",
        sql="SELECT 1 AS id, 'Alice' AS name, 100 AS lifetime_value",
        primary_key="id",
        dimensions=[
            Dimension(name="id", sql="id", type="categorical"),
            Dimension(name="name", sql="name", type="categorical"),
        ],
        metrics=[Metric(name="ltv", agg="sum", sql="lifetime_value")],
    )

    graph = SemanticGraph()
    graph.add_model(customers)
    generator = SQLGenerator(graph)

    # Generate view
    view_sql = generator.generate_view(
        view_name="customer_metrics", metrics=["customers.ltv"], dimensions=["customers.name"]
    )

    # Should start with CREATE VIEW
    assert view_sql.startswith("CREATE VIEW customer_metrics AS\n")

    # Should contain valid SQL query
    assert "SELECT" in view_sql
    assert "FROM" in view_sql


def test_view_can_be_queried():
    """Test that generated view can be created and queried in DuckDB."""
    # Setup models
    orders = Model(
        name="orders",
        sql="SELECT 1 AS id, 100 AS amount, 'completed' AS status UNION ALL SELECT 2, 200, 'completed'",
        primary_key="id",
        dimensions=[
            Dimension(name="id", sql="id", type="categorical"),
            Dimension(name="status", sql="status", type="categorical"),
        ],
        metrics=[Metric(name="amount", agg="sum", sql="amount")],
    )

    total_revenue = Metric(name="total_revenue", sql="orders.amount")

    graph = SemanticGraph()
    graph.add_model(orders)
    graph.add_metric(total_revenue)
    generator = SQLGenerator(graph)

    # Generate view
    view_sql = generator.generate_view(
        view_name="revenue_by_status", metrics=["total_revenue"], dimensions=["orders.status"]
    )

    # Execute in DuckDB
    conn = duckdb.connect(":memory:")
    conn.execute(view_sql)

    # Query the view
    result = conn.execute("SELECT * FROM revenue_by_status")
    rows = fetch_rows(result)

    assert len(rows) == 1
    assert rows[0][1] == 300  # total revenue


def test_join_view_against_other_tables():
    """Test that view can be joined against arbitrary SQL."""
    # Setup semantic layer
    products = Model(
        name="products",
        sql="SELECT 1 AS id, 'Widget' AS name, 10 AS price UNION ALL SELECT 2, 'Gadget', 20",
        primary_key="id",
        dimensions=[
            Dimension(name="id", sql="id", type="categorical"),
            Dimension(name="name", sql="name", type="categorical"),
        ],
        metrics=[Metric(name="price", agg="avg", sql="price")],
    )

    avg_price = Metric(name="avg_price", sql="products.price")

    graph = SemanticGraph()
    graph.add_model(products)
    graph.add_metric(avg_price)
    generator = SQLGenerator(graph)

    # Generate view
    view_sql = generator.generate_view(view_name="product_metrics", metrics=["avg_price"], dimensions=["products.name"])

    # Execute in DuckDB
    conn = duckdb.connect(":memory:")
    conn.execute(view_sql)

    # Create another table and join against the view
    conn.execute("""
        CREATE TABLE sales AS
        SELECT 'Widget' AS product_name, 100 AS units_sold
        UNION ALL
        SELECT 'Gadget', 50
    """)

    # Join view with sales table
    result = conn.execute("""
        SELECT
            s.product_name,
            s.units_sold,
            pm.avg_price,
            s.units_sold * pm.avg_price AS revenue
        FROM sales s
        JOIN product_metrics pm ON s.product_name = pm.name
        ORDER BY revenue DESC
    """)
    rows = fetch_rows(result)

    assert len(rows) == 2
    # Both have same revenue (100 * 10 = 1000, 50 * 20 = 1000)
    # Just verify the join worked correctly
    assert rows[0][2] in (10, 20)  # avg_price exists
    assert rows[1][2] in (10, 20)  # avg_price exists
