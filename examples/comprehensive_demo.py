#!/usr/bin/env python3
"""Comprehensive demo of Sidemantic semantic layer features.

This script demonstrates:
- Auto-detecting dependencies from SQL expressions
- SQL rewriting with semantic layer queries
- Querying metrics without GROUP BY
- Joining semantic layer with regular tables
- Rails-like join specifications
- Multiple metric types (simple, ratio, derived, cumulative)
- View generation for reusable queries

Run with: uv run examples/comprehensive_demo.py
"""
# /// script
# dependencies = [
#   "duckdb",
#   "sqlglot",
#   "pydantic",
#   "pyyaml"
# ]
# requires-python = ">=3.12"
# ///

# Add parent directory to path so we can import sidemantic
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb
from sidemantic.core.join import Join
from sidemantic.core.measure import Measure
from sidemantic.sql.rewriter import SemanticSQLRewriter

from sidemantic.core.dimension import Dimension
from sidemantic.core.model import Model
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.sql.generator_v2 import SQLGenerator


def print_section(title: str):
    """Print a section header."""
    print(f"\n{'=' * 80}")
    print(f"  {title}")
    print("=" * 80)


def print_sql(sql: str):
    """Print SQL with formatting."""
    print("\nSQL:")
    print("-" * 80)
    print(sql)
    print("-" * 80)


def print_results(results, description: str = "Results"):
    """Print query results."""
    print(f"\n{description}:")
    for row in results:
        print(f"  {row}")


def main():
    print_section("Sidemantic Comprehensive Demo")

    # =========================================================================
    # 1. Set up semantic layer models
    # =========================================================================
    print_section("1. Setting up Semantic Layer Models")

    # Create customers model
    customers = Model(
        name="customers",
        sql="""
            SELECT 1 AS customer_id, 'Alice' AS name, 'US' AS region, 'alice@example.com' AS email
            UNION ALL
            SELECT 2, 'Bob', 'EU', 'bob@example.com'
            UNION ALL
            SELECT 3, 'Charlie', 'US', 'charlie@example.com'
        """,
        primary_key="customer_id",
        dimensions=[
            Dimension(name="customer_id", sql="customer_id", type="categorical"),
            Dimension(name="name", sql="name", type="categorical"),
            Dimension(name="region", sql="region", type="categorical"),
            Dimension(name="email", sql="email", type="categorical"),
        ],
    )

    # Create orders model with Rails-like joins
    orders = Model(
        name="orders",
        sql="""
            SELECT 1 AS order_id, 1 AS customer_id, '2024-01-15' AS order_date, 100 AS amount, 'completed' AS status
            UNION ALL
            SELECT 2, 1, '2024-01-20', 150, 'completed'
            UNION ALL
            SELECT 3, 2, '2024-01-18', 200, 'completed'
            UNION ALL
            SELECT 4, 2, '2024-02-01', 50, 'pending'
            UNION ALL
            SELECT 5, 3, '2024-02-05', 300, 'completed'
        """,
        primary_key="order_id",
        dimensions=[
            Dimension(name="order_id", sql="order_id", type="categorical"),
            Dimension(name="customer_id", sql="customer_id", type="categorical"),
            Dimension(name="order_date", sql="order_date", type="time"),
            Dimension(name="status", sql="status", type="categorical"),
        ],
        measures=[
            Measure(name="amount", agg="sum", expr="amount"),
            Measure(name="order_count", agg="count", expr="order_id"),
        ],
        joins=[
            # Rails-like belongs_to relationship
            Join(
                name="customers",
                type="belongs_to",
                foreign_key="customer_id",
                primary_key="customer_id",
            )
        ],
    )

    print("Created customers model with 3 records")
    print("Created orders model with 5 records")
    print("Configured Rails-like belongs_to join from orders -> customers")

    # =========================================================================
    # 2. Define metrics with auto-detected dependencies
    # =========================================================================
    print_section("2. Defining Metrics (Dependencies Auto-Detected!)")

    # Simple metric
    total_revenue = Measure(
        name="total_revenue", expr="orders.amount", description="Total revenue from all orders"
    )
    print("Simple metric: total_revenue")

    # Ratio metric
    avg_order_value = Measure(
        name="avg_order_value",
        type="ratio",
        numerator="orders.amount",
        denominator="orders.order_count",
        description="Average order value",
    )
    print("Ratio metric: avg_order_value")

    # Derived metric
    revenue_per_customer = Measure(
        name="revenue_per_customer",
        type="derived",
        expr="total_revenue / order_count",
        description="Revenue divided by number of orders",
    )
    print("Derived metric: revenue_per_customer")

    # Cumulative metric
    running_total = Measure(
        name="running_total",
        type="cumulative",
        expr="orders.amount",
        window="all",
        description="Running total of revenue",
    )
    print("Cumulative metric: running_total")

    # Build semantic graph
    graph = SemanticGraph()
    graph.add_model(customers)
    graph.add_model(orders)
    graph.add_metric(total_revenue)
    graph.add_metric(avg_order_value)
    graph.add_metric(revenue_per_customer)
    graph.add_metric(running_total)

    print(
        f"\nSemantic graph built with {len(graph.models)} models and {len(graph.metrics)} metrics"
    )

    # =========================================================================
    # 3. Traditional API: Generate SQL directly
    # =========================================================================
    print_section("3. Traditional API: Generate SQL with Generator")

    generator = SQLGenerator(graph)

    # Query metrics by dimension
    sql = generator.generate(
        metrics=["total_revenue", "avg_order_value"], dimensions=["orders.status"]
    )

    print_sql(sql)

    conn = duckdb.connect(":memory:")
    results = conn.execute(sql).fetchall()
    print_results(results, "Revenue by Status")

    # =========================================================================
    # 4. SQL Rewriter: Query semantic layer with SQL!
    # =========================================================================
    print_section("4. SQL Rewriter: Write SQL Against Semantic Layer")

    rewriter = SemanticSQLRewriter(graph)

    # Example 1: Simple query without GROUP BY
    print("\nExample 1: Query without GROUP BY")
    user_sql = """
        SELECT
            status,
            total_revenue,
            avg_order_value
        FROM semantic_layer.orders
    """
    print(f"\nUser writes:\n{user_sql}")

    rewritten = rewriter.rewrite(user_sql)
    print_sql(rewritten)

    results = conn.execute(rewritten).fetchall()
    print_results(results, "Revenue Metrics by Status")

    # Example 2: Join semantic layer with regular table
    print("\nExample 2: Join semantic layer with regular table")

    # Create a regular table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS promotions AS
        SELECT 'completed' AS status, 'Completion Bonus' AS promo_name, 0.10 AS discount
        UNION ALL
        SELECT 'pending', 'Pending Discount', 0.05
    """)

    user_sql = """
        SELECT
            o.status,
            o.total_revenue,
            p.promo_name,
            p.discount
        FROM semantic_layer.orders AS o
        JOIN promotions AS p ON o.status = p.status
    """
    print(f"\nUser writes:\n{user_sql}")

    rewritten = rewriter.rewrite(user_sql)
    print_sql(rewritten)

    results = conn.execute(rewritten).fetchall()
    print_results(results, "Revenue + Promotions")

    # Example 3: Cross-model join (using Rails relationships)
    print("\nExample 3: Cross-model join using Rails-like relationships")

    user_sql = """
        SELECT
            c.name,
            o.status,
            o.total_revenue
        FROM semantic_layer.customers AS c
        JOIN semantic_layer.orders AS o ON c.customer_id = o.customer_id
    """
    print(f"\nUser writes:\n{user_sql}")

    rewritten = rewriter.rewrite(user_sql)
    print_sql(rewritten)

    results = conn.execute(rewritten).fetchall()
    print_results(results, "Customer Revenue (cross-model join)")

    # =========================================================================
    # 5. Generate reusable views
    # =========================================================================
    print_section("5. Generate Reusable Views")

    view_sql = generator.generate_view(
        view_name="revenue_summary",
        metrics=["total_revenue", "avg_order_value"],
        dimensions=["orders.status"],
    )

    print_sql(view_sql)

    conn.execute(view_sql)
    print("Created view 'revenue_summary'")

    # Query the view
    results = conn.execute("SELECT * FROM revenue_summary ORDER BY status").fetchall()
    print_results(results, "Querying the view")

    # Join against the view
    join_sql = """
        SELECT
            r.status,
            r.total_revenue,
            p.promo_name
        FROM revenue_summary AS r
        JOIN promotions AS p ON r.status = p.status
    """
    results = conn.execute(join_sql).fetchall()
    print_results(results, "Joining against the view")

    # =========================================================================
    # 6. Demonstrate dependency auto-detection
    # =========================================================================
    print_section("6. Dependency Auto-Detection")

    print("\nMetric dependencies (auto-detected from SQL expressions):")
    for metric_name, metric in graph.metrics.items():
        deps = metric.get_dependencies(graph)
        print(f"  {metric_name}: {deps}")

    # =========================================================================
    # 7. Cumulative metrics
    # =========================================================================
    print_section("7. Cumulative Metrics (Running Totals)")

    sql = generator.generate(metrics=["running_total"], dimensions=["orders.order_date"])

    print_sql(sql)

    results = conn.execute(sql).fetchall()
    print_results(results, "Running Total by Date")

    # =========================================================================
    # Summary
    # =========================================================================
    print_section("Demo Complete")

    print("""
Key Features Demonstrated:

Auto-detecting metric dependencies from SQL expressions
SQL rewriting - query semantic layer using SQL syntax
No GROUP BY needed - automatically inferred from metrics
Join semantic layer models with regular tables
Cross-model joins using Rails-like relationships (belongs_to)
Multiple metric types: simple, ratio, derived, cumulative
Generate reusable views for composition
Full SQLGlot integration for parsing and transforming SQL
    """)


if __name__ == "__main__":
    main()
