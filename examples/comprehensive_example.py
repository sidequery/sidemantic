#!/usr/bin/env python3
# /// script
# dependencies = [
#   "sidemantic",
#   "duckdb",
# ]
# ///
"""Comprehensive example showcasing all sidemantic features.

This example demonstrates:
1. Parameters (user input)
2. Symmetric aggregates (fan-out joins)
3. Table calculations (post-query)
4. Advanced metrics (MTD, YTD, offset ratios, conversions)
"""

import duckdb

from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.parameter import Parameter
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.core.table_calculation import TableCalculation
from sidemantic.sql.generator import SQLGenerator
from sidemantic.sql.table_calc_processor import TableCalculationProcessor


def print_section(title):
    """Print a section header."""
    print("\n" + "=" * 80)
    print(f" {title}")
    print("=" * 80 + "\n")


def main():
    # Create sample data in DuckDB
    print_section("Setting up sample data in DuckDB")

    conn = duckdb.connect(":memory:")

    # Orders table
    conn.execute("""
        CREATE TABLE orders AS
        SELECT * FROM (VALUES
            (1, 1, '2024-01-01'::DATE, 100, 'completed'),
            (2, 1, '2024-01-15'::DATE, 200, 'completed'),
            (3, 2, '2024-01-20'::DATE, 150, 'pending'),
            (4, 1, '2024-02-01'::DATE, 300, 'completed'),
            (5, 3, '2024-02-15'::DATE, 250, 'completed')
        ) AS t(id, customer_id, order_date, amount, status)
    """)

    # Order items table (for symmetric aggregates demo)
    conn.execute("""
        CREATE TABLE order_items AS
        SELECT * FROM (VALUES
            (1, 1, 10, 'Widget A'),
            (2, 1, 5, 'Widget B'),
            (3, 2, 8, 'Widget A'),
            (4, 2, 12, 'Widget C'),
            (5, 4, 15, 'Widget A'),
            (6, 5, 20, 'Widget B')
        ) AS t(id, order_id, quantity, product_name)
    """)

    # Shipments table (for symmetric aggregates demo)
    conn.execute("""
        CREATE TABLE shipments AS
        SELECT * FROM (VALUES
            (1, 1, '2024-01-02'::DATE),
            (2, 1, '2024-01-03'::DATE),
            (3, 2, '2024-01-16'::DATE),
            (4, 4, '2024-02-02'::DATE),
            (5, 5, '2024-02-16'::DATE)
        ) AS t(id, order_id, shipment_date)
    """)

    print("Created tables: orders, order_items, shipments")

    # Build semantic graph
    print_section("Building Semantic Graph")

    graph = SemanticGraph()

    # Define parameters
    status_param = Parameter(
        name="status_filter",
        type="string",
        default_value="completed",
        allowed_values=["completed", "pending", "cancelled"],
        description="Filter orders by status",
    )
    min_amount_param = Parameter(
        name="min_amount", type="number", default_value=0, description="Minimum order amount filter"
    )

    graph.add_parameter(status_param)
    graph.add_parameter(min_amount_param)
    print(f"Added parameters: {list(graph.parameters.keys())}")

    # Define orders model with relationships (for symmetric aggregates)
    orders = Model(
        name="orders",
        table="orders",
        primary_key="id",
        dimensions=[
            Dimension(name="order_date", type="time", sql="order_date"),
            Dimension(name="status", type="categorical", sql="status"),
        ],
        metrics=[
            Metric(name="revenue", agg="sum", sql="amount"),
            Metric(name="order_count", agg="count"),
        ],
        relationships=[
            Relationship(name="order_items", type="one_to_many", foreign_key="order_id"),
            Relationship(name="shipments", type="one_to_many", foreign_key="order_id"),
        ],
    )

    # Define order_items model
    order_items = Model(
        name="order_items",
        table="order_items",
        primary_key="id",
        dimensions=[
            Dimension(name="product_name", type="categorical", sql="product_name"),
        ],
        metrics=[
            Metric(name="total_quantity", agg="sum", sql="quantity"),
        ],
        relationships=[
            Relationship(name="orders", type="many_to_one", foreign_key="order_id"),
        ],
    )

    # Define shipments model
    shipments = Model(
        name="shipments",
        table="shipments",
        primary_key="id",
        dimensions=[
            Dimension(name="shipment_date", type="time", sql="shipment_date"),
        ],
        metrics=[
            Metric(name="shipment_count", agg="count"),
        ],
        relationships=[
            Relationship(name="orders", type="many_to_one", foreign_key="order_id"),
        ],
    )

    graph.add_model(orders)
    graph.add_model(order_items)
    graph.add_model(shipments)
    print(f"Added models: {list(graph.models.keys())}")

    # Define advanced metrics

    # Month-to-date revenue (grain-to-date)
    mtd_revenue = Metric(
        name="mtd_revenue",
        type="cumulative",
        sql="orders.revenue",
        grain_to_date="month",
        description="Month-to-date cumulative revenue",
    )

    # Year-to-date revenue
    ytd_revenue = Metric(
        name="ytd_revenue",
        type="cumulative",
        sql="orders.revenue",
        grain_to_date="year",
        description="Year-to-date cumulative revenue",
    )

    # Month-over-month growth (offset ratio)
    mom_growth = Metric(
        name="mom_growth",
        type="ratio",
        numerator="orders.revenue",
        denominator="orders.revenue",
        offset_window="1 month",
        description="Month-over-month revenue growth rate",
    )

    graph.add_metric(mtd_revenue)
    graph.add_metric(ytd_revenue)
    graph.add_metric(mom_growth)
    print(f"Added metrics: {list(graph.metrics.keys())}")

    # Define table calculations

    # Percent of total
    pct_of_total = TableCalculation(
        name="revenue_pct_of_total",
        type="percent_of_total",
        field="revenue",
        description="Revenue as % of total",
    )

    # Running total
    running_total = TableCalculation(
        name="revenue_running_total",
        type="running_total",
        field="revenue",
        order_by=["order_date"],
        description="Cumulative revenue over time",
    )

    graph.add_table_calculation(pct_of_total)
    graph.add_table_calculation(running_total)
    print(f"Added table calculations: {list(graph.table_calculations.keys())}")

    # Initialize SQL generator
    generator = SQLGenerator(graph)

    # Example 1: Basic query with parameters
    print_section("Example 1: Basic Query with Parameters")

    sql = generator.generate(
        metrics=["orders.revenue", "orders.order_count"],
        dimensions=["orders.order_date"],
        filters=["orders.status = {{ status_filter }}"],
        parameters={"status_filter": "completed"},
        order_by=["orders.order_date"],
    )

    print("SQL with parameters (status='completed'):")
    print(sql)
    print("\nExecuting query...")

    result = conn.execute(sql).fetchall()
    print("\nResults:")
    for row in result:
        print(f"  {row}")

    # Example 2: Symmetric aggregates (fan-out joins)
    print_section("Example 2: Symmetric Aggregates (Fan-out Joins)")

    print("Querying orders with BOTH order_items and shipments creates a fan-out.")
    print("Symmetric aggregates prevent double-counting of order revenue.\n")

    sql = generator.generate(
        metrics=["orders.revenue", "order_items.total_quantity", "shipments.shipment_count"],
        dimensions=["orders.order_date"],
        order_by=["orders.order_date"],
    )

    print("SQL (note the HASH function for symmetric aggregates):")
    print(sql)
    print("\nExecuting query...")

    result = conn.execute(sql).fetchall()
    print("\nResults:")
    print("  order_date  | revenue | total_quantity | shipment_count")
    print("  " + "-" * 60)
    for row in result:
        # Handle None values
        revenue = row[1] if row[1] is not None else 0
        quantity = row[2] if row[2] is not None else 0
        shipments = row[3] if row[3] is not None else 0
        print(f"  {row[0]!s:<12} | {revenue:>7} | {quantity:>14} | {shipments:>14}")

    # Example 3: Advanced metrics (MTD, YTD)
    print_section("Example 3: Advanced Metrics (MTD, YTD)")

    sql = generator.generate(
        metrics=["orders.revenue", "mtd_revenue"],
        dimensions=["orders.order_date"],
        order_by=["orders.order_date"],
    )

    print("SQL for MTD revenue:")
    print(sql)
    print("\nExecuting query...")

    result = conn.execute(sql).fetchall()
    print("\nResults:")
    print("  order_date  | revenue | mtd_revenue (cumulative within month)")
    print("  " + "-" * 60)
    for row in result:
        print(f"  {row[0]:<12} | {row[1]:>7} | {row[2]:>7}")

    # Example 4: Table calculations
    print_section("Example 4: Table Calculations (Post-Query)")

    # First generate base query
    sql = generator.generate(
        metrics=["orders.revenue"],
        dimensions=["orders.order_date"],
        order_by=["orders.order_date"],
    )

    # Execute to get results
    base_results = conn.execute(sql).fetchall()
    column_names = ["order_date", "revenue"]

    print("Base query results:")
    for row in base_results:
        print(f"  {row}")

    # Apply table calculations
    calc_processor = TableCalculationProcessor(
        [
            graph.get_table_calculation("revenue_pct_of_total"),
            graph.get_table_calculation("revenue_running_total"),
        ]
    )

    # Apply all calculations
    results_with_calcs, updated_columns = calc_processor.process(base_results, column_names)

    print("\nAfter applying table calculations (percent_of_total + running_total):")
    print(f"  Columns: {updated_columns}")
    for row in results_with_calcs:
        print(f"  {row}")

    # Example 5: Changing parameter values
    print_section("Example 5: Dynamic Parameters")

    print("Same query with different parameter values:\n")

    for status in ["completed", "pending"]:
        sql = generator.generate(
            metrics=["orders.revenue", "orders.order_count"],
            dimensions=["orders.status"],
            filters=["orders.status = {{ status_filter }}"],
            parameters={"status_filter": status},
        )

        result = conn.execute(sql).fetchall()
        print(f"Status filter = '{status}':")
        for row in result:
            print(f"  {row}")
        print()

    # Summary
    print_section("Summary of Features Demonstrated")

    print("""
1. Parameters
   - User input with {{ parameter_name }} syntax
   - Type safety (string, number, date, etc.)
   - Default values and allowed values

2. Symmetric Aggregates
   - Automatic detection of fan-out joins
   - Prevents double-counting with HASH-based formula
   - Only applies when needed (2+ one-to-many joins)

3. Table Calculations
   - Post-query runtime calculations
   - Percent of total, running total, rank, etc.
   - Applied after SQL execution

4. Advanced Metrics
   - Grain-to-date (MTD, QTD, YTD)
   - Offset ratios (MoM, YoY growth)
   - Conversion metrics (funnels)
   - Fill nulls with defaults

5. Comprehensive Type System
   - Models with Rails-like joins
   - Entities for graph traversal
   - Dimensions and measures
   - Cross-model metric composition
    """)

    conn.close()


if __name__ == "__main__":
    main()
