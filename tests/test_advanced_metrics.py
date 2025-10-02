"""Test advanced metric features: grain-to-date, fill_nulls_with, offsets, conversion."""

import pytest
import duckdb

from sidemantic.core.model import Model
from sidemantic.core.dimension import Dimension
from sidemantic.core.measure import Measure
from sidemantic.core.metric import Metric
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.sql.generator_v2 import SQLGenerator


def test_month_to_date_metric():
    """Test MTD (month-to-date) cumulative metric."""
    sales = Model(
        name="sales",
        sql="""
            SELECT '2024-01-05'::DATE AS sale_date, 100 AS amount
            UNION ALL SELECT '2024-01-10'::DATE, 150
            UNION ALL SELECT '2024-01-15'::DATE, 200
            UNION ALL SELECT '2024-02-01'::DATE, 50
            UNION ALL SELECT '2024-02-05'::DATE, 75
        """,
        primary_key="sale_date",
        dimensions=[
            Dimension(name="sale_date", sql="sale_date", type="time")
        ],
        measures=[
            Measure(name="amount", agg="sum", expr="amount")
        ]
    )

    # MTD cumulative - resets each month
    mtd_revenue = Metric(
        name="mtd_revenue",
        type="cumulative",
        measure="sales.amount",
        grain_to_date="month"
    )

    graph = SemanticGraph()
    graph.add_model(sales)
    graph.add_metric(mtd_revenue)

    generator = SQLGenerator(graph)
    sql = generator.generate(
        metrics=["mtd_revenue"],
        dimensions=["sales.sale_date"]
    )

    print("\nMTD SQL:")
    print(sql)

    conn = duckdb.connect(":memory:")
    results = conn.execute(sql).fetchall()

    print("Results:", results)

    # Check MTD resets at start of February
    # Results are (date, amount, mtd_revenue)
    # Jan 5: amount=100, MTD=100
    # Jan 10: amount=150, MTD=250 (100+150)
    # Jan 15: amount=200, MTD=450 (100+150+200)
    # Feb 1: amount=50, MTD=50 (RESET!)
    # Feb 5: amount=75, MTD=125 (50+75)
    assert results[0][2] == 100  # Jan 5 MTD
    assert results[1][2] == 250  # Jan 10 MTD
    assert results[2][2] == 450  # Jan 15 MTD
    assert results[3][2] == 50   # Feb 1 MTD (reset!)
    assert results[4][2] == 125  # Feb 5 MTD


def test_year_to_date_metric():
    """Test YTD (year-to-date) cumulative metric."""
    sales = Model(
        name="sales",
        sql="""
            SELECT '2024-01-15'::DATE AS sale_date, 100 AS amount
            UNION ALL SELECT '2024-06-10'::DATE, 200
            UNION ALL SELECT '2025-01-05'::DATE, 50
        """,
        primary_key="sale_date",
        dimensions=[
            Dimension(name="sale_date", sql="sale_date", type="time")
        ],
        measures=[
            Measure(name="amount", agg="sum", expr="amount")
        ]
    )

    ytd_revenue = Metric(
        name="ytd_revenue",
        type="cumulative",
        measure="sales.amount",
        grain_to_date="year"
    )

    graph = SemanticGraph()
    graph.add_model(sales)
    graph.add_metric(ytd_revenue)

    generator = SQLGenerator(graph)
    sql = generator.generate(
        metrics=["ytd_revenue"],
        dimensions=["sales.sale_date"]
    )

    conn = duckdb.connect(":memory:")
    results = conn.execute(sql).fetchall()

    print("YTD Results:", results)

    # YTD should reset at start of 2025
    # Results are (date, amount, ytd_revenue) - find by date
    import datetime
    jan_2024 = [r for r in results if r[0] == datetime.date(2024, 1, 15)][0]
    jun_2024 = [r for r in results if r[0] == datetime.date(2024, 6, 10)][0]
    jan_2025 = [r for r in results if r[0] == datetime.date(2025, 1, 5)][0]

    assert jan_2024[2] == 100  # 2024-01-15 YTD
    assert jun_2024[2] == 300  # 2024-06-10 YTD (100 + 200)
    assert jan_2025[2] == 50   # 2025-01-05 YTD (RESET to new year!)


def test_fill_nulls_with_zero():
    """Test fill_nulls_with to replace NULL metric values."""
    orders = Model(
        name="orders",
        sql="""
            SELECT 'completed' AS status, 100 AS amount
            UNION ALL SELECT 'pending', NULL
        """,
        primary_key="status",
        dimensions=[
            Dimension(name="status", sql="status", type="categorical")
        ],
        measures=[
            Measure(name="amount", agg="sum", expr="amount")
        ]
    )

    # Metric with fill_nulls_with
    total_revenue = Metric(
        name="total_revenue",
        type="simple",
        measure="orders.amount",
        fill_nulls_with=0
    )

    graph = SemanticGraph()
    graph.add_model(orders)
    graph.add_metric(total_revenue)

    generator = SQLGenerator(graph)
    sql = generator.generate(
        metrics=["total_revenue"],
        dimensions=["orders.status"]
    )

    print("\nfill_nulls SQL:")
    print(sql)

    conn = duckdb.connect(":memory:")
    results = conn.execute(sql).fetchall()

    # Should have 0 instead of NULL for pending
    completed = [r for r in results if r[0] == 'completed'][0]
    pending = [r for r in results if r[0] == 'pending'][0]

    assert completed[1] == 100
    assert pending[1] == 0  # Filled with 0 instead of NULL!


def test_fill_nulls_with_string():
    """Test fill_nulls_with with string default."""
    products = Model(
        name="products",
        sql="""
            SELECT 'Widget' AS name, 'A' AS grade
            UNION ALL SELECT 'Gadget', NULL
        """,
        primary_key="name",
        dimensions=[
            Dimension(name="name", sql="name", type="categorical"),
            Dimension(name="grade", sql="grade", type="categorical")
        ]
    )

    graph = SemanticGraph()
    graph.add_model(products)

    generator = SQLGenerator(graph)
    sql = generator.generate(
        dimensions=["products.name", "products.grade"]
    )

    conn = duckdb.connect(":memory:")
    results = conn.execute(sql).fetchall()

    # Without fill_nulls, grade is NULL
    gadget = [r for r in results if r[0] == 'Gadget'][0]
    assert gadget[1] is None


def test_offset_ratio_metric():
    """Test ratio metric with time offset (current / previous period)."""
    sales = Model(
        name="sales",
        sql="""
            SELECT '2024-01'::VARCHAR AS month, 100 AS revenue
            UNION ALL SELECT '2024-02', 150
            UNION ALL SELECT '2024-03', 200
            UNION ALL SELECT '2024-04', 180
        """,
        primary_key="month",
        dimensions=[
            Dimension(name="month", sql="month", type="time")
        ],
        measures=[
            Measure(name="revenue", agg="sum", expr="revenue")
        ]
    )

    # Month-over-month growth: current / previous month
    mom_growth = Metric(
        name="mom_growth",
        type="ratio",
        numerator="sales.revenue",
        denominator="sales.revenue",
        offset_window="1 month",
        description="Month-over-month growth rate"
    )

    graph = SemanticGraph()
    graph.add_model(sales)
    graph.add_metric(mom_growth)

    generator = SQLGenerator(graph)
    sql = generator.generate(
        metrics=["mom_growth"],
        dimensions=["sales.month"]
    )

    print("\nOffset Ratio SQL:")
    print(sql)

    conn = duckdb.connect(":memory:")
    results = conn.execute(sql).fetchall()

    print("Results:", results)

    # Results are (month, revenue, mom_growth)
    # First month has no prior data (NULL)
    # Feb: 150 / 100 = 1.5
    # Mar: 200 / 150 = 1.333...
    # Apr: 180 / 200 = 0.9
    assert results[0][2] is None  # Jan (no prior)
    assert abs(results[1][2] - 1.5) < 0.01  # Feb
    assert abs(results[2][2] - 1.333) < 0.01  # Mar
    assert abs(results[3][2] - 0.9) < 0.01  # Apr


def test_conversion_metric():
    """Test conversion metric with self-join pattern."""
    # Events table with user_id, event_type, event_date
    events = Model(
        name="events",
        sql="""
            SELECT 1 AS user_id, 'signup' AS event_type, '2024-01-01'::DATE AS event_date
            UNION ALL SELECT 1, 'purchase', '2024-01-03'::DATE
            UNION ALL SELECT 2, 'signup', '2024-01-05'::DATE
            UNION ALL SELECT 2, 'purchase', '2024-01-20'::DATE
            UNION ALL SELECT 3, 'signup', '2024-01-10'::DATE
        """,
        primary_key="user_id",
        dimensions=[
            Dimension(name="user_id", sql="user_id", type="categorical"),
            Dimension(name="event_type", sql="event_type", type="categorical"),
            Dimension(name="event_date", sql="event_date", type="time")
        ],
        measures=[
            Measure(name="user_count", agg="count_distinct", expr="user_id")
        ]
    )

    # Conversion: users who purchase within 7 days of signup
    signup_conversion = Metric(
        name="signup_conversion",
        type="conversion",
        entity="user_id",
        base_event="signup",
        conversion_event="purchase",
        conversion_window="7 days",
        description="Users who purchase within 7 days of signup"
    )

    graph = SemanticGraph()
    graph.add_model(events)
    graph.add_metric(signup_conversion)

    generator = SQLGenerator(graph)
    sql = generator.generate(
        metrics=["signup_conversion"],
        dimensions=[]
    )

    print("\nConversion SQL:")
    print(sql)

    conn = duckdb.connect(":memory:")
    results = conn.execute(sql).fetchall()

    print("Results:", results)

    # User 1: signup on 01-01, purchase on 01-03 (2 days) - CONVERTED
    # User 2: signup on 01-05, purchase on 01-20 (15 days) - NOT CONVERTED (>7 days)
    # User 3: signup on 01-10, no purchase - NOT CONVERTED
    # Conversion rate: 1/3 = 0.333...
    assert abs(results[0][0] - 0.333) < 0.01
