"""Test advanced metric features: grain-to-date, fill_nulls_with, offsets, conversion."""

import duckdb

from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.sql.generator_v2 import SQLGenerator
from tests.utils import df_rows


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
        dimensions=[Dimension(name="sale_date", sql="sale_date", type="time")],
        metrics=[Metric(name="amount", agg="sum", sql="amount")],
    )

    # MTD cumulative - resets each month
    mtd_revenue = Metric(name="mtd_revenue", type="cumulative", sql="sales.amount", grain_to_date="month")

    graph = SemanticGraph()
    graph.add_model(sales)
    graph.add_metric(mtd_revenue)

    generator = SQLGenerator(graph)
    sql = generator.generate(metrics=["mtd_revenue"], dimensions=["sales.sale_date"])

    print("\nMTD SQL:")
    print(sql)

    conn = duckdb.connect(":memory:")
    result = conn.execute(sql)
    rows = df_rows(result)

    print("Results:", rows)

    # Check MTD resets at start of February
    # Results are (date, amount, mtd_revenue)
    # Jan 5: amount=100, MTD=100
    # Jan 10: amount=150, MTD=250 (100+150)
    # Jan 15: amount=200, MTD=450 (100+150+200)
    # Feb 1: amount=50, MTD=50 (RESET!)
    # Feb 5: amount=75, MTD=125 (50+75)
    assert rows[0][2] == 100  # Jan 5 MTD
    assert rows[1][2] == 250  # Jan 10 MTD
    assert rows[2][2] == 450  # Jan 15 MTD
    assert rows[3][2] == 50  # Feb 1 MTD (reset!)
    assert rows[4][2] == 125  # Feb 5 MTD


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
        dimensions=[Dimension(name="sale_date", sql="sale_date", type="time")],
        metrics=[Metric(name="amount", agg="sum", sql="amount")],
    )

    ytd_revenue = Metric(name="ytd_revenue", type="cumulative", sql="sales.amount", grain_to_date="year")

    graph = SemanticGraph()
    graph.add_model(sales)
    graph.add_metric(ytd_revenue)

    generator = SQLGenerator(graph)
    sql = generator.generate(metrics=["ytd_revenue"], dimensions=["sales.sale_date"])

    conn = duckdb.connect(":memory:")
    result = conn.execute(sql)
    rows = df_rows(result)

    print("YTD Results:", rows)

    # YTD should reset at start of 2025
    # Results are (date, amount, ytd_revenue) - find by date
    import datetime

    jan_2024 = [r for r in rows if r[0] == datetime.date(2024, 1, 15)][0]
    jun_2024 = [r for r in rows if r[0] == datetime.date(2024, 6, 10)][0]
    jan_2025 = [r for r in rows if r[0] == datetime.date(2025, 1, 5)][0]

    assert jan_2024[2] == 100  # 2024-01-15 YTD
    assert jun_2024[2] == 300  # 2024-06-10 YTD (100 + 200)
    assert jan_2025[2] == 50  # 2025-01-05 YTD (RESET to new year!)


def test_fill_nulls_with_zero():
    """Test fill_nulls_with to replace NULL metric values."""
    orders = Model(
        name="orders",
        sql="""
            SELECT 'completed' AS status, 100 AS amount
            UNION ALL SELECT 'pending', NULL
        """,
        primary_key="status",
        dimensions=[Dimension(name="status", sql="status", type="categorical")],
        metrics=[Metric(name="amount", agg="sum", sql="amount")],
    )

    # Metric with fill_nulls_with
    total_revenue = Metric(name="total_revenue", sql="orders.amount", fill_nulls_with=0)

    graph = SemanticGraph()
    graph.add_model(orders)
    graph.add_metric(total_revenue)

    generator = SQLGenerator(graph)
    sql = generator.generate(metrics=["total_revenue"], dimensions=["orders.status"])

    print("\nfill_nulls SQL:")
    print(sql)

    conn = duckdb.connect(":memory:")
    result = conn.execute(sql)
    rows = df_rows(result)

    # Should have 0 instead of NULL for pending
    completed = [r for r in rows if r[0] == "completed"][0]
    pending = [r for r in rows if r[0] == "pending"][0]

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
            Dimension(name="grade", sql="grade", type="categorical"),
        ],
    )

    graph = SemanticGraph()
    graph.add_model(products)

    generator = SQLGenerator(graph)
    sql = generator.generate(dimensions=["products.name", "products.grade"])

    conn = duckdb.connect(":memory:")
    result = conn.execute(sql)
    rows = df_rows(result)

    # Without fill_nulls, grade is NULL
    gadget = [r for r in rows if r[0] == "Gadget"][0]
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
        dimensions=[Dimension(name="month", sql="month", type="time")],
        metrics=[Metric(name="revenue", agg="sum", sql="revenue")],
    )

    # Month-over-month growth: current / previous month
    mom_growth = Metric(
        name="mom_growth",
        type="ratio",
        numerator="sales.revenue",
        denominator="sales.revenue",
        offset_window="1 month",
        description="Month-over-month growth rate",
    )

    graph = SemanticGraph()
    graph.add_model(sales)
    graph.add_metric(mom_growth)

    generator = SQLGenerator(graph)
    sql = generator.generate(metrics=["mom_growth"], dimensions=["sales.month"])

    print("\nOffset Ratio SQL:")
    print(sql)

    conn = duckdb.connect(":memory:")
    result = conn.execute(sql)
    rows = df_rows(result)

    print("Results:", rows)

    # Results are (month, revenue, mom_growth)
    # First month has no prior data (NULL)
    # Feb: 150 / 100 = 1.5
    # Mar: 200 / 150 = 1.333...
    # Apr: 180 / 200 = 0.9
    assert rows[0][2] is None  # Jan (no prior)
    assert abs(rows[1][2] - 1.5) < 0.01  # Feb
    assert abs(rows[2][2] - 1.333) < 0.01  # Mar
    assert abs(rows[3][2] - 0.9) < 0.01  # Apr


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
            Dimension(name="event_date", sql="event_date", type="time"),
        ],
        metrics=[Metric(name="user_count", agg="count_distinct", sql="user_id")],
    )

    # Conversion: users who purchase within 7 days of signup
    signup_conversion = Metric(
        name="signup_conversion",
        type="conversion",
        entity="user_id",
        base_event="signup",
        conversion_event="purchase",
        conversion_window="7 days",
        description="Users who purchase within 7 days of signup",
    )

    graph = SemanticGraph()
    graph.add_model(events)
    graph.add_metric(signup_conversion)

    generator = SQLGenerator(graph)
    sql = generator.generate(metrics=["signup_conversion"], dimensions=[])

    print("\nConversion SQL:")
    print(sql)

    conn = duckdb.connect(":memory:")
    result = conn.execute(sql)
    rows = df_rows(result)

    print("Results:", rows)

    # User 1: signup on 01-01, purchase on 01-03 (2 days) - CONVERTED
    # User 2: signup on 01-05, purchase on 01-20 (15 days) - NOT CONVERTED (>7 days)
    # User 3: signup on 01-10, no purchase - NOT CONVERTED
    # Conversion rate: 1/3 = 0.333...
    assert abs(rows[0][0] - 0.333) < 0.01


def test_yoy_percent_change():
    """Test year-over-year percent change metric with monthly granularity.

    Note: YoY with monthly data requires LAG(12) which means you need at least 12 months
    of historical data. This test includes full year data to demonstrate proper YoY calculation.
    """
    # Create 12 months of 2023 data + 3 months of 2024 data
    sales = Model(
        name="sales",
        sql="""
            SELECT '2023-01-01'::DATE AS sale_date, 100 AS revenue
            UNION ALL SELECT '2023-02-01'::DATE, 100
            UNION ALL SELECT '2023-03-01'::DATE, 100
            UNION ALL SELECT '2023-04-01'::DATE, 100
            UNION ALL SELECT '2023-05-01'::DATE, 100
            UNION ALL SELECT '2023-06-01'::DATE, 100
            UNION ALL SELECT '2023-07-01'::DATE, 100
            UNION ALL SELECT '2023-08-01'::DATE, 100
            UNION ALL SELECT '2023-09-01'::DATE, 100
            UNION ALL SELECT '2023-10-01'::DATE, 100
            UNION ALL SELECT '2023-11-01'::DATE, 100
            UNION ALL SELECT '2023-12-01'::DATE, 100
            UNION ALL SELECT '2024-01-01'::DATE, 150
            UNION ALL SELECT '2024-02-01'::DATE, 200
            UNION ALL SELECT '2024-03-01'::DATE, 180
        """,
        primary_key="sale_date",
        dimensions=[Dimension(name="sale_date", sql="sale_date", type="time")],
        metrics=[Metric(name="revenue", agg="sum", sql="revenue")],
    )

    # YoY percent change metric
    revenue_yoy = Metric(
        name="revenue_yoy",
        type="time_comparison",
        base_metric="sales.revenue",
        comparison_type="yoy",
        calculation="percent_change",
        description="Year-over-year revenue growth",
    )

    graph = SemanticGraph()
    graph.add_model(sales)
    graph.add_metric(revenue_yoy)

    generator = SQLGenerator(graph)
    sql = generator.generate(
        metrics=["revenue_yoy"],
        dimensions=["sales.sale_date__month"],  # Use explicit month granularity
    )

    print("\nYoY Percent Change SQL:")
    print(sql)

    conn = duckdb.connect(":memory:")
    result = conn.execute(sql)
    rows = df_rows(result)

    print("YoY Results:", rows)

    # Find 2024 results (dates are datetime objects after month truncation)
    results_2024 = [r for r in rows if r[0].year == 2024]
    assert len(results_2024) == 3

    # Check YoY percent changes
    # 2024-01: (150-100)/100 * 100 = 50%
    # 2024-02: (200-100)/100 * 100 = 100%
    # 2024-03: (180-100)/100 * 100 = 80%
    assert abs(results_2024[0][2] - 50.0) < 0.1  # 2024-01 YoY
    assert abs(results_2024[1][2] - 100.0) < 0.1  # 2024-02 YoY
    assert abs(results_2024[2][2] - 80.0) < 0.1  # 2024-03 YoY


def test_mom_difference():
    """Test month-over-month difference metric."""
    sales = Model(
        name="sales",
        sql="""
            SELECT '2024-01'::VARCHAR AS month, 100 AS revenue
            UNION ALL SELECT '2024-02', 150
            UNION ALL SELECT '2024-03', 120
            UNION ALL SELECT '2024-04', 180
        """,
        primary_key="month",
        dimensions=[Dimension(name="month", sql="month", type="time")],
        metrics=[Metric(name="revenue", agg="sum", sql="revenue")],
    )

    # MoM difference metric
    revenue_mom = Metric(
        name="revenue_mom_change",
        type="time_comparison",
        base_metric="sales.revenue",
        comparison_type="mom",
        calculation="difference",
        description="Month-over-month revenue change",
    )

    graph = SemanticGraph()
    graph.add_model(sales)
    graph.add_metric(revenue_mom)

    generator = SQLGenerator(graph)
    sql = generator.generate(metrics=["revenue_mom_change"], dimensions=["sales.month"])

    print("\nMoM Difference SQL:")
    print(sql)

    conn = duckdb.connect(":memory:")
    result = conn.execute(sql)
    rows = df_rows(result)

    print("MoM Results:", rows)

    # Results should be:
    # 2024-01: NULL (no prior month)
    # 2024-02: 150 - 100 = 50
    # 2024-03: 120 - 150 = -30
    # 2024-04: 180 - 120 = 60

    assert rows[0][2] is None  # Jan (no prior)
    assert rows[1][2] == 50  # Feb
    assert rows[2][2] == -30  # Mar
    assert rows[3][2] == 60  # Apr


def test_wow_ratio():
    """Test week-over-week ratio metric with weekly granularity."""
    sales = Model(
        name="sales",
        sql="""
            SELECT '2024-01-01'::DATE AS sale_date, 100 AS revenue
            UNION ALL SELECT '2024-01-08'::DATE, 150
            UNION ALL SELECT '2024-01-15'::DATE, 120
            UNION ALL SELECT '2024-01-22'::DATE, 180
        """,
        primary_key="sale_date",
        dimensions=[Dimension(name="sale_date", sql="sale_date", type="time")],
        metrics=[Metric(name="revenue", agg="sum", sql="revenue")],
    )

    # WoW ratio metric
    revenue_wow = Metric(
        name="revenue_wow_ratio",
        type="time_comparison",
        base_metric="sales.revenue",
        comparison_type="wow",
        calculation="ratio",
        description="Week-over-week revenue ratio",
    )

    graph = SemanticGraph()
    graph.add_model(sales)
    graph.add_metric(revenue_wow)

    generator = SQLGenerator(graph)
    sql = generator.generate(metrics=["revenue_wow_ratio"], dimensions=["sales.sale_date__week"])

    print("\nWoW Ratio SQL:")
    print(sql)

    conn = duckdb.connect(":memory:")
    result = conn.execute(sql)
    rows = df_rows(result)

    print("WoW Results:", rows)

    # Results should be (with week granularity, LAG offset = 1):
    # Week 1 (2024-01-01): NULL (no prior week)
    # Week 2 (2024-01-08): 150/100 = 1.5
    # Week 3 (2024-01-15): 120/150 = 0.8
    # Week 4 (2024-01-22): 180/120 = 1.5

    assert rows[0][2] is None  # Week 1 (no prior)
    assert abs(rows[1][2] - 1.5) < 0.01  # Week 2
    assert abs(rows[2][2] - 0.8) < 0.01  # Week 3
    assert abs(rows[3][2] - 1.5) < 0.01  # Week 4
