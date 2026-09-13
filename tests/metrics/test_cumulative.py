"""Test cumulative metrics with window functions.

Cumulative metrics use window functions for running totals and rolling windows.
Examples: running_total_revenue, 7_day_rolling_average
"""

import duckdb
import pytest

from sidemantic import Dimension, Metric, Model


@pytest.mark.parametrize(
    ("options", "expected"),
    [
        ({}, [1, 3, 7]),
        ({"window": "2 days"}, [1, 2, 6]),
        ({"grain_to_date": "month"}, [1, 3, 4]),
        ({"window_expression": "SUM(base.revenue)"}, [1, 3, 7]),
    ],
    ids=["running", "rolling", "month_to_date", "window_expression"],
)
@pytest.mark.parametrize("with_comparison", [False, True])
def test_cumulative_partitions_groups_and_nulls(layer, options, expected, with_comparison):
    """Each group has its own temporal window, including the null group."""
    layer.conn.execute("create table grouped_sales (id integer, day date, category varchar, amount integer)")
    dates = ["2024-01-28", "2024-01-31", "2024-02-01"]
    groups = [("a", 100), ("b", 10), (None, 1)]
    rows = [
        (group_index * 3 + day_index, day, category, factor * amount)
        for group_index, (category, factor) in enumerate(groups)
        for day_index, (day, amount) in enumerate(zip(dates, [1, 2, 4]))
    ]
    layer.conn.executemany("insert into grouped_sales values (?, ?, ?, ?)", rows)
    layer.add_model(
        Model(
            name="grouped_sales",
            table="grouped_sales",
            primary_key="id",
            dimensions=[
                Dimension(name="day", type="time", granularity="day"),
                Dimension(name="category", type="categorical"),
            ],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        )
    )
    layer.add_metric(Metric(name="cumulative", type="cumulative", sql="grouped_sales.revenue", **options))
    metrics = ["cumulative"]
    if with_comparison:
        layer.add_metric(
            Metric(
                name="daily_change",
                type="time_comparison",
                base_metric="grouped_sales.revenue",
                comparison_type="dod",
                calculation="difference",
            )
        )
        metrics.append("daily_change")

    result = layer.query(metrics=metrics, dimensions=["grouped_sales.day", "grouped_sales.category"])
    columns = [column[0] for column in result.description]
    records = [dict(zip(columns, row)) for row in result.fetchall()]
    by_group_day = {(row["category"], str(row["day"])): row for row in records}
    assert len(by_group_day) == 9
    for category, factor in groups:
        for day, amount in zip(dates, expected):
            assert by_group_day[category, day]["cumulative"] == factor * amount
        if with_comparison:
            assert by_group_day[category, dates[0]]["daily_change"] is None
            assert by_group_day[category, dates[1]]["daily_change"] is None
            assert by_group_day[category, dates[2]]["daily_change"] == factor * 2


@pytest.fixture
def timeseries_db():
    """Create test database with time-series order data."""
    conn = duckdb.connect(":memory:")

    conn.execute("""
        CREATE TABLE orders (
            order_id INTEGER,
            order_date DATE,
            order_amount DECIMAL(10, 2)
        )
    """)

    conn.execute("""
        INSERT INTO orders VALUES
            (1, '2024-01-01', 100.00),
            (2, '2024-01-02', 150.00),
            (3, '2024-01-03', 200.00),
            (4, '2024-01-04', 120.00),
            (5, '2024-01-05', 180.00)
    """)

    return conn


def test_running_total(timeseries_db, layer):
    """Test running total cumulative metric."""
    layer.conn = timeseries_db

    # Define model
    orders = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        dimensions=[Dimension(name="order_date", type="time", granularity="day", sql="order_date")],
        metrics=[Metric(name="daily_revenue", agg="sum", sql="order_amount")],
    )
    layer.add_model(orders)

    # Define cumulative metric
    running_total = Metric(
        name="running_total_revenue",
        type="cumulative",
        sql="orders.daily_revenue",
    )
    layer.add_metric(running_total)

    # Query with cumulative metric
    result = layer.query(
        metrics=["orders.daily_revenue", "running_total_revenue"],
        dimensions=["orders.order_date"],
        order_by=["orders.order_date"],
    )

    df = result.df()
    print("\nRunning Total Results:")
    print(df)

    # Verify running total
    assert len(df) == 5
    assert df["running_total_revenue"].iloc[0] == 100.00  # Day 1: 100
    assert df["running_total_revenue"].iloc[1] == 250.00  # Day 2: 100 + 150
    assert df["running_total_revenue"].iloc[2] == 450.00  # Day 3: 100 + 150 + 200
    assert df["running_total_revenue"].iloc[3] == 570.00  # Day 4: 100 + 150 + 200 + 120
    assert df["running_total_revenue"].iloc[4] == 750.00  # Day 5: 100 + 150 + 200 + 120 + 180


def test_rolling_window(timeseries_db, layer):
    """Test rolling window cumulative metric."""
    layer.conn = timeseries_db

    # Define model
    orders = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        dimensions=[Dimension(name="order_date", type="time", granularity="day", sql="order_date")],
        metrics=[Metric(name="daily_revenue", agg="sum", sql="order_amount")],
    )
    layer.add_model(orders)

    # Define 3-day rolling window metric
    rolling_metric = Metric(
        name="rolling_3day_revenue",
        type="cumulative",
        sql="orders.daily_revenue",
        window="2 days",  # Current + 2 preceding = 3 days total
    )
    layer.add_metric(rolling_metric)

    # Query
    result = layer.query(
        metrics=["orders.daily_revenue", "rolling_3day_revenue"],
        dimensions=["orders.order_date"],
        order_by=["orders.order_date"],
    )

    df = result.df()
    print("\nRolling Window Results:")
    print(df)

    # Verify rolling window
    assert len(df) == 5
    # Day 1: just 100
    assert df["rolling_3day_revenue"].iloc[0] == 100.00
    # Day 2: 100 + 150 = 250
    assert df["rolling_3day_revenue"].iloc[1] == 250.00
    # Day 3: 100 + 150 + 200 = 450
    assert df["rolling_3day_revenue"].iloc[2] == 450.00
    # Day 4: 150 + 200 + 120 = 470 (drops day 1)
    assert df["rolling_3day_revenue"].iloc[3] == 470.00
    # Day 5: 200 + 120 + 180 = 500 (drops days 1-2)
    assert df["rolling_3day_revenue"].iloc[4] == 500.00


def test_cumulative_with_regular_metric(timeseries_db, layer):
    """Test cumulative metric alongside regular metric."""
    layer.conn = timeseries_db

    # Define model
    orders = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        dimensions=[Dimension(name="order_date", type="time", granularity="day", sql="order_date")],
        metrics=[Metric(name="daily_revenue", agg="sum", sql="order_amount")],
    )
    layer.add_model(orders)

    # Define both regular and cumulative metrics
    total_revenue = Metric(name="total_revenue", sql="orders.daily_revenue")
    running_total = Metric(name="running_total", type="cumulative", sql="orders.daily_revenue")

    layer.add_metric(total_revenue)
    layer.add_metric(running_total)

    # Query with both metric types
    result = layer.query(
        metrics=["total_revenue", "running_total"],
        dimensions=["orders.order_date"],
        order_by=["orders.order_date"],
    )

    df = result.df()
    print("\nCumulative with Regular Metric:")
    print(df)

    # Verify both metrics work together
    assert len(df) == 5
    # Note: daily_revenue appears because it's the base measure for both metrics
    assert "order_date" in df.columns
    assert "total_revenue" in df.columns
    assert "running_total" in df.columns
    # Day 1
    assert df["total_revenue"].iloc[0] == 100.00
    assert df["running_total"].iloc[0] == 100.00
    # Day 3
    assert df["total_revenue"].iloc[2] == 200.00
    assert df["running_total"].iloc[2] == 450.00


def test_rolling_average(timeseries_db, layer):
    """Test rolling average using agg: avg with cumulative type."""
    layer.conn = timeseries_db

    # Define model
    orders = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        dimensions=[Dimension(name="order_date", type="time", granularity="day", sql="order_date")],
        metrics=[Metric(name="daily_revenue", agg="sum", sql="order_amount")],
    )
    layer.add_model(orders)

    # Define 3-day rolling average using agg field
    rolling_avg = Metric(
        name="rolling_3day_avg",
        type="cumulative",
        agg="avg",
        sql="orders.daily_revenue",
        window="2 days",  # Current + 2 preceding = 3 days total
    )
    layer.add_metric(rolling_avg)

    # Query
    result = layer.query(
        metrics=["orders.daily_revenue", "rolling_3day_avg"],
        dimensions=["orders.order_date"],
        order_by=["orders.order_date"],
    )

    df = result.df()
    print("\nRolling Average Results:")
    print(df)

    # Verify rolling average
    assert len(df) == 5
    # Day 1: just 100 / 1 = 100
    assert df["rolling_3day_avg"].iloc[0] == 100.00
    # Day 2: (100 + 150) / 2 = 125
    assert df["rolling_3day_avg"].iloc[1] == 125.00
    # Day 3: (100 + 150 + 200) / 3 = 150
    assert df["rolling_3day_avg"].iloc[2] == 150.00
    # Day 4: (150 + 200 + 120) / 3 = 156.67 (drops day 1)
    assert abs(df["rolling_3day_avg"].iloc[3] - 156.67) < 0.1
    # Day 5: (200 + 120 + 180) / 3 = 166.67 (drops days 1-2)
    assert abs(df["rolling_3day_avg"].iloc[4] - 166.67) < 0.1


def test_rolling_average_parsed_from_sql(timeseries_db, layer):
    """Test rolling average where AVG() is parsed from sql field."""
    layer.conn = timeseries_db

    # Define model
    orders = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        dimensions=[Dimension(name="order_date", type="time", granularity="day", sql="order_date")],
        metrics=[Metric(name="daily_revenue", agg="sum", sql="order_amount")],
    )
    layer.add_model(orders)

    # Define rolling average with AVG() in sql - should be parsed to agg=avg
    rolling_avg = Metric(
        name="rolling_avg_parsed",
        type="cumulative",
        sql="AVG(orders.daily_revenue)",  # Should parse to agg=avg, sql=orders.daily_revenue
        window="2 days",
    )
    layer.add_metric(rolling_avg)

    # Verify the metric was parsed correctly
    assert rolling_avg.agg == "avg"
    assert rolling_avg.sql == "orders.daily_revenue"

    # Query
    result = layer.query(
        metrics=["orders.daily_revenue", "rolling_avg_parsed"],
        dimensions=["orders.order_date"],
        order_by=["orders.order_date"],
    )

    df = result.df()
    print("\nRolling Average (Parsed) Results:")
    print(df)

    # Verify results match explicit agg version
    assert len(df) == 5
    assert df["rolling_avg_parsed"].iloc[0] == 100.00
    assert df["rolling_avg_parsed"].iloc[1] == 125.00


def test_window_expression_passthrough(timeseries_db, layer):
    """Test arbitrary window expression using window_expression field."""
    layer.conn = timeseries_db

    # Define model
    orders = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        dimensions=[Dimension(name="order_date", type="time", granularity="day", sql="order_date")],
        metrics=[Metric(name="daily_revenue", agg="sum", sql="order_amount")],
    )
    layer.add_model(orders)

    # Define metric with raw window expression
    custom_window = Metric(
        name="custom_window_metric",
        type="cumulative",
        window_expression="AVG(base.daily_revenue)",
        window_frame="RANGE BETWEEN INTERVAL 2 DAY PRECEDING AND CURRENT ROW",
        window_order="order_date",
    )
    layer.add_metric(custom_window)

    # Query
    result = layer.query(
        metrics=["orders.daily_revenue", "custom_window_metric"],
        dimensions=["orders.order_date"],
        order_by=["orders.order_date"],
    )

    df = result.df()
    print("\nWindow Expression Passthrough Results:")
    print(df)

    # Should produce same results as rolling average
    assert len(df) == 5
    assert df["custom_window_metric"].iloc[0] == 100.00


def test_rolling_count(timeseries_db, layer):
    """Test rolling count using agg: count."""
    layer.conn = timeseries_db

    # Define model with count metric
    orders = Model(
        name="orders",
        table="orders",
        primary_key="order_id",
        dimensions=[Dimension(name="order_date", type="time", granularity="day", sql="order_date")],
        metrics=[Metric(name="order_count", agg="count", sql="order_id")],
    )
    layer.add_model(orders)

    # Define 3-day rolling count
    rolling_count = Metric(
        name="rolling_3day_count",
        type="cumulative",
        agg="count",
        sql="orders.order_count",
        window="2 days",
    )
    layer.add_metric(rolling_count)

    # Query
    result = layer.query(
        metrics=["orders.order_count", "rolling_3day_count"],
        dimensions=["orders.order_date"],
        order_by=["orders.order_date"],
    )

    df = result.df()
    print("\nRolling Count Results:")
    print(df)

    # Each day has 1 order, so rolling count should be 1, 2, 3, 3, 3
    assert len(df) == 5
    assert df["rolling_3day_count"].iloc[0] == 1
    assert df["rolling_3day_count"].iloc[1] == 2
    assert df["rolling_3day_count"].iloc[2] == 3
    assert df["rolling_3day_count"].iloc[3] == 3
    assert df["rolling_3day_count"].iloc[4] == 3


def test_cumulative_with_time_comparison(layer):
    """Cumulative window expressions must appear in LAG CTE output."""
    from sidemantic.core.semantic_graph import SemanticGraph
    from sidemantic.sql.generator import SQLGenerator
    from tests.utils import fetch_dicts

    sales = Model(
        name="sales",
        sql="""
            SELECT '2024-01'::VARCHAR AS month, 100 AS revenue
            UNION ALL SELECT '2024-02', 150
            UNION ALL SELECT '2024-03', 120
            UNION ALL SELECT '2024-04', 180
        """,
        primary_key="month",
        # Source months are strings; declare their calendar meaning explicitly.
        dimensions=[Dimension(name="calendar_month", sql="CAST(month || '-01' AS DATE)", type="time")],
        metrics=[Metric(name="revenue", agg="sum", sql="revenue")],
    )

    graph = SemanticGraph()
    graph.add_model(sales)

    graph.add_metric(
        Metric(
            name="revenue_mom",
            type="time_comparison",
            base_metric="sales.revenue",
            comparison_type="mom",
            calculation="difference",
        )
    )

    graph.add_metric(
        Metric(
            name="running_revenue",
            type="cumulative",
            sql="sales.revenue",
        )
    )

    gen = SQLGenerator(graph)
    sql = gen.generate(
        metrics=["running_revenue", "revenue_mom"],
        dimensions=["sales.calendar_month"],
    )

    assert "running_revenue" in sql
    assert "revenue_mom" in sql

    conn = duckdb.connect(":memory:")
    result = conn.execute(sql)
    records = fetch_dicts(result)
    conn.close()

    assert len(records) == 4
    assert records[0]["running_revenue"] == 100
    assert records[1]["running_revenue"] == 250
    assert records[0]["revenue_mom"] is None
    assert records[1]["revenue_mom"] == 50


def test_time_comparison_over_derived_cumulative_chain():
    """Time comparison should support cumulative -> derived dependency chains."""
    from sidemantic.core.semantic_graph import SemanticGraph
    from sidemantic.sql.generator import SQLGenerator
    from tests.utils import fetch_dicts

    sales = Model(
        name="sales",
        sql="""
            SELECT
                month_start,
                CASE WHEN month_start < DATE '2024-01-01' THEN 100 ELSE 200 END AS gbv,
                CASE WHEN month_start < DATE '2024-01-01' THEN 10 ELSE 20 END AS bookings
            FROM generate_series(
                DATE '2023-01-01',
                DATE '2024-03-01',
                INTERVAL 1 MONTH
            ) AS t(month_start)
        """,
        primary_key="month_start",
        dimensions=[Dimension(name="month_start", sql="month_start", type="time")],
        metrics=[
            Metric(name="gbv", agg="sum", sql="gbv"),
            Metric(name="bookings", agg="sum", sql="bookings"),
        ],
    )

    graph = SemanticGraph()
    graph.add_model(sales)
    graph.add_metric(Metric(name="ytd_gbv", type="cumulative", sql="sales.gbv", grain_to_date="year"))
    graph.add_metric(Metric(name="ytd_bookings", type="cumulative", sql="sales.bookings", grain_to_date="year"))
    graph.add_metric(Metric(name="ytd_abv", type="derived", sql="ytd_gbv / ytd_bookings"))
    graph.add_metric(
        Metric(
            name="yoy_ytd_abv_growth",
            type="time_comparison",
            base_metric="ytd_abv",
            comparison_type="yoy",
            calculation="percent_change",
        )
    )

    gen = SQLGenerator(graph)
    sql = gen.generate(
        metrics=["ytd_abv", "yoy_ytd_abv_growth"],
        dimensions=["sales.month_start__month"],
        order_by=["sales.month_start__month"],
    )

    conn = duckdb.connect(":memory:")
    records = fetch_dicts(conn.execute(sql))
    conn.close()

    assert len(records) == 15
    results_2024 = [r for r in records if r["month_start__month"].year == 2024]
    assert len(results_2024) == 3
    assert all(abs(float(r["ytd_abv"]) - 10.0) < 1e-9 for r in results_2024)
    assert all(abs(float(r["yoy_ytd_abv_growth"])) < 1e-9 for r in results_2024)


def test_time_comparison_over_ratio_cumulative_chain():
    """Time comparison should support cumulative -> ratio dependency chains."""
    from sidemantic.core.semantic_graph import SemanticGraph
    from sidemantic.sql.generator import SQLGenerator
    from tests.utils import fetch_dicts

    sales = Model(
        name="sales",
        sql="""
            SELECT
                month_start,
                CASE WHEN month_start < DATE '2024-01-01' THEN 100 ELSE 200 END AS gbv,
                CASE WHEN month_start < DATE '2024-01-01' THEN 10 ELSE 20 END AS bookings
            FROM generate_series(
                DATE '2023-01-01',
                DATE '2024-03-01',
                INTERVAL 1 MONTH
            ) AS t(month_start)
        """,
        primary_key="month_start",
        dimensions=[Dimension(name="month_start", sql="month_start", type="time")],
        metrics=[
            Metric(name="gbv", agg="sum", sql="gbv"),
            Metric(name="bookings", agg="sum", sql="bookings"),
        ],
    )

    graph = SemanticGraph()
    graph.add_model(sales)
    graph.add_metric(Metric(name="ytd_gbv", type="cumulative", sql="sales.gbv", grain_to_date="year"))
    graph.add_metric(Metric(name="ytd_bookings", type="cumulative", sql="sales.bookings", grain_to_date="year"))
    graph.add_metric(Metric(name="ytd_abv_ratio", type="ratio", numerator="ytd_gbv", denominator="ytd_bookings"))
    graph.add_metric(
        Metric(
            name="yoy_ytd_abv_ratio_growth",
            type="time_comparison",
            base_metric="ytd_abv_ratio",
            comparison_type="yoy",
            calculation="percent_change",
        )
    )

    gen = SQLGenerator(graph)
    sql = gen.generate(
        metrics=["ytd_abv_ratio", "yoy_ytd_abv_ratio_growth"],
        dimensions=["sales.month_start__month"],
        order_by=["sales.month_start__month"],
    )

    conn = duckdb.connect(":memory:")
    records = fetch_dicts(conn.execute(sql))
    conn.close()

    assert len(records) == 15
    results_2024 = [r for r in records if r["month_start__month"].year == 2024]
    assert len(results_2024) == 3
    assert all(abs(float(r["ytd_abv_ratio"]) - 10.0) < 1e-9 for r in results_2024)
    assert all(abs(float(r["yoy_ytd_abv_ratio_growth"])) < 1e-9 for r in results_2024)


def test_time_comparison_with_dependency_free_expression_metric():
    """Dependency-free expression metrics should not be recomputed in outer window queries."""
    from sidemantic.core.semantic_graph import SemanticGraph
    from sidemantic.sql.generator import SQLGenerator

    sales = Model(
        name="sales",
        sql="""
            SELECT
                month_start,
                CASE WHEN month_start < DATE '2024-01-01' THEN 100 ELSE 200 END AS gbv,
                CASE WHEN month_start < DATE '2024-01-01' THEN 10 ELSE 20 END AS bookings,
                CASE WHEN month_start < DATE '2024-01-01' THEN 50 ELSE 75 END AS revenue
            FROM generate_series(
                DATE '2023-01-01',
                DATE '2024-03-01',
                INTERVAL 1 MONTH
            ) AS t(month_start)
        """,
        primary_key="month_start",
        dimensions=[Dimension(name="month_start", sql="month_start", type="time")],
        metrics=[
            Metric(name="gbv", agg="sum", sql="gbv"),
            Metric(name="bookings", agg="sum", sql="bookings"),
            Metric(name="revenue", agg="sum", sql="revenue"),
        ],
    )

    graph = SemanticGraph()
    graph.add_model(sales)
    graph.add_metric(Metric(name="abv_inline", sql="SUM(sales.gbv) / NULLIF(SUM(sales.bookings), 0)"))
    graph.add_metric(Metric(name="ytd_revenue", type="cumulative", sql="sales.revenue", grain_to_date="year"))
    graph.add_metric(
        Metric(
            name="yoy_ytd_revenue_growth",
            type="time_comparison",
            base_metric="ytd_revenue",
            comparison_type="yoy",
            calculation="percent_change",
        )
    )

    gen = SQLGenerator(graph)
    sql = gen.generate(
        metrics=["abv_inline", "ytd_revenue", "yoy_ytd_revenue_growth"],
        dimensions=["sales.month_start__month"],
        order_by=["sales.month_start__month"],
    )

    assert sql.count(" AS abv_inline") == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
