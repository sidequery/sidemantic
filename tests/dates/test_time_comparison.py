"""Test that time_comparison metrics defined in model.metrics are auto-registered at graph level."""

import duckdb
import pytest

from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.sql.generator import SQLGenerator
from tests.utils import fetch_dicts


@pytest.mark.parametrize(
    ("comparison_type", "prior", "current"),
    [
        ("dod", "2024-03-01", "2024-03-02"),
        ("wow", "2024-03-01", "2024-03-08"),
        ("mom", "2024-03-01", "2024-04-01"),
        ("qoq", "2024-03-01", "2024-06-01"),
        ("yoy", "2023-03-01", "2024-03-01"),
    ],
)
def test_named_calendar_comparison_without_declared_granularity(comparison_type, prior, current):
    graph = SemanticGraph()
    graph.add_model(
        Model(
            name="sales",
            table="sales",
            primary_key="id",
            dimensions=[Dimension(name="day", type="time"), Dimension(name="category", type="categorical")],
            metrics=[Metric(name="revenue", agg="sum", sql="amount")],
        )
    )
    graph.add_metric(
        Metric(
            name="change",
            type="time_comparison",
            base_metric="sales.revenue",
            comparison_type=comparison_type,
            calculation="difference",
        )
    )
    sql = SQLGenerator(graph).generate(metrics=["change"], dimensions=["sales.day", "sales.category"])
    with duckdb.connect() as conn:
        conn.execute("create table sales(id integer, day date, category varchar, amount integer)")
        conn.executemany(
            "insert into sales values (?, ?, ?, ?)",
            [
                (1, "2020-01-01", "a", 100),
                (2, prior, "a", 180),
                (3, current, "a", 210),
                (4, "2020-01-01", None, 10),
                (5, prior, None, 30),
                (6, current, None, 50),
            ],
        )
        records = fetch_dicts(conn.execute(sql))
    by_key = {(str(row["day"]), row["category"]): row["change"] for row in records}
    assert by_key[prior, "a"] is None
    assert by_key[prior, None] is None
    assert by_key[current, "a"] == 30
    assert by_key[current, None] == 20


def test_model_level_time_comparison_metric():
    """Test that time_comparison metrics in model.metrics are auto-registered at graph level."""
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
        metrics=[
            Metric(name="revenue", agg="sum", sql="revenue"),
            # Time comparison metric defined at model level - should be auto-registered at graph
            Metric(
                name="revenue_mom_change",
                type="time_comparison",
                base_metric="sales.revenue",
                comparison_type="mom",
                calculation="difference",
            ),
        ],
    )

    graph = SemanticGraph()
    graph.add_model(sales)

    # Verify the time_comparison metric was auto-registered at graph level
    assert "revenue_mom_change" in graph.metrics
    assert graph.metrics["revenue_mom_change"].type == "time_comparison"

    # Verify it works in SQL generation
    generator = SQLGenerator(graph)
    sql = generator.generate(metrics=["revenue_mom_change"], dimensions=["sales.calendar_month"])

    print("\nMoM Difference SQL:")
    print(sql)

    conn = duckdb.connect(":memory:")
    result = conn.execute(sql)
    records = fetch_dicts(result)

    print("MoM Results:", records)

    # Results should be:
    # 2024-01: NULL (no prior month)
    # 2024-02: 150 - 100 = 50
    # 2024-03: 120 - 150 = -30
    # 2024-04: 180 - 120 = 60

    assert records[0]["revenue_mom_change"] is None  # Jan (no prior)
    assert records[1]["revenue_mom_change"] == 50  # Feb
    assert records[2]["revenue_mom_change"] == -30  # Mar
    assert records[3]["revenue_mom_change"] == 60  # Apr


def test_model_level_conversion_metric():
    """Test that conversion metrics in model.metrics are auto-registered at graph level."""
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
        metrics=[
            Metric(name="user_count", agg="count_distinct", sql="user_id"),
            # Conversion metric defined at model level - should be auto-registered at graph
            Metric(
                name="signup_conversion",
                type="conversion",
                entity="user_id",
                base_event="signup",
                conversion_event="purchase",
                conversion_window="7 days",
            ),
        ],
    )

    graph = SemanticGraph()
    graph.add_model(events)

    # Verify the conversion metric was auto-registered at graph level
    assert "signup_conversion" in graph.metrics
    assert graph.metrics["signup_conversion"].type == "conversion"

    # Verify it works in SQL generation
    generator = SQLGenerator(graph)
    sql = generator.generate(metrics=["signup_conversion"], dimensions=[])

    print("\nConversion SQL:")
    print(sql)

    conn = duckdb.connect(":memory:")
    result = conn.execute(sql)
    records = fetch_dicts(result)

    print("Results:", records)

    # User 1: signup on 01-01, purchase on 01-03 (2 days) - CONVERTED
    # User 2: signup on 01-05, purchase on 01-20 (15 days) - NOT CONVERTED (>7 days)
    # User 3: signup on 01-10, no purchase - NOT CONVERTED
    # Conversion rate: 1/3 = 0.333...
    assert abs(records[0]["signup_conversion"] - 0.333) < 0.01


def test_time_comparison_missing_base_metric_error():
    """Test that time_comparison metric without base_metric gives clear error at construction."""
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError, match="time_comparison metric requires 'base_metric' field"):
        Metric(
            name="revenue_mom",
            type="time_comparison",
            comparison_type="mom",
        )
