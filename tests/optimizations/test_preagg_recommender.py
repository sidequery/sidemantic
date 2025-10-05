"""Tests for pre-aggregation recommendation."""

import pytest

from sidemantic import Dimension, Metric, Model, SemanticLayer
from sidemantic.core.preagg_recommender import PreAggregationRecommender, QueryPattern


def test_query_instrumentation():
    """Test that generated queries include instrumentation comments."""
    sl = SemanticLayer()

    model = Model(
        name="orders",
        table="public.orders",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical", sql="status"),
            Dimension(name="created_at", type="time", sql="created_at", granularity="day"),
        ],
        metrics=[
            Metric(name="count", agg="count"),
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    sl.add_model(model)

    # Generate query
    sql = sl.compile(
        metrics=["orders.revenue", "orders.count"],
        dimensions=["orders.status", "orders.created_at__day"],
    )

    # Check instrumentation is present
    assert "-- sidemantic:" in sql
    assert "models=orders" in sql
    assert "metrics=orders.count,orders.revenue" in sql
    assert "dimensions=orders.created_at,orders.status" in sql
    assert "granularities=day" in sql


def test_extract_pattern_from_query():
    """Test extracting query pattern from instrumented query."""
    recommender = PreAggregationRecommender()

    query = """
    SELECT status, revenue
    FROM orders_cte
    -- sidemantic: models=orders metrics=orders.revenue dimensions=orders.status
    """

    pattern = recommender._extract_pattern(query)

    assert pattern is not None
    assert pattern.model == "orders"
    assert pattern.metrics == frozenset(["orders.revenue"])
    assert pattern.dimensions == frozenset(["orders.status"])
    assert pattern.granularities == frozenset()


def test_extract_pattern_with_granularity():
    """Test extracting pattern with time granularity."""
    recommender = PreAggregationRecommender()

    query = """
    SELECT created_at, revenue
    FROM orders_cte
    -- sidemantic: models=orders metrics=orders.revenue dimensions=orders.created_at granularities=day
    """

    pattern = recommender._extract_pattern(query)

    assert pattern is not None
    assert pattern.model == "orders"
    assert pattern.metrics == frozenset(["orders.revenue"])
    assert pattern.dimensions == frozenset(["orders.created_at"])
    assert pattern.granularities == frozenset(["day"])


def test_parse_query_log():
    """Test parsing multiple queries to build patterns."""
    recommender = PreAggregationRecommender(min_query_count=2)

    queries = [
        "SELECT revenue FROM orders -- sidemantic: models=orders metrics=orders.revenue dimensions=orders.status",
        "SELECT revenue FROM orders -- sidemantic: models=orders metrics=orders.revenue dimensions=orders.status",
        "SELECT revenue FROM orders -- sidemantic: models=orders metrics=orders.revenue dimensions=orders.status",
        "SELECT count FROM orders -- sidemantic: models=orders metrics=orders.count dimensions=orders.region",
        "SELECT count FROM orders -- sidemantic: models=orders metrics=orders.count dimensions=orders.region",
    ]

    recommender.parse_query_log(queries)

    # Should have 2 unique patterns
    assert len(recommender.patterns) == 2

    # Pattern 1: revenue + status (count=3)
    pattern1 = QueryPattern(
        model="orders",
        metrics=frozenset(["orders.revenue"]),
        dimensions=frozenset(["orders.status"]),
        granularities=frozenset(),
    )
    assert recommender.patterns[pattern1] == 3

    # Pattern 2: count + region (count=2)
    pattern2 = QueryPattern(
        model="orders",
        metrics=frozenset(["orders.count"]),
        dimensions=frozenset(["orders.region"]),
        granularities=frozenset(),
    )
    assert recommender.patterns[pattern2] == 2


def test_get_recommendations():
    """Test generating recommendations from patterns."""
    recommender = PreAggregationRecommender(min_query_count=2)

    # Simulate 100 queries for one pattern, 50 for another, 1 for a third
    for i in range(100):
        recommender.parse_query_log(
            [
                "SELECT revenue FROM orders -- sidemantic: models=orders metrics=orders.revenue dimensions=orders.status granularities=day"
            ]
        )

    for i in range(50):
        recommender.parse_query_log(
            ["SELECT count FROM orders -- sidemantic: models=orders metrics=orders.count dimensions=orders.region"]
        )

    for i in range(1):
        recommender.parse_query_log(
            [
                "SELECT revenue FROM orders -- sidemantic: models=orders metrics=orders.revenue dimensions=orders.customer_id"
            ]
        )

    recommendations = recommender.get_recommendations()

    # Should have 2 recommendations (third pattern below threshold)
    assert len(recommendations) == 2

    # First recommendation should be the one with 100 queries
    assert recommendations[0].query_count == 100
    assert recommendations[0].pattern.metrics == frozenset(["orders.revenue"])
    assert recommendations[0].pattern.dimensions == frozenset(["orders.status"])

    # Second recommendation should have 50 queries
    assert recommendations[1].query_count == 50


def test_recommendation_name_generation():
    """Test suggested name generation."""
    recommender = PreAggregationRecommender()

    # Pattern with granularity and single dimension
    pattern = QueryPattern(
        model="orders",
        metrics=frozenset(["orders.revenue"]),
        dimensions=frozenset(["orders.status"]),
        granularities=frozenset(["day"]),
    )

    name = recommender._generate_name(pattern)
    assert name == "day_status_revenue"

    # Pattern with multiple metrics
    pattern2 = QueryPattern(
        model="orders",
        metrics=frozenset(["orders.revenue", "orders.count"]),
        dimensions=frozenset(["orders.status"]),
        granularities=frozenset(),
    )

    name2 = recommender._generate_name(pattern2)
    assert name2 == "status_2metrics"


def test_generate_preagg_definition():
    """Test generating PreAggregation from recommendation."""
    recommender = PreAggregationRecommender(min_query_count=1)

    # Add pattern
    recommender.parse_query_log(
        [
            "SELECT revenue FROM orders -- sidemantic: models=orders metrics=orders.revenue dimensions=orders.created_at granularities=day"
        ]
    )

    recommendations = recommender.get_recommendations()
    assert len(recommendations) == 1

    # Generate pre-aggregation definition
    preagg = recommender.generate_preagg_definition(recommendations[0])

    assert preagg.name == "day_created_at_revenue"
    assert "revenue" in preagg.measures
    assert preagg.time_dimension == "created_at"
    assert preagg.granularity == "day"


def test_benefit_score_calculation():
    """Test benefit score calculation."""
    recommender = PreAggregationRecommender()

    # High query count, few dimensions = high score
    pattern1 = QueryPattern(
        model="orders",
        metrics=frozenset(["orders.revenue", "orders.count"]),
        dimensions=frozenset(["orders.status"]),
        granularities=frozenset(),
    )
    score1 = recommender._calculate_benefit_score(pattern1, count=1000)
    assert score1 > 0.5

    # Low query count, many dimensions = lower score
    pattern2 = QueryPattern(
        model="orders",
        metrics=frozenset(["orders.revenue"]),
        dimensions=frozenset(["orders.status", "orders.region", "orders.category", "orders.customer_id"]),
        granularities=frozenset(),
    )
    score2 = recommender._calculate_benefit_score(pattern2, count=10)
    assert score2 < score1


def test_get_summary():
    """Test getting summary statistics."""
    recommender = PreAggregationRecommender(min_query_count=2)

    queries = [
        "SELECT revenue FROM orders -- sidemantic: models=orders metrics=orders.revenue dimensions=orders.status",
        "SELECT revenue FROM orders -- sidemantic: models=orders metrics=orders.revenue dimensions=orders.status",
        "SELECT revenue FROM orders -- sidemantic: models=orders metrics=orders.revenue dimensions=orders.status",
        "SELECT count FROM customers -- sidemantic: models=customers metrics=customers.count dimensions=customers.region",
        "SELECT count FROM customers -- sidemantic: models=customers metrics=customers.count dimensions=customers.region",
    ]

    recommender.parse_query_log(queries)

    summary = recommender.get_summary()

    assert summary["total_queries"] == 5
    assert summary["unique_patterns"] == 2
    assert summary["models"]["orders"] == 3
    assert summary["models"]["customers"] == 2
    assert summary["patterns_above_threshold"] == 2


def test_multi_model_queries_ignored():
    """Test that multi-model queries are ignored (can't use pre-aggs)."""
    recommender = PreAggregationRecommender()

    query = """
    SELECT orders.revenue, customers.name
    FROM orders_cte
    -- sidemantic: models=customers,orders metrics=orders.revenue dimensions=customers.name
    """

    pattern = recommender._extract_pattern(query)

    # Multi-model queries should be ignored
    assert pattern is None


def test_end_to_end_with_semantic_layer():
    """Test end-to-end workflow with semantic layer."""
    sl = SemanticLayer()

    orders = Model(
        name="orders",
        table="public.orders",
        primary_key="order_id",
        dimensions=[
            Dimension(name="status", type="categorical", sql="status"),
            Dimension(name="created_at", type="time", sql="created_at", granularity="day"),
        ],
        metrics=[
            Metric(name="count", agg="count"),
            Metric(name="revenue", agg="sum", sql="amount"),
        ],
    )

    sl.add_model(orders)

    # Generate 100 similar queries
    queries = []
    for i in range(100):
        sql = sl.compile(
            metrics=["orders.revenue"],
            dimensions=["orders.status", "orders.created_at__day"],
        )
        queries.append(sql)

    # Analyze queries
    recommender = PreAggregationRecommender(min_query_count=10)
    recommender.parse_query_log(queries)

    # Get recommendations
    recommendations = recommender.get_recommendations(top_n=1)

    assert len(recommendations) == 1
    assert recommendations[0].query_count == 100

    # Generate pre-aggregation
    preagg = recommender.generate_preagg_definition(recommendations[0])

    assert preagg.type == "rollup"
    assert "revenue" in preagg.measures
    assert preagg.granularity == "day"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
