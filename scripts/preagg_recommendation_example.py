#!/usr/bin/env python
"""Example: Analyzing query patterns to recommend pre-aggregations.

This script demonstrates how to:
1. Generate instrumented queries from the semantic layer
2. Parse query logs to identify patterns
3. Get pre-aggregation recommendations based on usage
"""

from sidemantic import Dimension, Metric, Model, PreAggregationRecommender, SemanticLayer

# 1. Set up semantic layer
sl = SemanticLayer()

orders = Model(
    name="orders",
    table="public.orders",
    primary_key="order_id",
    dimensions=[
        Dimension(name="status", type="categorical", sql="status"),
        Dimension(name="region", type="categorical", sql="region"),
        Dimension(name="created_at", type="time", sql="created_at", granularity="day"),
    ],
    metrics=[
        Metric(name="count", agg="count"),
        Metric(name="revenue", agg="sum", sql="amount"),
        Metric(name="avg_order_value", agg="avg", sql="amount"),
    ],
)

sl.add_model(orders)

# 2. Simulate production queries (in reality, these would come from query logs)
print("Generating sample queries...")
queries = []

# Common pattern: Daily revenue by status (100 queries)
for i in range(100):
    sql = sl.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.status", "orders.created_at__day"],
    )
    queries.append(sql)

# Another pattern: Regional revenue (50 queries)
for i in range(50):
    sql = sl.compile(
        metrics=["orders.revenue", "orders.count"],
        dimensions=["orders.region"],
    )
    queries.append(sql)

# Less common pattern: Revenue by status (5 queries - below threshold)
for i in range(5):
    sql = sl.compile(
        metrics=["orders.revenue"],
        dimensions=["orders.status"],
    )
    queries.append(sql)

print(f"Generated {len(queries)} queries")
print("\nExample instrumented query:")
print(queries[0])
print()

# 3. Analyze query patterns
print("Analyzing query patterns...")
recommender = PreAggregationRecommender(
    min_query_count=10,  # Require at least 10 occurrences
    min_benefit_score=0.0,  # No minimum benefit score
)

recommender.parse_query_log(queries)

# 4. Get summary statistics
summary = recommender.get_summary()
print("\nQuery Analysis Summary:")
print(f"  Total queries: {summary['total_queries']}")
print(f"  Unique patterns: {summary['unique_patterns']}")
print(f"  Patterns above threshold: {summary['patterns_above_threshold']}")
print(f"  Models: {summary['models']}")

# 5. Get recommendations
print("\nPre-Aggregation Recommendations:")
recommendations = recommender.get_recommendations(top_n=5)

for i, rec in enumerate(recommendations, 1):
    print(f"\n{i}. {rec.suggested_name}")
    print(f"   Query count: {rec.query_count}")
    print(f"   Benefit score: {rec.estimated_benefit_score:.3f}")
    print("   Pattern:")
    print(f"     - Model: {rec.pattern.model}")
    print(f"     - Metrics: {', '.join(sorted(rec.pattern.metrics))}")
    print(f"     - Dimensions: {', '.join(sorted(rec.pattern.dimensions))}")
    if rec.pattern.granularities:
        print(f"     - Granularities: {', '.join(sorted(rec.pattern.granularities))}")

    # Generate pre-aggregation definition
    preagg = recommender.generate_preagg_definition(rec)
    print("\n   PreAggregation definition:")
    print("   PreAggregation(")
    print(f"       name='{preagg.name}',")
    print(f"       measures={preagg.measures},")
    print(f"       dimensions={preagg.dimensions},")
    if preagg.time_dimension:
        print(f"       time_dimension='{preagg.time_dimension}',")
    if preagg.granularity:
        print(f"       granularity='{preagg.granularity}',")
    print("   )")

# 6. Example: Parsing from a query log file
print("\n" + "=" * 60)
print("Example: Parsing from a file")
print("=" * 60)

# Create a sample query log file
with open("/tmp/query_log.sql", "w") as f:
    f.write(";\n".join(queries))

# Parse from file
recommender2 = PreAggregationRecommender(min_query_count=10)
recommender2.parse_query_log_file("/tmp/query_log.sql")

print(f"\nParsed {recommender2.get_summary()['total_queries']} queries from file")
print(f"Found {len(recommender2.get_recommendations())} recommendations")

print("\nDone!")
