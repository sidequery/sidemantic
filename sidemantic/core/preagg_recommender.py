"""Pre-aggregation recommendation based on query analysis."""

import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from sidemantic.core.pre_aggregation import PreAggregation


@dataclass
class QueryPattern:
    """Represents a unique query pattern for pre-aggregation analysis."""

    model: str
    metrics: frozenset[str]
    dimensions: frozenset[str]
    granularities: frozenset[str]
    count: int = 0

    def __hash__(self):
        return hash((self.model, self.metrics, self.dimensions, self.granularities))

    def __eq__(self, other):
        if not isinstance(other, QueryPattern):
            return False
        return (
            self.model == other.model
            and self.metrics == other.metrics
            and self.dimensions == other.dimensions
            and self.granularities == other.granularities
        )


@dataclass
class PreAggRecommendation:
    """A recommended pre-aggregation with usage statistics."""

    pattern: QueryPattern
    suggested_name: str
    query_count: int
    estimated_benefit_score: float


class PreAggregationRecommender:
    """Analyzes query patterns to recommend pre-aggregations.

    Parses instrumented query comments to identify frequently used
    metric/dimension combinations and recommends pre-aggregations.
    """

    def __init__(self, min_query_count: int = 10, min_benefit_score: float = 0.0):
        """Initialize recommender.

        Args:
            min_query_count: Minimum queries for a pattern to be recommended
            min_benefit_score: Minimum benefit score (0-1) for recommendation
        """
        self.min_query_count = min_query_count
        self.min_benefit_score = min_benefit_score
        self.patterns: dict[QueryPattern, int] = defaultdict(int)

    def parse_query_log(self, queries: list[str]) -> None:
        """Parse instrumented queries and extract patterns.

        Args:
            queries: List of SQL queries with instrumentation comments
        """
        for query in queries:
            pattern = self._extract_pattern(query)
            if pattern:
                self.patterns[pattern] += 1

    def parse_query_log_file(self, file_path: str) -> None:
        """Parse queries from a file (one query per line or semicolon-separated).

        Args:
            file_path: Path to file containing queries
        """
        with open(file_path) as f:
            content = f.read()

        # Split by semicolon for multi-query files
        queries = [q.strip() for q in content.split(";") if q.strip()]

        self.parse_query_log(queries)

    def fetch_and_parse_query_history(self, connection: Any, days_back: int = 7, limit: int = 1000) -> None:
        """Fetch query history from database and parse for pre-aggregation patterns.

        Args:
            connection: Database adapter with get_query_history() method
            days_back: Number of days of history to fetch (default: 7)
            limit: Maximum number of queries to return (default: 1000)

        Raises:
            AttributeError: If adapter doesn't support get_query_history()
        """
        if not hasattr(connection, "get_query_history"):
            raise AttributeError(
                f"Database adapter {type(connection).__name__} does not support get_query_history(). "
                "Supported adapters: BigQueryAdapter, SnowflakeAdapter, DatabricksAdapter, ClickHouseAdapter"
            )

        queries = connection.get_query_history(days_back=days_back, limit=limit)
        self.parse_query_log(queries)

    def _extract_pattern(self, query: str) -> QueryPattern | None:
        """Extract query pattern from instrumented query.

        Args:
            query: SQL query with instrumentation comment

        Returns:
            QueryPattern if found, None otherwise
        """
        # Look for instrumentation comment: -- sidemantic: models=... metrics=... dimensions=...
        match = re.search(r"--\s*sidemantic:\s*(.+)", query)
        if not match:
            return None

        metadata = match.group(1)

        # Parse metadata
        parts = {}
        for part in metadata.split():
            if "=" in part:
                key, value = part.split("=", 1)
                parts[key] = value

        # Extract components
        models = parts.get("models", "").split(",") if parts.get("models") else []
        metrics = parts.get("metrics", "").split(",") if parts.get("metrics") else []
        dimensions = parts.get("dimensions", "").split(",") if parts.get("dimensions") else []
        granularities = parts.get("granularities", "").split(",") if parts.get("granularities") else []

        # Filter empty strings
        models = [m for m in models if m]
        metrics = [m for m in metrics if m]
        dimensions = [d for d in dimensions if d]
        granularities = [g for g in granularities if g]

        # Only track single-model queries (multi-model queries can't use pre-aggs currently)
        if len(models) != 1:
            return None

        if not metrics:
            return None

        return QueryPattern(
            model=models[0],
            metrics=frozenset(metrics),
            dimensions=frozenset(dimensions),
            granularities=frozenset(granularities),
        )

    def get_recommendations(self, top_n: int | None = None) -> list[PreAggRecommendation]:
        """Get pre-aggregation recommendations based on query patterns.

        Args:
            top_n: Return only top N recommendations (by query count)

        Returns:
            List of recommendations sorted by estimated benefit
        """
        recommendations = []

        for pattern, count in self.patterns.items():
            # Skip patterns below threshold
            if count < self.min_query_count:
                continue

            # Calculate benefit score
            benefit_score = self._calculate_benefit_score(pattern, count)

            if benefit_score < self.min_benefit_score:
                continue

            # Generate suggested name
            suggested_name = self._generate_name(pattern)

            recommendations.append(
                PreAggRecommendation(
                    pattern=pattern,
                    suggested_name=suggested_name,
                    query_count=count,
                    estimated_benefit_score=benefit_score,
                )
            )

        # Sort by benefit score descending
        recommendations.sort(key=lambda r: r.estimated_benefit_score, reverse=True)

        if top_n:
            recommendations = recommendations[:top_n]

        return recommendations

    def _calculate_benefit_score(self, pattern: QueryPattern, count: int) -> float:
        """Calculate benefit score for a query pattern.

        Higher scores indicate better candidates for pre-aggregation.

        Factors:
        - Query count (more queries = higher score)
        - Dimension count (fewer dimensions = higher score, more reusable)
        - Metric count (more metrics = higher score, more consolidation)

        Args:
            pattern: Query pattern
            count: Number of times this pattern appeared

        Returns:
            Benefit score between 0 and 1
        """
        # Normalize query count (log scale to handle wide range)
        import math

        query_score = math.log10(count + 1) / 6.0  # Max at ~1M queries

        # Dimension score: fewer dimensions = more reusable
        # 0 dims = 1.0, 1 dim = 0.9, 2 dims = 0.8, etc
        dim_count = len(pattern.dimensions)
        dim_score = max(0.0, 1.0 - (dim_count * 0.1))

        # Metric score: more metrics = better consolidation
        # 1 metric = 0.5, 2 metrics = 0.75, 3+ metrics = 1.0
        metric_count = len(pattern.metrics)
        metric_score = min(1.0, 0.25 + (metric_count * 0.25))

        # Weighted average
        benefit_score = (query_score * 0.5) + (dim_score * 0.25) + (metric_score * 0.25)

        return min(1.0, benefit_score)

    def _generate_name(self, pattern: QueryPattern) -> str:
        """Generate a suggested pre-aggregation name.

        Args:
            pattern: Query pattern

        Returns:
            Suggested name string
        """
        parts = []

        # Add granularity if present
        if pattern.granularities:
            # Use the finest granularity for naming
            grans = sorted(
                pattern.granularities,
                key=lambda g: ["hour", "day", "week", "month", "quarter", "year"].index(g)
                if g in ["hour", "day", "week", "month", "quarter", "year"]
                else 99,
            )
            if grans:
                parts.append(grans[0])

        # Add primary dimension indicators
        if pattern.dimensions:
            dims = sorted(pattern.dimensions)
            if len(dims) <= 2:
                parts.extend([d.split(".")[-1] for d in dims])
            else:
                parts.append(f"{len(dims)}dims")

        # Add metric indicator
        if len(pattern.metrics) == 1:
            parts.append(list(pattern.metrics)[0].split(".")[-1])
        else:
            parts.append(f"{len(pattern.metrics)}metrics")

        # Combine into name
        if parts:
            return "_".join(parts)
        else:
            return "rollup"

    def generate_preagg_definition(self, recommendation: PreAggRecommendation) -> PreAggregation:
        """Generate a PreAggregation definition from a recommendation.

        Args:
            recommendation: Recommendation to convert

        Returns:
            PreAggregation definition
        """
        pattern = recommendation.pattern

        # Extract model name from metrics/dimensions
        # Strip model prefix from metrics and dimensions
        measures = [m.split(".")[-1] for m in pattern.metrics]
        dimensions = [d.split(".")[-1] for d in pattern.dimensions]

        # Determine time dimension and granularity
        time_dimension = None
        granularity = None

        if pattern.granularities:
            # Look for common time dimension names
            # This is heuristic - may need refinement
            time_candidates = ["created_at", "updated_at", "date", "timestamp", "time", "datetime"]
            for dim in dimensions:
                dim_name = dim.split(".")[-1]
                if any(tc in dim_name for tc in time_candidates):
                    time_dimension = dim_name
                    break

            if not time_dimension and dimensions:
                # Fallback: use first dimension
                time_dimension = dimensions[0].split(".")[-1]

            # Use finest granularity
            if time_dimension:
                grans = sorted(
                    pattern.granularities,
                    key=lambda g: ["hour", "day", "week", "month", "quarter", "year"].index(g)
                    if g in ["hour", "day", "week", "month", "quarter", "year"]
                    else 99,
                )
                if grans:
                    granularity = grans[0]

        return PreAggregation(
            name=recommendation.suggested_name,
            type="rollup",
            measures=measures,
            dimensions=dimensions if not time_dimension else [d for d in dimensions if d != time_dimension],
            time_dimension=time_dimension,
            granularity=granularity,
        )

    def get_summary(self) -> dict[str, Any]:
        """Get summary statistics about analyzed queries.

        Returns:
            Dictionary with summary statistics
        """
        total_queries = sum(self.patterns.values())
        unique_patterns = len(self.patterns)

        # Group by model
        model_counts = defaultdict(int)
        for pattern, count in self.patterns.items():
            model_counts[pattern.model] += count

        return {
            "total_queries": total_queries,
            "unique_patterns": unique_patterns,
            "models": dict(model_counts),
            "patterns_above_threshold": sum(1 for p, c in self.patterns.items() if c >= self.min_query_count),
        }
