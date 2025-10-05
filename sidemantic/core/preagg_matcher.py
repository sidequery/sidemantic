"""Pre-aggregation query matching logic."""

from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.pre_aggregation import PreAggregation

# Time granularity hierarchy (coarser to finer)
GRANULARITY_HIERARCHY = {
    "year": 1,
    "quarter": 2,
    "month": 3,
    "week": 4,
    "day": 5,
    "hour": 6,
}


class PreAggregationMatcher:
    """Matches queries to pre-aggregations for automatic optimization.

    The matcher implements the core query routing logic:
    1. Check if query dimensions are subset of pre-agg dimensions
    2. Check if query measures are derivable from pre-agg measures
    3. Check if query time granularity is compatible with pre-agg granularity
    4. Select the smallest/most specific matching pre-aggregation
    """

    def __init__(self, model: Model):
        """Initialize matcher with a model.

        Args:
            model: The model containing pre-aggregations
        """
        self.model = model

    def find_matching_preagg(
        self,
        metrics: list[str] | None = None,
        dimensions: list[str] | None = None,
        time_granularity: str | None = None,
        filters: list[str] | None = None,
    ) -> PreAggregation | None:
        """Find the best matching pre-aggregation for a query.

        Args:
            metrics: List of metric names requested
            dimensions: List of dimension names requested (without model prefix)
            time_granularity: Time granularity requested (e.g., 'day', 'month')
            filters: List of filter expressions (optional, for compatibility checking)

        Returns:
            Best matching PreAggregation, or None if no match found

        Example:
            >>> matcher = PreAggregationMatcher(orders_model)
            >>> preagg = matcher.find_matching_preagg(
            ...     metrics=['revenue', 'count'],
            ...     dimensions=['status', 'region'],
            ...     time_granularity='day'
            ... )
        """
        metrics = metrics or []
        dimensions = dimensions or []
        filters = filters or []

        candidates = []

        for preagg in self.model.pre_aggregations:
            if self.can_satisfy_query(
                preagg=preagg,
                query_metrics=metrics,
                query_dimensions=dimensions,
                query_granularity=time_granularity,
                filters=filters,
            ):
                # Score based on specificity (prefer smaller, more specific rollups)
                score = self._score_match(preagg, dimensions, time_granularity)
                candidates.append((preagg, score))

        if not candidates:
            return None

        # Return highest scoring match
        candidates.sort(key=lambda x: x[1], reverse=True)
        return candidates[0][0]

    def can_satisfy_query(
        self,
        preagg: PreAggregation,
        query_metrics: list[str],
        query_dimensions: list[str],
        query_granularity: str | None = None,
        filters: list[str] | None = None,
    ) -> bool:
        """Check if a pre-aggregation can satisfy a query.

        Args:
            preagg: Pre-aggregation to check
            query_metrics: Metrics requested in query
            query_dimensions: Dimensions requested in query
            query_granularity: Time granularity requested (e.g., 'day', 'month')
            filters: Filter expressions in query (optional)

        Returns:
            True if pre-aggregation can satisfy the query
        """
        # 1. Check dimension subset
        # Query dimensions must be subset of pre-agg dimensions
        preagg_dims = set(preagg.dimensions or [])
        query_dims = set(query_dimensions)

        # Remove time dimension from query dims if present (handled separately)
        if preagg.time_dimension:
            query_dims.discard(preagg.time_dimension)

        if not query_dims.issubset(preagg_dims):
            return False

        # 2. Check measure compatibility
        # All query measures must be derivable from pre-agg measures
        for metric_name in query_metrics:
            metric = self.model.get_metric(metric_name)
            if not metric:
                return False

            if not self._is_measure_derivable(metric, preagg):
                return False

        # 3. Check time granularity compatibility
        # Query granularity must be >= pre-agg granularity
        # (can roll up from day→month, but not month→day)
        if query_granularity and preagg.granularity:
            if not self._is_granularity_compatible(query_granularity, preagg.granularity):
                return False

        # 4. Check filter compatibility
        # All filter columns must be available in the pre-agg
        if filters:
            filter_columns = self._extract_filter_columns(filters)
            available_columns = set(preagg.dimensions or [])
            if preagg.time_dimension:
                available_columns.add(preagg.time_dimension)

            for col in filter_columns:
                if col not in available_columns:
                    return False

        return True

    def _extract_filter_columns(self, filters: list[str]) -> set[str]:
        """Extract column names referenced in filter expressions.

        Args:
            filters: List of filter expressions (e.g., ["status = 'completed'", "created_at >= '2024-01-01'"])

        Returns:
            Set of column names (without model prefix)
        """
        import re

        columns = set()
        for filter_expr in filters:
            # Remove model prefix if present (e.g., "orders.status" -> "status")
            # Simple regex to extract column names before operators
            # This handles: column = value, column >= value, etc.
            matches = re.findall(r"(\w+\.)?(\w+)\s*[=<>!]", filter_expr)
            for match in matches:
                # match[0] is model prefix (optional), match[1] is column name
                columns.add(match[1])

        return columns

    def _is_measure_derivable(self, query_metric: Metric, preagg: PreAggregation) -> bool:
        """Check if a metric can be derived from pre-aggregation measures.

        Args:
            query_metric: Metric requested in query
            preagg: Pre-aggregation to check

        Returns:
            True if metric can be derived from pre-agg measures
        """
        preagg_measures = preagg.measures or []

        # Check if metric is in the pre-agg measures list
        if query_metric.name not in preagg_measures:
            return False

        # Additional checks based on aggregation type
        agg_type = query_metric.agg

        if not agg_type:
            # Complex metric types (ratio, derived, etc.)
            # For now, conservatively require all component metrics to be present
            # TODO: More sophisticated logic for derived metrics
            return True

        # Simple aggregations
        if agg_type in ["sum", "count", "min", "max"]:
            # These are directly derivable if present
            return True

        if agg_type == "avg":
            # AVG is derivable if we have the sum measure AND a compatible count
            # We can compute AVG by re-aggregating: SUM(sum_raw) / SUM(count_raw)
            # Need to find the specific count measure available
            count_measure = self._find_count_measure_for_avg(query_metric, preagg_measures)
            return count_measure is not None

        if agg_type == "count_distinct":
            # COUNT DISTINCT is NOT derivable from pre-aggregated data
            # (would need HyperLogLog or storing exact values)
            return False

        # Default: allow if present
        return True

    def _find_count_measure_for_avg(self, avg_metric: Metric, preagg_measures: list[str]) -> str | None:
        """Find the appropriate count measure for an AVG metric.

        Args:
            avg_metric: The AVG metric
            preagg_measures: Available measures in pre-aggregation

        Returns:
            Name of the count measure, or None if not found
        """
        # Look for count measures in priority order:
        # 1. Exact match: if avg is "avg_amount", look for "count_amount"
        # 2. Exact match: if avg is "revenue_avg", look for "revenue_count"
        # 3. Generic "count" measure
        # 4. Any count measure (order_count, user_count, etc.)

        # Try exact match with _count suffix (e.g., avg_amount -> count_amount)
        if avg_metric.name.startswith("avg_"):
            base_name = avg_metric.name[4:]  # Remove "avg_" prefix
            count_candidate = f"count_{base_name}"
            if count_candidate in preagg_measures:
                return count_candidate

        # Try exact match with count_ prefix (e.g., revenue_avg -> revenue_count)
        if "_avg" in avg_metric.name:
            base_name = avg_metric.name.replace("_avg", "_count")
            if base_name in preagg_measures:
                return base_name

        # Try generic "count"
        if "count" in preagg_measures:
            return "count"

        # Accept any count measure as a fallback
        # This is safe for AVG re-aggregation as long as:
        # 1. The count matches the avg's population (unfiltered for unfiltered avg)
        # 2. We're re-aggregating at compatible grain
        for measure in preagg_measures:
            if measure.startswith("count") or "count" in measure:
                return measure

        return None

    def _is_granularity_compatible(
        self,
        query_granularity: str,
        preagg_granularity: str,
    ) -> bool:
        """Check if query granularity is compatible with pre-agg granularity.

        Query granularity must be >= pre-agg granularity (coarser or equal).
        Can roll up from day→month, but not month→day.

        IMPORTANT: Week cannot roll up to month because weeks span month boundaries,
        causing data to be misallocated.

        Args:
            query_granularity: Requested granularity (e.g., 'month')
            preagg_granularity: Pre-agg granularity (e.g., 'day')

        Returns:
            True if query can be satisfied by pre-agg
        """
        query_level = GRANULARITY_HIERARCHY.get(query_granularity)
        preagg_level = GRANULARITY_HIERARCHY.get(preagg_granularity)

        if query_level is None or preagg_level is None:
            # Unknown granularity, be conservative
            return query_granularity == preagg_granularity

        # SPECIAL CASE: Week cannot roll up to month or quarter or year
        # because weeks span month boundaries
        if preagg_granularity == "week" and query_granularity in ["month", "quarter", "year"]:
            return False

        # Query level must be coarser or equal to pre-agg level
        # (lower number = coarser, higher number = finer)
        return query_level <= preagg_level

    def _score_match(
        self,
        preagg: PreAggregation,
        query_dimensions: list[str],
        query_granularity: str | None,
    ) -> int:
        """Score a pre-aggregation match for selection.

        Higher scores are better. Prefers:
        - Exact dimension match over superset
        - Exact granularity match over coarser
        - Fewer total dimensions (smaller rollup)

        Args:
            preagg: Pre-aggregation to score
            query_dimensions: Dimensions in query
            query_granularity: Time granularity in query

        Returns:
            Score (higher is better)
        """
        score = 0

        preagg_dims = set(preagg.dimensions or [])
        query_dims = set(query_dimensions)

        # Remove time dimension from scoring
        if preagg.time_dimension:
            query_dims.discard(preagg.time_dimension)

        # Prefer exact dimension match
        if preagg_dims == query_dims:
            score += 1000

        # Prefer fewer extra dimensions (smaller rollup)
        extra_dims = len(preagg_dims - query_dims)
        score -= extra_dims * 10

        # Prefer exact granularity match
        if query_granularity and preagg.granularity:
            if query_granularity == preagg.granularity:
                score += 100
            else:
                # Penalize granularity mismatch
                query_level = GRANULARITY_HIERARCHY.get(query_granularity, 0)
                preagg_level = GRANULARITY_HIERARCHY.get(preagg.granularity, 0)
                score -= abs(query_level - preagg_level) * 5

        return score
