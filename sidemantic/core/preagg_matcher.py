"""Pre-aggregation query matching logic."""

from __future__ import annotations

from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.pre_aggregation import PreAggregation
from sidemantic.core.query_plan import PreaggCandidate, PreaggCheck

# Time granularity hierarchy (coarser to finer)
GRANULARITY_HIERARCHY = {
    "year": 1,
    "quarter": 2,
    "month": 3,
    "week": 4,
    "day": 5,
    "hour": 6,
    "minute": 7,
    "second": 8,
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
        # original_sql pre-aggregations stage a base query, not an aggregation
        # rollup, so they never directly satisfy a metric query.
        if preagg.type == "original_sql":
            return False

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

            if metric.agg in ("count_distinct", "approx_count_distinct"):
                if metric.name not in (preagg.measures or []):
                    return False
                if not self._is_exact_grain(preagg, query_dimensions, query_granularity):
                    return False
                continue

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
            available_columns = self._available_filter_columns(preagg)

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
            try:
                import sqlglot
                from sqlglot import exp

                parsed = sqlglot.parse_one(filter_expr)
                parsed_columns = set()
                for column in parsed.find_all(exp.Column):
                    if column.find_ancestor(exp.Select):
                        continue
                    parsed_columns.add(column.name)
                if parsed_columns:
                    columns.update(parsed_columns)
                    continue
            except Exception:
                pass

            # Fallback for malformed or dialect-specific predicates.
            matches = re.findall(r"(?:(\w+)\.)?(\w+)\s*(?:=|<>|!=|<=|>=|<|>|IN|BETWEEN|LIKE|IS)\b", filter_expr, re.I)
            for _table_name, column_name in matches:
                columns.add(column_name)

        return columns

    def _available_filter_columns(self, preagg: PreAggregation) -> set[str]:
        available_columns = set(preagg.dimensions or [])
        if preagg.time_dimension:
            available_columns.add(preagg.time_dimension)
            if preagg.granularity:
                available_columns.add(f"{preagg.time_dimension}__{preagg.granularity}")
        return available_columns

    def _is_measure_derivable(self, query_metric: Metric, preagg: PreAggregation) -> bool:
        """Check if a metric can be derived from pre-aggregation measures.

        Args:
            query_metric: Metric requested in query
            preagg: Pre-aggregation to check

        Returns:
            True if metric can be derived from pre-agg measures
        """
        preagg_measures = preagg.measures or []

        # Complete-expression measures (e.g. number_agg / PERCENTILE) carry a full aggregate
        # in sql that cannot be re-derived from a rollup's stored columns, so they are never
        # rollup-derivable (the materializer likewise does not store them).
        if getattr(query_metric, "sql_is_complete", False):
            return False

        # Complex metrics can be rebuilt from additive leaf state even when
        # the derived metric itself is not materialized.
        if query_metric.type in {"ratio", "derived"} or (
            not query_metric.type and not query_metric.agg and query_metric.sql
        ):
            return self._complex_metric_derivable(query_metric, preagg)

        # Check if metric is in the pre-agg measures list
        if query_metric.name not in preagg_measures:
            return False

        # Additional checks based on aggregation type
        agg_type = query_metric.agg

        if not agg_type:
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

        if agg_type in ("count_distinct", "approx_count_distinct"):
            # COUNT DISTINCT is NOT derivable from re-aggregated pre-agg rows
            # (would need HyperLogLog or storing exact values). approx_count_distinct
            # would be additive if we stored HLL sketches, but sidemantic materializes a
            # plain integer, so it is not safely rollup-derivable either.
            return False

        # Default: allow if present
        return True

    def _complex_metric_derivable(self, query_metric: Metric, preagg: PreAggregation) -> bool:
        preagg_measures = set(preagg.measures or [])
        dependencies = query_metric.get_dependencies(model_context=self.model.name)
        if not dependencies:
            return query_metric.name in preagg_measures

        for dependency in dependencies:
            dep_name = dependency.split(".", 1)[1] if "." in dependency else dependency
            dep_metric = self.model.get_metric(dep_name)
            if not dep_metric:
                return False
            if dep_metric.agg not in {"sum", "count"}:
                return False
            if dep_name not in preagg_measures:
                return False
        return True

    def _find_count_measure_for_avg(self, avg_metric: Metric, preagg_measures: list[str]) -> str | None:
        """Find the appropriate count measure for an AVG metric.

        Args:
            avg_metric: The AVG metric
            preagg_measures: Available measures in pre-aggregation

        Returns:
            Name of the count measure, or None if not found
        """
        candidates = []
        if avg_metric.name.startswith("avg_"):
            candidates.append(f"count_{avg_metric.name[4:]}")
        if "_avg" in avg_metric.name:
            candidates.append(avg_metric.name.replace("_avg", "_count"))
        candidates.extend(["count", *preagg_measures])
        for name in dict.fromkeys(candidates):
            if name not in preagg_measures:
                continue
            count_metric = self.model.get_metric(name)
            if count_metric and self._count_matches_average_population(avg_metric, count_metric):
                return name
        return None

    def _count_matches_average_population(self, average: Metric, count: Metric) -> bool:
        """An AVG denominator must count the same qualifying non-null inputs."""
        import sqlglot
        from sqlglot import exp
        from sqlglot.errors import SqlglotError

        from sidemantic.sql.fragment import replace_outside_sql_protected

        if count.agg != "count" or count.type not in (None, "simple") or count.sql_is_complete:
            return False

        def normalize(sql):
            sql = replace_outside_sql_protected(sql, "{model}", self.model.name)
            expression = sqlglot.parse_one(sql, read="duckdb")
            for column in expression.find_all(exp.Column):
                if len(column.parts) > 2 or column.table not in ("", self.model.name):
                    return None
                column.set("table", None)
            return expression

        def same_expression(left, right):
            if left.strip() == right.strip():
                return True
            try:
                normalized = normalize(left)
                return normalized is not None and normalized == normalize(right)
            except SqlglotError:
                return False

        if len(average.filters or []) != len(count.filters or []) or not all(
            same_expression(left, right) for left, right in zip(average.filters or [], count.filters or [], strict=True)
        ):
            return False
        count_input = count.sql or "*"
        if same_expression(average.sql_expr, count_input):
            return True

        def nonnull(expression):
            if isinstance(expression, (exp.Literal, exp.Boolean)):
                return True
            if isinstance(expression, (exp.Paren, exp.Neg)):
                return nonnull(expression.this)
            if isinstance(expression, (exp.Add, exp.Sub, exp.Mul)):
                return nonnull(expression.this) and nonnull(expression.expression)
            if isinstance(expression, exp.Coalesce):
                return any(nonnull(value) for value in [expression.this, *expression.expressions])
            if isinstance(expression, exp.Case):
                default = expression.args.get("default")
                return (
                    default is not None
                    and nonnull(default)
                    and all(nonnull(branch.args["true"]) for branch in expression.args.get("ifs", []))
                )
            return False

        try:
            if not nonnull(normalize(average.sql_expr)):
                return False
            return count_input.strip() == "*" or nonnull(normalize(count_input))
        except SqlglotError:
            return False

    def _is_exact_grain(
        self,
        preagg: PreAggregation,
        query_dimensions: list[str],
        query_granularity: str | None,
    ) -> bool:
        preagg_dims = set(preagg.dimensions or [])
        query_dims = set(query_dimensions)
        if preagg.time_dimension:
            query_dims.discard(preagg.time_dimension)
        if query_dims != preagg_dims:
            return False
        if preagg.time_dimension or query_granularity:
            return bool(preagg.time_dimension) and query_granularity == preagg.granularity
        return True

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
        elif not query_granularity and preagg.granularity:
            # A total query can be answered from a time rollup, but an
            # otherwise-equivalent total rollup is smaller and should win.
            score -= 20

        return score

    def explain_query(
        self,
        preagg: PreAggregation,
        query_metrics: list[str],
        query_dimensions: list[str],
        query_granularity: str | None = None,
        filters: list[str] | None = None,
    ) -> PreaggCandidate:
        """Evaluate a pre-aggregation with detailed check results.

        Same logic as can_satisfy_query but returns structured check details
        instead of a boolean.
        """
        # original_sql pre-aggregations are staged base tables, not queryable rollups.
        if preagg.type == "original_sql":
            return PreaggCandidate(
                name=preagg.name,
                matched=False,
                score=None,
                selected=False,
                checks=[PreaggCheck("type", False, "original_sql is a staged base table, not a queryable rollup")],
            )

        checks: list[PreaggCheck] = []

        # 1. Dimension subset check
        preagg_dims = set(preagg.dimensions or [])
        query_dims = set(query_dimensions)
        if preagg.time_dimension:
            query_dims.discard(preagg.time_dimension)

        dims_ok = query_dims.issubset(preagg_dims)
        if dims_ok:
            if query_dims:
                checks.append(
                    PreaggCheck(
                        "dimensions",
                        True,
                        f"query {{{', '.join(sorted(query_dims))}}} subset of preagg {{{', '.join(sorted(preagg_dims))}}}",
                    )
                )
            else:
                checks.append(PreaggCheck("dimensions", True, "no non-time dimensions required"))
        else:
            missing = query_dims - preagg_dims
            checks.append(
                PreaggCheck(
                    "dimensions",
                    False,
                    f"query needs {{{', '.join(sorted(missing))}}} not in preagg {{{', '.join(sorted(preagg_dims))}}}",
                )
            )

        # 2. Measure derivability check
        measures_ok = True
        measure_details = []
        for metric_name in query_metrics:
            metric = self.model.get_metric(metric_name)
            if not metric:
                measure_details.append(f"{metric_name} (not found)")
                measures_ok = False
            elif metric.agg in ("count_distinct", "approx_count_distinct") and metric.name in (preagg.measures or []):
                if self._is_exact_grain(preagg, query_dimensions, query_granularity):
                    measure_details.append(f"{metric_name} ({metric.agg} exact grain)")
                else:
                    measure_details.append(f"{metric_name} ({metric.agg}_not_rollup_safe)")
                    measures_ok = False
            elif not self._is_measure_derivable(metric, preagg):
                agg = metric.agg or "complex"
                if agg in ("count_distinct", "approx_count_distinct"):
                    measure_details.append(f"{metric_name} ({agg}_not_rollup_safe)")
                elif agg == "avg":
                    measure_details.append(f"{metric_name} (avg needs companion count measure)")
                elif metric.name not in (preagg.measures or []):
                    measure_details.append(f"{metric_name} (not in preagg measures)")
                else:
                    measure_details.append(f"{metric_name} (not derivable)")
                measures_ok = False
            else:
                agg = metric.agg or "complex"
                measure_details.append(f"{metric_name} ({agg})")

        if query_metrics:
            checks.append(
                PreaggCheck(
                    "measures",
                    measures_ok,
                    f"{', '.join(measure_details)}{'' if measures_ok else ''}",
                )
            )
        else:
            checks.append(PreaggCheck("measures", True, "no metrics required"))

        # 3. Granularity compatibility check
        if query_granularity and preagg.granularity:
            gran_ok = self._is_granularity_compatible(query_granularity, preagg.granularity)
            if gran_ok:
                if query_granularity == preagg.granularity:
                    detail = f"{query_granularity} (exact match)"
                else:
                    detail = f"query {query_granularity} rollable from preagg {preagg.granularity}"
            else:
                detail = f"query {query_granularity} cannot roll up from preagg {preagg.granularity}"
            checks.append(PreaggCheck("granularity", gran_ok, detail))
        else:
            checks.append(PreaggCheck("granularity", True, "not constrained"))

        # 4. Filter compatibility check
        filters = filters or []
        if filters:
            filter_columns = self._extract_filter_columns(filters)
            available_columns = self._available_filter_columns(preagg)

            missing_cols = {col for col in filter_columns if col not in available_columns}
            filters_ok = len(missing_cols) == 0
            if filters_ok:
                checks.append(
                    PreaggCheck(
                        "filters",
                        True,
                        f"filter columns {{{', '.join(sorted(filter_columns))}}} all available",
                    )
                )
            else:
                checks.append(
                    PreaggCheck(
                        "filters",
                        False,
                        f"filter columns {{{', '.join(sorted(missing_cols))}}} not in preagg",
                    )
                )
        else:
            checks.append(PreaggCheck("filters", True, "none"))

        matched = all(c.passed for c in checks)
        score = self._score_match(preagg, query_dimensions, query_granularity) if matched else None

        return PreaggCandidate(
            name=preagg.name,
            matched=matched,
            score=score,
            selected=False,
            checks=checks,
        )

    def explain_matching(
        self,
        metrics: list[str] | None = None,
        dimensions: list[str] | None = None,
        time_granularity: str | None = None,
        filters: list[str] | None = None,
    ) -> list[PreaggCandidate]:
        """Evaluate all pre-aggregation candidates with detailed explanations.

        Returns a list of PreaggCandidate, one per pre-aggregation defined on the
        model, with the best match marked as selected.
        """
        metrics = metrics or []
        dimensions = dimensions or []
        filters = filters or []

        candidates = []
        for preagg in self.model.pre_aggregations:
            candidate = self.explain_query(
                preagg=preagg,
                query_metrics=metrics,
                query_dimensions=dimensions,
                query_granularity=time_granularity,
                filters=filters,
            )
            candidates.append(candidate)

        # Mark the best match as selected
        matched = [(c, c.score) for c in candidates if c.matched and c.score is not None]
        if matched:
            matched.sort(key=lambda x: x[1], reverse=True)
            matched[0][0].selected = True

        return candidates
