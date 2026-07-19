"""SQL query rewriter for semantic layer.

Parses user SQL and rewrites it to use the semantic layer.
"""

import os
import re
from dataclasses import dataclass
from typing import Any

import sqlglot
from sqlglot import exp
from sqlglot.tokens import TokenType

from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.rust_bridge import get_rust_module, graph_to_rust_yaml
from sidemantic.sql.aggregation_detection import sql_has_aggregate
from sidemantic.sql.generator import SQLGenerator
from sidemantic.sql.planner import CandidatePlan, RewriteExplanation, SemanticQueryPlan

_YARDSTICK_SYNTAX_HINT_RE = re.compile(r"\b(?:SEMANTIC|AGGREGATE|AT)\b|\{", re.IGNORECASE)


@dataclass
class _YardstickAggregateCall:
    placeholder: str
    argument_sql: str
    modifiers: list[str]
    include_visible_default: bool


@dataclass
class _WrappedSemanticSource:
    inner_select: exp.Select
    source_name: str
    source_kind: str


@dataclass
class _ProjectionAnalysis:
    metrics: list[str]
    dimensions: list[str]
    aliases: dict[str, str]
    visible_name_to_ref: dict[str, str]
    projected_refs: set[str]
    applied_rules: list[str]


@dataclass
class _AggregateBoundaryAnalysis:
    metrics: list[str]
    dimensions: list[str]
    aliases: dict[str, str]
    visible_name_to_ref: dict[str, str]
    projected_refs: set[str]
    row_filters: list[str]
    aggregate_filters: list[str]
    applied_rules: list[str]

    @property
    def pushed_filters(self) -> list[str]:
        return [*self.row_filters, *self.aggregate_filters]


@dataclass
class _FilterTranslation:
    row_filters: list[str]
    aggregate_filters: list[str]

    @property
    def filters(self) -> list[str]:
        return [*self.row_filters, *self.aggregate_filters]


@dataclass
class _WrappedOptimization:
    plan: SemanticQueryPlan
    pushed_filters: list[str]
    applied_rules: list[str]
    rejected_rules: dict[str, str]


@dataclass
class _SemanticIslandRewrite:
    name: str
    source_kind: str
    source_sql: str
    rewritten_sql: str
    plan: SemanticQueryPlan
    applied_rules: list[str]
    rejected_rules: dict[str, str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "source_kind": self.source_kind,
            "source_sql": self.source_sql,
            "rewritten_sql": self.rewritten_sql,
            "chosen_plan": self.plan.candidate_kind,
            "metrics": list(self.plan.metrics),
            "dimensions": list(self.plan.dimensions),
            "filters": list(self.plan.filters),
            "row_filters": list(self.plan.row_filters),
            "aggregate_filters": list(self.plan.aggregate_filters),
            "applied_rules": list(self.applied_rules),
            "rejected_rules": dict(self.rejected_rules),
            "preaggregation": dict(self.plan.eligibility.get("single_model_preaggregation", {})),
            "fanout": dict(self.plan.eligibility.get("fanout_preaggregation", {})),
        }


class QueryRewriter:
    """Rewrites user SQL queries to use the semantic layer."""

    def __init__(
        self,
        graph: SemanticGraph,
        dialect: str = "duckdb",
        use_preaggregations: bool = False,
        enforce_visibility: bool = False,
    ):
        """Initialize query rewriter.

        Args:
            graph: Semantic graph with models and metrics
            dialect: SQL dialect for parsing/generation
            use_preaggregations: Enable single-model pre-aggregation routing
            enforce_visibility: Reject semantic references to fields declared ``public: false``
        """
        self.graph = graph
        self.dialect = dialect
        self.use_preaggregations = use_preaggregations
        self.generator = SQLGenerator(graph, dialect=dialect, enforce_visibility=enforce_visibility)
        self.enforce_visibility = enforce_visibility
        self._dialect_instance = self.generator._dialect_instance
        self._rewrite_cache: dict[tuple[object, ...], str] = {}
        self._rewrite_cache_limit = 256
        self._use_rust_rewriter = os.getenv("SIDEMANTIC_RS_REWRITER", "0") == "1"
        self._rust_no_fallback = os.getenv("SIDEMANTIC_RS_NO_FALLBACK", "0") == "1"
        self._rust_module = None
        self._rust_models_yaml: str | None = None

        if self._use_rust_rewriter:
            try:
                self._rust_module = get_rust_module()
                self._rust_models_yaml = graph_to_rust_yaml(self.graph)
            except Exception:
                if self._rust_no_fallback:
                    raise

    def rewrite(self, sql: str, strict: bool = True, user_attributes: dict | None = None) -> str:
        """Rewrite user SQL to use semantic layer.

        Supports:
        - Direct semantic layer queries: SELECT revenue FROM orders
        - CTEs with semantic queries: WITH agg AS (SELECT revenue FROM orders) SELECT * FROM agg
        - Subqueries: SELECT * FROM (SELECT revenue FROM orders) WHERE revenue > 100

        Args:
            sql: User SQL query
            strict: If True, raise errors for invalid SQL or non-SELECT queries.
                   If False, pass through queries that can't be rewritten.
            user_attributes: Optional per-user security attributes threaded into the underlying
                SQL generation so access gates, deny-by-default, and row filters are evaluated
                against the caller.

        Returns:
            Rewritten SQL using semantic layer

        Raises:
            ValueError: If SQL cannot be rewritten (unsupported features, invalid references, etc.)
                       Only raised when strict=True
        """
        sql = sql.strip()
        # Expose the attributes to the internal generate() calls for this rewrite.
        self._rewrite_user_attributes = user_attributes
        # Never serve a per-user rewrite from the shared cache (another user's access decision
        # could be baked in); only cache the attribute-free case.
        use_cache = user_attributes is None
        cache_key = (getattr(self.graph, "_version", 0), self.dialect, self.use_preaggregations, strict, sql)
        cached = self._rewrite_cache.get(cache_key) if use_cache else None
        if cached is not None:
            return cached

        def cache_result(result: str) -> str:
            if not use_cache:
                return result
            if len(self._rewrite_cache) >= self._rewrite_cache_limit:
                self._rewrite_cache.pop(next(iter(self._rewrite_cache)))
            self._rewrite_cache[cache_key] = result
            return result

        if self._looks_like_yardstick_query(sql):
            try:
                return cache_result(self._rewrite_yardstick_query(sql, strict=strict))
            except Exception:
                if strict:
                    raise
                # Keep non-strict passthrough behavior when Yardstick rewrite cannot be applied safely.
                return cache_result(sql)

        # Handle multiple statements (some PostgreSQL clients send these).
        # Use sqlglot.parse() so semicolons inside string literals are not
        # mistaken for statement separators.
        if ";" in sql:
            try:
                statements = sqlglot.parse(sql, dialect=self.dialect)
            except Exception:
                if strict:
                    raise
                # In non-strict mode, pass through unparseable SQL
                return cache_result(sql)
            if len(statements) > 1:
                if strict:
                    raise ValueError("Multiple statements are not supported")
                # In non-strict mode, pass through
                return cache_result(sql)

        # Parse SQL
        try:
            parsed = sqlglot.parse_one(sql, dialect=self.dialect)
        except Exception as e:
            if strict:
                raise ValueError(f"Failed to parse SQL: {e}")
            # In non-strict mode, pass through unparseable SQL (e.g., SHOW, SET commands)
            return cache_result(sql)

        if not isinstance(parsed, (exp.Select, exp.SetOperation)):
            if strict:
                raise ValueError("Only SELECT queries are supported")
            # In non-strict mode, pass through non-SELECT queries
            return cache_result(sql)

        if isinstance(parsed, exp.SetOperation):
            if not self._expression_tree_references_semantic_model(parsed):
                return cache_result(sql)
            self._raise_on_user_cte_name_collision(parsed)
            return cache_result(self._rewrite_set_operation(parsed))

        if self._graph_has_yardstick_models() and self._contains_implicit_yardstick_measure_query(parsed):
            try:
                return cache_result(self._rewrite_yardstick_query(sql, strict=strict, allow_plain_measures=True))
            except Exception:
                if strict:
                    raise
                return cache_result(sql)

        # Projection-only SQL (no root FROM/CTE) should pass through unless Yardstick paths above matched.
        if parsed.args.get("from_") is None and parsed.args.get("with_") is None:
            if any(isinstance(expr, exp.Star) for expr in parsed.expressions):
                if strict:
                    raise ValueError("SELECT * requires a FROM clause with a single table")
                return cache_result(sql)
            return cache_result(sql)

        references_semantic_model = self._select_tree_references_semantic_model(parsed)
        if not references_semantic_model:
            return cache_result(sql)

        self._raise_on_user_cte_name_collision(parsed)

        if self._use_rust_rewriter:
            rust_sql = self._prepare_sql_for_rust(parsed, sql)
            rust_rewritten = self._rewrite_with_rust(rust_sql, strict=strict)
            if rust_rewritten is not None:
                return cache_result(rust_rewritten)

        # Check if this is a CTE-based query or has subqueries
        has_ctes = parsed.args.get("with_") is not None
        has_subquery_in_from = self._has_subquery_in_from(parsed)
        has_subquery_in_joins = any(isinstance(join.this, exp.Subquery) for join in (parsed.args.get("joins") or []))

        if has_ctes or has_subquery_in_from or has_subquery_in_joins:
            # Handle CTEs and subqueries
            return cache_result(self._rewrite_with_ctes_or_subqueries(parsed))

        # Otherwise, treat as simple semantic layer query
        return cache_result(self._rewrite_simple_query(parsed))

    def _source_aliases(self, select: exp.Select) -> dict[str, str]:
        """Map source aliases in this SELECT scope to semantic model names."""
        aliases: dict[str, str] = {}

        def add_table(table_expr: exp.Expression | None) -> None:
            if not isinstance(table_expr, exp.Table):
                return
            model_name = table_expr.name
            if model_name in self.graph.models or model_name == "metrics":
                aliases[table_expr.alias_or_name] = model_name
                aliases[model_name] = model_name

        from_clause = select.args.get("from_")
        if from_clause:
            add_table(from_clause.this)
        for join in select.args.get("joins") or []:
            add_table(join.this)

        return aliases

    def _normalize_source_aliases(self, expression: exp.Expression) -> exp.Expression:
        """Rewrite table aliases in column refs to semantic model names."""
        aliases = getattr(self, "table_aliases", {})
        if not aliases:
            return expression

        rewritten = expression.copy()
        for column in rewritten.find_all(exp.Column):
            if column.table in aliases:
                column.set("table", exp.to_identifier(aliases[column.table]))
        return rewritten

    def explain(self, sql: str, strict: bool = True) -> RewriteExplanation:
        """Explain how a SQL query would be rewritten by the semantic layer.

        The explanation follows the same routing as rewrite() but returns
        structured planner state and candidate decisions instead of only SQL.
        """
        sql = sql.strip()

        if self._looks_like_yardstick_query(sql):
            try:
                rewritten_sql = self._rewrite_yardstick_query(sql, strict=strict)
            except Exception as e:
                if strict:
                    raise
                return self._passthrough_explanation(
                    sql,
                    reason="yardstick_rewrite_failed",
                    warning=f"Yardstick rewrite failed: {e}",
                )

            return RewriteExplanation(
                input_sql=sql,
                rewritten_sql=rewritten_sql,
                chosen_plan="yardstick_semantic_sql",
                source_kind="yardstick",
                candidate_plans=[
                    CandidatePlan(
                        name="yardstick_semantic_sql",
                        valid=True,
                        reason="query uses Yardstick semantic SQL syntax",
                    ),
                    CandidatePlan(
                        name="direct_semantic",
                        valid=False,
                        reason="primary semantic SQL planner does not handle Yardstick syntax",
                    ),
                ],
                warnings=["Yardstick semantic SQL uses a separate rewrite path."],
            )

        if ";" in sql:
            try:
                statements = sqlglot.parse(sql, dialect=self.dialect)
            except Exception as e:
                if strict:
                    raise
                return self._passthrough_explanation(
                    sql,
                    reason="parse_failed",
                    warning=f"SQL parse failed: {e}",
                )
            if len(statements) > 1:
                if strict:
                    raise ValueError("Multiple statements are not supported")
                return self._passthrough_explanation(sql, reason="multiple_statements")

        try:
            parsed = sqlglot.parse_one(sql, dialect=self.dialect)
        except Exception as e:
            if strict:
                raise ValueError(f"Failed to parse SQL: {e}")
            return self._passthrough_explanation(
                sql,
                reason="parse_failed",
                warning=f"SQL parse failed: {e}",
            )

        if not isinstance(parsed, (exp.Select, exp.SetOperation)):
            if strict:
                raise ValueError("Only SELECT queries are supported")
            return self._passthrough_explanation(sql, reason="not_select")

        if isinstance(parsed, exp.SetOperation):
            if not self._expression_tree_references_semantic_model(parsed):
                return self._passthrough_explanation(sql, reason="no_semantic_model_reference")
            self._raise_on_user_cte_name_collision(parsed)
            return self._explain_set_operation(sql, parsed)

        if self._graph_has_yardstick_models() and self._contains_implicit_yardstick_measure_query(parsed):
            try:
                rewritten_sql = self._rewrite_yardstick_query(sql, strict=strict, allow_plain_measures=True)
            except Exception as e:
                if strict:
                    raise
                return self._passthrough_explanation(
                    sql,
                    reason="yardstick_rewrite_failed",
                    warning=f"Yardstick rewrite failed: {e}",
                )
            return RewriteExplanation(
                input_sql=sql,
                rewritten_sql=rewritten_sql,
                chosen_plan="yardstick_semantic_sql",
                source_kind="yardstick",
                candidate_plans=[
                    CandidatePlan(
                        name="yardstick_semantic_sql",
                        valid=True,
                        reason="query uses implicit Yardstick measure syntax",
                    )
                ],
                warnings=["Yardstick semantic SQL uses a separate rewrite path."],
            )

        if parsed.args.get("from_") is None and parsed.args.get("with_") is None:
            if any(isinstance(expr, exp.Star) for expr in parsed.expressions):
                if strict:
                    raise ValueError("SELECT * requires a FROM clause with a single table")
                return self._passthrough_explanation(sql, reason="select_star_without_from")
            return self._passthrough_explanation(sql, reason="projection_only")

        references_semantic_model = self._select_tree_references_semantic_model(parsed)
        if not references_semantic_model:
            return self._passthrough_explanation(sql, reason="no_semantic_model_reference")

        self._raise_on_user_cte_name_collision(parsed)

        if self._use_rust_rewriter:
            rust_sql = self._prepare_sql_for_rust(parsed, sql)
            rust_rewritten = self._rewrite_with_rust(rust_sql, strict=strict)
            if rust_rewritten is not None:
                return RewriteExplanation(
                    input_sql=sql,
                    rewritten_sql=rust_rewritten,
                    chosen_plan="rust_semantic_rewriter",
                    source_kind="rust",
                    candidate_plans=[
                        CandidatePlan(
                            name="rust_semantic_rewriter",
                            valid=True,
                            reason="SIDEMANTIC_RS_REWRITER is enabled",
                        )
                    ],
                    warnings=["Rust rewriter handled this query before the Python planner."],
                )

        has_ctes = parsed.args.get("with_") is not None
        has_subquery_in_from = self._has_subquery_in_from(parsed)
        has_subquery_in_joins = any(isinstance(join.this, exp.Subquery) for join in (parsed.args.get("joins") or []))

        if has_ctes or has_subquery_in_from or has_subquery_in_joins:
            return self._explain_ctes_or_subqueries(sql, parsed)

        plan = self._plan_simple_query(parsed)
        rewritten_sql = self._generate_from_plan(plan)
        return self._explanation_from_plan(sql, plan, rewritten_sql)

    def _passthrough_explanation(self, sql: str, reason: str, warning: str | None = None) -> RewriteExplanation:
        warnings = [warning] if warning else []
        return RewriteExplanation(
            input_sql=sql,
            rewritten_sql=sql,
            chosen_plan="passthrough_plain_sql",
            source_kind="plain_sql",
            candidate_plans=[
                CandidatePlan(
                    name="passthrough_plain_sql",
                    valid=True,
                    reason=reason,
                ),
                CandidatePlan(
                    name="direct_semantic",
                    valid=False,
                    reason="query does not reference a semantic model",
                ),
            ],
            warnings=warnings,
        )

    def _explain_ctes_or_subqueries(self, sql: str, parsed: exp.Select) -> RewriteExplanation:
        optimization, rejected_rules = self._optimize_wrapped_semantic_query(parsed)
        if optimization is not None:
            explanation = self._explanation_from_plan(
                sql, optimization.plan, self._generate_from_plan(optimization.plan)
            )
            explanation.pushed_filters = optimization.pushed_filters
            explanation.applied_rules = optimization.applied_rules
            explanation.rejected_rules = optimization.rejected_rules
            return explanation

        root_references_semantic_model = self._references_semantic_model(parsed)

        rewritten_sql, semantic_islands, island_rejected_rules, warnings = (
            self._rewrite_with_ctes_or_subqueries_and_islands(parsed.copy())
        )
        semantic_scopes = [island.plan for island in semantic_islands]
        if not semantic_scopes:
            semantic_scopes, scope_warnings = self._collect_semantic_scopes(parsed)
            warnings.extend(scope_warnings)
        rejected_rules = {**rejected_rules, **island_rejected_rules}

        if root_references_semantic_model and len(semantic_scopes) == 1:
            chosen_plan = semantic_scopes[0].candidate_kind
            source_kind = semantic_scopes[0].source_kind
        else:
            chosen_plan = "semantic_plus_postprocess"
            source_kind = "subquery"

        candidate_plans = self._candidate_plans_for_scopes(semantic_scopes, chosen_plan)
        return RewriteExplanation(
            input_sql=sql,
            rewritten_sql=rewritten_sql,
            chosen_plan=chosen_plan,
            source_kind=source_kind,
            metrics=self._dedupe([metric for scope in semantic_scopes for metric in scope.metrics]),
            dimensions=self._dedupe([dimension for scope in semantic_scopes for dimension in scope.dimensions]),
            filters=self._dedupe([filter_expr for scope in semantic_scopes for filter_expr in scope.filters]),
            row_filters=self._dedupe([filter_expr for scope in semantic_scopes for filter_expr in scope.row_filters]),
            aggregate_filters=self._dedupe(
                [filter_expr for scope in semantic_scopes for filter_expr in scope.aggregate_filters]
            ),
            candidate_plans=candidate_plans,
            semantic_scopes=semantic_scopes,
            semantic_islands=[island.to_dict() for island in semantic_islands],
            post_process=parsed.sql(dialect=self.dialect) if not root_references_semantic_model else None,
            applied_rules=self._dedupe(
                [
                    *(
                        ["set_operation_branch_optimization"]
                        if any(island.source_kind == "set_operation_branch" for island in semantic_islands)
                        else []
                    ),
                    *[rule for island in semantic_islands for rule in island.applied_rules],
                ]
            ),
            rejected_rules=rejected_rules,
            warnings=warnings,
        )

    def _explain_set_operation(self, sql: str, parsed: exp.SetOperation) -> RewriteExplanation:
        rewritten_sql, semantic_islands, rejected_rules, warnings = self._rewrite_set_operation_with_islands(
            parsed.copy()
        )
        semantic_scopes = [island.plan for island in semantic_islands]
        chosen_plan = "semantic_plus_postprocess" if semantic_scopes else "passthrough_plain_sql"
        candidate_plans = self._candidate_plans_for_scopes(semantic_scopes, chosen_plan)
        if semantic_scopes:
            candidate_plans.append(
                CandidatePlan(
                    name="set_operation_branch_optimization",
                    valid=True,
                    reason="semantic branches inside a set operation were optimized independently",
                    details={"island_count": len(semantic_islands)},
                )
            )

        return RewriteExplanation(
            input_sql=sql,
            rewritten_sql=rewritten_sql,
            chosen_plan=chosen_plan,
            source_kind="set_operation",
            metrics=self._dedupe([metric for scope in semantic_scopes for metric in scope.metrics]),
            dimensions=self._dedupe([dimension for scope in semantic_scopes for dimension in scope.dimensions]),
            filters=self._dedupe([filter_expr for scope in semantic_scopes for filter_expr in scope.filters]),
            row_filters=self._dedupe([filter_expr for scope in semantic_scopes for filter_expr in scope.row_filters]),
            aggregate_filters=self._dedupe(
                [filter_expr for scope in semantic_scopes for filter_expr in scope.aggregate_filters]
            ),
            candidate_plans=candidate_plans,
            semantic_scopes=semantic_scopes,
            semantic_islands=[island.to_dict() for island in semantic_islands],
            post_process=parsed.sql(dialect=self.dialect),
            applied_rules=self._dedupe(
                [
                    *(["set_operation_branch_optimization"] if semantic_scopes else []),
                    *[rule for island in semantic_islands for rule in island.applied_rules],
                ]
            ),
            rejected_rules=rejected_rules,
            warnings=warnings,
        )

    def _optimize_wrapped_semantic_query(
        self, parsed: exp.Select
    ) -> tuple[_WrappedOptimization | None, dict[str, str]]:
        rejected_rules: dict[str, str] = {}

        qualify_topn_optimization = self._optimize_qualify_row_number_topn(parsed, rejected_rules)
        if qualify_topn_optimization is not None:
            return qualify_topn_optimization, rejected_rules

        topn_optimization = self._optimize_global_row_number_topn(parsed, rejected_rules)
        if topn_optimization is not None:
            return topn_optimization, rejected_rules

        cte_chain_optimization = self._optimize_linear_cte_chain(parsed, rejected_rules)
        if cte_chain_optimization is not None:
            return cte_chain_optimization, rejected_rules

        source = self._wrapped_semantic_source(parsed, rejected_rules)
        if source is None:
            return None, rejected_rules

        try:
            plan = self._plan_simple_query(source.inner_select.copy())
        except Exception as e:
            rejected_rules["wrapped_semantic_optimizer"] = f"inner semantic query cannot be planned: {e}"
            return None, rejected_rules

        output_name_to_ref = self._plan_output_name_to_ref(plan)

        if self._outer_has_aggregate_boundary(parsed):
            same_grain = self._analyze_same_grain_aggregate_wrapper(
                parsed,
                source,
                plan,
                output_name_to_ref,
                rejected_rules,
            )
            if same_grain is not None:
                plan.metrics = same_grain.metrics
                plan.dimensions = same_grain.dimensions
                plan.aliases = same_grain.aliases

                applied_rules = ["wrapper_flattening", *same_grain.applied_rules]
                pushed_filters: list[str] = []

                filters = self._translated_outer_filters(parsed, source, plan, output_name_to_ref, rejected_rules)
                if filters is None:
                    return None, rejected_rules
                if filters.filters:
                    plan.filters = [*plan.filters, *filters.filters]
                    plan.row_filters = [*plan.row_filters, *filters.row_filters]
                    plan.aggregate_filters = [*plan.aggregate_filters, *filters.aggregate_filters]
                    pushed_filters = filters.filters
                    if filters.row_filters:
                        applied_rules.append("safe_filter_pushdown")
                    if filters.aggregate_filters:
                        applied_rules.append("safe_metric_filter_having_pushdown")

                order_by = self._translated_outer_order_by(
                    parsed,
                    source,
                    same_grain.visible_name_to_ref,
                    output_name_to_ref,
                    same_grain.projected_refs,
                    rejected_rules,
                )
                if order_by is None:
                    return None, rejected_rules
                if order_by:
                    plan.order_by = order_by
                    applied_rules.append("safe_order_pushdown")

                if not self._apply_outer_limit_offset(parsed, plan, rejected_rules):
                    return None, rejected_rules
                if parsed.args.get("limit") is not None or parsed.args.get("offset") is not None:
                    applied_rules.append("safe_limit_pushdown")

                plan.eligibility = self._plan_eligibility(plan)
                plan.candidate_kind = self._chosen_candidate_kind(plan)
                plan.candidate_plans = self._candidate_plans_for_plan(plan)
                plan.candidate_plans.append(
                    CandidatePlan(
                        name="same_grain_metric_wrapper",
                        valid=True,
                        reason="outer GROUP BY repeats the inner semantic output grain",
                    )
                )
                if plan.candidate_kind == "single_model_preaggregation":
                    applied_rules.append("preaggregation_route_selection")
                if plan.candidate_kind == "join_key_preaggregation":
                    applied_rules.append("join_key_preaggregation_route_selection")
                if plan.candidate_kind == "fanout_preaggregation":
                    applied_rules.append("fanout_strategy_selection")
                plan.applied_rules = self._dedupe(applied_rules)
                plan.rejected_rules = rejected_rules

                return (
                    _WrappedOptimization(
                        plan=plan,
                        pushed_filters=pushed_filters,
                        applied_rules=plan.applied_rules,
                        rejected_rules=rejected_rules,
                    ),
                    rejected_rules,
                )

            aggregate_boundary = self._analyze_wrapper_aggregate_boundary(
                parsed,
                source,
                plan,
                output_name_to_ref,
                rejected_rules,
            )
            if aggregate_boundary is None:
                return None, rejected_rules

            plan.metrics = aggregate_boundary.metrics
            plan.dimensions = aggregate_boundary.dimensions
            plan.aliases = aggregate_boundary.aliases
            if aggregate_boundary.pushed_filters:
                plan.filters = [*plan.filters, *aggregate_boundary.pushed_filters]
                plan.row_filters = [*plan.row_filters, *aggregate_boundary.row_filters]
                plan.aggregate_filters = [*plan.aggregate_filters, *aggregate_boundary.aggregate_filters]

            applied_rules = ["wrapper_flattening", *aggregate_boundary.applied_rules]

            order_by = self._translated_outer_order_by(
                parsed,
                source,
                aggregate_boundary.visible_name_to_ref,
                output_name_to_ref,
                aggregate_boundary.projected_refs,
                rejected_rules,
            )
            if order_by is None:
                return None, rejected_rules
            if order_by:
                plan.order_by = order_by
                applied_rules.append("safe_order_pushdown")

            if not self._apply_outer_limit_offset(parsed, plan, rejected_rules):
                return None, rejected_rules
            if parsed.args.get("limit") is not None or parsed.args.get("offset") is not None:
                applied_rules.append("safe_limit_pushdown")

            plan.eligibility = self._plan_eligibility(plan)
            plan.candidate_kind = self._chosen_candidate_kind(plan)
            plan.candidate_plans = self._candidate_plans_for_plan(plan)
            plan.candidate_plans.append(
                CandidatePlan(
                    name="aggregate_boundary_rollup",
                    valid=True,
                    reason="outer aggregate rolls up semantic metric outputs",
                )
            )
            if plan.candidate_kind == "single_model_preaggregation":
                applied_rules.append("preaggregation_route_selection")
            if plan.candidate_kind == "join_key_preaggregation":
                applied_rules.append("join_key_preaggregation_route_selection")
            if plan.candidate_kind == "fanout_preaggregation":
                applied_rules.append("fanout_strategy_selection")
            plan.applied_rules = self._dedupe(applied_rules)
            plan.rejected_rules = rejected_rules

            return (
                _WrappedOptimization(
                    plan=plan,
                    pushed_filters=aggregate_boundary.pushed_filters,
                    applied_rules=plan.applied_rules,
                    rejected_rules=rejected_rules,
                ),
                rejected_rules,
            )

        if self._wrapper_has_blocking_features(parsed, rejected_rules):
            distinct_optimization = self._optimize_dimension_distinct_wrapper(
                parsed,
                source,
                plan,
                output_name_to_ref,
                rejected_rules,
            )
            if distinct_optimization is not None:
                return distinct_optimization, rejected_rules
            return None, rejected_rules

        projection = self._analyze_wrapper_projection(parsed, source, plan, output_name_to_ref, rejected_rules)
        if projection is None:
            return None, rejected_rules

        plan.metrics = projection.metrics
        plan.dimensions = projection.dimensions
        plan.aliases = projection.aliases

        applied_rules = ["wrapper_flattening", *projection.applied_rules]
        pushed_filters: list[str] = []

        filters = self._translated_outer_filters(parsed, source, plan, output_name_to_ref, rejected_rules)
        if filters is None:
            return None, rejected_rules
        if filters.filters:
            plan.filters = [*plan.filters, *filters.filters]
            plan.row_filters = [*plan.row_filters, *filters.row_filters]
            plan.aggregate_filters = [*plan.aggregate_filters, *filters.aggregate_filters]
            pushed_filters = filters.filters
            if filters.row_filters:
                applied_rules.append("safe_filter_pushdown")
            if filters.aggregate_filters:
                applied_rules.append("safe_metric_filter_having_pushdown")

        order_by = self._translated_outer_order_by(
            parsed,
            source,
            projection.visible_name_to_ref,
            output_name_to_ref,
            projection.projected_refs,
            rejected_rules,
        )
        if order_by is None:
            return None, rejected_rules
        if order_by:
            plan.order_by = order_by
            applied_rules.append("safe_order_pushdown")

        if not self._apply_outer_limit_offset(parsed, plan, rejected_rules):
            return None, rejected_rules
        if parsed.args.get("limit") is not None or parsed.args.get("offset") is not None:
            applied_rules.append("safe_limit_pushdown")

        plan.eligibility = self._plan_eligibility(plan)
        plan.candidate_kind = self._chosen_candidate_kind(plan)
        plan.candidate_plans = self._candidate_plans_for_plan(plan)
        if plan.candidate_kind == "single_model_preaggregation":
            applied_rules.append("preaggregation_route_selection")
        if plan.candidate_kind == "join_key_preaggregation":
            applied_rules.append("join_key_preaggregation_route_selection")
        if plan.candidate_kind == "fanout_preaggregation":
            applied_rules.append("fanout_strategy_selection")
        plan.applied_rules = self._dedupe(applied_rules)
        plan.rejected_rules = rejected_rules

        return (
            _WrappedOptimization(
                plan=plan,
                pushed_filters=pushed_filters,
                applied_rules=plan.applied_rules,
                rejected_rules=rejected_rules,
            ),
            rejected_rules,
        )

    def _finalize_wrapped_optimization(
        self,
        plan: SemanticQueryPlan,
        applied_rules: list[str],
        pushed_filters: list[str],
        rejected_rules: dict[str, str],
        extra_candidate: CandidatePlan | None = None,
    ) -> _WrappedOptimization:
        plan.eligibility = self._plan_eligibility(plan)
        plan.candidate_kind = self._chosen_candidate_kind(plan)
        plan.candidate_plans = self._candidate_plans_for_plan(plan)
        if extra_candidate is not None:
            plan.candidate_plans.append(extra_candidate)
        if plan.candidate_kind == "single_model_preaggregation":
            applied_rules.append("preaggregation_route_selection")
        if plan.candidate_kind == "join_key_preaggregation":
            applied_rules.append("join_key_preaggregation_route_selection")
        if plan.candidate_kind == "fanout_preaggregation":
            applied_rules.append("fanout_strategy_selection")
        plan.applied_rules = self._dedupe(applied_rules)
        plan.rejected_rules = rejected_rules
        return _WrappedOptimization(
            plan=plan,
            pushed_filters=pushed_filters,
            applied_rules=plan.applied_rules,
            rejected_rules=rejected_rules,
        )

    def _optimize_linear_cte_chain(
        self,
        parsed: exp.Select,
        rejected_rules: dict[str, str],
    ) -> _WrappedOptimization | None:
        with_clause = parsed.args.get("with_")
        if not with_clause or len(with_clause.expressions) < 2:
            return None

        from_clause = parsed.args.get("from_")
        if not from_clause or not isinstance(from_clause.this, exp.Table):
            rejected_rules["linear_cte_chain_flattening"] = "root query does not read from a CTE"
            return None

        ctes_by_name = {cte.alias: cte for cte in with_clause.expressions}
        if len(ctes_by_name) != len(with_clause.expressions):
            rejected_rules["linear_cte_chain_flattening"] = "duplicate CTE aliases are not supported"
            return None

        root_name = from_clause.this.name
        if root_name not in ctes_by_name:
            return None

        cte_names = set(ctes_by_name)
        reference_counts = {name: 0 for name in cte_names}
        for table in parsed.find_all(exp.Table):
            if table.name in reference_counts:
                reference_counts[table.name] += 1
        multi_refs = [name for name, count in reference_counts.items() if count > 1]
        if multi_refs:
            rejected_rules["linear_cte_chain_flattening"] = "CTE referenced more than once"
            return None

        chain_names_reversed: list[str] = []
        current_name = root_name
        base_cte = None
        while current_name in ctes_by_name:
            cte = ctes_by_name[current_name]
            cte_select = cte.this
            if not isinstance(cte_select, exp.Select):
                rejected_rules["linear_cte_chain_flattening"] = "chain CTE is not a SELECT"
                return None
            chain_names_reversed.append(current_name)
            if self._references_semantic_model(cte_select):
                base_cte = cte
                break
            cte_from = cte_select.args.get("from_")
            if (
                not cte_from
                or not isinstance(cte_from.this, exp.Table)
                or cte_from.this.name not in ctes_by_name
                or cte_select.args.get("joins")
            ):
                rejected_rules["linear_cte_chain_flattening"] = "CTE chain is not linear"
                return None
            current_name = cte_from.this.name

        if base_cte is None:
            rejected_rules["linear_cte_chain_flattening"] = "chain does not start from a semantic CTE"
            return None

        chain_names = list(reversed(chain_names_reversed))
        if set(chain_names) != cte_names:
            rejected_rules["linear_cte_chain_flattening"] = "WITH contains CTEs outside the linear chain"
            return None

        try:
            plan = self._plan_simple_query(base_cte.this.copy())
        except Exception as e:
            rejected_rules["linear_cte_chain_flattening"] = f"base semantic CTE cannot be planned: {e}"
            return None

        applied_rules = ["wrapper_flattening", "linear_cte_chain_flattening"]
        pushed_filters: list[str] = []
        previous_name = chain_names[0]
        for step_name in chain_names[1:]:
            step = ctes_by_name[step_name].this.copy()
            step_filters = self._apply_linear_wrapper_step(
                plan,
                step,
                previous_name,
                rejected_rules,
                applied_rules,
            )
            if step_filters is None:
                return None
            pushed_filters.extend(step_filters)
            previous_name = step_name

        root_step = parsed.copy()
        root_step.set("with_", None)
        root_filters = self._apply_linear_wrapper_step(
            plan,
            root_step,
            previous_name,
            rejected_rules,
            applied_rules,
        )
        if root_filters is None:
            return None
        pushed_filters.extend(root_filters)

        return self._finalize_wrapped_optimization(
            plan=plan,
            applied_rules=applied_rules,
            pushed_filters=pushed_filters,
            rejected_rules=rejected_rules,
            extra_candidate=CandidatePlan(
                name="linear_cte_chain_flattening",
                valid=True,
                reason="linear CTE wrapper chain composed into one semantic plan",
            ),
        )

    def _apply_linear_wrapper_step(
        self,
        plan: SemanticQueryPlan,
        select: exp.Select,
        source_name: str,
        rejected_rules: dict[str, str],
        applied_rules: list[str],
    ) -> list[str] | None:
        rule_name = "linear_cte_chain_flattening"
        if select.args.get("with_"):
            rejected_rules[rule_name] = "nested WITH is not supported in a linear CTE step"
            return None
        if self._outer_has_aggregate_boundary(select):
            rejected_rules[rule_name] = "aggregate wrapper step is not supported"
            return None
        if self._wrapper_has_blocking_features(select, rejected_rules):
            rejected_rules[rule_name] = rejected_rules.get("wrapper_flattening", "wrapper step has blocking features")
            return None
        if (plan.limit is not None or plan.offset is not None) and (
            select.args.get("where") is not None or select.args.get("order") is not None
        ):
            rejected_rules[rule_name] = "filter or order after LIMIT is not safely composable"
            return None

        output_name_to_ref = self._plan_output_name_to_ref(plan)
        source = _WrappedSemanticSource(
            inner_select=exp.select("*"),
            source_name=source_name,
            source_kind="cte_chain",
        )
        projection = self._analyze_wrapper_projection(select, source, plan, output_name_to_ref, rejected_rules)
        if projection is None:
            rejected_rules[rule_name] = rejected_rules.get("wrapper_flattening", "projection step is not supported")
            return None

        plan.metrics = projection.metrics
        plan.dimensions = projection.dimensions
        plan.aliases = projection.aliases
        applied_rules.extend(projection.applied_rules)

        output_name_to_ref = self._plan_output_name_to_ref(plan)
        filters = self._translated_outer_filters(select, source, plan, output_name_to_ref, rejected_rules)
        if filters is None:
            rejected_rules[rule_name] = rejected_rules.get("safe_filter_pushdown", "filter step is not supported")
            return None
        if filters.filters:
            plan.filters = [*plan.filters, *filters.filters]
            plan.row_filters = [*plan.row_filters, *filters.row_filters]
            plan.aggregate_filters = [*plan.aggregate_filters, *filters.aggregate_filters]
            if filters.row_filters:
                applied_rules.append("safe_filter_pushdown")
            if filters.aggregate_filters:
                applied_rules.append("safe_metric_filter_having_pushdown")

        order_by = self._translated_outer_order_by(
            select,
            source,
            projection.visible_name_to_ref,
            output_name_to_ref,
            projection.projected_refs,
            rejected_rules,
        )
        if order_by is None:
            rejected_rules[rule_name] = rejected_rules.get("safe_order_pushdown", "order step is not supported")
            return None
        if order_by:
            plan.order_by = order_by
            applied_rules.append("safe_order_pushdown")

        if not self._apply_outer_limit_offset(select, plan, rejected_rules):
            rejected_rules[rule_name] = rejected_rules.get("safe_limit_pushdown", "limit step is not supported")
            return None
        if select.args.get("limit") is not None or select.args.get("offset") is not None:
            applied_rules.append("safe_limit_pushdown")

        return filters.filters

    def _optimize_dimension_distinct_wrapper(
        self,
        parsed: exp.Select,
        source: _WrappedSemanticSource,
        plan: SemanticQueryPlan,
        output_name_to_ref: dict[str, str],
        rejected_rules: dict[str, str],
    ) -> _WrappedOptimization | None:
        rule_name = "dimension_distinct_wrapper"
        if not parsed.args.get("distinct"):
            return None
        if plan.metrics:
            rejected_rules[rule_name] = "inner semantic query projects metrics"
            return None
        if parsed.args.get("joins"):
            rejected_rules[rule_name] = "outer query joins to another relation"
            return None
        if parsed.args.get("group") or parsed.args.get("having"):
            rejected_rules[rule_name] = "outer query changes aggregation"
            return None
        if parsed.args.get("qualify"):
            rejected_rules[rule_name] = "outer query uses QUALIFY"
            return None
        if any(parsed.find_all(exp.Window)):
            rejected_rules[rule_name] = "outer query contains window functions"
            return None
        if source.inner_select.args.get("limit") or source.inner_select.args.get("offset"):
            rejected_rules[rule_name] = "inner semantic query limits row membership"
            return None

        projection = self._analyze_wrapper_projection(parsed, source, plan, output_name_to_ref, rejected_rules)
        if projection is None:
            rejected_rules[rule_name] = rejected_rules.get("wrapper_flattening", "distinct projection is not supported")
            return None
        if projection.metrics:
            rejected_rules[rule_name] = "distinct metric projection is not rollup-safe"
            return None

        plan.metrics = []
        plan.dimensions = projection.dimensions
        plan.aliases = projection.aliases

        applied_rules = ["wrapper_flattening", rule_name, *projection.applied_rules]
        pushed_filters: list[str] = []

        filters = self._translated_outer_filters(parsed, source, plan, output_name_to_ref, rejected_rules)
        if filters is None:
            rejected_rules[rule_name] = rejected_rules.get("safe_filter_pushdown", "distinct filter is not supported")
            return None
        if filters.filters:
            plan.filters = [*plan.filters, *filters.filters]
            plan.row_filters = [*plan.row_filters, *filters.row_filters]
            plan.aggregate_filters = [*plan.aggregate_filters, *filters.aggregate_filters]
            pushed_filters = filters.filters
            if filters.row_filters:
                applied_rules.append("safe_filter_pushdown")
            if filters.aggregate_filters:
                rejected_rules[rule_name] = "distinct metric filters are not supported"
                return None

        order_by = self._translated_outer_order_by(
            parsed,
            source,
            projection.visible_name_to_ref,
            output_name_to_ref,
            projection.projected_refs,
            rejected_rules,
        )
        if order_by is None:
            rejected_rules[rule_name] = rejected_rules.get("safe_order_pushdown", "distinct order is not supported")
            return None
        if order_by:
            plan.order_by = order_by
            applied_rules.append("safe_order_pushdown")

        if not self._apply_outer_limit_offset(parsed, plan, rejected_rules):
            rejected_rules[rule_name] = rejected_rules.get("safe_limit_pushdown", "distinct limit is not supported")
            return None
        if parsed.args.get("limit") is not None or parsed.args.get("offset") is not None:
            applied_rules.append("safe_limit_pushdown")

        return self._finalize_wrapped_optimization(
            plan=plan,
            applied_rules=applied_rules,
            pushed_filters=pushed_filters,
            rejected_rules=rejected_rules,
            extra_candidate=CandidatePlan(
                name=rule_name,
                valid=True,
                reason="dimension-only DISTINCT wrapper has the same grain as a semantic dimension query",
            ),
        )

    def _optimize_global_row_number_topn(
        self,
        parsed: exp.Select,
        rejected_rules: dict[str, str],
    ) -> _WrappedOptimization | None:
        from_clause = parsed.args.get("from_")
        if not from_clause or not isinstance(from_clause.this, exp.Subquery):
            return None

        ranked_subquery = from_clause.this
        ranked_select = ranked_subquery.this
        if not isinstance(ranked_select, exp.Select):
            return None
        if not any(isinstance(self._projection_expression(expr), exp.Window) for expr in ranked_select.expressions):
            return None

        rule_name = "global_row_number_topn"
        if (
            parsed.args.get("joins")
            or parsed.args.get("group")
            or parsed.args.get("having")
            or parsed.args.get("qualify")
        ):
            rejected_rules[rule_name] = "outer query has blocking features"
            return None
        if parsed.args.get("distinct") or parsed.args.get("limit") or parsed.args.get("offset"):
            rejected_rules[rule_name] = "outer DISTINCT/LIMIT/OFFSET is not supported"
            return None

        topn_limit, topn_offset, row_number_alias = self._extract_row_number_topn_filter(
            parsed, ranked_subquery.alias_or_name
        )
        if topn_limit is None or row_number_alias is None:
            rejected_rules[rule_name] = "outer WHERE is not a simple row_number upper bound"
            return None

        source = self._wrapped_semantic_source(ranked_select, rejected_rules)
        if source is None:
            rejected_rules[rule_name] = "ranked source is not a direct semantic wrapper"
            return None

        try:
            plan = self._plan_simple_query(source.inner_select.copy())
        except Exception as e:
            rejected_rules[rule_name] = f"ranked semantic source cannot be planned: {e}"
            return None

        ranked_projection = self._analyze_ranked_topn_projection(
            ranked_select,
            source,
            plan,
            row_number_alias,
            rejected_rules,
        )
        if ranked_projection is None:
            return None

        plan.metrics = ranked_projection.metrics
        plan.dimensions = ranked_projection.dimensions
        plan.aliases = ranked_projection.aliases

        order_by = self._translated_row_number_order_by(
            ranked_select,
            row_number_alias,
            ranked_projection.visible_name_to_ref,
            ranked_projection.projected_refs,
            rejected_rules,
        )
        if order_by is None:
            return None
        plan.order_by = order_by
        plan.limit = topn_limit
        plan.offset = topn_offset

        outer_source = _WrappedSemanticSource(
            inner_select=ranked_select,
            source_name=ranked_subquery.alias_or_name or "",
            source_kind="row_number_topn",
        )
        output_name_to_ref = dict(ranked_projection.visible_name_to_ref)
        outer_projection = self._analyze_wrapper_projection(
            parsed,
            outer_source,
            plan,
            output_name_to_ref,
            rejected_rules,
        )
        if outer_projection is None:
            rejected_rules[rule_name] = rejected_rules.get(
                "wrapper_flattening", "outer top-N projection is not supported"
            )
            return None

        plan.metrics = outer_projection.metrics
        plan.dimensions = outer_projection.dimensions
        plan.aliases = outer_projection.aliases

        outer_order_by = self._translated_outer_order_by(
            parsed,
            outer_source,
            outer_projection.visible_name_to_ref,
            output_name_to_ref,
            outer_projection.projected_refs,
            rejected_rules,
        )
        if outer_order_by is None:
            rejected_rules[rule_name] = rejected_rules.get("safe_order_pushdown", "outer top-N order is not supported")
            return None
        if outer_order_by and outer_order_by != order_by:
            rejected_rules[rule_name] = "outer ORDER BY differs from row_number order"
            return None

        return self._finalize_wrapped_optimization(
            plan=plan,
            applied_rules=[
                "wrapper_flattening",
                rule_name,
                "safe_order_pushdown",
                "safe_limit_pushdown",
                *ranked_projection.applied_rules,
                *outer_projection.applied_rules,
            ],
            pushed_filters=[],
            rejected_rules=rejected_rules,
            extra_candidate=CandidatePlan(
                name=rule_name,
                valid=True,
                reason="global ROW_NUMBER filter is equivalent to ORDER BY plus LIMIT",
            ),
        )

    def _extract_row_number_topn_filter(
        self,
        select: exp.Select,
        source_name: str,
    ) -> tuple[int | None, int | None, str | None]:
        where_clause = select.args.get("where")
        if not where_clause:
            return None, None, None
        predicate = where_clause.this

        if isinstance(predicate, exp.Between):
            row_number = predicate.this
            low = predicate.args.get("low")
            high = predicate.args.get("high")
            if (
                not isinstance(row_number, exp.Column)
                or not isinstance(low, exp.Literal)
                or not isinstance(high, exp.Literal)
                or not low.is_int
                or not high.is_int
            ):
                return None, None, None
            if row_number.table and row_number.table != source_name:
                return None, None, None
            start = int(low.this)
            end = int(high.this)
            if start <= 0 or end < start:
                return None, None, None
            return end - start + 1, start - 1, row_number.name

        if not isinstance(predicate, (exp.LTE, exp.LT)):
            return None, None, None

        left = predicate.this
        right = predicate.expression
        if not isinstance(left, exp.Column) or not isinstance(right, exp.Literal) or not right.is_int:
            return None, None, None
        if left.table and left.table != source_name:
            return None, None, None

        value = int(right.this)
        if isinstance(predicate, exp.LT):
            value -= 1
        if value <= 0:
            return None, None, None
        return value, None, left.name

    def _optimize_qualify_row_number_topn(
        self,
        parsed: exp.Select,
        rejected_rules: dict[str, str],
    ) -> _WrappedOptimization | None:
        qualify_clause = parsed.args.get("qualify")
        if not qualify_clause:
            return None

        rule_name = "qualify_row_number_topn"
        if parsed.args.get("joins") or parsed.args.get("group") or parsed.args.get("having"):
            rejected_rules[rule_name] = "outer query has blocking features"
            return None
        if parsed.args.get("distinct") or parsed.args.get("limit") or parsed.args.get("offset"):
            rejected_rules[rule_name] = "outer DISTINCT/LIMIT/OFFSET is not supported"
            return None
        if parsed.args.get("where"):
            rejected_rules[rule_name] = "outer WHERE with QUALIFY top-N is not supported"
            return None

        topn_limit, row_number_window = self._extract_qualify_row_number_bound(qualify_clause.this)
        if topn_limit is None or row_number_window is None:
            rejected_rules[rule_name] = "QUALIFY is not a simple row_number upper bound"
            return None

        source = self._wrapped_semantic_source(parsed, rejected_rules)
        if source is None:
            rejected_rules[rule_name] = "QUALIFY source is not a direct semantic wrapper"
            return None

        try:
            plan = self._plan_simple_query(source.inner_select.copy())
        except Exception as e:
            rejected_rules[rule_name] = f"QUALIFY semantic source cannot be planned: {e}"
            return None

        output_name_to_ref = self._plan_output_name_to_ref(plan)
        projection = self._analyze_wrapper_projection(parsed, source, plan, output_name_to_ref, rejected_rules)
        if projection is None:
            rejected_rules[rule_name] = rejected_rules.get(
                "wrapper_flattening", "QUALIFY top-N projection is not supported"
            )
            return None

        plan.metrics = projection.metrics
        plan.dimensions = projection.dimensions
        plan.aliases = projection.aliases

        order_by = self._translated_window_order_by(
            row_number_window,
            projection.visible_name_to_ref,
            projection.projected_refs,
            rejected_rules,
            rule_name=rule_name,
        )
        if order_by is None:
            return None
        plan.order_by = order_by
        plan.limit = topn_limit

        return self._finalize_wrapped_optimization(
            plan=plan,
            applied_rules=[
                "wrapper_flattening",
                rule_name,
                "safe_order_pushdown",
                "safe_limit_pushdown",
                *projection.applied_rules,
            ],
            pushed_filters=[],
            rejected_rules=rejected_rules,
            extra_candidate=CandidatePlan(
                name=rule_name,
                valid=True,
                reason="global QUALIFY ROW_NUMBER filter is equivalent to ORDER BY plus LIMIT",
            ),
        )

    def _extract_qualify_row_number_bound(self, predicate: exp.Expression) -> tuple[int | None, exp.Window | None]:
        if not isinstance(predicate, (exp.LTE, exp.LT)):
            return None, None

        left = predicate.this
        right = predicate.expression
        if not isinstance(left, exp.Window) or not isinstance(right, exp.Literal) or not right.is_int:
            return None, None
        if not isinstance(left.this, exp.RowNumber):
            return None, None
        if left.args.get("partition_by"):
            return None, None
        if not left.args.get("order"):
            return None, None

        value = int(right.this)
        if isinstance(predicate, exp.LT):
            value -= 1
        if value <= 0:
            return None, None
        return value, left

    def _analyze_ranked_topn_projection(
        self,
        select: exp.Select,
        source: _WrappedSemanticSource,
        plan: SemanticQueryPlan,
        row_number_alias: str,
        rejected_rules: dict[str, str],
    ) -> _ProjectionAnalysis | None:
        rule_name = "global_row_number_topn"
        if (
            select.args.get("where")
            or select.args.get("group")
            or select.args.get("having")
            or select.args.get("qualify")
        ):
            rejected_rules[rule_name] = "ranked query has blocking clauses"
            return None
        if select.args.get("distinct") or select.args.get("limit") or select.args.get("offset"):
            rejected_rules[rule_name] = "ranked query DISTINCT/LIMIT/OFFSET is not supported"
            return None
        if select.args.get("joins"):
            rejected_rules[rule_name] = "ranked query joins another relation"
            return None

        row_number_count = 0
        non_window_projections: list[exp.Expression] = []
        for projection in select.expressions:
            alias = projection.alias if isinstance(projection, exp.Alias) else None
            expression = self._projection_expression(projection)
            if isinstance(expression, exp.Window):
                if alias != row_number_alias:
                    rejected_rules[rule_name] = "row_number alias does not match outer filter"
                    return None
                if not isinstance(expression.this, exp.RowNumber):
                    rejected_rules[rule_name] = "only ROW_NUMBER is supported"
                    return None
                if expression.args.get("partition_by"):
                    rejected_rules[rule_name] = "partitioned ROW_NUMBER is not global"
                    return None
                if not expression.args.get("order"):
                    rejected_rules[rule_name] = "ROW_NUMBER has no ORDER BY"
                    return None
                row_number_count += 1
                continue
            if any(expression.find_all(exp.Window)):
                rejected_rules[rule_name] = "ranked query contains another window expression"
                return None
            non_window_projections.append(projection.copy())

        if row_number_count != 1:
            rejected_rules[rule_name] = "ranked query must project exactly one ROW_NUMBER"
            return None

        projection_select = exp.select(*non_window_projections)
        output_name_to_ref = self._plan_output_name_to_ref(plan)
        return self._analyze_wrapper_projection(
            projection_select,
            source,
            plan,
            output_name_to_ref,
            rejected_rules,
        )

    def _translated_row_number_order_by(
        self,
        ranked_select: exp.Select,
        row_number_alias: str,
        visible_name_to_ref: dict[str, str],
        projected_refs: set[str],
        rejected_rules: dict[str, str],
    ) -> list[str] | None:
        rule_name = "global_row_number_topn"
        row_number_window = None
        for projection in ranked_select.expressions:
            if projection.alias != row_number_alias:
                continue
            expression = self._projection_expression(projection)
            if isinstance(expression, exp.Window):
                row_number_window = expression
                break
        if row_number_window is None:
            rejected_rules[rule_name] = "ROW_NUMBER projection not found"
            return None

        return self._translated_window_order_by(
            row_number_window,
            visible_name_to_ref,
            projected_refs,
            rejected_rules,
            rule_name=rule_name,
        )

    def _translated_window_order_by(
        self,
        row_number_window: exp.Window,
        visible_name_to_ref: dict[str, str],
        projected_refs: set[str],
        rejected_rules: dict[str, str],
        rule_name: str,
    ) -> list[str] | None:
        order_clause = row_number_window.args.get("order")
        if not order_clause:
            rejected_rules[rule_name] = "ROW_NUMBER has no ORDER BY"
            return None

        order_by: list[str] = []
        for order_expr in order_clause.expressions:
            if isinstance(order_expr, exp.Ordered) and order_expr.args.get("nulls_first"):
                rejected_rules[rule_name] = "ROW_NUMBER ORDER BY uses explicit NULLS ordering"
                return None
            expression = order_expr.this if isinstance(order_expr, exp.Ordered) else order_expr
            if not isinstance(expression, exp.Column):
                rejected_rules[rule_name] = "ROW_NUMBER ORDER BY computes a new expression"
                return None
            ref = visible_name_to_ref.get(expression.name)
            if ref is None or ref not in projected_refs:
                rejected_rules[rule_name] = "ROW_NUMBER ORDER BY references a non-projected field"
                return None
            order_name = next(
                (name for name, visible_ref in visible_name_to_ref.items() if visible_ref == ref),
                expression.name,
            )
            if isinstance(order_expr, exp.Ordered) and order_expr.args.get("desc", False):
                order_name = f"{order_name} DESC"
            elif isinstance(order_expr, exp.Ordered):
                order_name = f"{order_name} ASC"
            order_by.append(order_name)

        return order_by

    def _wrapped_semantic_source(
        self, select: exp.Select, rejected_rules: dict[str, str]
    ) -> _WrappedSemanticSource | None:
        from_clause = select.args.get("from_")
        if not from_clause:
            rejected_rules["wrapped_semantic_optimizer"] = "outer query has no FROM clause"
            return None

        with_clause = select.args.get("with_")
        table_expr = from_clause.this

        if isinstance(table_expr, exp.Subquery):
            if with_clause:
                rejected_rules["cte_wrapper"] = "outer WITH plus subquery wrapper is not flattened"
                return None
            inner_select = table_expr.this
            if not isinstance(inner_select, exp.Select):
                rejected_rules["wrapped_semantic_optimizer"] = "subquery is not a SELECT"
                return None
            if not self._references_semantic_model(inner_select):
                rejected_rules["wrapped_semantic_optimizer"] = "subquery does not directly reference a semantic model"
                return None
            return _WrappedSemanticSource(
                inner_select=inner_select,
                source_name=table_expr.alias_or_name or "",
                source_kind="subquery",
            )

        if isinstance(table_expr, exp.Table) and with_clause:
            ctes = list(with_clause.expressions)
            if len(ctes) != 1:
                rejected_rules["cte_wrapper"] = "only single-CTE semantic wrappers can be flattened"
                return None
            cte = ctes[0]
            if table_expr.name != cte.alias:
                rejected_rules["cte_wrapper"] = "outer query does not read directly from the semantic CTE"
                return None
            inner_select = cte.this
            if not isinstance(inner_select, exp.Select):
                rejected_rules["cte_wrapper"] = "semantic CTE is not a SELECT"
                return None
            if not self._references_semantic_model(inner_select):
                rejected_rules["cte_wrapper"] = "CTE does not directly reference a semantic model"
                return None
            return _WrappedSemanticSource(
                inner_select=inner_select,
                source_name=table_expr.name,
                source_kind="cte",
            )

        rejected_rules["wrapped_semantic_optimizer"] = "outer query is not a single semantic subquery or CTE wrapper"
        return None

    def _outer_has_aggregate_boundary(self, select: exp.Select) -> bool:
        if select.args.get("group") or select.args.get("having"):
            return True
        return any(
            self._is_supported_outer_rollup_aggregate(self._projection_expression(expr)) for expr in select.expressions
        )

    def _analyze_same_grain_aggregate_wrapper(
        self,
        select: exp.Select,
        source: _WrappedSemanticSource,
        plan: SemanticQueryPlan,
        output_name_to_ref: dict[str, str],
        rejected_rules: dict[str, str],
    ) -> _ProjectionAnalysis | None:
        rule_name = "same_grain_metric_wrapper"
        group_clause = select.args.get("group")
        if not group_clause:
            rejected_rules[rule_name] = "outer_query_has_no_group_by"
            return None
        if select.args.get("joins"):
            rejected_rules[rule_name] = "outer_query_joins_relation"
            return None
        if select.args.get("distinct"):
            rejected_rules[rule_name] = "outer_query_uses_distinct"
            return None
        if select.args.get("having"):
            rejected_rules[rule_name] = "outer_query_uses_having"
            return None
        if select.args.get("qualify"):
            rejected_rules[rule_name] = "outer_query_uses_qualify"
            return None
        if any(select.find_all(exp.Window)):
            rejected_rules[rule_name] = "outer_query_uses_window"
            return None
        if sql_has_aggregate(select.sql(dialect=self.dialect)):
            rejected_rules[rule_name] = "outer_query_contains_aggregate"
            return None

        selected_refs: list[str] = []
        aliases = dict(plan.aliases)
        visible_name_to_ref: dict[str, str] = {}
        projected_refs: set[str] = set()

        for projection in select.expressions:
            alias = projection.alias if isinstance(projection, exp.Alias) else None
            expression = self._projection_expression(projection)
            if not isinstance(expression, exp.Column):
                rejected_rules[rule_name] = "outer_projection_computes_expression"
                return None
            if expression.table and expression.table != source.source_name:
                rejected_rules[rule_name] = "outer_projection_references_another_relation"
                return None
            ref = output_name_to_ref.get(expression.name)
            if ref is None:
                rejected_rules[rule_name] = f"outer_projection_unknown_field_{expression.name}"
                return None
            if ref in projected_refs:
                rejected_rules[rule_name] = "outer_projection_selects_duplicate_field"
                return None

            selected_refs.append(ref)
            projected_refs.add(ref)
            output_name = alias or expression.name
            visible_name_to_ref[output_name] = ref
            if alias:
                aliases[ref] = alias

        group_refs: set[str] = set()
        for group_expr in group_clause.expressions:
            if not isinstance(group_expr, exp.Column):
                rejected_rules[rule_name] = "outer_group_expression_not_supported"
                return None
            if group_expr.table and group_expr.table != source.source_name:
                rejected_rules[rule_name] = "outer_group_references_another_relation"
                return None
            ref = output_name_to_ref.get(group_expr.name)
            if ref is None:
                rejected_rules[rule_name] = f"outer_group_unknown_field_{group_expr.name}"
                return None
            group_refs.add(ref)

        inner_refs = set(plan.dimensions) | set(plan.metrics)
        if group_refs != inner_refs or projected_refs != inner_refs:
            rejected_rules[rule_name] = "outer_group_does_not_match_inner_grain"
            return None

        return _ProjectionAnalysis(
            metrics=[metric for metric in plan.metrics if metric in projected_refs],
            dimensions=[dimension for dimension in plan.dimensions if dimension in projected_refs],
            aliases={ref: alias for ref, alias in aliases.items() if ref in projected_refs},
            visible_name_to_ref=visible_name_to_ref,
            projected_refs=projected_refs,
            applied_rules=[rule_name],
        )

    def _analyze_wrapper_aggregate_boundary(
        self,
        select: exp.Select,
        source: _WrappedSemanticSource,
        plan: SemanticQueryPlan,
        output_name_to_ref: dict[str, str],
        rejected_rules: dict[str, str],
    ) -> _AggregateBoundaryAnalysis | None:
        rule_name = "aggregate_boundary_rollup"

        if select.args.get("joins"):
            rejected_rules[rule_name] = "outer_query_joins_relation"
            return None
        if select.args.get("distinct"):
            rejected_rules[rule_name] = "outer_query_uses_distinct"
            return None
        if select.args.get("qualify"):
            rejected_rules[rule_name] = "outer_query_uses_qualify"
            return None
        if select.args.get("having"):
            rejected_rules[rule_name] = "aggregate_boundary_having_not_supported"
            return None
        if any(select.find_all(exp.Window)):
            rejected_rules[rule_name] = "outer_query_uses_window"
            return None
        if source.inner_select.args.get("limit") or source.inner_select.args.get("offset"):
            rejected_rules[rule_name] = "inner_query_limits_row_membership"
            return None
        if source.inner_select.args.get("distinct"):
            rejected_rules[rule_name] = "inner_query_uses_distinct"
            return None
        if source.inner_select.args.get("having"):
            rejected_rules[rule_name] = "inner_query_has_metric_having"
            return None

        group_refs = self._resolve_outer_group_refs(select, source, output_name_to_ref, plan, rejected_rules)
        if group_refs is None:
            return None

        selected_dimensions: list[str] = []
        selected_metrics: list[str] = []
        aliases: dict[str, str] = {}
        visible_name_to_ref: dict[str, str] = {}
        projected_refs: set[str] = set()
        applied_rules = [rule_name]

        for projection in select.expressions:
            alias = projection.alias if isinstance(projection, exp.Alias) else None
            expression = self._projection_expression(projection)

            time_rollup_ref = self._resolve_outer_time_rollup_dimension(
                expression,
                source,
                output_name_to_ref,
                plan,
                rejected_rules,
                rule_name,
            )
            if time_rollup_ref is None and isinstance(expression, exp.TimestampTrunc) and rule_name in rejected_rules:
                return None
            if time_rollup_ref is not None:
                if time_rollup_ref not in group_refs:
                    rejected_rules[rule_name] = "outer_projection_dimension_not_grouped"
                    return None
                if time_rollup_ref in selected_dimensions:
                    rejected_rules[rule_name] = "outer_projection_selects_duplicate_dimension"
                    return None
                output_name = alias or time_rollup_ref.split(".", 1)[1]
                selected_dimensions.append(time_rollup_ref)
                projected_refs.add(time_rollup_ref)
                visible_name_to_ref[output_name] = time_rollup_ref
                if alias:
                    aliases[time_rollup_ref] = alias
                applied_rules.append("time_grain_rollup")
                continue

            if isinstance(expression, exp.Column):
                if expression.table and expression.table != source.source_name:
                    rejected_rules[rule_name] = "outer_projection_references_another_relation"
                    return None
                ref = output_name_to_ref.get(expression.name)
                if ref not in group_refs:
                    rejected_rules[rule_name] = "outer_projection_dimension_not_grouped"
                    return None
                if ref in selected_dimensions:
                    rejected_rules[rule_name] = "outer_projection_selects_duplicate_dimension"
                    return None
                output_name = alias or expression.name
                selected_dimensions.append(ref)
                projected_refs.add(ref)
                visible_name_to_ref[output_name] = ref
                if alias:
                    aliases[ref] = alias
                continue

            metric_analysis = self._analyze_outer_rollup_aggregate(
                expression,
                alias,
                source,
                plan,
                output_name_to_ref,
                group_refs,
                rejected_rules,
            )
            if metric_analysis is None:
                return None

            metric_ref, output_name, metric_rule = metric_analysis
            if metric_ref in selected_metrics:
                rejected_rules[rule_name] = "outer_projection_selects_duplicate_metric"
                return None
            selected_metrics.append(metric_ref)
            projected_refs.add(metric_ref)
            aliases[metric_ref] = output_name
            visible_name_to_ref[output_name] = metric_ref
            applied_rules.append(metric_rule)

        if not selected_metrics:
            rejected_rules[rule_name] = "outer_projection_has_no_rollup_metrics"
            return None

        if set(selected_dimensions) != group_refs:
            rejected_rules[rule_name] = "outer_group_dimension_not_projected"
            return None

        filters = self._translated_outer_filters(select, source, plan, output_name_to_ref, rejected_rules)
        if filters is None:
            rejected_rules[rule_name] = rejected_rules.pop(
                "safe_filter_pushdown",
                "aggregate_boundary_filter_not_supported",
            )
            return None
        if filters.row_filters:
            applied_rules.append("aggregate_boundary_dimension_filter_pushdown")
        if filters.aggregate_filters:
            applied_rules.append("safe_metric_filter_having_pushdown")

        return _AggregateBoundaryAnalysis(
            metrics=selected_metrics,
            dimensions=selected_dimensions,
            aliases=aliases,
            visible_name_to_ref=visible_name_to_ref,
            projected_refs=projected_refs,
            row_filters=filters.row_filters,
            aggregate_filters=filters.aggregate_filters,
            applied_rules=self._dedupe(applied_rules),
        )

    def _projection_expression(self, projection: exp.Expression) -> exp.Expression:
        return projection.this if isinstance(projection, exp.Alias) else projection

    def _resolve_outer_group_refs(
        self,
        select: exp.Select,
        source: _WrappedSemanticSource,
        output_name_to_ref: dict[str, str],
        plan: SemanticQueryPlan,
        rejected_rules: dict[str, str],
    ) -> set[str] | None:
        rule_name = "aggregate_boundary_rollup"
        group_clause = select.args.get("group")
        if not group_clause:
            return set()

        dimension_refs = set(plan.dimensions)
        group_refs: set[str] = set()
        for group_expr in group_clause.expressions:
            resolved_group_expr = self._resolve_group_expression_position(select, group_expr)
            if resolved_group_expr is None:
                rejected_rules[rule_name] = "outer_group_position_not_supported"
                return None
            time_rollup_ref = self._resolve_outer_time_rollup_dimension(
                resolved_group_expr,
                source,
                output_name_to_ref,
                plan,
                rejected_rules,
                rule_name,
            )
            if (
                time_rollup_ref is None
                and isinstance(resolved_group_expr, exp.TimestampTrunc)
                and rule_name in rejected_rules
            ):
                return None
            if time_rollup_ref is not None:
                group_refs.add(time_rollup_ref)
                continue
            if not isinstance(resolved_group_expr, exp.Column):
                rejected_rules[rule_name] = "outer_group_expression_not_supported"
                return None
            if resolved_group_expr.table and resolved_group_expr.table != source.source_name:
                rejected_rules[rule_name] = "outer_group_references_another_relation"
                return None
            ref = output_name_to_ref.get(resolved_group_expr.name)
            if ref not in dimension_refs:
                rejected_rules[rule_name] = "outer_group_field_not_inner_dimension"
                return None
            group_refs.add(ref)

        return group_refs

    def _resolve_outer_time_rollup_dimension(
        self,
        expression: exp.Expression,
        source: _WrappedSemanticSource,
        output_name_to_ref: dict[str, str],
        plan: SemanticQueryPlan,
        rejected_rules: dict[str, str],
        rule_name: str,
    ) -> str | None:
        if not isinstance(expression, exp.TimestampTrunc):
            return None

        column = expression.this
        if not isinstance(column, exp.Column):
            rejected_rules[rule_name] = "time_rollup_input_not_dimension"
            return None
        if column.table and column.table != source.source_name:
            rejected_rules[rule_name] = "time_rollup_references_another_relation"
            return None

        inner_ref = output_name_to_ref.get(column.name)
        if inner_ref not in set(plan.dimensions):
            rejected_rules[rule_name] = "time_rollup_input_not_inner_dimension"
            return None
        if "__" not in inner_ref:
            rejected_rules[rule_name] = "time_rollup_input_has_no_grain"
            return None

        base_ref, inner_grain = inner_ref.rsplit("__", 1)
        target_grain = self._timestamp_trunc_unit(expression)
        if target_grain is None:
            rejected_rules[rule_name] = "time_rollup_grain_not_supported"
            return None
        if not self._is_time_grain_rollup_safe(inner_grain, target_grain):
            rejected_rules[rule_name] = "time_grain_rollup_not_safe"
            return None
        return f"{base_ref}__{target_grain}"

    def _timestamp_trunc_unit(self, expression: exp.TimestampTrunc) -> str | None:
        unit = expression.args.get("unit")
        if unit is None:
            return None
        value = getattr(unit, "this", None)
        if value is None:
            value = unit.sql(dialect=self.dialect)
        return str(value).strip("'\"").lower()

    def _is_time_grain_rollup_safe(self, inner_grain: str, target_grain: str) -> bool:
        order = ["second", "minute", "hour", "day", "week", "month", "quarter", "year"]
        if inner_grain not in order or target_grain not in order:
            return False
        if inner_grain == target_grain:
            return True
        if inner_grain == "week":
            return False
        return order.index(inner_grain) < order.index(target_grain)

    def _resolve_group_expression_position(
        self,
        select: exp.Select,
        group_expr: exp.Expression,
    ) -> exp.Expression | None:
        if not isinstance(group_expr, exp.Literal) or not group_expr.is_int:
            return group_expr
        index = int(group_expr.this) - 1
        if index < 0 or index >= len(select.expressions):
            return None
        return self._projection_expression(select.expressions[index])

    def _analyze_outer_rollup_aggregate(
        self,
        expression: exp.Expression,
        alias: str | None,
        source: _WrappedSemanticSource,
        plan: SemanticQueryPlan,
        output_name_to_ref: dict[str, str],
        group_refs: set[str],
        rejected_rules: dict[str, str],
    ) -> tuple[str, str, str] | None:
        rule_name = "aggregate_boundary_rollup"
        outer_agg = self._outer_rollup_aggregate_name(expression)
        if outer_agg is None:
            rejected_rules[rule_name] = "outer_projection_computes_expression"
            return None

        if any(expression.find_all(exp.Distinct)):
            rejected_rules[rule_name] = "outer_aggregate_distinct_not_supported"
            return None
        if any(expression.find_all(exp.Order)):
            rejected_rules[rule_name] = "outer_aggregate_order_not_supported"
            return None

        argument = expression.this
        if not isinstance(argument, exp.Column):
            rejected_rules[rule_name] = "outer_aggregate_input_not_metric"
            return None
        if argument.table and argument.table != source.source_name:
            rejected_rules[rule_name] = "outer_aggregate_references_another_relation"
            return None

        metric_ref = output_name_to_ref.get(argument.name)
        if metric_ref not in set(plan.metrics):
            rejected_rules[rule_name] = "outer_aggregate_input_not_metric"
            return None

        metric, _model_context = self._resolve_metric(metric_ref)
        if not metric:
            rejected_rules[rule_name] = "outer_metric_not_found"
            return None

        dropped_dimensions = set(plan.dimensions) - group_refs
        if metric.non_additive_dimension and self._dimension_is_dropped(
            metric.non_additive_dimension,
            dropped_dimensions,
        ):
            rejected_rules[rule_name] = "non_additive_dimension_dropped"
            return None

        metric_agg = metric.agg
        if outer_agg == "sum" and metric_agg == "sum":
            metric_rule = "additive_metric_rollup"
        elif outer_agg == "sum" and metric_agg == "count":
            metric_rule = "count_metric_rollup"
        elif outer_agg == "min" and metric_agg == "min":
            if metric.fill_nulls_with is not None:
                rejected_rules[rule_name] = "min_max_rollup_with_fill_nulls_not_supported"
                return None
            metric_rule = "min_metric_rollup"
        elif outer_agg == "max" and metric_agg == "max":
            if metric.fill_nulls_with is not None:
                rejected_rules[rule_name] = "min_max_rollup_with_fill_nulls_not_supported"
                return None
            metric_rule = "max_metric_rollup"
        else:
            rejected_rules[rule_name] = "outer_aggregate_not_rollup_safe"
            return None

        return metric_ref, alias or argument.name, metric_rule

    def _is_supported_outer_rollup_aggregate(self, expression: exp.Expression) -> bool:
        return self._outer_rollup_aggregate_name(expression) is not None

    def _outer_rollup_aggregate_name(self, expression: exp.Expression) -> str | None:
        if isinstance(expression, exp.Sum):
            return "sum"
        if isinstance(expression, exp.Min):
            return "min"
        if isinstance(expression, exp.Max):
            return "max"
        return None

    def _dimension_is_dropped(self, dimension_name: str, dropped_dimensions: set[str]) -> bool:
        for dropped in dropped_dimensions:
            if dropped == dimension_name:
                return True
            if "." in dropped and dropped.split(".", 1)[1] == dimension_name:
                return True
        return False

    def _wrapper_has_blocking_features(self, select: exp.Select, rejected_rules: dict[str, str]) -> bool:
        if select.args.get("joins"):
            rejected_rules["wrapper_flattening"] = "outer query joins to another relation"
            return True

        if select.args.get("distinct"):
            rejected_rules["wrapper_flattening"] = "outer query uses DISTINCT"
            return True

        if select.args.get("group") or select.args.get("having"):
            rejected_rules["wrapper_flattening"] = "outer query changes aggregation"
            return True

        if select.args.get("qualify"):
            rejected_rules["wrapper_flattening"] = "outer query uses QUALIFY"
            return True

        outer_sql = select.sql(dialect=self.dialect)
        if sql_has_aggregate(outer_sql):
            rejected_rules["wrapper_flattening"] = "outer query contains aggregate expressions"
            return True

        if any(select.find_all(exp.Window)):
            rejected_rules["wrapper_flattening"] = "outer query contains window functions"
            return True

        return False

    def _plan_output_name_to_ref(self, plan: SemanticQueryPlan) -> dict[str, str]:
        refs = [*plan.dimensions, *plan.metrics]
        field_names: dict[str, list[str]] = {}
        for ref in refs:
            if "." in ref:
                model_name, field_name = ref.split(".", 1)
                field_names.setdefault(field_name, []).append(model_name)
            else:
                field_names.setdefault(ref, []).append("")

        output_name_to_ref: dict[str, str] = {}
        for ref in refs:
            if ref in plan.aliases:
                output_name = plan.aliases[ref]
            elif "." in ref:
                model_name, field_name = ref.split(".", 1)
                output_name = f"{model_name}_{field_name}" if len(field_names.get(field_name, [])) > 1 else field_name
            else:
                output_name = ref
            output_name_to_ref[output_name] = ref
        return output_name_to_ref

    def _analyze_wrapper_projection(
        self,
        select: exp.Select,
        source: _WrappedSemanticSource,
        plan: SemanticQueryPlan,
        output_name_to_ref: dict[str, str],
        rejected_rules: dict[str, str],
    ) -> _ProjectionAnalysis | None:
        if not select.expressions:
            rejected_rules["wrapper_flattening"] = "outer query has no projections"
            return None

        if len(select.expressions) == 1 and isinstance(select.expressions[0], exp.Star):
            visible_name_to_ref = dict(output_name_to_ref)
            return _ProjectionAnalysis(
                metrics=list(plan.metrics),
                dimensions=list(plan.dimensions),
                aliases=dict(plan.aliases),
                visible_name_to_ref=visible_name_to_ref,
                projected_refs=set(visible_name_to_ref.values()),
                applied_rules=["trivial_wrapper_flattening"],
            )

        selected_refs: list[str] = []
        aliases = dict(plan.aliases)
        visible_name_to_ref: dict[str, str] = {}
        applied_rules = ["wrapper_projection_flattening"]

        for projection in select.expressions:
            alias = projection.alias if isinstance(projection, exp.Alias) else None
            expression = projection.this if isinstance(projection, exp.Alias) else projection
            if not isinstance(expression, exp.Column):
                rejected_rules["wrapper_flattening"] = "outer projection computes a new expression"
                return None
            if expression.table and expression.table != source.source_name:
                rejected_rules["wrapper_flattening"] = "outer projection references another relation"
                return None
            column_name = expression.name
            ref = output_name_to_ref.get(column_name)
            if ref is None:
                rejected_rules["wrapper_flattening"] = (
                    f"outer projection '{column_name}' is not an inner semantic field"
                )
                return None
            if ref in selected_refs:
                rejected_rules["wrapper_flattening"] = "outer projection selects the same semantic field more than once"
                return None

            selected_refs.append(ref)
            output_name = alias or column_name
            if alias:
                aliases[ref] = alias
            visible_name_to_ref[output_name] = ref

        selected = set(selected_refs)
        selected_dimensions = [ref for ref in selected_refs if ref in plan.dimensions]
        if set(selected_dimensions) != set(plan.dimensions):
            rejected_rules["wrapper_flattening"] = (
                "outer projection drops dimensions and would change semantic grouping"
            )
            return None

        return _ProjectionAnalysis(
            metrics=[ref for ref in selected_refs if ref in plan.metrics],
            dimensions=selected_dimensions,
            aliases={ref: alias for ref, alias in aliases.items() if ref in selected},
            visible_name_to_ref=visible_name_to_ref,
            projected_refs=selected,
            applied_rules=applied_rules,
        )

    def _translated_outer_filters(
        self,
        select: exp.Select,
        source: _WrappedSemanticSource,
        plan: SemanticQueryPlan,
        output_name_to_ref: dict[str, str],
        rejected_rules: dict[str, str],
    ) -> _FilterTranslation | None:
        where_clause = select.args.get("where")
        if not where_clause:
            return _FilterTranslation(row_filters=[], aggregate_filters=[])

        if plan.limit is not None or plan.offset is not None or source.inner_select.args.get("distinct"):
            rejected_rules["safe_filter_pushdown"] = "inner semantic query limits row membership"
            return None

        selected_refs = set(plan.dimensions) | set(plan.metrics)
        row_filters: list[str] = []
        aggregate_filters: list[str] = []
        try:
            for filter_part in self._top_level_and_parts(where_clause.this):
                stage = self._wrapper_filter_stage(
                    filter_part,
                    output_name_to_ref,
                    source.source_name,
                    selected_refs,
                    rule_name="safe_filter_pushdown",
                )
                translated = self._translate_wrapper_expression(
                    filter_part,
                    output_name_to_ref,
                    source.source_name,
                    allowed_refs=selected_refs,
                    rule_name="safe_filter_pushdown",
                )
                if stage == "aggregate":
                    aggregate_filters.extend(self._filters_from_expression(translated))
                else:
                    row_filters.extend(self._filters_from_expression(translated))
        except ValueError as e:
            rejected_rules["safe_filter_pushdown"] = str(e)
            return None

        return _FilterTranslation(row_filters=row_filters, aggregate_filters=aggregate_filters)

    def _top_level_and_parts(self, expression: exp.Expression) -> list[exp.Expression]:
        if isinstance(expression, exp.And):
            return [*self._top_level_and_parts(expression.left), *self._top_level_and_parts(expression.right)]
        return [expression]

    def _wrapper_filter_stage(
        self,
        expression: exp.Expression,
        name_to_ref: dict[str, str],
        source_name: str,
        allowed_refs: set[str],
        rule_name: str,
    ) -> str:
        refs = set()
        for column in expression.find_all(exp.Column):
            if column.table and column.table != source_name:
                raise ValueError(f"{rule_name} references another relation")
            ref = name_to_ref.get(column.name)
            if ref is None:
                raise ValueError(f"{rule_name} references unknown field '{column.name}'")
            if ref not in allowed_refs:
                raise ValueError(f"{rule_name} cannot move unprojected or computed field '{column.name}'")
            if self._metric_needs_window_function(ref):
                raise ValueError(f"{rule_name} cannot move window metric filter '{column.name}'")
            refs.add(ref)

        metric_refs = {ref for ref in refs if self._is_metric_ref(ref)}
        dimension_refs = refs - metric_refs
        if metric_refs and dimension_refs and any(expression.find_all(exp.Or)):
            raise ValueError(f"{rule_name} cannot split mixed metric/dimension OR predicate")
        return "aggregate" if metric_refs else "row"

    def _translated_outer_order_by(
        self,
        select: exp.Select,
        source: _WrappedSemanticSource,
        visible_name_to_ref: dict[str, str],
        output_name_to_ref: dict[str, str],
        projected_refs: set[str],
        rejected_rules: dict[str, str],
    ) -> list[str] | None:
        order_clause = select.args.get("order")
        if not order_clause:
            return []

        if select.args.get("limit") is not None and self._extract_limit(source.inner_select) is not None:
            rejected_rules["safe_order_pushdown"] = "inner semantic query already has LIMIT"
            return None

        order_by: list[str] = []
        for order_expr in order_clause.expressions:
            if isinstance(order_expr, exp.Ordered) and order_expr.args.get("nulls_first"):
                rejected_rules["safe_order_pushdown"] = "outer ORDER BY uses explicit NULLS ordering"
                return None

            expression = order_expr.this if isinstance(order_expr, exp.Ordered) else order_expr
            if not isinstance(expression, exp.Column):
                rejected_rules["safe_order_pushdown"] = "outer ORDER BY computes a new expression"
                return None
            if expression.table and expression.table != source.source_name:
                rejected_rules["safe_order_pushdown"] = "outer ORDER BY references another relation"
                return None

            column_name = expression.name
            ref = visible_name_to_ref.get(column_name) or output_name_to_ref.get(column_name)
            if ref is None:
                rejected_rules["safe_order_pushdown"] = f"outer ORDER BY '{column_name}' is not a semantic field"
                return None
            if ref not in projected_refs:
                rejected_rules["safe_order_pushdown"] = "outer ORDER BY references a non-projected field"
                return None

            order_name = next(
                (name for name, visible_ref in visible_name_to_ref.items() if visible_ref == ref), column_name
            )
            if isinstance(order_expr, exp.Ordered) and order_expr.args.get("desc", False):
                order_name = f"{order_name} DESC"
            elif isinstance(order_expr, exp.Ordered):
                order_name = f"{order_name} ASC"
            order_by.append(order_name)

        return order_by

    def _apply_outer_limit_offset(
        self,
        select: exp.Select,
        plan: SemanticQueryPlan,
        rejected_rules: dict[str, str],
    ) -> bool:
        outer_limit = self._extract_limit(select)
        outer_offset = self._extract_offset(select)

        if outer_limit is not None and plan.limit is not None:
            rejected_rules["safe_limit_pushdown"] = "inner semantic query already has LIMIT"
            return False
        if outer_offset is not None and plan.offset is not None:
            rejected_rules["safe_limit_pushdown"] = "inner semantic query already has OFFSET"
            return False

        if outer_limit is not None:
            plan.limit = outer_limit
        if outer_offset is not None:
            plan.offset = outer_offset
        return True

    def _translate_wrapper_expression(
        self,
        expression: exp.Expression,
        name_to_ref: dict[str, str],
        source_name: str,
        allowed_refs: set[str],
        rule_name: str,
    ) -> exp.Expression:
        if any(expression.find_all(exp.Select)):
            raise ValueError("outer expression contains a subquery")
        if any(expression.find_all(exp.Window)):
            raise ValueError("outer expression contains a window function")
        if sql_has_aggregate(expression.sql(dialect=self.dialect)):
            raise ValueError("outer expression contains an aggregate")

        def replace_column(node):
            if not isinstance(node, exp.Column):
                return node
            if node.table and node.table != source_name:
                raise ValueError(f"{rule_name} references another relation")
            ref = name_to_ref.get(node.name)
            if ref is None:
                raise ValueError(f"{rule_name} references unknown field '{node.name}'")
            if ref not in allowed_refs:
                raise ValueError(f"{rule_name} cannot move metric or computed field '{node.name}'")
            return self._column_expression_for_ref(ref)

        return expression.copy().transform(replace_column)

    def _column_expression_for_ref(self, ref: str) -> exp.Expression:
        if "." not in ref:
            return exp.column(ref)
        table, name = ref.split(".", 1)
        return exp.column(name, table=table)

    def _filters_from_expression(self, expression: exp.Expression) -> list[str]:
        if isinstance(expression, (exp.And, exp.Or)):
            return self._extract_compound_filters(expression)
        return [expression.sql(dialect=self.dialect)]

    def _collect_semantic_scopes(self, select: exp.Select) -> tuple[list[SemanticQueryPlan], list[str]]:
        scopes: list[SemanticQueryPlan] = []
        warnings: list[str] = []
        had_inferred_table = hasattr(self, "inferred_table")
        previous_inferred_table = getattr(self, "inferred_table", None)

        try:
            for nested_select in select.find_all(exp.Select):
                if not self._references_semantic_model(nested_select):
                    continue
                try:
                    scopes.append(self._plan_simple_query(nested_select.copy()))
                except Exception as e:
                    warnings.append(f"Could not plan semantic scope {nested_select.sql(dialect=self.dialect)}: {e}")
        finally:
            if had_inferred_table:
                self.inferred_table = previous_inferred_table
            elif hasattr(self, "inferred_table"):
                del self.inferred_table

        return scopes, warnings

    def _candidate_plans_for_scopes(self, scopes: list[SemanticQueryPlan], chosen_plan: str) -> list[CandidatePlan]:
        if not scopes:
            return [
                CandidatePlan(
                    name="semantic_plus_postprocess",
                    valid=False,
                    reason="no semantic scopes found",
                )
            ]

        candidates_by_name: dict[str, CandidatePlan] = {}
        for scope in scopes:
            for candidate in scope.candidate_plans:
                existing = candidates_by_name.get(candidate.name)
                if existing is None or (candidate.valid and not existing.valid):
                    candidates_by_name[candidate.name] = candidate

        candidates_by_name["semantic_plus_postprocess"] = CandidatePlan(
            name="semantic_plus_postprocess",
            valid=chosen_plan == "semantic_plus_postprocess",
            reason=(
                "outer SQL performs post-processing around semantic scopes"
                if chosen_plan == "semantic_plus_postprocess"
                else "root semantic query can be planned directly"
            ),
        )
        candidates_by_name.setdefault(
            "passthrough_plain_sql",
            CandidatePlan(
                name="passthrough_plain_sql",
                valid=False,
                reason="query contains semantic scopes",
            ),
        )

        return list(candidates_by_name.values())

    def _explanation_from_plan(
        self,
        sql: str,
        plan: SemanticQueryPlan,
        rewritten_sql: str | None,
    ) -> RewriteExplanation:
        return RewriteExplanation(
            input_sql=sql,
            rewritten_sql=rewritten_sql,
            chosen_plan=plan.candidate_kind,
            source_kind=plan.source_kind,
            metrics=plan.metrics,
            dimensions=plan.dimensions,
            filters=plan.filters,
            row_filters=plan.row_filters,
            aggregate_filters=plan.aggregate_filters,
            order_by=plan.order_by,
            limit=plan.limit,
            offset=plan.offset,
            aliases=plan.aliases,
            candidate_plans=plan.candidate_plans,
            semantic_scopes=[plan],
            preaggregation=plan.eligibility.get("single_model_preaggregation", {}),
            fanout=plan.eligibility.get("fanout_preaggregation", {}),
            applied_rules=plan.applied_rules,
            rejected_rules=plan.rejected_rules,
        )

    def _plan_simple_query(self, parsed: exp.Select, include_candidate_details: bool = True) -> SemanticQueryPlan:
        """Build a behavior-preserving semantic query plan for a simple SELECT."""
        explicit_join_filters = []
        if parsed.args.get("joins"):
            explicit_join_filters = self._validate_explicit_semantic_joins(parsed)

        self.inferred_table = self._extract_from_table(parsed)
        self.table_aliases = self._source_aliases(parsed)

        if self._needs_expression_postprocess(parsed):
            raise ValueError("Semantic expression queries cannot be represented as a simple rewrite plan")

        metrics, dimensions, aliases = self._extract_metrics_and_dimensions(parsed)
        self._validate_root_group_by(parsed, dimensions, aliases)
        filters = [*self._extract_filters(parsed), *explicit_join_filters]
        having_clause = parsed.args.get("having")
        if having_clause:
            qualified_having = self._qualify_root_filter_expression(having_clause.this.copy())
            filters.extend(self._filters_from_expression(qualified_having))
        row_filters, aggregate_filters = self._stage_semantic_filters(filters)
        order_by = self._extract_order_by(parsed)
        limit = self._extract_limit(parsed)
        offset = self._extract_offset(parsed)

        if not metrics and not dimensions:
            raise ValueError("Query must select at least one metric or dimension")

        plan = SemanticQueryPlan(
            source_sql=parsed.sql(dialect=self.dialect),
            source_kind=self._source_kind_for_table(self.inferred_table),
            metrics=metrics,
            dimensions=dimensions,
            filters=filters,
            row_filters=row_filters,
            aggregate_filters=aggregate_filters,
            order_by=order_by,
            limit=limit,
            offset=offset,
            aliases=aliases,
        )
        plan.eligibility = (
            self._plan_eligibility(plan)
            if include_candidate_details or self.use_preaggregations
            else self._lightweight_plan_eligibility(plan)
        )
        plan.candidate_kind = self._chosen_candidate_kind(plan)
        plan.candidate_plans = self._candidate_plans_for_plan(plan) if include_candidate_details else []
        if plan.candidate_kind == "single_model_preaggregation":
            plan.applied_rules.append("preaggregation_route_selection")
        if plan.candidate_kind == "join_key_preaggregation":
            plan.applied_rules.append("join_key_preaggregation_route_selection")
        if plan.candidate_kind == "fanout_preaggregation":
            plan.applied_rules.append("fanout_strategy_selection")
        return plan

    def _validate_root_group_by(
        self,
        parsed: exp.Select,
        dimensions: list[str],
        aliases: dict[str, str],
    ) -> None:
        group_clause = parsed.args.get("group")
        if not group_clause:
            return

        dimension_refs = set(dimensions)
        name_to_ref: dict[str, str] = {}
        ambiguous_names: set[str] = set()
        for ref in dimensions:
            if "." in ref:
                _model_name, field_name = ref.split(".", 1)
                if field_name in name_to_ref and name_to_ref[field_name] != ref:
                    ambiguous_names.add(field_name)
                else:
                    name_to_ref[field_name] = ref
            name_to_ref[ref] = ref
            alias = aliases.get(ref)
            if alias:
                name_to_ref[alias] = ref

        grouped_refs = set()
        for expression in group_clause.expressions:
            if not isinstance(expression, exp.Column):
                raise ValueError("GROUP BY is only supported when it repeats selected semantic dimensions exactly.")

            if expression.table:
                ref = f"{expression.table}.{expression.name}"
            elif expression.name in ambiguous_names:
                raise ValueError(f"GROUP BY field '{expression.name}' is ambiguous; use a qualified semantic field.")
            else:
                ref = name_to_ref.get(expression.name)

            if ref not in dimension_refs:
                raise ValueError("GROUP BY is only supported when it repeats selected semantic dimensions exactly.")
            grouped_refs.add(ref)

        if grouped_refs != dimension_refs:
            raise ValueError("GROUP BY must include exactly the selected semantic dimensions.")

    def _source_kind_for_table(self, table_name: str | None) -> str:
        if table_name == "metrics":
            return "metrics"
        if table_name in self.graph.models:
            return "model"
        if table_name:
            return "table"
        return "unknown"

    def _plan_eligibility(self, plan: SemanticQueryPlan) -> dict[str, dict[str, object]]:
        window_metrics = [metric for metric in plan.metrics if self._metric_needs_window_function(metric)]
        fanout_needed = self.generator._needs_preaggregation_for_fanout(plan.metrics, plan.dimensions)
        window_details: dict[str, object] = {
            "eligible": bool(window_metrics),
            "metrics": window_metrics,
            "reason": "window_metric_required" if window_metrics else "no_window_metrics",
        }
        if window_metrics:
            window_details["inner_preaggregation"] = self._window_inner_preaggregation_eligibility(
                plan,
                window_metrics,
            )
        return {
            "window_metric": window_details,
            "fanout_preaggregation": {
                "eligible": fanout_needed,
                "reason": "fanout_protection_required" if fanout_needed else "fanout_protection_not_needed",
            },
            "join_key_preaggregation": self._join_key_preaggregation_eligibility(plan),
            "single_model_preaggregation": self._single_model_preaggregation_eligibility(plan),
        }

    def _lightweight_plan_eligibility(self, plan: SemanticQueryPlan) -> dict[str, dict[str, object]]:
        return {
            "window_metric": {
                "eligible": False,
                "metrics": [],
                "reason": "not_evaluated_for_rewrite",
            },
            "fanout_preaggregation": {
                "eligible": False,
                "reason": "not_evaluated_for_rewrite",
            },
            "join_key_preaggregation": {
                "eligible": False,
                "reason": "preaggregations_disabled",
                "enabled": False,
                "requires_enablement": True,
            },
            "single_model_preaggregation": {
                "eligible": False,
                "reason": "preaggregations_disabled",
                "enabled": False,
                "requires_enablement": True,
            },
        }

    def _join_key_preaggregation_eligibility(self, plan: SemanticQueryPlan) -> dict[str, object]:
        details = self.generator.explain_join_key_preaggregation(
            metrics=plan.metrics,
            dimensions=plan.dimensions,
            filters=plan.filters,
            aliases=plan.aliases,
        )
        details["enabled"] = self.use_preaggregations
        details["requires_enablement"] = True
        return details

    def _window_inner_preaggregation_eligibility(
        self,
        plan: SemanticQueryPlan,
        window_metrics: list[str],
    ) -> dict[str, object]:
        if not self.use_preaggregations:
            return {
                "eligible": False,
                "reason": "preaggregations_not_enabled",
            }

        base_metrics = self._window_inner_base_metrics(window_metrics)
        if not base_metrics:
            return {
                "eligible": False,
                "reason": "no_preaggregatable_window_base_metrics",
            }

        inner_plan = SemanticQueryPlan(
            source_sql=plan.source_sql,
            source_kind=plan.source_kind,
            metrics=base_metrics,
            dimensions=plan.dimensions,
            filters=plan.filters,
            order_by=None,
            aliases={},
        )
        details = self._single_model_preaggregation_eligibility(inner_plan)
        details["metrics"] = base_metrics
        return details

    def _window_inner_base_metrics(self, window_metrics: list[str]) -> list[str]:
        base_metrics: list[str] = []

        def add(metric_ref: str | None, model_context: str | None = None) -> None:
            if not metric_ref:
                return
            if "." not in metric_ref and model_context:
                metric_ref = f"{model_context}.{metric_ref}"
            if metric_ref not in base_metrics:
                base_metrics.append(metric_ref)

        for metric_ref in window_metrics:
            metric, model_context = self._resolve_metric(metric_ref)
            if not metric:
                continue

            if metric.type == "cumulative":
                add(metric.sql or metric.base_metric, model_context)
            elif metric.type == "time_comparison":
                add(metric.base_metric, model_context)
            elif metric.type == "ratio" and metric.offset_window:
                add(metric.numerator, model_context)
                add(metric.denominator, model_context)

        return base_metrics

    def _resolve_metric(self, metric_ref: str):
        if "." in metric_ref:
            model_name, metric_name = metric_ref.split(".", 1)
            try:
                model = self.graph.get_model(model_name)
                model_metric = model.get_metric(metric_name) if model else None
                if model_metric:
                    return model_metric, model_name
            except KeyError:
                pass
            try:
                return self.graph.get_metric(metric_ref), None
            except KeyError:
                return None, model_name

        try:
            return self.graph.get_metric(metric_ref), None
        except KeyError:
            pass

        matches = []
        for model_name, model in self.graph.models.items():
            metric = model.get_metric(metric_ref)
            if metric:
                matches.append((metric, model_name))
        if len(matches) == 1:
            return matches[0]

        return None, None

    def _is_metric_ref(self, ref: str) -> bool:
        metric, _model_context = self._resolve_metric(ref)
        return metric is not None

    def _candidate_plans_for_plan(self, plan: SemanticQueryPlan) -> list[CandidatePlan]:
        window_details = plan.eligibility["window_metric"]
        fanout_details = plan.eligibility["fanout_preaggregation"]
        preagg_details = plan.eligibility["single_model_preaggregation"]
        join_key_details = plan.eligibility["join_key_preaggregation"]

        return [
            CandidatePlan(
                name="direct_semantic",
                valid=True,
                reason="simple SELECT references semantic model fields",
            ),
            CandidatePlan(
                name="semantic_plus_postprocess",
                valid=False,
                reason="no outer SQL post-processing required",
            ),
            CandidatePlan(
                name="single_model_preaggregation",
                valid=bool(preagg_details["eligible"]),
                reason=str(preagg_details["reason"]),
                details=preagg_details,
            ),
            CandidatePlan(
                name="join_key_preaggregation",
                valid=bool(join_key_details["eligible"]),
                reason=str(join_key_details["reason"]),
                details=join_key_details,
            ),
            CandidatePlan(
                name="fanout_preaggregation",
                valid=bool(fanout_details["eligible"]),
                reason=str(fanout_details["reason"]),
                details=fanout_details,
            ),
            CandidatePlan(
                name="window_metric",
                valid=bool(window_details["eligible"]),
                reason=str(window_details["reason"]),
                details=window_details,
            ),
            CandidatePlan(
                name="passthrough_plain_sql",
                valid=False,
                reason="query references semantic model fields",
            ),
        ]

    def _chosen_candidate_kind(self, plan: SemanticQueryPlan) -> str:
        if plan.eligibility["window_metric"]["eligible"]:
            return "window_metric"
        if plan.eligibility["fanout_preaggregation"]["eligible"]:
            return "fanout_preaggregation"
        if self.use_preaggregations and plan.eligibility["join_key_preaggregation"]["eligible"]:
            return "join_key_preaggregation"
        if self.use_preaggregations and plan.eligibility["single_model_preaggregation"]["eligible"]:
            return "single_model_preaggregation"
        return "direct_semantic"

    def _single_model_preaggregation_eligibility(self, plan: SemanticQueryPlan) -> dict[str, object]:
        try:
            model_names = self.generator._find_required_models(plan.metrics, plan.dimensions, plan.filters)
        except Exception as e:
            return {
                "eligible": False,
                "reason": "model_resolution_failed",
                "error": str(e),
            }

        if len(model_names) != 1:
            return {
                "eligible": False,
                "reason": "not_single_model_query",
                "models": model_names,
            }

        model_name = model_names[0]
        model = self.graph.get_model(model_name)
        if not model.pre_aggregations:
            return {
                "eligible": False,
                "reason": "model_has_no_preaggregations",
                "model": model_name,
            }

        try:
            parsed_dims = self.generator._parse_dimension_refs(plan.dimensions)
            metric_names = [metric.split(".", 1)[1] if "." in metric else metric for metric in plan.metrics]
            dim_names = []
            time_granularity = None
            for dim_ref, gran in parsed_dims:
                dim_name = dim_ref.split(".", 1)[1] if "." in dim_ref else dim_ref
                dim_names.append(dim_name)
                if gran:
                    time_granularity = gran
            row_filters = plan.row_filters or [
                f for f in plan.filters if not self._semantic_filter_references_metric(f)
            ]
            filter_exprs = [f.replace(f"{model_name}.", "").replace(f"{model_name}_cte.", "") for f in row_filters]

            from sidemantic.core.preagg_matcher import PreAggregationMatcher

            matcher = PreAggregationMatcher(model)
            preagg_candidates = matcher.explain_matching(
                metrics=metric_names,
                dimensions=dim_names,
                time_granularity=time_granularity,
                filters=filter_exprs,
            )
            preagg_sql = self.generator._try_use_preaggregation(
                model_name=model_name,
                metrics=plan.metrics,
                parsed_dims=parsed_dims,
                filters=plan.filters,
                order_by=plan.order_by,
                limit=plan.limit,
                offset=plan.offset,
                aliases=plan.aliases,
            )
        except Exception as e:
            return {
                "eligible": False,
                "reason": "preaggregation_check_failed",
                "model": model_name,
                "error": str(e),
            }

        candidate_details = [
            {
                "name": candidate.name,
                "matched": candidate.matched,
                "selected": candidate.selected,
                "score": candidate.score,
                "checks": [
                    {
                        "name": check.name,
                        "passed": check.passed,
                        "detail": check.detail,
                    }
                    for check in candidate.checks
                ],
            }
            for candidate in preagg_candidates
        ]
        reason = "matching_preaggregation" if preagg_sql else self._preaggregation_rejection_reason(candidate_details)

        return {
            "eligible": preagg_sql is not None,
            "reason": reason,
            "model": model_name,
            "enabled": self.use_preaggregations,
            "requires_enablement": True,
            "row_filters": list(plan.row_filters or []),
            "aggregate_filters": list(plan.aggregate_filters or []),
            "candidates": candidate_details,
        }

    def _preaggregation_rejection_reason(self, candidates: list[dict[str, object]]) -> str:
        failed_checks = []
        for candidate in candidates:
            for check in candidate.get("checks", []):
                if not check["passed"]:
                    if "count_distinct_not_rollup_safe" in str(check.get("detail", "")):
                        return "count_distinct_not_rollup_safe"
                    failed_checks.append(check["name"])

        if "filters" in failed_checks:
            return "filter_not_compatible"
        if "granularity" in failed_checks:
            return "time_grain_mismatch"
        if "dimensions" in failed_checks:
            return "dimension_not_in_rollup"
        if "measures" in failed_checks:
            return "metric_not_in_rollup"
        return "no_matching_preaggregation"

    def _metric_needs_window_function(self, metric_ref: str) -> bool:
        metric = None
        if "." in metric_ref:
            model_name, metric_name = metric_ref.split(".", 1)
            try:
                model = self.graph.get_model(model_name)
                metric = model.get_metric(metric_name) if model else None
            except KeyError:
                pass
            if not metric:
                try:
                    metric = self.graph.get_metric(metric_ref)
                except KeyError:
                    pass
        else:
            try:
                metric = self.graph.get_metric(metric_ref)
            except KeyError:
                pass
            if not metric:
                for model in self.graph.models.values():
                    found = model.get_metric(metric_ref)
                    if found:
                        metric = found
                        break

        if not metric:
            return False

        if metric.type in ("cumulative", "time_comparison", "conversion", "retention", "cohort"):
            return True
        return metric.type == "ratio" and bool(metric.offset_window)

    def _generate_from_plan(self, plan: SemanticQueryPlan) -> str:
        return self.generator.generate(
            metrics=plan.metrics,
            dimensions=plan.dimensions,
            filters=plan.filters,
            order_by=plan.order_by,
            limit=plan.limit,
            offset=plan.offset,
            use_preaggregations=self.use_preaggregations,
            aliases=plan.aliases,
            user_attributes=getattr(self, "_rewrite_user_attributes", None),
        )

    def _dedupe(self, values: list[str]) -> list[str]:
        deduped = []
        seen = set()
        for value in values:
            if value in seen:
                continue
            deduped.append(value)
            seen.add(value)
        return deduped

    def _looks_like_yardstick_query(self, sql: str) -> bool:
        """Return True if query appears to use Yardstick query syntax."""
        if not _YARDSTICK_SYNTAX_HINT_RE.search(sql):
            return False

        try:
            tokens = sqlglot.tokenize(sql, read=self.dialect)
        except Exception:
            return False

        if not tokens:
            return False

        if tokens[0].text.upper() == "SEMANTIC":
            return True

        if self._contains_yardstick_curly_measure_reference(sql, tokens):
            return True

        for i in range(len(tokens) - 1):
            if tokens[i].text.upper() == "AGGREGATE" and tokens[i + 1].token_type == TokenType.L_PAREN:
                return True
            # Support Yardstick `measure AT (...)` syntax without AGGREGATE wrapper.
            if tokens[i].token_type == TokenType.VAR and tokens[i + 1].text.upper() == "AT":
                if i + 2 < len(tokens) and tokens[i + 2].token_type == TokenType.L_PAREN:
                    return True
            if (
                i + 3 < len(tokens)
                and tokens[i].token_type == TokenType.VAR
                and tokens[i + 1].token_type == TokenType.DOT
                and tokens[i + 2].token_type == TokenType.VAR
                and tokens[i + 3].text.upper() == "AT"
            ):
                if i + 4 < len(tokens) and tokens[i + 4].token_type == TokenType.L_PAREN:
                    return True

        return False

    def would_use_yardstick_rewrite(self, sql: str) -> bool:
        """Return whether SQL would take an explicit or implicit Yardstick rewrite path."""
        if self._looks_like_yardstick_query(sql):
            return True
        if not self._graph_has_yardstick_models():
            return False
        try:
            parsed = sqlglot.parse_one(sql, dialect=self.dialect)
        except Exception:
            return False
        return isinstance(parsed, exp.Select) and self._contains_implicit_yardstick_measure_query(parsed)

    def _graph_has_yardstick_models(self) -> bool:
        return any(
            isinstance(model.metadata, dict) and "yardstick" in model.metadata for model in self.graph.models.values()
        )

    def _is_yardstick_identifier_token(self, token) -> bool:
        return token.token_type in {TokenType.VAR, TokenType.IDENTIFIER, TokenType.SCHEMA}

    def _is_yardstick_curly_measure_inner(self, inner_tokens: list) -> bool:
        if not inner_tokens:
            return False

        expect_identifier = True
        for token in inner_tokens:
            if expect_identifier:
                if not self._is_yardstick_identifier_token(token):
                    return False
            else:
                if token.token_type != TokenType.DOT:
                    return False
            expect_identifier = not expect_identifier
        return not expect_identifier

    def _contains_yardstick_curly_measure_reference(self, sql: str, tokens: list) -> bool:
        i = 0
        while i < len(tokens):
            if tokens[i].token_type != TokenType.L_BRACE:
                i += 1
                continue

            depth = 1
            j = i + 1
            while j < len(tokens):
                if tokens[j].token_type == TokenType.L_BRACE:
                    depth += 1
                elif tokens[j].token_type == TokenType.R_BRACE:
                    depth -= 1
                    if depth == 0:
                        break
                j += 1

            if depth != 0:
                raise ValueError("Invalid Yardstick measure reference: unclosed '{...}'")

            inner_tokens = tokens[i + 1 : j]
            if self._is_yardstick_curly_measure_inner(inner_tokens):
                return True
            i = j + 1

        return False

    def _expand_yardstick_curly_measure_references(self, sql: str) -> str:
        """Expand Yardstick `{measure}` shorthand into plain measure references."""
        tokens = sqlglot.tokenize(sql, read=self.dialect)
        rewritten_parts: list[str] = []
        cursor = 0
        i = 0

        while i < len(tokens):
            if tokens[i].token_type != TokenType.L_BRACE:
                i += 1
                continue

            depth = 1
            j = i + 1
            while j < len(tokens):
                if tokens[j].token_type == TokenType.L_BRACE:
                    depth += 1
                elif tokens[j].token_type == TokenType.R_BRACE:
                    depth -= 1
                    if depth == 0:
                        break
                j += 1

            if depth != 0:
                raise ValueError("Invalid Yardstick measure reference: unclosed '{...}'")

            inner_tokens = tokens[i + 1 : j]
            if self._is_yardstick_curly_measure_inner(inner_tokens):
                rewritten_parts.append(sql[cursor : tokens[i].start])
                inner_start = inner_tokens[0].start
                inner_end = inner_tokens[-1].end + 1
                rewritten_parts.append(sql[inner_start:inner_end].strip())
                cursor = tokens[j].end + 1

            i = j + 1

        rewritten_parts.append(sql[cursor:])
        return "".join(rewritten_parts)

    def _contains_implicit_yardstick_measure_query(self, parsed: exp.Select) -> bool:
        """Return True when query references plain measure columns in Yardstick contexts."""
        for select_scope in parsed.find_all(exp.Select):
            source_models = self._extract_source_models_from_select(select_scope)
            if not source_models:
                continue
            if not any(self._is_yardstick_model(model_name) for model_name in source_models.values()):
                continue

            single_model_scope = len(source_models) == 1 and self._has_single_source_relation(select_scope)
            default_alias = next(iter(source_models)) if single_model_scope else None
            placeholder_names: set[str] = set()

            candidate_expressions: list[exp.Expression] = list(select_scope.expressions)
            having_clause = select_scope.args.get("having")
            if having_clause:
                candidate_expressions.append(having_clause.this)
            order_clause = select_scope.args.get("order")
            if order_clause:
                candidate_expressions.extend(order_expr.this for order_expr in order_clause.expressions)

            for candidate_expr in candidate_expressions:
                for column in candidate_expr.find_all(exp.Column):
                    if self._resolve_implicit_yardstick_measure_reference(
                        column,
                        source_models=source_models,
                        default_alias=default_alias,
                        single_model_scope=single_model_scope,
                        placeholder_names=placeholder_names,
                    ):
                        return True

        return False

    def _is_yardstick_model(self, model_name: str) -> bool:
        model = self.graph.get_model(model_name)
        metadata = model.metadata or {}
        return isinstance(metadata, dict) and "yardstick" in metadata

    def _rewrite_yardstick_query(self, sql: str, strict: bool = True, allow_plain_measures: bool = False) -> str:
        """Rewrite Yardstick-style SQL (`SEMANTIC`, `AGGREGATE`, `AT`) to plain SQL."""
        original_sql = sql
        tokens = sqlglot.tokenize(sql, read=self.dialect)
        has_semantic_prefix = bool(tokens and tokens[0].text.upper() == "SEMANTIC")
        sql = self._expand_yardstick_curly_measure_references(sql)
        transformed_sql, calls = self._replace_yardstick_aggregate_calls(sql)

        # SEMANTIC prefix without AGGREGATE: fall back to normal SQL rewrite path.
        if not calls and not allow_plain_measures:
            if not has_semantic_prefix and transformed_sql == original_sql:
                return transformed_sql
            return self.rewrite(transformed_sql, strict=strict)

        try:
            parsed = sqlglot.parse_one(transformed_sql, dialect=self.dialect)
        except Exception as e:
            raise ValueError(f"Failed to parse Yardstick SQL: {e}") from e

        select_scopes = list(parsed.find_all(exp.Select))
        if not select_scopes:
            raise ValueError("Yardstick rewrite requires a SELECT query or statement containing SELECT")

        call_map = {call.placeholder: call for call in calls}
        placeholder_names = set(call_map)
        rewritten_root: exp.Expression = parsed
        # Rewrite innermost SELECT scopes first so nested Yardstick placeholders are
        # resolved in their own FROM/JOIN context before outer scopes are processed.
        for select_scope in reversed(select_scopes):
            rewritten_scope = self._rewrite_yardstick_select_scope(
                select_scope,
                call_map=call_map,
                placeholder_names=placeholder_names,
                allow_plain_measures=allow_plain_measures,
            )
            if rewritten_scope is select_scope:
                continue
            if select_scope.parent:
                select_scope.replace(rewritten_scope)
            else:
                rewritten_root = rewritten_scope

        return rewritten_root.sql(dialect=self.dialect)

    def _rewrite_yardstick_select_scope(
        self,
        select_scope: exp.Select,
        call_map: dict[str, _YardstickAggregateCall],
        placeholder_names: set[str],
        allow_plain_measures: bool = False,
    ) -> exp.Select:
        initial_placeholders = {
            column.name
            for column in self._columns_without_nested_scopes(select_scope)
            if not column.table and column.name in placeholder_names
        }
        if not initial_placeholders and not allow_plain_measures:
            return select_scope

        source_models = self._extract_source_models_from_select(select_scope)
        if not source_models:
            if initial_placeholders:
                raise ValueError("Yardstick query must reference at least one known semantic model in FROM/JOIN")
            return select_scope

        # Only default-qualify unaliased columns when this scope truly has a single source relation.
        single_model_scope = len(source_models) == 1 and self._has_single_source_relation(select_scope)
        default_alias = next(iter(source_models)) if single_model_scope else None

        select_scope = self._inject_implicit_yardstick_measure_calls(
            select_scope=select_scope,
            source_models=source_models,
            default_alias=default_alias,
            single_model_scope=single_model_scope,
            call_map=call_map,
            placeholder_names=placeholder_names,
        )

        scope_placeholders = {
            column.name
            for column in self._columns_without_nested_scopes(select_scope)
            if not column.table and column.name in placeholder_names
        }
        if not scope_placeholders:
            return select_scope

        select_scope = self._expand_yardstick_dimension_references(
            select_scope,
            source_models=source_models,
            default_alias=default_alias,
            placeholder_names=placeholder_names,
        )

        if default_alias and single_model_scope:
            qualified_projections: list[exp.Expression] = []
            for projection in select_scope.expressions:
                expr_obj = projection.this if isinstance(projection, exp.Alias) else projection
                if self._expression_contains_yardstick_placeholder(expr_obj, scope_placeholders):
                    qualified_projections.append(projection)
                    continue

                qualified_expr = self._qualify_unaliased_columns(expr_obj.copy(), default_alias)
                if isinstance(projection, exp.Alias):
                    projection.set("this", qualified_expr)
                    qualified_projections.append(projection)
                else:
                    qualified_projections.append(qualified_expr)

            select_scope.set("expressions", qualified_projections)

        select_scope = self._alias_unaliased_yardstick_dimension_expressions(
            select_scope,
            placeholder_names=scope_placeholders,
        )

        has_metric_projection = False
        projection_is_metric: list[bool] = []
        projection_is_literal: list[bool] = []
        for projection in select_scope.expressions:
            expr_obj = projection.this if isinstance(projection, exp.Alias) else projection
            is_metric = self._expression_contains_yardstick_placeholder(expr_obj, scope_placeholders)
            is_literal = isinstance(expr_obj, (exp.Literal, exp.Null, exp.Boolean))
            projection_is_metric.append(is_metric)
            projection_is_literal.append(is_literal)
            has_metric_projection = has_metric_projection or is_metric

        # Yardstick semantics: when AGGREGATE appears and GROUP BY is omitted, group by non-aggregate expressions.
        if has_metric_projection and select_scope.args.get("group") is None:
            group_exprs = []
            for projection, is_metric, is_literal in zip(
                select_scope.expressions, projection_is_metric, projection_is_literal, strict=False
            ):
                if is_metric:
                    continue
                expr_obj = projection.this if isinstance(projection, exp.Alias) else projection
                if is_literal:
                    continue
                group_exprs.append(expr_obj.copy())
            if group_exprs:
                select_scope.set("group", exp.Group(expressions=group_exprs))
            elif all(
                is_metric or is_literal for is_metric, is_literal in zip(projection_is_metric, projection_is_literal)
            ):
                # Scalar aggregate projections should collapse to one row.
                select_scope.set("distinct", exp.Distinct())

        group_clause = select_scope.args.get("group")
        if group_clause:
            self._resolve_yardstick_group_ordinals(group_clause, select_scope.expressions)
        if group_clause and default_alias and single_model_scope:
            group_clause.set(
                "expressions",
                [
                    self._qualify_unaliased_columns(group_expr.copy(), default_alias)
                    for group_expr in group_clause.expressions
                ],
            )
            for group_key in ("rollup", "cube", "grouping_sets"):
                grouped_exprs = group_clause.args.get(group_key)
                if grouped_exprs:
                    group_clause.set(
                        group_key,
                        [
                            self._qualify_unaliased_columns(group_expr.copy(), default_alias)
                            for group_expr in grouped_exprs
                        ],
                    )
        group_expressions = self._collect_yardstick_group_expressions(group_clause) if group_clause else []
        outer_where = select_scope.args.get("where")
        projection_aliases: dict[str, str] = {}
        for projection in select_scope.expressions:
            if not isinstance(projection, exp.Alias):
                continue
            if self._expression_contains_yardstick_placeholder(projection.this, scope_placeholders):
                continue
            signature = self._expr_signature_without_tables(projection.this)
            projection_aliases[signature] = projection.alias

        replacement_sql: dict[str, str] = {}
        for placeholder in scope_placeholders:
            replacement_sql[placeholder] = self._build_yardstick_call_sql(
                call=call_map[placeholder],
                source_models=source_models,
                group_expressions=group_expressions,
                projection_aliases=projection_aliases,
                outer_where=outer_where,
                default_alias=default_alias,
                single_model_scope=single_model_scope,
            )

        replacement_expr_cache = {
            key: sqlglot.parse_one(value, dialect=self.dialect) for key, value in replacement_sql.items()
        }

        def replace_placeholder(node: exp.Expression) -> exp.Expression:
            if isinstance(node, exp.Column) and not node.table and node.name in replacement_expr_cache:
                return replacement_expr_cache[node.name].copy()
            return node

        rewritten = select_scope.transform(replace_placeholder)
        rewritten = self._inline_yardstick_order_by_subquery_aliases(rewritten)
        return self._rewrite_source_model_relations(rewritten)

    def _inline_yardstick_order_by_subquery_aliases(self, select_scope: exp.Select) -> exp.Select:
        """Inline subquery-backed SELECT aliases inside compound ORDER BY expressions.

        DuckDB accepts ``ORDER BY alias`` for a select item whose expression is a
        scalar subquery, but it rejects compound expressions such as
        ``ORDER BY metric_alias / total_alias`` when either alias expands to a
        scalar subquery. Yardstick upstream fixes this by inlining only the
        subquery-backed aliases in non-simple ORDER BY expressions.
        """
        order_clause = select_scope.args.get("order")
        if not order_clause:
            return select_scope

        aliases: dict[str, tuple[exp.Expression, bool]] = {}
        has_subquery_alias = False
        for projection in select_scope.expressions:
            if not isinstance(projection, exp.Alias) or not projection.alias:
                continue
            has_subquery = self._expression_has_subquery(projection.this)
            aliases[projection.alias.lower()] = (projection.this, has_subquery)
            has_subquery_alias = has_subquery_alias or has_subquery

        if not aliases or not has_subquery_alias:
            return select_scope

        table_qualifiers = self._yardstick_order_table_qualifiers(select_scope)
        for order_expr in order_clause.expressions:
            expr_obj = order_expr.this
            if self._is_simple_yardstick_order_alias_ref(expr_obj, aliases, table_qualifiers):
                continue
            if not self._yardstick_order_expr_references_subquery_alias(expr_obj, aliases, table_qualifiers):
                continue

            rewritten_expr, changed = self._inline_yardstick_order_alias_expr(
                expr_obj,
                aliases,
                table_qualifiers,
            )
            if changed:
                order_expr.set("this", rewritten_expr)

        return select_scope

    def _expression_has_subquery(self, expression: exp.Expression) -> bool:
        return any(isinstance(node, exp.Subquery) for node in expression.walk())

    def _yardstick_order_table_qualifiers(self, select_scope: exp.Select) -> set[str]:
        qualifiers: set[str] = set()

        def add_relation(relation: exp.Expression | None) -> None:
            if relation is None:
                return
            alias = relation.alias
            if alias:
                qualifiers.add(alias.lower())
                return
            if isinstance(relation, exp.Table):
                qualifiers.add(relation.name.lower())

        from_clause = select_scope.args.get("from_")
        if from_clause:
            add_relation(from_clause.this)

        for join in select_scope.args.get("joins") or []:
            add_relation(join.this)

        return qualifiers

    def _yardstick_order_alias_key(
        self,
        expression: exp.Expression,
        aliases: dict[str, tuple[exp.Expression, bool]],
        table_qualifiers: set[str],
    ) -> str | None:
        if not isinstance(expression, exp.Column):
            return None

        table = expression.table
        if table and not (table.lower() == "alias" and "alias" not in table_qualifiers):
            return None

        alias_key = expression.name.lower()
        if alias_key not in aliases:
            return None
        return alias_key

    def _is_simple_yardstick_order_alias_ref(
        self,
        expression: exp.Expression,
        aliases: dict[str, tuple[exp.Expression, bool]],
        table_qualifiers: set[str],
    ) -> bool:
        return self._yardstick_order_alias_key(expression, aliases, table_qualifiers) is not None

    def _yardstick_order_expr_references_subquery_alias(
        self,
        expression: exp.Expression,
        aliases: dict[str, tuple[exp.Expression, bool]],
        table_qualifiers: set[str],
    ) -> bool:
        if isinstance(expression, (exp.Select, exp.Subquery)):
            return False

        alias_key = self._yardstick_order_alias_key(expression, aliases, table_qualifiers)
        if alias_key is not None and aliases[alias_key][1]:
            return True

        for value in expression.args.values():
            if isinstance(value, exp.Expression):
                if self._yardstick_order_expr_references_subquery_alias(value, aliases, table_qualifiers):
                    return True
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, exp.Expression) and self._yardstick_order_expr_references_subquery_alias(
                        item,
                        aliases,
                        table_qualifiers,
                    ):
                        return True
        return False

    def _inline_yardstick_order_alias_expr(
        self,
        expression: exp.Expression,
        aliases: dict[str, tuple[exp.Expression, bool]],
        table_qualifiers: set[str],
    ) -> tuple[exp.Expression, bool]:
        if isinstance(expression, (exp.Select, exp.Subquery)):
            return expression, False

        alias_key = self._yardstick_order_alias_key(expression, aliases, table_qualifiers)
        if alias_key is not None and aliases[alias_key][1]:
            return aliases[alias_key][0].copy(), True

        rewritten = expression.copy()
        changed = False
        for arg_key, value in list(rewritten.args.items()):
            if isinstance(value, exp.Expression):
                child, child_changed = self._inline_yardstick_order_alias_expr(value, aliases, table_qualifiers)
                if child_changed:
                    rewritten.set(arg_key, child)
                    changed = True
            elif isinstance(value, list):
                new_values = []
                list_changed = False
                for item in value:
                    if isinstance(item, exp.Expression):
                        child, child_changed = self._inline_yardstick_order_alias_expr(item, aliases, table_qualifiers)
                        new_values.append(child)
                        list_changed = list_changed or child_changed
                    else:
                        new_values.append(item)
                if list_changed:
                    rewritten.set(arg_key, new_values)
                    changed = True

        if not changed:
            return expression, False
        return rewritten, True

    def _collect_yardstick_group_expressions(self, group_clause: exp.Group) -> list[exp.Expression]:
        expressions: list[exp.Expression] = []
        expressions.extend(group_clause.expressions)
        for group_key in ("rollup", "cube", "grouping_sets"):
            grouped_exprs = group_clause.args.get(group_key)
            if grouped_exprs:
                expressions.extend(grouped_exprs)
        return expressions

    def _resolve_yardstick_group_ordinals(self, group_clause: exp.Group, projections: list[exp.Expression]) -> None:
        resolved: list[exp.Expression] = []
        for group_expr in group_clause.expressions:
            if isinstance(group_expr, exp.Literal) and not group_expr.is_string:
                try:
                    ordinal = int(group_expr.this)
                except ValueError:
                    resolved.append(group_expr)
                    continue

                if 1 <= ordinal <= len(projections):
                    projection = projections[ordinal - 1]
                    projection_expr = projection.this if isinstance(projection, exp.Alias) else projection
                    resolved.append(projection_expr.copy())
                    continue
            resolved.append(group_expr)
        group_clause.set("expressions", resolved)

    def _alias_unaliased_yardstick_dimension_expressions(
        self,
        select_scope: exp.Select,
        placeholder_names: set[str],
    ) -> exp.Select:
        """Alias complex unaliased dimension expressions for stable correlation references."""
        existing_aliases = {
            projection.alias
            for projection in select_scope.expressions
            if isinstance(projection, exp.Alias) and projection.alias
        }
        alias_index = 0
        rewritten_projections: list[exp.Expression] = []

        for projection in select_scope.expressions:
            if isinstance(projection, exp.Alias):
                rewritten_projections.append(projection)
                continue

            if isinstance(projection, (exp.Column, exp.Literal, exp.Null, exp.Boolean, exp.Star)):
                rewritten_projections.append(projection)
                continue

            if self._expression_contains_yardstick_placeholder(projection, placeholder_names):
                rewritten_projections.append(projection)
                continue

            if not any(isinstance(column, exp.Column) for column in projection.find_all(exp.Column)):
                rewritten_projections.append(projection)
                continue

            while True:
                alias_name = f"__ysdim_{alias_index}"
                alias_index += 1
                if alias_name not in existing_aliases:
                    existing_aliases.add(alias_name)
                    break

            rewritten_projections.append(exp.alias_(projection.copy(), alias_name, quoted=False))

        select_scope.set("expressions", rewritten_projections)
        return select_scope

    def _replace_yardstick_aggregate_calls(self, sql: str) -> tuple[str, list[_YardstickAggregateCall]]:
        """Strip optional SEMANTIC prefix and replace AGGREGATE...AT chains with placeholders."""
        tokens = sqlglot.tokenize(sql, read=self.dialect)
        calls: list[_YardstickAggregateCall] = []
        segments: list[str] = []
        cursor = 0
        i = 0

        if tokens and tokens[0].text.upper() == "SEMANTIC":
            cursor = tokens[0].end + 1
            while cursor < len(sql) and sql[cursor].isspace():
                cursor += 1
            i = 1

        def parse_at_chain(start_idx: int, default_end_idx: int) -> tuple[list[str], int]:
            modifiers: list[str] = []
            end_idx = default_end_idx
            k = start_idx
            while (
                k + 1 < len(tokens) and tokens[k].text.upper() == "AT" and tokens[k + 1].token_type == TokenType.L_PAREN
            ):
                m_open = k + 1
                depth = 0
                m_close = m_open
                while m_close < len(tokens):
                    if tokens[m_close].token_type == TokenType.L_PAREN:
                        depth += 1
                    elif tokens[m_close].token_type == TokenType.R_PAREN:
                        depth -= 1
                        if depth == 0:
                            break
                    m_close += 1
                if m_close >= len(tokens):
                    raise ValueError("Invalid AT (...) modifier: unclosed parenthesis")

                mod_start = tokens[m_open].end + 1
                mod_end = tokens[m_close].start
                modifiers.append(sql[mod_start:mod_end].strip())
                end_idx = m_close
                k = m_close + 1
            return modifiers, end_idx

        while i < len(tokens):
            token = tokens[i]
            if (
                token.text.upper() == "AGGREGATE"
                and i + 1 < len(tokens)
                and tokens[i + 1].token_type == TokenType.L_PAREN
            ):
                func_start = token.start
                if (
                    i >= 2
                    and tokens[i - 1].token_type == TokenType.DOT
                    and tokens[i - 2].token_type
                    in {
                        TokenType.VAR,
                        TokenType.SCHEMA,
                    }
                ):
                    # Support qualified function syntax like `schema.AGGREGATE(...)`.
                    # The qualifier is syntactic noise for Yardstick resolution.
                    func_start = tokens[i - 2].start
                j = i + 1
                depth = 0
                while j < len(tokens):
                    if tokens[j].token_type == TokenType.L_PAREN:
                        depth += 1
                    elif tokens[j].token_type == TokenType.R_PAREN:
                        depth -= 1
                        if depth == 0:
                            break
                    j += 1
                if j >= len(tokens):
                    raise ValueError("Invalid AGGREGATE() call: unclosed parenthesis")

                arg_start = tokens[i + 1].end + 1
                arg_end = tokens[j].start
                argument_sql = sql[arg_start:arg_end].strip()
                if not self._is_yardstick_aggregate_argument(argument_sql):
                    i = j + 1
                    continue

                modifiers, end_idx = parse_at_chain(j + 1, j)

                placeholder = f"__ysagg_{len(calls)}"
                calls.append(
                    _YardstickAggregateCall(
                        placeholder=placeholder,
                        argument_sql=argument_sql,
                        modifiers=modifiers,
                        include_visible_default=True,
                    )
                )

                segments.append(sql[cursor:func_start])
                segments.append(placeholder)
                cursor = tokens[end_idx].end + 1
                i = end_idx + 1
                continue

            # Support Yardstick's `measure AT (...)` syntax without AGGREGATE wrapper.
            # Examples: `avg_revenue AT (VISIBLE)`, `o.avg_revenue AT (WHERE ...)`.
            if token.token_type == TokenType.VAR:
                measure_start = None
                measure_end = None
                at_index = None
                if (
                    i + 2 < len(tokens)
                    and tokens[i + 1].text.upper() == "AT"
                    and tokens[i + 2].token_type == TokenType.L_PAREN
                ):
                    measure_start = i
                    measure_end = i
                    at_index = i + 1
                elif (
                    i + 4 < len(tokens)
                    and tokens[i + 1].token_type == TokenType.DOT
                    and tokens[i + 2].token_type == TokenType.VAR
                    and tokens[i + 3].text.upper() == "AT"
                    and tokens[i + 4].token_type == TokenType.L_PAREN
                ):
                    measure_start = i
                    measure_end = i + 2
                    at_index = i + 3

                if measure_start is not None and measure_end is not None and at_index is not None:
                    argument_sql = sql[tokens[measure_start].start : tokens[measure_end].end + 1].strip()
                    modifiers, end_idx = parse_at_chain(at_index, measure_end)
                    if modifiers:
                        placeholder = f"__ysagg_{len(calls)}"
                        calls.append(
                            _YardstickAggregateCall(
                                placeholder=placeholder,
                                argument_sql=argument_sql,
                                modifiers=modifiers,
                                include_visible_default=False,
                            )
                        )

                        segments.append(sql[cursor : tokens[measure_start].start])
                        segments.append(placeholder)
                        cursor = tokens[end_idx].end + 1
                        i = end_idx + 1
                        continue

            i += 1

        segments.append(sql[cursor:])
        return "".join(segments), calls

    def _is_yardstick_aggregate_argument(self, argument_sql: str) -> bool:
        try:
            argument = sqlglot.parse_one(argument_sql, dialect=self.dialect)
        except Exception:
            return False
        return isinstance(argument, exp.Column)

    def _extract_source_models_from_select(self, select: exp.Select) -> dict[str, str]:
        """Map SQL source aliases to semantic model names."""
        alias_to_model: dict[str, str] = {}

        def add_table(table_expr: exp.Expression | None) -> None:
            if not isinstance(table_expr, exp.Table):
                return
            model_name = table_expr.name
            if model_name not in self.graph.models:
                return
            alias = table_expr.alias_or_name
            alias_to_model[alias] = model_name

        from_clause = select.args.get("from_")
        if from_clause:
            add_table(from_clause.this)

        for join in select.args.get("joins") or []:
            add_table(join.this)

        return alias_to_model

    def _has_single_source_relation(self, select: exp.Select) -> bool:
        """Return True only when SELECT scope has exactly one FROM relation and no JOINs."""
        from_clause = select.args.get("from_")
        if not from_clause or from_clause.this is None:
            return False
        return len(select.args.get("joins") or []) == 0

    def _parse_relation_factor(self, relation_sql: str) -> exp.Expression:
        probe = sqlglot.parse_one(f"SELECT 1 FROM {relation_sql}", dialect=self.dialect)
        from_clause = probe.args.get("from_")
        if not from_clause:
            raise ValueError(f"Failed to parse relation: {relation_sql}")
        return from_clause.this

    def _rewrite_source_model_relations(self, select: exp.Select) -> exp.Select:
        """Replace semantic model names in FROM/JOIN with their physical source relation."""

        def replace_table(table_expr: exp.Expression | None) -> exp.Expression | None:
            if not isinstance(table_expr, exp.Table):
                return table_expr

            model_name = table_expr.name
            if model_name not in self.graph.models:
                return table_expr

            model = self.graph.get_model(model_name)
            alias = table_expr.alias_or_name
            if getattr(model, "has_untranslated_dax", False):
                raise ValueError(
                    f"Model '{model_name}' contains DAX table expression but has no SQL/table translation. "
                    "DAX table lowering is not available in this build."
                )
            if model.sql:
                return self._parse_relation_factor(f"({model.sql}) AS {alias}")
            if model.table:
                return self._parse_relation_factor(f"{model.table} AS {alias}")
            return self._parse_relation_factor(f"{model_name} AS {alias}")

        from_clause = select.args.get("from_")
        if from_clause:
            from_clause.set("this", replace_table(from_clause.this))

        for join in select.args.get("joins") or []:
            join.set("this", replace_table(join.this))

        return select

    def _expression_contains_yardstick_placeholder(
        self, expression: exp.Expression, placeholder_names: set[str]
    ) -> bool:
        for column in self._columns_without_nested_scopes(expression):
            if not column.table and column.name in placeholder_names:
                return True
        return False

    def _columns_without_nested_scopes(self, expression: exp.Expression) -> list[exp.Column]:
        columns: list[exp.Column] = []

        def visit(node: exp.Expression, is_root: bool = False) -> None:
            if isinstance(node, exp.Column):
                columns.append(node)
                return

            if not is_root and isinstance(node, (exp.Select, exp.Subquery)):
                return

            for arg in node.args.values():
                if isinstance(arg, exp.Expression):
                    visit(arg)
                elif isinstance(arg, list):
                    for item in arg:
                        if isinstance(item, exp.Expression):
                            visit(item)

        visit(expression, is_root=True)
        return columns

    def _qualify_unaliased_columns(self, expression: exp.Expression, table_alias: str) -> exp.Expression:
        qualified = expression.copy()
        for column in qualified.find_all(exp.Column):
            if not column.table:
                column.set("table", exp.to_identifier(table_alias))
        return qualified

    def _rewrite_tables(
        self,
        expression: exp.Expression,
        table_mapping: dict[str, str],
        default_table: str | None = None,
    ) -> exp.Expression:
        rewritten = expression.copy()
        for column in rewritten.find_all(exp.Column):
            if column.table:
                if column.table in table_mapping:
                    column.set("table", exp.to_identifier(table_mapping[column.table]))
            elif default_table:
                column.set("table", exp.to_identifier(default_table))
        return rewritten

    def _expr_signature_without_tables(self, expression: exp.Expression) -> str:
        normalized = expression.copy()
        for column in normalized.find_all(exp.Column):
            column.set("table", None)
        return normalized.sql(dialect=self.dialect).lower()

    def _resolve_implicit_yardstick_measure_reference(
        self,
        column: exp.Column,
        source_models: dict[str, str],
        default_alias: str | None,
        single_model_scope: bool,
        placeholder_names: set[str],
    ) -> str | None:
        if not column.name or column.name in placeholder_names:
            return None

        if column.table:
            if column.table not in source_models:
                return None
            model_name = source_models[column.table]
            if not self._is_yardstick_model(model_name):
                return None
            model = self.graph.get_model(model_name)
            if not model.get_metric(column.name):
                return None
            return f"{column.table}.{column.name}"

        if default_alias and single_model_scope:
            model_name = source_models[default_alias]
            if not self._is_yardstick_model(model_name):
                return None
            model = self.graph.get_model(model_name)
            if model.get_metric(column.name):
                return column.name
            return None

        candidate_aliases = [
            alias
            for alias, model_name in source_models.items()
            if self._is_yardstick_model(model_name)
            and self.graph.get_model(model_name).get_metric(column.name) is not None
        ]
        if len(candidate_aliases) != 1:
            return None
        return f"{candidate_aliases[0]}.{column.name}"

    def _inject_implicit_yardstick_measure_calls(
        self,
        select_scope: exp.Select,
        source_models: dict[str, str],
        default_alias: str | None,
        single_model_scope: bool,
        call_map: dict[str, _YardstickAggregateCall],
        placeholder_names: set[str],
    ) -> exp.Select:
        rewritten = select_scope.copy()

        def next_placeholder() -> str:
            idx = len(call_map)
            while True:
                placeholder = f"__ysagg_{idx}"
                if placeholder not in placeholder_names:
                    return placeholder
                idx += 1

        def register_implicit_measure_call(argument_sql: str) -> str:
            placeholder = next_placeholder()
            call_map[placeholder] = _YardstickAggregateCall(
                placeholder=placeholder,
                argument_sql=argument_sql,
                modifiers=[],
                include_visible_default=False,
            )
            placeholder_names.add(placeholder)
            return placeholder

        def maybe_replace_column(node: exp.Expression) -> exp.Expression:
            if not isinstance(node, exp.Column):
                return node

            argument_sql = self._resolve_implicit_yardstick_measure_reference(
                node,
                source_models=source_models,
                default_alias=default_alias,
                single_model_scope=single_model_scope,
                placeholder_names=placeholder_names,
            )
            if not argument_sql:
                return node

            placeholder = register_implicit_measure_call(argument_sql)
            return exp.column(placeholder)

        rewritten_projections: list[exp.Expression] = []
        for projection in rewritten.expressions:
            if isinstance(projection, exp.Alias):
                projection.set("this", projection.this.transform(maybe_replace_column))
                rewritten_projections.append(projection)
                continue

            if isinstance(projection, exp.Column):
                argument_sql = self._resolve_implicit_yardstick_measure_reference(
                    projection,
                    source_models=source_models,
                    default_alias=default_alias,
                    single_model_scope=single_model_scope,
                    placeholder_names=placeholder_names,
                )
                if argument_sql:
                    placeholder = register_implicit_measure_call(argument_sql)
                    rewritten_projections.append(exp.alias_(exp.column(placeholder), projection.name, quoted=False))
                    continue

            rewritten_projections.append(projection.transform(maybe_replace_column))
        rewritten.set("expressions", rewritten_projections)

        having_clause = rewritten.args.get("having")
        if having_clause:
            having_clause.set("this", having_clause.this.transform(maybe_replace_column))

        order_clause = rewritten.args.get("order")
        if order_clause:
            for order_expr in order_clause.expressions:
                order_expr.set("this", order_expr.this.transform(maybe_replace_column))

        return rewritten

    def _resolve_yardstick_dimension_expression(
        self,
        column: exp.Column,
        source_models: dict[str, str],
        default_alias: str | None,
        placeholder_names: set[str],
    ) -> tuple[exp.Expression, str] | None:
        if not column.name or column.name in placeholder_names:
            return None

        alias_candidates: list[str] = []
        if column.table:
            if column.table not in source_models:
                return None
            alias_candidates = [column.table]
        elif default_alias:
            alias_candidates = [default_alias]
        else:
            alias_candidates = [
                alias
                for alias, model_name in source_models.items()
                if self.graph.get_model(model_name).get_dimension(column.name) is not None
            ]
            if len(alias_candidates) != 1:
                return None

        table_alias = alias_candidates[0]
        model_name = source_models[table_alias]
        model = self.graph.get_model(model_name)
        dimension = model.get_dimension(column.name)
        if not dimension:
            return None

        dimension_sql = dimension.sql_expr.strip()

        # Replace {model} placeholder (used by LookML adapter) with table alias
        dimension_sql = dimension_sql.replace("{model}", table_alias)

        # If the resolved SQL is just the column name (possibly table-qualified),
        # no expansion is needed and the original column reference should be preserved
        if dimension_sql.lower() == column.name.lower():
            return None
        if dimension_sql.lower() == f"{table_alias}.{column.name}".lower():
            return None

        expr = sqlglot.parse_one(dimension_sql, dialect=self.dialect)
        expr = self._rewrite_tables(
            expr,
            table_mapping={model_name: table_alias},
            default_table=table_alias,
        )
        return expr, column.name

    def _expand_yardstick_dimension_references(
        self,
        select_scope: exp.Select,
        source_models: dict[str, str],
        default_alias: str | None,
        placeholder_names: set[str],
    ) -> exp.Select:
        def replace_columns(node: exp.Expression) -> exp.Expression:
            if not isinstance(node, exp.Column):
                return node

            resolved = self._resolve_yardstick_dimension_expression(
                node,
                source_models=source_models,
                default_alias=default_alias,
                placeholder_names=placeholder_names,
            )
            if not resolved:
                return node
            replacement, _ = resolved
            return replacement

        rewritten = select_scope.copy()
        rewritten_projections: list[exp.Expression] = []
        for projection in rewritten.expressions:
            if isinstance(projection, exp.Alias):
                projection.set("this", projection.this.transform(replace_columns))
                rewritten_projections.append(projection)
                continue

            if isinstance(projection, exp.Column):
                resolved = self._resolve_yardstick_dimension_expression(
                    projection,
                    source_models=source_models,
                    default_alias=default_alias,
                    placeholder_names=placeholder_names,
                )
                if resolved:
                    replacement, output_name = resolved
                    rewritten_projections.append(exp.alias_(replacement, output_name))
                    continue

            rewritten_projections.append(projection.transform(replace_columns))

        rewritten.set("expressions", rewritten_projections)

        group_clause = rewritten.args.get("group")
        if group_clause:
            group_clause.set("expressions", [expr.transform(replace_columns) for expr in group_clause.expressions])

        where_clause = rewritten.args.get("where")
        if where_clause:
            where_clause.set("this", where_clause.this.transform(replace_columns))

        having_clause = rewritten.args.get("having")
        if having_clause:
            having_clause.set("this", having_clause.this.transform(replace_columns))

        order_clause = rewritten.args.get("order")
        if order_clause:
            for order_expr in order_clause.expressions:
                order_expr.set("this", order_expr.this.transform(replace_columns))

        return rewritten

    def _resolve_yardstick_measure_call(self, argument_sql: str, source_models: dict[str, str]) -> tuple[str, str, str]:
        """Resolve AGGREGATE(argument) to (model_alias, model_name, measure_name)."""
        try:
            arg_expr = sqlglot.parse_one(argument_sql, dialect=self.dialect)
        except Exception as e:
            raise ValueError(f"Invalid AGGREGATE argument '{argument_sql}': {e}") from e

        if not isinstance(arg_expr, exp.Column):
            raise ValueError(f"AGGREGATE argument must be a metric/measure reference, got: {argument_sql}")

        measure_name = arg_expr.name
        source_alias = arg_expr.table

        if source_alias:
            if source_alias in source_models:
                model_name = source_models[source_alias]
                model_alias = source_alias
            elif source_alias in self.graph.models:
                model_name = source_alias
                aliases = [alias for alias, model in source_models.items() if model == model_name]
                if not aliases:
                    raise ValueError(f"Model '{model_name}' is not present in query FROM/JOIN")
                model_alias = aliases[0]
            else:
                raise ValueError(f"Unknown table/model alias '{source_alias}' in AGGREGATE({argument_sql})")

            model = self.graph.get_model(model_name)
            if not model.get_metric(measure_name):
                raise ValueError(f"Measure '{measure_name}' not found in model '{model_name}'")
            return model_alias, model_name, measure_name

        candidates: list[tuple[str, str]] = []
        for alias, model_name in source_models.items():
            model = self.graph.get_model(model_name)
            if model.get_metric(measure_name):
                candidates.append((alias, model_name))

        if not candidates:
            raise ValueError(f"Could not resolve AGGREGATE({measure_name}) to any model in query scope")
        if len(candidates) > 1:
            candidates_str = ", ".join(f"{alias}.{measure_name}" for alias, _ in candidates)
            raise ValueError(f"Ambiguous AGGREGATE({measure_name}); use a qualifier: {candidates_str}")

        model_alias, model_name = candidates[0]
        return model_alias, model_name, measure_name

    def _build_yardstick_call_sql(
        self,
        call: _YardstickAggregateCall,
        source_models: dict[str, str],
        group_expressions: list[exp.Expression],
        projection_aliases: dict[str, str],
        outer_where: exp.Where | None,
        default_alias: str | None,
        single_model_scope: bool,
    ) -> str:
        model_alias, model_name, measure_name = self._resolve_yardstick_measure_call(call.argument_sql, source_models)
        return self._build_yardstick_measure_sql(
            model_alias=model_alias,
            model_name=model_name,
            measure_name=measure_name,
            modifiers=call.modifiers,
            include_visible_default=call.include_visible_default,
            source_models=source_models,
            group_expressions=group_expressions,
            projection_aliases=projection_aliases,
            outer_where=outer_where,
            default_alias=default_alias,
            single_model_scope=single_model_scope,
            visiting=set(),
        )

    def _build_yardstick_measure_sql(
        self,
        model_alias: str,
        model_name: str,
        measure_name: str,
        modifiers: list[str],
        include_visible_default: bool,
        source_models: dict[str, str],
        group_expressions: list[exp.Expression],
        projection_aliases: dict[str, str],
        outer_where: exp.Where | None,
        default_alias: str | None,
        single_model_scope: bool,
        visiting: set[tuple[str, str]],
    ) -> str:
        model = self.graph.get_model(model_name)
        measure = model.get_metric(measure_name)
        if not measure:
            raise ValueError(f"Measure '{measure_name}' not found in model '{model_name}'")

        visit_key = (model_name, measure_name)
        if visit_key in visiting:
            raise ValueError(f"Circular derived measure reference detected for '{model_name}.{measure_name}'")

        is_derived_formula = measure.type == "derived" or (
            not measure.type and not measure.agg and measure.sql and not sql_has_aggregate(measure.sql, self.dialect)
        )
        if is_derived_formula and measure.sql:
            visiting.add(visit_key)
            # Replace {model} placeholder (used by LookML adapter) with model alias
            _formula_sql = measure.sql.replace("{model}", model_alias)
            formula_expr = sqlglot.parse_one(_formula_sql, dialect=self.dialect)

            def replace_measure_refs(node: exp.Expression) -> exp.Expression:
                if not isinstance(node, exp.Column):
                    return node
                if node.table and node.table not in {model_alias, model_name}:
                    return node
                ref_name = node.name
                if ref_name == measure_name:
                    return node
                if not model.get_metric(ref_name):
                    return node
                dep_sql = self._build_yardstick_measure_sql(
                    model_alias=model_alias,
                    model_name=model_name,
                    measure_name=ref_name,
                    modifiers=modifiers,
                    include_visible_default=include_visible_default,
                    source_models=source_models,
                    group_expressions=group_expressions,
                    projection_aliases=projection_aliases,
                    outer_where=outer_where,
                    default_alias=default_alias,
                    single_model_scope=single_model_scope,
                    visiting=visiting.copy(),
                )
                return sqlglot.parse_one(dep_sql, dialect=self.dialect)

            rewritten_formula = formula_expr.transform(replace_measure_refs)
            return f"({rewritten_formula.sql(dialect=self.dialect)})"

        agg_expr = self._build_yardstick_aggregation_expr(
            measure=measure,
            model_alias=model_alias,
            model_name=model_name,
        )

        context_dimensions = self._build_yardstick_context_dimensions(
            group_expressions=group_expressions,
            model_alias=model_alias,
            model_name=model_name,
            default_alias=default_alias,
            source_models=source_models,
            projection_aliases=projection_aliases,
            single_model_scope=single_model_scope,
        )
        (
            active_dimensions,
            where_modifier_predicates,
            set_modifier_predicates,
            include_visible,
        ) = self._apply_yardstick_modifiers(
            modifiers=modifiers,
            context_dimensions=context_dimensions,
            model_alias=model_alias,
            model_name=model_name,
            default_alias=default_alias,
            single_model=single_model_scope,
            include_visible_default=include_visible_default,
            fixed_context_signatures=self._extract_fixed_context_signatures_from_where(
                outer_where=outer_where,
                default_alias=default_alias,
                single_model=single_model_scope,
                model_alias=model_alias,
                model_name=model_name,
            ),
        )

        correlation_predicates: list[str] = []
        for dim in active_dimensions:
            correlation_predicates.append(f"({dim['inner_sql']}) IS NOT DISTINCT FROM ({dim['outer_sql']})")

        base_predicates = list(where_modifier_predicates)
        if include_visible and outer_where is not None:
            visible_expr = outer_where.this.copy()
            if default_alias and single_model_scope:
                visible_expr = self._qualify_unaliased_columns(visible_expr, default_alias)
            visible_expr = self._rewrite_tables(
                visible_expr,
                table_mapping={model_alias: "_inner", model_name: "_inner"},
                default_table="_inner" if single_model_scope else None,
            )
            base_predicates.append(visible_expr.sql(dialect=self.dialect))

        for measure_filter in measure.filters or []:
            # Replace {model} placeholder (used by LookML adapter) with inner alias
            _filter_sql = measure_filter.replace("{model}", "_inner")
            filter_expr = sqlglot.parse_one(_filter_sql, dialect=self.dialect)
            if default_alias and single_model_scope:
                filter_expr = self._qualify_unaliased_columns(filter_expr, default_alias)
            filter_expr = self._rewrite_tables(
                filter_expr,
                table_mapping={model_alias: "_inner", model_name: "_inner"},
                default_table="_inner" if single_model_scope else None,
            )
            base_predicates.append(filter_expr.sql(dialect=self.dialect))

        predicates = list(base_predicates) + list(set_modifier_predicates) + list(correlation_predicates)
        where_clause = f" WHERE {' AND '.join(predicates)}" if predicates else ""
        source_sql = f"({model.sql})" if model.sql else model.table

        if measure.sql and not measure.agg and self._is_window_measure_expression(measure.sql):
            pre_where_clause = f" WHERE {' AND '.join(base_predicates)}" if base_predicates else ""
            post_predicates = list(set_modifier_predicates) + list(correlation_predicates)
            post_where_clause = f" WHERE {' AND '.join(post_predicates)}" if post_predicates else ""
            window_value_subquery = (
                f"SELECT _inner.*, {agg_expr} AS __ys_window_value FROM {source_sql} AS _inner{pre_where_clause}"
            )
            return (
                "(SELECT CASE "
                "WHEN COUNT(*) = 0 THEN NULL "
                "WHEN COUNT(DISTINCT __ys_window_value) = 1 THEN MIN(__ys_window_value) "
                f"ELSE error('Window measure {measure_name} returned multiple values for the evaluation context') END "
                f"FROM ({window_value_subquery}) AS _inner{post_where_clause})"
            )

        return f"(SELECT {agg_expr} FROM {source_sql} AS _inner{where_clause})"

    def _build_yardstick_aggregation_expr(self, measure, model_alias: str, model_name: str) -> str:
        if measure.agg:
            agg = measure.agg.lower()
            if agg == "count":
                if not measure.sql or measure.sql == "*":
                    return "COUNT(*)"
                count_expr = self._rewrite_yardstick_measure_expression(
                    measure.sql, model_alias=model_alias, model_name=model_name, target_alias="_inner"
                )
                return f"COUNT({count_expr})"

            expr_sql = self._rewrite_yardstick_measure_expression(
                measure.sql_expr, model_alias=model_alias, model_name=model_name, target_alias="_inner"
            )

            if agg == "count_distinct":
                return f"COUNT(DISTINCT {expr_sql})"

            agg_map = {
                "sum": "SUM",
                "avg": "AVG",
                "min": "MIN",
                "max": "MAX",
                "median": "MEDIAN",
                "stddev": "STDDEV",
                "stddev_pop": "STDDEV_POP",
                "variance": "VARIANCE",
                "variance_pop": "VAR_POP",
            }
            if agg not in agg_map:
                raise ValueError(f"Unsupported Yardstick aggregation '{measure.agg}'")
            return f"{agg_map[agg]}({expr_sql})"

        if measure.sql:
            return self._rewrite_yardstick_measure_expression(
                measure.sql, model_alias=model_alias, model_name=model_name, target_alias="_inner"
            )

        raise ValueError(f"Measure '{measure.name}' cannot be aggregated")

    def _rewrite_yardstick_measure_expression(
        self, sql_expr: str, model_alias: str, model_name: str, target_alias: str
    ) -> str:
        # Replace {model} placeholder (used by LookML adapter) with target alias
        sql_expr = sql_expr.replace("{model}", target_alias)
        parsed = sqlglot.parse_one(sql_expr, dialect=self.dialect)
        parsed = self._rewrite_tables(
            parsed,
            table_mapping={
                model_alias: target_alias,
                model_name: target_alias,
                f"{model_name}_cte": target_alias,
            },
            default_table=target_alias,
        )
        return parsed.sql(dialect=self.dialect)

    def _is_window_measure_expression(self, sql_expr: str) -> bool:
        # Strip {model} placeholder to avoid parse errors
        parsed = sqlglot.parse_one(sql_expr.replace("{model}", "__model"), dialect=self.dialect)
        return any(isinstance(node, exp.Window) for node in parsed.walk())

    def _build_yardstick_context_dimensions(
        self,
        group_expressions: list[exp.Expression],
        model_alias: str,
        model_name: str,
        default_alias: str | None,
        source_models: dict[str, str],
        projection_aliases: dict[str, str],
        single_model_scope: bool,
    ) -> list[dict[str, str]]:
        context_dimensions: list[dict[str, str]] = []
        model = self.graph.get_model(model_name)
        seen_signatures: set[str] = set()

        for raw_group_expr in group_expressions:
            expanded_group_expressions = self._expand_group_expression_for_yardstick_context(raw_group_expr)
            for group_expr in expanded_group_expressions:
                outer_base_expr = group_expr.copy()
                expr_obj = group_expr.copy()
                if default_alias and single_model_scope:
                    expr_obj = self._qualify_unaliased_columns(expr_obj, default_alias)

                columns = list(expr_obj.find_all(exp.Column))
                if not columns:
                    continue

                inner_base_expr = expr_obj.copy()
                tables = {column.table for column in columns if column.table}
                if tables and not tables.issubset({model_alias, model_name}):
                    # Multi-fact join case: if grouped by another alias's same-named dimensions,
                    # map those columns onto this model alias so each measure correlates correctly.
                    if not all(model.get_dimension(column.name) for column in columns):
                        continue
                    for column in inner_base_expr.find_all(exp.Column):
                        column.set("table", exp.to_identifier(model_alias))
                if not tables and not single_model_scope:
                    continue

                outer_expr = self._rewrite_tables(outer_base_expr, table_mapping={model_name: model_alias})
                inner_expr = self._rewrite_tables(
                    inner_base_expr,
                    table_mapping={model_alias: "_inner", model_name: "_inner"},
                    default_table="_inner" if single_model_scope else None,
                )
                signature = self._expr_signature_without_tables(inner_base_expr)
                if signature in seen_signatures:
                    continue
                seen_signatures.add(signature)
                outer_sql = outer_expr.sql(dialect=self.dialect)
                projection_alias = projection_aliases.get(signature)
                unsafe_aliases: set[str] = set()
                for dimension in model.dimensions:
                    dim_expr = dimension.sql_expr
                    try:
                        parsed_dim = sqlglot.parse_one(dim_expr, dialect=self.dialect)
                        if isinstance(parsed_dim, exp.Column) and parsed_dim.name.lower() == dimension.name.lower():
                            unsafe_aliases.add(dimension.name.lower())
                    except Exception:
                        if dim_expr.strip().lower() == dimension.name.lower():
                            unsafe_aliases.add(dimension.name.lower())

                if projection_alias and projection_alias.lower() not in unsafe_aliases:
                    outer_sql = exp.to_identifier(projection_alias).sql(dialect=self.dialect)

                context_dimensions.append(
                    {
                        "signature": signature,
                        "outer_sql": outer_sql,
                        "inner_sql": inner_expr.sql(dialect=self.dialect),
                    }
                )

        return context_dimensions

    def _expand_group_expression_for_yardstick_context(self, group_expr: exp.Expression) -> list[exp.Expression]:
        if isinstance(group_expr, (exp.Rollup, exp.Cube)):
            expanded: list[exp.Expression] = []
            for sub_expr in group_expr.expressions:
                expanded.extend(self._expand_group_expression_for_yardstick_context(sub_expr))
            return expanded

        if isinstance(group_expr, exp.GroupingSets):
            expanded: list[exp.Expression] = []
            for sub_expr in group_expr.expressions:
                expanded.extend(self._expand_group_expression_for_yardstick_context(sub_expr))
            return expanded

        if isinstance(group_expr, exp.Tuple):
            expanded: list[exp.Expression] = []
            for sub_expr in group_expr.expressions:
                expanded.extend(self._expand_group_expression_for_yardstick_context(sub_expr))
            return expanded

        return [group_expr]

    def _split_set_modifier(self, modifier_sql: str) -> tuple[str, str]:
        tokens = sqlglot.tokenize(modifier_sql, read=self.dialect)
        if not tokens or tokens[0].text.upper() != "SET":
            raise ValueError(f"Invalid SET modifier: {modifier_sql}")

        depth = 0
        eq_token = None
        for token in tokens[1:]:
            if token.token_type == TokenType.L_PAREN:
                depth += 1
            elif token.token_type == TokenType.R_PAREN:
                depth -= 1
            elif token.token_type == TokenType.EQ and depth == 0:
                eq_token = token
                break

        if not eq_token:
            raise ValueError(f"SET modifier must contain '=': {modifier_sql}")

        left_sql = modifier_sql[tokens[0].end + 1 : eq_token.start].strip()
        right_sql = modifier_sql[eq_token.end + 1 :].strip()
        return left_sql, right_sql

    def _split_compound_yardstick_modifier(self, modifier_sql: str) -> list[str]:
        tokens = sqlglot.tokenize(modifier_sql, read=self.dialect)
        if not tokens:
            return []

        modifier_heads = {"ALL", "SET", "WHERE", "VISIBLE"}
        split_indexes = [0]
        depth = 0

        for idx, token in enumerate(tokens):
            if token.token_type == TokenType.L_PAREN:
                depth += 1
            elif token.token_type == TokenType.R_PAREN:
                depth -= 1

            if idx == 0 or depth != 0:
                continue

            if token.text.upper() in modifier_heads:
                split_indexes.append(idx)

        parts: list[str] = []
        for i, start_idx in enumerate(split_indexes):
            end_idx = split_indexes[i + 1] - 1 if i + 1 < len(split_indexes) else len(tokens) - 1
            start_pos = tokens[start_idx].start
            end_pos = tokens[end_idx].end + 1
            part = modifier_sql[start_pos:end_pos].strip()
            if part:
                parts.append(part)

        return parts

    def _split_all_modifier_targets(self, modifier_sql: str) -> list[str]:
        """Split `ALL` modifier targets, supporting single-clause multi-dimension syntax."""
        tokens = sqlglot.tokenize(modifier_sql, read=self.dialect)
        if not tokens or tokens[0].text.upper() != "ALL":
            return []
        if len(tokens) == 1:
            return []

        raw_targets = modifier_sql[tokens[1].start :].strip()

        # For complex arithmetic/boolean expressions, keep legacy single-expression behavior.
        unsafe_top_level_ops = {"+", "-", "*", "/", "%", "AND", "OR", "=", "<>", "!=", "<", ">", "<=", ">="}
        depth = 0
        for token in tokens[1:]:
            if token.token_type == TokenType.L_PAREN:
                depth += 1
                continue
            if token.token_type == TokenType.R_PAREN:
                depth -= 1
                continue
            if depth == 0 and token.text.upper() in unsafe_top_level_ops:
                return [raw_targets]

        targets: list[str] = []
        idx = 1
        while idx < len(tokens):
            if tokens[idx].token_type == TokenType.COMMA:
                idx += 1
                continue

            start_idx = idx
            if idx + 1 < len(tokens) and tokens[idx + 1].token_type == TokenType.L_PAREN:
                # Function-style dimension (e.g., MONTH(order_date)).
                depth = 0
                idx += 1
                while idx < len(tokens):
                    if tokens[idx].token_type == TokenType.L_PAREN:
                        depth += 1
                    elif tokens[idx].token_type == TokenType.R_PAREN:
                        depth -= 1
                        if depth == 0:
                            idx += 1
                            break
                    idx += 1
                if depth != 0:
                    return [raw_targets]
            else:
                # Identifier-style dimension, optionally qualified (table.column).
                idx += 1
                while idx + 1 < len(tokens) and tokens[idx].token_type == TokenType.DOT:
                    idx += 2

            end_idx = idx - 1
            target_sql = modifier_sql[tokens[start_idx].start : tokens[end_idx].end + 1].strip()
            if target_sql:
                targets.append(target_sql)

            if idx < len(tokens) and tokens[idx].token_type != TokenType.COMMA:
                next_text = tokens[idx].text
                # Allow space-separated target lists when next token looks like an expression start.
                if not (next_text[:1].isalpha() or next_text[:1] in {"_", '"', "`"}):
                    return [raw_targets]

        return targets or [raw_targets]

    def _extract_fixed_context_signatures_from_where(
        self,
        outer_where: exp.Where | None,
        default_alias: str | None,
        single_model: bool,
        model_alias: str,
        model_name: str,
    ) -> set[str]:
        """Extract dimensions fixed to a literal by conjunctive outer WHERE predicates."""
        if outer_where is None:
            return set()

        where_expr = outer_where.this.copy()
        if default_alias and single_model:
            where_expr = self._qualify_unaliased_columns(where_expr, default_alias)
        where_expr = self._rewrite_tables(where_expr, table_mapping={model_name: model_alias})

        signatures: set[str] = set()
        stack: list[exp.Expression] = [where_expr]
        while stack:
            condition = stack.pop()
            if isinstance(condition, exp.And):
                stack.append(condition.this)
                stack.append(condition.expression)
                continue
            if not isinstance(condition, exp.EQ):
                continue
            left = condition.this
            right = condition.expression
            if isinstance(right, exp.Literal):
                signatures.add(self._expr_signature_without_tables(left))
            elif isinstance(left, exp.Literal):
                signatures.add(self._expr_signature_without_tables(right))

        return signatures

    def _rewrite_current_keyword(
        self,
        sql_expr: str,
        context_dimensions: list[dict[str, str]],
        fixed_context_signatures: set[str] | None = None,
    ) -> str:
        """Rewrite CURRENT references to context-aware expressions.

        CURRENT <dim> resolves to <dim> only when that dimension is present in the
        current context. Otherwise it resolves to NULL (ambiguous context).
        """
        tokens = sqlglot.tokenize(sql_expr, read=self.dialect)
        if not tokens:
            return sql_expr

        context_signatures = {dim["signature"] for dim in context_dimensions}
        if fixed_context_signatures:
            context_signatures |= fixed_context_signatures
        parts: list[str] = []
        i = 0
        while i < len(tokens):
            token = tokens[i]
            if token.text.upper() != "CURRENT":
                parts.append(token.text)
                i += 1
                continue

            if i + 1 >= len(tokens):
                i += 1
                continue

            start_idx = i + 1
            end_idx = start_idx

            if start_idx + 1 < len(tokens) and tokens[start_idx + 1].token_type == TokenType.L_PAREN:
                depth = 0
                j = start_idx + 1
                while j < len(tokens):
                    if tokens[j].token_type == TokenType.L_PAREN:
                        depth += 1
                    elif tokens[j].token_type == TokenType.R_PAREN:
                        depth -= 1
                        if depth == 0:
                            end_idx = j
                            break
                    j += 1
            else:
                end_idx = start_idx
                while end_idx + 2 < len(tokens) and tokens[end_idx + 1].token_type == TokenType.DOT:
                    end_idx += 2

            target_sql = sql_expr[tokens[start_idx].start : tokens[end_idx].end + 1].strip()

            replacement = "NULL"
            try:
                target_expr = sqlglot.parse_one(target_sql, dialect=self.dialect)
                signature = self._expr_signature_without_tables(target_expr)
                if signature in context_signatures:
                    replacement = target_sql
            except Exception:
                replacement = "NULL"

            parts.append(replacement)
            i = end_idx + 1

        return " ".join(parts)

    def _apply_yardstick_modifiers(
        self,
        modifiers: list[str],
        context_dimensions: list[dict[str, str]],
        model_alias: str,
        model_name: str,
        default_alias: str | None,
        single_model: bool,
        include_visible_default: bool,
        fixed_context_signatures: set[str] | None = None,
    ) -> tuple[list[dict[str, str]], list[str], list[str], bool]:
        expanded_modifiers: list[str] = []
        for modifier in modifiers:
            expanded_modifiers.extend(self._split_compound_yardstick_modifier(modifier))

        active_dimensions = list(context_dimensions)
        where_predicates: list[str] = []
        set_predicates: dict[str, str] = {}
        include_visible = include_visible_default
        has_set = False
        has_all_global = False
        removed_signatures: set[str] = set()
        single_where_modifier = False

        for modifier in expanded_modifiers:
            tokens = sqlglot.tokenize(modifier, read=self.dialect)
            if not tokens:
                continue
            if tokens[0].text.upper() == "SET":
                has_set = True
        if has_set:
            include_visible = False
        if len(expanded_modifiers) == 1:
            tokens = sqlglot.tokenize(expanded_modifiers[0], read=self.dialect)
            single_where_modifier = bool(tokens and tokens[0].text.upper() == "WHERE")

        for modifier in reversed(expanded_modifiers):
            tokens = sqlglot.tokenize(modifier, read=self.dialect)
            if not tokens:
                continue

            modifier_type = tokens[0].text.upper()

            if modifier_type == "ALL":
                if len(tokens) == 1:
                    active_dimensions = []
                    set_predicates.clear()
                    where_predicates.clear()
                    include_visible = False
                    has_all_global = True
                    continue

                if has_all_global:
                    continue

                for target_sql in self._split_all_modifier_targets(modifier):
                    target_expr = sqlglot.parse_one(target_sql, dialect=self.dialect)
                    if default_alias and single_model:
                        target_expr = self._qualify_unaliased_columns(target_expr, default_alias)
                    target_signature = self._expr_signature_without_tables(target_expr)
                    active_dimensions = [d for d in active_dimensions if d["signature"] != target_signature]
                    removed_signatures.add(target_signature)
                    set_predicates.pop(target_signature, None)
                continue

            if modifier_type == "WHERE":
                if has_all_global:
                    continue
                where_sql = modifier[tokens[0].end + 1 :].strip()
                where_expr = sqlglot.parse_one(where_sql, dialect=self.dialect)
                # In AT(WHERE ...), unqualified columns belong to the inner evaluation context.
                # Keep explicitly-qualified outer aliases untouched so predicates can correlate
                # (e.g. `prod_name = o.prod_name` from paper listing-style queries).
                where_expr = self._rewrite_tables(
                    where_expr,
                    table_mapping={model_name: "_inner"},
                    default_table="_inner" if single_model else None,
                )
                where_predicates.append(where_expr.sql(dialect=self.dialect))
                # Single WHERE modifier evaluates in a non-correlated context.
                if single_where_modifier:
                    active_dimensions = []
                continue

            if modifier_type == "SET":
                if has_all_global:
                    continue
                try:
                    left_sql, right_sql = self._split_set_modifier(modifier)
                except ValueError:
                    # Support Yardstick predicate-style SET forms like:
                    # AT (SET region IN ('North', 'South'))
                    set_predicate_sql = modifier[tokens[0].end + 1 :].strip()
                    set_predicate_expr = sqlglot.parse_one(set_predicate_sql, dialect=self.dialect)

                    if default_alias and single_model:
                        set_predicate_expr = self._qualify_unaliased_columns(set_predicate_expr, default_alias)

                    target_signatures: list[str] = []
                    if isinstance(set_predicate_expr, exp.In):
                        target_signatures.append(self._expr_signature_without_tables(set_predicate_expr.this))
                    else:
                        raise ValueError(f"Unsupported SET modifier: {modifier}")

                    if any(signature in removed_signatures for signature in target_signatures):
                        continue

                    for signature in target_signatures:
                        active_dimensions = [d for d in active_dimensions if d["signature"] != signature]
                        set_predicates.pop(signature, None)

                    set_inner_predicate = self._rewrite_tables(
                        set_predicate_expr,
                        table_mapping={model_name: "_inner"},
                        default_table="_inner" if single_model else None,
                    )
                    where_predicates.append(set_inner_predicate.sql(dialect=self.dialect))
                    continue

                left_expr = sqlglot.parse_one(left_sql, dialect=self.dialect)
                right_expr = sqlglot.parse_one(
                    self._rewrite_current_keyword(
                        right_sql,
                        context_dimensions,
                        fixed_context_signatures=fixed_context_signatures,
                    ),
                    dialect=self.dialect,
                )

                if default_alias and single_model:
                    left_expr = self._qualify_unaliased_columns(left_expr, default_alias)
                    right_expr = self._qualify_unaliased_columns(right_expr, default_alias)

                left_signature = self._expr_signature_without_tables(left_expr)
                active_dimensions = [d for d in active_dimensions if d["signature"] != left_signature]
                if left_signature in removed_signatures:
                    continue

                left_inner = self._rewrite_tables(
                    left_expr,
                    table_mapping={model_alias: "_inner", model_name: "_inner"},
                    default_table="_inner" if single_model else None,
                )
                right_outer = self._rewrite_tables(
                    right_expr,
                    table_mapping={model_name: model_alias},
                )
                set_predicates[left_signature] = (
                    f"({left_inner.sql(dialect=self.dialect)}) IS NOT DISTINCT FROM "
                    f"({right_outer.sql(dialect=self.dialect)})"
                )
                continue

            if modifier_type == "VISIBLE":
                if has_all_global or has_set:
                    continue
                include_visible = True
                continue

            raise ValueError(f"Unsupported AT modifier: {modifier}")

        return active_dimensions, where_predicates, list(set_predicates.values()), include_visible

    def _has_subquery_in_from(self, select: exp.Select) -> bool:
        """Check if FROM clause contains a subquery."""
        from_clause = select.args.get("from_")
        if not from_clause:
            return False

        return isinstance(from_clause.this, exp.Subquery)

    def _rewrite_with_ctes_or_subqueries(self, parsed: exp.Select) -> str:
        rewritten_sql, _semantic_islands, _rejected_rules, _warnings = (
            self._rewrite_with_ctes_or_subqueries_and_islands(parsed)
        )
        return rewritten_sql

    def _rewrite_with_ctes_or_subqueries_and_islands(
        self, parsed: exp.Select
    ) -> tuple[str, list[_SemanticIslandRewrite], dict[str, str], list[str]]:
        """Rewrite query that contains CTEs or subqueries.

        Recursively walks the query tree bottom-up, rewriting any
        SELECT whose FROM target resolves to a semantic model.
        Outer queries are left as plain SQL, so post-processing
        (CASE, window functions, arithmetic, etc.) works naturally.
        """
        optimization, _rejected_rules = self._optimize_wrapped_semantic_query(parsed)
        if optimization is not None:
            return self._generate_from_plan(optimization.plan), [], _rejected_rules, []

        semantic_islands, rejected_rules, warnings = self._rewrite_select_tree(parsed)

        # If the root SELECT itself references a semantic model, it must
        # still go through _rewrite_simple_query (which enforces the
        # explicit JOIN guard and performs semantic rewriting).
        if self._references_semantic_model(parsed):
            # Save user-defined CTEs before _rewrite_simple_query replaces
            # the entire query with fresh generator output.
            original_with = parsed.args.get("with_")

            rewritten_sql = self._rewrite_simple_query(parsed)

            if original_with:
                # Merge user CTEs into the generated SQL so references
                # from filters/expressions (e.g. IN (SELECT ... FROM cte))
                # remain valid.
                rewritten = sqlglot.parse_one(rewritten_sql, dialect=self.dialect)
                gen_with = rewritten.args.get("with_")
                if gen_with:
                    # Check for CTE name collisions between user and generated CTEs
                    user_names = {cte.alias for cte in original_with.expressions}
                    for gen_cte in gen_with.expressions:
                        if gen_cte.alias in user_names:
                            raise ValueError(
                                f"CTE name '{gen_cte.alias}' conflicts with an internally "
                                f"generated name. Please choose a different CTE name."
                            )

                    user_ctes = [cte.copy() for cte in original_with.expressions]
                    gen_with.set("expressions", user_ctes + list(gen_with.expressions))
                    # Preserve WITH RECURSIVE from the original query
                    if original_with.args.get("recursive"):
                        gen_with.set("recursive", True)
                else:
                    rewritten.set("with_", original_with.copy())
                return rewritten.sql(dialect=self.dialect), semantic_islands, rejected_rules, warnings

            return rewritten_sql, semantic_islands, rejected_rules, warnings

        return parsed.sql(dialect=self.dialect), semantic_islands, rejected_rules, warnings

    def _rewrite_set_operation(self, parsed: exp.SetOperation) -> str:
        rewritten_sql, _semantic_islands, _rejected_rules, _warnings = self._rewrite_set_operation_with_islands(parsed)
        return rewritten_sql

    def _rewrite_set_operation_with_islands(
        self, parsed: exp.SetOperation
    ) -> tuple[str, list[_SemanticIslandRewrite], dict[str, str], list[str]]:
        semantic_islands, rejected_rules, warnings = self._rewrite_set_operation_tree(parsed)
        return parsed.sql(dialect=self.dialect), semantic_islands, rejected_rules, warnings

    def _select_tree_references_semantic_model(self, select: exp.Select) -> bool:
        if self._references_semantic_model(select):
            return True

        for nested_select in select.find_all(exp.Select):
            if nested_select is select:
                continue
            if self._references_semantic_model(nested_select):
                return True

        return False

    def _expression_tree_references_semantic_model(self, expression: exp.Expression) -> bool:
        for nested_select in expression.find_all(exp.Select):
            if self._references_semantic_model(nested_select):
                return True
        return False

    def _raise_on_user_cte_name_collision(self, expression: exp.Expression) -> None:
        with_clause = expression.args.get("with_")
        if not with_clause:
            return

        reserved_names = self._generated_cte_names_for_select_tree(expression)
        for cte in with_clause.expressions:
            if cte.alias in reserved_names:
                raise ValueError(
                    f"CTE name '{cte.alias}' conflicts with an internally "
                    f"generated name. Please choose a different CTE name."
                )

    def _generated_cte_names_for_select_tree(self, expression: exp.Expression) -> set[str]:
        reserved_names: set[str] = set()
        for nested_select in expression.find_all(exp.Select):
            if not self._references_semantic_model(nested_select):
                continue
            reserved_names.update(self._generated_cte_names_for_semantic_select(nested_select))
        return reserved_names

    def _generated_cte_names_for_semantic_select(self, select: exp.Select) -> set[str]:
        had_inferred_table = hasattr(self, "inferred_table")
        previous_inferred_table = getattr(self, "inferred_table", None)
        had_table_aliases = hasattr(self, "table_aliases")
        previous_table_aliases = getattr(self, "table_aliases", None)
        try:
            self.inferred_table = self._extract_from_table(select)
            self.table_aliases = self._source_aliases(select)
            metrics, dimensions, _aliases = self._extract_metrics_and_dimensions(select)
            filters = self._extract_filters(select)
            model_names = self.generator._find_required_models(metrics, dimensions, filters)
            return {
                self.generator._cte_name(model_name) for model_name in model_names if model_name in self.graph.models
            }
        except Exception:
            return set()
        finally:
            if had_inferred_table:
                self.inferred_table = previous_inferred_table
            elif hasattr(self, "inferred_table"):
                del self.inferred_table
            if had_table_aliases:
                self.table_aliases = previous_table_aliases
            elif hasattr(self, "table_aliases"):
                del self.table_aliases

    def _rewrite_with_rust(self, sql: str, strict: bool = True) -> str | None:
        """Rewrite using sidemantic-rs bindings, returning None to allow Python fallback."""
        if not self._rust_module:
            if strict and self._rust_no_fallback:
                raise ValueError("Rust rewriter backend is not initialized")
            return None

        try:
            models_yaml = self._rust_models_yaml
            if models_yaml is None:
                models_yaml = graph_to_rust_yaml(self.graph)
                self._rust_models_yaml = models_yaml
            return self._rust_module.rewrite_with_yaml(models_yaml, sql)
        except Exception as e:
            if self._rust_no_fallback:
                raise ValueError(f"Rust rewriter failed: {e}") from e
            return None

    def _prepare_sql_for_rust(self, parsed: exp.Select, original_sql: str) -> str:
        """Normalize Python-only graph metric shorthand to SQL sidemantic-rs can rewrite."""
        if self._extract_from_table(parsed) != "metrics":
            return original_sql

        changed = False
        rewritten_projections: list[exp.Expression] = []

        for projection in parsed.expressions:
            alias_name = projection.alias_or_name if isinstance(projection, exp.Alias) else None
            node = projection.this if isinstance(projection, exp.Alias) else projection

            if isinstance(node, exp.Column) and not node.table and node.name in self.graph.metrics:
                graph_metric = self.graph.metrics[node.name]
                if graph_metric.sql:
                    try:
                        metric_expr = sqlglot.parse_one(graph_metric.sql, dialect=self.dialect)
                    except Exception:
                        return original_sql
                    rewritten_projections.append(exp.alias_(metric_expr, alias_name or node.name, copy=False))
                    changed = True
                    continue

            rewritten_projections.append(projection)

        if not changed:
            return original_sql

        rewritten = parsed.copy()
        rewritten.set("expressions", rewritten_projections)
        return rewritten.sql(dialect=self.dialect)

    def _rewrite_select_tree(
        self, select: exp.Select
    ) -> tuple[list[_SemanticIslandRewrite], dict[str, str], list[str]]:
        """Recursively rewrite semantic subqueries and CTEs (bottom-up).

        At each level: recurse into children first, then rewrite this
        node if it directly references a semantic model.
        """
        semantic_islands: list[_SemanticIslandRewrite] = []
        rejected_rules: dict[str, str] = {}
        warnings: list[str] = []

        # Recurse into CTEs
        if select.args.get("with_"):
            for cte in select.args["with_"].expressions:
                cte_query = cte.this
                if isinstance(cte_query, (exp.Select, exp.SetOperation)):
                    replacement, child_islands, child_rejected, child_warnings = self._rewrite_query_island(
                        cte_query,
                        name=cte.alias or "cte",
                        source_kind="cte",
                    )
                    cte.set("this", replacement)
                    semantic_islands.extend(child_islands)
                    rejected_rules.update(child_rejected)
                    warnings.extend(child_warnings)

        # Recurse into FROM subquery
        from_clause = select.args.get("from_")
        if from_clause and isinstance(from_clause.this, exp.Subquery):
            subquery = from_clause.this
            subquery_select = subquery.this
            if isinstance(subquery_select, (exp.Select, exp.SetOperation)):
                replacement, child_islands, child_rejected, child_warnings = self._rewrite_query_island(
                    subquery_select,
                    name=subquery.alias_or_name or "subquery",
                    source_kind="subquery",
                )
                subquery.set("this", replacement)
                self._annotate_conditional_aggregate_islands(select, child_islands, child_rejected)
                semantic_islands.extend(child_islands)
                rejected_rules.update(child_rejected)
                warnings.extend(child_warnings)

        # Recurse into JOIN subqueries
        for join in select.args.get("joins") or []:
            join_expr = join.this
            if isinstance(join_expr, exp.Subquery):
                join_select = join_expr.this
                if isinstance(join_select, (exp.Select, exp.SetOperation)):
                    replacement, child_islands, child_rejected, child_warnings = self._rewrite_query_island(
                        join_select,
                        name=join_expr.alias_or_name or "join_subquery",
                        source_kind="join_subquery",
                    )
                    join_expr.set("this", replacement)
                    self._annotate_conditional_aggregate_islands(select, child_islands, child_rejected)
                    semantic_islands.extend(child_islands)
                    rejected_rules.update(child_rejected)
                    warnings.extend(child_warnings)

        for clause_subquery in self._current_select_clause_subqueries(select):
            subquery_select = clause_subquery.this
            if isinstance(subquery_select, (exp.Select, exp.SetOperation)):
                replacement, child_islands, child_rejected, child_warnings = self._rewrite_query_island(
                    subquery_select,
                    name=clause_subquery.alias_or_name or "clause_subquery",
                    source_kind="clause_subquery",
                )
                clause_subquery.set("this", replacement)
                semantic_islands.extend(child_islands)
                rejected_rules.update(child_rejected)
                warnings.extend(child_warnings)

        for exists in self._current_select_clause_exists(select):
            exists_select = exists.this
            if isinstance(exists_select, (exp.Select, exp.SetOperation)):
                replacement, child_islands, child_rejected, child_warnings = self._rewrite_query_island(
                    exists_select,
                    name="exists_subquery",
                    source_kind="clause_subquery",
                )
                exists.set("this", replacement)
                semantic_islands.extend(child_islands)
                rejected_rules.update(child_rejected)
                warnings.extend(child_warnings)

        return semantic_islands, rejected_rules, warnings

    def _current_select_clause_expressions(self, select: exp.Select) -> list[exp.Expression]:
        clauses: list[exp.Expression] = list(select.expressions)
        for key in ("where", "having", "qualify"):
            clause = select.args.get(key)
            if clause is not None:
                clauses.append(clause.this)
        for key in ("group", "order"):
            clause = select.args.get(key)
            if clause is not None:
                clauses.extend(list(clause.expressions))
        return clauses

    def _current_select_clause_subqueries(self, select: exp.Select) -> list[exp.Subquery]:
        clauses = self._current_select_clause_expressions(select)
        subqueries: list[exp.Subquery] = []
        seen: set[int] = set()
        for clause in clauses:
            for subquery in clause.find_all(exp.Subquery):
                identity = id(subquery)
                if identity not in seen:
                    subqueries.append(subquery)
                    seen.add(identity)
        return subqueries

    def _current_select_clause_exists(self, select: exp.Select) -> list[exp.Exists]:
        clauses = self._current_select_clause_expressions(select)
        exists_nodes: list[exp.Exists] = []
        seen: set[int] = set()
        for clause in clauses:
            for exists in clause.find_all(exp.Exists):
                identity = id(exists)
                if identity not in seen:
                    exists_nodes.append(exists)
                    seen.add(identity)
        return exists_nodes

    def _annotate_conditional_aggregate_islands(
        self,
        select: exp.Select,
        semantic_islands: list[_SemanticIslandRewrite],
        rejected_rules: dict[str, str],
    ) -> None:
        if not semantic_islands:
            return
        valid = False
        rejection: str | None = None
        for island in semantic_islands:
            valid, rejection = self._conditional_aggregate_wrapper_is_safe(select, island.plan)
            if valid:
                island.applied_rules = self._dedupe([*island.applied_rules, "conditional_aggregate_wrapper"])
        if not valid and rejection:
            rejected_rules["conditional_aggregate_wrapper"] = rejection

    def _conditional_aggregate_wrapper_is_safe(
        self,
        select: exp.Select,
        plan: SemanticQueryPlan,
    ) -> tuple[bool, str | None]:
        conditional_sums: list[exp.Sum] = []
        for projection in select.expressions:
            expression = self._projection_expression(projection)
            if isinstance(expression, exp.Sum) and isinstance(expression.this, exp.Case):
                conditional_sums.append(expression)
            conditional_sums.extend(
                sum_expr
                for sum_expr in expression.find_all(exp.Sum)
                if isinstance(sum_expr.this, exp.Case) and sum_expr is not expression
            )
        if not conditional_sums:
            return False, None

        output_name_to_ref = self._plan_output_name_to_ref(plan)
        dimension_refs = set(plan.dimensions)
        metric_refs = set(plan.metrics)

        for sum_expr in conditional_sums:
            case_expr = sum_expr.this
            if not isinstance(case_expr, exp.Case):
                return False, "conditional aggregate input is not CASE"
            default = case_expr.args.get("default")
            if default is not None and not (
                isinstance(default, exp.Literal) and not default.is_string and str(default.this) in {"0", "0.0"}
            ):
                return False, "conditional aggregate CASE default is not zero"
            if case_expr.args.get("this") is not None:
                return False, "simple CASE conditional aggregates are not supported"

            ifs = case_expr.args.get("ifs") or []
            if not ifs:
                return False, "conditional aggregate CASE has no predicates"
            for if_expr in ifs:
                metric_input = if_expr.args.get("true")
                if not isinstance(metric_input, exp.Column):
                    return False, "conditional aggregate result is not a metric column"
                metric_ref = output_name_to_ref.get(metric_input.name)
                if metric_ref not in metric_refs:
                    return False, "conditional aggregate result is not an inner metric"
                metric, _model_context = self._resolve_metric(metric_ref)
                if metric is None or metric.agg not in {"sum", "count"}:
                    return False, "conditional aggregate metric is not additive"

                predicate = if_expr.this
                for column in predicate.find_all(exp.Column):
                    ref = output_name_to_ref.get(column.name)
                    if ref not in dimension_refs:
                        return False, "conditional aggregate predicate references a non-dimension"

        return True, None

    def _rewrite_query_island(
        self,
        query: exp.Expression,
        name: str,
        source_kind: str,
        *,
        as_set_branch: bool = False,
    ) -> tuple[exp.Expression, list[_SemanticIslandRewrite], dict[str, str], list[str]]:
        if isinstance(query, exp.SetOperation):
            semantic_islands, rejected_rules, warnings = self._rewrite_set_operation_tree(query)
            return query, semantic_islands, rejected_rules, warnings

        if not isinstance(query, exp.Select):
            return query, [], {}, []

        if not self._select_tree_references_semantic_model(query):
            return query, [], {}, []

        replacement, island, rejected_rules = self._try_optimize_select_as_island(
            query,
            name=name,
            source_kind=source_kind,
            as_set_branch=as_set_branch,
        )
        if island is not None and replacement is not None:
            return replacement, [island], rejected_rules, []

        child_islands, child_rejected, child_warnings = self._rewrite_select_tree(query)
        rejected_rules.update(child_rejected)
        return query, child_islands, rejected_rules, child_warnings

    def _rewrite_set_operation_tree(
        self, expression: exp.SetOperation
    ) -> tuple[list[_SemanticIslandRewrite], dict[str, str], list[str]]:
        semantic_islands: list[_SemanticIslandRewrite] = []
        rejected_rules: dict[str, str] = {}
        warnings: list[str] = []

        set_rejection = self._set_operation_branch_rejection(expression)
        if set_rejection is not None:
            rejected_rules["set_operation_branch_optimization"] = set_rejection
            return semantic_islands, rejected_rules, warnings

        with_clause = expression.args.get("with_")
        if with_clause:
            for cte in with_clause.expressions:
                cte_query = cte.this
                if isinstance(cte_query, (exp.Select, exp.SetOperation)):
                    replacement, child_islands, child_rejected, child_warnings = self._rewrite_query_island(
                        cte_query,
                        name=cte.alias or "cte",
                        source_kind="cte",
                    )
                    cte.set("this", replacement)
                    semantic_islands.extend(child_islands)
                    rejected_rules.update(child_rejected)
                    warnings.extend(child_warnings)

        for arg_name, branch_name in (("this", "left_branch"), ("expression", "right_branch")):
            branch = expression.args.get(arg_name)
            if not isinstance(branch, (exp.Select, exp.SetOperation)):
                continue
            replacement, child_islands, child_rejected, child_warnings = self._rewrite_query_island(
                branch,
                name=branch_name,
                source_kind="set_operation_branch",
                as_set_branch=True,
            )
            expression.set(arg_name, replacement)
            semantic_islands.extend(child_islands)
            rejected_rules.update(child_rejected)
            warnings.extend(child_warnings)

        return semantic_islands, rejected_rules, warnings

    def _set_operation_branch_rejection(self, expression: exp.SetOperation) -> str | None:
        branch_selects = [
            branch
            for branch in (expression.args.get("this"), expression.args.get("expression"))
            if isinstance(branch, exp.Select)
        ]
        for branch in branch_selects:
            if branch.args.get("order"):
                return "branch-level ORDER BY is not safe to optimize inside a set operation"

        known_arities = [
            len(branch.expressions)
            for branch in branch_selects
            if not (len(branch.expressions) == 1 and isinstance(branch.expressions[0], exp.Star))
        ]
        if known_arities and any(arity != known_arities[0] for arity in known_arities):
            return "set operation branch projections do not align"
        return None

    def _try_optimize_select_as_island(
        self,
        select: exp.Select,
        name: str,
        source_kind: str,
        *,
        as_set_branch: bool,
    ) -> tuple[exp.Expression | None, _SemanticIslandRewrite | None, dict[str, str]]:
        rejected_rules: dict[str, str] = {}
        source_sql = select.sql(dialect=self.dialect)

        if self._current_select_references_outer_scope(select):
            rejected_rules["semantic_island_optimization"] = "semantic island references outer query scope"
            return None, None, rejected_rules

        optimization, wrapped_rejected = self._optimize_wrapped_semantic_query(select.copy())
        rejected_rules.update(wrapped_rejected)
        if optimization is not None:
            plan = optimization.plan
            if as_set_branch and not self._set_branch_output_is_compatible(select, plan):
                rejected_rules["set_operation_branch_optimization"] = (
                    "branch output columns would not align after rewrite"
                )
                return None, None, rejected_rules
            rewritten_sql = self._generate_from_plan(plan)
            replacement = self._parse_island_replacement(rewritten_sql, name=name, as_set_branch=as_set_branch)
            return (
                replacement,
                _SemanticIslandRewrite(
                    name=name,
                    source_kind=source_kind,
                    source_sql=source_sql,
                    rewritten_sql=rewritten_sql,
                    plan=plan,
                    applied_rules=self._dedupe(["semantic_island_optimization", *optimization.applied_rules]),
                    rejected_rules=rejected_rules,
                ),
                rejected_rules,
            )

        if not self._references_semantic_model(select):
            return None, None, rejected_rules

        try:
            plan = self._plan_simple_query(select.copy())
        except Exception as e:
            rejected_rules["semantic_island_optimization"] = f"semantic island cannot be planned: {e}"
            return None, None, rejected_rules

        if as_set_branch and not self._set_branch_output_is_compatible(select, plan):
            rejected_rules["set_operation_branch_optimization"] = "branch output columns would not align after rewrite"
            return None, None, rejected_rules

        rewritten_sql = self._generate_from_plan(plan)
        replacement = self._parse_island_replacement(rewritten_sql, name=name, as_set_branch=as_set_branch)
        return (
            replacement,
            _SemanticIslandRewrite(
                name=name,
                source_kind=source_kind,
                source_sql=source_sql,
                rewritten_sql=rewritten_sql,
                plan=plan,
                applied_rules=self._dedupe(["semantic_island_optimization", *plan.applied_rules]),
                rejected_rules=rejected_rules,
            ),
            rejected_rules,
        )

    def _parse_island_replacement(self, rewritten_sql: str, name: str, *, as_set_branch: bool) -> exp.Expression:
        if not as_set_branch:
            return sqlglot.parse_one(rewritten_sql, dialect=self.dialect)

        alias = self._safe_internal_alias(f"{name}_semantic")
        return sqlglot.parse_one(
            f"SELECT * FROM (\n{rewritten_sql}\n) AS {alias}",
            dialect=self.dialect,
        )

    def _safe_internal_alias(self, name: str) -> str:
        return "".join(char if char.isalnum() or char == "_" else "_" for char in name) or "semantic_branch"

    def _set_branch_output_is_compatible(self, select: exp.Select, plan: SemanticQueryPlan) -> bool:
        if len(select.expressions) == 1 and isinstance(select.expressions[0], exp.Star):
            return True
        original_names = list(select.named_selects)
        planned_names = list(self._plan_output_name_to_ref(plan))
        return len(original_names) == len(planned_names) and original_names == planned_names

    def _current_select_references_outer_scope(self, select: exp.Select) -> bool:
        local_names = self._current_select_relation_names(select)
        clauses: list[exp.Expression] = list(select.expressions)
        for key in ("where", "having", "qualify"):
            clause = select.args.get(key)
            if clause is not None:
                clauses.append(clause.this)
        for key in ("group", "order"):
            clause = select.args.get(key)
            if clause is not None:
                clauses.extend(list(clause.expressions))

        for clause in clauses:
            for column in clause.find_all(exp.Column):
                if (
                    column.table
                    and column.table not in local_names
                    and column.table not in self.graph.models
                    and column.table != "metrics"
                ):
                    return True
        return False

    def _current_select_relation_names(self, select: exp.Select) -> set[str]:
        names: set[str] = set()
        from_clause = select.args.get("from_")
        if from_clause is not None:
            self._add_relation_names(from_clause.this, names)
        for join in select.args.get("joins") or []:
            self._add_relation_names(join.this, names)
        with_clause = select.args.get("with_")
        if with_clause:
            for cte in with_clause.expressions:
                if cte.alias:
                    names.add(cte.alias)
        return names

    def _add_relation_names(self, relation: exp.Expression, names: set[str]) -> None:
        if isinstance(relation, exp.Table):
            names.add(relation.name)
            if relation.alias:
                names.add(relation.alias)
            return
        if isinstance(relation, exp.Subquery):
            if relation.alias_or_name:
                names.add(relation.alias_or_name)

    def _references_semantic_model(self, select: exp.Select) -> bool:
        """Check if a SELECT statement references any semantic models."""
        from_clause = select.args.get("from_")
        if not from_clause:
            return False

        table_expr = from_clause.this
        if isinstance(table_expr, exp.Table):
            table_name = table_expr.name
            # "metrics" is a special virtual table for semantic layer
            if table_name == "metrics":
                return True
            # Check if this is a known model
            return table_name in self.graph.models

        return False

    def _rewrite_simple_query(self, parsed: exp.Select) -> str:
        """Rewrite a simple semantic layer query (no CTEs/subqueries).

        Args:
            parsed: Parsed SELECT statement

        Returns:
            Rewritten SQL using semantic layer
        """
        explicit_join_filters = []
        if parsed.args.get("joins"):
            explicit_join_filters = self._validate_explicit_semantic_joins(parsed)

        self.inferred_table = self._extract_from_table(parsed)
        self.table_aliases = self._source_aliases(parsed)

        if self._needs_expression_postprocess(parsed):
            return self._rewrite_expression_query(parsed, extra_filters=explicit_join_filters)

        plan = self._plan_simple_query(parsed, include_candidate_details=False)
        return self._generate_from_plan(plan)

    def _validate_explicit_semantic_joins(self, select: exp.Select) -> list[str]:
        """Allow explicit joins only when they point at modeled semantic relationships."""
        from_clause = select.args.get("from_")
        if not from_clause or not isinstance(from_clause.this, exp.Table):
            raise ValueError("Explicit JOIN syntax is only supported from a semantic model table")

        base_model = from_clause.this.name
        if base_model not in self.graph.models:
            return []

        source_aliases = self._source_aliases(select)
        known_aliases = {from_clause.this.alias_or_name: base_model, base_model: base_model}
        join_filters = []

        for join in select.args.get("joins") or []:
            if not isinstance(join.this, exp.Table):
                raise ValueError(
                    "Explicit JOINs from semantic models only support direct model tables. "
                    "Use a semantic subquery first when joining arbitrary SQL."
                )

            joined_model = join.this.name
            if joined_model not in self.graph.models:
                raise ValueError(
                    "Explicit JOINs from semantic models only support modeled semantic tables. "
                    "Use a semantic subquery first when joining arbitrary SQL."
                )

            joined_alias = join.this.alias_or_name
            on_expr = join.args.get("on")
            if on_expr and not self._join_on_matches_relationship(on_expr, joined_model, source_aliases, known_aliases):
                raise ValueError(
                    f"Explicit JOIN from semantic model '{base_model}' to '{joined_model}' does not match a declared relationship"
                )

            if not on_expr:
                # No ON clause: still require that a modeled path exists.
                try:
                    self.graph.find_relationship_path(base_model, joined_model)
                except ValueError as exc:
                    raise ValueError(
                        f"Explicit JOIN from semantic model '{base_model}' to '{joined_model}' has no declared relationship path"
                    ) from exc

            known_aliases[joined_alias] = joined_model
            known_aliases[joined_model] = joined_model
            join_filters.extend(self._explicit_join_filters(join, joined_model))

        return join_filters

    def _explicit_join_filters(self, join: exp.Join, joined_model: str) -> list[str]:
        side = (join.args.get("side") or "").upper()
        kind = (join.args.get("kind") or "").upper()

        if side and side != "LEFT":
            raise ValueError("Explicit semantic JOINs support INNER and LEFT joins only")
        if kind and kind not in ("INNER", "OUTER"):
            raise ValueError("Explicit semantic JOINs support INNER and LEFT joins only")
        if side == "LEFT":
            return []

        model = self.graph.get_model(joined_model)
        return [f"{joined_model}.{column} IS NOT NULL" for column in model.primary_key_columns]

    def _join_on_matches_relationship(
        self,
        on_expr: exp.Expression,
        joined_model: str,
        source_aliases: dict[str, str],
        known_aliases: dict[str, str],
    ) -> bool:
        pairs = self._extract_join_equality_pairs(on_expr)
        if not pairs:
            return False

        other_model: str | None = None
        actual_pairs: set[tuple[str, str]] = set()
        for left_col, right_col in pairs:
            if not left_col.table or not right_col.table:
                return False
            left_model = source_aliases.get(left_col.table)
            right_model = source_aliases.get(right_col.table)
            if not left_model or not right_model:
                return False
            if joined_model not in {left_model, right_model}:
                return False

            current_other_model = right_model if left_model == joined_model else left_model
            if current_other_model not in known_aliases.values():
                return False
            if other_model and current_other_model != other_model:
                return False
            other_model = current_other_model

            if left_model == joined_model:
                actual_pairs.add((right_col.name, left_col.name))
            else:
                actual_pairs.add((left_col.name, right_col.name))

        if not other_model:
            return False

        expected_pairs = self._direct_relationship_key_pairs(other_model, joined_model)
        return actual_pairs == expected_pairs

    def _extract_join_equality_pairs(self, expression: exp.Expression) -> list[tuple[exp.Column, exp.Column]]:
        pairs: list[tuple[exp.Column, exp.Column]] = []
        expression = self._unwrap_join_predicate(expression)

        for part in expression.flatten() if isinstance(expression, exp.And) else [expression]:
            part = self._unwrap_join_predicate(part)
            if not isinstance(part, exp.EQ):
                return []
            left = part.left
            right = part.right
            if not isinstance(left, exp.Column) or not isinstance(right, exp.Column):
                return []
            pairs.append((left, right))

        return pairs

    def _unwrap_join_predicate(self, expression: exp.Expression) -> exp.Expression:
        while isinstance(expression, exp.Paren) and isinstance(expression.this, exp.Expression):
            expression = expression.this
        return expression

    def _direct_relationship_key_pairs(
        self,
        from_model: str,
        to_model: str,
    ) -> set[tuple[str, str]]:
        try:
            path = self.graph.find_relationship_path(from_model, to_model)
        except ValueError:
            return set()
        if len(path) != 1:
            return set()
        hop = path[0]
        return set(zip(hop.from_columns, hop.to_columns, strict=False))

    def _needs_expression_postprocess(self, select: exp.Select) -> bool:
        for projection in select.expressions:
            node = projection.this if isinstance(projection, exp.Alias) else projection
            if isinstance(node, (exp.Column, exp.Star)):
                continue
            return True
        return False

    def _rewrite_expression_query(self, parsed: exp.Select, extra_filters: list[str] | None = None) -> str:
        """Compile semantic dependencies, then evaluate SQL expressions over the result."""
        from sidemantic.core.metric import Metric

        metrics: list[str] = []
        dimensions: list[str] = []
        aliases: dict[str, str] = {}
        ref_aliases: dict[str, str] = {}
        projection_aliases: set[str] = set()
        adhoc_metrics: list[tuple[str, Metric]] = []
        alias_index = 0
        adhoc_index = 0

        def next_alias(prefix: str = "__sd_expr") -> str:
            nonlocal alias_index
            value = f"{prefix}_{alias_index}"
            alias_index += 1
            return value

        def add_ref(ref: str) -> str:
            if ref not in ref_aliases:
                ref_aliases[ref] = next_alias("__sd_field")
                aliases[ref] = ref_aliases[ref]
                if "." not in ref:
                    metrics.append(ref)
                    return ref_aliases[ref]

                model_name, field_name = ref.split(".", 1)
                base_field_name = field_name.rsplit("__", 1)[0] if "__" in field_name else field_name
                model = self.graph.get_model(model_name)
                if model.get_metric(base_field_name) or f"{model_name}.{base_field_name}" in self.graph.metrics:
                    metrics.append(ref)
                elif model.get_dimension(base_field_name):
                    dimensions.append(ref)
                else:
                    raise ValueError(
                        f"Field '{model_name}.{base_field_name}' not found. "
                        f"Must be a metric, measure, or dimension in model '{model_name}'"
                    )
            return ref_aliases[ref]

        def normalize_adhoc_metric_sql(node: exp.AggFunc, model_name: str) -> str:
            metric_expr = node.copy()
            for column in metric_expr.find_all(exp.Column):
                if column.table:
                    resolved_model = getattr(self, "table_aliases", {}).get(column.table, column.table)
                    if resolved_model == model_name:
                        column.set("table", None)
            return metric_expr.sql(dialect=self.dialect)

        def ad_hoc_metric_model(node: exp.AggFunc) -> str:
            if not self.inferred_table or self.inferred_table == "metrics":
                raise ValueError("Ad hoc aggregate expressions require a single semantic model in FROM")

            referenced_models = set()
            for column in node.find_all(exp.Column):
                if column.table:
                    referenced_models.add(getattr(self, "table_aliases", {}).get(column.table, column.table))
                else:
                    referenced_models.add(self.inferred_table)

            if not referenced_models:
                return self.inferred_table

            if len(referenced_models) != 1:
                raise ValueError("Ad hoc aggregate expressions can only reference one semantic model")

            model_name = next(iter(referenced_models))
            if model_name != self.inferred_table:
                raise ValueError(
                    "Ad hoc aggregate expressions can only reference columns from the base semantic model. "
                    f"Define a metric on '{model_name}' to aggregate joined-model columns."
                )

            return model_name

        def add_adhoc_metric(node: exp.AggFunc) -> str:
            nonlocal adhoc_index
            model_name = ad_hoc_metric_model(node)

            metric_name = f"__adhoc_metric_{adhoc_index}"
            adhoc_index += 1
            metric = Metric(name=metric_name, sql=normalize_adhoc_metric_sql(node, model_name))
            model = self.graph.get_model(model_name)
            model.metrics.append(metric)
            adhoc_metrics.append((model_name, metric))
            ref = f"{model_name}.{metric_name}"
            metrics.append(ref)
            alias = next_alias("__sd_metric")
            aliases[ref] = alias
            return alias

        def transform_for_outer(expression: exp.Expression) -> exp.Expression:
            rewritten = expression.copy()

            def replace_node(node: exp.Expression) -> exp.Expression:
                if isinstance(node, exp.AggFunc):
                    return exp.column(add_adhoc_metric(node))
                if isinstance(node, exp.Column):
                    ref = self._resolve_column(node)
                    if not ref:
                        raise ValueError(f"Cannot resolve column: {node.sql(dialect=self.dialect)}")
                    return exp.column(add_ref(ref))
                return node

            return rewritten.transform(replace_node)

        outer_projections: list[exp.Expression] = []
        try:
            for projection in parsed.expressions:
                custom_alias = projection.alias if isinstance(projection, exp.Alias) else None
                node = projection.this if isinstance(projection, exp.Alias) else projection

                if isinstance(node, exp.Star):
                    raise ValueError("SELECT * is not supported in semantic expression queries")

                outer_expr = transform_for_outer(node)
                if custom_alias:
                    projection_aliases.add(custom_alias)
                    outer_projections.append(exp.alias_(outer_expr, custom_alias, quoted=False))
                elif isinstance(node, exp.Column):
                    projection_aliases.add(node.name)
                    outer_projections.append(exp.alias_(outer_expr, node.name, quoted=False))
                elif isinstance(node, exp.AggFunc):
                    projection_aliases.add(node.key.lower())
                    outer_projections.append(exp.alias_(outer_expr, node.key.lower(), quoted=False))
                else:
                    outer_projections.append(outer_expr)

            filters = [*self._extract_filters(parsed), *(extra_filters or [])]
            inner_sql = self.generator.generate(
                metrics=metrics,
                dimensions=dimensions,
                filters=filters,
                aliases=aliases,
                user_attributes=getattr(self, "_rewrite_user_attributes", None),
            )

            projection_sql = ",\n  ".join(projection.sql(dialect=self.dialect) for projection in outer_projections)
            outer_sql = f"SELECT\n  {projection_sql}\nFROM (\n{inner_sql}\n) AS __sdq"

            order_clause = parsed.args.get("order")
            if order_clause:
                outer_order = []
                for order_expr in order_clause.expressions:
                    transformed = order_expr.copy()
                    if (
                        isinstance(order_expr.this, exp.Column)
                        and not order_expr.this.table
                        and order_expr.this.name in projection_aliases
                    ):
                        transformed.set("this", order_expr.this.copy())
                    else:
                        transformed.set("this", transform_for_outer(order_expr.this))
                    outer_order.append(transformed)
                outer_sql += "\nORDER BY " + ", ".join(expr.sql(dialect=self.dialect) for expr in outer_order)

            limit = self._extract_limit(parsed)
            offset = self._extract_offset(parsed)
            if limit is not None:
                outer_sql += f"\nLIMIT {limit}"
            if offset is not None:
                outer_sql += f"\nOFFSET {offset}"

            return outer_sql
        finally:
            for model_name, metric in adhoc_metrics:
                model = self.graph.get_model(model_name)
                model.metrics = [existing for existing in model.metrics if existing is not metric]

    def _extract_metrics_and_dimensions(self, select: exp.Select) -> tuple[list[str], list[str], dict[str, str]]:
        """Extract metrics and dimensions from SELECT clause.

        Args:
            select: Parsed SELECT statement

        Returns:
            Tuple of (metrics, dimensions, aliases)
            where aliases is a dict mapping field reference to custom alias
        """
        metrics = []
        dimensions = []
        aliases = {}

        for projection in select.expressions:
            # Handle SELECT *
            if isinstance(projection, exp.Star):
                # Expand to all fields from the inferred table
                if not self.inferred_table:
                    raise ValueError("SELECT * requires a FROM clause with a single table")

                # FROM metrics: expand to all metrics/dimensions from all models
                if self.inferred_table == "metrics":
                    raise ValueError(
                        "SELECT * is not supported with FROM metrics.\n"
                        "You must explicitly select fields, e.g.:\n"
                        "  SELECT orders.revenue, customers.region FROM metrics"
                    )

                model = self.graph.get_model(self.inferred_table)

                # Add all dimensions
                for dim in model.dimensions:
                    dimensions.append(f"{self.inferred_table}.{dim.name}")

                # Add all measures as metrics
                for measure in model.metrics:
                    metrics.append(f"{self.inferred_table}.{measure.name}")

                continue

            # Get column name and alias
            custom_alias = None
            if isinstance(projection, exp.Alias):
                column = projection.this
                custom_alias = projection.alias
            else:
                column = projection

            # Skip literal values
            if isinstance(column, exp.Literal):
                raise ValueError(
                    "Literal values in SELECT are not supported in semantic layer queries.\n"
                    "Only metrics and dimensions can be selected."
                )

            # Extract table.column reference
            ref = self._resolve_column(column)
            if not ref:
                raise ValueError(f"Cannot resolve column: {column.sql(dialect=self.dialect)}")

            # Store custom alias if provided
            if custom_alias:
                aliases[ref] = custom_alias

            # Handle graph-level metrics (no model prefix)
            if "." not in ref:
                # This is a graph-level metric
                if ref in self.graph.metrics:
                    metrics.append(ref)
                    continue
                else:
                    raise ValueError(f"Field '{ref}' not found as a graph-level metric")

            model_name, field_name = ref.split(".", 1)

            # Check if field_name includes time granularity suffix (e.g., order_date__day)
            base_field_name = field_name
            if "__" in field_name:
                parts = field_name.rsplit("__", 1)
                potential_gran = parts[1]
                # Validate granularity
                valid_grans = ["year", "quarter", "month", "week", "day", "hour", "minute", "second"]
                if potential_gran in valid_grans:
                    base_field_name = parts[0]

            # Check if it's a metric (using base name without granularity)
            metric_ref = f"{model_name}.{base_field_name}"
            if metric_ref in self.graph.metrics:
                metrics.append(f"{model_name}.{field_name}")  # Keep original field_name with granularity
                continue

            # Check if it's a measure (should be accessed as metric)
            model = self.graph.get_model(model_name)
            if any(m.name == base_field_name for m in model.metrics):
                # Measure referenced directly - treat as implicit metric
                metrics.append(f"{model_name}.{field_name}")  # Keep original field_name
                continue

            # Check if it's a dimension
            if any(d.name == base_field_name for d in model.dimensions):
                # Keep the full ref including __granularity if present
                dimensions.append(ref)
                continue

            raise ValueError(
                f"Field '{model_name}.{base_field_name}' not found. Must be a metric, measure, or dimension in model '{model_name}'"
            )

        return metrics, dimensions, aliases

    def _extract_filters(self, select: exp.Select) -> list[str]:
        """Extract filters from WHERE clause.

        Args:
            select: Parsed SELECT statement

        Returns:
            List of filter expressions
        """
        if not select.args.get("where"):
            return []

        where = self._normalize_source_aliases(select.args["where"].this.copy())
        where = self._qualify_root_filter_expression(where)

        # Handle compound conditions (AND/OR)
        if isinstance(where, (exp.And, exp.Or)):
            return self._extract_compound_filters(where)

        # Single condition
        return [where.sql(dialect=self.dialect)]

    def _qualify_root_filter_expression(self, expression: exp.Expression) -> exp.Expression:
        default_model = getattr(self, "inferred_table", None)
        if default_model not in self.graph.models:
            return expression

        model = self.graph.get_model(default_model)

        def qualify(node: exp.Expression) -> exp.Expression:
            if isinstance(node, exp.Select):
                return node
            if isinstance(node, exp.Subquery):
                return node
            if isinstance(node, exp.Column) and not node.table:
                field_name = node.name
                base_field_name = field_name.rsplit("__", 1)[0] if "__" in field_name else field_name
                if model.get_dimension(base_field_name) or model.get_metric(base_field_name):
                    node.set("table", exp.to_identifier(default_model))
                return node

            for key, value in list(node.args.items()):
                if isinstance(value, exp.Expression):
                    node.set(key, qualify(value))
                elif isinstance(value, list):
                    node.set(
                        key,
                        [qualify(item) if isinstance(item, exp.Expression) else item for item in value],
                    )
            return node

        return qualify(expression)

    def _stage_semantic_filters(self, filters: list[str]) -> tuple[list[str], list[str]]:
        row_filters: list[str] = []
        aggregate_filters: list[str] = []
        for filter_expr in filters:
            if self._semantic_filter_references_metric(filter_expr):
                aggregate_filters.append(filter_expr)
            else:
                row_filters.append(filter_expr)
        return row_filters, aggregate_filters

    def _semantic_filter_references_metric(self, filter_expr: str) -> bool:
        if not self._filter_may_reference_metric(filter_expr):
            return False

        try:
            parsed = sqlglot.parse_one(filter_expr, dialect=self.dialect)
        except Exception:
            return False

        for column in parsed.find_all(exp.Column):
            table_name = column.table.replace("_cte", "") if column.table else None
            field_name = column.name.rsplit("__", 1)[0] if "__" in column.name else column.name
            if table_name and table_name in self.graph.models:
                model = self.graph.get_model(table_name)
                if model.get_metric(field_name):
                    return True
            elif not table_name:
                if field_name in self.graph.metrics:
                    return True
                inferred_table = getattr(self, "inferred_table", None)
                if inferred_table in self.graph.models:
                    model = self.graph.get_model(inferred_table)
                    if model.get_metric(field_name):
                        return True
        return False

    def _filter_may_reference_metric(self, filter_expr: str) -> bool:
        lower_filter = filter_expr.lower()
        for metric_name in self.graph.metrics:
            if metric_name.lower() in lower_filter:
                return True
        for model in self.graph.models.values():
            for metric in model.metrics:
                if metric.name.lower() in lower_filter:
                    return True
        return False

    def _extract_compound_filters(self, condition: exp.Expression) -> list[str]:
        """Extract filters from compound AND/OR conditions.

        Args:
            condition: Compound condition (AND/OR)

        Returns:
            List of filter expressions
        """
        filters = []

        if isinstance(condition, exp.And):
            # Split AND into separate filters
            for expr in [condition.left, condition.right]:
                if isinstance(expr, (exp.And, exp.Or)):
                    filters.extend(self._extract_compound_filters(expr))
                else:
                    filters.append(expr.sql(dialect=self.dialect))
        elif isinstance(condition, exp.Or):
            # OR must stay together as single filter
            filters.append(condition.sql(dialect=self.dialect))
        else:
            filters.append(condition.sql(dialect=self.dialect))

        return filters

    def _extract_order_by(self, select: exp.Select) -> list[str] | None:
        """Extract ORDER BY clause.

        Args:
            select: Parsed SELECT statement

        Returns:
            List of order by expressions or None
        """
        if not select.args.get("order"):
            return None

        order_expressions = []
        for order_expr in select.args["order"].expressions:
            # Get column (might have ASC/DESC)
            if isinstance(order_expr, exp.Ordered):
                column = self._normalize_source_aliases(order_expr.this)
                desc = order_expr.args.get("desc", False)
                col_name = self._get_column_name(column)
                order_expressions.append(f"{col_name} {'DESC' if desc else 'ASC'}")
            else:
                col_name = self._get_column_name(self._normalize_source_aliases(order_expr))
                order_expressions.append(col_name)

        return order_expressions if order_expressions else None

    def _extract_limit(self, select: exp.Select) -> int | None:
        """Extract LIMIT clause.

        Args:
            select: Parsed SELECT statement

        Returns:
            Limit value or None
        """
        if not select.args.get("limit"):
            return None

        limit = select.args["limit"]
        if hasattr(limit, "expression"):
            limit_expr = limit.expression
            if isinstance(limit_expr, exp.Literal):
                return int(limit_expr.this)
        if isinstance(limit, exp.Fetch):
            count_expr = limit.args.get("count")
            if isinstance(count_expr, exp.Literal):
                return int(count_expr.this)

        return None

    def _extract_offset(self, select: exp.Select) -> int | None:
        """Extract OFFSET clause.

        Args:
            select: Parsed SELECT statement

        Returns:
            Offset value or None
        """
        if not select.args.get("offset"):
            return None

        offset = select.args["offset"]
        if hasattr(offset, "expression"):
            offset_expr = offset.expression
            if isinstance(offset_expr, exp.Literal):
                return int(offset_expr.this)

        return None

    def _extract_from_table(self, select: exp.Select) -> str | None:
        """Extract table name from FROM clause if there's only one table.

        Args:
            select: Parsed SELECT statement

        Returns:
            Table name or None if multiple tables or no FROM.
            Returns "metrics" if FROM metrics (special generic semantic layer table)
        """
        from_clause = select.args.get("from_")
        if not from_clause:
            return None

        # Get the table expression
        table_expr = from_clause.this
        if isinstance(table_expr, exp.Table):
            table_name = table_expr.name
            # "metrics" is a special virtual table for generic semantic queries
            if table_name == "metrics":
                return "metrics"
            return table_name

        return None

    def _resolve_column(self, column: exp.Expression) -> str | None:
        """Resolve column reference to model.field format.

        Args:
            column: Column expression

        Returns:
            Reference like "orders.revenue" or "orders.order_date__day" or None
        """
        if isinstance(column, exp.Column):
            table = column.table
            name = column.name

            if table:
                table = getattr(self, "table_aliases", {}).get(table, table)
                # Explicit table.column (may include __granularity suffix)
                return f"{table}.{name}"
            else:
                # Try to infer from single FROM table
                if self.inferred_table:
                    # FROM metrics allows unqualified top-level metrics
                    if self.inferred_table == "metrics":
                        # Check if this is a top-level/graph metric
                        if name in self.graph.metrics:
                            # Top-level metric, return as-is (no model prefix)
                            return name
                        else:
                            raise ValueError(
                                f"Column '{name}' must be fully qualified when using FROM metrics.\n"
                                f"Use model.{name} for model-level metrics, or define '{name}' as a graph-level metric.\n\n"
                                f"Example: SELECT orders.revenue, total_orders FROM metrics"
                            )
                    # Column name may include __granularity suffix (e.g., order_date__day)
                    return f"{self.inferred_table}.{name}"
                else:
                    raise ValueError(f"Column '{name}' must have table prefix (e.g., orders.{name})")

        # Handle aggregate functions - must be pre-defined as measures
        if isinstance(column, exp.Func):
            func_sql = column.sql(dialect=self.dialect)
            func_name = column.key.upper()

            # Extract the expression being aggregated
            if column.args.get("this"):
                arg = column.args["this"]
                # Handle both expression objects and strings
                if isinstance(arg, str):
                    arg_sql = arg
                elif isinstance(arg, exp.Star):
                    arg_sql = "*"
                else:
                    arg_sql = arg.sql(dialect=self.dialect)
            else:
                arg_sql = "*"

            # Provide helpful error with YAML example (use wording expected by docs/tests)
            raise ValueError(
                f"Aggregate functions must be defined as a metric.\n\n"
                f"To use {func_sql}, add to your model:\n\n"
                f"measures:\n"
                f"  - name: my_metric\n"
                f"    agg: {func_name.lower()}\n"
                f"    expr: {arg_sql}\n\n"
                f"Then query with: SELECT my_metric FROM {self.inferred_table or 'your_model'}"
            )

        return None

    def _get_column_name(self, column: exp.Expression) -> str:
        """Get simple column name from expression.

        Args:
            column: Column expression

        Returns:
            Column name
        """
        if isinstance(column, exp.Column):
            return column.name
        return column.sql(dialect=self.dialect)
