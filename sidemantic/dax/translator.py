"""Translate DAX AST into Sidemantic-friendly SQL and metadata."""

from __future__ import annotations

import re
from collections import deque
from collections.abc import Iterable
from contextlib import nullcontext
from dataclasses import dataclass, field
from typing import Any

from sidemantic.core.relationship import RelationshipOverride


class DaxTranslationError(ValueError):
    pass


@dataclass(frozen=True)
class ColumnRef:
    table: str | None
    column: str


@dataclass(frozen=True)
class FilterClause:
    sql: str
    columns: frozenset[ColumnRef]
    keep: bool = False


@dataclass(frozen=True)
class FilterRemoval:
    table: str | None = None
    column: str | None = None


@dataclass(frozen=True)
class FilterRetention:
    table: str
    columns: frozenset[str]


@dataclass
class MetricTranslation:
    sql: str | None
    agg: str | None = None
    type: str | None = None
    source_table: str | None = None
    base_metric: str | None = None
    inline_base_sql: str | None = None
    inline_base_agg: str | None = None
    inline_base_filters: list[str] = field(default_factory=list)
    comparison_type: str | None = None
    calculation: str | None = None
    time_offset: str | None = None
    window: str | None = None
    grain_to_date: str | None = None
    window_order: str | None = None
    filters: list[str] = field(default_factory=list)
    relationship_overrides: list[RelationshipOverride] = field(default_factory=list)
    required_models: set[str] = field(default_factory=set)


@dataclass
class TableTranslation:
    sql: str
    required_models: set[str] = field(default_factory=set)
    warnings: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class QueryEvaluateTranslation:
    sql: str
    required_models: set[str] = field(default_factory=set)
    warnings: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class QueryTranslation:
    evaluates: list[QueryEvaluateTranslation]
    warnings: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class RelationshipEdge:
    from_table: str
    from_column: str
    to_table: str
    to_column: str


@dataclass(frozen=True)
class TableColumnArg:
    name: str
    table: str | None = None


def translate_dax_metric(
    expr: Any,
    model_name: str,
    column_sql_by_table: dict[str, dict[str, str]] | None = None,
    measure_names_by_table: dict[str, set[str]] | None = None,
    measure_aggs_by_table: dict[str, dict[str, str]] | None = None,
    measure_sql_by_table: dict[str, dict[str, str]] | None = None,
    measure_filters_by_table: dict[str, dict[str, list[str]]] | None = None,
    time_dimensions_by_table: dict[str, set[str]] | None = None,
    relationship_edges: list[RelationshipEdge] | None = None,
) -> MetricTranslation:
    dax_ast = _load_dax_ast()
    translator = _DaxTranslator(
        dax_ast,
        model_name=model_name,
        column_sql_by_table=column_sql_by_table or {},
        measure_names_by_table=measure_names_by_table or {},
        measure_aggs_by_table=measure_aggs_by_table or {},
        measure_sql_by_table=measure_sql_by_table or {},
        measure_filters_by_table=measure_filters_by_table or {},
        time_dimensions_by_table=time_dimensions_by_table or {},
        relationship_edges=relationship_edges or [],
    )
    return translator.translate_metric(expr)


def translate_dax_table(
    expr: Any,
    model_name: str | None = None,
    column_sql_by_table: dict[str, dict[str, str]] | None = None,
    measure_names_by_table: dict[str, set[str]] | None = None,
    relationship_edges: list[RelationshipEdge] | None = None,
) -> TableTranslation:
    dax_ast = _load_dax_ast()
    translator = _DaxTranslator(
        dax_ast,
        model_name=model_name,
        column_sql_by_table=column_sql_by_table or {},
        measure_names_by_table=measure_names_by_table or {},
        measure_aggs_by_table={},
        measure_sql_by_table={},
        measure_filters_by_table={},
        time_dimensions_by_table={},
        relationship_edges=relationship_edges or [],
        allow_unrelated_table_cross_join=True,
    )
    return translator.translate_table(expr)


def translate_dax_scalar(
    expr: Any,
    model_name: str | None = None,
    column_sql_by_table: dict[str, dict[str, str]] | None = None,
    measure_names_by_table: dict[str, set[str]] | None = None,
    time_dimensions_by_table: dict[str, set[str]] | None = None,
) -> str:
    dax_ast = _load_dax_ast()
    translator = _DaxTranslator(
        dax_ast,
        model_name=model_name,
        column_sql_by_table=column_sql_by_table or {},
        measure_names_by_table=measure_names_by_table or {},
        measure_aggs_by_table={},
        measure_sql_by_table={},
        measure_filters_by_table={},
        time_dimensions_by_table=time_dimensions_by_table or {},
        relationship_edges=[],
    )
    context = translator._allow_cross_table_context() if model_name is None else nullcontext()
    with context:
        return translator._translate_scalar(expr).sql


def translate_dax_query(
    query: Any,
    column_sql_by_table: dict[str, dict[str, str]] | None = None,
    measure_names_by_table: dict[str, set[str]] | None = None,
    measure_aggs_by_table: dict[str, dict[str, str]] | None = None,
    measure_sql_by_table: dict[str, dict[str, str]] | None = None,
    measure_filters_by_table: dict[str, dict[str, list[str]]] | None = None,
    time_dimensions_by_table: dict[str, set[str]] | None = None,
    relationship_edges: list[RelationshipEdge] | None = None,
) -> QueryTranslation:
    dax_ast = _load_dax_ast()
    if not isinstance(query, dax_ast.Query):
        raise DaxTranslationError("translate_dax_query expects sidemantic_dax.ast.Query")

    resolver = _DefineResolver(dax_ast, query.define)
    evaluates: list[QueryEvaluateTranslation] = []
    for stmt in query.evaluates:
        translator = _DaxTranslator(
            dax_ast,
            model_name=None,
            column_sql_by_table=column_sql_by_table or {},
            measure_names_by_table=measure_names_by_table or {},
            measure_aggs_by_table=measure_aggs_by_table or {},
            measure_sql_by_table=measure_sql_by_table or {},
            measure_filters_by_table=measure_filters_by_table or {},
            time_dimensions_by_table=time_dimensions_by_table or {},
            relationship_edges=relationship_edges or [],
            allow_unrelated_table_cross_join=True,
        )
        statement_expr = resolver.resolve_expr(stmt.expr)
        sql = translator._translate_table(statement_expr)
        order_keys = _translate_order_keys(stmt, translator, resolver)
        sql = _apply_order_and_start_at(sql, stmt, translator, resolver, order_keys)
        evaluates.append(
            QueryEvaluateTranslation(
                sql=sql,
                required_models=set(translator._required_models),
                warnings=translator.warnings,
            )
        )

    return QueryTranslation(evaluates=evaluates)


class _DaxTranslator:
    def __init__(
        self,
        dax_ast: Any,
        model_name: str | None,
        column_sql_by_table: dict[str, dict[str, str]],
        measure_names_by_table: dict[str, set[str]],
        measure_aggs_by_table: dict[str, dict[str, str]],
        measure_sql_by_table: dict[str, dict[str, str]],
        measure_filters_by_table: dict[str, dict[str, list[str]]],
        time_dimensions_by_table: dict[str, set[str]],
        relationship_edges: list[RelationshipEdge],
        allow_unrelated_table_cross_join: bool = False,
    ) -> None:
        self.dax = dax_ast
        self.model_name = model_name
        self.column_sql_by_table = column_sql_by_table
        self.measure_names_by_table = measure_names_by_table
        self.measure_aggs_by_table = measure_aggs_by_table
        self.measure_sql_by_table = measure_sql_by_table
        self.measure_filters_by_table = measure_filters_by_table
        self.time_dimensions_by_table = time_dimensions_by_table
        self._env: dict[str, _SqlFragment] = {}
        self._required_models: set[str] = set()
        self._relationship_overrides: list[RelationshipOverride] = []
        self._base_table: str | None = None
        self._allow_cross_table = False
        self._prefer_unqualified_base_table = False
        self._relationship_edges = relationship_edges
        self._relationship_adjacency = self._build_relationship_adjacency(relationship_edges)
        self._allow_unrelated_table_cross_join = allow_unrelated_table_cross_join
        self._current_group_by_columns: frozenset[ColumnRef] = frozenset()
        self._current_filter_columns: frozenset[ColumnRef] = frozenset()
        self._warnings: list[dict[str, Any]] = []
        self._warning_keys: set[tuple[str, str, str]] = set()

    @property
    def warnings(self) -> list[dict[str, Any]]:
        return [dict(warning) for warning in self._warnings]

    def translate_metric(self, expr: Any) -> MetricTranslation:
        expr = self._unwrap(expr)
        if isinstance(expr, self.dax.VarBlock):
            return self._translate_var_block_metric(expr)

        metric_ref = self._translate_metric_reference(expr)
        if metric_ref is not None:
            return metric_ref

        if isinstance(expr, self.dax.FunctionCall):
            name = expr.name.lower()
            if name == "calculate":
                return self._translate_calculate(expr)
            if name in ("totalytd", "totalmtd", "totalqtd", "totalwtd"):
                return self._translate_total_to_date(expr)
            if name in ("min", "max"):
                if len(expr.args) == 1:
                    return self._translate_aggregate(expr)
                with self._allow_cross_table_context():
                    sql = self._translate_min_max(expr).sql
                return MetricTranslation(sql=sql, type="derived", required_models=set(self._required_models))
            if name in (
                "sum",
                "average",
                "averagea",
                "avg",
                "mina",
                "maxa",
                "median",
                "count",
                "countrows",
                "counta",
                "countblank",
                "distinctcount",
                "distinctcountnoblank",
                "approximatedistinctcount",
            ):
                return self._translate_aggregate(expr)
            if name in ("sumx", "averagex", "avgx", "minx", "maxx", "medianx", "countx", "countax"):
                return self._translate_iter_aggregate(expr)

        if isinstance(expr, self.dax.Paren):
            return self.translate_metric(expr.expr)

        with self._allow_cross_table_context():
            sql = self._translate_scalar(expr).sql
        return MetricTranslation(sql=sql, type="derived", required_models=set(self._required_models))

    def _translate_var_block_metric(self, var_block: Any) -> MetricTranslation:
        prior_env = dict(self._env)
        metric_vars: dict[str, MetricTranslation] = {}

        try:
            for decl in var_block.decls:
                metric_value = None
                try:
                    metric_value = self.translate_metric(decl.expr)
                except DaxTranslationError:
                    metric_value = None
                if metric_value is not None:
                    metric_vars[decl.name.lower()] = metric_value

                try:
                    with self._allow_cross_table_context():
                        scalar_value = self._translate_scalar(decl.expr)
                except DaxTranslationError:
                    # Keep metric-only vars available for RETURN var metric paths.
                    # Some metric vars (for example time-intelligence CALCULATE) do not map to scalar SQL.
                    if metric_value is not None:
                        continue
                    raise
                self._env[decl.name.lower()] = scalar_value

            body = self._unwrap(var_block.body)
            if isinstance(body, self.dax.Identifier):
                key = body.name.lower()
                if key in metric_vars:
                    result = metric_vars[key]
                    result.required_models.update(self._required_models)
                    return result
            if isinstance(body, self.dax.BracketRef):
                key = body.name.lower()
                if key in metric_vars:
                    result = metric_vars[key]
                    result.required_models.update(self._required_models)
                    return result

            return MetricTranslation(
                sql=self._translate_projection_scalar(var_block.body).sql,
                type="derived",
                required_models=set(self._required_models),
            )
        finally:
            self._env = prior_env

    def translate_table(self, expr: Any) -> TableTranslation:
        sql = self._translate_table(expr)
        return TableTranslation(sql=sql, required_models=set(self._required_models), warnings=self.warnings)

    def _translate_calculate(self, call: Any) -> MetricTranslation:
        if not call.args:
            raise DaxTranslationError("CALCULATE requires an expression")

        base_expr = call.args[0]
        filter_args = call.args[1:]

        time_translation = self._translate_calculate_time_intelligence(base_expr, filter_args)
        if time_translation is not None:
            return time_translation

        base_metric = self.translate_metric(base_expr)

        new_filters, removals, retentions, remove_all, clear_non_keep, overrides = self._translate_filter_args(
            filter_args
        )
        inherited_filters = [self._filter_clause_from_sql(sql, keep=True) for sql in (base_metric.filters or [])]
        combined_filters = self._merge_filter_clauses(inherited_filters, new_filters)
        combined_filters = self._apply_non_keep_clear(combined_filters, remove_all, clear_non_keep)
        filters = self._apply_filter_retentions(combined_filters, retentions, remove_all)
        filters = self._apply_filter_removals(filters, removals, remove_all)

        base_metric.relationship_overrides.extend(overrides)
        base_metric.required_models.update(self._required_models)
        base_metric.filters = []

        if base_metric.type in ("time_comparison", "cumulative"):
            if filters:
                base_metric.filters.extend(filters)
            return base_metric

        if base_metric.agg:
            base_metric.filters.extend(filters)
            return base_metric

        if filters and base_metric.sql:
            predicate = " AND ".join(filters)
            base_metric.sql = f"CASE WHEN {predicate} THEN {base_metric.sql} ELSE NULL END"
        return base_metric

    def _translate_calculate_time_intelligence(
        self, base_expr: Any, filter_args: list[Any]
    ) -> MetricTranslation | None:
        if not filter_args:
            return None

        time_filter_idx: int | None = None
        time_filter: Any | None = None
        for idx, arg in enumerate(filter_args):
            candidate = self._extract_time_filter_call(arg)
            if candidate is None:
                continue
            time_filter_idx = idx
            time_filter = candidate
            break

        if time_filter is None or time_filter_idx is None:
            return None

        name = time_filter.name.lower()
        if time_filter.args:
            with self._allow_cross_table_context():
                window_order = self._translate_scalar(time_filter.args[0]).sql
        else:
            window_order = None
        base_metric = self._extract_measure_reference(base_expr)
        inline_base_sql = None
        inline_base_agg = None
        inline_base_filters: list[str] = []
        if not base_metric:
            base_translation = self.translate_metric(base_expr)
            if not base_translation.agg or not base_translation.sql:
                return None
            inline_base_sql = base_translation.sql
            inline_base_agg = base_translation.agg
            inline_base_filters = list(base_translation.filters or [])

        if name in ("datesytd", "datesmtd", "datesqtd", "dateswtd"):
            self._validate_time_argument(time_filter.args[0] if time_filter.args else None)
            grain = {
                "datesytd": "year",
                "datesmtd": "month",
                "datesqtd": "quarter",
                "dateswtd": "week",
            }[name]

            base_translation = self.translate_metric(base_expr)
            if base_translation.agg:
                translation = MetricTranslation(
                    sql=base_translation.sql,
                    agg=base_translation.agg,
                    type="cumulative",
                    grain_to_date=grain,
                    window_order=window_order,
                    required_models=set(self._required_models),
                )
            else:
                base_ref = base_translation.base_metric or self._extract_measure_reference(base_expr)
                if base_ref:
                    base_agg = self._lookup_measure_agg(base_ref)
                    translation = MetricTranslation(
                        sql=base_ref,
                        agg=base_agg,
                        type="cumulative",
                        grain_to_date=grain,
                        window_order=window_order,
                        required_models=set(self._required_models),
                    )
                elif base_translation.sql:
                    translation = MetricTranslation(
                        sql=base_translation.sql,
                        agg=base_translation.agg,
                        type="cumulative",
                        grain_to_date=grain,
                        window_order=window_order,
                        required_models=set(self._required_models),
                    )
                else:
                    return None

            translation.relationship_overrides = list(base_translation.relationship_overrides or [])
            translation.required_models.update(base_translation.required_models)
            if base_translation.filters:
                translation.filters = list(base_translation.filters)
        elif name == "datesinperiod":
            self._validate_time_argument(time_filter.args[0] if time_filter.args else None)
            if len(time_filter.args) < 4:
                return None
            periods = self._number_literal_value(time_filter.args[2])
            unit = self._identifier_literal_value(time_filter.args[3])
            if periods is None or unit is None:
                return None

            normalized_unit = unit.lower()
            if normalized_unit.endswith("s"):
                normalized_unit = normalized_unit[:-1]
            if periods > 0:
                window = f"{periods} {normalized_unit} following"
            else:
                window = f"{abs(periods)} {normalized_unit}"

            base_translation = self.translate_metric(base_expr)
            if base_translation.agg:
                translation = MetricTranslation(
                    sql=base_translation.sql,
                    agg=base_translation.agg,
                    type="cumulative",
                    window=window,
                    window_order=window_order,
                    required_models=set(self._required_models),
                )
            else:
                base_ref = base_translation.base_metric or self._extract_measure_reference(base_expr)
                if base_ref:
                    base_agg = self._lookup_measure_agg(base_ref)
                    translation = MetricTranslation(
                        sql=base_ref,
                        agg=base_agg,
                        type="cumulative",
                        window=window,
                        window_order=window_order,
                        required_models=set(self._required_models),
                    )
                elif base_translation.sql:
                    translation = MetricTranslation(
                        sql=base_translation.sql,
                        agg=base_translation.agg,
                        type="cumulative",
                        window=window,
                        window_order=window_order,
                        required_models=set(self._required_models),
                    )
                else:
                    return None

            translation.relationship_overrides = list(base_translation.relationship_overrides or [])
            translation.required_models.update(base_translation.required_models)
            if base_translation.filters:
                translation.filters = list(base_translation.filters)
        elif name == "sameperiodlastyear":
            self._validate_time_argument(time_filter.args[0] if time_filter.args else None)
            translation = MetricTranslation(
                sql=None,
                type="time_comparison",
                base_metric=base_metric,
                inline_base_sql=inline_base_sql,
                inline_base_agg=inline_base_agg,
                inline_base_filters=inline_base_filters,
                comparison_type="yoy",
                calculation="previous_value",
                window_order=window_order,
                required_models=set(self._required_models),
            )
        elif name in ("dateadd", "parallelperiod"):
            self._validate_time_argument(time_filter.args[0] if time_filter.args else None)
            time_info = self._parse_dateadd(time_filter)
            if not time_info:
                return None
            offset, unit = time_info
            comparison_type = _comparison_type_for_unit(unit)
            translation = MetricTranslation(
                sql=None,
                type="time_comparison",
                base_metric=base_metric,
                inline_base_sql=inline_base_sql,
                inline_base_agg=inline_base_agg,
                inline_base_filters=inline_base_filters,
                comparison_type=comparison_type,
                calculation="previous_value",
                time_offset=f"{offset} {unit}",
                window_order=window_order,
                required_models=set(self._required_models),
            )
        else:
            self._validate_time_argument(time_filter.args[0] if time_filter.args else None)
            time_info = _time_offset_for_period_function(name)
            if time_info is None:
                return None
            offset, unit = time_info
            comparison_type = _comparison_type_for_unit(unit)
            translation = MetricTranslation(
                sql=None,
                type="time_comparison",
                base_metric=base_metric,
                inline_base_sql=inline_base_sql,
                inline_base_agg=inline_base_agg,
                inline_base_filters=inline_base_filters,
                comparison_type=comparison_type,
                calculation="previous_value",
                time_offset=f"{offset} {unit}",
                window_order=window_order,
                required_models=set(self._required_models),
            )

        remaining = [arg for idx, arg in enumerate(filter_args) if idx != time_filter_idx]
        if not remaining:
            return translation

        new_filters, removals, retentions, remove_all, clear_non_keep, overrides = self._translate_filter_args(
            remaining
        )
        combined_filters = self._merge_filter_clauses([], new_filters)
        combined_filters = self._apply_non_keep_clear(combined_filters, remove_all, clear_non_keep)
        retained_filters = self._apply_filter_retentions(combined_filters, retentions, remove_all)
        translation.filters = self._apply_filter_removals(retained_filters, removals, remove_all)
        translation.relationship_overrides.extend(overrides)
        translation.required_models.update(self._required_models)
        return translation

        return None

    def _extract_time_filter_call(self, expr: Any) -> Any | None:
        candidate = self._unwrap(expr)
        if not isinstance(candidate, self.dax.FunctionCall):
            return None

        name = candidate.name.lower()
        if name in (
            "sameperiodlastyear",
            "dateadd",
            "parallelperiod",
            "datesytd",
            "datesmtd",
            "datesqtd",
            "dateswtd",
            "datesinperiod",
            "previousday",
            "previousweek",
            "previousmonth",
            "previousquarter",
            "previousyear",
            "nextday",
            "nextweek",
            "nextmonth",
            "nextquarter",
            "nextyear",
        ):
            return candidate

        if name == "keepfilters" and candidate.args:
            inner = self._unwrap(candidate.args[0])
            if isinstance(inner, self.dax.FunctionCall) and inner.name.lower() in (
                "sameperiodlastyear",
                "dateadd",
                "parallelperiod",
                "datesytd",
                "datesmtd",
                "datesqtd",
                "dateswtd",
                "datesinperiod",
                "previousday",
                "previousweek",
                "previousmonth",
                "previousquarter",
                "previousyear",
                "nextday",
                "nextweek",
                "nextmonth",
                "nextquarter",
                "nextyear",
            ):
                return inner

        return None

    def _translate_total_to_date(self, call: Any) -> MetricTranslation:
        if not call.args:
            raise DaxTranslationError("TOTAL* function requires an expression")
        if len(call.args) > 1:
            self._validate_time_argument(call.args[1])
        if len(call.args) > 1:
            with self._allow_cross_table_context():
                window_order = self._translate_scalar(call.args[1]).sql
        else:
            window_order = None

        extra_filter_args: list[Any] = []
        if len(call.args) > 2:
            for arg in call.args[2:]:
                # TOTALYTD optionally accepts a year-end literal after filter args.
                if isinstance(self._unwrap(arg), self.dax.String):
                    continue
                extra_filter_args.append(arg)

        base_expr = call.args[0]
        grain = {
            "totalytd": "year",
            "totalmtd": "month",
            "totalqtd": "quarter",
            "totalwtd": "week",
        }.get(call.name.lower())

        base_metric = self.translate_metric(base_expr)
        translation: MetricTranslation
        if base_metric.agg:
            translation = MetricTranslation(
                sql=base_metric.sql,
                agg=base_metric.agg,
                type="cumulative",
                grain_to_date=grain,
                window_order=window_order,
                required_models=set(self._required_models),
            )
        else:
            base_ref = base_metric.base_metric or self._extract_measure_reference(base_expr)
            if base_ref:
                base_agg = self._lookup_measure_agg(base_ref)
                translation = MetricTranslation(
                    sql=base_ref,
                    agg=base_agg,
                    type="cumulative",
                    grain_to_date=grain,
                    window_order=window_order,
                    required_models=set(self._required_models),
                )
            elif base_metric.sql:
                translation = MetricTranslation(
                    sql=base_metric.sql,
                    agg=base_metric.agg,
                    type="cumulative",
                    grain_to_date=grain,
                    window_order=window_order,
                    required_models=set(self._required_models),
                )
            else:
                raise DaxTranslationError("Unsupported TOTAL* expression")

        inherited_filters = [self._filter_clause_from_sql(sql, keep=True) for sql in (base_metric.filters or [])]
        (
            new_filters,
            removals,
            retentions,
            remove_all,
            clear_non_keep,
            overrides,
        ) = self._translate_filter_args(extra_filter_args)
        combined_filters = self._merge_filter_clauses(inherited_filters, new_filters)
        combined_filters = self._apply_non_keep_clear(combined_filters, remove_all, clear_non_keep)
        retained_filters = self._apply_filter_retentions(combined_filters, retentions, remove_all)
        translation.filters = self._apply_filter_removals(retained_filters, removals, remove_all)

        translation.relationship_overrides = [*base_metric.relationship_overrides, *overrides]
        translation.required_models.update(base_metric.required_models)
        translation.required_models.update(self._required_models)
        return translation

    def _translate_aggregate(self, call: Any) -> MetricTranslation:
        name = call.name.lower()
        agg_map = {
            "sum": "sum",
            "average": "avg",
            "averagea": "avg",
            "avg": "avg",
            "min": "min",
            "mina": "min",
            "max": "max",
            "maxa": "max",
            "median": "median",
            "count": "count",
            "countrows": "count",
            "counta": "count",
            "countblank": "count",
            "distinctcount": "count_distinct",
            "distinctcountnoblank": "count_distinct",
            "approximatedistinctcount": "count_distinct",
        }
        agg = agg_map.get(name)
        if agg is None:
            raise DaxTranslationError(f"Unsupported aggregate {call.name}")

        if name == "countrows":
            if len(call.args) > 1:
                raise DaxTranslationError("COUNTROWS supports at most one argument")
            if call.args:
                distinct_translation = self._translate_countrows_distinct_table(call.args[0])
                if distinct_translation is not None:
                    return distinct_translation
                with self._allow_cross_table_context():
                    filters, overrides = self._filters_from_table(call.args[0])
                return MetricTranslation(
                    sql=None,
                    agg=agg,
                    filters=filters,
                    relationship_overrides=overrides,
                    required_models=set(self._required_models),
                )
            return MetricTranslation(sql=None, agg=agg, required_models=set(self._required_models))

        if not call.args:
            raise DaxTranslationError(f"{call.name} requires an argument")
        if len(call.args) > 1:
            raise DaxTranslationError(f"{call.name} supports exactly one argument")

        prefer_unqualified_base = self.model_name is None and not self._allow_cross_table
        nested_context = self._prefer_unqualified_base_table_context() if prefer_unqualified_base else nullcontext()
        with self._allow_cross_table_context():
            with nested_context:
                arg_sql = self._translate_scalar(call.args[0])
        if name == "countblank":
            return MetricTranslation(
                sql=f"CASE WHEN {arg_sql.sql} IS NULL THEN 1 END",
                agg=agg,
                required_models=set(self._required_models),
            )
        return MetricTranslation(sql=arg_sql.sql, agg=agg, required_models=set(self._required_models))

    def _translate_countrows_distinct_table(self, table_expr: Any) -> MetricTranslation | None:
        table_expr = self._unwrap(table_expr)
        if not isinstance(table_expr, self.dax.FunctionCall):
            return None
        if table_expr.name.lower() not in ("values", "filters", "distinct"):
            return None
        if not table_expr.args:
            return None

        target = self._unwrap(table_expr.args[0])
        if not isinstance(
            target,
            (
                self.dax.TableColumnRef,
                self.dax.HierarchyRef,
                self.dax.BracketRef,
                self.dax.Identifier,
            ),
        ):
            return None

        with self._allow_cross_table_context():
            column_sql = self._translate_scalar(target)
        return MetricTranslation(
            sql=column_sql.sql,
            agg="count_distinct",
            required_models=set(self._required_models),
        )

    def _translate_iter_aggregate(self, call: Any) -> MetricTranslation:
        name = call.name.lower()
        agg_map = {
            "sumx": "sum",
            "averagex": "avg",
            "avgx": "avg",
            "minx": "min",
            "maxx": "max",
            "medianx": "median",
            "countx": "count",
            "countax": "count",
        }
        agg = agg_map[name]
        if len(call.args) < 2:
            raise DaxTranslationError(f"{call.name} requires a table and expression")

        table_expr = call.args[0]
        row_expr = call.args[1]

        with self._allow_cross_table_context():
            table_target = self._unwrap(table_expr)
            if isinstance(table_target, self.dax.FunctionCall) and table_target.name.lower() == "currentgroup":
                filters = []
                overrides = []
            else:
                filters, overrides = self._filters_from_table(table_expr)
            row_sql = self._translate_scalar(row_expr)
        return MetricTranslation(
            sql=row_sql.sql,
            agg=agg,
            filters=filters,
            relationship_overrides=overrides,
            required_models=set(self._required_models),
        )

    def _translate_filter_args(
        self, args: Iterable[Any]
    ) -> tuple[
        list[FilterClause],
        list[FilterRemoval],
        list[FilterRetention],
        bool,
        bool,
        list[RelationshipOverride],
    ]:
        filters: list[FilterClause] = []
        removals: list[FilterRemoval] = []
        retentions: list[FilterRetention] = []
        remove_all = False
        clear_non_keep = False
        overrides: list[RelationshipOverride] = []

        for arg in args:
            arg = self._unwrap(arg)
            if isinstance(arg, self.dax.FunctionCall):
                name = arg.name.lower()
                if name == "keepfilters":
                    inner = arg.args[0] if arg.args else None
                    if inner is None:
                        continue
                    candidate_filters, candidate_overrides = self._translate_filter_candidate(inner, keep=True)
                    filters.extend(candidate_filters)
                    overrides.extend(candidate_overrides)
                    continue
                if name in ("removefilters", "all", "allnoblankrow", "allselected", "allcrossfiltered"):
                    if not arg.args:
                        if name == "allselected":
                            clear_non_keep = True
                        else:
                            remove_all = True
                        continue
                    for target in arg.args:
                        removal = self._translate_filter_removal(target)
                        if removal:
                            removals.append(removal)
                    continue
                if name == "allexcept":
                    retention = self._translate_allexcept(arg)
                    if retention is not None:
                        retentions.append(retention)
                    continue
                if name == "userelationship":
                    override = self._translate_userelationship(arg)
                    if override:
                        overrides.append(override)
                    continue
                if name == "crossfilter":
                    override = self._translate_crossfilter(arg)
                    if override:
                        overrides.append(override)
                    continue
                if name == "filter":
                    candidate_filters, candidate_overrides = self._translate_filter_candidate(arg, keep=False)
                    filters.extend(candidate_filters)
                    overrides.extend(candidate_overrides)
                    continue
            candidate_filters, candidate_overrides = self._translate_filter_candidate(arg, keep=False)
            filters.extend(candidate_filters)
            overrides.extend(candidate_overrides)

        return filters, removals, retentions, remove_all, clear_non_keep, overrides

    def _translate_filter_candidate(
        self, expr: Any, keep: bool
    ) -> tuple[list[FilterClause], list[RelationshipOverride]]:
        expr = self._unwrap(expr)
        if isinstance(expr, self.dax.FunctionCall) and expr.name.lower() == "nonvisual":
            inner = self._unwrap(expr.args[0]) if expr.args else None
            if inner is None:
                return [], []
            return self._translate_filter_candidate(inner, keep=keep)
        if isinstance(expr, self.dax.FunctionCall) and expr.name.lower() == "filter":
            if len(expr.args) < 2:
                raise DaxTranslationError("FILTER requires table and predicate")
            source_expr = self._unwrap(expr.args[0])
            with self._allow_cross_table_context():
                table_filters_sql, table_overrides = self._filters_from_table(source_expr)
            table_filters = [self._filter_clause_from_sql(sql, keep=keep) for sql in table_filters_sql]
            predicate_clause = self._translate_predicate(expr.args[1], keep=keep)
            alias_predicate = self._translate_alias_backed_filter_predicate(source_expr, expr.args[1], keep=keep)
            if alias_predicate is not None:
                return [*table_filters, alias_predicate], table_overrides
            if self._table_name_from_expr(source_expr) is None and self._predicate_needs_derived_alias_fallback(
                expr.args[1], predicate_clause
            ):
                with self._allow_cross_table_context():
                    filtered_sql = self._translate_filter_table(expr)
                exists_sql = f"EXISTS (SELECT 1 FROM ({filtered_sql}) AS __filter_table)"
                # Treat derived-table alias predicates as opaque: avoid leaking inner table
                # lineage into outer FROM expansion.
                return [FilterClause(sql=exists_sql, columns=frozenset(), keep=keep)], table_overrides
            return [*table_filters, predicate_clause], table_overrides
        if isinstance(expr, self.dax.FunctionCall) and expr.name.lower() == "treatas":
            return [self._translate_treatas_filter(expr, keep=keep)], []
        if isinstance(expr, self.dax.FunctionCall) and expr.name.lower() == "datesbetween":
            clause = self._translate_datesbetween_filter(expr, keep=keep)
            if clause is None:
                return [], []
            return [clause], []
        if isinstance(expr, self.dax.FunctionCall) and expr.name.lower() == "datesinperiod":
            clause = self._translate_datesinperiod_filter(expr, keep=keep)
            if clause is None:
                return [], []
            return [clause], []
        if isinstance(expr, self.dax.FunctionCall) and expr.name.lower() in (
            "datesytd",
            "datesmtd",
            "datesqtd",
            "dateswtd",
        ):
            clause = self._translate_cumulative_period_filter(expr, keep=keep)
            if clause is None:
                return [], []
            return [clause], []
        if isinstance(expr, self.dax.FunctionCall) and expr.name.lower() in (
            "sameperiodlastyear",
            "dateadd",
            "parallelperiod",
            "previousday",
            "previousweek",
            "previousmonth",
            "previousquarter",
            "previousyear",
            "nextday",
            "nextweek",
            "nextmonth",
            "nextquarter",
            "nextyear",
        ):
            clause = self._translate_relative_period_filter(expr, keep=keep)
            if clause is None:
                return [], []
            return [clause], []
        if isinstance(expr, self.dax.FunctionCall) and self._is_filter_table_candidate(expr):
            with self._allow_cross_table_context():
                table_filters_sql, table_overrides = self._filters_from_table(expr)
            table_filters = [self._filter_clause_from_sql(sql, keep=keep) for sql in table_filters_sql]
            return table_filters, table_overrides
        if self._is_table_filter_candidate_expr(expr):
            with self._allow_cross_table_context():
                table_filters_sql, table_overrides = self._filters_from_table(expr)
            table_filters = [self._filter_clause_from_sql(sql, keep=keep) for sql in table_filters_sql]
            return table_filters, table_overrides

        return [self._translate_predicate(expr, keep=keep)], []

    def _translate_treatas_filter(self, call: Any, keep: bool) -> FilterClause:
        if len(call.args) < 2:
            raise DaxTranslationError("TREATAS requires a table expression and at least one target column")

        source_expr = self._unwrap(call.args[0])
        target_exprs = [self._unwrap(arg) for arg in call.args[1:]]

        with self._allow_cross_table_context():
            target_fragments: list[_SqlFragment] = [self._translate_scalar(expr) for expr in target_exprs]

        target_sql = [fragment.sql for fragment in target_fragments]
        target_columns: set[ColumnRef] = set()
        for fragment in target_fragments:
            target_columns.update(fragment.columns)

        if isinstance(source_expr, self.dax.TableConstructor):
            if not source_expr.rows:
                return FilterClause(sql="(1 = 0)", columns=frozenset(target_columns), keep=keep)

            width = len(source_expr.rows[0])
            if width != len(target_sql):
                raise DaxTranslationError("TREATAS table column count must match target column count")

            values_sql: list[str] = []
            for row in source_expr.rows:
                if len(row) != width:
                    raise DaxTranslationError("Table constructor rows must have the same number of values")
                fragments = [self._translate_scalar(value) for value in row]
                if width == 1:
                    values_sql.append(fragments[0].sql)
                else:
                    values_sql.append("(" + ", ".join(fragment.sql for fragment in fragments) + ")")

            if width == 1:
                predicate = f"{target_sql[0]} IN ({', '.join(values_sql)})"
            else:
                predicate = f"({', '.join(target_sql)}) IN ({', '.join(values_sql)})"
            return FilterClause(sql=f"({predicate})", columns=frozenset(target_columns), keep=keep)

        if isinstance(source_expr, self.dax.FunctionCall) or self._table_name_from_expr(source_expr) is not None:
            source_sql = self._translate_table(source_expr)
            source_width = self._treatas_source_width(source_expr, source_sql)
            if source_width is not None and source_width != len(target_sql):
                raise DaxTranslationError("TREATAS table column count must match target column count")
            left = target_sql[0] if len(target_sql) == 1 else f"({', '.join(target_sql)})"
            predicate = f"{left} IN (SELECT * FROM ({source_sql}) AS treatas_values)"
            return FilterClause(sql=f"({predicate})", columns=frozenset(target_columns), keep=keep)

        raise DaxTranslationError("TREATAS requires a table expression as first argument")

    def _treatas_source_width(self, source_expr: Any, source_sql: str) -> int | None:
        width = _query_output_width(source_sql)
        if width is not None:
            return width

        return self._treatas_source_expr_width(source_expr)

    def _treatas_source_expr_width(self, source_expr: Any) -> int | None:
        source_expr = self._unwrap(source_expr)

        source_table = self._table_name_from_expr(source_expr)
        if source_table is not None:
            column_map = self.column_sql_by_table.get(source_table, {})
            if column_map:
                return len(column_map)
            return None

        if not isinstance(source_expr, self.dax.FunctionCall):
            return None

        name = source_expr.name.lower()
        if (
            name in ("keepfilters", "nonvisual", "filter", "calculatetable", "distinct", "renamecolumns")
            and source_expr.args
        ):
            return self._treatas_source_expr_width(source_expr.args[0])
        if name == "keepcolumns" and len(source_expr.args) >= 2:
            keep_names: set[str] = set()
            for raw_arg in source_expr.args[1:]:
                keep_name = self._table_column_arg_name(raw_arg, function_name="KEEPCOLUMNS").lower()
                keep_names.add(keep_name)
            return len(keep_names)
        if name == "removecolumns" and len(source_expr.args) >= 2:
            source_names = self._treatas_source_expr_column_names(source_expr.args[0])
            if source_names is not None:
                removed_names: set[str] = set()
                for raw_arg in source_expr.args[1:]:
                    removed_name = self._table_column_arg_name(raw_arg, function_name="REMOVECOLUMNS").lower()
                    removed_names.add(removed_name)
                return len(source_names - removed_names)
            source_width = self._treatas_source_expr_width(source_expr.args[0])
            if source_width is None:
                return None
            removed_count = 0
            seen_removed: set[str] = set()
            for raw_arg in source_expr.args[1:]:
                removed_name = self._table_column_arg_name(raw_arg, function_name="REMOVECOLUMNS").lower()
                if removed_name in seen_removed:
                    continue
                seen_removed.add(removed_name)
                removed_count += 1
            return max(source_width - removed_count, 0)
        if name in ("values", "filters") and source_expr.args:
            target = self._unwrap(source_expr.args[0])
            if isinstance(target, (self.dax.TableColumnRef, self.dax.HierarchyRef, self.dax.BracketRef)):
                return 1
            return self._treatas_source_expr_width(target)
        if (
            name in ("all", "allnoblankrow", "allselected", "allcrossfiltered", "removefilters", "allexcept")
            and source_expr.args
        ):
            target = self._unwrap(source_expr.args[0])
            if isinstance(target, (self.dax.TableColumnRef, self.dax.HierarchyRef, self.dax.BracketRef)):
                return 1
            return self._treatas_source_expr_width(target)
        if name == "union" and len(source_expr.args) >= 2:
            widths = [self._treatas_source_expr_width(arg) for arg in source_expr.args]
            known_widths = [width for width in widths if width is not None]
            if not known_widths:
                return None
            first = known_widths[0]
            if any(width != first for width in known_widths):
                return None
            return first
        if name in ("intersect", "except") and len(source_expr.args) >= 2:
            left_width = self._treatas_source_expr_width(source_expr.args[0])
            right_width = self._treatas_source_expr_width(source_expr.args[1])
            if left_width is None and right_width is None:
                return None
            if left_width is not None and right_width is not None and left_width != right_width:
                return None
            return left_width if left_width is not None else right_width
        if name == "crossjoin" and len(source_expr.args) >= 2:
            widths = [self._treatas_source_expr_width(arg) for arg in source_expr.args]
            if any(width is None for width in widths):
                return None
            return sum(widths)
        if name in ("naturalinnerjoin", "naturalleftouterjoin") and len(source_expr.args) >= 2:
            left_columns = self._treatas_source_expr_column_names(source_expr.args[0])
            right_columns = self._treatas_source_expr_column_names(source_expr.args[1])
            if left_columns is None or right_columns is None:
                return None
            return len(left_columns | right_columns)
        if name in ("generate", "generateall") and len(source_expr.args) >= 2:
            left_width = self._treatas_source_expr_width(source_expr.args[0])
            right_width = self._treatas_source_expr_width(source_expr.args[1])
            if left_width is None or right_width is None:
                return None
            return left_width + right_width
        if name == "topn" and len(source_expr.args) >= 2:
            return self._treatas_source_expr_width(source_expr.args[1])
        if name == "topnskip" and len(source_expr.args) >= 3:
            return self._treatas_source_expr_width(source_expr.args[2])
        if name == "topnperlevel":
            table_idx = self._topnperlevel_table_index(source_expr)
            if table_idx is not None:
                return self._treatas_source_expr_width(source_expr.args[table_idx])

        return None

    def _treatas_source_expr_column_names(self, source_expr: Any) -> set[str] | None:
        source_expr = self._unwrap(source_expr)

        source_table = self._table_name_from_expr(source_expr)
        if source_table is not None:
            column_map = self.column_sql_by_table.get(source_table, {})
            if column_map:
                return {column.lower() for column in column_map}
            return None

        if not isinstance(source_expr, self.dax.FunctionCall):
            return None

        name = source_expr.name.lower()
        if name in ("keepfilters", "nonvisual", "filter", "calculatetable", "distinct") and source_expr.args:
            return self._treatas_source_expr_column_names(source_expr.args[0])
        if name == "renamecolumns" and source_expr.args:
            source_names = self._treatas_source_expr_column_names(source_expr.args[0])
            if source_names is None:
                return None
            renamed = set(source_names)
            rename_args = source_expr.args[1:]
            for idx in range(0, len(rename_args) - 1, 2):
                source_name = self._table_column_arg_name(rename_args[idx], function_name="RENAMECOLUMNS").lower()
                target_name = self._table_column_arg_name(rename_args[idx + 1], function_name="RENAMECOLUMNS").lower()
                if source_name in renamed:
                    renamed.remove(source_name)
                renamed.add(target_name)
            return renamed
        if name == "keepcolumns" and len(source_expr.args) >= 2:
            keep_names: set[str] = set()
            for raw_arg in source_expr.args[1:]:
                keep_name = self._table_column_arg_name(raw_arg, function_name="KEEPCOLUMNS").lower()
                keep_names.add(keep_name)
            return keep_names
        if name == "removecolumns" and source_expr.args:
            source_names = self._treatas_source_expr_column_names(source_expr.args[0])
            if source_names is None:
                return None
            removed_names: set[str] = set()
            for raw_arg in source_expr.args[1:]:
                removed_name = self._table_column_arg_name(raw_arg, function_name="REMOVECOLUMNS").lower()
                removed_names.add(removed_name)
            return source_names - removed_names
        if name in ("values", "filters") and source_expr.args:
            target = self._unwrap(source_expr.args[0])
            if isinstance(target, self.dax.TableColumnRef):
                return {target.column.lower()}
            if isinstance(target, self.dax.HierarchyRef):
                column = target.levels[-1] if target.levels else target.column
                return {column.lower()}
            if isinstance(target, self.dax.BracketRef):
                return {target.name.lower()}
            return self._treatas_source_expr_column_names(target)
        if (
            name in ("all", "allnoblankrow", "allselected", "allcrossfiltered", "removefilters", "allexcept")
            and source_expr.args
        ):
            target = self._unwrap(source_expr.args[0])
            if isinstance(target, self.dax.TableColumnRef):
                return {target.column.lower()}
            if isinstance(target, self.dax.HierarchyRef):
                column = target.levels[-1] if target.levels else target.column
                return {column.lower()}
            if isinstance(target, self.dax.BracketRef):
                return {target.name.lower()}
            return self._treatas_source_expr_column_names(target)
        return None

    def _translate_datesbetween_filter(self, call: Any, keep: bool) -> FilterClause | None:
        if len(call.args) < 3:
            raise DaxTranslationError("DATESBETWEEN requires a date column, start date, and end date")

        date_column_expr = self._unwrap(call.args[0])
        with self._allow_cross_table_context():
            date_column = self._translate_scalar(date_column_expr)

        start_expr = self._unwrap(call.args[1])
        end_expr = self._unwrap(call.args[2])

        predicates: list[str] = []
        columns = set(date_column.columns)

        if not self._is_blank_expr(start_expr):
            with self._allow_cross_table_context():
                start_fragment = self._translate_scalar(start_expr)
            predicates.append(f"{date_column.sql} >= {start_fragment.sql}")
            columns.update(start_fragment.columns)

        if not self._is_blank_expr(end_expr):
            with self._allow_cross_table_context():
                end_fragment = self._translate_scalar(end_expr)
            predicates.append(f"{date_column.sql} <= {end_fragment.sql}")
            columns.update(end_fragment.columns)

        if not predicates:
            return None

        return FilterClause(sql=f"({' AND '.join(predicates)})", columns=frozenset(columns), keep=keep)

    def _translate_datesinperiod_filter(self, call: Any, keep: bool) -> FilterClause | None:
        if len(call.args) < 4:
            raise DaxTranslationError(
                "DATESINPERIOD requires a date column, end date, number of intervals, and interval unit"
            )

        date_column_expr = self._unwrap(call.args[0])
        with self._allow_cross_table_context():
            date_column = self._translate_scalar(date_column_expr)
            end_fragment = self._translate_scalar(call.args[1])

        periods = self._number_literal_value(call.args[2])
        unit = self._identifier_literal_value(call.args[3])
        if periods is None or unit is None:
            raise DaxTranslationError("DATESINPERIOD interval count and unit must be literals")

        normalized_unit = unit.lower()
        if normalized_unit.endswith("s"):
            normalized_unit = normalized_unit[:-1]
        if normalized_unit not in ("day", "week", "month", "quarter", "year"):
            raise DaxTranslationError("DATESINPERIOD interval unit must be day, week, month, quarter, or year")

        interval_value = periods
        interval_unit = normalized_unit
        if normalized_unit == "quarter":
            interval_value = periods * 3
            interval_unit = "month"
        elif normalized_unit == "week":
            interval_value = periods * 7
            interval_unit = "day"

        offset_sql = f"({end_fragment.sql} + INTERVAL '{interval_value} {interval_unit}')"
        if periods < 0:
            predicate = f"{date_column.sql} > {offset_sql} AND {date_column.sql} <= {end_fragment.sql}"
        elif periods > 0:
            predicate = f"{date_column.sql} >= {end_fragment.sql} AND {date_column.sql} < {offset_sql}"
        else:
            predicate = f"{date_column.sql} = {end_fragment.sql}"

        columns = set(date_column.columns)
        columns.update(end_fragment.columns)
        return FilterClause(sql=f"({predicate})", columns=frozenset(columns), keep=keep)

    def _translate_cumulative_period_filter(self, call: Any, keep: bool) -> FilterClause | None:
        if not call.args:
            raise DaxTranslationError("DATESYTD/DATESMTD/DATESQTD/DATESWTD requires a date column argument")

        grain_map = {
            "datesytd": "year",
            "datesmtd": "month",
            "datesqtd": "quarter",
            "dateswtd": "week",
        }
        name = call.name.lower()
        grain = grain_map.get(name)
        if grain is None:
            raise DaxTranslationError(f"Unsupported cumulative period function {call.name}")

        date_expr = self._unwrap(call.args[0])
        with self._allow_cross_table_context():
            date_fragment = self._translate_scalar(date_expr)

        anchor_sql = self._relative_period_anchor_sql(date_fragment)
        start_sql = f"DATE_TRUNC('{grain}', {anchor_sql})"
        predicate = f"{date_fragment.sql} >= {start_sql} AND {date_fragment.sql} <= {anchor_sql}"
        return FilterClause(sql=f"({predicate})", columns=frozenset(date_fragment.columns), keep=keep)

    def _translate_relative_period_filter(self, call: Any, keep: bool) -> FilterClause | None:
        if not call.args:
            raise DaxTranslationError(f"{call.name} requires a date column argument")

        date_expr = self._unwrap(call.args[0])
        with self._allow_cross_table_context():
            date_fragment = self._translate_scalar(date_expr)

        name = call.name.lower()
        if name == "sameperiodlastyear":
            offset_unit: tuple[int, str] | None = (1, "year")
        elif name in ("dateadd", "parallelperiod"):
            offset_unit = self._parse_dateadd(call)
        else:
            offset_unit = _time_offset_for_period_function(name)

        if offset_unit is None:
            raise DaxTranslationError(f"Unsupported relative period function {call.name}")

        offset, unit = offset_unit
        interval_offset = offset
        interval_unit = unit
        if unit == "quarter":
            interval_offset = offset * 3
            interval_unit = "month"
        elif unit == "week":
            interval_offset = offset * 7
            interval_unit = "day"

        anchor_sql = self._relative_period_anchor_sql(date_fragment)
        if offset > 0:
            lower_sql = f"({anchor_sql} + INTERVAL '-{abs(interval_offset)} {interval_unit}')"
            predicate = f"{date_fragment.sql} > {lower_sql} AND {date_fragment.sql} <= {anchor_sql}"
        elif offset < 0:
            upper_sql = f"({anchor_sql} + INTERVAL '{abs(interval_offset)} {interval_unit}')"
            predicate = f"{date_fragment.sql} >= {anchor_sql} AND {date_fragment.sql} < {upper_sql}"
        else:
            predicate = f"{date_fragment.sql} = {anchor_sql}"

        return FilterClause(sql=f"({predicate})", columns=frozenset(date_fragment.columns), keep=keep)

    def _relative_period_anchor_sql(self, date_fragment: _SqlFragment) -> str:
        if len(date_fragment.columns) != 1:
            raise DaxTranslationError("Relative period filters require a single date column reference")
        column_ref = next(iter(date_fragment.columns))
        table = column_ref.table
        column = column_ref.column
        if table is None or column is None:
            raise DaxTranslationError("Relative period filters require a qualified date column reference")
        with self._allow_cross_table_context():
            column_sql = self._column_sql(table, column)
        table_sql = self._table_sql(table)
        return f"(SELECT MAX({column_sql}) FROM {table_sql})"

    def _translate_filter_removal(self, expr: Any) -> FilterRemoval | None:
        with self._allow_cross_table_context():
            expr = self._unwrap(expr)
            table_name = self._table_name_from_expr(expr)
            if table_name is not None:
                self._ensure_table_context(table_name)
                return FilterRemoval(table=table_name)
            if isinstance(expr, self.dax.TableColumnRef):
                self._ensure_table_context(expr.table.name)
                return FilterRemoval(table=expr.table.name, column=expr.column)
            if isinstance(expr, self.dax.HierarchyRef):
                column = expr.levels[-1] if expr.levels else expr.column
                self._ensure_table_context(expr.table.name)
                return FilterRemoval(table=expr.table.name, column=column)
            if isinstance(expr, self.dax.BracketRef):
                return FilterRemoval(column=expr.name)
            return None

    def _translate_allexcept(self, call: Any) -> FilterRetention | None:
        if not call.args:
            raise DaxTranslationError("ALLEXCEPT requires at least a table argument")

        with self._allow_cross_table_context():
            table_expr = self._unwrap(call.args[0])
            table_name = self._table_name_from_expr(table_expr)
            if table_name is None:
                raise DaxTranslationError("ALLEXCEPT first argument must be a table reference")
            self._ensure_table_context(table_name)

            kept_columns: set[str] = set()
            for arg in call.args[1:]:
                expr = self._unwrap(arg)
                if isinstance(expr, self.dax.TableColumnRef):
                    if expr.table.name.lower() != table_name.lower():
                        raise DaxTranslationError("ALLEXCEPT columns must belong to the same table")
                    kept_columns.add(expr.column.lower())
                    continue
                if isinstance(expr, self.dax.HierarchyRef):
                    if expr.table.name.lower() != table_name.lower():
                        raise DaxTranslationError("ALLEXCEPT columns must belong to the same table")
                    column = expr.levels[-1] if expr.levels else expr.column
                    kept_columns.add(column.lower())
                    continue
                if isinstance(expr, self.dax.BracketRef):
                    kept_columns.add(expr.name.lower())
                    continue
                if isinstance(expr, self.dax.Identifier):
                    kept_columns.add(expr.name.lower())
                    continue
                raise DaxTranslationError("ALLEXCEPT only supports column references after the table argument")

            return FilterRetention(table=table_name, columns=frozenset(kept_columns))

    def _translate_userelationship(self, call: Any) -> RelationshipOverride | None:
        if len(call.args) < 2:
            raise DaxTranslationError("USERELATIONSHIP requires two column references")
        left = self._unwrap(call.args[0])
        right = self._unwrap(call.args[1])
        if not isinstance(left, self.dax.TableColumnRef) or not isinstance(right, self.dax.TableColumnRef):
            raise DaxTranslationError("USERELATIONSHIP expects Table[Column] arguments")

        self._required_models.update({left.table.name, right.table.name})
        return RelationshipOverride(
            from_model=left.table.name,
            from_column=left.column,
            to_model=right.table.name,
            to_column=right.column,
            join_type=None,
            direction=None,
        )

    def _translate_crossfilter(self, call: Any) -> RelationshipOverride | None:
        if len(call.args) < 3:
            raise DaxTranslationError("CROSSFILTER requires two columns and a direction")
        left = self._unwrap(call.args[0])
        right = self._unwrap(call.args[1])
        direction_expr = call.args[2]

        if not isinstance(left, self.dax.TableColumnRef) or not isinstance(right, self.dax.TableColumnRef):
            raise DaxTranslationError("CROSSFILTER expects Table[Column] arguments")

        direction = None
        if isinstance(direction_expr, self.dax.String):
            direction = direction_expr.value
        elif isinstance(direction_expr, self.dax.Identifier):
            direction = direction_expr.name

        if not direction:
            raise DaxTranslationError("CROSSFILTER direction must be a string or identifier")

        normalized = direction.replace(" ", "").upper()
        allowed: dict[str, tuple[str | None, str]] = {
            "BOTH": ("inner", "Both"),
            "NONE": ("left", "None"),
            "ONEWAY": (None, "OneWay"),
            "ONEWAY_LEFTFILTERSRIGHT": (None, "OneWay_LeftFiltersRight"),
            "ONEWAY_RIGHTFILTERSLEFT": (None, "OneWay_RightFiltersLeft"),
        }
        if normalized not in allowed:
            raise DaxTranslationError(
                "CROSSFILTER direction must be one of BOTH, NONE, ONEWAY, "
                "ONEWAY_LEFTFILTERSRIGHT, ONEWAY_RIGHTFILTERSLEFT"
            )
        join_type, canonical_direction = allowed[normalized]

        self._required_models.update({left.table.name, right.table.name})
        return RelationshipOverride(
            from_model=left.table.name,
            from_column=left.column,
            to_model=right.table.name,
            to_column=right.column,
            join_type=join_type,
            direction=canonical_direction,
        )

    def _apply_filter_retentions(
        self, filters: list[FilterClause], retentions: list[FilterRetention], remove_all: bool
    ) -> list[FilterClause]:
        if remove_all:
            return []
        if not retentions:
            return filters

        remaining = filters
        for retention in retentions:
            retained: list[FilterClause] = []
            for clause in remaining:
                if self._is_removed_by_retention(clause, retention):
                    continue
                retained.append(clause)
            remaining = retained
        return remaining

    @staticmethod
    def _is_removed_by_retention(clause: FilterClause, retention: FilterRetention) -> bool:
        matching_columns = [col for col in clause.columns if col.table and col.table.lower() == retention.table.lower()]
        if not matching_columns:
            return False
        if not retention.columns:
            return True
        for col in matching_columns:
            if col.column.lower() not in retention.columns:
                return True
        return False

    @staticmethod
    def _apply_non_keep_clear(
        filters: list[FilterClause], remove_all: bool, clear_non_keep: bool
    ) -> list[FilterClause]:
        if remove_all:
            return []
        if not clear_non_keep:
            return filters
        return [clause for clause in filters if clause.keep]

    def _apply_filter_removals(
        self, filters: list[FilterClause], removals: list[FilterRemoval], remove_all: bool
    ) -> list[str]:
        if remove_all:
            return []

        remaining = []
        for clause in filters:
            if self._is_removed(clause, removals):
                continue
            remaining.append(clause.sql)
        return remaining

    @staticmethod
    def _is_removed(clause: FilterClause, removals: list[FilterRemoval]) -> bool:
        for removal in removals:
            for col in clause.columns:
                if removal.table and col.table and removal.table.lower() != col.table.lower():
                    continue
                if removal.column and removal.column.lower() != col.column.lower():
                    continue
                return True
        return False

    def _filter_clause_from_sql(self, sql: str, keep: bool = False) -> FilterClause:
        return FilterClause(sql=sql, columns=frozenset(self._columns_from_sql(sql)), keep=keep)

    def _columns_from_sql(self, sql: str) -> set[ColumnRef]:
        try:
            import sqlglot
            from sqlglot import exp

            parsed = sqlglot.parse_one(sql, dialect="duckdb")
        except Exception:
            return set()

        columns: set[ColumnRef] = set()
        for column in parsed.find_all(exp.Column):
            table = column.table
            if table:
                columns.add(ColumnRef(table=table, column=column.name))
            elif self.model_name:
                columns.add(ColumnRef(table=self.model_name, column=column.name))
            elif self._base_table:
                columns.add(ColumnRef(table=self._base_table, column=column.name))
        return columns

    def _filters_from_table(self, table_expr: Any) -> tuple[list[str], list[RelationshipOverride]]:
        table_expr = self._unwrap(table_expr)
        table_name = self._table_name_from_expr(table_expr)
        if table_name is not None:
            self._ensure_table_context(table_name)
            return [], []

        if isinstance(table_expr, self.dax.FunctionCall) and table_expr.name.lower() == "filter":
            base_filters, base_overrides = self._filters_from_table(table_expr.args[0]) if table_expr.args else ([], [])
            predicate = table_expr.args[1] if len(table_expr.args) > 1 else None
            if predicate is None:
                return base_filters, base_overrides
            clause = self._translate_predicate(predicate, keep=False)
            return [*base_filters, clause.sql], base_overrides

        if isinstance(table_expr, self.dax.FunctionCall):
            name = table_expr.name.lower()
            if name in ("keepfilters", "nonvisual"):
                if not table_expr.args:
                    return [], []
                return self._filters_from_table(table_expr.args[0])
            if name in ("all", "allnoblankrow", "allselected", "allcrossfiltered", "removefilters", "allexcept"):
                return [], []
            if name == "datesbetween":
                clause = self._translate_datesbetween_filter(table_expr, keep=False)
                if clause is None:
                    return [], []
                return [clause.sql], []
            if name == "datesinperiod":
                clause = self._translate_datesinperiod_filter(table_expr, keep=False)
                if clause is None:
                    return [], []
                return [clause.sql], []
            if name in ("datesytd", "datesmtd", "datesqtd", "dateswtd"):
                clause = self._translate_cumulative_period_filter(table_expr, keep=False)
                if clause is None:
                    return [], []
                return [clause.sql], []
            if name in (
                "sameperiodlastyear",
                "dateadd",
                "parallelperiod",
                "previousday",
                "previousweek",
                "previousmonth",
                "previousquarter",
                "previousyear",
                "nextday",
                "nextweek",
                "nextmonth",
                "nextquarter",
                "nextyear",
            ):
                clause = self._translate_relative_period_filter(table_expr, keep=False)
                if clause is None:
                    return [], []
                return [clause.sql], []
            if name == "calculatetable":
                if not table_expr.args:
                    return [], []
                base_filters, base_overrides = self._filters_from_table(table_expr.args[0])
                (
                    new_filters,
                    removals,
                    retentions,
                    remove_all,
                    clear_non_keep,
                    new_overrides,
                ) = self._translate_filter_args(table_expr.args[1:])
                inherited = [self._filter_clause_from_sql(sql, keep=False) for sql in base_filters]
                combined = self._merge_filter_clauses(inherited, new_filters)
                combined = self._apply_non_keep_clear(combined, remove_all, clear_non_keep)
                retained = self._apply_filter_retentions(combined, retentions, remove_all)
                return self._apply_filter_removals(retained, removals, remove_all), [*base_overrides, *new_overrides]
            if name in (
                "selectcolumns",
                "addcolumns",
                "summarize",
                "keepcolumns",
                "removecolumns",
                "renamecolumns",
                "substitutewithindex",
            ):
                self._translate_table(table_expr)
                if table_expr.args:
                    return self._filters_from_table(table_expr.args[0])
                return [], []
            if name == "topn":
                self._translate_table(table_expr)
                if len(table_expr.args) > 1:
                    return self._filters_from_table(table_expr.args[1])
                return [], []
            if name in ("calendar", "generateseries"):
                self._translate_table(table_expr)
                return [], []
            if name == "union":
                self._translate_table(table_expr)
                filters: list[str] = []
                overrides: list[RelationshipOverride] = []
                for arg in table_expr.args:
                    nested_filters, nested_overrides = self._filters_from_table(arg)
                    filters.extend(nested_filters)
                    overrides.extend(nested_overrides)
                return filters, overrides
            if name in ("crossjoin", "naturalinnerjoin", "naturalleftouterjoin"):
                self._translate_table(table_expr)
                filters: list[str] = []
                overrides: list[RelationshipOverride] = []
                for arg in table_expr.args:
                    nested_filters, nested_overrides = self._filters_from_table(arg)
                    filters.extend(nested_filters)
                    overrides.extend(nested_overrides)
                return filters, overrides
            if name == "groupby":
                self._translate_table(table_expr)
                if table_expr.args:
                    return self._filters_from_table(table_expr.args[0])
                return [], []
            if name == "datatable":
                self._translate_table(table_expr)
                return [], []
            if name == "relatedtable":
                self._translate_table(table_expr)
                return [], []
            if name in ("generate", "generateall"):
                self._translate_table(table_expr)
                filters: list[str] = []
                overrides: list[RelationshipOverride] = []
                for arg in table_expr.args[:2]:
                    nested_filters, nested_overrides = self._filters_from_table(arg)
                    filters.extend(nested_filters)
                    overrides.extend(nested_overrides)
                return filters, overrides
            if name == "topnskip":
                self._translate_table(table_expr)
                if len(table_expr.args) > 2:
                    return self._filters_from_table(table_expr.args[2])
                return [], []
            if name == "topnperlevel":
                self._translate_table(table_expr)
                table_idx = self._topnperlevel_table_index(table_expr)
                if table_idx is None:
                    return [], []
                return self._filters_from_table(table_expr.args[table_idx])
            if name == "addmissingitems":
                self._translate_table(table_expr)
                table_arg = self._addmissingitems_table_arg(table_expr)
                if table_arg is None:
                    return [], []
                return self._filters_from_table(table_arg)
            if name == "currentgroup":
                return [], []
            if name in ("intersect", "except"):
                self._translate_table(table_expr)
                return [], []
            if name == "summarizecolumns":
                self._translate_table(table_expr)
                filters: list[str] = []
                overrides: list[RelationshipOverride] = []
                for arg in table_expr.args:
                    arg = self._unwrap(arg)
                    if isinstance(arg, self.dax.FunctionCall) and arg.name.lower() in (
                        "filter",
                        "keepfilters",
                        "nonvisual",
                        "treatas",
                        "datesbetween",
                        "datesinperiod",
                        "datesytd",
                        "datesmtd",
                        "datesqtd",
                        "dateswtd",
                        "sameperiodlastyear",
                        "dateadd",
                        "parallelperiod",
                        "previousday",
                        "previousweek",
                        "previousmonth",
                        "previousquarter",
                        "previousyear",
                        "nextday",
                        "nextweek",
                        "nextmonth",
                        "nextquarter",
                        "nextyear",
                    ):
                        candidate = arg
                        if arg.name.lower() == "keepfilters":
                            inner = arg.args[0] if arg.args else None
                            if inner is None:
                                continue
                            candidate = self._unwrap(inner)
                        elif arg.name.lower() == "nonvisual":
                            inner = arg.args[0] if arg.args else None
                            if inner is None:
                                continue
                            candidate = self._unwrap(inner)
                        nested_filters, nested_overrides = self._translate_filter_candidate(candidate, keep=False)
                        filters.extend(clause.sql for clause in nested_filters)
                        overrides.extend(nested_overrides)
                return filters, overrides
            if name in ("values", "filters", "distinct"):
                self._translate_table(table_expr)
                if table_expr.args:
                    target = self._unwrap(table_expr.args[0])
                    if isinstance(
                        target,
                        (self.dax.Identifier, self.dax.TableRef, self.dax.FunctionCall, self.dax.TableConstructor),
                    ):
                        return self._filters_from_table(target)
                return [], []
        return [], []

    def _translate_predicate(self, expr: Any, keep: bool = False) -> FilterClause:
        with self._allow_cross_table_context():
            fragment = self._translate_scalar(expr)
        return FilterClause(sql=fragment.sql, columns=frozenset(fragment.columns), keep=keep)

    def _predicate_uses_unknown_columns(self, fragment: _SqlFragment) -> bool:
        for column in fragment.columns:
            table = column.table
            if table is None:
                return True
            resolved_table = self._resolve_known_table_name(table)
            if resolved_table is None:
                return True
            if not self._table_has_known_column(resolved_table, column.column):
                return True
        return False

    def _predicate_needs_derived_alias_fallback(self, predicate_expr: Any, predicate_clause: FilterClause) -> bool:
        fragment = _SqlFragment(predicate_clause.sql, predicate_clause.columns)
        if self._predicate_uses_unknown_columns(fragment):
            return True
        if not predicate_clause.columns and self._predicate_has_unqualified_identifier(predicate_expr):
            return True
        return False

    def _translate_alias_backed_filter_predicate(
        self,
        source_expr: Any,
        predicate_expr: Any,
        *,
        keep: bool,
    ) -> FilterClause | None:
        alias_env = self._derived_filter_alias_env(source_expr)
        if not alias_env:
            return None
        alias_keys = set(alias_env)
        if not self._predicate_references_alias(predicate_expr, alias_keys):
            return None
        prior_env = dict(self._env)
        self._env.update(alias_env)
        try:
            clause = self._translate_predicate(predicate_expr, keep=keep)
        finally:
            self._env = prior_env
        if clause.columns:
            return clause
        return None

    def _predicate_has_unqualified_identifier(self, expr: Any) -> bool:
        expr = self._unwrap(expr)
        if isinstance(expr, (self.dax.Identifier, self.dax.BracketRef)):
            return True
        if isinstance(expr, (self.dax.TableColumnRef, self.dax.HierarchyRef)):
            return False
        if isinstance(expr, self.dax.Unary):
            return self._predicate_has_unqualified_identifier(expr.expr)
        if isinstance(expr, self.dax.Binary):
            return self._predicate_has_unqualified_identifier(expr.left) or self._predicate_has_unqualified_identifier(
                expr.right
            )
        if isinstance(expr, self.dax.VarBlock):
            for decl in expr.decls:
                if self._predicate_has_unqualified_identifier(decl.expr):
                    return True
            return self._predicate_has_unqualified_identifier(expr.body)
        if isinstance(expr, self.dax.FunctionCall):
            for arg in expr.args:
                if self._predicate_has_unqualified_identifier(arg):
                    return True
            return False
        if isinstance(expr, self.dax.Paren):
            return self._predicate_has_unqualified_identifier(expr.expr)
        return False

    def _predicate_references_alias(self, expr: Any, alias_keys: set[str]) -> bool:
        expr = self._unwrap(expr)
        if isinstance(expr, (self.dax.Identifier, self.dax.BracketRef)):
            return expr.name.lower() in alias_keys
        if isinstance(expr, (self.dax.TableColumnRef, self.dax.HierarchyRef)):
            return False
        if isinstance(expr, self.dax.Unary):
            return self._predicate_references_alias(expr.expr, alias_keys)
        if isinstance(expr, self.dax.Binary):
            return self._predicate_references_alias(expr.left, alias_keys) or self._predicate_references_alias(
                expr.right, alias_keys
            )
        if isinstance(expr, self.dax.VarBlock):
            for decl in expr.decls:
                if self._predicate_references_alias(decl.expr, alias_keys):
                    return True
            return self._predicate_references_alias(expr.body, alias_keys)
        if isinstance(expr, self.dax.FunctionCall):
            for arg in expr.args:
                if self._predicate_references_alias(arg, alias_keys):
                    return True
            return False
        if isinstance(expr, self.dax.Paren):
            return self._predicate_references_alias(expr.expr, alias_keys)
        return False

    def _derived_filter_alias_env(self, expr: Any) -> dict[str, _SqlFragment]:
        expr = self._unwrap(expr)
        if isinstance(expr, self.dax.FunctionCall):
            name = expr.name.lower()
            if name in ("keepfilters", "nonvisual", "filter", "calculatetable", "distinct") and expr.args:
                return self._derived_filter_alias_env(expr.args[0])
            if name == "topn" and len(expr.args) > 1:
                return self._derived_filter_alias_env(expr.args[1])
            if name == "topnskip" and len(expr.args) > 2:
                return self._derived_filter_alias_env(expr.args[2])
            if name == "topnperlevel":
                table_idx = self._topnperlevel_table_index(expr)
                if table_idx is not None:
                    return self._derived_filter_alias_env(expr.args[table_idx])
                return {}
            if name == "selectcolumns":
                return self._selectcolumns_alias_env(expr)
            if name == "addcolumns":
                return self._addcolumns_alias_env(expr)
            if name == "renamecolumns":
                return self._renamecolumns_alias_env(expr)
            if name == "row":
                return self._row_alias_env(expr)
        return {}

    def _selectcolumns_alias_env(self, call: Any) -> dict[str, _SqlFragment]:
        if not call.args:
            return {}
        pairs = call.args[1:]
        if len(pairs) % 2 != 0:
            return {}
        alias_env: dict[str, _SqlFragment] = {}
        base_expr = self._unwrap(call.args[0])
        base_table_name = self._table_name_from_expr(base_expr)
        if base_table_name is not None:
            with self._allow_cross_table_context():
                self._ensure_table_context(base_table_name)
                for i in range(0, len(pairs), 2):
                    alias = self._string_literal_value(pairs[i])
                    if alias is None:
                        return {}
                    alias_env[alias.lower()] = self._translate_projection_scalar(pairs[i + 1])
            return alias_env
        _from_sql, wrapped = self._table_source_from_expr(call.args[0])
        for i in range(0, len(pairs), 2):
            alias = self._string_literal_value(pairs[i])
            if alias is None:
                return {}
            fragment = (
                self._translate_projection_scalar(pairs[i + 1]) if wrapped else self._translate_scalar(pairs[i + 1])
            )
            alias_env[alias.lower()] = fragment
        return alias_env

    def _addcolumns_alias_env(self, call: Any) -> dict[str, _SqlFragment]:
        if not call.args:
            return {}
        pairs = call.args[1:]
        if len(pairs) % 2 != 0:
            return {}
        alias_env = self._derived_filter_alias_env(call.args[0])
        base_expr = self._unwrap(call.args[0])
        base_table_name = self._table_name_from_expr(base_expr)
        wrapped = False
        if base_table_name is not None:
            with self._allow_cross_table_context():
                self._ensure_table_context(base_table_name)
        else:
            _from_sql, wrapped = self._table_source_from_expr(call.args[0])
        for i in range(0, len(pairs), 2):
            alias = self._string_literal_value(pairs[i])
            if alias is None:
                return {}
            fragment = (
                self._translate_projection_scalar(pairs[i + 1])
                if wrapped or base_table_name is not None
                else self._translate_scalar(pairs[i + 1])
            )
            alias_env[alias.lower()] = fragment
        return alias_env

    def _renamecolumns_alias_env(self, call: Any) -> dict[str, _SqlFragment]:
        if len(call.args) < 3:
            return {}
        alias_env = self._derived_filter_alias_env(call.args[0])
        rename_args = call.args[1:]
        if len(rename_args) % 2 != 0:
            return {}
        for i in range(0, len(rename_args), 2):
            source_name = self._table_column_arg_name(rename_args[i], function_name="RENAMECOLUMNS").lower()
            target_name = self._table_column_arg_name(rename_args[i + 1], function_name="RENAMECOLUMNS").lower()
            fragment = alias_env.pop(source_name, None)
            if fragment is None:
                try:
                    fragment = self._translate_scalar(rename_args[i])
                except DaxTranslationError:
                    return {}
            alias_env[target_name] = fragment
        return alias_env

    def _row_alias_env(self, call: Any) -> dict[str, _SqlFragment]:
        if len(call.args) < 2 or len(call.args) % 2 != 0:
            return {}
        alias_env: dict[str, _SqlFragment] = {}
        for i in range(0, len(call.args), 2):
            alias = self._string_literal_value(call.args[i])
            if alias is None:
                return {}
            try:
                fragment = self._translate_projection_scalar(call.args[i + 1])
            except DaxTranslationError:
                return {}
            alias_env[alias.lower()] = fragment
        return alias_env

    @staticmethod
    def _is_opaque_filter_predicate(predicate_sql: str) -> bool:
        return "AS __filter_table" in predicate_sql

    def _merge_filter_clauses(self, inherited: list[FilterClause], incoming: list[FilterClause]) -> list[FilterClause]:
        merged = list(inherited)
        for clause in incoming:
            if clause.keep or not clause.columns:
                merged.append(clause)
                continue

            retained: list[FilterClause] = []
            for existing in merged:
                if self._filters_overlap(existing, clause):
                    continue
                retained.append(existing)
            retained.append(clause)
            merged = retained
        return merged

    @staticmethod
    def _filters_overlap(left: FilterClause, right: FilterClause) -> bool:
        for left_col in left.columns:
            for right_col in right.columns:
                if _columns_match(left_col, right_col):
                    return True
        return False

    def _translate_table(self, expr: Any) -> str:
        expr = self._unwrap(expr)
        table_name = self._table_name_from_expr(expr)
        if table_name is not None:
            self._ensure_table_context(table_name)
            table_sql = self._table_sql(table_name)
            return f"SELECT * FROM {table_sql}"
        if isinstance(expr, self.dax.TableConstructor):
            return self._translate_table_constructor(expr)
        if isinstance(expr, self.dax.FunctionCall):
            name = expr.name.lower()
            if name in ("keepfilters", "nonvisual"):
                return self._translate_table_wrapper(expr)
            if name == "filter":
                return self._translate_filter_table(expr)
            if name == "row":
                return self._translate_row_table(expr)
            if name == "selectcolumns":
                return self._translate_selectcolumns(expr)
            if name == "addcolumns":
                return self._translate_addcolumns(expr)
            if name == "summarizecolumns":
                return self._translate_summarizecolumns(expr)
            if name == "summarize":
                return self._translate_summarize(expr)
            if name == "groupby":
                return self._translate_groupby(expr)
            if name == "topn":
                return self._translate_topn(expr)
            if name == "topnperlevel":
                return self._translate_topnperlevel(expr)
            if name == "union":
                return self._translate_union_table(expr)
            if name == "crossjoin":
                return self._translate_crossjoin_table(expr)
            if name in ("generate", "generateall"):
                return self._translate_generate_table(expr)
            if name == "naturalinnerjoin":
                return self._translate_natural_inner_join_table(expr)
            if name == "naturalleftouterjoin":
                return self._translate_natural_left_outer_join_table(expr)
            if name == "intersect":
                return self._translate_intersect_table(expr)
            if name == "except":
                return self._translate_except_table(expr)
            if name == "topnskip":
                return self._translate_topnskip(expr)
            if name == "calendar":
                return self._translate_calendar_table(expr)
            if name == "generateseries":
                return self._translate_generateseries_table(expr)
            if name == "datatable":
                return self._translate_datatable_table(expr)
            if name == "relatedtable":
                return self._translate_relatedtable_table(expr)
            if name == "calculatetable":
                return self._translate_calculatetable(expr)
            if name == "addmissingitems":
                return self._translate_addmissingitems_table(expr)
            if name == "treatas":
                return self._translate_treatas_table(expr)
            if name == "datesbetween":
                return self._translate_datesbetween_table(expr)
            if name in ("all", "allnoblankrow", "allselected", "allcrossfiltered", "removefilters", "allexcept"):
                return self._translate_all_like_table(expr)
            if name in (
                "firstdate",
                "lastdate",
                "startofmonth",
                "startofquarter",
                "startofyear",
                "endofmonth",
                "endofquarter",
                "endofyear",
            ):
                return self._translate_date_boundary_table(expr)
            if name == "values":
                return self._translate_values_table(expr)
            if name == "filters":
                return self._translate_filters_table(expr)
            if name == "distinct":
                return self._translate_distinct_table(expr)
            if name == "renamecolumns":
                return self._translate_renamecolumns_table(expr)
            if name == "keepcolumns":
                return self._translate_keepcolumns_table(expr)
            if name == "removecolumns":
                return self._translate_removecolumns_table(expr)
            if name == "substitutewithindex":
                return self._translate_substitutewithindex_table(expr)
            if name in ("selectedmeasure", "selectedmeasurename", "selectedmeasureformatstring", "isselectedmeasure"):
                raise DaxTranslationError(f"{expr.name} is only supported in calculation group expressions")
            if name == "detailrows":
                raise DaxTranslationError("DETAILROWS is only supported in model detail rows expressions")
        if isinstance(expr, self.dax.FunctionCall):
            raise DaxTranslationError(f"Unsupported table function '{expr.name}'")
        if isinstance(expr, self.dax.Identifier):
            raise DaxTranslationError(f"Unknown table identifier '{expr.name}'")
        raise DaxTranslationError(f"Unsupported table expression type '{type(expr).__name__}'")

    def _translate_table_wrapper(self, call: Any) -> str:
        if len(call.args) != 1:
            raise DaxTranslationError(f"{call.name} requires exactly one table-expression argument")
        inner = self._unwrap(call.args[0])
        if self._table_name_from_expr(inner) is not None:
            return self._translate_table(inner)
        if isinstance(inner, (self.dax.FunctionCall, self.dax.TableConstructor)):
            return self._translate_table(inner)
        raise DaxTranslationError(f"{call.name} requires a table-expression argument")

    def _translate_datesbetween_table(self, call: Any) -> str:
        if len(call.args) < 3:
            raise DaxTranslationError("DATESBETWEEN requires a date column, start date, and end date")

        date_column_expr = self._unwrap(call.args[0])
        table_name: str | None = None
        if isinstance(date_column_expr, self.dax.TableColumnRef):
            table_name = date_column_expr.table.name
        elif isinstance(date_column_expr, self.dax.HierarchyRef):
            table_name = date_column_expr.table.name
        elif self.model_name:
            table_name = self.model_name
        elif self._base_table:
            table_name = self._base_table

        if table_name is None:
            raise DaxTranslationError("DATESBETWEEN requires a table-qualified date column")

        with self._allow_cross_table_context():
            self._ensure_table_context(table_name)

        clause = self._translate_datesbetween_filter(call, keep=False)
        tables_in_order = [table_name]
        seen_tables = {table_name.lower()}
        if clause is not None:
            self._append_tables(tables_in_order, seen_tables, clause.columns)
        from_clause = self._build_from_clause_for_tables(tables_in_order)
        select_sql = "*" if len(tables_in_order) == 1 else f"{self._table_sql(table_name)}.*"
        if clause is None:
            return f"SELECT {select_sql} FROM {from_clause}"
        return f"SELECT {select_sql} FROM {from_clause} WHERE {clause.sql}"

    def _translate_treatas_table(self, call: Any) -> str:
        if len(call.args) < 2:
            raise DaxTranslationError("TREATAS requires a table expression and at least one target column")

        target_exprs = [self._unwrap(arg) for arg in call.args[1:]]
        with self._allow_cross_table_context():
            target_fragments = [self._translate_scalar(target) for target in target_exprs]

        tables_in_order: list[str] = []
        seen_tables: set[str] = set()
        for fragment in target_fragments:
            self._append_tables(tables_in_order, seen_tables, fragment.columns)
        if not tables_in_order:
            raise DaxTranslationError("TREATAS target arguments must reference table columns")

        from_clause = self._build_from_clause_for_tables(tables_in_order)
        clause = self._translate_treatas_filter(call, keep=False)

        select_parts: list[str] = []
        for idx, fragment in enumerate(target_fragments):
            alias = _column_name_from_expr_sql(fragment.sql) or f"value{idx + 1}"
            select_parts.append(f"{fragment.sql} AS {self._quote_identifier(alias)}")

        return f"SELECT DISTINCT {', '.join(select_parts)} FROM {from_clause} WHERE {clause.sql}"

    def _translate_table_constructor(self, constructor: Any) -> str:
        if not constructor.rows:
            return "SELECT NULL AS value1 WHERE FALSE"

        width = len(constructor.rows[0])
        if width == 0:
            return "SELECT NULL AS value1 WHERE FALSE"

        row_selects: list[str] = []
        for row in constructor.rows:
            if len(row) != width:
                raise DaxTranslationError("Table constructor rows must have the same number of values")
            fragments: list[_SqlFragment] = []
            tables_in_order: list[str] = []
            seen_tables: set[str] = set()
            all_values_self_contained = True
            for value in row:
                try:
                    fragment = self._translate_scalar(value)
                except DaxTranslationError as exc:
                    if str(exc) != "DAX table expressions must reference a single base table":
                        raise
                    with self._allow_cross_table_context():
                        fragment = self._translate_scalar(value)
                fragments.append(fragment)
                self._append_tables(tables_in_order, seen_tables, fragment.columns)
                fragment_sql_upper = fragment.sql.strip().upper()
                if not (fragment_sql_upper.startswith("SELECT ") or fragment_sql_upper.startswith("(SELECT ")):
                    all_values_self_contained = False

            cols = [f"{fragment.sql} AS value{idx + 1}" for idx, fragment in enumerate(fragments)]
            row_sql = "SELECT " + ", ".join(cols)
            if tables_in_order and not all_values_self_contained:
                row_sql = f"{row_sql} FROM {self._build_from_clause_for_tables(tables_in_order)}"
            row_selects.append(row_sql)

        return " UNION ALL ".join(row_selects)

    def _translate_filter_table(self, call: Any) -> str:
        if len(call.args) < 2:
            raise DaxTranslationError("FILTER requires table and predicate")
        base_expr = self._unwrap(call.args[0])
        base_table_name = self._table_name_from_expr(base_expr)
        if base_table_name is not None:
            self._ensure_table_context(base_table_name)
            with self._allow_cross_table_context():
                with self._prefer_unqualified_base_table_context():
                    predicate = self._translate_scalar(call.args[1])

            tables_in_order = [base_table_name]
            seen_tables = {base_table_name.lower()}
            self._append_tables(tables_in_order, seen_tables, predicate.columns)
            from_clause = self._build_from_clause_for_tables(tables_in_order)
            select_sql = "*" if len(tables_in_order) == 1 else f"{self._table_sql(base_table_name)}.*"
            return f"SELECT {select_sql} FROM {from_clause} WHERE {predicate.sql}"

        from_sql, wrapped = self._table_source_from_expr(call.args[0])
        if wrapped:
            with self._prefer_unqualified_base_table_context():
                predicate = self._translate_projection_scalar(call.args[1])
        else:
            predicate = self._translate_scalar(call.args[1])
        select_sql = "*"
        if wrapped and self._base_table:
            tables_in_order = [self._base_table]
            seen_tables = {self._base_table.lower()}
            self._append_tables(tables_in_order, seen_tables, predicate.columns)
            if len(tables_in_order) > 1:
                from_sql = self._build_from_clause_for_wrapped_base(from_sql, self._base_table, tables_in_order)
                select_sql = "t.*"
        return f"SELECT {select_sql} FROM {from_sql} WHERE {predicate.sql}"

    def _translate_row_table(self, call: Any) -> str:
        if len(call.args) < 2 or len(call.args) % 2 != 0:
            raise DaxTranslationError("ROW requires name/expression pairs")

        prior_base_table = self._base_table
        select_parts: list[str] = []
        tables_in_order: list[str] = []
        seen_tables: set[str] = set()
        all_values_self_contained = True
        for idx in range(0, len(call.args), 2):
            alias = self._string_literal_value(call.args[idx])
            if alias is None:
                raise DaxTranslationError("ROW name must be a string")
            value_expr = call.args[idx + 1]
            try:
                fragment = self._translate_scalar(value_expr)
            except DaxTranslationError as exc:
                if str(exc) != "DAX table expressions must reference a single base table":
                    raise
                prefer_unqualified_base = self.model_name is None and not self._allow_cross_table
                qualifier_ctx = (
                    self._prefer_unqualified_base_table_context() if prefer_unqualified_base else nullcontext()
                )
                with self._allow_cross_table_context():
                    with qualifier_ctx:
                        fragment = self._translate_scalar(value_expr)
            select_parts.append(f"{fragment.sql} AS {self._quote_identifier(alias)}")
            self._append_tables(tables_in_order, seen_tables, fragment.columns)
            fragment_sql_upper = fragment.sql.strip().upper()
            if not (fragment_sql_upper.startswith("SELECT ") or fragment_sql_upper.startswith("(SELECT ")):
                all_values_self_contained = False

        select_sql = ", ".join(select_parts)
        if not tables_in_order:
            return f"SELECT {select_sql}"
        if all_values_self_contained:
            return f"SELECT {select_sql}"
        if prior_base_table is not None and all(table.lower() == prior_base_table.lower() for table in tables_in_order):
            return f"SELECT {select_sql}"
        from_clause = self._build_from_clause_for_tables(tables_in_order)
        return f"SELECT {select_sql} FROM {from_clause}"

    def _translate_union_table(self, call: Any) -> str:
        if len(call.args) < 2:
            raise DaxTranslationError("UNION requires at least two table arguments")

        parts: list[str] = []
        for idx, arg in enumerate(call.args):
            base_sql = self._translate_table_with_isolated_base_context(arg, preserve_result_base=idx == 0)
            parts.append(f"SELECT * FROM ({base_sql}) AS t{idx}")
        return " UNION ALL ".join(parts)

    def _translate_crossjoin_table(self, call: Any) -> str:
        if len(call.args) < 2:
            raise DaxTranslationError("CROSSJOIN requires at least two table arguments")

        from_parts: list[str] = []
        for idx, arg in enumerate(call.args):
            base_sql = self._translate_table_with_isolated_base_context(arg, preserve_result_base=idx == 0)
            from_parts.append(f"({base_sql}) AS t{idx}")
        return f"SELECT * FROM {' CROSS JOIN '.join(from_parts)}"

    def _translate_natural_inner_join_table(self, call: Any) -> str:
        if len(call.args) != 2:
            raise DaxTranslationError("NATURALINNERJOIN requires exactly two table arguments")

        left_sql = self._translate_table_with_isolated_base_context(call.args[0], preserve_result_base=True)
        right_sql = self._translate_table_with_isolated_base_context(call.args[1])
        return f"SELECT * FROM ({left_sql}) AS t0 NATURAL INNER JOIN ({right_sql}) AS t1"

    def _translate_natural_left_outer_join_table(self, call: Any) -> str:
        if len(call.args) != 2:
            raise DaxTranslationError("NATURALLEFTOUTERJOIN requires exactly two table arguments")

        left_sql = self._translate_table_with_isolated_base_context(call.args[0], preserve_result_base=True)
        right_sql = self._translate_table_with_isolated_base_context(call.args[1])
        return f"SELECT * FROM ({left_sql}) AS t0 NATURAL LEFT JOIN ({right_sql}) AS t1"

    def _translate_intersect_table(self, call: Any) -> str:
        if len(call.args) != 2:
            raise DaxTranslationError("INTERSECT requires exactly two table arguments")

        left_sql = self._translate_table_with_isolated_base_context(call.args[0], preserve_result_base=True)
        right_sql = self._translate_table_with_isolated_base_context(call.args[1])
        return f"SELECT * FROM ({left_sql}) AS t0 INTERSECT ALL SELECT * FROM ({right_sql}) AS t1"

    def _translate_except_table(self, call: Any) -> str:
        if len(call.args) != 2:
            raise DaxTranslationError("EXCEPT requires exactly two table arguments")

        left_sql = self._translate_table_with_isolated_base_context(call.args[0], preserve_result_base=True)
        right_sql = self._translate_table_with_isolated_base_context(call.args[1])
        return f"SELECT * FROM ({left_sql}) AS t0 EXCEPT ALL SELECT * FROM ({right_sql}) AS t1"

    def _translate_substitutewithindex_table(self, call: Any) -> str:
        if len(call.args) < 4:
            raise DaxTranslationError(
                "SUBSTITUTEWITHINDEX requires a source table, index column name, index table, and order-by expression"
            )

        left_expr = self._unwrap(call.args[0])
        left_sql = self._translate_table_with_isolated_base_context(left_expr, preserve_result_base=True)

        index_name = self._string_literal_value(call.args[1])
        if index_name is None:
            raise DaxTranslationError("SUBSTITUTEWITHINDEX index column name must be a string")

        trailing_args = [self._unwrap(arg) for arg in call.args[2:]]
        table_positions = [idx for idx, arg in enumerate(trailing_args) if self._is_table_expression_node(arg)]
        if len(table_positions) != 1:
            raise DaxTranslationError("SUBSTITUTEWITHINDEX requires exactly one index table argument")

        index_expr = trailing_args[table_positions[0]]
        order_args = [arg for idx, arg in enumerate(trailing_args) if idx != table_positions[0]]
        if not order_args:
            raise DaxTranslationError("SUBSTITUTEWITHINDEX requires at least one order-by expression")

        index_sql = self._translate_table_with_isolated_base_context(index_expr)
        left_cols = self._infer_table_expr_output_columns(left_expr, left_sql)
        index_cols = self._infer_table_expr_output_columns(index_expr, index_sql)
        if not left_cols or not index_cols:
            raise DaxTranslationError("SUBSTITUTEWITHINDEX requires inferable source and index table columns")
        left_col_counts = self._infer_table_expr_output_column_counts(left_expr, left_sql)
        index_col_counts = self._infer_table_expr_output_column_counts(index_expr, index_sql)

        left_map = {name.lower(): name for name in sorted(left_cols, key=str.lower)}
        index_map = {name.lower(): name for name in sorted(index_cols, key=str.lower)}
        common_keys = sorted(set(left_map) & set(index_map))
        if not common_keys:
            raise DaxTranslationError(
                "SUBSTITUTEWITHINDEX requires at least one common column between source and index tables"
            )
        for key in common_keys:
            if left_col_counts.get(key, 0) > 1:
                source_name = left_map.get(key, key)
                raise DaxTranslationError(
                    f"SUBSTITUTEWITHINDEX source table has ambiguous common column '{source_name}'"
                )
            if index_col_counts.get(key, 0) > 1:
                source_name = index_map.get(key, key)
                raise DaxTranslationError(
                    f"SUBSTITUTEWITHINDEX index table has ambiguous common column '{source_name}'"
                )

        index_tables = self._collect_table_references(index_expr)
        order_by_parts = self._parse_substitutewithindex_order_by_parts(
            order_args,
            index_tables,
            index_cols,
            index_col_counts,
        )

        common_index_cols = [index_map[key] for key in common_keys]
        group_cols_sql = ", ".join(f"i1.{self._quote_identifier(name)}" for name in common_index_cols)
        ranked_alias = "__substitutewithindex_rank"
        ranked_sql = (
            f"SELECT i0.*, DENSE_RANK() OVER (ORDER BY {', '.join(order_by_parts)}) AS {self._quote_identifier(ranked_alias)} "
            f"FROM ({index_sql}) AS i0"
        )
        mapping_sql = (
            f"SELECT {group_cols_sql}, MIN(i1.{self._quote_identifier(ranked_alias)}) AS {self._quote_identifier(ranked_alias)} "
            f"FROM ({ranked_sql}) AS i1 GROUP BY {group_cols_sql}"
        )

        join_predicates = [
            f"l.{self._quote_identifier(left_map[key])} IS NOT DISTINCT FROM i.{self._quote_identifier(index_map[key])}"
            for key in common_keys
        ]
        left_keep = [left_map[key] for key in sorted(left_map) if key not in common_keys]
        projections = [f"l.{self._quote_identifier(name)}" for name in left_keep]
        projections.append(f"i.{self._quote_identifier(ranked_alias)} AS {self._quote_identifier(index_name)}")
        return (
            f"SELECT {', '.join(projections)} FROM ({left_sql}) AS l "
            f"LEFT JOIN ({mapping_sql}) AS i ON {' AND '.join(join_predicates)}"
        )

    def _is_table_expression_node(self, expr: Any) -> bool:
        if self._table_name_from_expr(expr) is not None:
            return True
        if isinstance(expr, self.dax.FunctionCall):
            return self._is_table_function_name(expr.name.lower())
        if isinstance(expr, self.dax.TableConstructor):
            return True
        if isinstance(expr, self.dax.Identifier):
            if self._is_known_measure_identifier(expr.name):
                return False
            return self._table_exists(expr.name)
        return False

    def _parse_substitutewithindex_order_by_parts(
        self,
        order_args: list[Any],
        source_tables: set[str],
        source_columns: set[str],
        source_column_counts: dict[str, int] | None = None,
    ) -> list[str]:
        parts: list[str] = []
        source_tables_lower = {table.lower() for table in source_tables}
        source_columns_lower = {column.lower() for column in source_columns}
        source_column_counts = source_column_counts or {}
        idx = 0
        while idx < len(order_args):
            expr = order_args[idx]
            direction = "ASC"
            if idx + 1 < len(order_args):
                direction_ident = self._identifier_literal_value(order_args[idx + 1])
                if direction_ident is not None and direction_ident.upper() in ("ASC", "DESC"):
                    direction = direction_ident.upper()
                    idx += 2
                else:
                    idx += 1
            else:
                idx += 1

            with self._allow_cross_table_context():
                fragment = self._translate_scalar(expr)
            for ref in fragment.columns:
                if ref.table is not None and ref.table.lower() not in source_tables_lower:
                    raise DaxTranslationError(
                        "SUBSTITUTEWITHINDEX ORDER BY expressions must reference columns from the index table argument"
                    )
                if ref.column.lower() not in source_columns_lower:
                    raise DaxTranslationError(
                        "SUBSTITUTEWITHINDEX ORDER BY expressions must reference columns from the index table argument"
                    )
                if source_column_counts.get(ref.column.lower(), 0) > 1:
                    raise DaxTranslationError(
                        f"SUBSTITUTEWITHINDEX ORDER BY column '{ref.column}' is ambiguous in index table expression"
                    )
            try:
                rewritten = _rewrite_expr_for_alias(
                    fragment.sql,
                    "i0",
                    source_tables=source_tables,
                    source_columns=source_columns,
                    allow_fallback=False,
                    strict_source_resolution=True,
                )
            except DaxTranslationError as exc:
                raise DaxTranslationError(
                    "SUBSTITUTEWITHINDEX ORDER BY expressions must reference columns from the index table argument"
                ) from exc
            parts.append(f"{rewritten} {direction}")
        return parts

    def _translate_calendar_table(self, call: Any) -> str:
        if len(call.args) < 2:
            raise DaxTranslationError("CALENDAR requires start date and end date")
        with self._allow_cross_table_context():
            start_fragment = self._translate_scalar(call.args[0])
            end_fragment = self._translate_scalar(call.args[1])
        start = self._scalar_fragment_sql_with_from(start_fragment)
        end = self._scalar_fragment_sql_with_from(end_fragment)
        return (
            "SELECT date_value AS Date FROM generate_series("
            f"CAST({start} AS DATE), CAST({end} AS DATE), INTERVAL '1 day'"
            ") AS gs(date_value)"
        )

    def _translate_generateseries_table(self, call: Any) -> str:
        if len(call.args) < 2:
            raise DaxTranslationError("GENERATESERIES requires start and end arguments")
        with self._allow_cross_table_context():
            start_fragment = self._translate_scalar(call.args[0])
            end_fragment = self._translate_scalar(call.args[1])
            step_fragment = (
                self._translate_scalar(call.args[2]) if len(call.args) > 2 else _SqlFragment("1", frozenset())
            )
        start = self._scalar_fragment_sql_with_from(start_fragment)
        end = self._scalar_fragment_sql_with_from(end_fragment)
        step = self._scalar_fragment_sql_with_from(step_fragment)
        return f"SELECT value FROM generate_series({start}, {end}, {step}) AS gs(value)"

    def _translate_selectcolumns(self, call: Any) -> str:
        if not call.args:
            raise DaxTranslationError("SELECTCOLUMNS requires a table and column pairs")
        pairs = call.args[1:]
        if len(pairs) % 2 != 0:
            raise DaxTranslationError("SELECTCOLUMNS requires name/expression pairs")

        base_expr = self._unwrap(call.args[0])
        base_table_name = self._table_name_from_expr(base_expr)
        select_parts = []
        if base_table_name is not None:
            self._ensure_table_context(base_table_name)
            tables_in_order = [base_table_name]
            seen_tables = {base_table_name.lower()}
            for i in range(0, len(pairs), 2):
                name_expr = pairs[i]
                value_expr = pairs[i + 1]
                alias = self._string_literal_value(name_expr)
                if alias is None:
                    raise DaxTranslationError("SELECTCOLUMNS name must be a string")
                fragment = self._translate_projection_scalar(value_expr)
                select_parts.append(f"{fragment.sql} AS {self._quote_identifier(alias)}")
                self._append_tables(tables_in_order, seen_tables, fragment.columns)
            from_sql = self._build_from_clause_for_tables(tables_in_order)
        else:
            from_sql, wrapped = self._table_source_from_expr(call.args[0])
            tables_in_order: list[str] = []
            seen_tables: set[str] = set()
            if wrapped and self._base_table:
                tables_in_order = [self._base_table]
                seen_tables = {self._base_table.lower()}
            for i in range(0, len(pairs), 2):
                name_expr = pairs[i]
                value_expr = pairs[i + 1]
                alias = self._string_literal_value(name_expr)
                if alias is None:
                    raise DaxTranslationError("SELECTCOLUMNS name must be a string")
                fragment = (
                    self._translate_projection_scalar(value_expr) if wrapped else self._translate_scalar(value_expr)
                )
                select_parts.append(f"{fragment.sql} AS {self._quote_identifier(alias)}")
                if wrapped and self._base_table:
                    self._append_tables(tables_in_order, seen_tables, fragment.columns)
            if wrapped and self._base_table and len(tables_in_order) > 1:
                from_sql = self._build_from_clause_for_wrapped_base(from_sql, self._base_table, tables_in_order)

        select_sql = ", ".join(select_parts)
        return f"SELECT {select_sql} FROM {from_sql}"

    def _translate_addcolumns(self, call: Any) -> str:
        if not call.args:
            raise DaxTranslationError("ADDCOLUMNS requires a table and column pairs")
        pairs = call.args[1:]
        if len(pairs) % 2 != 0:
            raise DaxTranslationError("ADDCOLUMNS requires name/expression pairs")

        base_expr = self._unwrap(call.args[0])
        base_table_name = self._table_name_from_expr(base_expr)
        select_parts: list[str] = []
        if base_table_name is not None:
            self._ensure_table_context(base_table_name)
            tables_in_order = [base_table_name]
            seen_tables = {base_table_name.lower()}
            for i in range(0, len(pairs), 2):
                alias = self._string_literal_value(pairs[i])
                if alias is None:
                    raise DaxTranslationError("ADDCOLUMNS name must be a string")
                fragment = self._translate_projection_scalar(pairs[i + 1])
                select_parts.append(f"{fragment.sql} AS {self._quote_identifier(alias)}")
                self._append_tables(tables_in_order, seen_tables, fragment.columns)
            from_sql = self._build_from_clause_for_tables(tables_in_order)
            if len(tables_in_order) == 1:
                select_parts.insert(0, "*")
            else:
                select_parts.insert(0, f"{self._table_sql(base_table_name)}.*")
        else:
            from_sql, wrapped = self._table_source_from_expr(call.args[0])
            select_parts = ["t.*" if wrapped else "*"]
            tables_in_order: list[str] = []
            seen_tables: set[str] = set()
            if wrapped and self._base_table:
                tables_in_order = [self._base_table]
                seen_tables = {self._base_table.lower()}
            for i in range(0, len(pairs), 2):
                alias = self._string_literal_value(pairs[i])
                if alias is None:
                    raise DaxTranslationError("ADDCOLUMNS name must be a string")
                fragment = (
                    self._translate_projection_scalar(pairs[i + 1]) if wrapped else self._translate_scalar(pairs[i + 1])
                )
                select_parts.append(f"{fragment.sql} AS {self._quote_identifier(alias)}")
                if wrapped and self._base_table:
                    self._append_tables(tables_in_order, seen_tables, fragment.columns)
            if wrapped and self._base_table and len(tables_in_order) > 1:
                from_sql = self._build_from_clause_for_wrapped_base(from_sql, self._base_table, tables_in_order)

        select_sql = ", ".join(select_parts)
        return f"SELECT {select_sql} FROM {from_sql}"

    def _translate_summarizecolumns(self, call: Any) -> str:
        if not call.args:
            raise DaxTranslationError("SUMMARIZECOLUMNS requires arguments")

        group_by: list[str] = []
        measures: list[str] = []
        filters: list[str] = []
        tables_in_order: list[str] = []
        seen_tables: set[str] = set()
        group_by_columns: set[ColumnRef] = set()
        filter_columns: set[ColumnRef] = set()

        args = list(call.args)
        idx = 0
        with self._allow_cross_table_context():
            while idx < len(args):
                arg = self._unwrap(args[idx])
                if isinstance(arg, self.dax.String):
                    break
                group_by_args = self._extract_group_by_args(arg)
                if group_by_args is not None:
                    for group_arg in group_by_args:
                        fragment = self._translate_scalar(group_arg)
                        group_by.append(fragment.sql)
                        group_by_columns.update(fragment.columns)
                        self._append_tables(tables_in_order, seen_tables, fragment.columns)
                    idx += 1
                    continue
                if isinstance(arg, self.dax.FunctionCall) and self._is_filter_table_candidate(arg):
                    filter_arg = arg
                    if arg.name.lower() in ("keepfilters", "nonvisual"):
                        inner = arg.args[0] if arg.args else None
                        if inner is None:
                            idx += 1
                            continue
                        filter_arg = self._unwrap(inner)
                    clauses, _overrides = self._translate_filter_candidate(filter_arg, keep=False)
                    for clause in clauses:
                        filters.append(clause.sql)
                        filter_columns.update(clause.columns)
                        self._append_tables(tables_in_order, seen_tables, clause.columns)
                    idx += 1
                    continue
                if isinstance(arg, self.dax.FunctionCall):
                    try:
                        self._translate_table(arg)
                    except DaxTranslationError as exc:
                        if not _is_unsupported_table_expression_error(exc):
                            raise
                    else:
                        nested_filters, _overrides = self._filters_from_table(arg)
                        for clause_sql in nested_filters:
                            clause = self._filter_clause_from_sql(clause_sql, keep=False)
                            filters.append(clause.sql)
                            filter_columns.update(clause.columns)
                            self._append_tables(tables_in_order, seen_tables, clause.columns)
                        idx += 1
                        continue
                table_name = self._table_name_from_expr(arg)
                if table_name is not None:
                    self._ensure_table_context(table_name)
                    if table_name.lower() not in seen_tables:
                        tables_in_order.append(table_name)
                        seen_tables.add(table_name.lower())
                    idx += 1
                    continue
                raise DaxTranslationError("Unsupported SUMMARIZECOLUMNS argument")

            remaining = args[idx:]
            if len(remaining) % 2 != 0:
                raise DaxTranslationError("SUMMARIZECOLUMNS requires name/expression pairs")

            with self._measure_eval_context(group_by_columns, filter_columns):
                for i in range(0, len(remaining), 2):
                    alias = self._string_literal_value(remaining[i])
                    if alias is None:
                        raise DaxTranslationError("SUMMARIZECOLUMNS name must be a string")
                    fragment = self._translate_scalar(remaining[i + 1])
                    measures.append(f"{fragment.sql} AS {self._quote_identifier(alias)}")
                    self._append_tables(tables_in_order, seen_tables, fragment.columns)

        if not group_by and not measures:
            raise DaxTranslationError("SUMMARIZECOLUMNS produced no columns")

        from_clause = self._build_from_clause_for_tables(tables_in_order)
        select_parts = group_by + measures
        select_sql = ", ".join(select_parts)
        group_by_sql = ""
        if group_by:
            group_by_sql = f" GROUP BY {', '.join(group_by)}"
        where_sql = ""
        if filters:
            where_sql = f" WHERE {' AND '.join(filters)}"
        return f"SELECT {select_sql} FROM {from_clause}{where_sql}{group_by_sql}"

    def _translate_summarize(self, call: Any) -> str:
        if len(call.args) < 2:
            raise DaxTranslationError("SUMMARIZE requires a table and at least one group-by column")

        base_expr = call.args[0]
        from_sql, wrapped = self._table_source_from_expr(base_expr)
        base_table = self._base_table.lower() if self._base_table else None

        group_by: list[str] = []
        measures: list[str] = []
        tables_in_order: list[str] = []
        seen_tables: set[str] = set()
        group_by_columns: set[ColumnRef] = set()

        if base_table:
            tables_in_order.append(self._base_table or "")
            seen_tables.add(base_table)

        args = list(call.args[1:])
        idx = 0
        with self._allow_cross_table_context():
            qualifier_ctx = self._prefer_unqualified_base_table_context() if wrapped else nullcontext()
            with qualifier_ctx:
                while idx < len(args):
                    arg = self._unwrap(args[idx])
                    if isinstance(arg, self.dax.String):
                        break
                    group_by_args = self._extract_group_by_args(arg)
                    if group_by_args is None and wrapped and isinstance(arg, self.dax.Identifier):
                        group_by_args = [arg]
                    if group_by_args is not None:
                        for group_arg in group_by_args:
                            if (
                                wrapped
                                and base_table is None
                                and isinstance(group_arg, (self.dax.BracketRef, self.dax.Identifier))
                            ):
                                fragment = _SqlFragment(self._quote_identifier(group_arg.name), frozenset())
                            else:
                                fragment = self._translate_scalar(group_arg)
                            group_by.append(fragment.sql)
                            group_by_columns.update(fragment.columns)
                            self._append_tables(tables_in_order, seen_tables, fragment.columns)
                        idx += 1
                        continue
                    raise DaxTranslationError("Unsupported SUMMARIZE group-by argument")

                remaining = args[idx:]
                if len(remaining) % 2 != 0:
                    raise DaxTranslationError("SUMMARIZE requires name/expression pairs after group-by columns")

                with self._measure_eval_context(group_by_columns, set()):
                    for i in range(0, len(remaining), 2):
                        alias = self._string_literal_value(remaining[i])
                        if alias is None:
                            raise DaxTranslationError("SUMMARIZE name must be a string")
                        fragment = self._translate_scalar(remaining[i + 1])
                        measures.append(f"{fragment.sql} AS {self._quote_identifier(alias)}")
                        self._append_tables(tables_in_order, seen_tables, fragment.columns)

        if not group_by and not measures:
            raise DaxTranslationError("SUMMARIZE produced no columns")

        if wrapped:
            if base_table:
                from_clause = self._build_from_clause_for_wrapped_base(
                    from_sql,
                    self._base_table or "",
                    tables_in_order,
                )
            else:
                from_clause = from_sql
        else:
            from_clause = self._build_from_clause_for_tables(tables_in_order)
        select_parts = group_by + measures
        select_sql = ", ".join(select_parts)
        group_by_sql = ""
        if group_by:
            group_by_sql = f" GROUP BY {', '.join(group_by)}"
        return f"SELECT {select_sql} FROM {from_clause}{group_by_sql}"

    def _build_from_clause_for_wrapped_base(self, from_sql: str, base_table: str, tables_in_order: list[str]) -> str:
        if not tables_in_order:
            return from_sql

        base_key = base_table.lower()
        from_parts = [from_sql]
        joined_tables = {base_key}
        joined_order = [base_table]

        for table in tables_in_order:
            table_key = table.lower()
            if table_key in joined_tables:
                continue
            path = self._find_relationship_path_from_joined(joined_order, table)
            if path is None:
                if self._allow_unrelated_table_cross_join:
                    self._append_unrelated_cross_join_warning(base_table, table)
                    from_parts.append(f"CROSS JOIN {self._table_sql(table)}")
                    joined_tables.add(table_key)
                    joined_order.append(table)
                    continue
                raise DaxTranslationError(f"No relationship path between {base_table} and {table}")

            for from_table, to_table, from_col, to_col in path:
                to_key = to_table.lower()
                if to_key in joined_tables:
                    continue
                left_table = "t" if from_table.lower() == base_key else self._table_sql(from_table)
                right_table = self._table_sql(to_table)
                from_col_sql = self._quote_identifier(from_col)
                to_col_sql = self._quote_identifier(to_col)
                from_parts.append(
                    f"LEFT JOIN {right_table} ON {left_table}.{from_col_sql} = {right_table}.{to_col_sql}"
                )
                joined_tables.add(to_key)
                joined_order.append(to_table)

        return " ".join(from_parts)

    def _extract_group_by_args(self, expr: Any) -> list[Any] | None:
        expr = self._unwrap(expr)
        if isinstance(expr, (self.dax.TableColumnRef, self.dax.HierarchyRef, self.dax.BracketRef)):
            return [expr]
        if isinstance(expr, self.dax.FunctionCall) and expr.name.lower() in (
            "rollup",
            "rollupgroup",
            "rollupissubtotal",
            "rollupaddissubtotal",
        ):
            group_by_args: list[Any] = []
            if expr.name.lower() == "rollupaddissubtotal":
                idx = 0
                while idx < len(expr.args):
                    current = self._unwrap(expr.args[idx])
                    if isinstance(current, self.dax.String):
                        idx += 1
                        continue
                    nested = self._extract_group_by_args(current)
                    if nested is None:
                        raise DaxTranslationError(
                            f"{expr.name} only supports column and hierarchy references in this context"
                        )
                    group_by_args.extend(nested)
                    idx += 1
                    if idx < len(expr.args) and isinstance(self._unwrap(expr.args[idx]), self.dax.String):
                        idx += 1
            else:
                for arg in expr.args:
                    nested = self._extract_group_by_args(arg)
                    if nested is None:
                        raise DaxTranslationError(
                            f"{expr.name} only supports column and hierarchy references in this context"
                        )
                    group_by_args.extend(nested)
            if not group_by_args:
                raise DaxTranslationError(f"{expr.name} requires at least one group-by argument")
            return group_by_args
        return None

    def _translate_topn(self, call: Any) -> str:
        if len(call.args) < 2:
            raise DaxTranslationError("TOPN requires count and table")
        count_sql = self._topn_numeric_arg_sql(call.args[0], function_name="TOPN", arg_name="count")
        table_expr = self._unwrap(call.args[1])
        base_table_name = self._table_name_from_expr(table_expr)
        if base_table_name is not None:
            self._ensure_table_context(base_table_name)
            tables_in_order = [base_table_name]
            seen_tables = {base_table_name.lower()}
            order_by_parts, order_columns = self._parse_order_by_parts_with_columns(
                call.args[2:],
                projection_safe=True,
            )
            self._append_tables(tables_in_order, seen_tables, order_columns)
            from_sql = self._build_from_clause_for_tables(tables_in_order)
            select_sql = "*" if len(tables_in_order) == 1 else f"{self._table_sql(base_table_name)}.*"
            order_by_sql = f" ORDER BY {', '.join(order_by_parts)}" if order_by_parts else ""
            return f"SELECT {select_sql} FROM {from_sql}{order_by_sql} LIMIT {count_sql}"

        from_sql, wrapped = self._table_source_from_expr(call.args[1])
        order_by_parts, order_columns = self._parse_order_by_parts_with_columns(
            call.args[2:],
            projection_safe=wrapped,
        )
        select_sql = "*"
        if wrapped and self._base_table:
            tables_in_order = [self._base_table]
            seen_tables = {self._base_table.lower()}
            self._append_tables(tables_in_order, seen_tables, order_columns)
            if len(tables_in_order) > 1:
                from_sql = self._build_from_clause_for_wrapped_base(from_sql, self._base_table, tables_in_order)
                select_sql = "t.*"
        order_by_sql = f" ORDER BY {', '.join(order_by_parts)}" if order_by_parts else ""
        return f"SELECT {select_sql} FROM {from_sql}{order_by_sql} LIMIT {count_sql}"

    def _translate_topnperlevel(self, call: Any) -> str:
        if len(call.args) < 3:
            raise DaxTranslationError("TOPNPERLEVEL requires count, group-by column(s), and table")
        count_sql = self._topn_numeric_arg_sql(call.args[0], function_name="TOPNPERLEVEL", arg_name="count")
        table_idx = self._topnperlevel_table_index(call)
        if table_idx is None:
            raise DaxTranslationError("TOPNPERLEVEL requires a table argument")
        table_expr = self._unwrap(call.args[table_idx])
        base_table_name = self._table_name_from_expr(table_expr)
        wrapped = False
        from_sql: str | None = None
        if base_table_name is None:
            from_sql, wrapped = self._table_source_from_expr(call.args[table_idx])
            if wrapped and self._base_table:
                base_table_name = self._base_table
        if base_table_name is not None:
            self._ensure_table_context(base_table_name)
        group_by_parts, group_by_columns = self._topnperlevel_group_by_parts(
            call,
            table_idx,
            projection_safe=wrapped,
        )
        if not group_by_parts:
            raise DaxTranslationError("TOPNPERLEVEL requires at least one group-by column")

        if base_table_name is not None and not wrapped:
            tables_in_order = [base_table_name]
            seen_tables = {base_table_name.lower()}
            self._append_tables(tables_in_order, seen_tables, group_by_columns)
            order_by_parts, order_by_columns = self._parse_order_by_parts_with_columns(
                call.args[table_idx + 1 :],
                projection_safe=True,
            )
            self._append_tables(tables_in_order, seen_tables, order_by_columns)
            from_sql = self._build_from_clause_for_tables(tables_in_order)
            row_projection = "*" if len(tables_in_order) == 1 else f"{self._table_sql(base_table_name)}.*"
            if not order_by_parts:
                order_by_parts = list(group_by_parts)
            rank_alias = "__topnperlevel_rank"
            ranked_sql = (
                f"SELECT {row_projection}, RANK() OVER (PARTITION BY {', '.join(group_by_parts)} "
                f"ORDER BY {', '.join(order_by_parts)}) AS {rank_alias} FROM {from_sql}"
            )
            return f"SELECT * EXCLUDE ({rank_alias}) FROM ({ranked_sql}) AS q WHERE {rank_alias} <= {count_sql}"

        if wrapped and base_table_name and from_sql is not None:
            tables_in_order = [base_table_name]
            seen_tables = {base_table_name.lower()}
            self._append_tables(tables_in_order, seen_tables, group_by_columns)
            order_by_parts, order_by_columns = self._parse_order_by_parts_with_columns(
                call.args[table_idx + 1 :],
                projection_safe=True,
            )
            self._append_tables(tables_in_order, seen_tables, order_by_columns)
            row_projection = "*"
            if len(tables_in_order) > 1:
                from_sql = self._build_from_clause_for_wrapped_base(from_sql, base_table_name, tables_in_order)
                row_projection = "t.*"
            if not order_by_parts:
                order_by_parts = list(group_by_parts)
            rank_alias = "__topnperlevel_rank"
            ranked_sql = (
                f"SELECT {row_projection}, RANK() OVER (PARTITION BY {', '.join(group_by_parts)} "
                f"ORDER BY {', '.join(order_by_parts)}) AS {rank_alias} FROM {from_sql}"
            )
            return f"SELECT * EXCLUDE ({rank_alias}) FROM ({ranked_sql}) AS q WHERE {rank_alias} <= {count_sql}"

        if from_sql is None:
            from_sql, _wrapped = self._table_source_from_expr(call.args[table_idx])
        order_by_parts, _order_columns = self._parse_order_by_parts_with_columns(
            call.args[table_idx + 1 :],
            projection_safe=wrapped,
        )
        if not order_by_parts:
            order_by_parts = list(group_by_parts)
        rank_alias = "__topnperlevel_rank"
        ranked_sql = (
            f"SELECT *, RANK() OVER (PARTITION BY {', '.join(group_by_parts)} "
            f"ORDER BY {', '.join(order_by_parts)}) AS {rank_alias} FROM {from_sql}"
        )
        return f"SELECT * EXCLUDE ({rank_alias}) FROM ({ranked_sql}) AS q WHERE {rank_alias} <= {count_sql}"

    def _translate_topnskip(self, call: Any) -> str:
        if len(call.args) < 3:
            raise DaxTranslationError("TOPNSKIP requires count, skip, and table")
        count_sql = self._topn_numeric_arg_sql(call.args[0], function_name="TOPNSKIP", arg_name="count")
        skip_sql = self._topn_numeric_arg_sql(call.args[1], function_name="TOPNSKIP", arg_name="skip")
        table_expr = self._unwrap(call.args[2])
        base_table_name = self._table_name_from_expr(table_expr)
        if base_table_name is not None:
            self._ensure_table_context(base_table_name)
            tables_in_order = [base_table_name]
            seen_tables = {base_table_name.lower()}
            order_by_parts, order_columns = self._parse_order_by_parts_with_columns(
                call.args[3:],
                projection_safe=True,
            )
            self._append_tables(tables_in_order, seen_tables, order_columns)
            from_sql = self._build_from_clause_for_tables(tables_in_order)
            select_sql = "*" if len(tables_in_order) == 1 else f"{self._table_sql(base_table_name)}.*"
            order_by_sql = f" ORDER BY {', '.join(order_by_parts)}" if order_by_parts else ""
            return f"SELECT {select_sql} FROM {from_sql}{order_by_sql} LIMIT {count_sql} OFFSET {skip_sql}"

        from_sql, wrapped = self._table_source_from_expr(call.args[2])
        order_by_parts, order_columns = self._parse_order_by_parts_with_columns(
            call.args[3:],
            projection_safe=wrapped,
        )
        select_sql = "*"
        if wrapped and self._base_table:
            tables_in_order = [self._base_table]
            seen_tables = {self._base_table.lower()}
            self._append_tables(tables_in_order, seen_tables, order_columns)
            if len(tables_in_order) > 1:
                from_sql = self._build_from_clause_for_wrapped_base(from_sql, self._base_table, tables_in_order)
                select_sql = "t.*"
        order_by_sql = f" ORDER BY {', '.join(order_by_parts)}" if order_by_parts else ""
        return f"SELECT {select_sql} FROM {from_sql}{order_by_sql} LIMIT {count_sql} OFFSET {skip_sql}"

    def _topnperlevel_table_index(self, call: Any) -> int | None:
        for idx, arg in enumerate(call.args[1:], start=1):
            candidate = self._unwrap(arg)
            if self._table_name_from_expr(candidate) is not None:
                return idx
            if isinstance(candidate, (self.dax.FunctionCall, self.dax.TableConstructor)):
                return idx
        return None

    def _topnperlevel_group_by_parts(
        self,
        call: Any,
        table_idx: int,
        *,
        projection_safe: bool = False,
    ) -> tuple[list[str], set[ColumnRef]]:
        group_parts: list[str] = []
        columns: set[ColumnRef] = set()
        context = nullcontext() if projection_safe else self._allow_cross_table_context()
        with context:
            for raw_arg in call.args[1:table_idx]:
                group_args = self._extract_group_by_args(raw_arg)
                if group_args is None:
                    raise DaxTranslationError("TOPNPERLEVEL group-by arguments must be column or hierarchy references")
                for group_arg in group_args:
                    fragment = (
                        self._translate_projection_scalar(group_arg)
                        if projection_safe
                        else self._translate_scalar(group_arg)
                    )
                    group_parts.append(fragment.sql)
                    columns.update(fragment.columns)
        return group_parts, columns

    def _parse_order_by_parts(self, args: list[Any]) -> list[str]:
        order_by_parts, _columns = self._parse_order_by_parts_with_columns(args)
        return order_by_parts

    def _parse_order_by_parts_with_columns(
        self,
        args: list[Any],
        *,
        projection_safe: bool = False,
    ) -> tuple[list[str], set[ColumnRef]]:
        order_by_parts: list[str] = []
        columns: set[ColumnRef] = set()
        idx = 0
        while idx < len(args):
            expr = args[idx]
            direction = None
            if idx + 1 < len(args):
                direction = self._identifier_literal_value(args[idx + 1])
                if direction is not None and direction.upper() in ("ASC", "DESC"):
                    idx += 2
                else:
                    direction = None
                    idx += 1
            else:
                idx += 1
            fragment = self._translate_projection_scalar(expr) if projection_safe else self._translate_scalar(expr)
            expr_sql = fragment.sql
            columns.update(fragment.columns)
            if direction:
                order_by_parts.append(f"{expr_sql} {direction.upper()}")
            else:
                order_by_parts.append(expr_sql)
        return order_by_parts, columns

    def _translate_groupby(self, call: Any) -> str:
        if len(call.args) < 2:
            raise DaxTranslationError("GROUPBY requires a table and at least one group-by column")
        return self._translate_summarize(call)

    def _translate_generate_table(self, call: Any) -> str:
        if len(call.args) < 2:
            raise DaxTranslationError(f"{call.name} requires at least two table arguments")
        left_sql = self._translate_table(call.args[0])
        try:
            right_sql = self._translate_table(call.args[1])
        except DaxTranslationError as exc:
            if str(exc) != "DAX table expressions must reference a single base table":
                raise
            with self._allow_cross_table_context():
                right_sql = self._translate_table(call.args[1])
        left_tables = self._collect_table_references(call.args[0])
        left_columns = _query_output_columns(left_sql)
        if not left_columns and left_tables and _query_uses_star_projection(left_sql):
            left_columns = self._known_columns_for_tables(left_tables)
        ambiguous_left_columns = self._ambiguous_columns_for_tables(left_tables)
        left_column_aliases, ambiguous_left_lineage_aliases = _query_output_lineage_aliases(left_sql)
        right_source_tables = _query_source_table_names(right_sql)
        right_local_columns = self._known_columns_for_tables(right_source_tables)
        if left_tables:
            right_sql = _rewrite_expr_for_alias(
                right_sql,
                "l",
                source_tables=left_tables,
                source_columns=left_columns,
                source_column_aliases=left_column_aliases,
                ambiguous_source_aliases=ambiguous_left_lineage_aliases,
                local_columns=right_local_columns,
                ambiguous_source_columns=ambiguous_left_columns,
                allow_fallback=False,
                strict_source_resolution=True,
            )
        if call.name.lower() == "generateall":
            return f"SELECT * FROM ({left_sql}) AS l LEFT JOIN LATERAL ({right_sql}) AS r ON TRUE"
        return f"SELECT * FROM ({left_sql}) AS l CROSS JOIN LATERAL ({right_sql}) AS r"

    def _translate_table_with_isolated_base_context(self, expr: Any, *, preserve_result_base: bool = False) -> str:
        prior_base_table = self._base_table
        self._base_table = None
        try:
            sql = self._translate_table(expr)
            translated_base_table = self._base_table
        finally:
            self._base_table = prior_base_table
        if preserve_result_base and prior_base_table is None and translated_base_table is not None:
            self._base_table = translated_base_table
        return sql

    def _translate_addmissingitems_table(self, call: Any) -> str:
        table_idx = self._addmissingitems_table_index(call)
        if table_idx is None:
            raise DaxTranslationError("ADDMISSINGITEMS requires a table expression argument")
        table_arg = call.args[table_idx]
        base_sql = self._translate_table(table_arg)

        group_specs: list[Any] = []
        domain_filter_clauses: list[str] = []
        domain_filter_columns: set[ColumnRef] = set()
        other_args = [arg for idx, arg in enumerate(call.args) if idx != table_idx]
        for candidate_arg in other_args:
            if self._extract_group_by_args(candidate_arg) is not None:
                group_specs.append(candidate_arg)
                continue
            with self._allow_cross_table_context():
                nested_filters, _overrides = self._filters_from_table(candidate_arg)
            domain_filter_clauses.extend(nested_filters)
            for clause_sql in nested_filters:
                domain_filter_columns.update(self._columns_from_sql(clause_sql))

        if not group_specs:
            return base_sql

        group_parts: list[_SqlFragment] = []
        seen_group_sql: set[str] = set()
        tables_in_order: list[str] = []
        seen_tables: set[str] = set()
        with self._allow_cross_table_context():
            for raw_arg in group_specs:
                group_args = self._extract_group_by_args(raw_arg)
                if group_args is None:
                    raise DaxTranslationError(
                        "ADDMISSINGITEMS group-by arguments must be column or hierarchy references"
                    )
                for group_arg in group_args:
                    fragment = self._translate_scalar(group_arg)
                    group_key = fragment.sql.strip().lower()
                    if group_key in seen_group_sql:
                        continue
                    seen_group_sql.add(group_key)
                    group_parts.append(fragment)
                    self._append_tables(tables_in_order, seen_tables, fragment.columns)

        if not group_parts or not tables_in_order:
            return base_sql

        if domain_filter_columns:
            self._append_tables(tables_in_order, seen_tables, domain_filter_columns)

        domain_selects: list[str] = []
        projections: list[str] = []
        projection_names: list[str] = []
        join_predicates: list[str] = []
        base_output_cols = _query_output_columns(base_sql)
        base_output_keys = {name.lower() for name in base_output_cols if name and name != "*"}
        for idx, fragment in enumerate(group_parts):
            key_alias = f"__addmissingitems_k{idx}"
            output_name = _column_name_from_expr_sql(fragment.sql) or f"value{idx + 1}"
            domain_selects.append(f"{fragment.sql} AS {key_alias}")
            projections.append(f"d.{key_alias} AS {self._quote_identifier(output_name)}")
            projection_names.append(output_name)
            if output_name.lower() in base_output_keys:
                base_expr = _rewrite_expr_for_alias(fragment.sql, "b")
                join_predicates.append(f"{base_expr} IS NOT DISTINCT FROM d.{key_alias}")

        domain_from = self._build_from_clause_for_tables(tables_in_order)
        domain_where = f" WHERE {' AND '.join(domain_filter_clauses)}" if domain_filter_clauses else ""
        domain_sql = f"SELECT DISTINCT {', '.join(domain_selects)} FROM {domain_from}{domain_where}"
        on_sql = " AND ".join(join_predicates) if join_predicates else "TRUE"
        projected_name_keys = {name.lower() for name in projection_names}
        duplicate_base_cols = sorted(
            (name for name in base_output_cols if name and name != "*" and name.lower() in projected_name_keys),
            key=str.lower,
        )
        base_select = "b.*"
        if duplicate_base_cols:
            excluded_cols = ", ".join(self._quote_identifier(name) for name in duplicate_base_cols)
            base_select = f"b.* EXCLUDE ({excluded_cols})"
        select_sql = ", ".join([*projections, base_select])
        return f"SELECT {select_sql} FROM ({domain_sql}) AS d LEFT JOIN ({base_sql}) AS b ON {on_sql}"

    def _translate_datatable_table(self, call: Any) -> str:
        if len(call.args) < 3:
            raise DaxTranslationError("DATATABLE requires column definitions and row values")
        rows_expr = self._unwrap(call.args[-1])
        if not isinstance(rows_expr, self.dax.TableConstructor):
            raise DaxTranslationError("DATATABLE requires a table-constructor row argument")

        column_args = list(call.args[:-1])
        if len(column_args) % 2 != 0:
            raise DaxTranslationError("DATATABLE requires name/datatype column pairs")

        columns: list[str] = []
        for idx in range(0, len(column_args), 2):
            col_name = self._string_literal_value(column_args[idx])
            if col_name is None:
                raise DaxTranslationError("DATATABLE column names must be strings")
            columns.append(col_name)

        if not columns:
            raise DaxTranslationError("DATATABLE requires at least one column")

        row_values_sql: list[str] = []
        for row in rows_expr.rows:
            normalized_row = self._normalize_datatable_row(row, len(columns))
            if normalized_row is None:
                raise DaxTranslationError("DATATABLE row width must match column definition count")
            fragments = [self._translate_scalar(value) for value in normalized_row]
            row_values_sql.append("(" + ", ".join(fragment.sql for fragment in fragments) + ")")

        aliased_columns = ", ".join(self._quote_identifier(name) for name in columns)
        if not row_values_sql:
            nulls = ", ".join(f"CAST(NULL AS VARCHAR) AS {self._quote_identifier(name)}" for name in columns)
            return f"SELECT {nulls} WHERE 1 = 0"
        return f"SELECT * FROM (VALUES {', '.join(row_values_sql)}) AS t({aliased_columns})"

    def _translate_relatedtable_table(self, call: Any) -> str:
        if not call.args:
            raise DaxTranslationError("RELATEDTABLE requires a table argument")
        target = self._unwrap(call.args[0])
        table_name = self._table_name_from_expr(target)
        if table_name is None:
            raise DaxTranslationError("RELATEDTABLE requires a table reference argument")
        with self._allow_cross_table_context():
            self._ensure_table_context(table_name)
        return f"SELECT * FROM {self._table_sql(table_name)}"

    def _normalize_datatable_row(self, row: list[Any], width: int) -> list[Any] | None:
        if len(row) == width:
            return row
        if len(row) == 1:
            nested = self._unwrap(row[0])
            if isinstance(nested, self.dax.TableConstructor):
                flattened: list[Any] = []
                for nested_row in nested.rows:
                    if len(nested_row) != 1:
                        return None
                    flattened.append(nested_row[0])
                if len(flattened) == width:
                    return flattened
        return None

    def _addmissingitems_table_arg(self, call: Any) -> Any | None:
        table_idx = self._addmissingitems_table_index(call)
        if table_idx is not None:
            return call.args[table_idx]
        return None

    def _addmissingitems_table_index(self, call: Any) -> int | None:
        group_tables = self._addmissingitems_group_tables(call)
        non_group_candidates: list[tuple[int, Any]] = []
        for idx, arg in enumerate(call.args):
            candidate = self._unwrap(arg)
            if self._extract_group_by_args(candidate) is not None:
                continue
            if isinstance(candidate, (self.dax.TableRef, self.dax.Identifier, self.dax.TableConstructor)):
                non_group_candidates.append((idx, candidate))
                continue
            if isinstance(candidate, self.dax.FunctionCall):
                non_group_candidates.append((idx, candidate))

        if not non_group_candidates:
            return None

        for idx, candidate in non_group_candidates:
            core_candidate = self._addmissingitems_table_core_expr(candidate)
            if isinstance(core_candidate, self.dax.FunctionCall) and core_candidate.name.lower() == "summarizecolumns":
                return idx
        for idx, candidate in non_group_candidates:
            core_candidate = self._addmissingitems_table_core_expr(candidate)
            if self._is_addmissingitems_strong_preferred_table_core(core_candidate):
                return idx
        for idx, candidate in non_group_candidates:
            core_candidate = self._addmissingitems_table_core_expr(candidate)
            if self._is_addmissingitems_preferred_table_core(
                core_candidate
            ) and self._addmissingitems_core_has_non_group_table(core_candidate, group_tables):
                return idx
        for idx, candidate in non_group_candidates:
            core_candidate = self._addmissingitems_table_core_expr(candidate)
            if self._is_addmissingitems_likely_main_table_core(core_candidate, group_tables):
                return idx
        for idx, candidate in non_group_candidates:
            core_candidate = self._addmissingitems_table_core_expr(candidate)
            if self._is_addmissingitems_preferred_table_core(core_candidate):
                return idx

        non_filter_candidates = [
            idx for idx, candidate in non_group_candidates if not self._is_filter_table_candidate(candidate)
        ]
        if non_filter_candidates:
            return non_filter_candidates[0]

        for idx, candidate in non_group_candidates:
            if not self._is_explicit_filter_wrapper(candidate):
                return idx

        return non_group_candidates[0][0]

    def _addmissingitems_group_tables(self, call: Any) -> set[str]:
        tables: set[str] = set()
        for arg in call.args:
            candidate = self._unwrap(arg)
            group_args = self._extract_group_by_args(candidate)
            if group_args is None:
                continue
            for group_arg in group_args:
                tables.update(self._collect_table_references(group_arg))
        return {table.lower() for table in tables}

    def _addmissingitems_core_has_non_group_table(self, expr: Any, group_tables: set[str]) -> bool:
        tables = {table.lower() for table in self._collect_table_references(expr)}
        if not tables:
            return False
        return any(table not in group_tables for table in tables)

    def _addmissingitems_table_core_expr(self, expr: Any) -> Any:
        expr = self._unwrap(expr)
        if not isinstance(expr, self.dax.FunctionCall):
            return expr
        name = expr.name.lower()
        if (
            name in ("keepfilters", "nonvisual", "filter", "renamecolumns", "keepcolumns", "removecolumns")
            and expr.args
        ):
            return self._addmissingitems_table_core_expr(expr.args[0])
        if name == "calculatetable" and expr.args:
            return self._addmissingitems_table_core_expr(expr.args[0])
        return expr

    def _is_addmissingitems_preferred_table_core(self, expr: Any) -> bool:
        expr = self._unwrap(expr)
        if not isinstance(expr, self.dax.FunctionCall):
            return False
        name = expr.name.lower()
        return name in (
            "summarize",
            "groupby",
            "row",
            "selectcolumns",
            "addcolumns",
            "renamecolumns",
            "keepcolumns",
            "removecolumns",
            "topn",
            "topnskip",
            "topnperlevel",
            "union",
            "crossjoin",
            "naturalinnerjoin",
            "naturalleftouterjoin",
            "intersect",
            "except",
            "generate",
            "generateall",
            "calendar",
            "generateseries",
            "datatable",
        )

    def _is_addmissingitems_strong_preferred_table_core(self, expr: Any) -> bool:
        expr = self._unwrap(expr)
        if not isinstance(expr, self.dax.FunctionCall):
            return False
        name = expr.name.lower()
        return name in (
            "summarize",
            "groupby",
            "row",
            "selectcolumns",
            "addcolumns",
            "renamecolumns",
            "keepcolumns",
            "removecolumns",
            "topn",
            "topnskip",
            "topnperlevel",
            "generate",
            "generateall",
        )

    def _is_addmissingitems_likely_main_table_core(self, expr: Any, group_tables: set[str]) -> bool:
        expr = self._unwrap(expr)
        table_name = self._table_name_from_expr(expr)
        if table_name is None:
            return self._addmissingitems_core_has_non_group_table(expr, group_tables)
        return table_name.lower() not in group_tables

    def _is_filter_table_candidate(self, expr: Any) -> bool:
        expr = self._unwrap(expr)
        if not isinstance(expr, self.dax.FunctionCall):
            return False
        name = expr.name.lower()
        return name in (
            "filter",
            "calculatetable",
            "keepfilters",
            "nonvisual",
            "treatas",
            "datesbetween",
            "datesinperiod",
            "datesytd",
            "datesmtd",
            "datesqtd",
            "dateswtd",
            "sameperiodlastyear",
            "dateadd",
            "parallelperiod",
            "previousday",
            "previousweek",
            "previousmonth",
            "previousquarter",
            "previousyear",
            "nextday",
            "nextweek",
            "nextmonth",
            "nextquarter",
            "nextyear",
            "all",
            "allnoblankrow",
            "allselected",
            "allcrossfiltered",
            "removefilters",
            "allexcept",
            "values",
            "filters",
            "distinct",
            "renamecolumns",
            "keepcolumns",
            "removecolumns",
            "substitutewithindex",
            "union",
            "crossjoin",
            "naturalinnerjoin",
            "naturalleftouterjoin",
            "intersect",
            "except",
        )

    def _is_table_filter_candidate_expr(self, expr: Any) -> bool:
        expr = self._unwrap(expr)
        if isinstance(expr, self.dax.TableRef):
            return True
        if isinstance(expr, self.dax.Identifier):
            if self._is_known_measure_identifier(expr.name):
                return False
            return self._table_exists(expr.name)
        return False

    def _is_known_measure_identifier(self, name: str) -> bool:
        if self.model_name:
            model_key = self.model_name.lower()
            for table, measure_names in self.measure_names_by_table.items():
                if table.lower() != model_key:
                    continue
                if name in measure_names:
                    return True
                lower = name.lower()
                return any(known.lower() == lower for known in measure_names)
            return False
        return self._resolve_measure_reference(name) is not None

    def _table_exists(self, name: str) -> bool:
        key = name.lower()
        for table_name in self.column_sql_by_table:
            if table_name.lower() == key:
                return True
        for table_name in self.measure_names_by_table:
            if table_name.lower() == key:
                return True
        return False

    def _is_explicit_filter_wrapper(self, expr: Any) -> bool:
        expr = self._unwrap(expr)
        if not isinstance(expr, self.dax.FunctionCall):
            return False
        name = expr.name.lower()
        return name in (
            "filter",
            "calculatetable",
            "keepfilters",
            "nonvisual",
            "treatas",
            "datesbetween",
            "datesinperiod",
            "datesytd",
            "datesmtd",
            "datesqtd",
            "dateswtd",
            "sameperiodlastyear",
            "dateadd",
            "parallelperiod",
            "previousday",
            "previousweek",
            "previousmonth",
            "previousquarter",
            "previousyear",
            "nextday",
            "nextweek",
            "nextmonth",
            "nextquarter",
            "nextyear",
            "all",
            "allnoblankrow",
            "allselected",
            "allcrossfiltered",
            "removefilters",
            "allexcept",
        )

    def _collect_table_references(self, expr: Any) -> set[str]:
        expr = self._unwrap(expr)
        if isinstance(expr, self.dax.TableRef):
            return {expr.table.name}
        if isinstance(expr, self.dax.TableColumnRef):
            return {expr.table.name}
        if isinstance(expr, self.dax.HierarchyRef):
            return {expr.table.name}
        if isinstance(expr, self.dax.Identifier):
            return {expr.name}
        if isinstance(expr, self.dax.FunctionCall):
            tables: set[str] = set()
            for arg in expr.args:
                tables.update(self._collect_table_references(arg))
            return tables
        if isinstance(expr, self.dax.TableConstructor):
            tables: set[str] = set()
            for row in expr.rows:
                for value in row:
                    tables.update(self._collect_table_references(value))
            return tables
        if isinstance(expr, self.dax.Unary):
            return self._collect_table_references(expr.expr)
        if isinstance(expr, self.dax.Binary):
            return self._collect_table_references(expr.left) | self._collect_table_references(expr.right)
        if isinstance(expr, self.dax.VarBlock):
            tables: set[str] = set()
            for decl in expr.decls:
                tables.update(self._collect_table_references(decl.expr))
            tables.update(self._collect_table_references(expr.body))
            return tables
        if isinstance(expr, self.dax.Paren):
            return self._collect_table_references(expr.expr)
        return set()

    def _known_columns_for_tables(self, tables: set[str]) -> set[str]:
        columns: set[str] = set()
        for table in tables:
            column_map = self._column_map_for_table(table)
            for source_name, mapped in column_map.items():
                columns.add(source_name)
                mapped_name = _identifier_name_from_sql(mapped)
                if mapped_name:
                    columns.add(mapped_name)
        return columns

    def _column_map_for_table(self, table: str) -> dict[str, str]:
        if table in self.column_sql_by_table:
            return self.column_sql_by_table[table]
        table_key = table.lower()
        for table_name, column_map in self.column_sql_by_table.items():
            if table_name.lower() == table_key:
                return column_map
        return {}

    def _known_table_references(self, expr: Any) -> set[str]:
        known: set[str] = set()
        for table in self._collect_table_references(expr):
            resolved = self._resolve_known_table_name(table)
            if resolved is not None:
                known.add(resolved)
        return known

    def _resolve_known_table_name(self, table: str) -> str | None:
        if table in self.column_sql_by_table or table in self.measure_names_by_table:
            return table
        key = table.lower()
        for table_name in self.column_sql_by_table:
            if table_name.lower() == key:
                return table_name
        for table_name in self.measure_names_by_table:
            if table_name.lower() == key:
                return table_name
        return None

    def _table_has_known_column(self, table: str, column: str) -> bool:
        column_key = column.lower()
        for source_name, mapped in self._column_map_for_table(table).items():
            if source_name.lower() == column_key:
                return True
            mapped_name = _identifier_name_from_sql(mapped)
            if mapped_name and mapped_name.lower() == column_key:
                return True
        return False

    def _infer_table_expr_output_columns(self, expr: Any, sql: str) -> set[str]:
        shape_columns = self._infer_table_expr_output_columns_by_shape(expr)
        columns = _query_output_columns(sql)
        uses_star = _query_uses_star_projection(sql)
        if columns and not uses_star:
            return columns
        tables = self._collect_table_references(expr)
        if tables and not columns and uses_star:
            return self._known_columns_for_tables(tables)
        if columns and uses_star:
            star_qualifiers = _query_star_projection_qualifiers(sql)
            qualified_star_tables = {name for name in star_qualifiers if name and self._table_exists(name)}
            if qualified_star_tables:
                return columns | self._known_columns_for_tables(qualified_star_tables)
            if None in star_qualifiers and len(tables) == 1:
                return columns | self._known_columns_for_tables(tables)
            if shape_columns:
                return shape_columns
            return set()
        if shape_columns:
            return shape_columns
        return set()

    def _infer_table_expr_output_columns_by_shape(self, expr: Any) -> set[str]:
        counts = self._infer_table_expr_output_column_counts_by_shape(expr)
        names: set[str] = set()
        for key in counts:
            names.add(key)
        return names

    def _infer_table_expr_output_column_counts(self, expr: Any, sql: str) -> dict[str, int]:
        counts = self._infer_table_expr_output_column_counts_by_shape(expr)
        sql_counts = _query_output_column_name_counts(sql)
        if not counts:
            return sql_counts
        if not sql_counts:
            return counts
        merged = dict(sql_counts)
        for key, value in counts.items():
            merged[key] = max(merged.get(key, 0), value)
        return merged

    def _infer_table_expr_output_column_counts_by_shape(self, expr: Any) -> dict[str, int]:
        expr = self._unwrap(expr)

        table_name = self._table_name_from_expr(expr)
        if table_name is not None:
            counts: dict[str, int] = {}
            for column_name in self._known_columns_for_tables({table_name}):
                counts[column_name.lower()] = 1
            return counts

        if isinstance(expr, self.dax.FunctionCall):
            name = expr.name.lower()
            passthrough_first_arg = {
                "filter",
                "calculatetable",
                "keepfilters",
                "nonvisual",
                "distinct",
                "all",
                "allnoblankrow",
                "allselected",
                "allcrossfiltered",
                "removefilters",
            }
            if name in passthrough_first_arg and expr.args:
                return self._infer_table_expr_output_column_counts_by_shape(expr.args[0])
            if name == "topn" and len(expr.args) >= 2:
                return self._infer_table_expr_output_column_counts_by_shape(expr.args[1])
            if name == "topnskip" and len(expr.args) >= 3:
                return self._infer_table_expr_output_column_counts_by_shape(expr.args[2])
            if name == "topnperlevel":
                table_idx = self._topnperlevel_table_index(expr)
                if table_idx is not None and table_idx < len(expr.args):
                    return self._infer_table_expr_output_column_counts_by_shape(expr.args[table_idx])
                return {}
            if name == "selectcolumns":
                if len(expr.args) < 1:
                    return {}
                pairs = expr.args[1:]
                if len(pairs) % 2 != 0:
                    return {}
                counts: dict[str, int] = {}
                for i in range(0, len(pairs), 2):
                    alias = self._string_literal_value(pairs[i])
                    if alias is None:
                        return {}
                    alias_key = alias.lower()
                    counts[alias_key] = counts.get(alias_key, 0) + 1
                return counts
            if name == "addcolumns":
                if not expr.args:
                    return {}
                base = self._infer_table_expr_output_column_counts_by_shape(expr.args[0])
                if not base:
                    return {}
                pairs = expr.args[1:]
                if len(pairs) % 2 != 0:
                    return {}
                out = dict(base)
                for i in range(0, len(pairs), 2):
                    alias = self._string_literal_value(pairs[i])
                    if alias is None:
                        return {}
                    alias_key = alias.lower()
                    out[alias_key] = out.get(alias_key, 0) + 1
                return out
            if name == "keepcolumns":
                if len(expr.args) < 2:
                    return {}
                keep_counts: dict[str, int] = {}
                for raw_arg in expr.args[1:]:
                    try:
                        keep_name = self._table_column_arg_name(raw_arg, function_name="KEEPCOLUMNS")
                    except DaxTranslationError:
                        return {}
                    keep_counts.setdefault(keep_name.lower(), 1)
                return keep_counts
            if name == "removecolumns":
                if len(expr.args) < 2:
                    return {}
                base = self._infer_table_expr_output_column_counts_by_shape(expr.args[0])
                if not base:
                    return {}
                remove_keys: set[str] = set()
                for raw_arg in expr.args[1:]:
                    try:
                        remove_name = self._table_column_arg_name(raw_arg, function_name="REMOVECOLUMNS")
                    except DaxTranslationError:
                        return {}
                    remove_keys.add(remove_name.lower())
                return {key: value for key, value in base.items() if key not in remove_keys}
            if name == "renamecolumns":
                if len(expr.args) < 3:
                    return {}
                pairs = expr.args[1:]
                if len(pairs) % 2 != 0:
                    return {}
                base = self._infer_table_expr_output_column_counts_by_shape(expr.args[0])
                if not base:
                    return {}
                out = dict(base)
                for i in range(0, len(pairs), 2):
                    try:
                        source = self._table_column_arg_name(pairs[i], function_name="RENAMECOLUMNS")
                        target = self._table_column_arg_name(pairs[i + 1], function_name="RENAMECOLUMNS")
                    except DaxTranslationError:
                        return {}
                    source_key = source.lower()
                    target_key = target.lower()
                    source_count = out.pop(source_key, None)
                    if source_count is None:
                        return {}
                    out[target_key] = out.get(target_key, 0) + source_count
                return out
            if name in ("union", "intersect", "except") and expr.args:
                return self._infer_table_expr_output_column_counts_by_shape(expr.args[0])
            if name in (
                "crossjoin",
                "naturalinnerjoin",
                "naturallefterouterjoin",
                "naturallefterjoin",
                "naturalleftouterjoin",
                "generate",
                "generateall",
            ):
                counts: dict[str, int] = {}
                for arg in expr.args:
                    nested = self._infer_table_expr_output_column_counts_by_shape(arg)
                    for key, value in nested.items():
                        counts[key] = counts.get(key, 0) + value
                return counts
            if name == "row":
                if len(expr.args) < 2 or len(expr.args) % 2 != 0:
                    return {}
                counts: dict[str, int] = {}
                for i in range(0, len(expr.args), 2):
                    alias = self._string_literal_value(expr.args[i])
                    if alias is None:
                        return {}
                    alias_key = alias.lower()
                    counts[alias_key] = counts.get(alias_key, 0) + 1
                return counts
            if name == "datatable":
                if len(expr.args) < 3:
                    return {}
                column_args = list(expr.args[:-1])
                if len(column_args) % 2 != 0:
                    return {}
                counts: dict[str, int] = {}
                for i in range(0, len(column_args), 2):
                    col_name = self._string_literal_value(column_args[i])
                    if col_name is None:
                        return {}
                    col_key = col_name.lower()
                    counts[col_key] = counts.get(col_key, 0) + 1
                return counts

        return {}

    def _ambiguous_columns_for_tables(self, tables: set[str]) -> set[str]:
        counts: dict[str, int] = {}
        for table in tables:
            column_map = self.column_sql_by_table.get(table, {})
            table_columns: set[str] = set()
            for source_name, mapped in column_map.items():
                table_columns.add(source_name.lower())
                mapped_name = _identifier_name_from_sql(mapped)
                if mapped_name:
                    table_columns.add(mapped_name.lower())
            for column_name in table_columns:
                counts[column_name] = counts.get(column_name, 0) + 1
        return {column_name for column_name, count in counts.items() if count > 1}

    def _translate_values_table(self, call: Any) -> str:
        if not call.args:
            raise DaxTranslationError("VALUES requires an argument")
        target = self._unwrap(call.args[0])

        table_name = self._table_name_from_expr(target)
        if table_name is not None:
            self._ensure_table_context(table_name)
            return f"SELECT DISTINCT * FROM {self._table_sql(table_name)}"

        if isinstance(target, (self.dax.FunctionCall, self.dax.TableConstructor)):
            base_sql = self._translate_table(target)
            return f"SELECT DISTINCT * FROM ({base_sql}) AS t"

        with self._allow_cross_table_context():
            fragment = self._translate_scalar(target)

        referenced_tables: dict[str, str] = {}
        for column in fragment.columns:
            if not column.table:
                continue
            key = column.table.lower()
            referenced_tables.setdefault(key, column.table)

        tables: list[str] = []
        if self._base_table and self._base_table.lower() in referenced_tables:
            tables.append(referenced_tables.pop(self._base_table.lower()))
        for _, table_name in sorted(referenced_tables.items(), key=lambda item: item[0]):
            tables.append(table_name)

        if tables:
            if len(tables) == 1:
                table_sql = self._table_sql(tables[0])
            else:
                table_sql = self._build_from_clause_for_tables(tables)
        else:
            table_sql = self._default_table_sql()

        return f"SELECT DISTINCT {fragment.sql} FROM {table_sql}"

    def _translate_filters_table(self, call: Any) -> str:
        if not call.args:
            raise DaxTranslationError("FILTERS requires an argument")
        values_call = self.dax.FunctionCall(name="VALUES", args=[call.args[0]])
        return self._translate_values_table(values_call)

    def _translate_all_like_table(self, call: Any) -> str:
        name = call.name.lower()
        if name == "allexcept":
            if not call.args:
                raise DaxTranslationError("ALLEXCEPT requires at least a table argument")
            target = self._unwrap(call.args[0])
            table_name = self._table_name_from_expr(target)
            if table_name is None:
                raise DaxTranslationError("ALLEXCEPT first argument must be a table reference")
            self._ensure_table_context(table_name)
            return f"SELECT * FROM {self._table_sql(table_name)}"

        if not call.args:
            table_name = self.model_name or self._base_table
            if table_name is None:
                raise DaxTranslationError(f"{call.name} requires a table argument without a table context")
            self._ensure_table_context(table_name)
            return f"SELECT * FROM {self._table_sql(table_name)}"

        target = self._unwrap(call.args[0])
        table_name = self._table_name_from_expr(target)
        if table_name is not None:
            self._ensure_table_context(table_name)
            return f"SELECT * FROM {self._table_sql(table_name)}"

        if isinstance(target, (self.dax.FunctionCall, self.dax.TableConstructor)):
            base_sql = self._translate_table(target)
            return f"SELECT DISTINCT * FROM ({base_sql}) AS t"

        values_call = self.dax.FunctionCall(name="VALUES", args=[target])
        return self._translate_values_table(values_call)

    def _translate_date_boundary_table(self, call: Any) -> str:
        fragment = self._translate_function_scalar(call)
        tables_in_order: list[str] = []
        seen_tables: set[str] = set()
        self._append_tables(tables_in_order, seen_tables, fragment.columns)
        from_clause = (
            self._build_from_clause_for_tables(tables_in_order) if tables_in_order else self._default_table_sql()
        )
        return f"SELECT {fragment.sql} AS value1 FROM {from_clause}"

    def _translate_distinct_table(self, call: Any) -> str:
        if not call.args:
            raise DaxTranslationError("DISTINCT requires an argument")
        target = self._unwrap(call.args[0])
        if isinstance(
            target, (self.dax.Identifier, self.dax.TableRef, self.dax.FunctionCall, self.dax.TableConstructor)
        ):
            base_sql = self._translate_table(target)
            return f"SELECT DISTINCT * FROM ({base_sql}) AS t"
        return self._translate_values_table(call)

    def _translate_renamecolumns_table(self, call: Any) -> str:
        if len(call.args) < 3:
            raise DaxTranslationError("RENAMECOLUMNS requires a table expression and at least one old/new column pair")

        rename_args = call.args[1:]
        if len(rename_args) % 2 != 0:
            raise DaxTranslationError("RENAMECOLUMNS requires old/new column argument pairs")

        base_expr = call.args[0]
        base_sql = self._translate_table(base_expr)
        available_columns = self._infer_table_expr_output_columns(base_expr, base_sql)
        input_column_counts = self._infer_table_expr_output_column_counts(base_expr, base_sql)
        ambiguous_input_columns = {key for key, count in input_column_counts.items() if count > 1}
        input_tables = self._known_table_references(base_expr)
        available_lookup = {name.lower(): name for name in available_columns}
        rename_parts: list[str] = []
        seen_sources: set[str] = set()
        seen_targets: set[str] = set()
        for idx in range(0, len(rename_args), 2):
            source_spec = self._table_column_arg_spec(rename_args[idx], function_name="RENAMECOLUMNS")
            source_name = source_spec.name
            target_name = self._table_column_arg_name(rename_args[idx + 1], function_name="RENAMECOLUMNS")
            source_key = source_name.lower()
            target_key = target_name.lower()
            self._validate_table_qualified_column_arg(
                source_spec,
                function_name="RENAMECOLUMNS",
                input_tables=input_tables,
            )
            if source_key in ambiguous_input_columns:
                raise DaxTranslationError(
                    f"RENAMECOLUMNS source column '{source_name}' is ambiguous in input table expression"
                )
            if available_lookup and source_key not in available_lookup:
                raise DaxTranslationError(
                    f"RENAMECOLUMNS source column '{source_name}' is not present in input table expression"
                )
            if source_key in seen_sources:
                raise DaxTranslationError("RENAMECOLUMNS source columns must be unique")
            if target_key in seen_targets:
                raise DaxTranslationError("RENAMECOLUMNS target column names must be unique")
            seen_sources.add(source_key)
            seen_targets.add(target_key)
            resolved_source = available_lookup.get(source_key, source_name)
            rename_parts.append(f"{self._quote_identifier(resolved_source)} AS {self._quote_identifier(target_name)}")

        if not rename_parts:
            raise DaxTranslationError("RENAMECOLUMNS requires at least one valid old/new column pair")

        return f"SELECT * RENAME ({', '.join(rename_parts)}) FROM ({base_sql}) AS t"

    def _translate_keepcolumns_table(self, call: Any) -> str:
        if len(call.args) < 2:
            raise DaxTranslationError("KEEPCOLUMNS requires a table expression and at least one column argument")

        base_expr = call.args[0]
        base_sql = self._translate_table(base_expr)
        available_columns = self._infer_table_expr_output_columns(base_expr, base_sql)
        input_column_counts = self._infer_table_expr_output_column_counts(base_expr, base_sql)
        ambiguous_input_columns = {key for key, count in input_column_counts.items() if count > 1}
        input_tables = self._known_table_references(base_expr)
        available_lookup = {name.lower(): name for name in available_columns}
        keep_names: list[str] = []
        seen: set[str] = set()
        for raw_arg in call.args[1:]:
            spec = self._table_column_arg_spec(raw_arg, function_name="KEEPCOLUMNS")
            column_name = spec.name
            key = column_name.lower()
            self._validate_table_qualified_column_arg(spec, function_name="KEEPCOLUMNS", input_tables=input_tables)
            if key in ambiguous_input_columns:
                raise DaxTranslationError(f"KEEPCOLUMNS column '{column_name}' is ambiguous in input table expression")
            if available_lookup and key not in available_lookup:
                raise DaxTranslationError(
                    f"KEEPCOLUMNS column '{column_name}' is not present in input table expression"
                )
            if key in seen:
                continue
            seen.add(key)
            keep_names.append(available_lookup.get(key, column_name))

        if not keep_names:
            raise DaxTranslationError("KEEPCOLUMNS requires at least one valid column argument")

        projections = ", ".join(f"t.{self._quote_identifier(name)}" for name in keep_names)
        return f"SELECT {projections} FROM ({base_sql}) AS t"

    def _translate_removecolumns_table(self, call: Any) -> str:
        if len(call.args) < 2:
            raise DaxTranslationError("REMOVECOLUMNS requires a table expression and at least one column argument")

        base_expr = call.args[0]
        base_sql = self._translate_table(base_expr)
        available_columns = self._infer_table_expr_output_columns(base_expr, base_sql)
        input_column_counts = self._infer_table_expr_output_column_counts(base_expr, base_sql)
        ambiguous_input_columns = {key for key, count in input_column_counts.items() if count > 1}
        input_tables = self._known_table_references(base_expr)
        available_lookup = {name.lower(): name for name in available_columns}
        exclude_names: list[str] = []
        seen: set[str] = set()
        for raw_arg in call.args[1:]:
            spec = self._table_column_arg_spec(raw_arg, function_name="REMOVECOLUMNS")
            column_name = spec.name
            key = column_name.lower()
            self._validate_table_qualified_column_arg(spec, function_name="REMOVECOLUMNS", input_tables=input_tables)
            if key in ambiguous_input_columns:
                raise DaxTranslationError(
                    f"REMOVECOLUMNS column '{column_name}' is ambiguous in input table expression"
                )
            if available_lookup and key not in available_lookup:
                raise DaxTranslationError(
                    f"REMOVECOLUMNS column '{column_name}' is not present in input table expression"
                )
            if key in seen:
                continue
            seen.add(key)
            exclude_names.append(available_lookup.get(key, column_name))

        if not exclude_names:
            raise DaxTranslationError("REMOVECOLUMNS requires at least one valid column argument")

        excluded = ", ".join(self._quote_identifier(name) for name in exclude_names)
        return f"SELECT * EXCLUDE ({excluded}) FROM ({base_sql}) AS t"

    def _table_column_arg_spec(self, expr: Any, *, function_name: str) -> TableColumnArg:
        expr = self._unwrap(expr)
        if isinstance(expr, self.dax.TableColumnRef):
            return TableColumnArg(name=expr.column, table=expr.table.name)
        if isinstance(expr, self.dax.HierarchyRef):
            return TableColumnArg(
                name=expr.levels[-1] if expr.levels else expr.column,
                table=expr.table.name,
            )
        if isinstance(expr, self.dax.String):
            return TableColumnArg(name=expr.value)
        if isinstance(expr, self.dax.Identifier):
            return TableColumnArg(name=expr.name)
        if isinstance(expr, self.dax.BracketRef):
            return TableColumnArg(name=expr.name)
        raise DaxTranslationError(f"{function_name} column arguments must be column references or names")

    def _table_column_arg_name(self, expr: Any, *, function_name: str) -> str:
        return self._table_column_arg_spec(expr, function_name=function_name).name

    def _validate_table_qualified_column_arg(
        self,
        arg: TableColumnArg,
        *,
        function_name: str,
        input_tables: set[str],
    ) -> None:
        if arg.table is None:
            return

        table_name = self._resolve_known_table_name(arg.table)
        if table_name is None:
            raise DaxTranslationError(f"{function_name} column '{arg.name}' references unknown table '{arg.table}'")
        if table_name not in input_tables:
            raise DaxTranslationError(
                f"{function_name} column '{arg.name}' references table '{arg.table}' not present in input table expression"
            )
        if not self._table_has_known_column(table_name, arg.name):
            raise DaxTranslationError(
                f"{function_name} column '{arg.name}' is not present on referenced table '{arg.table}'"
            )

    def _translate_calculatetable(self, call: Any) -> str:
        if not call.args:
            raise DaxTranslationError("CALCULATETABLE requires a table expression")
        from_sql, wrapped, inherited_filters = self._flatten_calculatetable_source(call.args[0])
        if wrapped:
            with self._prefer_unqualified_base_table_context():
                (
                    new_filters,
                    removals,
                    retentions,
                    remove_all,
                    clear_non_keep,
                    _overrides,
                ) = self._translate_filter_args(call.args[1:])
        else:
            (
                new_filters,
                removals,
                retentions,
                remove_all,
                clear_non_keep,
                _overrides,
            ) = self._translate_filter_args(call.args[1:])
        inherited = [self._filter_clause_from_sql(sql, keep=True) for sql in inherited_filters]
        combined = self._merge_filter_clauses(inherited, new_filters)
        combined = self._apply_non_keep_clear(combined, remove_all, clear_non_keep)
        retained = self._apply_filter_retentions(combined, retentions, remove_all)
        predicates = self._apply_filter_removals(retained, removals, remove_all)
        select_sql = "*"
        if wrapped and self._base_table:
            tables_in_order = [self._base_table]
            seen_tables = {self._base_table.lower()}
            for predicate in predicates:
                if self._is_opaque_filter_predicate(predicate):
                    continue
                self._append_tables(tables_in_order, seen_tables, self._columns_from_sql(predicate))
            if len(tables_in_order) > 1:
                from_sql = self._build_from_clause_for_wrapped_base(from_sql, self._base_table, tables_in_order)
                select_sql = "t.*"
        elif not wrapped:
            base_table_name = self._base_table or self.model_name
            if base_table_name:
                tables_in_order = [base_table_name]
                seen_tables = {base_table_name.lower()}
                for predicate in predicates:
                    if self._is_opaque_filter_predicate(predicate):
                        continue
                    self._append_tables(tables_in_order, seen_tables, self._columns_from_sql(predicate))
                if len(tables_in_order) > 1:
                    from_sql = self._build_from_clause_for_tables(tables_in_order)
                    select_sql = f"{self._table_sql(base_table_name)}.*"
        if not predicates:
            return f"SELECT {select_sql} FROM {from_sql}"
        return f"SELECT {select_sql} FROM {from_sql} WHERE {' AND '.join(predicates)}"

    def _flatten_calculatetable_source(self, expr: Any) -> tuple[str, bool, list[str]]:
        expr = self._unwrap(expr)
        if isinstance(expr, self.dax.FunctionCall) and expr.name.lower() == "calculatetable":
            if not expr.args:
                return self._default_table_sql(), False, []

            from_sql, wrapped, inherited_filters = self._flatten_calculatetable_source(expr.args[0])
            if wrapped:
                with self._prefer_unqualified_base_table_context():
                    (
                        new_filters,
                        removals,
                        retentions,
                        remove_all,
                        clear_non_keep,
                        _overrides,
                    ) = self._translate_filter_args(expr.args[1:])
            else:
                (
                    new_filters,
                    removals,
                    retentions,
                    remove_all,
                    clear_non_keep,
                    _overrides,
                ) = self._translate_filter_args(expr.args[1:])

            inherited = [self._filter_clause_from_sql(sql, keep=True) for sql in inherited_filters]
            combined = self._merge_filter_clauses(inherited, new_filters)
            combined = self._apply_non_keep_clear(combined, remove_all, clear_non_keep)
            retained = self._apply_filter_retentions(combined, retentions, remove_all)
            predicates = self._apply_filter_removals(retained, removals, remove_all)
            return from_sql, wrapped, predicates

        from_sql, wrapped = self._table_source_from_expr(expr)
        return from_sql, wrapped, []

    def _table_source_from_expr(self, expr: Any) -> tuple[str, bool]:
        expr = self._unwrap(expr)
        table_name = self._table_name_from_expr(expr)
        if table_name is not None:
            self._ensure_table_context(table_name)
            return self._table_sql(table_name), False
        base_sql = self._translate_table(expr)
        return f"({base_sql}) AS t", True

    def _translate_scalar(self, expr: Any) -> _SqlFragment:
        expr = self._unwrap(expr)

        if isinstance(expr, self.dax.Number):
            return _SqlFragment(expr.value, frozenset())
        if isinstance(expr, self.dax.String):
            return _SqlFragment(self._quote_string(expr.value), frozenset())
        if isinstance(expr, self.dax.Boolean):
            return _SqlFragment("TRUE" if expr.value else "FALSE", frozenset())
        if isinstance(expr, self.dax.Blank):
            return _SqlFragment("NULL", frozenset())
        if isinstance(expr, self.dax.Parameter):
            return _SqlFragment(f"@{expr.name}", frozenset())
        if isinstance(expr, self.dax.Identifier):
            return self._translate_identifier(expr.name)
        if isinstance(expr, self.dax.BracketRef):
            return self._translate_identifier(expr.name)
        if isinstance(expr, self.dax.TableColumnRef):
            return self._translate_table_column(expr.table.name, expr.column)
        if isinstance(expr, self.dax.HierarchyRef):
            column = expr.levels[-1] if expr.levels else expr.column
            return self._translate_table_column(expr.table.name, column)
        if isinstance(expr, self.dax.Unary):
            inner = self._translate_scalar(expr.expr)
            if expr.op == self.dax.UnaryOp.not_:
                return inner.wrap(f"NOT {inner.sql}")
            if expr.op == self.dax.UnaryOp.minus:
                return inner.wrap(f"-{inner.sql}")
            if expr.op == self.dax.UnaryOp.plus:
                return inner.wrap(f"+{inner.sql}")
        if isinstance(expr, self.dax.Binary):
            return self._translate_binary(expr)
        if isinstance(expr, self.dax.VarBlock):
            return self._with_vars(expr, self._translate_scalar, expr.body)
        if isinstance(expr, self.dax.Paren):
            inner = self._translate_scalar(expr.expr)
            return inner.wrap(f"({inner.sql})")
        if isinstance(expr, self.dax.TableConstructor):
            raise DaxTranslationError("Table constructor not valid in scalar context")
        if isinstance(expr, self.dax.FunctionCall):
            return self._translate_function_scalar(expr)

        raise DaxTranslationError(f"Unsupported DAX expression type '{type(expr).__name__}'")

    def _translate_binary(self, expr: Any) -> _SqlFragment:
        left = self._translate_scalar(expr.left)
        right = self._translate_scalar(expr.right)
        op = expr.op

        if op == self.dax.BinaryOp.in_:
            in_list = self._translate_in_list(expr.right)
            sql = f"{left.sql} IN {in_list}"
            return _SqlFragment(sql, left.columns | right.columns)

        op_map = {
            self.dax.BinaryOp.or_: "OR",
            self.dax.BinaryOp.and_: "AND",
            self.dax.BinaryOp.eq: "=",
            self.dax.BinaryOp.strict_eq: "=",
            self.dax.BinaryOp.neq: "<>",
            self.dax.BinaryOp.lt: "<",
            self.dax.BinaryOp.lte: "<=",
            self.dax.BinaryOp.gt: ">",
            self.dax.BinaryOp.gte: ">=",
            self.dax.BinaryOp.concat: "||",
            self.dax.BinaryOp.add: "+",
            self.dax.BinaryOp.sub: "-",
            self.dax.BinaryOp.mul: "*",
            self.dax.BinaryOp.div: "/",
            self.dax.BinaryOp.pow: "POWER",
        }

        if op == self.dax.BinaryOp.pow:
            sql = f"POWER({left.sql}, {right.sql})"
        else:
            op_sql = op_map.get(op)
            if not op_sql:
                raise DaxTranslationError("Unsupported binary operator")
            sql = f"({left.sql} {op_sql} {right.sql})"

        return _SqlFragment(sql, left.columns | right.columns)

    def _translate_function_scalar(self, call: Any) -> _SqlFragment:
        name = call.name.lower()
        args = call.args

        if name in ("ignore", "nonvisual"):
            if not args:
                raise DaxTranslationError(f"{call.name} requires an argument")
            if len(args) > 1:
                raise DaxTranslationError(f"{call.name} supports exactly one argument")
            return self._translate_scalar(args[0])
        if name == "evaluateandlog":
            if not args:
                raise DaxTranslationError("EVALUATEANDLOG requires an argument")
            if len(args) > 1:
                raise DaxTranslationError("EVALUATEANDLOG supports exactly one argument")
            return self._translate_scalar(args[0])
        if name == "nameof":
            return self._translate_nameof(args)
        if name == "convert":
            return self._translate_convert(args)
        if name == "lookupvalue":
            return self._translate_lookupvalue(args)
        if name == "related":
            return self._translate_related(args)
        if name == "value":
            return self._translate_value(args)
        if name == "concatenate":
            return self._translate_concatenate(args)
        if name == "concatenatex":
            return self._translate_concatenatex(args)
        if name == "roundup":
            return self._translate_roundup(args)
        if name == "round":
            return self._translate_round(args)
        if name == "rounddown":
            return self._translate_rounddown(args)
        if name == "int":
            return self._translate_int(args)
        if name == "trunc":
            return self._translate_trunc(args)
        if name == "mround":
            return self._translate_mround(args)
        if name == "ceiling":
            return self._translate_ceiling(args)
        if name == "floor":
            return self._translate_floor(args)
        if name == "abs":
            return self._translate_abs(args)
        if name == "mod":
            return self._translate_mod(args)
        if name == "power":
            return self._translate_power(args)
        if name == "sqrt":
            return self._translate_sqrt(args)
        if name == "exp":
            return self._translate_exp(args)
        if name == "ln":
            return self._translate_ln(args)
        if name == "log10":
            return self._translate_log10(args)
        if name == "log":
            return self._translate_log(args)
        if name == "pi":
            return self._translate_pi(args)
        if name == "blank":
            if args:
                raise DaxTranslationError("BLANK does not take arguments")
            return _SqlFragment("NULL", frozenset())
        if name == "true":
            if args:
                raise DaxTranslationError("TRUE does not take arguments")
            return _SqlFragment("TRUE", frozenset())
        if name == "false":
            if args:
                raise DaxTranslationError("FALSE does not take arguments")
            return _SqlFragment("FALSE", frozenset())
        if name == "if":
            return self._translate_if(args)
        if name == "switch":
            return self._translate_switch(args)
        if name == "selectedvalue":
            return self._translate_selectedvalue(args)
        if name in ("hasonevalue", "hasonefilter"):
            return self._translate_hasone(args)
        if name in ("firstnonblank", "firstnonblankvalue"):
            return self._translate_first_last_nonblank(args, pick="first")
        if name in ("lastnonblank", "lastnonblankvalue"):
            return self._translate_first_last_nonblank(args, pick="last")
        if name in ("firstdate", "lastdate"):
            return self._translate_first_last_date(args, pick="first" if name == "firstdate" else "last")
        if name in ("startofmonth", "startofquarter", "startofyear"):
            grain = {"startofmonth": "month", "startofquarter": "quarter", "startofyear": "year"}[name]
            return self._translate_period_boundary_date(args, grain=grain, end=False)
        if name in ("endofmonth", "endofquarter", "endofyear"):
            grain = {"endofmonth": "month", "endofquarter": "quarter", "endofyear": "year"}[name]
            return self._translate_period_boundary_date(args, grain=grain, end=True)
        if name == "date":
            return self._translate_date_ctor(args)
        if name == "time":
            return self._translate_time_ctor(args)
        if name == "datevalue":
            return self._translate_datevalue(args)
        if name == "timevalue":
            return self._translate_timevalue(args)
        if name == "edate":
            return self._translate_edate(args)
        if name == "eomonth":
            return self._translate_eomonth(args)
        if name == "datediff":
            return self._translate_datediff(args)
        if name == "weekday":
            return self._translate_weekday(args)
        if name == "weeknum":
            return self._translate_weeknum(args)
        if name == "containsstring":
            return self._translate_containsstring(args, exact=False)
        if name == "containsstringexact":
            return self._translate_containsstring(args, exact=True)
        if name == "containsrow":
            return self._translate_containsrow(args)
        if name == "upper":
            if not args:
                raise DaxTranslationError("UPPER requires an argument")
            if len(args) > 1:
                raise DaxTranslationError("UPPER supports exactly one argument")
            target = self._translate_scalar(args[0])
            return _SqlFragment(f"UPPER({target.sql})", target.columns)
        if name == "lower":
            if not args:
                raise DaxTranslationError("LOWER requires an argument")
            if len(args) > 1:
                raise DaxTranslationError("LOWER supports exactly one argument")
            target = self._translate_scalar(args[0])
            return _SqlFragment(f"LOWER({target.sql})", target.columns)
        if name == "len":
            return self._translate_len(args)
        if name == "replace":
            return self._translate_replace(args)
        if name == "substitute":
            return self._translate_substitute(args)
        if name == "rept":
            return self._translate_rept(args)
        if name == "trim":
            return self._translate_trim(args)
        if name == "left":
            return self._translate_left(args)
        if name == "right":
            return self._translate_right(args)
        if name == "mid":
            return self._translate_mid(args)
        if name == "search":
            return self._translate_find_search(args, case_sensitive=False, func="SEARCH")
        if name == "find":
            return self._translate_find_search(args, case_sensitive=True, func="FIND")
        if name == "exact":
            return self._translate_exact(args)
        if name == "today":
            if args:
                raise DaxTranslationError("TODAY does not take arguments")
            return _SqlFragment("CURRENT_DATE", frozenset())
        if name == "now":
            if args:
                raise DaxTranslationError("NOW does not take arguments")
            return _SqlFragment("CURRENT_TIMESTAMP", frozenset())
        if name == "utcnow":
            if args:
                raise DaxTranslationError("UTCNOW does not take arguments")
            return _SqlFragment("CURRENT_TIMESTAMP", frozenset())
        if name == "utctoday":
            if args:
                raise DaxTranslationError("UTCTODAY does not take arguments")
            return _SqlFragment("CURRENT_DATE", frozenset())
        if name in ("year", "month", "day", "hour", "minute", "second", "quarter"):
            return self._translate_date_part(args, part=name)
        if name == "rand":
            if args:
                raise DaxTranslationError("RAND does not take arguments")
            return _SqlFragment("RANDOM()", frozenset())
        if name == "randbetween":
            return self._translate_randbetween(args)
        if name == "format":
            return self._translate_format(args)
        if name == "iferror":
            return self._translate_iferror(args)
        if name == "isinscope":
            return self._translate_isinscope(args)
        if name in ("isfiltered", "iscrossfiltered"):
            return self._translate_isfiltered(args)
        if name == "coalesce":
            return self._translate_coalesce(args)
        if name == "divide":
            return self._translate_divide(args)
        if name == "and":
            return self._translate_and_or(args, op="AND")
        if name == "or":
            return self._translate_and_or(args, op="OR")
        if name == "not":
            if not args:
                raise DaxTranslationError("NOT requires an argument")
            if len(args) > 1:
                raise DaxTranslationError("NOT supports exactly one argument")
            inner = self._translate_scalar(args[0])
            return inner.wrap(f"NOT {inner.sql}")
        if name == "isblank":
            if not args:
                raise DaxTranslationError("ISBLANK requires an argument")
            if len(args) > 1:
                raise DaxTranslationError("ISBLANK supports exactly one argument")
            inner = self._translate_scalar(args[0])
            return inner.wrap(f"{inner.sql} IS NULL")
        if name == "isempty":
            return self._translate_isempty(args)
        if name == "calculate":
            with self._allow_cross_table_context():
                metric = self._translate_calculate(call)
            return self._metric_to_scalar_fragment(metric)
        if name in ("min", "max"):
            return self._translate_min_max(call)
        if name in (
            "sumx",
            "averagex",
            "avgx",
            "minx",
            "maxx",
            "medianx",
            "countx",
            "countax",
            "totalytd",
            "totalmtd",
            "totalqtd",
            "totalwtd",
        ):
            with self._allow_cross_table_context():
                metric = self.translate_metric(call)
            return self._metric_to_scalar_fragment(metric)
        if name in (
            "sum",
            "average",
            "averagea",
            "avg",
            "min",
            "mina",
            "max",
            "maxa",
            "median",
            "count",
            "countrows",
            "counta",
            "countblank",
            "distinctcount",
            "distinctcountnoblank",
            "approximatedistinctcount",
        ):
            return self._translate_inline_aggregate(call)
        if name in ("selectedmeasure", "selectedmeasurename", "selectedmeasureformatstring", "isselectedmeasure"):
            raise DaxTranslationError(f"{call.name} is only supported in calculation group expressions")

        if self._is_table_function_name(name):
            raise DaxTranslationError(f"{call.name} returns a table and is not valid in scalar context")
        if name in ("userelationship", "crossfilter"):
            raise DaxTranslationError(f"{call.name} is only valid in CALCULATE filter arguments")

        raise DaxTranslationError(f"Unsupported scalar function '{call.name}'")

    def _is_table_function_name(self, name: str) -> bool:
        return name in {
            "filter",
            "row",
            "selectcolumns",
            "addcolumns",
            "summarizecolumns",
            "summarize",
            "groupby",
            "topn",
            "topnperlevel",
            "union",
            "crossjoin",
            "naturalinnerjoin",
            "naturalleftouterjoin",
            "intersect",
            "except",
            "topnskip",
            "calendar",
            "generateseries",
            "datatable",
            "relatedtable",
            "calculatetable",
            "addmissingitems",
            "treatas",
            "datesbetween",
            "datesinperiod",
            "datesytd",
            "datesmtd",
            "datesqtd",
            "dateswtd",
            "dateadd",
            "parallelperiod",
            "sameperiodlastyear",
            "previousday",
            "previousweek",
            "previousmonth",
            "previousquarter",
            "previousyear",
            "nextday",
            "nextweek",
            "nextmonth",
            "nextquarter",
            "nextyear",
            "rollup",
            "rollupgroup",
            "rollupaddissubtotal",
            "rollupissubtotal",
            "values",
            "filters",
            "distinct",
            "renamecolumns",
            "keepcolumns",
            "removecolumns",
            "substitutewithindex",
            "detailrows",
            "all",
            "allnoblankrow",
            "allselected",
            "allcrossfiltered",
            "removefilters",
            "allexcept",
            "generate",
            "generateall",
            "currentgroup",
        }

    def _translate_inline_aggregate(self, call: Any) -> _SqlFragment:
        name = call.name.lower()
        agg_map = {
            "sum": "SUM",
            "average": "AVG",
            "averagea": "AVG",
            "avg": "AVG",
            "min": "MIN",
            "mina": "MIN",
            "max": "MAX",
            "maxa": "MAX",
            "median": "MEDIAN",
            "count": "COUNT",
            "countrows": "COUNT",
            "counta": "COUNT",
            "countblank": "COUNT",
            "distinctcount": "COUNT",
            "distinctcountnoblank": "COUNT",
            "approximatedistinctcount": "COUNT",
        }
        func = agg_map[name]
        if name == "countrows":
            if len(call.args) > 1:
                raise DaxTranslationError("COUNTROWS supports at most one argument")
            if call.args:
                target = self._unwrap(call.args[0])
                if isinstance(target, self.dax.FunctionCall) and target.name.lower() == "currentgroup":
                    return _SqlFragment("COUNT(*)", frozenset())
                distinct_translation = self._translate_countrows_distinct_table(call.args[0])
                if distinct_translation is not None and distinct_translation.sql:
                    columns = set(self._columns_from_sql(distinct_translation.sql))
                    return _SqlFragment(f"COUNT(DISTINCT {distinct_translation.sql})", frozenset(columns))
                target = self._unwrap(call.args[0])
                table_name = self._table_name_from_expr(target)
                if table_name is not None:
                    self._ensure_table_context(table_name)
                    default_table = self.model_name or self._base_table
                    if default_table and table_name.lower() == default_table.lower():
                        return _SqlFragment("COUNT(*)", frozenset())
                    grouped_count = self._translate_grouped_countrows_for_table(table_name)
                    if grouped_count is not None:
                        return grouped_count
                filters, _overrides = self._filters_from_table(call.args[0])
                distinct_table = self._countrows_distinct_table_name(call.args[0])
                if distinct_table is not None:
                    grouped_distinct = self._translate_grouped_countrows_distinct_for_table(
                        distinct_table, filters=filters
                    )
                    if grouped_distinct is not None:
                        return grouped_distinct
                if filters:
                    sql = self._render_aggregate_sql("count", None, filters)
                    columns = set()
                    for clause in filters:
                        columns.update(self._columns_from_sql(clause))
                    return _SqlFragment(sql, frozenset(columns))
                base_table = self._countrows_base_table_name(call.args[0])
                if base_table is not None:
                    grouped_count = self._translate_grouped_countrows_for_table(base_table)
                    if grouped_count is not None:
                        return grouped_count
                from_sql, _wrapped = self._table_source_from_expr(call.args[0])
                return _SqlFragment(f"(SELECT COUNT(*) FROM {from_sql})", frozenset())
            return _SqlFragment("COUNT(*)", frozenset())
        if not call.args:
            raise DaxTranslationError(f"{call.name} requires an argument")
        if len(call.args) > 1:
            raise DaxTranslationError(f"{call.name} supports exactly one argument")
        arg = self._translate_scalar(call.args[0])
        if name == "countblank":
            return _SqlFragment(f"COUNT(CASE WHEN {arg.sql} IS NULL THEN 1 END)", arg.columns)
        if name == "distinctcount":
            return _SqlFragment(f"COUNT(DISTINCT {arg.sql})", arg.columns)
        if name == "distinctcountnoblank":
            return _SqlFragment(f"COUNT(DISTINCT {arg.sql})", arg.columns)
        if name == "approximatedistinctcount":
            return _SqlFragment(f"COUNT(DISTINCT {arg.sql})", arg.columns)
        return _SqlFragment(f"{func}({arg.sql})", arg.columns)

    def _translate_min_max(self, call: Any) -> _SqlFragment:
        name = call.name.lower()
        if not call.args:
            raise DaxTranslationError(f"{call.name} requires an argument")
        if len(call.args) == 1:
            return self._translate_inline_aggregate(call)
        if len(call.args) == 2:
            left = self._translate_scalar(call.args[0])
            right = self._translate_scalar(call.args[1])
            func = "LEAST" if name == "min" else "GREATEST"
            return _SqlFragment(f"{func}({left.sql}, {right.sql})", left.columns | right.columns)
        raise DaxTranslationError(f"{call.name} supports one aggregate argument or two scalar arguments")

    def _translate_grouped_countrows_for_table(self, table_name: str) -> _SqlFragment | None:
        if not self._current_group_by_columns:
            return None

        group_tables = {col.table for col in self._current_group_by_columns if col.table}
        if not group_tables:
            return None

        count_column = self._grouped_countrows_column_from_relationship_path(table_name, group_tables)
        if count_column is None:
            table_key = table_name.lower()
            has_relationship_path = any(
                group_table.lower() == table_key or self._find_relationship_path(group_table, table_name) is not None
                for group_table in group_tables
            )
            if not has_relationship_path:
                return None
            count_column = self._representative_table_column(table_name)
        if count_column is None:
            return None

        count_sql = self._column_sql(table_name, count_column)
        return _SqlFragment(f"COUNT({count_sql})", frozenset({ColumnRef(table_name, count_column)}))

    def _grouped_countrows_column_from_relationship_path(self, table_name: str, group_tables: set[str]) -> str | None:
        table_key = table_name.lower()
        best_path: list[tuple[str, str, str, str]] | None = None
        for group_table in group_tables:
            if group_table.lower() == table_key:
                return self._representative_table_column(table_name)
            path = self._find_relationship_path(group_table, table_name)
            if path is None:
                continue
            if best_path is None or len(path) < len(best_path):
                best_path = path
        if not best_path:
            return None
        _from_table, to_table, _from_col, to_col = best_path[-1]
        if to_table.lower() != table_key:
            return None
        return to_col

    def _representative_table_column(self, table_name: str) -> str | None:
        table_key = table_name.lower()
        for candidate_table, column_map in self.column_sql_by_table.items():
            if candidate_table.lower() != table_key:
                continue
            if column_map:
                return next(iter(column_map))
            break
        for edge in self._relationship_edges:
            if edge.from_table.lower() == table_key:
                return edge.from_column
            if edge.to_table.lower() == table_key:
                return edge.to_column
        return None

    def _countrows_base_table_name(self, expr: Any) -> str | None:
        expr = self._unwrap(expr)
        table_name = self._table_name_from_expr(expr)
        if table_name is not None:
            return table_name
        if isinstance(expr, self.dax.FunctionCall) and expr.args:
            name = expr.name.lower()
            if name in ("calculatetable", "keepfilters", "nonvisual"):
                return self._countrows_base_table_name(expr.args[0])
        return None

    def _translate_grouped_countrows_distinct_for_table(
        self, table_name: str, filters: list[str]
    ) -> _SqlFragment | None:
        count_column = self._grouped_countrows_column_from_relationship_path(
            table_name, {col.table for col in self._current_group_by_columns if col.table}
        )
        if count_column is None:
            return None
        count_sql = self._column_sql(table_name, count_column)
        sql = self._render_aggregate_sql("count_distinct", count_sql, filters)
        columns = {ColumnRef(table_name, count_column)}
        for clause in filters:
            columns.update(self._columns_from_sql(clause))
        return _SqlFragment(sql, frozenset(columns))

    def _countrows_distinct_table_name(self, expr: Any) -> str | None:
        expr = self._unwrap(expr)
        if not isinstance(expr, self.dax.FunctionCall):
            return None
        name = expr.name.lower()
        if name in ("values", "filters", "distinct"):
            if not expr.args:
                return None
            return self._table_name_from_expr(expr.args[0])
        if name in ("calculatetable", "keepfilters", "nonvisual"):
            if not expr.args:
                return None
            return self._countrows_distinct_table_name(expr.args[0])
        return None

    def _metric_to_scalar_fragment(self, metric: MetricTranslation) -> _SqlFragment:
        if metric.type in ("time_comparison", "cumulative"):
            return self._translate_time_metric_scalar(metric)

        filters = list(metric.filters or [])
        if metric.agg:
            sql = self._render_aggregate_sql(metric.agg, metric.sql, filters)
        else:
            if not metric.sql:
                raise DaxTranslationError("CALCULATE requires a scalar or aggregate expression")
            if filters:
                predicate = " AND ".join(filters)
                sql = f"CASE WHEN {predicate} THEN {metric.sql} ELSE NULL END"
            else:
                sql = metric.sql

        columns = set()
        if metric.sql:
            columns.update(self._columns_from_sql(metric.sql))
        if metric.source_table and not any(
            col.table and col.table.lower() == metric.source_table.lower() for col in columns
        ):
            representative = self._representative_table_column(metric.source_table)
            columns.add(ColumnRef(metric.source_table, representative or ""))
        for clause in filters:
            columns.update(self._columns_from_sql(clause))
        return _SqlFragment(sql, frozenset(columns))

    def _translate_time_metric_scalar(self, metric: MetricTranslation) -> _SqlFragment:
        base_agg, base_sql, base_filters = self._time_metric_base(metric)
        base_expr = self._render_aggregate_sql(base_agg, base_sql, base_filters)

        order_col, partition_cols = self._time_window_context(metric)
        if order_col is None:
            return _SqlFragment(base_expr, frozenset(self._columns_from_sql(base_expr)))

        partition_sql = f"PARTITION BY {', '.join(partition_cols)} " if partition_cols else ""
        if metric.type == "cumulative":
            if metric.window:
                parts = metric.window.split()
                if len(parts) == 2:
                    num, unit = parts
                    sql = (
                        f"{base_expr} OVER ({partition_sql}ORDER BY {order_col} "
                        f"RANGE BETWEEN INTERVAL '{num} {unit}' PRECEDING AND CURRENT ROW)"
                    )
                elif len(parts) == 3 and parts[2].lower() == "following":
                    num, unit = parts[0], parts[1]
                    sql = (
                        f"{base_expr} OVER ({partition_sql}ORDER BY {order_col} "
                        f"RANGE BETWEEN CURRENT ROW AND INTERVAL '{num} {unit}' FOLLOWING)"
                    )
                else:
                    sql = f"{base_expr} OVER ({partition_sql}ORDER BY {order_col} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
            elif metric.grain_to_date:
                grain_partition = f"DATE_TRUNC('{metric.grain_to_date}', {order_col})"
                all_parts = [grain_partition, *partition_cols]
                sql = (
                    f"{base_expr} OVER (PARTITION BY {', '.join(all_parts)} "
                    f"ORDER BY {order_col} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
                )
            else:
                sql = f"{base_expr} OVER ({partition_sql}ORDER BY {order_col} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
            return _SqlFragment(sql, frozenset(self._columns_from_sql(sql)))

        lag_offset = self._time_comparison_lag_offset(metric)
        if metric.calculation == "previous_value":
            sql = f"LAG({base_expr}, {lag_offset}) OVER ({partition_sql}ORDER BY {order_col})"
            return _SqlFragment(sql, frozenset(self._columns_from_sql(sql)))
        raise DaxTranslationError(f"Unsupported time comparison calculation '{metric.calculation}'")

    def _time_metric_base(self, metric: MetricTranslation) -> tuple[str, str | None, list[str]]:
        if metric.type == "time_comparison":
            agg = metric.inline_base_agg or self._lookup_measure_agg(metric.base_metric or "")
            sql = metric.inline_base_sql
            if sql is None and metric.base_metric:
                sql = self._lookup_measure_sql(metric.base_metric)
            if sql is None and metric.base_metric:
                sql = metric.base_metric
            filters = list(metric.inline_base_filters or [])
        else:
            agg = metric.agg
            sql = metric.sql
            filters = list(metric.filters or [])

        if agg is None:
            agg = "sum"
        return agg, sql, filters

    def _time_window_context(self, metric: MetricTranslation) -> tuple[str | None, list[str]]:
        group_cols = list(self._current_group_by_columns)
        if not group_cols:
            return None, []

        grouped_sql: list[str] = []
        time_candidates: list[str] = []
        for col in group_cols:
            if not col.table:
                continue
            col_sql = self._column_sql(col.table, col.column)
            grouped_sql.append(col_sql)
            known_time = col.column in self.time_dimensions_by_table.get(col.table, set())
            if known_time:
                time_candidates.append(col_sql)

        if metric.window_order:
            order_sql = metric.window_order
        elif time_candidates:
            order_sql = time_candidates[0]
        elif grouped_sql:
            order_sql = grouped_sql[0]
        else:
            return None, []

        partition_cols = [sql for sql in grouped_sql if sql != order_sql]
        return order_sql, partition_cols

    def _time_comparison_lag_offset(self, metric: MetricTranslation) -> int:
        if metric.time_offset:
            parts = metric.time_offset.split()
            if parts:
                try:
                    return abs(int(parts[0]))
                except ValueError:
                    pass
        by_comp = {
            "dod": 1,
            "wow": 1,
            "mom": 1,
            "qoq": 1,
            "yoy": 1,
            "prior_period": 1,
        }
        return by_comp.get(metric.comparison_type or "", 1)

    def _render_aggregate_sql(self, agg: str, sql: str | None, filters: list[str]) -> str:
        predicate = " AND ".join(filters) if filters else None
        if agg == "count":
            if sql is None:
                if predicate:
                    return f"COUNT(CASE WHEN {predicate} THEN 1 END)"
                return "COUNT(*)"
            if predicate:
                return f"COUNT(CASE WHEN {predicate} THEN {sql} END)"
            return f"COUNT({sql})"

        if sql is None:
            raise DaxTranslationError(f"{agg} aggregation requires a SQL expression")

        if agg == "count_distinct":
            if predicate:
                return f"COUNT(DISTINCT CASE WHEN {predicate} THEN {sql} END)"
            return f"COUNT(DISTINCT {sql})"

        func_map = {"sum": "SUM", "avg": "AVG", "min": "MIN", "max": "MAX", "median": "MEDIAN"}
        func = func_map.get(agg)
        if func is None:
            raise DaxTranslationError(f"Unsupported aggregate '{agg}' in scalar context")
        if predicate:
            return f"{func}(CASE WHEN {predicate} THEN {sql} ELSE NULL END)"
        return f"{func}({sql})"

    def _translate_nameof(self, args: list[Any]) -> _SqlFragment:
        if not args:
            raise DaxTranslationError("NAMEOF requires an argument")
        if len(args) > 1:
            raise DaxTranslationError("NAMEOF supports exactly one argument")
        target = self._unwrap(args[0])
        if isinstance(target, self.dax.TableColumnRef):
            return _SqlFragment(self._quote_string(f"{target.table.name}[{target.column}]"), frozenset())
        if isinstance(target, self.dax.HierarchyRef):
            col = target.levels[-1] if target.levels else target.column
            return _SqlFragment(self._quote_string(f"{target.table.name}[{col}]"), frozenset())
        if isinstance(target, self.dax.BracketRef):
            return _SqlFragment(self._quote_string(target.name), frozenset())
        if isinstance(target, self.dax.Identifier):
            return _SqlFragment(self._quote_string(target.name), frozenset())
        resolved = self._translate_scalar(args[0])
        return _SqlFragment(self._quote_string(resolved.sql), resolved.columns)

    def _translate_convert(self, args: list[Any]) -> _SqlFragment:
        if len(args) < 2:
            raise DaxTranslationError("CONVERT requires value and datatype arguments")
        if len(args) > 2:
            raise DaxTranslationError("CONVERT supports exactly value and datatype arguments")
        value = self._translate_scalar(args[0])
        dtype = self._identifier_literal_value(args[1])
        if dtype is None:
            return value
        normalized = dtype.strip().lower()
        cast_type = {
            "integer": "BIGINT",
            "int64": "BIGINT",
            "double": "DOUBLE",
            "decimal": "DECIMAL",
            "currency": "DECIMAL",
            "string": "VARCHAR",
            "boolean": "BOOLEAN",
            "datetime": "TIMESTAMP",
            "date": "DATE",
            "time": "TIME",
        }.get(normalized)
        if cast_type is None:
            return value
        return _SqlFragment(f"CAST({value.sql} AS {cast_type})", value.columns)

    def _translate_lookupvalue(self, args: list[Any]) -> _SqlFragment:
        if len(args) < 3:
            raise DaxTranslationError("LOOKUPVALUE requires result column and at least one search pair")

        result_expr = self._unwrap(args[0])
        if isinstance(result_expr, self.dax.TableColumnRef):
            result_table = result_expr.table.name
        elif isinstance(result_expr, self.dax.HierarchyRef):
            result_table = result_expr.table.name
        else:
            raise DaxTranslationError("LOOKUPVALUE result argument must be a table column reference")

        with self._allow_cross_table_context():
            result_col = self._translate_scalar(args[0])

            search_args = list(args[1:])
            alternate: _SqlFragment | None = None
            if len(search_args) % 2 == 1:
                alternate = self._translate_scalar(search_args[-1])
                search_args = search_args[:-1]

            if len(search_args) < 2 or len(search_args) % 2 != 0:
                raise DaxTranslationError("LOOKUPVALUE requires search column/value pairs")

            predicates: list[str] = []
            outer_columns: set[ColumnRef] = set()
            for idx in range(0, len(search_args), 2):
                search_col = self._translate_scalar(search_args[idx])
                search_val = self._translate_scalar(search_args[idx + 1])
                predicates.append(f"{search_col.sql} = {search_val.sql}")
                outer_columns.update(search_val.columns)

        table_sql = self._table_sql(result_table)
        where_sql = f" WHERE {' AND '.join(predicates)}" if predicates else ""
        subquery = f"(SELECT {result_col.sql} FROM {table_sql}{where_sql} LIMIT 1)"
        if alternate is None:
            return _SqlFragment(subquery, frozenset(outer_columns))
        outer_columns.update(alternate.columns)
        return _SqlFragment(f"COALESCE({subquery}, {alternate.sql})", frozenset(outer_columns))

    def _translate_related(self, args: list[Any]) -> _SqlFragment:
        if not args:
            raise DaxTranslationError("RELATED requires an argument")
        if len(args) > 1:
            raise DaxTranslationError("RELATED supports exactly one argument")
        with self._allow_cross_table_context():
            return self._translate_scalar(args[0])

    def _translate_value(self, args: list[Any]) -> _SqlFragment:
        if not args:
            raise DaxTranslationError("VALUE requires an argument")
        if len(args) > 1:
            raise DaxTranslationError("VALUE supports exactly one argument")
        target = self._translate_scalar(args[0])
        return _SqlFragment(f"CAST({target.sql} AS DOUBLE)", target.columns)

    def _translate_concatenate(self, args: list[Any]) -> _SqlFragment:
        if len(args) < 2:
            raise DaxTranslationError("CONCATENATE requires two arguments")
        if len(args) > 2:
            raise DaxTranslationError("CONCATENATE supports exactly two arguments")
        left = self._translate_scalar(args[0])
        right = self._translate_scalar(args[1])
        return _SqlFragment(f"({left.sql} || {right.sql})", left.columns | right.columns)

    def _translate_concatenatex(self, args: list[Any]) -> _SqlFragment:
        if len(args) < 2:
            raise DaxTranslationError("CONCATENATEX requires table and expression arguments")

        table_expr = args[0]
        value_expr = args[1]
        delimiter_expr = args[2] if len(args) > 2 else None
        order_args = args[3:] if len(args) > 3 else []

        table_target = self._unwrap(table_expr)
        base_table_name = self._table_name_from_expr(table_target)
        wrapped = base_table_name is None
        from_sql, _ = self._table_source_from_expr(table_expr)

        value_fragment: _SqlFragment
        delimiter_fragment: _SqlFragment
        order_by_parts: list[str]
        order_columns: set[ColumnRef]
        qualifier_ctx = self._prefer_unqualified_base_table_context() if wrapped else nullcontext()
        with qualifier_ctx:
            value_fragment = (
                self._translate_projection_scalar(value_expr) if wrapped else self._translate_scalar(value_expr)
            )
            if delimiter_expr is None:
                delimiter_fragment = _SqlFragment("''", frozenset())
            else:
                delimiter_fragment = (
                    self._translate_projection_scalar(delimiter_expr)
                    if wrapped
                    else self._translate_scalar(delimiter_expr)
                )
            order_by_parts, order_columns = self._parse_order_by_parts_with_columns(order_args, projection_safe=wrapped)

        if base_table_name is not None:
            tables_in_order = [base_table_name]
            seen_tables = {base_table_name.lower()}
            self._append_tables(tables_in_order, seen_tables, value_fragment.columns)
            self._append_tables(tables_in_order, seen_tables, delimiter_fragment.columns)
            self._append_tables(tables_in_order, seen_tables, order_columns)
            from_sql = self._build_from_clause_for_tables(tables_in_order)
        else:
            value_fragment = value_fragment.wrap(_rewrite_expr_for_alias(value_fragment.sql, "t"))
            delimiter_fragment = delimiter_fragment.wrap(_rewrite_expr_for_alias(delimiter_fragment.sql, "t"))
            order_by_parts = [_rewrite_expr_for_alias(order_sql, "t") for order_sql in order_by_parts]

        order_sql = f" ORDER BY {', '.join(order_by_parts)}" if order_by_parts else ""
        aggregate_sql = (
            f"STRING_AGG(CAST({value_fragment.sql} AS VARCHAR), CAST({delimiter_fragment.sql} AS VARCHAR){order_sql})"
        )
        columns = value_fragment.columns | delimiter_fragment.columns | frozenset(order_columns)
        return _SqlFragment(f"(SELECT {aggregate_sql} FROM {from_sql})", columns)

    def _translate_roundup(self, args: list[Any]) -> _SqlFragment:
        if len(args) != 2:
            raise DaxTranslationError("ROUNDUP requires number and num_digits arguments")
        value = self._translate_scalar(args[0])
        digits = self._translate_scalar(args[1])
        sql = (
            f"CASE WHEN {digits.sql} >= 0 "
            f"THEN SIGN({value.sql}) * CEIL(ABS({value.sql}) * POWER(10, {digits.sql})) / POWER(10, {digits.sql}) "
            f"ELSE SIGN({value.sql}) * CEIL(ABS({value.sql}) / POWER(10, -({digits.sql}))) * POWER(10, -({digits.sql})) END"
        )
        return _SqlFragment(sql, value.columns | digits.columns)

    def _translate_round(self, args: list[Any]) -> _SqlFragment:
        if len(args) != 2:
            raise DaxTranslationError("ROUND requires number and num_digits arguments")
        value = self._translate_scalar(args[0])
        digits = self._translate_scalar(args[1])
        sql = (
            f"CASE WHEN {digits.sql} >= 0 "
            f"THEN SIGN({value.sql}) * FLOOR(ABS({value.sql}) * POWER(10, {digits.sql}) + 0.5) / POWER(10, {digits.sql}) "
            f"ELSE SIGN({value.sql}) * FLOOR(ABS({value.sql}) / POWER(10, -({digits.sql})) + 0.5) * POWER(10, -({digits.sql})) END"
        )
        return _SqlFragment(sql, value.columns | digits.columns)

    def _translate_rounddown(self, args: list[Any]) -> _SqlFragment:
        if len(args) != 2:
            raise DaxTranslationError("ROUNDDOWN requires number and num_digits arguments")
        value = self._translate_scalar(args[0])
        digits = self._translate_scalar(args[1])
        sql = (
            f"CASE WHEN {digits.sql} >= 0 "
            f"THEN SIGN({value.sql}) * FLOOR(ABS({value.sql}) * POWER(10, {digits.sql})) / POWER(10, {digits.sql}) "
            f"ELSE SIGN({value.sql}) * FLOOR(ABS({value.sql}) / POWER(10, -({digits.sql}))) * POWER(10, -({digits.sql})) END"
        )
        return _SqlFragment(sql, value.columns | digits.columns)

    def _translate_int(self, args: list[Any]) -> _SqlFragment:
        if not args:
            raise DaxTranslationError("INT requires an argument")
        if len(args) > 1:
            raise DaxTranslationError("INT supports exactly one argument")
        value = self._translate_scalar(args[0])
        return _SqlFragment(f"FLOOR({value.sql})", value.columns)

    def _translate_trunc(self, args: list[Any]) -> _SqlFragment:
        if not args:
            raise DaxTranslationError("TRUNC requires a number argument")
        if len(args) > 2:
            raise DaxTranslationError("TRUNC supports at most number and num_digits arguments")
        value = self._translate_scalar(args[0])
        digits = self._translate_scalar(args[1]) if len(args) > 1 else _SqlFragment("0", frozenset())
        sql = (
            f"CASE WHEN {digits.sql} >= 0 "
            f"THEN SIGN({value.sql}) * FLOOR(ABS({value.sql}) * POWER(10, {digits.sql})) / POWER(10, {digits.sql}) "
            f"ELSE SIGN({value.sql}) * FLOOR(ABS({value.sql}) / POWER(10, -({digits.sql}))) * POWER(10, -({digits.sql})) END"
        )
        return _SqlFragment(sql, value.columns | digits.columns)

    def _translate_mround(self, args: list[Any]) -> _SqlFragment:
        if len(args) != 2:
            raise DaxTranslationError("MROUND requires number and multiple arguments")
        value = self._translate_scalar(args[0])
        multiple = self._translate_scalar(args[1])
        sql = (
            f"CASE WHEN {multiple.sql} = 0 THEN 0 "
            f"ELSE SIGN({value.sql}) * FLOOR((ABS({value.sql}) / ABS({multiple.sql})) + 0.5) * ABS({multiple.sql}) END"
        )
        return _SqlFragment(sql, value.columns | multiple.columns)

    def _translate_ceiling(self, args: list[Any]) -> _SqlFragment:
        if not args:
            raise DaxTranslationError("CEILING requires at least one argument")
        if len(args) > 2:
            raise DaxTranslationError("CEILING supports at most number and significance arguments")
        value = self._translate_scalar(args[0])
        if len(args) == 1:
            return _SqlFragment(f"CEIL({value.sql})", value.columns)
        significance = self._translate_scalar(args[1])
        sql = f"(CEIL({value.sql} / {significance.sql}) * {significance.sql})"
        return _SqlFragment(sql, value.columns | significance.columns)

    def _translate_floor(self, args: list[Any]) -> _SqlFragment:
        if not args:
            raise DaxTranslationError("FLOOR requires at least one argument")
        if len(args) > 2:
            raise DaxTranslationError("FLOOR supports at most number and significance arguments")
        value = self._translate_scalar(args[0])
        if len(args) == 1:
            return _SqlFragment(f"FLOOR({value.sql})", value.columns)
        significance = self._translate_scalar(args[1])
        sql = f"(FLOOR({value.sql} / {significance.sql}) * {significance.sql})"
        return _SqlFragment(sql, value.columns | significance.columns)

    def _translate_abs(self, args: list[Any]) -> _SqlFragment:
        if len(args) != 1:
            raise DaxTranslationError("ABS requires exactly one argument")
        value = self._translate_scalar(args[0])
        return _SqlFragment(f"ABS({value.sql})", value.columns)

    def _translate_mod(self, args: list[Any]) -> _SqlFragment:
        if len(args) != 2:
            raise DaxTranslationError("MOD requires number and divisor arguments")
        number = self._translate_scalar(args[0])
        divisor = self._translate_scalar(args[1])
        return _SqlFragment(f"MOD({number.sql}, {divisor.sql})", number.columns | divisor.columns)

    def _translate_power(self, args: list[Any]) -> _SqlFragment:
        if len(args) != 2:
            raise DaxTranslationError("POWER requires number and exponent arguments")
        number = self._translate_scalar(args[0])
        exponent = self._translate_scalar(args[1])
        return _SqlFragment(f"POWER({number.sql}, {exponent.sql})", number.columns | exponent.columns)

    def _translate_sqrt(self, args: list[Any]) -> _SqlFragment:
        if len(args) != 1:
            raise DaxTranslationError("SQRT requires exactly one argument")
        value = self._translate_scalar(args[0])
        return _SqlFragment(f"SQRT({value.sql})", value.columns)

    def _translate_exp(self, args: list[Any]) -> _SqlFragment:
        if len(args) != 1:
            raise DaxTranslationError("EXP requires exactly one argument")
        value = self._translate_scalar(args[0])
        return _SqlFragment(f"EXP({value.sql})", value.columns)

    def _translate_ln(self, args: list[Any]) -> _SqlFragment:
        if len(args) != 1:
            raise DaxTranslationError("LN requires exactly one argument")
        value = self._translate_scalar(args[0])
        return _SqlFragment(f"LN({value.sql})", value.columns)

    def _translate_log10(self, args: list[Any]) -> _SqlFragment:
        if len(args) != 1:
            raise DaxTranslationError("LOG10 requires exactly one argument")
        value = self._translate_scalar(args[0])
        return _SqlFragment(f"LOG10({value.sql})", value.columns)

    def _translate_log(self, args: list[Any]) -> _SqlFragment:
        if not args:
            raise DaxTranslationError("LOG requires at least one argument")
        if len(args) > 2:
            raise DaxTranslationError("LOG supports at most number and base arguments")
        value = self._translate_scalar(args[0])
        if len(args) == 1:
            return _SqlFragment(f"LOG10({value.sql})", value.columns)
        base = self._translate_scalar(args[1])
        return _SqlFragment(f"(LN({value.sql}) / LN({base.sql}))", value.columns | base.columns)

    def _translate_pi(self, args: list[Any]) -> _SqlFragment:
        if args:
            raise DaxTranslationError("PI does not take arguments")
        return _SqlFragment("PI()", frozenset())

    def _translate_if(self, args: list[Any]) -> _SqlFragment:
        if not args:
            raise DaxTranslationError("IF requires condition")
        if len(args) > 3:
            raise DaxTranslationError("IF supports at most condition, value_if_true, and value_if_false arguments")
        condition = self._translate_scalar(args[0])
        true_expr = self._translate_scalar(args[1]) if len(args) > 1 else _SqlFragment("NULL", frozenset())
        false_expr = self._translate_scalar(args[2]) if len(args) > 2 else _SqlFragment("NULL", frozenset())
        sql = f"CASE WHEN {condition.sql} THEN {true_expr.sql} ELSE {false_expr.sql} END"
        columns = condition.columns | true_expr.columns | false_expr.columns
        return _SqlFragment(sql, columns)

    def _translate_selectedvalue(self, args: list[Any]) -> _SqlFragment:
        if not args:
            raise DaxTranslationError("SELECTEDVALUE requires an argument")
        if len(args) > 2:
            raise DaxTranslationError("SELECTEDVALUE supports at most column and alternate_result arguments")
        target = self._translate_scalar(args[0])
        alternate = self._translate_scalar(args[1]) if len(args) > 1 else _SqlFragment("NULL", frozenset())
        sql = f"CASE WHEN COUNT(DISTINCT {target.sql}) = 1 THEN MIN({target.sql}) ELSE {alternate.sql} END"
        return _SqlFragment(sql, target.columns | alternate.columns)

    def _translate_hasone(self, args: list[Any]) -> _SqlFragment:
        if not args:
            raise DaxTranslationError("HASONEVALUE requires an argument")
        if len(args) > 1:
            raise DaxTranslationError("HASONEVALUE/HASONEFILTER supports exactly one argument")
        target = self._translate_scalar(args[0])
        return _SqlFragment(f"(COUNT(DISTINCT {target.sql}) = 1)", target.columns)

    def _translate_first_last_nonblank(self, args: list[Any], *, pick: str) -> _SqlFragment:
        if len(args) < 2:
            raise DaxTranslationError("FIRSTNONBLANK/LASTNONBLANK requires a column and expression")
        if len(args) > 2:
            raise DaxTranslationError("FIRSTNONBLANK/LASTNONBLANK supports exactly column and expression arguments")
        target = self._translate_scalar(args[0])
        predicate = self._translate_scalar(args[1])
        agg = "MIN" if pick == "first" else "MAX"
        sql = f"{agg}(CASE WHEN {predicate.sql} IS NOT NULL THEN {target.sql} ELSE NULL END)"
        return _SqlFragment(sql, target.columns | predicate.columns)

    def _translate_first_last_date(self, args: list[Any], *, pick: str) -> _SqlFragment:
        if not args:
            raise DaxTranslationError("FIRSTDATE/LASTDATE requires an argument")
        if len(args) > 1:
            raise DaxTranslationError("FIRSTDATE/LASTDATE supports exactly one argument")
        target = self._translate_scalar(args[0])
        agg = "MIN" if pick == "first" else "MAX"
        return _SqlFragment(f"{agg}({target.sql})", target.columns)

    def _translate_period_boundary_date(self, args: list[Any], *, grain: str, end: bool) -> _SqlFragment:
        if not args:
            raise DaxTranslationError("STARTOF*/ENDOF* requires an argument")
        if len(args) > 1:
            raise DaxTranslationError("STARTOF*/ENDOF* supports exactly one argument")
        target = self._translate_scalar(args[0])
        base = f"DATE_TRUNC('{grain}', {target.sql})"
        if not end:
            return _SqlFragment(f"MIN({base})", target.columns)
        interval = {"month": "1 month", "quarter": "3 month", "year": "1 year"}[grain]
        sql = f"MIN({base} + INTERVAL '{interval}' - INTERVAL '1 day')"
        return _SqlFragment(sql, target.columns)

    def _translate_date_ctor(self, args: list[Any]) -> _SqlFragment:
        if len(args) != 3:
            raise DaxTranslationError("DATE requires year, month, and day arguments")
        year = self._translate_scalar(args[0])
        month = self._translate_scalar(args[1])
        day = self._translate_scalar(args[2])
        sql = f"MAKE_DATE({year.sql}, {month.sql}, {day.sql})"
        return _SqlFragment(sql, year.columns | month.columns | day.columns)

    def _translate_time_ctor(self, args: list[Any]) -> _SqlFragment:
        if len(args) != 3:
            raise DaxTranslationError("TIME requires hour, minute, and second arguments")
        hour = self._translate_scalar(args[0])
        minute = self._translate_scalar(args[1])
        second = self._translate_scalar(args[2])
        sql = f"MAKE_TIME({hour.sql}, {minute.sql}, {second.sql})"
        return _SqlFragment(sql, hour.columns | minute.columns | second.columns)

    def _translate_datevalue(self, args: list[Any]) -> _SqlFragment:
        if not args:
            raise DaxTranslationError("DATEVALUE requires an argument")
        if len(args) > 1:
            raise DaxTranslationError("DATEVALUE supports exactly one argument")
        target = self._translate_scalar(args[0])
        return _SqlFragment(f"CAST({target.sql} AS DATE)", target.columns)

    def _translate_timevalue(self, args: list[Any]) -> _SqlFragment:
        if not args:
            raise DaxTranslationError("TIMEVALUE requires an argument")
        if len(args) > 1:
            raise DaxTranslationError("TIMEVALUE supports exactly one argument")
        target = self._translate_scalar(args[0])
        return _SqlFragment(f"CAST({target.sql} AS TIME)", target.columns)

    def _translate_edate(self, args: list[Any]) -> _SqlFragment:
        if len(args) != 2:
            raise DaxTranslationError("EDATE requires start date and month offset arguments")
        start = self._translate_scalar(args[0])
        months = self._translate_scalar(args[1])
        sql = f"(CAST({start.sql} AS DATE) + ({months.sql}) * INTERVAL '1 month')"
        return _SqlFragment(sql, start.columns | months.columns)

    def _translate_eomonth(self, args: list[Any]) -> _SqlFragment:
        if len(args) != 2:
            raise DaxTranslationError("EOMONTH requires start date and month offset arguments")
        start = self._translate_scalar(args[0])
        months = self._translate_scalar(args[1])
        shifted = f"(CAST({start.sql} AS DATE) + ({months.sql}) * INTERVAL '1 month')"
        sql = f"(DATE_TRUNC('month', {shifted}) + INTERVAL '1 month' - INTERVAL '1 day')"
        return _SqlFragment(sql, start.columns | months.columns)

    def _translate_datediff(self, args: list[Any]) -> _SqlFragment:
        if len(args) != 3:
            raise DaxTranslationError("DATEDIFF requires start date, end date, and interval arguments")
        start = self._translate_scalar(args[0])
        end = self._translate_scalar(args[1])
        unit = self._identifier_literal_value(args[2])
        if unit is None:
            raise DaxTranslationError("DATEDIFF interval must be an identifier or string")
        normalized = unit.lower()
        if normalized.endswith("s"):
            normalized = normalized[:-1]
        sql = f"DATE_DIFF('{normalized}', CAST({start.sql} AS DATE), CAST({end.sql} AS DATE))"
        return _SqlFragment(sql, start.columns | end.columns)

    def _translate_weekday(self, args: list[Any]) -> _SqlFragment:
        if not args:
            raise DaxTranslationError("WEEKDAY requires a date argument")
        if len(args) > 2:
            raise DaxTranslationError("WEEKDAY supports at most date and return_type arguments")
        date_value = self._translate_scalar(args[0])
        return_type = self._number_literal_value(args[1]) if len(args) > 1 else 1
        dow = f"EXTRACT(DOW FROM CAST({date_value.sql} AS DATE))"
        if return_type == 1:
            sql = f"({dow} + 1)"
        elif return_type == 2:
            sql = f"((({dow} + 6) % 7) + 1)"
        elif return_type == 3:
            sql = f"(({dow} + 6) % 7)"
        elif return_type in (11, 12, 13, 14, 15, 16, 17):
            start_dow = return_type - 10
            if start_dow == 7:
                start_dow = 0
            sql = f"((({dow} - {start_dow} + 7) % 7) + 1)"
        else:
            raise DaxTranslationError("WEEKDAY return_type currently supports 1, 2, 3, or 11-17")
        return _SqlFragment(sql, date_value.columns)

    def _translate_weeknum(self, args: list[Any]) -> _SqlFragment:
        if not args:
            raise DaxTranslationError("WEEKNUM requires a date argument")
        if len(args) > 2:
            raise DaxTranslationError("WEEKNUM supports at most date and return_type arguments")
        date_value = self._translate_scalar(args[0])
        return_type = self._number_literal_value(args[1]) if len(args) > 1 else 1
        if return_type == 1:
            sql = f"(CAST(STRFTIME(CAST({date_value.sql} AS DATE), '%U') AS INTEGER) + 1)"
        elif return_type == 2:
            sql = f"(CAST(STRFTIME(CAST({date_value.sql} AS DATE), '%W') AS INTEGER) + 1)"
        elif return_type == 21:
            sql = f"CAST(STRFTIME(CAST({date_value.sql} AS DATE), '%V') AS INTEGER)"
        else:
            raise DaxTranslationError("WEEKNUM return_type currently supports 1, 2, or 21")
        return _SqlFragment(sql, date_value.columns)

    def _translate_containsstring(self, args: list[Any], *, exact: bool) -> _SqlFragment:
        if len(args) < 2:
            func = "CONTAINSSTRINGEXACT" if exact else "CONTAINSSTRING"
            raise DaxTranslationError(f"{func} requires text and search arguments")
        if len(args) > 2:
            func = "CONTAINSSTRINGEXACT" if exact else "CONTAINSSTRING"
            raise DaxTranslationError(f"{func} supports exactly two arguments")
        text = self._translate_scalar(args[0])
        search = self._translate_scalar(args[1])
        if exact:
            sql = f"(POSITION({search.sql} IN {text.sql}) > 0)"
        else:
            sql = f"(POSITION(LOWER({search.sql}) IN LOWER({text.sql})) > 0)"
        return _SqlFragment(sql, text.columns | search.columns)

    def _translate_containsrow(self, args: list[Any]) -> _SqlFragment:
        if len(args) < 2:
            raise DaxTranslationError("CONTAINSROW requires a table expression and at least one value argument")

        table_expr = self._unwrap(args[0])
        if self._table_name_from_expr(table_expr) is None and not isinstance(
            table_expr, (self.dax.FunctionCall, self.dax.TableConstructor)
        ):
            raise DaxTranslationError("CONTAINSROW requires a table expression as first argument")

        table_sql = self._translate_table(table_expr)
        table_width = self._treatas_source_width(table_expr, table_sql)
        if table_width is None:
            raise DaxTranslationError("CONTAINSROW requires an inferable table column count")

        value_exprs = args[1:]
        if len(value_exprs) != table_width:
            raise DaxTranslationError("CONTAINSROW value argument count must match table column count")

        value_fragments = [self._translate_scalar(expr) for expr in value_exprs]
        alias_names = [f"c{idx + 1}" for idx in range(table_width)]
        alias_list_sql = ", ".join(self._quote_identifier(alias) for alias in alias_names)
        predicates = [
            f"t.{self._quote_identifier(alias_name)} IS NOT DISTINCT FROM {value_fragment.sql}"
            for alias_name, value_fragment in zip(alias_names, value_fragments, strict=False)
        ]
        predicate_sql = " AND ".join(predicates) if predicates else "TRUE"
        sql = f"EXISTS (SELECT 1 FROM ({table_sql}) AS t({alias_list_sql}) WHERE {predicate_sql})"

        columns = set(self._columns_from_sql(table_sql))
        for value_fragment in value_fragments:
            columns.update(value_fragment.columns)
        return _SqlFragment(sql, frozenset(columns))

    def _translate_len(self, args: list[Any]) -> _SqlFragment:
        if not args:
            raise DaxTranslationError("LEN requires an argument")
        if len(args) > 1:
            raise DaxTranslationError("LEN supports exactly one argument")
        value = self._translate_scalar(args[0])
        return _SqlFragment(f"LENGTH({value.sql})", value.columns)

    def _translate_replace(self, args: list[Any]) -> _SqlFragment:
        if len(args) < 4:
            raise DaxTranslationError("REPLACE requires old_text, start_num, num_chars, and new_text arguments")
        text = self._translate_scalar(args[0])
        start_num = self._translate_scalar(args[1])
        num_chars = self._translate_scalar(args[2])
        new_text = self._translate_scalar(args[3])
        safe_start = f"GREATEST({start_num.sql}, 1)"
        safe_chars = f"GREATEST({num_chars.sql}, 0)"
        sql = (
            f"(CASE WHEN {safe_start} <= 1 THEN '' ELSE SUBSTRING({text.sql}, 1, {safe_start} - 1) END "
            f"|| {new_text.sql} || SUBSTRING({text.sql}, {safe_start} + {safe_chars}))"
        )
        return _SqlFragment(sql, text.columns | start_num.columns | num_chars.columns | new_text.columns)

    def _translate_substitute(self, args: list[Any]) -> _SqlFragment:
        if len(args) < 3:
            raise DaxTranslationError("SUBSTITUTE requires text, old_text, and new_text arguments")
        if len(args) > 4:
            raise DaxTranslationError(
                "SUBSTITUTE supports at most text, old_text, new_text, and instance_num arguments"
            )
        text = self._translate_scalar(args[0])
        old_text = self._translate_scalar(args[1])
        new_text = self._translate_scalar(args[2])
        if len(args) == 3:
            sql = f"REPLACE({text.sql}, {old_text.sql}, {new_text.sql})"
            return _SqlFragment(sql, text.columns | old_text.columns | new_text.columns)

        instance_num = self._number_literal_value(args[3])
        if instance_num is not None:
            if instance_num < 1:
                raise DaxTranslationError("SUBSTITUTE instance_num must be >= 1")

            pos_sql = self._nth_occurrence_position_sql(text.sql, old_text.sql, instance_num)
            sql = (
                f"CASE WHEN {old_text.sql} = '' THEN {text.sql} "
                f"WHEN ({pos_sql}) = 0 THEN {text.sql} "
                f"ELSE SUBSTR({text.sql}, 1, ({pos_sql}) - 1) || {new_text.sql} "
                f"|| SUBSTR({text.sql}, ({pos_sql}) + LENGTH({old_text.sql})) END"
            )
            return _SqlFragment(sql, text.columns | old_text.columns | new_text.columns)

        instance_fragment = self._translate_scalar(args[3])
        instance_sql = f"CAST(({instance_fragment.sql}) AS BIGINT)"
        split_sql = f"STRING_SPLIT({text.sql}, {old_text.sql})"
        occurrences_sql = f"(ARRAY_LENGTH({split_sql}) - 1)"
        prefix_sql = f"ARRAY_TO_STRING(LIST_SLICE({split_sql}, 1, {instance_sql}), {old_text.sql})"
        suffix_sql = (
            f"ARRAY_TO_STRING(LIST_SLICE({split_sql}, {instance_sql} + 1, ARRAY_LENGTH({split_sql})), {old_text.sql})"
        )
        sql = (
            f"CASE WHEN {old_text.sql} = '' THEN {text.sql} "
            f"WHEN {instance_sql} IS NULL OR {instance_sql} < 1 THEN {text.sql} "
            f"WHEN {instance_sql} > {occurrences_sql} THEN {text.sql} "
            f"ELSE {prefix_sql} || {new_text.sql} || {suffix_sql} END"
        )
        return _SqlFragment(sql, text.columns | old_text.columns | new_text.columns | instance_fragment.columns)

    def _nth_occurrence_position_sql(self, text_sql: str, needle_sql: str, occurrence: int) -> str:
        if occurrence < 1:
            raise DaxTranslationError("SUBSTITUTE instance_num must be >= 1")
        pos_sql = f"INSTR({text_sql}, {needle_sql})"
        for _ in range(2, occurrence + 1):
            next_sql = f"INSTR(SUBSTR({text_sql}, ({pos_sql}) + LENGTH({needle_sql})), {needle_sql})"
            pos_sql = (
                f"CASE WHEN ({pos_sql}) = 0 THEN 0 "
                f"WHEN ({next_sql}) = 0 THEN 0 "
                f"ELSE ({next_sql}) + ({pos_sql}) + LENGTH({needle_sql}) - 1 END"
            )
        return pos_sql

    def _translate_rept(self, args: list[Any]) -> _SqlFragment:
        if len(args) < 2:
            raise DaxTranslationError("REPT requires text and number_times arguments")
        if len(args) > 2:
            raise DaxTranslationError("REPT supports exactly two arguments")
        text = self._translate_scalar(args[0])
        number_times = self._translate_scalar(args[1])
        safe_count = f"GREATEST(CAST(FLOOR({number_times.sql}) AS BIGINT), 0)"
        return _SqlFragment(f"REPEAT({text.sql}, {safe_count})", text.columns | number_times.columns)

    def _translate_trim(self, args: list[Any]) -> _SqlFragment:
        if not args:
            raise DaxTranslationError("TRIM requires an argument")
        if len(args) > 1:
            raise DaxTranslationError("TRIM supports exactly one argument")
        value = self._translate_scalar(args[0])
        sql = f"TRIM(REGEXP_REPLACE(CAST({value.sql} AS VARCHAR), ' +', ' ', 'g'))"
        return _SqlFragment(sql, value.columns)

    def _translate_left(self, args: list[Any]) -> _SqlFragment:
        if not args:
            raise DaxTranslationError("LEFT requires a text argument")
        if len(args) > 2:
            raise DaxTranslationError("LEFT supports at most text and num_chars arguments")
        text = self._translate_scalar(args[0])
        num_chars = self._translate_scalar(args[1]) if len(args) > 1 else _SqlFragment("1", frozenset())
        sql = f"SUBSTRING({text.sql}, 1, GREATEST({num_chars.sql}, 0))"
        return _SqlFragment(sql, text.columns | num_chars.columns)

    def _translate_right(self, args: list[Any]) -> _SqlFragment:
        if not args:
            raise DaxTranslationError("RIGHT requires a text argument")
        if len(args) > 2:
            raise DaxTranslationError("RIGHT supports at most text and num_chars arguments")
        text = self._translate_scalar(args[0])
        num_chars = self._translate_scalar(args[1]) if len(args) > 1 else _SqlFragment("1", frozenset())
        sql = (
            f"CASE WHEN {num_chars.sql} <= 0 THEN '' "
            f"ELSE SUBSTRING({text.sql}, GREATEST(LENGTH({text.sql}) - {num_chars.sql} + 1, 1), {num_chars.sql}) END"
        )
        return _SqlFragment(sql, text.columns | num_chars.columns)

    def _translate_mid(self, args: list[Any]) -> _SqlFragment:
        if len(args) != 3:
            raise DaxTranslationError("MID requires text, start_num, and num_chars arguments")
        text = self._translate_scalar(args[0])
        start_num = self._translate_scalar(args[1])
        num_chars = self._translate_scalar(args[2])
        sql = (
            f"CASE WHEN {num_chars.sql} <= 0 THEN '' "
            f"ELSE SUBSTRING({text.sql}, GREATEST({start_num.sql}, 1), {num_chars.sql}) END"
        )
        return _SqlFragment(sql, text.columns | start_num.columns | num_chars.columns)

    def _translate_find_search(self, args: list[Any], *, case_sensitive: bool, func: str) -> _SqlFragment:
        if len(args) < 2:
            raise DaxTranslationError(f"{func} requires find_text and within_text arguments")
        if len(args) > 4:
            raise DaxTranslationError(f"{func} supports at most find_text, within_text, start_num, and not_found_value")
        needle = self._translate_scalar(args[0])
        haystack = self._translate_scalar(args[1])
        start_num = self._translate_scalar(args[2]) if len(args) > 2 else None
        not_found = self._translate_scalar(args[3]) if len(args) > 3 else None

        if case_sensitive:
            needle_sql = needle.sql
            haystack_sql = haystack.sql
        else:
            needle_sql = f"LOWER({needle.sql})"
            haystack_sql = f"LOWER({haystack.sql})"

        if start_num is None:
            base_pos = f"POSITION({needle_sql} IN {haystack_sql})"
            adjusted = base_pos
        else:
            segment = f"SUBSTRING({haystack_sql}, {start_num.sql})"
            base_pos = f"POSITION({needle_sql} IN {segment})"
            adjusted = f"CASE WHEN {base_pos} = 0 THEN 0 ELSE ({base_pos} + {start_num.sql} - 1) END"

        if not_found is not None:
            sql = f"CASE WHEN {adjusted} = 0 THEN {not_found.sql} ELSE {adjusted} END"
        else:
            sql = adjusted

        columns = set(needle.columns) | set(haystack.columns)
        if start_num is not None:
            columns.update(start_num.columns)
        if not_found is not None:
            columns.update(not_found.columns)
        return _SqlFragment(sql, frozenset(columns))

    def _translate_date_part(self, args: list[Any], *, part: str) -> _SqlFragment:
        if not args:
            raise DaxTranslationError(f"{part.upper()} requires an argument")
        if len(args) > 1:
            raise DaxTranslationError(f"{part.upper()} supports exactly one argument")
        value = self._translate_scalar(args[0])
        part_map = {
            "year": "YEAR",
            "month": "MONTH",
            "day": "DAY",
            "hour": "HOUR",
            "minute": "MINUTE",
            "second": "SECOND",
            "quarter": "QUARTER",
        }
        part_sql = part_map[part]
        cast_type = "TIMESTAMP" if part in ("hour", "minute", "second") else "DATE"
        sql = f"EXTRACT({part_sql} FROM CAST({value.sql} AS {cast_type}))"
        return _SqlFragment(sql, value.columns)

    def _translate_exact(self, args: list[Any]) -> _SqlFragment:
        if len(args) < 2:
            raise DaxTranslationError("EXACT requires two arguments")
        if len(args) > 2:
            raise DaxTranslationError("EXACT supports exactly two arguments")
        left = self._translate_scalar(args[0])
        right = self._translate_scalar(args[1])
        return _SqlFragment(f"({left.sql} = {right.sql})", left.columns | right.columns)

    def _translate_randbetween(self, args: list[Any]) -> _SqlFragment:
        if len(args) < 2:
            raise DaxTranslationError("RANDBETWEEN requires bottom and top arguments")
        if len(args) > 2:
            raise DaxTranslationError("RANDBETWEEN supports exactly two arguments")
        lower = self._translate_scalar(args[0])
        upper = self._translate_scalar(args[1])
        sql = f"CAST(FLOOR(RANDOM() * (({upper.sql}) - ({lower.sql}) + 1) + ({lower.sql})) AS BIGINT)"
        return _SqlFragment(sql, lower.columns | upper.columns)

    def _translate_format(self, args: list[Any]) -> _SqlFragment:
        if len(args) < 2:
            raise DaxTranslationError("FORMAT requires value and format_string arguments")
        if len(args) > 3:
            raise DaxTranslationError("FORMAT supports at most value, format_string, and locale arguments")
        value = self._translate_scalar(args[0])
        # Current lowering preserves value-to-text behavior and ignores format mask semantics.
        return _SqlFragment(f"CAST({value.sql} AS VARCHAR)", value.columns)

    def _translate_iferror(self, args: list[Any]) -> _SqlFragment:
        if len(args) < 2:
            raise DaxTranslationError("IFERROR requires value and value_if_error arguments")
        if len(args) > 2:
            raise DaxTranslationError("IFERROR supports exactly two arguments")
        value = self._translate_scalar(args[0])
        fallback = self._translate_scalar(args[1])
        sql = f"CASE WHEN {value.sql} IS NULL THEN {fallback.sql} ELSE {value.sql} END"
        return _SqlFragment(sql, value.columns | fallback.columns)

    def _translate_isinscope(self, args: list[Any]) -> _SqlFragment:
        if not args:
            raise DaxTranslationError("ISINSCOPE requires an argument")
        if len(args) > 1:
            raise DaxTranslationError("ISINSCOPE supports exactly one argument")
        target = self._translate_scalar(args[0])
        in_scope = self._any_column_in_context(target.columns, self._current_group_by_columns)
        return _SqlFragment("TRUE" if in_scope else "FALSE", target.columns)

    def _translate_isfiltered(self, args: list[Any]) -> _SqlFragment:
        if not args:
            raise DaxTranslationError("ISFILTERED requires an argument")
        if len(args) > 1:
            raise DaxTranslationError("ISFILTERED/ISCROSSFILTERED supports exactly one argument")
        target = self._translate_scalar(args[0])
        is_filtered = self._any_column_in_context(target.columns, self._current_filter_columns)
        return _SqlFragment("TRUE" if is_filtered else "FALSE", target.columns)

    def _translate_isempty(self, args: list[Any]) -> _SqlFragment:
        if not args:
            raise DaxTranslationError("ISEMPTY requires a table argument")
        if len(args) > 1:
            raise DaxTranslationError("ISEMPTY supports exactly one argument")
        with self._allow_cross_table_context():
            table_sql = self._translate_table(args[0])
        return _SqlFragment(
            f"NOT EXISTS (SELECT 1 FROM ({table_sql}) AS t)", frozenset(self._columns_from_sql(table_sql))
        )

    @staticmethod
    def _any_column_in_context(target_cols: frozenset[ColumnRef], context_cols: frozenset[ColumnRef]) -> bool:
        for target in target_cols:
            for context in context_cols:
                if _columns_match(target, context):
                    return True
        return False

    def _translate_switch(self, args: list[Any]) -> _SqlFragment:
        if len(args) < 3:
            raise DaxTranslationError("SWITCH requires expression and at least one value/result pair")

        first = self._translate_scalar(args[0])
        pairs = args[1:]

        is_boolean_switch = self._is_true_literal(args[0])
        when_clauses = []
        columns = set(first.columns)

        idx = 0
        while idx + 1 < len(pairs):
            cond_expr = pairs[idx]
            result_expr = pairs[idx + 1]
            cond = self._translate_scalar(cond_expr)
            result = self._translate_scalar(result_expr)
            if is_boolean_switch:
                when_sql = cond.sql
            else:
                when_sql = f"{first.sql} = {cond.sql}"
            when_clauses.append(f"WHEN {when_sql} THEN {result.sql}")
            columns.update(cond.columns)
            columns.update(result.columns)
            idx += 2

        else_expr = None
        if idx < len(pairs):
            else_expr = self._translate_scalar(pairs[idx])
            columns.update(else_expr.columns)

        else_sql = else_expr.sql if else_expr else "NULL"
        sql = f"CASE {' '.join(when_clauses)} ELSE {else_sql} END"
        return _SqlFragment(sql, frozenset(columns))

    def _translate_coalesce(self, args: list[Any]) -> _SqlFragment:
        if len(args) < 2:
            raise DaxTranslationError("COALESCE requires at least two arguments")
        fragments = [self._translate_scalar(arg) for arg in args]
        sql = f"COALESCE({', '.join(f.sql for f in fragments)})"
        columns = set()
        for frag in fragments:
            columns.update(frag.columns)
        return _SqlFragment(sql, frozenset(columns))

    def _translate_divide(self, args: list[Any]) -> _SqlFragment:
        if len(args) < 2:
            raise DaxTranslationError("DIVIDE requires numerator and denominator")
        if len(args) > 3:
            raise DaxTranslationError("DIVIDE supports at most numerator, denominator, and alternate result arguments")
        numerator = self._translate_scalar(args[0])
        denominator = self._translate_scalar(args[1])
        alternate = self._translate_scalar(args[2]) if len(args) > 2 else _SqlFragment("NULL", frozenset())
        sql = (
            f"CASE WHEN {denominator.sql} IS NULL OR {denominator.sql} = 0 "
            f"THEN {alternate.sql} ELSE {numerator.sql} / {denominator.sql} END"
        )
        columns = numerator.columns | denominator.columns | alternate.columns
        return _SqlFragment(sql, frozenset(columns))

    def _translate_and_or(self, args: list[Any], op: str) -> _SqlFragment:
        if len(args) < 2:
            raise DaxTranslationError(f"{op} requires two arguments")
        if len(args) > 2:
            raise DaxTranslationError(f"{op} supports exactly two arguments")
        left = self._translate_scalar(args[0])
        right = self._translate_scalar(args[1])
        sql = f"({left.sql} {op} {right.sql})"
        return _SqlFragment(sql, left.columns | right.columns)

    def _translate_projection_scalar(self, expr: Any) -> _SqlFragment:
        try:
            return self._translate_scalar(expr)
        except DaxTranslationError as exc:
            if str(exc) != "DAX table expressions must reference a single base table":
                raise

        with self._allow_cross_table_context():
            with self._prefer_unqualified_base_table_context():
                return self._translate_scalar(expr)

    def _translate_in_list(self, expr: Any) -> str:
        expr = self._unwrap(expr)
        if isinstance(expr, self.dax.TableConstructor):
            if not expr.rows:
                return "(NULL)"
            row_sqls = []
            for row in expr.rows:
                if len(row) == 1:
                    row_sqls.append(self._translate_scalar(row[0]).sql)
                else:
                    row_sqls.append("(" + ", ".join(self._translate_scalar(item).sql for item in row) + ")")
            return f"({', '.join(row_sqls)})"
        raise DaxTranslationError("IN requires a table constructor")

    def _translate_identifier(self, name: str) -> _SqlFragment:
        env_key = name.lower()
        if env_key in self._env:
            return self._env[env_key]

        if self.model_name is None:
            metric = self._translate_metric_reference_name(name)
            if metric is not None:
                return self._metric_to_scalar_fragment(metric)
        elif self._is_measure_name(name):
            return _SqlFragment(self._quote_identifier(name), frozenset())

        if self.model_name is None and self._base_table is None:
            raise DaxTranslationError(f"Ambiguous identifier '{name}' without a table context")

        table_name = self.model_name or self._base_table
        column_sql = self._column_sql(table_name, name)
        columns = frozenset({ColumnRef(table_name, name)})
        return _SqlFragment(column_sql, columns)

    def _translate_table_column(self, table: str, column: str) -> _SqlFragment:
        self._ensure_table_context(table)
        column_sql = self._column_sql(table, column)
        columns = frozenset({ColumnRef(table, column)})
        return _SqlFragment(column_sql, columns)

    def _column_sql(self, table: str | None, column: str) -> str:
        if table is not None and self.model_name is not None:
            if table.lower() != self.model_name.lower():
                if not self._allow_cross_table:
                    raise DaxTranslationError(
                        f"DAX translation only supports references to '{self.model_name}', found '{table}'"
                    )
                self._required_models.add(table)
        elif table is not None and self.model_name is None:
            self._required_models.add(table)

        table_key = table or self.model_name or ""
        column_map = self.column_sql_by_table.get(table_key, {})
        mapped = column_map.get(column)
        if mapped is None:
            mapped = self._quote_identifier(column)

        if table is None:
            return mapped
        if self.model_name and table.lower() == self.model_name.lower():
            return mapped
        # In table-query translation (no explicit model), same-table references are rendered
        # unqualified so wrapped table expressions remain valid (`FROM (<subquery>) AS t`).
        if (
            self.model_name is None
            and self._base_table is not None
            and table.lower() == self._base_table.lower()
            and (not self._allow_cross_table or self._prefer_unqualified_base_table)
        ):
            return mapped

        table_sql = self._quote_identifier(table)
        if _can_qualify_identifier(mapped):
            return f"{table_sql}.{mapped}"
        return mapped

    def _table_sql(self, table: str) -> str:
        return self._quote_identifier(table)

    def _default_table_sql(self) -> str:
        if self.model_name:
            return self._table_sql(self.model_name)
        if self._base_table:
            return self._table_sql(self._base_table)
        raise DaxTranslationError("No default table context for DAX table expression")

    def _is_measure_name(self, name: str) -> bool:
        return self._resolve_measure_reference(name) is not None

    def _resolve_measure_reference(self, measure: str) -> tuple[str, str] | None:
        if "." in measure:
            table, name = measure.split(".", 1)
            return table, name

        if self.model_name:
            return self.model_name, measure

        candidates: list[tuple[str, str]] = []
        measure_lower = measure.lower()
        for table, measure_names in self.measure_names_by_table.items():
            if measure in measure_names:
                candidates.append((table, measure))
                continue
            for known in measure_names:
                if known.lower() == measure_lower:
                    candidates.append((table, known))
                    break
        if len(candidates) == 1:
            return candidates[0]
        return None

    def _lookup_measure_agg(self, measure: str) -> str | None:
        resolved = self._resolve_measure_reference(measure)
        if resolved is None:
            return None
        table, name = resolved
        return self.measure_aggs_by_table.get(table, {}).get(name)

    def _lookup_measure_sql(self, measure: str) -> str | None:
        resolved = self._resolve_measure_reference(measure)
        if resolved is None:
            return None
        table, name = resolved
        return self.measure_sql_by_table.get(table, {}).get(name)

    def _translate_metric_reference(self, expr: Any) -> MetricTranslation | None:
        expr = self._unwrap(expr)
        if isinstance(expr, self.dax.TableColumnRef):
            return self._translate_metric_reference_name(expr.column, table=expr.table.name)
        if isinstance(expr, self.dax.BracketRef):
            return self._translate_metric_reference_name(expr.name)
        if isinstance(expr, self.dax.Identifier):
            return self._translate_metric_reference_name(expr.name)
        return None

    def _translate_metric_reference_name(self, name: str, table: str | None = None) -> MetricTranslation | None:
        if table is not None:
            resolved = self._resolve_measure_reference_for_table(table, name)
        else:
            resolved = self._resolve_measure_reference(name)
        if resolved is None:
            return None

        table, measure = resolved
        agg = self.measure_aggs_by_table.get(table, {}).get(measure)
        sql = self.measure_sql_by_table.get(table, {}).get(measure)
        filters = list(self.measure_filters_by_table.get(table, {}).get(measure, []))
        if agg is None and sql is None:
            return MetricTranslation(sql=self._quote_identifier(measure), type="derived")

        if sql:
            sql = self._measure_sql_for_context(table, sql)
        elif agg:
            sql = f"{table}.{measure}" if self.model_name is not None else None

        return MetricTranslation(
            sql=sql,
            agg=agg,
            type=None if agg else "derived",
            source_table=table,
            filters=filters,
        )

    def _resolve_measure_reference_for_table(self, table: str, measure: str) -> tuple[str, str] | None:
        table_lower = table.lower()
        measure_lower = measure.lower()
        for known_table, measure_names in self.measure_names_by_table.items():
            if known_table.lower() != table_lower:
                continue
            for known_measure in measure_names:
                if known_measure == measure or known_measure.lower() == measure_lower:
                    return known_table, known_measure
        return None

    def _measure_sql_for_context(self, table: str, sql: str) -> str:
        if self.model_name is not None:
            if table.lower() == self.model_name.lower():
                return sql
            self._required_models.add(table)
        return self._qualify_measure_sql(table, sql)

    def _qualify_measure_sql(self, table: str, sql: str | None) -> str | None:
        if not sql:
            return sql

        try:
            import sqlglot
            from sqlglot import exp

            parsed = sqlglot.parse_one(sql, dialect="duckdb")
            for column in parsed.find_all(exp.Column):
                if column.table:
                    continue
                column.set("table", exp.to_identifier(table))
            return parsed.sql(dialect="duckdb")
        except Exception:
            if _can_qualify_identifier(sql):
                return f"{self._table_sql(table)}.{sql}"
            return sql

    def _unwrap(self, expr: Any) -> Any:
        while isinstance(expr, self.dax.Paren):
            expr = expr.expr
        return expr

    def _table_name_from_expr(self, expr: Any) -> str | None:
        expr = self._unwrap(expr)
        if isinstance(expr, self.dax.TableRef):
            return expr.table.name
        if isinstance(expr, self.dax.Identifier):
            if self._is_known_measure_identifier(expr.name):
                return None
            if not self._table_exists(expr.name):
                return None
            return expr.name
        if isinstance(expr, self.dax.FunctionCall) and expr.name.lower() == "relatedtable" and expr.args:
            return self._table_name_from_expr(expr.args[0])
        return None

    def _with_vars(self, var_block: Any, func, body: Any):
        prior = dict(self._env)
        for decl in var_block.decls:
            value = self._translate_scalar(decl.expr)
            self._env[decl.name.lower()] = value
        result = func(body)
        self._env = prior
        return result

    def _extract_measure_reference(self, expr: Any) -> str | None:
        expr = self._unwrap(expr)
        if isinstance(expr, self.dax.BracketRef):
            return self._qualify_measure(expr.name)
        if isinstance(expr, self.dax.Identifier):
            return self._qualify_measure(expr.name)
        return None

    def _qualify_measure(self, name: str) -> str:
        resolved = self._resolve_measure_reference(name)
        if resolved is not None:
            table, measure = resolved
            return f"{table}.{measure}"
        return name

    def _ensure_table_context(self, table: str | None) -> None:
        if table is None:
            return
        if self.model_name:
            if table.lower() != self.model_name.lower():
                if self._allow_cross_table:
                    self._required_models.add(table)
                    return
                raise DaxTranslationError(
                    f"DAX translation only supports references to '{self.model_name}', found '{table}'"
                )
            return
        if self._base_table is None:
            self._base_table = table
            return
        if table.lower() != self._base_table.lower():
            if self._allow_cross_table:
                self._required_models.add(table)
                return
            raise DaxTranslationError("DAX table expressions must reference a single base table")

    def _append_tables(self, ordered_tables: list[str], seen_tables: set[str], columns: Iterable[ColumnRef]) -> None:
        for column in columns:
            if not column.table:
                continue
            key = column.table.lower()
            if key in seen_tables:
                continue
            ordered_tables.append(column.table)
            seen_tables.add(key)

    def _tables_for_scalar_fragment(self, fragment: _SqlFragment) -> list[str]:
        referenced_tables: dict[str, str] = {}
        for column in fragment.columns:
            if not column.table:
                continue
            key = column.table.lower()
            referenced_tables.setdefault(key, column.table)

        tables: list[str] = []
        if self._base_table and self._base_table.lower() in referenced_tables:
            tables.append(referenced_tables.pop(self._base_table.lower()))
        for _, table_name in sorted(referenced_tables.items(), key=lambda item: item[0]):
            tables.append(table_name)
        return tables

    def _scalar_fragment_sql_with_from(self, fragment: _SqlFragment) -> str:
        tables = self._tables_for_scalar_fragment(fragment)
        if not tables:
            return fragment.sql
        from_clause = self._build_from_clause_for_tables(tables)
        return f"(SELECT {fragment.sql} FROM {from_clause})"

    def _build_relationship_adjacency(self, edges: list[RelationshipEdge]) -> dict[str, list[tuple[str, str, str]]]:
        adjacency: dict[str, list[tuple[str, str, str]]] = {}
        for edge in edges:
            from_table = edge.from_table
            to_table = edge.to_table
            adjacency.setdefault(from_table, []).append((to_table, edge.from_column, edge.to_column))
            adjacency.setdefault(to_table, []).append((from_table, edge.to_column, edge.from_column))
        return adjacency

    def _find_relationship_path(self, base_table: str, target_table: str) -> list[tuple[str, str, str, str]] | None:
        if base_table == target_table:
            return []

        visited = {base_table}
        queue = deque([base_table])
        parent: dict[str, tuple[str, str, str]] = {}

        while queue:
            current = queue.popleft()
            for next_table, current_col, next_col in self._relationship_adjacency.get(current, []):
                if next_table in visited:
                    continue
                visited.add(next_table)
                parent[next_table] = (current, current_col, next_col)
                if next_table == target_table:
                    path: list[tuple[str, str, str, str]] = []
                    node = target_table
                    while node != base_table:
                        prev, prev_col, node_col = parent[node]
                        path.append((prev, node, prev_col, node_col))
                        node = prev
                    path.reverse()
                    return path
                queue.append(next_table)

        return None

    def _find_relationship_path_from_joined(
        self, joined_tables: list[str], target_table: str
    ) -> list[tuple[str, str, str, str]] | None:
        best_path: list[tuple[str, str, str, str]] | None = None
        for anchor in joined_tables:
            path = self._find_relationship_path(anchor, target_table)
            if path is None:
                continue
            if best_path is None or len(path) < len(best_path):
                best_path = path
        return best_path

    def _build_from_clause_for_tables(self, tables_in_order: list[str]) -> str:
        if not tables_in_order:
            return self._default_table_sql()

        base_table = tables_in_order[0]
        from_parts = [self._table_sql(base_table)]
        joined_tables = {base_table.lower()}
        joined_order = [base_table]

        for table in tables_in_order[1:]:
            table_key = table.lower()
            if table_key in joined_tables:
                continue
            path = self._find_relationship_path_from_joined(joined_order, table)
            if path is None:
                if self._allow_unrelated_table_cross_join:
                    self._append_unrelated_cross_join_warning(base_table, table)
                    from_parts.append(f"CROSS JOIN {self._table_sql(table)}")
                    joined_tables.add(table_key)
                    joined_order.append(table)
                    continue
                raise DaxTranslationError(f"No relationship path between {base_table} and {table}")
            for from_table, to_table, from_col, to_col in path:
                to_key = to_table.lower()
                if to_key in joined_tables:
                    continue
                left_table = self._table_sql(from_table)
                right_table = self._table_sql(to_table)
                from_col_sql = self._quote_identifier(from_col)
                to_col_sql = self._quote_identifier(to_col)
                from_parts.append(
                    f"LEFT JOIN {right_table} ON {left_table}.{from_col_sql} = {right_table}.{to_col_sql}"
                )
                joined_tables.add(to_key)
                joined_order.append(to_table)

        return " ".join(from_parts)

    def _append_unrelated_cross_join_warning(self, base_table: str, table: str) -> None:
        key = ("dax_unrelated_cross_join", base_table.lower(), table.lower())
        if key in self._warning_keys:
            return
        self._warning_keys.add(key)
        self._warnings.append(
            {
                "code": "dax_unrelated_cross_join",
                "context": "query",
                "base_table": base_table,
                "table": table,
                "message": (
                    f"DAX query cross joins unrelated table '{table}' with '{base_table}' "
                    "because no relationship path is defined"
                ),
            }
        )

    def _validate_time_argument(self, expr: Any | None) -> None:
        if not expr:
            return
        candidate = self._unwrap(expr)
        if isinstance(candidate, self.dax.HierarchyRef):
            table = candidate.table.name
            column = candidate.levels[-1] if candidate.levels else candidate.column
            self._validate_time_dimension(table, column)
            return
        if isinstance(candidate, self.dax.TableColumnRef):
            self._validate_time_dimension(candidate.table.name, candidate.column)
            return
        if self.time_dimensions_by_table:
            raise DaxTranslationError("Time intelligence requires a table time column argument")

    def _validate_time_dimension(self, table: str | None, column: str) -> None:
        if not self.time_dimensions_by_table:
            return
        table_name = table or self.model_name
        if not table_name:
            return
        known_dims = self.time_dimensions_by_table.get(table_name)
        if known_dims is None:
            known_dims = set()
            for key, value in self.time_dimensions_by_table.items():
                if key.lower() == table_name.lower():
                    known_dims = value
                    break
        if known_dims and column in known_dims:
            return
        raise DaxTranslationError(f"{table_name}[{column}] is not a known time dimension")

    def _allow_cross_table_context(self):
        class _Context:
            def __init__(self, outer):
                self.outer = outer
                self.prior = outer._allow_cross_table

            def __enter__(self):
                self.outer._allow_cross_table = True
                return self

            def __exit__(self, exc_type, exc, tb):
                self.outer._allow_cross_table = self.prior
                return False

        return _Context(self)

    def _prefer_unqualified_base_table_context(self):
        class _Context:
            def __init__(self, outer):
                self.outer = outer
                self.prior = outer._prefer_unqualified_base_table

            def __enter__(self):
                self.outer._prefer_unqualified_base_table = True
                return self

            def __exit__(self, exc_type, exc, tb):
                self.outer._prefer_unqualified_base_table = self.prior
                return False

        return _Context(self)

    def _measure_eval_context(self, group_by_cols: set[ColumnRef], filter_cols: set[ColumnRef]):
        class _Context:
            def __init__(self, outer):
                self.outer = outer
                self.prior_group = outer._current_group_by_columns
                self.prior_filter = outer._current_filter_columns
                self.group = frozenset(group_by_cols)
                self.filters = frozenset(filter_cols)

            def __enter__(self):
                self.outer._current_group_by_columns = self.group
                self.outer._current_filter_columns = self.filters
                return self

            def __exit__(self, exc_type, exc, tb):
                self.outer._current_group_by_columns = self.prior_group
                self.outer._current_filter_columns = self.prior_filter
                return False

        return _Context(self)

    def _parse_dateadd(self, call: Any) -> tuple[int, str] | None:
        if len(call.args) < 3:
            return None
        offset = self._number_literal_value(call.args[1])
        unit = self._identifier_literal_value(call.args[2])
        if offset is None or unit is None:
            return None
        normalized_unit = unit.lower()
        if normalized_unit.endswith("s"):
            normalized_unit = normalized_unit[:-1]
        # DAX DATEADD direction is inverse of row-offset semantics used by
        # window LAG/LEAD generation: -1 YEAR means prior period (lag +1).
        return -offset, normalized_unit

    def _string_literal_value(self, expr: Any) -> str | None:
        expr = self._unwrap(expr)
        if isinstance(expr, self.dax.String):
            return expr.value
        return None

    def _identifier_literal_value(self, expr: Any) -> str | None:
        expr = self._unwrap(expr)
        if isinstance(expr, self.dax.Identifier):
            return expr.name
        if isinstance(expr, self.dax.String):
            return expr.value
        return None

    def _number_literal_value(self, expr: Any) -> int | None:
        expr = self._unwrap(expr)
        if isinstance(expr, self.dax.Number):
            try:
                return int(float(expr.value))
            except ValueError:
                return None
        if isinstance(expr, self.dax.Unary):
            inner = self._number_literal_value(expr.expr)
            if inner is None:
                return None
            op_name = getattr(expr.op, "name", str(expr.op)).lower()
            if "minus" in op_name:
                return -inner
            if "plus" in op_name:
                return inner
            return None
        return None

    def _topn_numeric_arg_sql(self, expr: Any, *, function_name: str, arg_name: str) -> str:
        literal_value = self._number_literal_value(expr)
        if literal_value is not None:
            return str(literal_value)

        try:
            with self._allow_cross_table_context():
                fragment = self._translate_scalar(expr)
        except DaxTranslationError as exc:
            raise DaxTranslationError(f"{function_name} {arg_name} must be a number") from exc

        if fragment.columns:
            raise DaxTranslationError(f"{function_name} {arg_name} must be a number")
        return f"CAST(({fragment.sql}) AS BIGINT)"

    def _is_true_literal(self, expr: Any) -> bool:
        expr = self._unwrap(expr)
        if isinstance(expr, self.dax.Boolean):
            return expr.value is True
        if isinstance(expr, self.dax.FunctionCall):
            return expr.name.lower() == "true" and not expr.args
        if isinstance(expr, self.dax.Identifier) and expr.name.lower() == "true":
            return True
        return False

    def _is_blank_expr(self, expr: Any) -> bool:
        expr = self._unwrap(expr)
        if isinstance(expr, self.dax.Blank):
            return True
        if isinstance(expr, self.dax.FunctionCall):
            return expr.name.lower() == "blank" and not expr.args
        if isinstance(expr, self.dax.Identifier):
            return expr.name.lower() == "blank"
        return False

    @staticmethod
    def _quote_string(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    @staticmethod
    def _quote_identifier(name: str) -> str:
        if _is_safe_identifier(name):
            return name
        escaped = name.replace('"', '""')
        return f'"{escaped}"'


@dataclass(frozen=True)
class _SqlFragment:
    sql: str
    columns: frozenset[ColumnRef]

    def wrap(self, sql: str) -> _SqlFragment:
        return _SqlFragment(sql, self.columns)


@dataclass(frozen=True)
class _OrderKeySql:
    direct_expr_sql: str
    direct_order_sql: str
    wrapped_order_sql: str
    wrapped_ref_sql: str
    direction: str


class _DefineResolver:
    _AMBIGUOUS = object()

    def __init__(self, dax_ast: Any, define_block: Any | None) -> None:
        self.dax = dax_ast
        self._measure_defs: dict[str, Any] = {}
        self._table_defs: dict[str, Any] = {}
        self._var_defs: dict[str, Any] = {}
        self._function_defs: dict[str, Any] = {}
        self._column_defs: dict[tuple[str | None, str], Any] = {}
        self._column_defs_by_name: dict[str, Any] = {}

        if define_block is None:
            return

        for definition in define_block.defs:
            if isinstance(definition, self.dax.MeasureDef):
                self._measure_defs[definition.name.lower()] = definition.expr
            elif isinstance(definition, self.dax.TableDef):
                self._table_defs[definition.name.lower()] = definition.expr
            elif isinstance(definition, self.dax.VarDef):
                self._var_defs[definition.name.lower()] = definition.expr
            elif isinstance(definition, self.dax.FunctionDef):
                self._function_defs[definition.name.lower()] = definition
            elif isinstance(definition, self.dax.ColumnDef):
                table_name = definition.table.name.lower() if definition.table else None
                col_name = definition.name.lower()
                self._column_defs[(table_name, col_name)] = definition.expr
                current = self._column_defs_by_name.get(col_name)
                if current is None:
                    self._column_defs_by_name[col_name] = definition.expr
                else:
                    self._column_defs_by_name[col_name] = self._AMBIGUOUS

    def resolve_expr(self, expr: Any) -> Any:
        return self._resolve(expr, stack=(), bindings={})

    def _resolve(
        self,
        expr: Any,
        stack: tuple[tuple[str, str], ...],
        bindings: dict[str, Any],
    ) -> Any:
        if isinstance(expr, self.dax.Paren):
            return self.dax.Paren(expr=self._resolve(expr.expr, stack, bindings))

        if isinstance(expr, self.dax.BracketRef):
            key = ("measure", expr.name.lower())
            target = self._measure_defs.get(key[1])
            if target is not None:
                if key in stack:
                    raise DaxTranslationError(f"Cyclic DEFINE MEASURE reference for [{expr.name}]")
                return self.dax.Paren(self._resolve(target, stack + (key,), bindings))
            column_target = self._column_defs_by_name.get(expr.name.lower())
            if column_target is self._AMBIGUOUS:
                return expr
            if column_target is not None:
                column_key = ("column", expr.name.lower())
                if column_key in stack:
                    raise DaxTranslationError(f"Cyclic DEFINE COLUMN reference for [{expr.name}]")
                return self.dax.Paren(self._resolve(column_target, stack + (column_key,), bindings))
            return expr

        if isinstance(expr, self.dax.TableRef):
            key = ("table", expr.table.name.lower())
            target = self._table_defs.get(key[1])
            if target is None:
                return expr
            if key in stack:
                raise DaxTranslationError(f"Cyclic DEFINE TABLE reference for {expr.table.name}")
            return self._resolve(target, stack + (key,), bindings)

        if isinstance(expr, self.dax.TableColumnRef):
            key_name = (expr.table.name.lower(), expr.column.lower())
            target = self._column_defs.get(key_name)
            if target is None:
                return expr
            key = ("column", f"{expr.table.name.lower()}.{expr.column.lower()}")
            if key in stack:
                raise DaxTranslationError(f"Cyclic DEFINE COLUMN reference for {expr.table.name}[{expr.column}]")
            return self.dax.Paren(self._resolve(target, stack + (key,), bindings))

        if isinstance(expr, self.dax.Identifier):
            bound = bindings.get(expr.name.lower())
            if bound is not None:
                return bound
            key = ("var", expr.name.lower())
            target = self._var_defs.get(key[1])
            if target is not None:
                if key in stack:
                    raise DaxTranslationError(f"Cyclic DEFINE VAR reference for {expr.name}")
                return self.dax.Paren(self._resolve(target, stack + (key,), bindings))

            table_target = self._table_defs.get(expr.name.lower())
            if table_target is not None:
                table_key = ("table", expr.name.lower())
                if table_key in stack:
                    raise DaxTranslationError(f"Cyclic DEFINE TABLE reference for {expr.name}")
                return self._resolve(table_target, stack + (table_key,), bindings)

            return expr

        if isinstance(expr, self.dax.FunctionCall):
            resolved_args = [self._resolve(arg, stack, bindings) for arg in expr.args]
            function_def = self._function_defs.get(expr.name.lower())
            if function_def is None:
                return self.dax.FunctionCall(name=expr.name, args=resolved_args)

            if len(resolved_args) != len(function_def.params):
                raise DaxTranslationError(
                    f"DEFINE FUNCTION {function_def.name} expects {len(function_def.params)} args, got {len(resolved_args)}"
                )

            key = ("function", function_def.name.lower())
            if key in stack:
                raise DaxTranslationError(f"Cyclic DEFINE FUNCTION reference for {function_def.name}")

            scoped_bindings = dict(bindings)
            for param, arg in zip(function_def.params, resolved_args, strict=False):
                scoped_bindings[param.name.lower()] = arg

            return self.dax.Paren(self._resolve(function_def.body, stack + (key,), scoped_bindings))

        if isinstance(expr, self.dax.Unary):
            return self.dax.Unary(op=expr.op, expr=self._resolve(expr.expr, stack, bindings))

        if isinstance(expr, self.dax.Binary):
            return self.dax.Binary(
                op=expr.op,
                left=self._resolve(expr.left, stack, bindings),
                right=self._resolve(expr.right, stack, bindings),
            )

        if isinstance(expr, self.dax.VarBlock):
            scoped_bindings = dict(bindings)
            decls = []
            for decl in expr.decls:
                resolved_decl = self._resolve(decl.expr, stack, scoped_bindings)
                decls.append(self.dax.VarDecl(name=decl.name, expr=resolved_decl))
                scoped_bindings[decl.name.lower()] = resolved_decl
            body = self._resolve(expr.body, stack, scoped_bindings)
            return self.dax.VarBlock(decls=decls, body=body)

        if isinstance(expr, self.dax.TableConstructor):
            rows = [[self._resolve(value, stack, bindings) for value in row] for row in expr.rows]
            return self.dax.TableConstructor(rows=rows)

        return expr


def _translate_order_keys(stmt: Any, translator: _DaxTranslator, resolver: _DefineResolver) -> list[_OrderKeySql]:
    order_keys: list[_OrderKeySql] = []
    for key in stmt.order_by:
        resolved_expr = resolver.resolve_expr(key.expr)
        with translator._allow_cross_table_context():
            fragment = translator._translate_scalar(resolved_expr)

        direction = "ASC" if key.direction == translator.dax.SortDirection.asc else "DESC"
        direct_order_sql = f"{fragment.sql} {direction}"

        wrapped_expr_sql = _rewrite_order_expr_for_wrapped(fragment.sql)
        wrapped_ref_sql = wrapped_expr_sql
        wrapped_order_sql = f"{wrapped_expr_sql} {direction}"

        order_keys.append(
            _OrderKeySql(
                direct_expr_sql=fragment.sql,
                direct_order_sql=direct_order_sql,
                wrapped_order_sql=wrapped_order_sql,
                wrapped_ref_sql=wrapped_ref_sql,
                direction=direction,
            )
        )

    return order_keys


def _apply_order_and_start_at(
    base_sql: str,
    stmt: Any,
    translator: _DaxTranslator,
    resolver: _DefineResolver,
    order_keys: list[_OrderKeySql],
) -> str:
    if stmt.start_at:
        if not order_keys:
            raise DaxTranslationError("START AT requires ORDER BY")

        if len(stmt.start_at) > len(order_keys):
            raise DaxTranslationError("START AT has more arguments than ORDER BY")

        value_sql: list[str] = []
        for value in stmt.start_at:
            resolved = resolver.resolve_expr(value)
            value_sql.append(translator._translate_scalar(resolved).sql)

        start_order_keys = order_keys[: len(value_sql)]
        predicate = _build_start_at_predicate(start_order_keys, value_sql)
        sql = f"SELECT * FROM ({base_sql}) AS q WHERE {predicate}"
        wrapped_order = [key.wrapped_order_sql for key in order_keys]
        return f"{sql} ORDER BY {', '.join(wrapped_order)}"

    if not order_keys:
        return base_sql

    wrapped_order = [key.wrapped_order_sql for key in order_keys]
    return f"SELECT * FROM ({base_sql}) AS q ORDER BY {', '.join(wrapped_order)}"


def _build_start_at_predicate(order_keys: list[_OrderKeySql], start_values_sql: list[str]) -> str:
    disjuncts: list[str] = []
    for idx, value_sql in enumerate(start_values_sql):
        conjuncts: list[str] = []
        for prev_idx in range(idx):
            prev_ref = order_keys[prev_idx].wrapped_ref_sql
            conjuncts.append(f"{prev_ref} = {start_values_sql[prev_idx]}")

        ref = order_keys[idx].wrapped_ref_sql

        is_last = idx == len(start_values_sql) - 1
        direction = order_keys[idx].direction
        if direction == "ASC":
            op = ">=" if is_last else ">"
        else:
            op = "<=" if is_last else "<"
        conjuncts.append(f"{ref} {op} {value_sql}")
        disjuncts.append("(" + " AND ".join(conjuncts) + ")")

    return " OR ".join(disjuncts)


def _rewrite_order_expr_for_wrapped(sql: str) -> str:
    return _rewrite_expr_for_alias(sql, "q")


def _rewrite_expr_for_alias(
    sql: str,
    alias: str,
    source_tables: set[str] | None = None,
    source_columns: set[str] | None = None,
    source_column_aliases: dict[str, str] | None = None,
    ambiguous_source_aliases: set[str] | None = None,
    local_columns: set[str] | None = None,
    ambiguous_source_columns: set[str] | None = None,
    allow_fallback: bool = True,
    strict_source_resolution: bool = False,
) -> str:
    try:
        import sqlglot
        from sqlglot import exp

        parsed = sqlglot.parse_one(sql, dialect="duckdb")
        source_tables_lower = {table.lower() for table in (source_tables or set())}
        source_columns_lower = {column.lower() for column in (source_columns or set())}
        ambiguous_source_aliases_lower = {name.lower() for name in (ambiguous_source_aliases or set())}
        local_columns_lower = {column.lower() for column in (local_columns or set())}
        ambiguous_source_columns_lower = {column.lower() for column in (ambiguous_source_columns or set())}
        source_has_wildcard = "*" in source_columns_lower
        source_has_multiple_tables = len(source_tables_lower) > 1
        source_aliases = source_column_aliases or {}
        for column in parsed.find_all(exp.Column):
            replacement_name: str | None = None
            if source_tables_lower:
                table_name = column.table
                if table_name:
                    if table_name.lower() not in source_tables_lower:
                        continue
                    if _column_table_is_local_to_select(column):
                        continue
                    qualified_key = f"{table_name.lower()}.{column.name.lower()}"
                    if (
                        qualified_key in ambiguous_source_aliases_lower
                        or column.name.lower() in ambiguous_source_aliases_lower
                    ):
                        if strict_source_resolution:
                            raise DaxTranslationError(
                                f"Ambiguous outer column reference '{table_name}.{column.name}' for alias '{alias}'"
                            )
                        continue
                    replacement_name = source_aliases.get(qualified_key) or source_aliases.get(column.name.lower())
                    if (
                        replacement_name is None
                        and not source_has_wildcard
                        and column.name.lower() not in source_columns_lower
                    ):
                        continue
                    if replacement_name is None and source_has_wildcard and source_has_multiple_tables:
                        if strict_source_resolution:
                            raise DaxTranslationError(
                                f"Ambiguous outer column reference '{table_name}.{column.name}' for alias '{alias}'"
                            )
                        continue
                    if replacement_name is None and column.name.lower() in ambiguous_source_columns_lower:
                        if strict_source_resolution:
                            raise DaxTranslationError(
                                f"Ambiguous outer column reference '{column.name}' for alias '{alias}'"
                            )
                        continue
                elif source_columns_lower:
                    column_key = column.name.lower()
                    if _column_name_is_local_to_select(column, local_columns_lower):
                        continue
                    if column_key in ambiguous_source_aliases_lower:
                        if strict_source_resolution:
                            raise DaxTranslationError(
                                f"Ambiguous outer column reference '{column.name}' for alias '{alias}'"
                            )
                        continue
                    replacement_name = source_aliases.get(column_key)
                    if column_key not in source_columns_lower and replacement_name is None:
                        continue
                    if replacement_name is None and column_key in ambiguous_source_columns_lower:
                        if strict_source_resolution:
                            raise DaxTranslationError(
                                f"Ambiguous outer column reference '{column.name}' for alias '{alias}'"
                            )
                        continue
                else:
                    continue
            column.set("table", exp.to_identifier("q"))
            if alias != "q":
                column.set("table", exp.to_identifier(alias))
            if replacement_name:
                column.set("this", exp.to_identifier(replacement_name))
        return parsed.sql(dialect="duckdb")
    except DaxTranslationError:
        raise
    except Exception as exc:
        if strict_source_resolution and not allow_fallback:
            raise DaxTranslationError(
                f"Unable to safely correlate outer column references for alias '{alias}'"
            ) from exc
        if not allow_fallback:
            return sql
        return _fallback_rewrite_expr_for_alias(sql, alias, source_tables, source_columns)


_QUALIFIED_TABLE_PREFIX_RE = re.compile(r'("(?:""|[^"])*"|[A-Za-z_][A-Za-z0-9_]*)\.')


def _fallback_rewrite_expr_for_alias(
    sql: str,
    alias: str,
    source_tables: set[str] | None = None,
    source_columns: set[str] | None = None,
) -> str:
    # Best-effort rewrite for SQLGlot parse failures. Keep expression usable against
    # SELECT * FROM (<base>) AS alias by collapsing table qualifiers.
    if source_tables:
        rewritten = sql
        source_columns_lower = {column.lower() for column in (source_columns or set())}
        source_has_wildcard = "*" in source_columns_lower
        for table in source_tables:
            table_quoted = table.replace('"', '""')
            if source_columns_lower and not source_has_wildcard:
                for column in source_columns_lower:
                    if column == "*":
                        continue
                    rewritten = re.sub(
                        rf"\b{re.escape(table)}\.{re.escape(column)}\b",
                        f"{alias}.{column}",
                        rewritten,
                        flags=re.IGNORECASE,
                    )
                    rewritten = rewritten.replace(
                        f'"{table_quoted}"."{column}"',
                        f"{alias}.{column}",
                    )
            else:
                rewritten = re.sub(rf"\b{re.escape(table)}\.", f"{alias}.", rewritten, flags=re.IGNORECASE)
                rewritten = rewritten.replace(f'"{table_quoted}".', f"{alias}.")
        return rewritten

    rewritten = _QUALIFIED_TABLE_PREFIX_RE.sub(f"{alias}.", sql)
    stripped = rewritten.strip()
    if "." not in stripped and _can_qualify_identifier(stripped):
        return f"{alias}.{stripped}"
    return rewritten


def _column_table_is_local_to_select(column_expr: Any) -> bool:
    try:
        from sqlglot import exp

        table_name = getattr(column_expr, "table", None)
        if not table_name:
            return False
        table_key = table_name.lower()
        node = column_expr
        while node is not None:
            if isinstance(node, exp.Select):
                if table_key in _select_scope_table_names(node):
                    return True
            node = getattr(node, "parent", None)
        return False
    except Exception:
        return False


def _column_name_is_local_to_select(column_expr: Any, known_local_columns: set[str] | None = None) -> bool:
    try:
        from sqlglot import exp

        column_name = getattr(column_expr, "name", None)
        if not column_name:
            return False
        column_key = column_name.lower()
        if known_local_columns and column_key in known_local_columns:
            return True
        if getattr(column_expr, "table", None):
            return _column_table_is_local_to_select(column_expr)

        node = column_expr
        while node is not None:
            if isinstance(node, exp.Select):
                for source_expr in _select_scope_source_exprs(node):
                    if _source_expr_exposes_column(source_expr, column_key):
                        return True
            node = getattr(node, "parent", None)
        return False
    except Exception:
        if not known_local_columns:
            return False
        column_name = getattr(column_expr, "name", None)
        return bool(column_name and column_name.lower() in known_local_columns)


def _select_scope_source_exprs(select_expr: Any) -> list[Any]:
    source_exprs: list[Any] = []
    from_expr = select_expr.args.get("from")
    if from_expr is not None and getattr(from_expr, "this", None) is not None:
        source_exprs.append(from_expr.this)
    for join_expr in select_expr.args.get("joins") or []:
        if getattr(join_expr, "this", None) is not None:
            source_exprs.append(join_expr.this)
    return source_exprs


def _source_expr_exposes_column(source_expr: Any, column_key: str) -> bool:
    try:
        from sqlglot import exp

        alias = source_expr.args.get("alias")
        if isinstance(alias, exp.TableAlias):
            alias_columns = [col.name.lower() for col in alias.columns if getattr(col, "name", None)]
            if alias_columns:
                return column_key in alias_columns

        if isinstance(source_expr, exp.Subquery):
            return column_key in _query_expr_output_columns(source_expr.this)
        if isinstance(source_expr, exp.Values):
            if isinstance(alias, exp.TableAlias):
                alias_columns = [col.name.lower() for col in alias.columns if getattr(col, "name", None)]
                return column_key in alias_columns
            return False
        if isinstance(source_expr, exp.Paren):
            return _source_expr_exposes_column(source_expr.this, column_key)
        return False
    except Exception:
        return False


def _query_expr_output_columns(expr: Any) -> set[str]:
    try:
        from sqlglot import exp

        if isinstance(expr, exp.Subquery):
            return _query_expr_output_columns(expr.this)
        if isinstance(expr, exp.Paren):
            return _query_expr_output_columns(expr.this)
        if isinstance(expr, exp.Select):
            names: set[str] = set()
            for projection in expr.expressions:
                if isinstance(projection, exp.Star):
                    names.update(name.lower() for name in _select_star_output_columns(expr))
                    continue
                if isinstance(projection, exp.Column) and projection.name == "*":
                    qualifier = projection.table if projection.table else None
                    names.update(name.lower() for name in _select_star_output_columns(expr, qualifier))
                    continue
                output_name = projection.alias_or_name
                if output_name and output_name != "*":
                    names.add(output_name.lower())
            return names
        if isinstance(expr, (exp.Union, exp.Intersect, exp.Except)):
            return _query_expr_output_columns(expr.this)
        return set()
    except Exception:
        return set()


def _select_star_output_columns(select_expr: Any, qualifier: str | None = None) -> set[str]:
    columns: set[str] = set()
    qualifier_key = qualifier.lower() if qualifier else None
    for source_expr in _select_scope_source_exprs(select_expr):
        if qualifier_key is not None:
            source_keys: set[str] = set()
            alias = getattr(source_expr, "alias_or_name", None)
            if alias:
                source_keys.add(alias.lower())
            table_name = getattr(source_expr, "name", None)
            if table_name:
                source_keys.add(table_name.lower())
            if qualifier_key not in source_keys:
                continue
        columns.update(_source_expr_output_columns(source_expr))
    return columns


def _source_expr_output_columns(source_expr: Any) -> set[str]:
    try:
        from sqlglot import exp

        alias = source_expr.args.get("alias")
        if isinstance(alias, exp.TableAlias):
            alias_columns = [col.name for col in alias.columns if getattr(col, "name", None)]
            if alias_columns:
                return set(alias_columns)

        if isinstance(source_expr, exp.Subquery):
            return _query_expr_output_column_names(source_expr.this)
        if isinstance(source_expr, exp.Values):
            if isinstance(alias, exp.TableAlias):
                alias_columns = [col.name for col in alias.columns if getattr(col, "name", None)]
                return set(alias_columns)
            return set()
        if isinstance(source_expr, exp.Paren):
            return _source_expr_output_columns(source_expr.this)
        return set()
    except Exception:
        return set()


def _query_expr_output_column_names(expr: Any) -> set[str]:
    try:
        from sqlglot import exp

        if isinstance(expr, exp.Subquery):
            return _query_expr_output_column_names(expr.this)
        if isinstance(expr, exp.Paren):
            return _query_expr_output_column_names(expr.this)
        if isinstance(expr, exp.Select):
            names: set[str] = set()
            for projection in expr.expressions:
                if isinstance(projection, exp.Star):
                    names.update(_select_star_output_columns(expr))
                    continue
                if isinstance(projection, exp.Column) and projection.name == "*":
                    qualifier = projection.table if projection.table else None
                    names.update(_select_star_output_columns(expr, qualifier))
                    continue
                output_name = projection.alias_or_name
                if output_name and output_name != "*":
                    names.add(output_name)
            return names
        if isinstance(expr, (exp.Union, exp.Intersect, exp.Except)):
            return _query_expr_output_column_names(expr.this)
        return set()
    except Exception:
        return set()


def _select_scope_table_names(select_expr: Any) -> set[str]:
    try:
        names: set[str] = set()
        source_exprs = _select_scope_source_exprs(select_expr)

        from sqlglot import exp

        for source_expr in source_exprs:
            alias = getattr(source_expr, "alias_or_name", None)
            if alias:
                names.add(alias.lower())
            if isinstance(source_expr, exp.Table):
                table_name = source_expr.name
                if table_name:
                    names.add(table_name.lower())
        return names
    except Exception:
        return set()


def _identifier_name_from_sql(sql: str) -> str | None:
    try:
        import sqlglot
        from sqlglot import exp

        parsed = sqlglot.parse_one(sql, dialect="duckdb")
        if isinstance(parsed, exp.Identifier):
            return parsed.name
        if isinstance(parsed, exp.Column):
            return parsed.name
    except Exception:
        return None
    return None


def _column_name_from_expr_sql(sql: str) -> str | None:
    try:
        import sqlglot
        from sqlglot import exp

        parsed = sqlglot.parse_one(sql, dialect="duckdb")
        if isinstance(parsed, exp.Column):
            return parsed.name
        if isinstance(parsed, exp.Identifier):
            return parsed.name
    except Exception:
        return None
    return None


def _query_output_columns(sql: str) -> set[str]:
    try:
        import sqlglot

        parsed = sqlglot.parse_one(sql, dialect="duckdb")
        return _query_expr_output_column_names(parsed)
    except Exception:
        return set()


def _query_expr_output_column_name_counts(expr: Any) -> dict[str, int]:
    try:
        from sqlglot import exp

        if isinstance(expr, exp.Subquery):
            return _query_expr_output_column_name_counts(expr.this)
        if isinstance(expr, exp.Paren):
            return _query_expr_output_column_name_counts(expr.this)
        if isinstance(expr, exp.Select):
            counts: dict[str, int] = {}
            for projection in expr.expressions:
                if isinstance(projection, exp.Star):
                    for name in _select_star_output_columns(expr):
                        key = name.lower()
                        counts[key] = counts.get(key, 0) + 1
                    continue
                if isinstance(projection, exp.Column) and projection.name == "*":
                    qualifier = projection.table if projection.table else None
                    for name in _select_star_output_columns(expr, qualifier):
                        key = name.lower()
                        counts[key] = counts.get(key, 0) + 1
                    continue
                output_name = projection.alias_or_name
                if output_name and output_name != "*":
                    key = output_name.lower()
                    counts[key] = counts.get(key, 0) + 1
            return counts
        if isinstance(expr, (exp.Union, exp.Intersect, exp.Except)):
            return _query_expr_output_column_name_counts(expr.this)
        return {}
    except Exception:
        return {}


def _query_output_column_name_counts(sql: str) -> dict[str, int]:
    try:
        import sqlglot

        parsed = sqlglot.parse_one(sql, dialect="duckdb")
        return _query_expr_output_column_name_counts(parsed)
    except Exception:
        return {}


def _query_source_table_names(sql: str) -> set[str]:
    try:
        import sqlglot
        from sqlglot import exp

        parsed = sqlglot.parse_one(sql, dialect="duckdb")
        table_names: set[str] = set()
        for table_expr in parsed.find_all(exp.Table):
            table_name = table_expr.name
            if table_name:
                table_names.add(table_name)
        return table_names
    except Exception:
        return set()


def _query_output_width(sql: str) -> int | None:
    try:
        import sqlglot
        from sqlglot import exp

        parsed = sqlglot.parse_one(sql, dialect="duckdb")
        select_exprs = parsed.selects if hasattr(parsed, "selects") else []
        if not select_exprs:
            return None
        for expr in select_exprs:
            if isinstance(expr, exp.Star):
                return None
            if isinstance(expr, exp.Column) and expr.name == "*":
                return None
        return len(select_exprs)
    except Exception:
        return None


def _query_uses_star_projection(sql: str) -> bool:
    try:
        import sqlglot
        from sqlglot import exp

        parsed = sqlglot.parse_one(sql, dialect="duckdb")
        select_exprs = parsed.selects if hasattr(parsed, "selects") else []
        for expr in select_exprs:
            if isinstance(expr, exp.Star):
                return True
            if isinstance(expr, exp.Column) and expr.name == "*":
                return True
        return False
    except Exception:
        return False


def _query_star_projection_qualifiers(sql: str) -> set[str | None]:
    try:
        import sqlglot
        from sqlglot import exp

        parsed = sqlglot.parse_one(sql, dialect="duckdb")
        qualifiers: set[str | None] = set()
        select_exprs = parsed.selects if hasattr(parsed, "selects") else []
        for expr in select_exprs:
            if isinstance(expr, exp.Star):
                qualifiers.add(None)
                continue
            if isinstance(expr, exp.Column) and expr.name == "*":
                qualifiers.add(expr.table or None)
        return qualifiers
    except Exception:
        return set()


def _query_output_lineage_aliases(sql: str) -> tuple[dict[str, str], set[str]]:
    try:
        import sqlglot
        from sqlglot import exp

        parsed = sqlglot.parse_one(sql, dialect="duckdb")
        select_exprs = parsed.selects if hasattr(parsed, "selects") else []
        aliases: dict[str, str] = {}
        ambiguous: set[str] = set()
        for expr in select_exprs:
            output_name = expr.alias_or_name
            if not output_name:
                continue

            source_expr = expr.this if isinstance(expr, exp.Alias) else expr
            if isinstance(source_expr, exp.Column):
                source_name = source_expr.name
                if not source_name:
                    continue
                source_keys = [source_name.lower()]
                source_table = source_expr.table
                if source_table:
                    source_keys.append(f"{source_table.lower()}.{source_name.lower()}")
                for source_key in source_keys:
                    if source_key in ambiguous:
                        continue
                    if source_key in aliases:
                        ambiguous.add(source_key)
                        aliases.pop(source_key, None)
                        continue
                    aliases[source_key] = output_name
        return aliases, ambiguous
    except Exception:
        return {}, set()


def _is_unsupported_table_expression_error(exc: DaxTranslationError) -> bool:
    message = str(exc)
    return message.startswith("Unsupported table function '") or message.startswith(
        "Unsupported table expression type '"
    )


def _order_ref_name(expr: Any, dax_ast: Any) -> str | None:
    while isinstance(expr, dax_ast.Paren):
        expr = expr.expr

    if isinstance(expr, dax_ast.TableColumnRef):
        return expr.column
    if isinstance(expr, dax_ast.HierarchyRef):
        return expr.levels[-1] if expr.levels else expr.column
    if isinstance(expr, dax_ast.BracketRef):
        return expr.name
    if isinstance(expr, dax_ast.Identifier):
        return expr.name
    return None


def _is_safe_identifier(name: str) -> bool:
    if not name:
        return False
    first = name[0]
    if not (first.isalpha() or first == "_"):
        return False
    for ch in name[1:]:
        if not (ch.isalnum() or ch == "_"):
            return False
    return True


def _can_qualify_identifier(sql: str) -> bool:
    if _is_safe_identifier(sql):
        return True
    if sql.startswith('"') and sql.endswith('"') and "(" not in sql and " " not in sql:
        return True
    return False


def _columns_match(left: ColumnRef, right: ColumnRef) -> bool:
    if left.column.lower() != right.column.lower():
        return False
    if left.table and right.table and left.table.lower() != right.table.lower():
        return False
    return True


def _comparison_type_for_unit(unit: str) -> str:
    return {
        "day": "dod",
        "week": "wow",
        "month": "mom",
        "quarter": "qoq",
        "year": "yoy",
    }.get(unit, "prior_period")


def _time_offset_for_period_function(name: str) -> tuple[int, str] | None:
    return {
        "previousday": (1, "day"),
        "previousweek": (1, "week"),
        "previousmonth": (1, "month"),
        "previousquarter": (1, "quarter"),
        "previousyear": (1, "year"),
        "nextday": (-1, "day"),
        "nextweek": (-1, "week"),
        "nextmonth": (-1, "month"),
        "nextquarter": (-1, "quarter"),
        "nextyear": (-1, "year"),
    }.get(name)


def _load_dax_ast():
    try:
        from sidemantic_dax import ast as dax_ast
    except Exception as exc:
        raise DaxTranslationError("sidemantic_dax is required for DAX translation") from exc
    return dax_ast
