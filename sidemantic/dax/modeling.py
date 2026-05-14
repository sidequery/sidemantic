"""Lower DAX-authored model definitions into executable Sidemantic fields."""

from __future__ import annotations

from typing import Any

from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.dax.translator import (
    DaxTranslationError,
    RelationshipEdge,
    translate_dax_metric,
    translate_dax_scalar,
    translate_dax_table,
)


class DaxModelingError(ValueError):
    """Raised when a DAX-authored model definition cannot be lowered."""


def lower_dax_graph_expressions(graph: SemanticGraph) -> None:
    """Lower first-class DAX expressions on graph models and graph metrics.

    TMDL import can keep warning/fallback behavior inside the TMDL adapter. Native
    Sidemantic authoring is stricter: if a user explicitly marks an expression as
    DAX, unsupported syntax should fail at model load time instead of becoming
    accidental SQL.
    """
    for model in graph.models.values():
        lower_dax_model_expressions(model, graph)

    for metric in graph.metrics.values():
        _lower_metric_dax(metric, graph, model=None)


def lower_dax_model_expressions(model: Model, graph: SemanticGraph) -> None:
    """Lower DAX table, dimension, and metric expressions for a model."""
    _lower_model_table_dax(model, graph)

    for dimension in model.dimensions:
        source = _dax_source(dimension)
        if source is None:
            continue

        expr = _parse_dax_expression(source, f"dimension '{model.name}.{dimension.name}'")
        context = _build_dax_translation_context(graph)
        try:
            dimension.sql = translate_dax_scalar(expr, model.name, **_scalar_context(context))
        except DaxTranslationError as exc:
            raise DaxModelingError(f"DAX dimension '{model.name}.{dimension.name}' is unsupported: {exc}") from exc
        dimension.dax = source
        dimension.expression_language = "dax"
        setattr(dimension, "_dax_expression", source)
        setattr(dimension, "_dax_lowered", True)

    # Lower metrics in declared order so simple base measures become available
    # to later DAX metrics in the same model.
    for metric in list(model.metrics):
        _lower_metric_dax(metric, graph, model=model)


def _lower_model_table_dax(model: Model, graph: SemanticGraph) -> None:
    source = _dax_source(model)
    if source is None:
        return

    expr = _parse_dax_expression(source, f"model '{model.name}'")
    context = _build_dax_translation_context(graph)
    try:
        translation = translate_dax_table(expr, model_name=None, **_table_context(context))
    except DaxTranslationError as exc:
        raise DaxModelingError(f"DAX model '{model.name}' is unsupported: {exc}") from exc

    model.sql = translation.sql
    model.table = None
    model.dax = source
    model.expression_language = "dax"
    setattr(model, "_dax_expression", source)
    setattr(model, "_dax_lowered", True)
    _append_modeling_warnings(graph, model, translation.warnings)
    if translation.required_models:
        setattr(model, "_dax_required_models", sorted(translation.required_models))


def _lower_metric_dax(metric: Metric, graph: SemanticGraph, model: Model | None) -> None:
    source = _dax_source(metric)
    if source is None:
        return

    context_name = f"metric '{metric.name}'" if model is None else f"metric '{model.name}.{metric.name}'"
    expr = _parse_dax_expression(source, context_name)
    context = _build_dax_translation_context(graph)
    model_name = model.name if model is not None else _single_model_for_graph_metric(metric, graph)
    try:
        translation = translate_dax_metric(expr, model_name, **_metric_context(context))
    except DaxTranslationError as exc:
        raise DaxModelingError(f"DAX {context_name} is unsupported: {exc}") from exc

    base_metric_ref = translation.base_metric
    if (
        model is not None
        and translation.type == "time_comparison"
        and not base_metric_ref
        and translation.inline_base_agg
        and translation.inline_base_sql
    ):
        existing_names = {candidate.name for candidate in model.metrics}
        base_name = _inline_base_metric_name(metric.name, existing_names)
        model.metrics.append(
            Metric(
                name=base_name,
                agg=translation.inline_base_agg,
                sql=translation.inline_base_sql,
                filters=translation.inline_base_filters or None,
            )
        )
        base_metric_ref = f"{model.name}.{base_name}"

    metric.dax = source
    metric.expression_language = "dax"
    metric.agg = translation.agg
    metric.sql = translation.sql
    metric.type = translation.type
    if metric.type is None and translation.sql and not translation.agg:
        metric.type = "derived"
    metric.base_metric = base_metric_ref
    metric.comparison_type = translation.comparison_type
    metric.calculation = translation.calculation
    metric.time_offset = translation.time_offset
    metric.window = translation.window
    metric.grain_to_date = translation.grain_to_date
    metric.window_order = translation.window_order
    metric.filters = translation.filters or None
    metric.relationship_overrides = translation.relationship_overrides or None
    metric.required_models = sorted(translation.required_models) if translation.required_models else None
    setattr(metric, "_dax_expression", source)
    setattr(metric, "_dax_lowered", True)


def _single_model_for_graph_metric(metric: Metric, graph: SemanticGraph) -> str:
    if len(graph.models) == 1:
        return next(iter(graph.models))
    raise DaxModelingError(
        f"DAX graph metric '{metric.name}' needs a model context; define it under a model or qualify it later"
    )


def _dax_source(obj: Any) -> str | None:
    if bool(getattr(obj, "_dax_skip_native_lowering", False)):
        return None
    if bool(getattr(obj, "_dax_lowered", False)):
        return None

    dax = getattr(obj, "dax", None)
    language = getattr(obj, "expression_language", None)

    if isinstance(dax, str) and dax.strip():
        if language == "sql":
            raise DaxModelingError(
                f"{obj.__class__.__name__} defines dax but expression_language='sql'; "
                "set expression_language='dax' or remove expression_language"
            )
        return dax
    if language == "dax":
        sql = getattr(obj, "sql", None)
        if isinstance(sql, str) and sql.strip():
            return sql
        raise DaxModelingError(f"{obj.__class__.__name__} uses expression_language='dax' but has no dax/sql text")
    return None


def _append_modeling_warnings(graph: SemanticGraph, model: Model, warnings: list[dict[str, Any]]) -> None:
    if not warnings:
        return
    existing = getattr(graph, "import_warnings", None)
    if not isinstance(existing, list):
        existing = []
        graph.import_warnings = existing
    for warning in warnings:
        if not isinstance(warning, dict):
            continue
        existing.append(
            {
                "code": str(warning.get("code") or "dax_translation_warning"),
                "context": "calculated_table",
                "model": model.name,
                "name": model.name,
                "message": str(warning.get("message") or "DAX translation warning"),
            }
        )


def _parse_dax_expression(source: str, context: str) -> Any:
    try:
        from sidemantic_dax.ast import parse_expression
    except Exception as exc:
        raise DaxModelingError(
            "sidemantic_dax is required for DAX model definitions. Install DAX extras and retry "
            "(e.g. `uv sync --extra dax`)."
        ) from exc

    try:
        return parse_expression(source)
    except Exception as exc:
        raise DaxModelingError(f"Could not parse DAX {context}: {exc}") from exc


def _metric_context(context: dict[str, Any]) -> dict[str, Any]:
    return {
        "column_sql_by_table": context["column_sql_by_table"],
        "measure_names_by_table": context["measure_names_by_table"],
        "measure_aggs_by_table": context["measure_aggs_by_table"],
        "measure_sql_by_table": context["measure_sql_by_table"],
        "measure_filters_by_table": context["measure_filters_by_table"],
        "time_dimensions_by_table": context["time_dimensions_by_table"],
        "relationship_edges": context["relationship_edges"],
    }


def _scalar_context(context: dict[str, Any]) -> dict[str, Any]:
    return {
        "column_sql_by_table": context["column_sql_by_table"],
        "measure_names_by_table": context["measure_names_by_table"],
        "time_dimensions_by_table": context["time_dimensions_by_table"],
    }


def _table_context(context: dict[str, Any]) -> dict[str, Any]:
    return {
        "column_sql_by_table": context["column_sql_by_table"],
        "measure_names_by_table": context["measure_names_by_table"],
        "relationship_edges": context["relationship_edges"],
    }


def _inline_base_metric_name(metric_name: str, existing_names: set[str]) -> str:
    stem_chars: list[str] = []
    for ch in metric_name:
        stem_chars.append(ch.lower() if ch.isalnum() else "_")
    stem = "".join(stem_chars).strip("_") or "metric"
    candidate = f"__{stem}_base"
    if candidate not in existing_names:
        return candidate
    idx = 2
    while f"{candidate}_{idx}" in existing_names:
        idx += 1
    return f"{candidate}_{idx}"


def _build_dax_translation_context(graph: SemanticGraph) -> dict[str, Any]:
    column_sql_by_table: dict[str, dict[str, str]] = {}
    measure_names_by_table: dict[str, set[str]] = {}
    measure_aggs_by_table: dict[str, dict[str, str]] = {}
    measure_sql_by_table: dict[str, dict[str, str]] = {}
    measure_filters_by_table: dict[str, dict[str, list[str]]] = {}
    time_dimensions_by_table: dict[str, set[str]] = {}

    for model_name, model in graph.models.items():
        column_sql_by_table[model_name] = {dim.name: (dim.sql or dim.name) for dim in model.dimensions}
        measure_names_by_table[model_name] = {metric.name for metric in model.metrics}
        measure_aggs_by_table[model_name] = {
            metric.name: metric.agg for metric in model.metrics if metric.agg and not _is_unlowered_dax_metric(metric)
        }
        measure_sql_by_table[model_name] = {
            metric.name: metric.sql for metric in model.metrics if metric.sql and not _is_unlowered_dax_metric(metric)
        }
        measure_filters_by_table[model_name] = {
            metric.name: list(metric.filters or [])
            for metric in model.metrics
            if metric.filters and not _is_unlowered_dax_metric(metric)
        }
        time_dimensions_by_table[model_name] = {dim.name for dim in model.dimensions if dim.type == "time"}

    edges: list[RelationshipEdge] = []
    seen_edges: set[tuple[str, str, str, str]] = set()
    for model_name, model in graph.models.items():
        for rel in model.relationships:
            if not rel.active:
                continue
            related_model = graph.models.get(rel.name)
            if related_model is None:
                continue

            if rel.type == "many_to_one":
                from_column = rel.foreign_key or rel.sql_expr
                to_column = rel.primary_key or related_model.primary_key
            elif rel.type in ("one_to_many", "one_to_one"):
                from_column = _relationship_tmdl_from_column(rel) or model.primary_key
                to_column = rel.foreign_key or rel.sql_expr
            elif rel.type == "many_to_many":
                from_column = rel.foreign_key or rel.sql_expr
                to_column = rel.primary_key or related_model.primary_key
            else:
                continue

            if not from_column or not to_column or not isinstance(from_column, str) or not isinstance(to_column, str):
                continue

            key = (model_name.lower(), from_column.lower(), related_model.name.lower(), to_column.lower())
            reverse = (related_model.name.lower(), to_column.lower(), model_name.lower(), from_column.lower())
            if key in seen_edges or reverse in seen_edges:
                continue

            seen_edges.add(key)
            edges.append(
                RelationshipEdge(
                    from_table=model_name,
                    from_column=from_column,
                    to_table=related_model.name,
                    to_column=to_column,
                )
            )

    return {
        "column_sql_by_table": column_sql_by_table,
        "measure_names_by_table": measure_names_by_table,
        "measure_aggs_by_table": measure_aggs_by_table,
        "measure_sql_by_table": measure_sql_by_table,
        "measure_filters_by_table": measure_filters_by_table,
        "time_dimensions_by_table": time_dimensions_by_table,
        "relationship_edges": edges,
    }


def _relationship_tmdl_from_column(rel: Any) -> str | None:
    value = getattr(rel, "_tmdl_from_column", None)
    if isinstance(value, str) and value.strip():
        return value
    return None


def _is_unlowered_dax_metric(metric: Metric) -> bool:
    return metric.expression_language == "dax" and not bool(getattr(metric, "_dax_lowered", False))
