"""Structured semantic graph metadata for UI/FFI consumers."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any

from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph


def describe_graph(graph: SemanticGraph, model_names: list[str] | None = None) -> dict[str, Any]:
    warnings = _warnings(graph)
    requested = set(model_names or [])
    models = [
        _describe_model(model, warnings) for model in graph.models.values() if not requested or model.name in requested
    ]
    metrics = [
        _describe_metric(metric, warnings, model_name=None)
        for metric in graph.metrics.values()
        if _include_graph_metric(metric, requested)
    ]

    return {
        "models": models,
        "metrics": metrics,
        "import_warnings": warnings,
    }


def _include_graph_metric(metric: Metric, requested_models: set[str]) -> bool:
    if not requested_models:
        return True
    owner_model = _metric_owner_model(metric)
    if owner_model and owner_model in requested_models:
        return True
    required_models = getattr(metric, "required_models", None) or []
    if not required_models:
        return True
    return set(required_models).issubset(requested_models)


def _metric_owner_model(metric: Metric) -> str | None:
    base_metric = getattr(metric, "base_metric", None)
    if isinstance(base_metric, str) and "." in base_metric:
        model_name, _metric_name = base_metric.split(".", 1)
        return model_name
    return None


def _describe_model(model: Model, warnings: list[dict[str, Any]]) -> dict[str, Any]:
    model_kind = _model_kind(model)
    info: dict[str, Any] = {
        "name": model.name,
        "kind": model_kind,
        "table": model.table,
        "sql": model.sql,
        "primary_key": model.primary_key,
        "dimensions": [_describe_dimension(dimension, warnings, model) for dimension in model.dimensions],
        "metrics": [_describe_metric(metric, warnings, model.name, model=model) for metric in model.metrics],
        "relationships": [
            _describe_relationship(relationship, warnings, model=model) for relationship in model.relationships
        ],
        "segments": [segment.name for segment in model.segments],
    }
    if model_kind == "calculated_table":
        info["calculated_table"] = True
    _add_common_fields(info, model, warnings, context="calculated_table")
    if model.description:
        info["description"] = model.description
    if model.default_time_dimension:
        info["default_time_dimension"] = model.default_time_dimension
    if model.default_grain:
        info["default_grain"] = model.default_grain
    if model.meta:
        info["meta"] = model.meta
    return _drop_none(info)


def _model_kind(model: Model) -> str:
    tmdl_node_type = str(getattr(model, "_tmdl_node_type", "")).lower()
    if tmdl_node_type == "calculatedtable":
        return "calculated_table"
    if getattr(model, "expression_language", None) == "dax" or _dax_expression_text(model):
        return "calculated_table"
    if model.sql and not model.table:
        return "derived_table"
    return "table"


def _describe_dimension(dimension: Any, warnings: list[dict[str, Any]], model: Model) -> dict[str, Any]:
    info: dict[str, Any] = {
        "name": dimension.name,
        "type": dimension.type,
        "sql": dimension.sql,
        "expression_language": getattr(dimension, "expression_language", None),
        "granularity": dimension.granularity,
        "supported_granularities": dimension.supported_granularities,
        "public": dimension.public,
    }
    _add_common_fields(info, dimension, warnings, context="column", model_name=model.name, inherited_from=model)
    if dimension.description:
        info["description"] = dimension.description
    if dimension.label:
        info["label"] = dimension.label
    if dimension.format:
        info["format"] = dimension.format
    if dimension.meta:
        info["meta"] = dimension.meta
    return _drop_none(info)


def _describe_metric(
    metric: Metric, warnings: list[dict[str, Any]], model_name: str | None, model: Model | None = None
) -> dict[str, Any]:
    info: dict[str, Any] = {
        "name": metric.name,
        "agg": metric.agg,
        "sql": metric.sql,
        "type": metric.type,
        "expression_language": getattr(metric, "expression_language", None),
        "base_metric": metric.base_metric,
        "comparison_type": metric.comparison_type,
        "calculation": metric.calculation,
        "time_offset": metric.time_offset,
        "window": metric.window,
        "grain_to_date": metric.grain_to_date,
        "window_order": metric.window_order,
        "filters": metric.filters or [],
        "drill_fields": metric.drill_fields or [],
        "required_models": metric.required_models,
        "relationship_overrides": [
            _relationship_override_info(override) for override in metric.relationship_overrides or []
        ],
        "public": metric.public,
    }
    _add_common_fields(info, metric, warnings, context="measure", model_name=model_name, inherited_from=model)
    if metric.description:
        info["description"] = metric.description
    if metric.label:
        info["label"] = metric.label
    if metric.format:
        info["format"] = metric.format
    if metric.meta:
        info["meta"] = metric.meta
    result = _drop_none(info)
    # Current Sidequery Swift DTOs decode these as non-optional arrays. Keep
    # them present even when empty while Sidequery moves to richer metadata.
    result.setdefault("filters", [])
    result.setdefault("drill_fields", [])
    return result


def _describe_relationship(
    relationship: Relationship, warnings: list[dict[str, Any]], model: Model | None = None
) -> dict[str, Any]:
    tmdl_name = getattr(relationship, "_tmdl_relationship_name", None)
    info: dict[str, Any] = {
        "name": relationship.name,
        "type": relationship.type,
        "foreign_key": relationship.foreign_key,
        "primary_key": relationship.primary_key,
        "through": relationship.through,
        "through_foreign_key": relationship.through_foreign_key,
        "related_foreign_key": relationship.related_foreign_key,
        "metadata": relationship.metadata,
    }
    if relationship.active is not True:
        info["active"] = relationship.active
    _add_common_fields(
        info,
        relationship,
        warnings,
        context="relationship",
        model_name=model.name if model else None,
        inherited_from=model,
        alternate_warning_names=[tmdl_name] if tmdl_name else None,
    )
    if tmdl_name:
        info["tmdl_name"] = tmdl_name
    return _drop_none(info)


def _add_common_fields(
    info: dict[str, Any],
    obj: Any,
    warnings: list[dict[str, Any]],
    *,
    context: str,
    model_name: str | None = None,
    inherited_from: Any | None = None,
    alternate_warning_names: list[str] | None = None,
) -> None:
    source_format = getattr(obj, "_source_format", None) or getattr(inherited_from, "_source_format", None)
    if source_format:
        info["source_format"] = source_format
    source_file = getattr(obj, "_source_file", None) or getattr(inherited_from, "_source_file", None)
    if source_file:
        info["source_file"] = source_file

    tmdl_expression = getattr(obj, "_tmdl_expression", None)
    dax_expression = _dax_expression_text(obj)
    if tmdl_expression:
        info["tmdl_expression"] = tmdl_expression
    if dax_expression:
        info["dax"] = dax_expression
    if tmdl_expression or dax_expression:
        info["original_expression"] = tmdl_expression or dax_expression

    lowered = bool(getattr(obj, "_dax_lowered", False))
    if lowered:
        info["dax_lowered"] = True
    if required_models := getattr(obj, "_dax_required_models", None):
        info["dax_required_models"] = required_models

    tmdl_metadata = _tmdl_metadata(obj)
    if tmdl_metadata:
        info["tmdl"] = tmdl_metadata

    _add_warning_fields(
        info,
        getattr(obj, "name", None),
        context,
        warnings,
        model_name=model_name,
        alternate_names=alternate_warning_names,
    )
    if "import_warnings" not in info and (tmdl_expression or dax_expression):
        info["faithful_lowering"] = True


def _tmdl_metadata(obj: Any) -> dict[str, Any]:
    fields = {
        "node_type": "_tmdl_node_type",
        "name_raw": "_tmdl_name_raw",
        "relationship_name": "_tmdl_relationship_name",
        "relationship_name_raw": "_tmdl_relationship_name_raw",
        "data_type": "_tmdl_data_type",
        "description": "_tmdl_description",
        "properties": "_tmdl_properties",
        "relationship_properties": "_tmdl_relationship_properties",
        "raw_value_properties": "_tmdl_raw_value_properties",
        "property_order": "_tmdl_property_order",
        "leading_comments": "_tmdl_leading_comments",
        "child_nodes": "_tmdl_child_nodes",
    }
    metadata: dict[str, Any] = {}
    for output_key, attr in fields.items():
        value = getattr(obj, attr, None)
        if value is None or value == [] or value == {}:
            continue
        metadata[output_key] = _json_safe(value)

    if getattr(obj, "_tmdl_is_active_explicit", False):
        metadata["is_active_explicit"] = True

    return metadata


def _add_warning_fields(
    info: dict[str, Any],
    name: str | None,
    context: str,
    warnings: list[dict[str, Any]],
    *,
    model_name: str | None = None,
    alternate_names: list[str] | None = None,
) -> None:
    matched = [
        warning
        for warning in warnings
        if warning.get("context") == context and _warning_matches(warning, name, model_name, alternate_names)
    ]
    if not matched:
        return
    info["import_warnings"] = matched
    info["unsupported"] = any(
        warning.get("code")
        in {"dax_parse_error", "dax_parser_unavailable", "dax_translation_fallback", "relationship_parse_skip"}
        for warning in matched
    )
    if context in {"column", "measure", "calculated_table", "relationship"}:
        info["faithful_lowering"] = not info["unsupported"]


def _dax_expression_text(obj: Any) -> str | None:
    for attr in ("_dax_expression", "dax"):
        value = getattr(obj, attr, None)
        if isinstance(value, str) and value.strip():
            return value
    return None


def _warning_matches(
    warning: dict[str, Any],
    name: str | None,
    model_name: str | None,
    alternate_names: list[str] | None = None,
) -> bool:
    warning_model = warning.get("model")
    if model_name and warning_model and warning_model != model_name:
        return False

    if name is None:
        return True

    warning_name = warning.get("name")
    if warning_name == name:
        return True
    for alternate_name in alternate_names or []:
        if alternate_name and warning_name == alternate_name:
            return True
    if model_name and warning_name == f"{model_name}.{name}":
        return True
    if warning_model and warning_name == f"{warning_model}.{name}":
        return True
    return False


def _warnings(graph: SemanticGraph) -> list[dict[str, Any]]:
    warnings = getattr(graph, "import_warnings", []) or []
    return [dict(warning) for warning in warnings if isinstance(warning, dict)]


def _json_safe(value: Any) -> Any:
    if is_dataclass(value):
        return _json_safe(asdict(value))
    if hasattr(value, "model_dump"):
        return _json_safe(value.model_dump(exclude_none=True))
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items() if item is not None}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _drop_none(value: dict[str, Any]) -> dict[str, Any]:
    return {key: item for key, item in value.items() if item is not None and item != []}


def _relationship_override_info(override: Any) -> dict[str, Any]:
    return _json_safe(override)
