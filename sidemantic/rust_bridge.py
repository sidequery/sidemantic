"""Helpers for calling sidemantic-rs from Python."""

from __future__ import annotations

import json
import re
from pathlib import Path

import yaml

from sidemantic.core.semantic_graph import SemanticGraph


def get_rust_module() -> object:
    """Import and return sidemantic_rs extension module."""
    try:
        import sidemantic_rs
    except ImportError as e:
        raise ValueError(
            "Rust backend requires the sidemantic_rs Python extension. "
            "Build it with: uv run --with maturin maturin develop "
            "--manifest-path sidemantic-rs/Cargo.toml --features python-adbc"
        ) from e
    return sidemantic_rs


def graph_to_rust_yaml(graph: SemanticGraph) -> str:
    """Serialize semantic graph to sidemantic-rs YAML schema."""
    extra_metrics_by_model, remaining_top_level_metrics = _assign_top_level_metrics_for_rust(graph)
    return models_to_rust_yaml(
        list(graph.models.values()),
        extra_metrics_by_model=extra_metrics_by_model,
        top_level_metrics=remaining_top_level_metrics,
        top_level_parameters=list(graph.parameters.values()),
    )


def _assign_top_level_metrics_for_rust(graph: SemanticGraph) -> tuple[dict[str, list], list]:
    """Assign Python graph-level metrics to model payloads for sidemantic-rs.

    Python can keep derived/ratio metrics at graph scope even when dependencies span
    models. The Rust YAML loader requires a single owner before it builds the graph,
    so choose one deterministically in the bridge and leave only unresolvable metrics
    at top level.
    """
    top_level_metrics = list(graph.metrics.values())
    top_level_by_name = {metric.name: metric for metric in top_level_metrics}
    cache: dict[str, str | None] = {}

    def model_metric_owners(metric_name: str) -> set[str]:
        return {
            model.name
            for model in graph.models.values()
            if any(model_metric.name == metric_name for model_metric in model.metrics)
        }

    def owner_from_dotted_reference(reference: str) -> str | None:
        if "." not in reference:
            return None
        model_name = reference.split(".", 1)[0]
        return model_name if model_name in graph.models else None

    def owners_from_sql_fragment(fragment: str) -> list[str]:
        owners = []
        for model_name, _field_name in re.findall(r"\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b", fragment):
            if model_name in graph.models and model_name not in owners:
                owners.append(model_name)
        return owners

    def preferred_ratio_owner(metric) -> str | None:
        if isinstance(metric.denominator, str):
            owner = owner_from_dotted_reference(metric.denominator)
            if owner:
                return owner
        if isinstance(metric.sql, str) and "/" in metric.sql:
            denominator_sql = metric.sql.split("/", 1)[1]
            owners = owners_from_sql_fragment(denominator_sql)
            if owners:
                return owners[0]
        return None

    def resolve_owner(metric) -> str | None:
        cached = cache.get(metric.name)
        if metric.name in cache:
            return cached

        cache[metric.name] = None
        owners = model_metric_owners(metric.name)

        try:
            dependencies = metric.get_dependencies(graph)
        except Exception:
            dependencies = set()

        for dep in sorted(dependencies):
            dotted_owner = owner_from_dotted_reference(dep)
            if dotted_owner:
                owners.add(dotted_owner)
                continue
            if dep in top_level_by_name:
                dep_owner = resolve_owner(top_level_by_name[dep])
                if dep_owner:
                    owners.add(dep_owner)
                continue
            owners.update(model_metric_owners(dep))

        for reference in (metric.sql, metric.base_metric, metric.numerator, metric.denominator):
            if not isinstance(reference, str):
                continue
            dotted_owner = owner_from_dotted_reference(reference)
            if dotted_owner:
                owners.add(dotted_owner)
            owners.update(owners_from_sql_fragment(reference))

        if not owners and len(graph.models) == 1:
            owners.add(next(iter(graph.models)))

        preferred_owner = preferred_ratio_owner(metric)
        owner = preferred_owner if preferred_owner in owners else (sorted(owners)[0] if owners else None)
        cache[metric.name] = owner
        return owner

    assigned: dict[str, list] = {}
    remaining = []
    for metric in top_level_metrics:
        owner = resolve_owner(metric)
        if owner:
            assigned.setdefault(owner, []).append(metric)
        else:
            remaining.append(metric)

    return assigned, remaining


def _normalize_metric_type(metric_payload: dict, *, empty_filters_to_none: bool = False) -> dict:
    normalized = dict(metric_payload)
    metric_type = normalized.get("type")
    if metric_type == "simple":
        normalized["type"] = None
    elif metric_type == "timecomparison":
        normalized["type"] = "time_comparison"
    if empty_filters_to_none and normalized.get("filters") == []:
        normalized["filters"] = None
    return normalized


def load_graph_from_yaml_with_rust(yaml_content: str) -> SemanticGraph:
    """Parse native YAML definitions via sidemantic-rs and build a Python SemanticGraph."""
    rust_module = get_rust_module()
    payload = json.loads(rust_module.load_graph_with_yaml(yaml_content))
    return _graph_from_loaded_payload(payload)


def load_graph_from_sql_with_rust(sql_content: str) -> SemanticGraph:
    """Parse SQL file content definitions via sidemantic-rs and build a Python SemanticGraph."""
    rust_module = get_rust_module()
    payload = json.loads(rust_module.load_graph_with_sql(sql_content))
    return _graph_from_loaded_payload(payload)


def load_graph_from_directory_with_rust(directory: str | Path) -> SemanticGraph:
    """Parse supported directory definitions via sidemantic-rs and build a Python SemanticGraph."""
    rust_module = get_rust_module()
    if not hasattr(rust_module, "load_graph_from_directory"):
        raise ValueError(
            "Rust backend requires a sidemantic_rs build with load_graph_from_directory. "
            "Rebuild it with: uv run --with maturin maturin develop "
            "--manifest-path sidemantic-rs/Cargo.toml --features python-adbc"
        )
    payload = json.loads(rust_module.load_graph_from_directory(str(directory)))
    return _graph_from_loaded_payload(payload)


def _graph_from_loaded_payload(payload: dict) -> SemanticGraph:
    """Build a Python graph from sidemantic-rs loaded graph payload JSON."""

    from sidemantic.core.metric import Metric
    from sidemantic.core.model import Model
    from sidemantic.core.parameter import Parameter

    graph = SemanticGraph()
    top_level_metric_names = {metric["name"] for metric in payload.get("top_level_metrics") or []}
    original_model_metrics = payload.get("original_model_metrics") or {}
    model_sources = payload.get("model_sources") or {}

    for model_data in payload.get("models") or []:
        normalized_model = dict(model_data)
        original_metric_names = set(original_model_metrics.get(normalized_model.get("name"), []))
        normalized_metrics = []
        for metric_data in normalized_model.get("metrics") or []:
            normalized_metric = _normalize_metric_type(metric_data)
            metric_name = normalized_metric.get("name")
            if metric_name in top_level_metric_names and metric_name not in original_metric_names:
                continue
            normalized_metrics.append(normalized_metric)
        normalized_model["metrics"] = normalized_metrics
        model = Model(**normalized_model)
        source_metadata = model_sources.get(model.name) or {}
        source_format = source_metadata.get("source_format")
        source_file = source_metadata.get("source_file")
        if source_format and not hasattr(model, "_source_format"):
            model._source_format = source_format
        if source_file and not hasattr(model, "_source_file"):
            model._source_file = source_file
        graph.add_model(model)

    for metric_data in payload.get("top_level_metrics") or []:
        metric = Metric(**_normalize_metric_type(metric_data))
        graph.add_metric(metric)

    for parameter_data in payload.get("parameters") or []:
        parameter = Parameter(**parameter_data)
        graph.add_parameter(parameter)

    return graph


def parse_sql_definitions_with_rust(sql: str) -> tuple[list, list]:
    """Parse SQL metric/segment definitions via sidemantic-rs."""
    rust_module = get_rust_module()
    payload = json.loads(rust_module.parse_sql_definitions_payload(sql))

    from sidemantic.core.metric import Metric
    from sidemantic.core.segment import Segment

    metrics = [
        Metric(**_normalize_metric_type(metric_payload, empty_filters_to_none=True))
        for metric_payload in payload.get("metrics") or []
    ]
    segments = [Segment(**segment_payload) for segment_payload in payload.get("segments") or []]
    return metrics, segments


def parse_sql_graph_definitions_with_rust(sql: str) -> tuple[list, list, list]:
    """Parse SQL graph definitions (metrics, segments, parameters) via sidemantic-rs."""
    rust_module = get_rust_module()
    payload = json.loads(rust_module.parse_sql_graph_definitions_payload(sql))

    from sidemantic.core.metric import Metric
    from sidemantic.core.parameter import Parameter
    from sidemantic.core.segment import Segment

    metrics = [
        Metric(**_normalize_metric_type(metric_payload, empty_filters_to_none=True))
        for metric_payload in payload.get("metrics") or []
    ]
    segments = [Segment(**segment_payload) for segment_payload in payload.get("segments") or []]
    parameters = [Parameter(**parameter_payload) for parameter_payload in payload.get("parameters") or []]
    return metrics, segments, parameters


def parse_sql_graph_definitions_extended_with_rust(sql: str) -> tuple[list, list, list, list]:
    """Parse SQL graph definitions including pre-aggregations via sidemantic-rs."""
    rust_module = get_rust_module()
    payload = json.loads(rust_module.parse_sql_graph_definitions_payload(sql))

    from sidemantic.core.metric import Metric
    from sidemantic.core.parameter import Parameter
    from sidemantic.core.pre_aggregation import PreAggregation
    from sidemantic.core.segment import Segment

    metrics = [
        Metric(**_normalize_metric_type(metric_payload, empty_filters_to_none=True))
        for metric_payload in payload.get("metrics") or []
    ]
    segments = [Segment(**segment_payload) for segment_payload in payload.get("segments") or []]
    parameters = [Parameter(**parameter_payload) for parameter_payload in payload.get("parameters") or []]
    pre_aggregations = [PreAggregation(**preagg_payload) for preagg_payload in payload.get("pre_aggregations") or []]
    return metrics, segments, parameters, pre_aggregations


def parse_sql_model_with_rust(sql: str):
    """Parse SQL MODEL/DIMENSION/METRIC/SEGMENT/RELATIONSHIP definitions via sidemantic-rs."""
    rust_module = get_rust_module()
    payload = json.loads(rust_module.parse_sql_model_payload(sql))

    from sidemantic.core.model import Model

    normalized_model = dict(payload)
    normalized_model["metrics"] = [
        _normalize_metric_type(metric_payload, empty_filters_to_none=True)
        for metric_payload in normalized_model.get("metrics") or []
    ]
    return Model(**normalized_model)


def parse_sql_statement_blocks_with_rust(sql: str) -> list[dict]:
    """Parse raw SQL statement blocks via sidemantic-rs."""
    rust_module = get_rust_module()
    payload = json.loads(rust_module.parse_sql_statement_blocks_payload(sql))
    if isinstance(payload, list):
        return payload
    return []


def models_to_rust_yaml(
    models: list,
    *,
    extra_metrics_by_model: dict[str, list] | None = None,
    top_level_metrics: list | None = None,
    top_level_parameters: list | None = None,
    include_extends: bool = False,
) -> str:
    """Serialize model list to sidemantic-rs YAML schema."""
    serialized_models = []
    extra_metrics_by_model = extra_metrics_by_model or {}
    top_level_metrics = top_level_metrics or []
    top_level_parameters = top_level_parameters or []
    models_by_name = {m.name: m for m in models}

    for model in models:
        primary_key_columns = model.primary_key if isinstance(model.primary_key, list) else [model.primary_key]
        model_data = {
            "name": model.name,
            "extends": model.extends if include_extends else None,
            "table": model.table or (model.name if not model.sql else None),
            "sql": model.sql,
            "source_uri": model.source_uri,
            "primary_key": primary_key_columns[0] if primary_key_columns else "id",
            "primary_key_columns": primary_key_columns,
            "unique_keys": model.unique_keys,
            "description": model.description,
            "label": None,
            "default_time_dimension": model.default_time_dimension,
            "default_grain": model.default_grain,
            "dimensions": [],
            "metrics": [],
            "relationships": [],
            "segments": [],
            "pre_aggregations": [],
        }

        for dimension in model.dimensions:
            model_data["dimensions"].append(
                {
                    "name": dimension.name,
                    "type": dimension.type,
                    "sql": dimension.sql,
                    "granularity": dimension.granularity,
                    "supported_granularities": dimension.supported_granularities,
                    "description": dimension.description,
                    "label": dimension.label,
                    "format": dimension.format,
                    "value_format_name": dimension.value_format_name,
                    "parent": dimension.parent,
                    "window": dimension.window,
                }
            )

        serialized_metric_names = set()
        for metric in [*model.metrics, *extra_metrics_by_model.get(model.name, [])]:
            if metric.name in serialized_metric_names:
                continue
            serialized_metric_names.add(metric.name)
            model_data["metrics"].append(_serialize_metric(metric, primary_key_columns=primary_key_columns))

        for relationship in model.relationships:
            rel_payload = _serialize_relationship(
                relationship,
                source_model=model,
                target_model=models_by_name.get(relationship.name),
            )
            if rel_payload:
                model_data["relationships"].append(rel_payload)

        for segment in model.segments:
            model_data["segments"].append(
                {
                    "name": segment.name,
                    "sql": segment.sql,
                    "description": segment.description,
                    "public": segment.public,
                }
            )

        for pre_aggregation in model.pre_aggregations:
            model_data["pre_aggregations"].append(_serialize_pre_aggregation(pre_aggregation))

        serialized_models.append(model_data)

    payload = {"models": serialized_models}
    if top_level_metrics:
        payload["metrics"] = [_serialize_metric(metric, primary_key_columns=None) for metric in top_level_metrics]
    if top_level_parameters:
        payload["parameters"] = [_serialize_parameter(parameter) for parameter in top_level_parameters]

    return yaml.safe_dump(payload, sort_keys=False)


def validate_query_with_rust(graph: SemanticGraph, metrics: list[str], dimensions: list[str]) -> list[str]:
    """Validate query references and join reachability via sidemantic-rs."""
    rust_module = get_rust_module()
    graph_yaml = graph_to_rust_yaml(graph)
    metric_refs = list(metrics or [])
    dimension_refs = list(dimensions or [])

    def _validate_with_legacy_payload() -> list[str]:
        payload = {
            "metrics": metric_refs,
            "dimensions": dimension_refs,
        }
        return rust_module.validate_query_with_yaml(
            graph_yaml,
            yaml.safe_dump(payload, sort_keys=False),
        )

    if hasattr(rust_module, "validate_query_references"):
        try:
            errors = rust_module.validate_query_references(
                graph_yaml,
                metric_refs,
                dimension_refs,
            )
        except TypeError:
            # Compatibility fallback for older sidemantic-rs extension builds.
            errors = _validate_with_legacy_payload()
    else:
        # Compatibility fallback for older sidemantic-rs extension builds.
        errors = _validate_with_legacy_payload()
    return [str(error) for error in errors]


def validate_models_payload_with_rust(
    models: list,
    *,
    top_level_metrics: list | None = None,
    top_level_parameters: list | None = None,
    include_extends: bool = True,
) -> bool:
    """Validate model payload set via sidemantic-rs loader/graph semantics."""
    rust_module = get_rust_module()
    models_yaml = models_to_rust_yaml(
        models,
        top_level_metrics=top_level_metrics or [],
        top_level_parameters=top_level_parameters or [],
        include_extends=include_extends,
    )
    return bool(rust_module.validate_models_yaml(models_yaml))


def validate_model_payload_with_rust(model_obj) -> bool:
    """Validate model payload shape via sidemantic-rs."""
    rust_module = get_rust_module()
    payload = yaml.safe_load(models_to_rust_yaml([model_obj], include_extends=False)) or {}
    model_payload = (payload.get("models") or [{}])[0]
    model_yaml = yaml.safe_dump(model_payload, sort_keys=False)
    return bool(rust_module.validate_model_payload(model_yaml))


def validate_metric_payload_with_rust(metric_obj) -> bool:
    """Validate metric payload shape via sidemantic-rs."""
    rust_module = get_rust_module()
    metric_yaml = yaml.safe_dump(metric_obj.model_dump(exclude_none=True), sort_keys=False)
    return bool(rust_module.validate_metric_payload(metric_yaml))


def validate_parameter_payload_with_rust(parameter_obj) -> bool:
    """Validate parameter payload shape via sidemantic-rs."""
    rust_module = get_rust_module()
    parameter_yaml = yaml.safe_dump(_serialize_parameter(parameter_obj), sort_keys=False)
    return bool(rust_module.validate_parameter_payload(parameter_yaml))


def validate_table_calculation_payload_with_rust(calculation_obj) -> bool:
    """Validate table-calculation payload shape via sidemantic-rs."""
    rust_module = get_rust_module()
    calculation_yaml = yaml.safe_dump(calculation_obj.model_dump(exclude_none=True), sort_keys=False)
    return bool(rust_module.validate_table_calculation_payload(calculation_yaml))


def resolve_model_inheritance_with_rust(models: dict[str, object]) -> dict[str, object]:
    """Resolve model inheritance via sidemantic-rs."""
    rust_module = get_rust_module()
    from sidemantic.core.model import Model

    passthrough_fields = ("source_uri", "unique_keys", "meta", "extends")
    passthrough_cache: dict[str, dict] = {}

    def inherited_passthrough(model_name: str) -> dict:
        cached = passthrough_cache.get(model_name)
        if cached is not None:
            return cached

        model = models.get(model_name)
        if model is None:
            values = {field: None for field in passthrough_fields}
            passthrough_cache[model_name] = values
            return values

        parent_name = getattr(model, "extends", None)
        if parent_name and parent_name in models:
            values = inherited_passthrough(parent_name).copy()
        else:
            values = {field: getattr(model, field, None) for field in passthrough_fields}
        passthrough_cache[model_name] = values
        return values

    models_yaml = models_to_rust_yaml(list(models.values()), include_extends=True)
    has_extends = any(getattr(model, "extends", None) for model in models.values())
    if not has_extends:
        # Keep exact Python model payloads for non-inheritance cases.
        # This avoids lossy schema conversion while still exercising the Rust path.
        rust_module.resolve_model_inheritance(models_yaml)
        return dict(models)

    resolved_yaml = rust_module.resolve_model_inheritance(models_yaml)
    resolved_payload = yaml.safe_load(resolved_yaml) or []
    resolved_models = {}
    for model_data in resolved_payload:
        normalized_data = dict(model_data)
        normalized_metrics = []
        for metric_data in normalized_data.get("metrics") or []:
            metric_payload = dict(metric_data)
            metric_type = metric_payload.get("type")
            if metric_type == "simple":
                metric_payload["type"] = None
            elif metric_type == "timecomparison":
                metric_payload["type"] = "time_comparison"
            normalized_metrics.append(metric_payload)
        normalized_data["metrics"] = normalized_metrics

        model_name = normalized_data.get("name")
        if isinstance(model_name, str):
            for field, value in inherited_passthrough(model_name).items():
                normalized_data.setdefault(field, value)

        model = Model(**normalized_data)
        resolved_models[model.name] = model
    return resolved_models


def resolve_metric_inheritance_with_rust(metrics: dict[str, object]) -> dict[str, object]:
    """Resolve metric inheritance via sidemantic-rs."""
    rust_module = get_rust_module()
    from sidemantic.core.metric import Metric

    has_extends = any(getattr(metric, "extends", None) for metric in metrics.values())
    serialized_metrics = []
    for metric in metrics.values():
        child_fields = metric.model_fields_set - {"extends"}
        metric_data = metric.model_dump(include=child_fields)
        metric_data["name"] = metric.name
        if metric.extends is not None:
            metric_data["extends"] = metric.extends
        serialized_metrics.append(metric_data)

    metrics_yaml = yaml.safe_dump(serialized_metrics, sort_keys=False)
    if not has_extends:
        # Keep exact Python metric payloads for non-inheritance cases.
        # This avoids lossy schema conversion while still exercising the Rust path.
        rust_module.resolve_metric_inheritance(metrics_yaml)
        return dict(metrics)

    resolved_yaml = rust_module.resolve_metric_inheritance(metrics_yaml)
    resolved_payload = yaml.safe_load(resolved_yaml) or []
    resolved_metrics = {}
    for metric_data in resolved_payload:
        normalized_data = dict(metric_data)
        metric_type = normalized_data.get("type")
        if metric_type == "simple":
            normalized_data["type"] = None
        elif metric_type == "timecomparison":
            normalized_data["type"] = "time_comparison"

        metric = Metric(**normalized_data)
        resolved_metrics[metric.name] = metric
    return resolved_metrics


def parse_reference_with_rust(graph: SemanticGraph, reference: str) -> tuple[str, str, str | None]:
    """Parse a qualified semantic reference via sidemantic-rs."""
    rust_module = get_rust_module()
    parsed = rust_module.parse_reference_with_yaml(
        graph_to_rust_yaml(graph),
        reference,
    )
    return _normalize_parsed_reference(parsed)


def find_relationship_path_with_rust(graph: SemanticGraph, from_model: str, to_model: str) -> list:
    """Find join path between models via sidemantic-rs."""
    rust_module = get_rust_module()
    rust_steps = rust_module.find_relationship_path_with_yaml(
        graph_to_rust_yaml(graph),
        from_model,
        to_model,
    )
    rust_steps = _deserialize_json_payload(rust_steps)

    from sidemantic.core.semantic_graph import JoinPath

    path = []
    for step in rust_steps:
        from_name, to_name, from_columns, to_columns, relationship = _normalize_relationship_path_step(step)
        path.append(
            JoinPath(
                from_model=str(from_name),
                to_model=str(to_name),
                from_columns=[str(column) for column in from_columns],
                to_columns=[str(column) for column in to_columns],
                relationship=str(relationship),
            )
        )
    return path


def _normalize_parsed_reference(parsed: object) -> tuple[str, str, str | None]:
    parsed = _deserialize_json_payload(parsed)
    if isinstance(parsed, dict):
        model_name = parsed.get("model_name", parsed.get("model"))
        field_name = parsed.get("field_name", parsed.get("field"))
        granularity = parsed.get("granularity")
    else:
        try:
            model_name, field_name, granularity = parsed
        except (TypeError, ValueError) as exc:
            raise TypeError("unexpected parse_reference_with_yaml result shape") from exc

    if model_name is None or field_name is None:
        raise TypeError("unexpected parse_reference_with_yaml result payload")

    return str(model_name), str(field_name), (str(granularity) if granularity is not None else None)


def _normalize_relationship_path_step(step: object) -> tuple[str, str, list[str], list[str], str]:
    if isinstance(step, dict):
        from_name = step.get("from_model")
        to_name = step.get("to_model")
        relationship = step.get("relationship")
        from_columns = step.get("from_columns")
        to_columns = step.get("to_columns")

        # Older payloads may expose only single-column aliases.
        if from_columns is None:
            from_entity = step.get("from_entity")
            from_columns = [from_entity] if from_entity is not None else None
        if to_columns is None:
            to_entity = step.get("to_entity")
            to_columns = [to_entity] if to_entity is not None else None
    else:
        try:
            from_name, to_name, from_columns, to_columns, relationship = step
        except (TypeError, ValueError) as exc:
            raise TypeError("unexpected find_relationship_path_with_yaml step shape") from exc

    if from_name is None or to_name is None or relationship is None:
        raise TypeError("unexpected find_relationship_path_with_yaml step payload")

    normalized_from_columns = _normalize_relationship_columns(from_columns)
    normalized_to_columns = _normalize_relationship_columns(to_columns)
    return str(from_name), str(to_name), normalized_from_columns, normalized_to_columns, str(relationship)


def _normalize_relationship_columns(columns: object) -> list[str]:
    columns = _deserialize_json_payload(columns)
    if columns is None:
        return []
    if isinstance(columns, str):
        return [columns]
    try:
        return [str(column) for column in columns]
    except TypeError as exc:
        raise TypeError("unexpected relationship column payload") from exc


def _deserialize_json_payload(payload: object) -> object:
    if not isinstance(payload, str):
        return payload

    text = payload.strip()
    if not text or text[0] not in "[{":
        return payload

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return payload


def _base_dimension_reference(reference: str) -> str:
    if "__" not in reference:
        return reference
    base_ref, granularity = reference.rsplit("__", 1)
    if granularity in {"hour", "day", "week", "month", "quarter", "year"}:
        return base_ref
    return reference


def _infer_models_for_unqualified_references(
    graph: SemanticGraph,
    dimensions: list[str],
    measures: list[str],
) -> set[str]:
    inferred_models: set[str] = set()

    for dimension_ref in dimensions:
        base_ref = _base_dimension_reference(dimension_ref)
        if "." in base_ref:
            continue
        for model in graph.models.values():
            if any(dimension.name == base_ref for dimension in model.dimensions):
                inferred_models.add(model.name)

    for metric_ref in measures:
        if "." in metric_ref:
            continue

        metric = graph.metrics.get(metric_ref)
        sql_ref = getattr(metric, "sql", None) if metric is not None else None
        if isinstance(sql_ref, str) and "." in sql_ref:
            model_name = sql_ref.split(".", 1)[0].strip()
            if model_name and model_name in graph.models:
                inferred_models.add(model_name)

        for model in graph.models.values():
            if any(model_metric.name == metric_ref for model_metric in model.metrics):
                inferred_models.add(model.name)

    return inferred_models


def find_models_for_query_with_rust(
    graph: SemanticGraph,
    dimensions: list[str],
    measures: list[str],
) -> set[str]:
    """Discover model names referenced by dimensions/measures via sidemantic-rs."""
    rust_module = get_rust_module()
    dimension_refs = list(dimensions or [])
    measure_refs = list(measures or [])

    if hasattr(rust_module, "find_models_for_query_with_yaml"):
        try:
            models = rust_module.find_models_for_query_with_yaml(
                graph_to_rust_yaml(graph),
                dimension_refs,
                measure_refs,
            )
        except TypeError:
            # Compatibility fallback for older sidemantic-rs extension builds.
            models = rust_module.find_models_for_query(dimension_refs, measure_refs)
    else:
        # Compatibility fallback for older sidemantic-rs extension builds.
        models = rust_module.find_models_for_query(dimension_refs, measure_refs)

    resolved_models = {str(model_name) for model_name in models}
    resolved_models.update(_infer_models_for_unqualified_references(graph, dimension_refs, measure_refs))
    return resolved_models


def _as_list(value: str | list[str] | None) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _serialize_metric(metric, *, primary_key_columns: list[str] | None) -> dict:
    metric_sql = metric.sql
    if metric.agg == "count_distinct" and not metric_sql and primary_key_columns:
        if len(primary_key_columns) == 1:
            metric_sql = primary_key_columns[0]
        else:
            casts = ", '|', ".join(f"CAST({col} AS VARCHAR)" for col in primary_key_columns)
            metric_sql = f"CONCAT({casts})"

    return {
        "name": metric.name,
        "extends": metric.extends,
        "type": metric.type,
        "agg": metric.agg,
        "sql": metric_sql,
        "numerator": metric.numerator,
        "denominator": metric.denominator,
        "offset_window": metric.offset_window,
        "window": metric.window,
        "grain_to_date": metric.grain_to_date,
        "window_expression": metric.window_expression,
        "window_frame": metric.window_frame,
        "window_order": metric.window_order,
        "base_metric": metric.base_metric,
        "comparison_type": metric.comparison_type,
        "time_offset": metric.time_offset,
        "calculation": metric.calculation,
        "entity": metric.entity,
        "base_event": metric.base_event,
        "conversion_event": metric.conversion_event,
        "conversion_window": metric.conversion_window,
        "fill_nulls_with": metric.fill_nulls_with,
        "format": metric.format,
        "value_format_name": metric.value_format_name,
        "drill_fields": metric.drill_fields,
        "non_additive_dimension": metric.non_additive_dimension,
        "filters": metric.filters or [],
        "description": metric.description,
        "label": metric.label,
    }


def _serialize_parameter(parameter) -> dict:
    return {
        "name": parameter.name,
        "type": parameter.type,
        "description": parameter.description,
        "label": parameter.label,
        "default_value": parameter.default_value,
        "allowed_values": parameter.allowed_values,
        "default_to_today": parameter.default_to_today,
    }


def is_sql_template_with_rust(sql: str) -> bool:
    """Check template marker presence via sidemantic-rs."""
    rust_module = get_rust_module()
    return bool(rust_module.is_sql_template(sql))


def render_sql_template_with_rust(template_str: str, context: dict) -> str:
    """Render a SQL template via sidemantic-rs."""
    rust_module = get_rust_module()
    context_yaml = yaml.safe_dump(context or {}, sort_keys=False)
    return rust_module.render_sql_template(template_str, context_yaml)


def format_parameter_value_with_rust(parameter, value) -> str:
    """Format a parameter value via sidemantic-rs."""
    rust_module = get_rust_module()
    parameter_yaml = yaml.safe_dump(_serialize_parameter(parameter), sort_keys=False)
    value_yaml = yaml.safe_dump(value, sort_keys=False)
    return rust_module.format_parameter_value_with_yaml(parameter_yaml, value_yaml)


def interpolate_sql_with_parameters_with_rust(
    sql: str,
    parameters: dict,
    values: dict | None = None,
) -> str:
    """Interpolate SQL placeholders/templates via sidemantic-rs."""
    rust_module = get_rust_module()
    payload = [_serialize_parameter(parameter) for parameter in parameters.values()]
    parameters_yaml = yaml.safe_dump(payload, sort_keys=False)
    values_yaml = yaml.safe_dump(values or {}, sort_keys=False)
    return rust_module.interpolate_sql_with_parameters(sql, parameters_yaml, values_yaml)


def evaluate_table_calculation_expression_with_rust(expr: str) -> float:
    """Evaluate a table-calculation arithmetic expression via sidemantic-rs."""
    rust_module = get_rust_module()
    return float(rust_module.evaluate_table_calculation_expression(expr))


def chart_auto_detect_columns_with_rust(columns: list[str], numeric_flags: list[bool]) -> tuple[str, list[str]]:
    """Auto-detect chart x/y columns via sidemantic-rs."""
    rust_module = get_rust_module()
    x_col, y_cols = rust_module.chart_auto_detect_columns(list(columns), list(numeric_flags))
    return str(x_col), [str(col) for col in y_cols]


def chart_select_type_with_rust(x: str, x_value_kind: str, y_count: int) -> str:
    """Select chart type via sidemantic-rs."""
    rust_module = get_rust_module()
    return str(rust_module.chart_select_type(x, x_value_kind, int(y_count)))


def chart_format_label_with_rust(column: str) -> str:
    """Format chart label via sidemantic-rs."""
    rust_module = get_rust_module()
    return str(rust_module.chart_format_label(column))


def chart_encoding_type_with_rust(column: str) -> str:
    """Determine chart encoding type via sidemantic-rs."""
    rust_module = get_rust_module()
    return str(rust_module.chart_encoding_type(column))


def extract_column_references_with_rust(sql_expr: str) -> set[str]:
    """Extract column references from a SQL expression via sidemantic-rs."""
    rust_module = get_rust_module()
    refs = rust_module.extract_column_references(sql_expr)
    return {str(ref) for ref in refs}


def analyze_migrator_query_with_rust(sql_query: str) -> dict:
    """Analyze migrator query extraction payload via sidemantic-rs."""
    rust_module = get_rust_module()
    payload = rust_module.analyze_migrator_query(sql_query)
    if isinstance(payload, str):
        return json.loads(payload)
    return dict(payload)


def extract_metric_dependencies_with_rust(metric_obj, graph=None, model_context: str | None = None) -> set[str]:
    """Extract metric dependencies via sidemantic-rs."""
    rust_module = get_rust_module()
    metric_yaml = yaml.safe_dump(metric_obj.model_dump(exclude_none=True), sort_keys=False)
    models_yaml = graph_to_rust_yaml(graph) if graph is not None else None
    refs = rust_module.extract_metric_dependencies(metric_yaml, models_yaml, model_context)
    return {str(ref) for ref in refs}


def parse_simple_metric_aggregation_with_rust(sql_expr: str) -> tuple[str, str | None] | None:
    """Parse a top-level simple metric aggregation via sidemantic-rs."""
    rust_module = get_rust_module()
    parsed = rust_module.parse_simple_metric_aggregation(sql_expr)
    if parsed is None:
        return None
    agg, inner = parsed
    return str(agg), (str(inner) if inner is not None else None)


def metric_to_sql_with_rust(metric_obj) -> str:
    """Render metric SQL aggregation via sidemantic-rs."""
    rust_module = get_rust_module()
    metric_yaml = yaml.safe_dump(metric_obj.model_dump(exclude_none=True), sort_keys=False)
    return rust_module.metric_to_sql(metric_yaml)


def metric_sql_expr_with_rust(metric_obj) -> str:
    """Resolve metric SQL expression via sidemantic-rs."""
    rust_module = get_rust_module()
    metric_yaml = yaml.safe_dump(metric_obj.model_dump(exclude_none=True), sort_keys=False)
    return rust_module.metric_sql_expr(metric_yaml)


def metric_is_simple_aggregation_with_rust(metric_obj) -> bool:
    """Check metric simple-aggregation status via sidemantic-rs."""
    rust_module = get_rust_module()
    metric_yaml = yaml.safe_dump(metric_obj.model_dump(exclude_none=True), sort_keys=False)
    return bool(rust_module.metric_is_simple_aggregation(metric_yaml))


def dimension_sql_expr_with_rust(dimension_obj) -> str:
    """Resolve dimension SQL expression via sidemantic-rs."""
    rust_module = get_rust_module()
    dimension_yaml = yaml.safe_dump(dimension_obj.model_dump(exclude_none=True), sort_keys=False)
    return rust_module.dimension_sql_expr(dimension_yaml)


def dimension_with_granularity_with_rust(dimension_obj, granularity: str) -> str:
    """Apply time granularity to a dimension SQL expression via sidemantic-rs."""
    rust_module = get_rust_module()
    dimension_yaml = yaml.safe_dump(dimension_obj.model_dump(exclude_none=True), sort_keys=False)
    return rust_module.dimension_with_granularity(dimension_yaml, granularity)


def model_get_hierarchy_path_with_rust(model_obj, dimension_name: str) -> list[str]:
    """Get model hierarchy path via sidemantic-rs."""
    rust_module = get_rust_module()
    model_yaml = yaml.safe_dump(model_obj.model_dump(exclude_none=True), sort_keys=False)
    return [str(item) for item in rust_module.model_get_hierarchy_path(model_yaml, dimension_name)]


def model_get_drill_down_with_rust(model_obj, dimension_name: str) -> str | None:
    """Get model drill-down target via sidemantic-rs."""
    rust_module = get_rust_module()
    model_yaml = yaml.safe_dump(model_obj.model_dump(exclude_none=True), sort_keys=False)
    result = rust_module.model_get_drill_down(model_yaml, dimension_name)
    return str(result) if result is not None else None


def model_get_drill_up_with_rust(model_obj, dimension_name: str) -> str | None:
    """Get model drill-up target via sidemantic-rs."""
    rust_module = get_rust_module()
    model_yaml = yaml.safe_dump(model_obj.model_dump(exclude_none=True), sort_keys=False)
    result = rust_module.model_get_drill_up(model_yaml, dimension_name)
    return str(result) if result is not None else None


def model_find_dimension_index_with_rust(model_obj, name: str) -> int | None:
    """Find model dimension index by name via sidemantic-rs."""
    rust_module = get_rust_module()
    model_yaml = yaml.safe_dump(model_obj.model_dump(exclude_none=True), sort_keys=False)
    result = rust_module.model_find_dimension_index(model_yaml, name)
    return int(result) if result is not None else None


def model_find_metric_index_with_rust(model_obj, name: str) -> int | None:
    """Find model metric index by name via sidemantic-rs."""
    rust_module = get_rust_module()
    model_yaml = yaml.safe_dump(model_obj.model_dump(exclude_none=True), sort_keys=False)
    result = rust_module.model_find_metric_index(model_yaml, name)
    return int(result) if result is not None else None


def model_find_segment_index_with_rust(model_obj, name: str) -> int | None:
    """Find model segment index by name via sidemantic-rs."""
    rust_module = get_rust_module()
    model_yaml = yaml.safe_dump(model_obj.model_dump(exclude_none=True), sort_keys=False)
    result = rust_module.model_find_segment_index(model_yaml, name)
    return int(result) if result is not None else None


def model_find_pre_aggregation_index_with_rust(model_obj, name: str) -> int | None:
    """Find model pre-aggregation index by name via sidemantic-rs."""
    rust_module = get_rust_module()
    model_yaml = yaml.safe_dump(model_obj.model_dump(exclude_none=True), sort_keys=False)
    result = rust_module.model_find_pre_aggregation_index(model_yaml, name)
    return int(result) if result is not None else None


def relationship_sql_expr_with_rust(relationship_obj) -> str:
    """Resolve relationship SQL expression via sidemantic-rs."""
    rust_module = get_rust_module()
    relationship_yaml = yaml.safe_dump(relationship_obj.model_dump(), sort_keys=False)
    return rust_module.relationship_sql_expr(relationship_yaml)


def relationship_related_key_with_rust(relationship_obj) -> str:
    """Resolve relationship related key via sidemantic-rs."""
    rust_module = get_rust_module()
    relationship_yaml = yaml.safe_dump(relationship_obj.model_dump(), sort_keys=False)
    return rust_module.relationship_related_key(relationship_yaml)


def relationship_foreign_key_columns_with_rust(relationship_obj) -> list[str]:
    """Resolve relationship foreign-key columns via sidemantic-rs."""
    rust_module = get_rust_module()
    relationship_yaml = yaml.safe_dump(relationship_obj.model_dump(), sort_keys=False)
    return [str(column) for column in rust_module.relationship_foreign_key_columns(relationship_yaml)]


def relationship_primary_key_columns_with_rust(relationship_obj) -> list[str]:
    """Resolve relationship primary-key columns via sidemantic-rs."""
    rust_module = get_rust_module()
    relationship_yaml = yaml.safe_dump(relationship_obj.model_dump(), sort_keys=False)
    return [str(column) for column in rust_module.relationship_primary_key_columns(relationship_yaml)]


def segment_get_sql_with_rust(segment_obj, model_alias: str = "model") -> str:
    """Resolve segment SQL placeholder interpolation via sidemantic-rs."""
    rust_module = get_rust_module()
    segment_yaml = yaml.safe_dump(segment_obj.model_dump(), sort_keys=False)
    return rust_module.segment_get_sql(segment_yaml, model_alias)


def validate_table_formula_expression_with_rust(expression: str) -> bool:
    """Validate table-calculation formula syntax via sidemantic-rs."""
    rust_module = get_rust_module()
    return bool(rust_module.validate_table_formula_expression(expression))


def build_symmetric_aggregate_sql_with_rust(
    measure_expr: str,
    primary_key: str,
    agg_type: str,
    model_alias: str | None = None,
    dialect: str = "duckdb",
) -> str:
    """Build symmetric aggregate SQL via sidemantic-rs."""
    rust_module = get_rust_module()
    return rust_module.build_symmetric_aggregate_sql(measure_expr, primary_key, agg_type, model_alias, dialect)


def needs_symmetric_aggregate_with_rust(relationship: str, is_base_model: bool) -> bool:
    """Evaluate symmetric aggregate need via sidemantic-rs."""
    rust_module = get_rust_module()
    return bool(rust_module.needs_symmetric_aggregate(relationship, is_base_model))


def parse_relative_date_with_rust(expr: str, dialect: str = "duckdb") -> str | None:
    """Parse a relative date expression into SQL via sidemantic-rs."""
    rust_module = get_rust_module()
    return rust_module.parse_relative_date(expr, dialect)


def relative_date_to_range_with_rust(
    expr: str,
    column: str = "date_col",
    dialect: str = "duckdb",
) -> str | None:
    """Convert a relative date expression to a SQL range filter via sidemantic-rs."""
    rust_module = get_rust_module()
    return rust_module.relative_date_to_range(expr, column, dialect)


def is_relative_date_with_rust(expr: str) -> bool:
    """Check relative-date expression recognition via sidemantic-rs."""
    rust_module = get_rust_module()
    return bool(rust_module.is_relative_date(expr))


def time_comparison_offset_interval_with_rust(
    comparison_type: str,
    offset: int | None = None,
    offset_unit: str | None = None,
) -> tuple[int, str]:
    """Resolve time comparison offset interval via sidemantic-rs."""
    rust_module = get_rust_module()
    amount, unit = rust_module.time_comparison_offset_interval(comparison_type, offset, offset_unit)
    return int(amount), str(unit)


def time_comparison_sql_offset_with_rust(
    comparison_type: str,
    offset: int | None = None,
    offset_unit: str | None = None,
) -> str:
    """Render SQL INTERVAL text for a time comparison via sidemantic-rs."""
    rust_module = get_rust_module()
    return rust_module.time_comparison_sql_offset(comparison_type, offset, offset_unit)


def trailing_period_sql_interval_with_rust(amount: int, unit: str) -> str:
    """Render SQL INTERVAL text for a trailing period via sidemantic-rs."""
    rust_module = get_rust_module()
    return rust_module.trailing_period_sql_interval(amount, unit)


def generate_time_comparison_sql_with_rust(
    *,
    comparison_type: str,
    calculation: str,
    current_metric_sql: str,
    time_dimension: str,
    offset: int | None = None,
    offset_unit: str | None = None,
) -> str:
    """Generate time-comparison SQL expression via sidemantic-rs."""
    rust_module = get_rust_module()
    return rust_module.generate_time_comparison_sql(
        comparison_type,
        calculation,
        current_metric_sql,
        time_dimension,
        offset,
        offset_unit,
    )


def generate_catalog_metadata_with_rust(graph: SemanticGraph, schema: str = "public") -> dict:
    """Generate Postgres-compatible catalog metadata via sidemantic-rs."""
    rust_module = get_rust_module()
    payload = rust_module.generate_catalog_metadata(graph_to_rust_yaml(graph), schema)
    return json.loads(payload)


def _serialize_pre_aggregation(pre_aggregation) -> dict:
    refresh_key = getattr(pre_aggregation, "refresh_key", None)
    refresh_key_payload = None
    if refresh_key is not None:
        refresh_key_payload = {
            "every": refresh_key.every,
            "sql": refresh_key.sql,
            "incremental": refresh_key.incremental,
            "update_window": refresh_key.update_window,
        }

    indexes = getattr(pre_aggregation, "indexes", None)
    index_payload = None
    if indexes is not None:
        index_payload = [
            {
                "name": idx.name,
                "columns": list(idx.columns),
                "type": idx.type,
            }
            for idx in indexes
        ]

    return {
        "name": pre_aggregation.name,
        "type": pre_aggregation.type,
        "measures": pre_aggregation.measures,
        "dimensions": pre_aggregation.dimensions,
        "time_dimension": pre_aggregation.time_dimension,
        "granularity": pre_aggregation.granularity,
        "partition_granularity": pre_aggregation.partition_granularity,
        "build_range_start": pre_aggregation.build_range_start,
        "build_range_end": pre_aggregation.build_range_end,
        "scheduled_refresh": pre_aggregation.scheduled_refresh,
        "refresh_key": refresh_key_payload,
        "indexes": index_payload,
    }


def match_preaggregation_with_rust(
    model,
    *,
    metrics: list[str] | None = None,
    dimensions: list[str] | None = None,
    time_granularity: str | None = None,
    filters: list[str] | None = None,
) -> str | None:
    """Return matching pre-aggregation name using sidemantic-rs compile routing.

    This is a thin compatibility bridge for Python PreAggregationMatcher:
    - compile a single-model query with `use_preaggregations=true`
    - detect routed pre-aggregation from generated SQL table reference
    """
    rust_module = get_rust_module()
    metrics = metrics or []
    dimensions = list(dimensions or [])
    filters = filters or []

    model_for_match = model.model_copy(deep=True) if hasattr(model, "model_copy") else model.copy(deep=True)
    existing_dim_names = {dimension.name for dimension in model_for_match.dimensions}
    requested_dimension_names = []
    for dimension in dimensions:
        bare = dimension.split(".", 1)[1] if "." in dimension else dimension
        bare = bare.split("__", 1)[0]
        requested_dimension_names.append(bare)

    for dim_name in requested_dimension_names:
        if dim_name not in existing_dim_names:
            from sidemantic.core.dimension import Dimension

            model_for_match.dimensions.append(
                Dimension(name=dim_name, type="categorical", sql=dim_name),
            )
            existing_dim_names.add(dim_name)

    qualified_metrics = [m if "." in m else f"{model_for_match.name}.{m}" for m in metrics]
    qualified_dimensions = [d if "." in d else f"{model_for_match.name}.{d}" for d in dimensions]

    if time_granularity and not any("__" in d for d in qualified_dimensions):
        time_dim = None
        preagg_time_dims = {
            p.time_dimension for p in model_for_match.pre_aggregations if getattr(p, "time_dimension", None)
        }
        if len(preagg_time_dims) == 1:
            time_dim = next(iter(preagg_time_dims))
        if not time_dim:
            time_dims = [d.name for d in model_for_match.dimensions if d.type == "time"]
            if len(time_dims) == 1:
                time_dim = time_dims[0]
        if time_dim:
            if time_dim not in existing_dim_names:
                from sidemantic.core.dimension import Dimension

                model_for_match.dimensions.append(
                    Dimension(name=time_dim, type="time", sql=time_dim),
                )
            qualified_dimensions.append(f"{model_for_match.name}.{time_dim}__{time_granularity}")

    models_yaml = models_to_rust_yaml([model_for_match])
    payload = {
        "metrics": qualified_metrics,
        "dimensions": qualified_dimensions,
        "filters": filters,
        "segments": [],
        "order_by": [],
        "limit": None,
        "ungrouped": False,
        "use_preaggregations": True,
        "preagg_database": None,
        "preagg_schema": None,
    }
    query_yaml = yaml.safe_dump(payload, sort_keys=False)
    sql = rust_module.compile_with_yaml(models_yaml, query_yaml)

    for pre_aggregation in sorted(model.pre_aggregations, key=lambda p: len(p.name), reverse=True):
        table_name = pre_aggregation.get_table_name(model_for_match.name)
        if table_name in sql:
            return pre_aggregation.name
    return None


def generate_preaggregation_materialization_sql_with_rust(model, pre_aggregation) -> str:
    """Generate pre-aggregation materialization SQL using sidemantic-rs."""
    rust_module = get_rust_module()
    model_for_materialization = model.model_copy(deep=True) if hasattr(model, "model_copy") else model.copy(deep=True)
    preagg_name = pre_aggregation.name
    retained = [p for p in model_for_materialization.pre_aggregations if p.name != preagg_name]
    retained.append(pre_aggregation)
    model_for_materialization.pre_aggregations = retained

    models_yaml = models_to_rust_yaml([model_for_materialization])
    return rust_module.generate_preaggregation_materialization_sql(
        models_yaml,
        model_for_materialization.name,
        preagg_name,
    )


def validate_engine_refresh_sql_compatibility_with_rust(source_sql: str, dialect: str) -> tuple[bool, str | None]:
    """Validate engine refresh SQL compatibility via sidemantic-rs."""
    rust_module = get_rust_module()
    is_valid, error_msg = rust_module.validate_engine_refresh_sql_compatibility(source_sql, dialect)
    return bool(is_valid), (str(error_msg) if error_msg is not None else None)


def build_preaggregation_refresh_statements_with_rust(
    *,
    mode: str,
    table_name: str,
    source_sql: str,
    watermark_column: str | None = None,
    from_watermark: str | None = None,
    lookback: str | None = None,
    dialect: str | None = None,
    refresh_every: str | None = None,
) -> list[str]:
    """Build pre-aggregation refresh SQL statements via sidemantic-rs planner."""
    rust_module = get_rust_module()
    statements = rust_module.build_preaggregation_refresh_statements(
        mode=mode,
        table_name=table_name,
        source_sql=source_sql,
        watermark_column=watermark_column,
        from_watermark=from_watermark,
        lookback=lookback,
        dialect=dialect,
        refresh_every=refresh_every,
    )
    return [str(statement) for statement in statements]


def refresh_preaggregation_with_rust(
    *,
    pre_aggregation,
    connection,
    source_sql: str,
    table_name: str,
    mode: str | None,
    watermark_column: str | None,
    lookback: str | None,
    from_watermark,
    to_watermark,
    dialect: str | None,
) -> dict:
    """Execute pre-aggregation refresh via sidemantic-rs."""
    rust_module = get_rust_module()

    refresh_key = getattr(pre_aggregation, "refresh_key", None)
    refresh_incremental = bool(refresh_key and refresh_key.incremental)
    refresh_every = refresh_key.every if refresh_key else None

    def _refresh_with_mode(resolved_mode: str | None) -> dict:
        return rust_module.refresh_preaggregation(
            connection=connection,
            source_sql=source_sql,
            table_name=table_name,
            mode=resolved_mode,
            watermark_column=watermark_column,
            lookback=lookback,
            from_watermark=from_watermark,
            to_watermark=to_watermark,
            dialect=dialect,
            refresh_incremental=refresh_incremental,
            refresh_every=refresh_every,
        )

    try:
        return _refresh_with_mode(mode)
    except TypeError:
        if mode is not None:
            raise

    resolved_mode: str | None = None
    if hasattr(rust_module, "plan_preaggregation_refresh_execution"):
        try:
            refresh_plan = rust_module.plan_preaggregation_refresh_execution(
                mode,
                refresh_incremental,
                watermark_column,
                dialect,
            )
            planner_mode = refresh_plan.get("mode") if hasattr(refresh_plan, "get") else None
            if planner_mode is not None:
                resolved_mode = str(planner_mode)
        except (TypeError, KeyError, AttributeError):
            resolved_mode = None

    if resolved_mode is None:
        if hasattr(rust_module, "resolve_preaggregation_refresh_mode"):
            resolved_mode = rust_module.resolve_preaggregation_refresh_mode(mode, refresh_incremental)
        else:
            resolved_mode = "incremental" if refresh_incremental else "full"

    return _refresh_with_mode(resolved_mode)


def extract_preaggregation_patterns_with_rust(queries: list[str]) -> list[dict]:
    """Extract grouped query patterns from instrumented SQL via sidemantic-rs."""
    rust_module = get_rust_module()
    payload = rust_module.extract_preaggregation_patterns(queries)
    return json.loads(payload)


def recommend_preaggregation_patterns_with_rust(
    patterns: list[dict],
    *,
    min_query_count: int,
    min_benefit_score: float,
    top_n: int | None = None,
) -> list[dict]:
    """Build pre-aggregation recommendations from pattern counts via sidemantic-rs."""
    rust_module = get_rust_module()
    payload = rust_module.recommend_preaggregation_patterns(
        json.dumps(patterns),
        min_query_count,
        min_benefit_score,
        top_n,
    )
    return json.loads(payload)


def summarize_preaggregation_patterns_with_rust(
    patterns: list[dict],
    *,
    min_query_count: int,
) -> dict:
    """Summarize pattern counts via sidemantic-rs."""
    rust_module = get_rust_module()
    payload = rust_module.summarize_preaggregation_patterns(
        json.dumps(patterns),
        min_query_count,
    )
    return json.loads(payload)


def calculate_preaggregation_benefit_score_with_rust(
    pattern: dict,
    *,
    count: int,
) -> float:
    """Calculate recommender benefit score via sidemantic-rs."""
    rust_module = get_rust_module()
    return rust_module.calculate_preaggregation_benefit_score(
        json.dumps(pattern),
        count,
    )


def generate_preaggregation_name_with_rust(pattern: dict) -> str:
    """Generate recommender name via sidemantic-rs."""
    rust_module = get_rust_module()
    return rust_module.generate_preaggregation_name(
        json.dumps(pattern),
    )


def generate_preaggregation_definition_with_rust(recommendation: dict) -> dict:
    """Generate a pre-aggregation definition payload via sidemantic-rs."""
    rust_module = get_rust_module()
    payload = rust_module.generate_preaggregation_definition(
        json.dumps(recommendation),
    )
    return json.loads(payload)


def _normalize_filter_sql(filter_sql: str) -> str:
    sql = filter_sql.replace("{model}.", "")
    sql = sql.replace("{model}", "")
    sql = re.sub(r"\b\w+_cte\.", "", sql)
    return sql


def _serialize_relationship(relationship, source_model, target_model) -> dict | None:
    foreign_keys = _as_list(relationship.foreign_key)
    if not foreign_keys:
        foreign_keys = [f"{relationship.name}_id"] if relationship.type == "many_to_one" else ["id"]

    if relationship.primary_key is not None:
        primary_keys = _as_list(relationship.primary_key)
    elif target_model:
        primary_keys = target_model.primary_key_columns
    else:
        primary_keys = ["id"]

    sql = None
    if len(foreign_keys) > 1 and len(foreign_keys) == len(primary_keys):
        sql = " AND ".join(f"{{from}}.{fk} = {{to}}.{pk}" for fk, pk in zip(foreign_keys, primary_keys, strict=False))
    if getattr(relationship, "sql", None):
        sql = relationship.sql

    through_foreign_key = getattr(relationship, "through_foreign_key", None)
    related_foreign_key = getattr(relationship, "related_foreign_key", None)
    if relationship.type == "many_to_many":
        junction_keys_fn = getattr(relationship, "junction_keys", None)
        if callable(junction_keys_fn):
            junction_self_fk, junction_related_fk = junction_keys_fn()
            through_foreign_key = through_foreign_key or junction_self_fk
            related_foreign_key = related_foreign_key or junction_related_fk

    return {
        "name": relationship.name,
        "type": relationship.type,
        "foreign_key": foreign_keys[0] if foreign_keys else None,
        "primary_key": primary_keys[0] if primary_keys else None,
        "foreign_key_columns": foreign_keys,
        "primary_key_columns": primary_keys,
        "through": getattr(relationship, "through", None),
        "through_foreign_key": through_foreign_key,
        "related_foreign_key": related_foreign_key,
        "sql": sql,
    }
