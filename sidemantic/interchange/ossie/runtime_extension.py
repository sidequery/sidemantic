"""Versioned native runtime preservation for consumers supporting Sidemantic.

The core fingerprint detects edits to a projection; it is not a signature or a
trust boundary. Native SQL and security templates retain the same trust model
as an authored Sidemantic model and are never evaluated while importing.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from copy import deepcopy

from pydantic import BaseModel, ConfigDict, TypeAdapter

from sidemantic.core.consumption import Explore, SavedQuery
from sidemantic.core.inheritance import (
    resolve_metric_inheritance,
    resolve_model_inheritance,
    resolve_model_metric_inheritance,
)
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.parameter import Parameter
from sidemantic.core.registry import reset_current_layer, set_current_layer
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.core.table_calculation import TableCalculation
from sidemantic.validation import validate_model

RUNTIME_VENDOR = "SIDEMANTIC"
_FORMAT = "sidemantic.runtime"
_VERSION = 1
_COLLECTIONS = {
    "models": Model,
    "metrics": Metric,
    "parameters": Parameter,
    "table_calculations": TableCalculation,
    "explores": Explore,
    "saved_queries": SavedQuery,
}


class _RuntimePayload(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    format: str
    version: int
    expression_dialect: str
    core_sha256: str
    graph: dict[str, object]


def _json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _core_hash(semantic_model: Mapping[str, object]) -> str:
    core = {key: value for key, value in semantic_model.items() if key != "custom_extensions"}
    return hashlib.sha256(_json(core).encode()).hexdigest()


def _graph_data(graph: SemanticGraph) -> dict[str, object]:
    data = {key: {name: _native_data(item) for name, item in getattr(graph, key).items()} for key in _COLLECTIONS}
    data.update(metric_owners=dict(graph.metric_owners), metadata=graph.metadata, import_warnings=graph.import_warnings)
    return data


def _native_data(item: BaseModel) -> dict[str, object]:
    """Preserve authored defaults and in-place collection edits for inheritance."""
    full = item.model_dump(mode="json")
    result = {}
    for name, field in type(item).model_fields.items():
        value = getattr(item, name)
        if name not in item.model_fields_set and value == field.get_default(call_default_factory=True):
            continue
        if isinstance(value, BaseModel):
            result[name] = _native_data(value)
        elif isinstance(value, list):
            result[name] = [
                _native_data(child) if isinstance(child, BaseModel) else serialized
                for child, serialized in zip(value, full[name], strict=True)
            ]
        else:
            # Some native execution fields (edge IDs and logical/time roles)
            # are excluded from ordinary model export, but belong here.
            result[name] = full[name] if name in full else TypeAdapter(field.annotation).dump_python(value, mode="json")
    return result


def resolve_runtime_graph(graph: SemanticGraph) -> SemanticGraph:
    """Resolve inheritance on isolated copies using the native runtime rules."""
    resolved = deepcopy(graph)
    token = set_current_layer(None)
    try:
        resolved.models = resolve_model_inheritance(resolved.models)
        resolved.metrics = resolve_metric_inheritance(resolved.metrics)
        # Native inheritance uses ordinary model_dump, which intentionally
        # omits interchange-only fields. Restore those from the declaration
        # that owns each merged member, without changing inheritance rules.
        for model in resolved.models.values():
            lineage = _lineage(graph.models[model.name], graph.models)
            for collection in ("dimensions", "metrics", "relationships"):
                members = {item.name: item for ancestor in lineage for item in getattr(ancestor, collection)}
                for item in getattr(model, collection):
                    _restore_excluded_fields(item, [members[item.name]])
        for metric in resolved.metrics.values():
            _restore_excluded_fields(metric, _lineage(graph.metrics[metric.name], graph.metrics))
        for model in resolved.models.values():
            declared_metrics = {metric.name: metric for metric in model.metrics}
            resolve_model_metric_inheritance(model)
            for metric in model.metrics:
                _restore_excluded_fields(metric, _lineage(declared_metrics[metric.name], declared_metrics))
            if bool(model.table) == bool(model.sql):
                raise ValueError(f"Runtime model {model.name!r} requires exactly one table or SQL source")
            if model.has_untranslated_dax or any(
                item.has_untranslated_dax for item in [*model.dimensions, *model.metrics]
            ):
                raise ValueError(f"Runtime model {model.name!r} contains untranslated DAX")
            errors = validate_model(model)
            if errors:
                raise ValueError("; ".join(errors))
            for relationship in model.relationships:
                if relationship.related_model not in resolved.models:
                    raise ValueError(f"Runtime relationship references unknown model {relationship.related_model!r}")
                if relationship.through is not None and relationship.through not in resolved.models:
                    raise ValueError(f"Runtime relationship references unknown junction {relationship.through!r}")
        if any(metric.has_untranslated_dax for metric in resolved.metrics.values()):
            raise ValueError("Runtime graph contains untranslated DAX metrics")
        resolved.build_adjacency()
    finally:
        reset_current_layer(token)
    return resolved


def _lineage(item: BaseModel, declarations: Mapping[str, BaseModel]) -> list[BaseModel]:
    ancestors = [item]
    while getattr(item, "extends", None):
        item = declarations[item.extends]
        ancestors.append(item)
    return list(reversed(ancestors))


def _restore_excluded_fields(item: BaseModel, ancestors: list[BaseModel]) -> None:
    for name, field in type(item).model_fields.items():
        if not field.exclude:
            continue
        for ancestor in ancestors:
            if name in ancestor.model_fields_set:
                setattr(item, name, deepcopy(getattr(ancestor, name)))


def encode_runtime_extension(
    graph: SemanticGraph,
    semantic_model: Mapping[str, object],
    *,
    expression_dialect: str,
) -> dict[str, str]:
    """Capture declarative graph state without connection or request/user state."""
    payload = {
        "format": _FORMAT,
        "version": _VERSION,
        "expression_dialect": expression_dialect.upper(),
        "core_sha256": _core_hash(semantic_model),
        "graph": _graph_data(graph),
    }
    extension = {"vendor_name": RUNTIME_VENDOR, "data": _json(payload)}
    # Exercise exactly the same validation before publishing native state.
    decode_runtime_extension(
        {**semantic_model, "custom_extensions": [extension]},
        target_dialect=expression_dialect,
    )
    return extension


def _object_without_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate runtime extension JSON key {key!r}")
        result[key] = value
    return result


def decode_runtime_extension(semantic_model: Mapping[str, object], *, target_dialect: str) -> SemanticGraph | None:
    """Restore native state, refusing malformed, unsupported, or stale payloads."""
    extensions = semantic_model.get("custom_extensions", [])
    if not isinstance(extensions, (list, tuple)):
        raise ValueError("custom_extensions must be an array")
    recognized = [
        item for item in extensions if isinstance(item, Mapping) and item.get("vendor_name") == RUNTIME_VENDOR
    ]
    if not recognized:
        return None
    if len(recognized) != 1:
        raise ValueError("Exactly one Sidemantic runtime extension is allowed per scope")
    extension = recognized[0]
    if set(extension) != {"vendor_name", "data"} or not isinstance(extension.get("data"), str):
        raise ValueError("Malformed Sidemantic runtime extension envelope")
    payload = _RuntimePayload.model_validate(
        json.loads(extension["data"], object_pairs_hook=_object_without_duplicates)
    )
    if payload.format != _FORMAT or payload.version != _VERSION:
        raise ValueError("Unsupported Sidemantic runtime extension format or version")
    dialect = payload.expression_dialect
    if dialect not in {"ANSI_SQL", "BIGQUERY", "DATABRICKS", "SNOWFLAKE"}:
        raise ValueError(f"Unsupported runtime expression dialect {dialect!r}")
    if dialect != "ANSI_SQL" and dialect.lower() != target_dialect.lower():
        raise ValueError(f"Runtime extension requires target dialect {dialect!r}, received {target_dialect!r}")
    if payload.core_sha256 != _core_hash(semantic_model):
        raise ValueError("Sidemantic runtime extension is stale: the Ossie core projection was edited")
    data = payload.graph
    if set(data) != {*_COLLECTIONS, "metric_owners", "metadata", "import_warnings"}:
        raise ValueError("Runtime graph has missing or unknown state collections")
    graph = SemanticGraph()
    token = set_current_layer(None)
    try:
        for collection, item_type in _COLLECTIONS.items():
            values = data[collection]
            if not isinstance(values, dict):
                raise ValueError(f"Runtime {collection} must be a named object")
            restored = {}
            for name, value in values.items():
                item = item_type.model_validate(deepcopy(value))
                if item.name != name:
                    raise ValueError(f"Runtime {collection} key {name!r} does not match its declared name")
                # Native models historically ignore unknown fields. Requiring the
                # canonical form prevents silently dropping future policy.
                if _json(_native_data(item)) != _json(value):
                    raise ValueError(f"Runtime {collection} entry {name!r} contains noncanonical or unknown fields")
                restored[name] = item
            setattr(graph, collection, restored)
    finally:
        reset_current_layer(token)
    owners = data["metric_owners"]
    if not isinstance(owners, dict) or any(
        name not in graph.metrics or not isinstance(owner, str) or owner not in graph.models
        for name, owner in owners.items()
    ):
        raise ValueError("Runtime metric owners reference unknown metrics or models")
    if not isinstance(data["metadata"], dict) or not isinstance(data["import_warnings"], list):
        raise ValueError("Runtime metadata and import warnings have invalid types")
    graph.metric_owners = owners
    graph.metadata = data["metadata"]
    graph.import_warnings = data["import_warnings"]
    resolve_runtime_graph(graph)
    graph.build_adjacency()
    return graph
