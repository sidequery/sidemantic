"""Versioned, inert input and compatibility errors for semantic compilation."""

import json
from collections.abc import Iterable
from copy import deepcopy
from typing import Any

from pydantic import BaseModel

from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_graph import SemanticGraph

SEMANTIC_INPUT_VERSION = 1


class RustBackendUnavailableError(ValueError):
    """The optional Rust runtime or its required entrypoint is unavailable."""


class UnsupportedSemanticFeaturesError(ValueError):
    """A runtime cannot execute all requirements of a semantic query."""

    def __init__(self, capabilities: Iterable[str]):
        self.capabilities = sorted(set(capabilities))
        super().__init__("Rust runtime does not support required capabilities: " + ", ".join(self.capabilities))


def _definition(value: BaseModel) -> dict[str, Any]:
    """Snapshot an already constructed definition without running authoring logic.

    The versioned contract uses the Python definitions' default values when a
    field is absent. Source identity/type fields excluded by authoring exports
    remain part of this compiler input.
    """
    if isinstance(value, Model) and value.extends:
        # Unresolved children distinguish omitted fields from explicit clears
        # and default-valued overrides. The receiver applies inheritance.
        data = value.model_dump(mode="json", exclude_unset=True)
    else:
        data = value.model_dump(mode="json", exclude_none=True, exclude_defaults=True)
    if isinstance(value, Relationship) and value.type in ("one_to_one", "one_to_many") and value.primary_key is None:
        # TMDL retains the declared source endpoint separately from the model's
        # primary key. Snapshot that existing endpoint without resolving SQL.
        source_column = getattr(value, "_tmdl_from_column", None)
        if isinstance(source_column, str) and source_column.strip():
            data["primary_key"] = source_column
    for name in ("logical_data_type", "declared_is_time", "edge_id"):
        field_value = getattr(value, name, None)
        if field_value is not None:
            data[name] = field_value
    for name in list(data):
        field_value = getattr(value, name, None)
        if isinstance(field_value, BaseModel):
            data[name] = _definition(field_value)
        elif isinstance(field_value, list) and any(isinstance(item, BaseModel) for item in field_value):
            data[name] = [_definition(item) if isinstance(item, BaseModel) else item for item in field_value]
    return data


def graph_to_semantic_input(graph: SemanticGraph, *, input_dialect: str = "duckdb") -> dict[str, Any]:
    """Create an inert versioned snapshot of a semantic graph.

    Graph metrics stay at graph scope. Only owners explicitly recorded by the
    graph are transmitted. SQL is copied verbatim; reference binding, dialect
    interpretation, and capability acceptance belong to the receiving engine.
    """
    if not isinstance(input_dialect, str) or not input_dialect.strip():
        raise ValueError("A non-empty input SQL dialect is required")
    models = []
    for model in graph.models.values():
        definition = _definition(model)
        if not model.extends or "primary_key" in model.model_fields_set:
            definition["primary_key"] = list(model.primary_key_columns) or None
        models.append(definition)
    return {
        "version": SEMANTIC_INPUT_VERSION,
        "input_dialect": input_dialect,
        "models": models,
        "metrics": [
            _definition(metric)
            for metric in graph.metrics.values()
            # add_model exposes these same objects through graph.metrics for
            # unqualified lookup. The native model index already does that.
            if not (
                metric.type in ("time_comparison", "conversion")
                and metric.name not in graph.metric_owners
                and any(metric is owned for model in graph.models.values() for owned in model.metrics)
            )
        ],
        "metric_owners": dict(graph.metric_owners),
        "parameters": [_definition(parameter) for parameter in graph.parameters.values()],
        "table_calculations": [_definition(calculation) for calculation in graph.table_calculations.values()],
        "explores": [_definition(explore) for explore in graph.explores.values()],
        "saved_queries": [_definition(query) for query in graph.saved_queries.values()],
        "metadata": deepcopy(graph.metadata),
        "import_warnings": deepcopy(graph.import_warnings),
        "required_capabilities": graph_handoff_requirements(graph, input_dialect=input_dialect),
    }


def graph_to_semantic_json(graph: SemanticGraph, *, input_dialect: str = "duckdb") -> str:
    """Encode semantic input as JSON without passing through native YAML."""
    return json.dumps(graph_to_semantic_input(graph, input_dialect=input_dialect), allow_nan=False)


def graph_handoff_requirements(graph: SemanticGraph, *, input_dialect: str = "duckdb") -> list[str]:
    """Declare whole-graph requirements; the receiver also checks the data.

    This is not a claim that all other features are supported. The receiving
    versioned decoder validates executable fields and the compiler validates queries.
    Named consumption and calculation catalogs are retained in every snapshot;
    query entrypoints decide whether those definitions are active. Their mere
    presence does not make a plain structured query execute a catalog entry.
    """
    requirements: set[str] = set()
    if input_dialect.lower() != "duckdb":
        requirements.add(f"input_dialect.{input_dialect.lower()}")
    for model in graph.models.values():
        if model.security is not None:
            requirements.add("model.security")
        if model.invariant_filters:
            requirements.add("model.invariant_filters")
        if model.schema_exposure is not None:
            requirements.add("model.schema_exposure")
        if model.has_untranslated_dax:
            requirements.add("model.dax")
        for relationship in model.relationships:
            if relationship.target_model is not None:
                requirements.add("relationship.roles")
            if not relationship.active:
                requirements.add("relationship.inactive")
        for dimension in model.dimensions:
            if dimension.has_untranslated_dax:
                requirements.add("dimension.dax")
        for preaggregation in model.pre_aggregations:
            if preaggregation.type == "lambda":
                requirements.add("preaggregation.lambda")
    for metric in [*graph.metrics.values(), *(metric for model in graph.models.values() for metric in model.metrics)]:
        if metric.has_untranslated_dax:
            requirements.add("metric.dax")
    if graph.table_calculations:
        requirements.add("graph.table_calculations")
    if graph.explores:
        requirements.add("graph.explores")
    if graph.saved_queries:
        requirements.add("graph.saved_queries")
    return sorted(requirements)
