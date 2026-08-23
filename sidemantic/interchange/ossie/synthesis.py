"""Fail-closed synthesis of Apache Ossie documents from runtime graphs.

This path is intentionally distinct from source-document serialization. A
``SemanticGraph`` is a lowered runtime projection and cannot reproduce lexical
source form, alternate dialect expressions, ontology documents, or every Ossie
field. Synthesis therefore requires the caller to name the output scope and the
actual dialect of every emitted expression, and refuses constructs that would
need invented semantic meaning.
"""

from __future__ import annotations

from dataclasses import dataclass

from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.interchange.ossie.diagnostics import (
    OssieDiagnostic,
    OssieDiagnosticSeverity,
    sort_diagnostics,
)
from sidemantic.interchange.ossie.documents import OssieLogicalDocument
from sidemantic.interchange.ossie.expression_validation import scalar_sql_expression_error
from sidemantic.interchange.ossie.profiles import (
    OssieConsumerProfile,
    OssieProfileError,
    OssieSerialization,
    resolve_ossie_profile,
)
from sidemantic.interchange.ossie.semantic_validation import validate_ossie_semantics
from sidemantic.interchange.ossie.validation import validate_ossie_schema

_SQL_DIALECTS = frozenset({"ANSI_SQL", "BIGQUERY", "DATABRICKS", "SNOWFLAKE"})
_SQLGLOT_DIALECTS = {"ANSI_SQL": None, "BIGQUERY": "bigquery", "DATABRICKS": "databricks", "SNOWFLAKE": "snowflake"}
_DATA_TYPES = frozenset(
    {"String", "Integer", "Decimal", "Float", "Boolean", "Date", "Time", "DateTime", "DateTimeTz", "Opaque"}
)


@dataclass(frozen=True, slots=True)
class OssieSynthesisResult:
    """A schema-valid synthesized document, or diagnostics explaining refusal."""

    document: OssieLogicalDocument | None
    diagnostics: tuple[OssieDiagnostic, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "diagnostics", sort_diagnostics(self.diagnostics))

    @property
    def valid(self) -> bool:
        return self.document is not None and not any(
            diagnostic.severity is OssieDiagnosticSeverity.ERROR for diagnostic in self.diagnostics
        )


class OssieSynthesisError(ValueError):
    """Graph synthesis refusal carrying stable structured diagnostics."""

    def __init__(self, diagnostics: tuple[OssieDiagnostic, ...]) -> None:
        self.diagnostics = sort_diagnostics(diagnostics)
        super().__init__("; ".join(diagnostic.message for diagnostic in self.diagnostics))


def _error(code: str, message: str, pointer: str = "") -> OssieDiagnostic:
    return OssieDiagnostic(
        severity=OssieDiagnosticSeverity.ERROR,
        code=code,
        message=message,
        json_pointer=pointer,
    )


def _warning(code: str, message: str, pointer: str = "") -> OssieDiagnostic:
    return OssieDiagnostic(
        severity=OssieDiagnosticSeverity.WARNING,
        code=code,
        message=message,
        json_pointer=pointer,
    )


def _expression(text: str, dialect: str) -> dict[str, object]:
    return {"dialects": [{"dialect": dialect, "expression": text}]}


def _scalar_expression_error(text: str, dialect: str) -> str | None:
    return scalar_sql_expression_error(text, sqlglot_dialect=_SQLGLOT_DIALECTS[dialect])


def _metric_expression(metric: Metric, model_name: str | None) -> str | None:
    if metric.sql_is_complete and metric.sql:
        return metric.sql
    if metric.type == "ratio":
        if not metric.numerator or not metric.denominator:
            return None
        return f"{metric.numerator} / NULLIF({metric.denominator}, 0)"
    if metric.type == "derived":
        return metric.sql
    if metric.type is not None:
        return None
    if metric.agg:
        inner = metric.sql or "*"
        if model_name and inner != "*" and "." not in inner:
            inner = f"{model_name}.{inner}"
        if metric.agg == "count_distinct":
            return f"COUNT(DISTINCT {inner})"
        aggregate = {"variance_pop": "VAR_POP"}.get(metric.agg, metric.agg.upper())
        return f"{aggregate}({inner})"
    return metric.sql


def _dataset(model: Model, dialect: str, index: int, diagnostics: list[OssieDiagnostic]) -> dict[str, object] | None:
    pointer = f"/semantic_model/0/datasets/{index}"
    if model.table and model.sql:
        diagnostics.append(
            _error(
                "ossie.synthesis.source_ambiguous",
                f"Model {model.name!r} has both table and SQL sources; choose one before Ossie synthesis.",
                f"{pointer}/source",
            )
        )
        return None
    source = model.sql or model.table
    if not source:
        diagnostics.append(
            _error(
                "ossie.synthesis.source_missing",
                f"Model {model.name!r} has no source; Ossie requires one and Sidemantic will not invent it.",
                f"{pointer}/source",
            )
        )
        return None
    if model.has_untranslated_dax:
        diagnostics.append(
            _error(
                "ossie.synthesis.source_language_unsupported",
                f"Model {model.name!r} contains untranslated DAX and cannot be emitted as SQL.",
                f"{pointer}/source",
            )
        )
        return None

    dataset: dict[str, object] = {"name": model.name, "source": source}
    if model.primary_key is not None:
        dataset["primary_key"] = model.primary_key_columns
    if model.unique_keys is not None:
        dataset["unique_keys"] = model.unique_keys
    if model.description is not None:
        dataset["description"] = model.description

    fields: list[dict[str, object]] = []
    for field_index, dimension in enumerate(model.dimensions):
        field_pointer = f"{pointer}/fields/{field_index}"
        if dimension.has_untranslated_dax:
            diagnostics.append(
                _error(
                    "ossie.synthesis.expression_language_unsupported",
                    f"Field {model.name}.{dimension.name} contains untranslated DAX.",
                    f"{field_pointer}/expression",
                )
            )
            continue
        expression_error = _scalar_expression_error(dimension.sql_expr, dialect)
        if expression_error is not None:
            diagnostics.append(
                _error(
                    "ossie.synthesis.expression_invalid",
                    f"Field {model.name}.{dimension.name} is not one {dialect} SQL expression: {expression_error}",
                    f"{field_pointer}/expression",
                )
            )
            continue
        field: dict[str, object] = {
            "name": dimension.name,
            "expression": _expression(dimension.sql_expr, dialect),
        }
        if dimension.logical_data_type is not None:
            if dimension.logical_data_type not in _DATA_TYPES:
                diagnostics.append(
                    _error(
                        "ossie.synthesis.datatype_unsupported",
                        f"Field {model.name}.{dimension.name} has unsupported logical datatype {dimension.logical_data_type!r}.",
                        f"{field_pointer}/datatype",
                    )
                )
            else:
                field["datatype"] = dimension.logical_data_type
        if dimension.declared_is_time is not None:
            field["dimension"] = {"is_time": dimension.declared_is_time}
        elif dimension.type == "time" and dimension.logical_data_type not in {"Date", "Time", "DateTime", "DateTimeTz"}:
            # Preserve runtime time-role semantics when datatype omission would
            # otherwise make Ossie default this field to non-time.
            field["dimension"] = {"is_time": True}
        if dimension.description is not None:
            field["description"] = dimension.description
        if dimension.label is not None:
            field["label"] = dimension.label
        fields.append(field)
    if fields:
        dataset["fields"] = fields
    return dataset


def _relationships(models: dict[str, Model], diagnostics: list[OssieDiagnostic]) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    used_names: set[str] = set()
    relationship_index = 0
    for from_model in models.values():
        for relationship in from_model.relationships:
            pointer = f"/semantic_model/0/relationships/{relationship_index}"
            relationship_index += 1
            if relationship.type != "many_to_one":
                diagnostics.append(
                    _error(
                        "ossie.synthesis.relationship_cardinality_unsupported",
                        f"Relationship from {from_model.name!r} to {relationship.name!r} is {relationship.type!r}; Ossie core requires from-many/to-one.",
                        pointer,
                    )
                )
                continue
            edge_id = relationship.edge_id or (relationship.metadata or {}).get("osi_name")
            if not isinstance(edge_id, str) or not edge_id:
                diagnostics.append(
                    _error(
                        "ossie.synthesis.relationship_identity_missing",
                        f"Relationship from {from_model.name!r} to {relationship.name!r} has no declared edge identity.",
                        f"{pointer}/name",
                    )
                )
                continue
            if edge_id in used_names:
                diagnostics.append(
                    _error(
                        "ossie.synthesis.relationship_identity_duplicate",
                        f"Relationship identity {edge_id!r} is duplicated.",
                        f"{pointer}/name",
                    )
                )
                continue
            used_names.add(edge_id)
            target = models.get(relationship.name)
            from_columns = relationship.foreign_key_columns
            to_columns = relationship.primary_key_columns
            if target is None or not from_columns or not to_columns or len(from_columns) != len(to_columns):
                diagnostics.append(
                    _error(
                        "ossie.synthesis.relationship_keys_unusable",
                        f"Relationship {edge_id!r} lacks a valid target or equal non-empty key lists; keys will not be invented.",
                        pointer,
                    )
                )
                continue
            target_keys = {tuple(target.primary_key_columns)} if target.primary_key_columns else set()
            target_keys.update(tuple(key) for key in target.unique_keys or ())
            if tuple(to_columns) not in target_keys:
                diagnostics.append(
                    _error(
                        "ossie.synthesis.relationship_target_not_unique",
                        f"Relationship {edge_id!r} targets columns that are not declared primary or unique.",
                        f"{pointer}/to_columns",
                    )
                )
                continue
            result.append(
                {
                    "name": edge_id,
                    "from": from_model.name,
                    "to": relationship.name,
                    "from_columns": from_columns,
                    "to_columns": to_columns,
                }
            )
    return result


def _metrics(
    graph: SemanticGraph,
    models: dict[str, Model],
    dialect: str,
    diagnostics: list[OssieDiagnostic],
) -> list[dict[str, object]]:
    candidates: list[tuple[Metric, str | None]] = [
        (metric, graph.metric_owners.get(name)) for name, metric in graph.metrics.items()
    ]
    candidates.extend((metric, model.name) for model in models.values() for metric in model.metrics)
    result: list[dict[str, object]] = []
    seen: dict[str, str] = {}
    for metric, owner in candidates:
        expression = _metric_expression(metric, owner)
        if expression is None:
            diagnostics.append(
                _error(
                    "ossie.synthesis.metric_unrepresentable",
                    f"Metric {metric.name!r} cannot be represented as one Ossie SQL expression.",
                )
            )
            continue
        expression_error = _scalar_expression_error(expression, dialect)
        if expression_error is not None:
            diagnostics.append(
                _error(
                    "ossie.synthesis.expression_invalid",
                    f"Metric {metric.name!r} is not one {dialect} SQL expression: {expression_error}",
                )
            )
            continue
        previous = seen.get(metric.name)
        if previous is not None:
            if previous != expression:
                diagnostics.append(
                    _error(
                        "ossie.synthesis.metric_name_collision",
                        f"Metric {metric.name!r} has multiple distinct runtime definitions.",
                    )
                )
            continue
        seen[metric.name] = expression
        value: dict[str, object] = {"name": metric.name, "expression": _expression(expression, dialect)}
        if metric.logical_data_type is not None:
            if metric.logical_data_type not in _DATA_TYPES:
                diagnostics.append(
                    _error(
                        "ossie.synthesis.datatype_unsupported",
                        f"Metric {metric.name!r} has unsupported logical datatype {metric.logical_data_type!r}.",
                    )
                )
            else:
                value["datatype"] = metric.logical_data_type
        if metric.description is not None:
            value["description"] = metric.description
        result.append(value)
    return result


def synthesize_ossie_document(
    graph: SemanticGraph,
    *,
    scope_name: str,
    expression_dialect: str,
    schema_version: str = "0.2.0.dev0",
    serialization: OssieSerialization | str = OssieSerialization.YAML,
    consumer_profile: OssieConsumerProfile | str = OssieConsumerProfile.OSSIE_CORE,
) -> OssieSynthesisResult:
    """Synthesize one logical document without guessing scope or dialect."""

    if not isinstance(graph, SemanticGraph):
        raise TypeError("graph must be a SemanticGraph")
    if not isinstance(scope_name, str) or not scope_name.strip():
        raise ValueError("scope_name must be an explicit non-empty string")
    if not isinstance(expression_dialect, str):
        raise TypeError("expression_dialect must be a string")
    dialect = expression_dialect.strip().upper()
    if dialect not in _SQL_DIALECTS:
        supported = ", ".join(sorted(_SQL_DIALECTS))
        raise ValueError(f"expression_dialect must identify emitted SQL exactly; supported: {supported}")

    diagnostics: list[OssieDiagnostic] = []
    try:
        profile = resolve_ossie_profile(schema_version, consumer_profile)
    except OssieProfileError as exc:
        return OssieSynthesisResult(
            document=None,
            diagnostics=(_error("ossie.synthesis.profile_unsupported", str(exc), "/version"),),
        )
    models = dict(graph.models)
    datasets = [
        dataset
        for index, model in enumerate(models.values())
        if (dataset := _dataset(model, dialect, index, diagnostics)) is not None
    ]
    semantic_model: dict[str, object] = {"name": scope_name, "datasets": datasets}
    relationships = _relationships(models, diagnostics)
    metrics = _metrics(graph, models, dialect, diagnostics)
    if relationships:
        semantic_model["relationships"] = relationships
    if metrics:
        semantic_model["metrics"] = metrics
    data = {"version": schema_version, "semantic_model": [semantic_model]}

    validation = validate_ossie_schema(data, profile=profile, consumer_profile=profile.consumer_profile)
    diagnostics.extend(validation.diagnostics)
    semantic_validation = validate_ossie_semantics(data, profile=profile)
    diagnostics.extend(semantic_validation.diagnostics)
    if any(diagnostic.severity is OssieDiagnosticSeverity.ERROR for diagnostic in diagnostics):
        return OssieSynthesisResult(document=None, diagnostics=tuple(diagnostics))

    document = OssieLogicalDocument(canonical_data=data, serialization=serialization)
    return OssieSynthesisResult(document=document, diagnostics=tuple(diagnostics))


def require_synthesized_document(result: OssieSynthesisResult) -> OssieLogicalDocument:
    """Return a successful document or raise its structured refusal."""

    if result.document is None:
        raise OssieSynthesisError(result.diagnostics)
    return result.document
