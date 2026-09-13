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

import sqlglot
from pydantic import BaseModel
from sqlglot import expressions as exp

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
from sidemantic.interchange.ossie.identifier import normalize_identifier
from sidemantic.interchange.ossie.profiles import (
    OssieConsumerProfile,
    OssieProfileError,
    OssieSerialization,
    resolve_ossie_profile,
)
from sidemantic.interchange.ossie.runtime_extension import encode_runtime_extension
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


def _metric_expression(metric: Metric) -> str | None:
    if metric.type not in {None, "ratio", "derived"}:
        return None
    if metric.sql_is_complete and metric.sql:
        return metric.sql
    if metric.type == "ratio":
        if not metric.numerator or not metric.denominator:
            return None
        return f"{metric.numerator} / NULLIF({metric.denominator}, 0)"
    if metric.agg:
        inner = metric.sql or "*"
        if metric.filters:
            condition = " AND ".join(f"({item})" for item in metric.filters)
            inner = f"CASE WHEN {condition} THEN {'1' if inner == '*' else inner} ELSE NULL END"
        if metric.agg == "count_distinct":
            return f"COUNT(DISTINCT {inner})"
        aggregate = {"variance_pop": "VAR_POP"}.get(metric.agg, metric.agg.upper())
        return f"{aggregate}({inner})"
    return metric.sql


def _generated_field(
    graph: SemanticGraph, fields: dict[str, dict[str, str]], owner: str, expression: str, preferred: str
) -> str:
    generated = fields.setdefault(owner, {})
    for name, existing in generated.items():
        if existing == expression:
            return name
    names = {normalize_identifier(d.name) for d in graph.models[owner].dimensions}
    names.update(normalize_identifier(m.name) for m in graph.models[owner].metrics)
    names.update(normalize_identifier(name) for name in generated)
    name = preferred
    while normalize_identifier(name) in names:
        name += "_"
    generated[name] = expression
    return name


def _qualify_metric_expression(
    expression: str,
    metric: Metric,
    owner: str | None,
    graph: SemanticGraph,
    dialect: str,
    generated_fields: dict[str, dict[str, str]],
    model_local: bool,
) -> str | None:
    """Move model-local column expressions into Ossie's scope-wide namespace."""
    parsed = sqlglot.parse_one(expression, read=_SQLGLOT_DIALECTS[dialect])
    # Aggregate and opaque SQL consume source columns, even when a column has
    # the same name as a metric. Derived formulas instead consume metric refs.
    raw_columns = metric.sql_is_complete or bool(metric.agg) or any(parsed.find_all(exp.AggFunc))
    if owner and raw_columns and not any(parsed.find_all(exp.Column)):
        # An explicit constant dataset field retains the row source even when
        # the aggregate contains only literals (including COUNT(*)).
        name = _generated_field(graph, generated_fields, owner, "1", "__sidemantic_row")
        for aggregate in parsed.find_all(exp.AggFunc):
            inner = aggregate.this
            if inner is None or isinstance(inner, exp.Distinct):
                return None
            anchor = exp.column(name, table=owner)
            aggregate.set(
                "this",
                anchor if isinstance(inner, exp.Star) else exp.Case().when(anchor.eq(1), inner),
            )
    metric_names = set(graph.metrics)
    metric_names.update(item.name for model in graph.models.values() for item in model.metrics)
    for column in parsed.find_all(exp.Column):
        if not raw_columns and column.name in metric_names:
            if column.table:
                model = graph.models.get(column.table)
                if model is not None and model.get_metric(column.name) is not None:
                    column.set("table", None)
            continue
        if owner and not column.table:
            column.set("table", exp.to_identifier(owner))
        if model_local and raw_columns and column.table == owner:
            dimension = graph.models[owner].get_dimension(column.name)
            if dimension is not None and dimension.sql_expr != column.name:
                # Model measures consume physical columns. A scope metric that
                # uses the same field name would instead apply its dimension's
                # expression, changing both measure inputs and filter operands.
                raw = column.copy()
                raw.set("table", None)
                field_name = _generated_field(
                    graph,
                    generated_fields,
                    owner,
                    raw.sql(dialect=_SQLGLOT_DIALECTS[dialect]),
                    "__sidemantic_raw",
                )
                column.set("this", exp.to_identifier(field_name))
    return parsed.sql(dialect=_SQLGLOT_DIALECTS[dialect])


def _unsupported_metric_options(metric: Metric) -> list[str]:
    # These require query context or additional runtime rewrites. Emitting just
    # sql would silently discard filters, null handling, or time semantics.
    options = (
        "non_additive_dimension",
        "non_additive_window_groupings",
        "offset_window",
        "window",
        "grain_to_date",
        "window_expression",
        "window_frame",
        "window_order",
        "base_metric",
        "comparison_type",
        "time_offset",
        "calculation",
        "entity",
        "base_event",
        "conversion_event",
        "conversion_window",
        "steps",
        "cohort_event",
        "activity_event",
        "periods",
        "retention_granularity",
        "inner_metrics",
        "entity_dimensions",
        "having",
        "extends",
    )
    unsupported = [name for name in options if getattr(metric, name) is not None and getattr(metric, name) != []]
    if metric.filters and (not metric.agg or metric.sql_is_complete):
        unsupported.append("filters")
    if not metric.public:
        unsupported.append("public=False")
    return unsupported


def _unrepresented_state_diagnostics(graph: SemanticGraph) -> list[OssieDiagnostic]:
    """Fail closed for populated fields outside the core projection's contract.

    List handled fields rather than omitted ones so new native options cannot
    silently become portable. Existing projection checks still validate the
    supported combinations of these fields.
    """
    diagnostics: list[OssieDiagnostic] = []

    def check(item: BaseModel, handled: set[str], location: str) -> None:
        omitted = [
            name
            for name, field in type(item).model_fields.items()
            if name not in handled and getattr(item, name) != field.get_default(call_default_factory=True)
        ]
        if omitted:
            diagnostics.append(
                _error(
                    "ossie.synthesis.native_state_unrepresented",
                    f"{location} has native state not represented in Ossie core: {', '.join(omitted)}.",
                )
            )

    for model in graph.models.values():
        # Lowering adds these source-location annotations to every core model.
        # They describe the input document, not native state to synthesize.
        source_annotations = (
            {"metadata"} if not set(model.metadata or {}) - {"ossie_source_kind", "ossie_pointer"} else set()
        )
        check(
            model,
            {
                "name",
                "table",
                "sql",
                "description",
                "primary_key",
                "unique_keys",
                "dimensions",
                "metrics",
                "relationships",
            }
            | source_annotations,
            f"Model {model.name!r}",
        )
        for dimension in model.dimensions:
            check(
                dimension,
                {"name", "type", "sql", "logical_data_type", "declared_is_time", "description", "label", "public"},
                f"Field {model.name}.{dimension.name}",
            )
        for relationship in model.relationships:
            check(
                relationship,
                {"name", "edge_id", "type", "foreign_key", "primary_key", "target_model", "sql", "active"},
                f"Relationship {model.name}.{relationship.name}",
            )
    for metric in [*graph.metrics.values(), *(metric for model in graph.models.values() for metric in model.metrics)]:
        check(
            metric,
            {
                "name",
                "type",
                "agg",
                "sql",
                "sql_is_complete",
                "numerator",
                "denominator",
                "filters",
                "fill_nulls_with",
                "logical_data_type",
                "description",
                "public",
            },
            f"Metric {metric.name!r}",
        )
    for name in ("parameters", "table_calculations", "explores", "saved_queries", "metadata", "import_warnings"):
        if getattr(graph, name):
            diagnostics.append(
                _error("ossie.synthesis.native_state_unrepresented", f"Graph {name} is not represented in Ossie core.")
            )
    return diagnostics


def _runtime_expression_diagnostics(graph: SemanticGraph, dialect: str) -> list[OssieDiagnostic]:
    """Keep native modifiers from masking otherwise invalid SQL declarations."""
    diagnostics: list[OssieDiagnostic] = []
    expressions: list[tuple[str, str]] = []
    for model in graph.models.values():
        for dimension in model.dimensions:
            if dimension.has_untranslated_dax:
                diagnostics.append(
                    _error(
                        "ossie.synthesis.expression_language_unsupported",
                        f"Field {model.name}.{dimension.name} contains untranslated DAX.",
                    )
                )
            expressions.append((f"{model.name}.{dimension.name}", dimension.sql_expr))
            if dimension.window:
                expressions.append((f"{model.name}.{dimension.name} window", dimension.window))
        expressions.extend((f"{model.name}.{segment.name}", segment.sql) for segment in model.segments)
    metrics = [*graph.metrics.values(), *(metric for model in graph.models.values() for metric in model.metrics)]
    for metric in metrics:
        if metric.has_untranslated_dax:
            diagnostics.append(
                _error(
                    "ossie.synthesis.expression_language_unsupported",
                    f"Metric {metric.name!r} contains untranslated DAX.",
                )
            )
        if metric.sql:
            expressions.append((metric.name, metric.sql))
        expressions.extend((metric.name, item) for item in metric.filters or ())
    for name, expression in expressions:
        # Parameter templates need native request context. The extension keeps
        # them verbatim; it never renders them using the exporting user's data.
        if "{{" in expression or "{%" in expression:
            continue
        error = _scalar_expression_error(expression.replace("{model}", "runtime_model"), dialect)
        if error is not None:
            diagnostics.append(
                _error("ossie.synthesis.expression_invalid", f"Expression for {name!r} is invalid: {error}")
            )
    return diagnostics


def _dataset(model: Model, dialect: str, index: int, diagnostics: list[OssieDiagnostic]) -> dict[str, object] | None:
    pointer = f"/semantic_model/0/datasets/{index}"
    if (
        model.extends
        or model.invariant_filters
        or model.default_time_dimension
        or model.schema_exposure
        or (model.security and (model.security.access is not True or model.security.row_filters))
    ):
        diagnostics.append(
            _error(
                "ossie.synthesis.model_semantics_unsupported",
                f"Model {model.name!r} requires Sidemantic inheritance, row scope, time defaults, or security semantics.",
                pointer,
            )
        )
        return None
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
        if not dimension.public:
            diagnostics.append(
                _error(
                    "ossie.synthesis.field_semantics_unsupported",
                    f"Field {model.name}.{dimension.name} is private; Ossie synthesis cannot preserve its access restriction.",
                    field_pointer,
                )
            )
            continue
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
            if relationship.target_model is not None:
                diagnostics.append(
                    _error(
                        "ossie.synthesis.relationship_semantics_unsupported",
                        f"Relationship role {relationship.name!r} targets {relationship.target_model!r}; Ossie synthesis cannot preserve its distinct query instance.",
                        pointer,
                    )
                )
                continue
            if relationship.sql is not None or not relationship.active:
                diagnostics.append(
                    _error(
                        "ossie.synthesis.relationship_semantics_unsupported",
                        f"Relationship from {from_model.name!r} to {relationship.name!r} has custom SQL or is inactive; Ossie key equality cannot preserve that behavior.",
                        pointer,
                    )
                )
                continue
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
            target_keys = (
                {frozenset(normalize_identifier(column) for column in target.primary_key_columns)}
                if target.primary_key_columns
                else set()
            )
            target_keys.update(
                frozenset(normalize_identifier(column) for column in key) for key in target.unique_keys or ()
            )
            if frozenset(normalize_identifier(column) for column in to_columns) not in target_keys:
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
    generated_fields: dict[str, dict[str, str]],
) -> list[dict[str, object]]:
    candidates: list[tuple[Metric, str | None, bool]] = [
        (metric, graph.metric_owners.get(name), False) for name, metric in graph.metrics.items()
    ]
    candidates.extend((metric, model.name, True) for model in models.values() for metric in model.metrics)
    result: list[dict[str, object]] = []
    seen: dict[str, str] = {}
    for metric, owner, model_local in candidates:
        unsupported = _unsupported_metric_options(metric)
        if unsupported or metric.has_untranslated_dax:
            diagnostics.append(
                _error(
                    "ossie.synthesis.metric_semantics_unsupported",
                    f"Metric {metric.name!r} has unsupported runtime semantics: "
                    + (", ".join(unsupported) if unsupported else "untranslated DAX")
                    + ".",
                )
            )
            continue
        expression = _metric_expression(metric)
        if expression is None:
            diagnostics.append(
                _error(
                    "ossie.synthesis.metric_unrepresentable",
                    f"Metric {metric.name!r} cannot be represented as one Ossie SQL expression.",
                )
            )
            continue
        if owner:
            expression = expression.replace("{model}", owner)
        if metric.fill_nulls_with is not None:
            value = exp.convert(metric.fill_nulls_with).sql(dialect=_SQLGLOT_DIALECTS[dialect])
            expression = f"COALESCE({expression}, {value})"
        expression_error = _scalar_expression_error(expression, dialect)
        if expression_error is not None:
            diagnostics.append(
                _error(
                    "ossie.synthesis.expression_invalid",
                    f"Metric {metric.name!r} is not one {dialect} SQL expression: {expression_error}",
                )
            )
            continue
        expression = _qualify_metric_expression(
            expression, metric, owner, graph, dialect, generated_fields, model_local
        )
        if expression is None:
            diagnostics.append(
                _error(
                    "ossie.synthesis.metric_owner_unrepresentable",
                    f"Metric {metric.name!r} depends on rows of {owner!r} without a column reference; Ossie cannot preserve its dataset binding.",
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
    portable_only: bool = False,
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
    generated_fields: dict[str, dict[str, str]] = {}
    metrics = _metrics(graph, models, dialect, diagnostics, generated_fields)
    for dataset in datasets:
        for field_name, expression in generated_fields.get(dataset["name"], {}).items():
            dataset.setdefault("fields", []).append(
                {"name": field_name, "expression": _expression(expression, dialect)}
            )
    if relationships:
        semantic_model["relationships"] = relationships
    if metrics:
        semantic_model["metrics"] = metrics
    data = {"version": schema_version, "semantic_model": [semantic_model]}

    diagnostics.extend(_unrepresented_state_diagnostics(graph))
    extension_codes = {
        "ossie.synthesis.native_state_unrepresented",
        "ossie.synthesis.model_semantics_unsupported",
        "ossie.synthesis.field_semantics_unsupported",
        "ossie.synthesis.metric_semantics_unsupported",
        "ossie.synthesis.metric_unrepresentable",
        "ossie.synthesis.metric_owner_unrepresentable",
        "ossie.synthesis.metric_name_collision",
        "ossie.synthesis.relationship_semantics_unsupported",
        "ossie.synthesis.relationship_cardinality_unsupported",
        "ossie.synthesis.relationship_identity_missing",
    }
    if diagnostics and not portable_only and all(item.code in extension_codes for item in diagnostics):
        expression_diagnostics = _runtime_expression_diagnostics(graph, dialect)
        if expression_diagnostics:
            return OssieSynthesisResult(document=None, diagnostics=tuple(expression_diagnostics))
        # Core-only consumers must not see an unrestricted source or a metric
        # with silently simplified behavior. The authoritative graph lives in
        # the vendor extension; the required core dataset is an empty sentinel.
        semantic_model = {
            "name": scope_name,
            "description": "Requires the Sidemantic runtime extension; core datasets contain no business data.",
            "datasets": [
                {
                    "name": "sidemantic_runtime_required",
                    "source": "SELECT NULL AS runtime_extension_required WHERE 1 = 0",
                }
            ],
        }
        try:
            extension = encode_runtime_extension(graph, semantic_model, expression_dialect=dialect)
        except (TypeError, ValueError) as exc:
            diagnostics.append(_error("ossie.synthesis.runtime_extension_invalid", str(exc)))
        else:
            semantic_model["custom_extensions"] = [extension]
            data = {"version": schema_version, "vendors": ["SIDEMANTIC"], "semantic_model": [semantic_model]}
            diagnostics = [
                _warning(
                    "ossie.synthesis.runtime_extension_required",
                    "This document requires Sidemantic runtime extension v1 to execute. "
                    "Other consumers see only an empty dataset; use portable_only=True to require Ossie core output.",
                )
            ]

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
