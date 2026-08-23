"""Loss-aware lowering from Apache Ossie documents into executable scopes."""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

import sqlglot
from sqlglot import expressions as exp

from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.registry import reset_current_layer, set_current_layer
from sidemantic.core.relationship import Relationship
from sidemantic.core.semantic_catalog import CompiledSemanticScope, SemanticCatalog
from sidemantic.core.semantic_graph import SemanticGraph
from sidemantic.interchange.ossie.diagnostics import (
    OssieDiagnostic,
    OssieDiagnosticSeverity,
    OssieSourceLocation,
    sort_diagnostics,
)
from sidemantic.interchange.ossie.documents import OssieLogicalDocument, OssieOntologyDocument
from sidemantic.interchange.ossie.expression_validation import scalar_sql_expression_error
from sidemantic.interchange.ossie.identifier import identifier_within_limit, normalize_identifier
from sidemantic.interchange.ossie.parser import OssieParseResult
from sidemantic.interchange.ossie.profiles import OssieImportPolicy
from sidemantic.interchange.ossie.semantic_validation import (
    SemanticValidationResult,
    validate_ossie_semantics,
)
from sidemantic.interchange.ossie.validation import SchemaValidationResult, validate_ossie_schema

JSONObject = Mapping[str, object]
_TEMPORAL_TYPES = frozenset({"Date", "Time", "DateTime", "DateTimeTz"})
_NUMERIC_TYPES = frozenset({"Integer", "Decimal", "Float"})
_DIALECT_LABELS = {
    "bigquery": "BIGQUERY",
    "databricks": "DATABRICKS",
    "snowflake": "SNOWFLAKE",
}
_SQLGLOT_DIALECTS = {
    "bigquery": "bigquery",
    "databricks": "databricks",
    "duckdb": "duckdb",
    "postgres": "postgres",
    "postgresql": "postgres",
    "snowflake": "snowflake",
    "spark": "spark",
}
_LOWERING_IMPLEMENTATION = "sidemantic-ossie-lowering-v1"


@dataclass(frozen=True, slots=True)
class OssieLoweringResult:
    """A preserved source document plus any safely executable scope projections."""

    parse_result: OssieParseResult
    catalog: SemanticCatalog
    schema_validation: SchemaValidationResult | None
    semantic_validation: SemanticValidationResult | None
    lowering_diagnostics: tuple[OssieDiagnostic, ...]

    @property
    def document(self):
        return self.parse_result.document

    @property
    def diagnostics(self) -> tuple[OssieDiagnostic, ...]:
        schema = (
            self.schema_validation.diagnostics
            if self.schema_validation is not None and self.schema_validation is not self.parse_result.schema_validation
            else ()
        )
        semantic = self.semantic_validation.diagnostics if self.semantic_validation else ()
        return sort_diagnostics((*self.parse_result.diagnostics, *schema, *semantic, *self.lowering_diagnostics))

    @property
    def executable(self) -> bool:
        return len(self.catalog) > 0

    @property
    def valid(self) -> bool:
        return not any(diagnostic.severity is OssieDiagnosticSeverity.ERROR for diagnostic in self.diagnostics)


def _mapping(value: object) -> JSONObject | None:
    return value if isinstance(value, Mapping) else None


def _array(value: object) -> Sequence[object] | None:
    return value if isinstance(value, (list, tuple)) else None


def _name(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _source_location(result: OssieParseResult) -> OssieSourceLocation | None:
    source = result.document.source
    if source is None or source.identifier is None:
        return None
    return OssieSourceLocation(identifier=source.identifier)


def _diagnostic(
    result: OssieParseResult,
    *,
    code: str,
    message: str,
    pointer: str,
    scope: str | None = None,
) -> OssieDiagnostic:
    return OssieDiagnostic(
        severity=OssieDiagnosticSeverity.ERROR,
        code=code,
        message=message,
        json_pointer=pointer,
        source=_source_location(result),
        scope=scope,
        profile=result.profile,
    )


def _canonical_hash(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def _document_id(result: OssieParseResult) -> str:
    source = result.document.source
    if source and source.sha256:
        return f"sha256:{source.sha256}"
    return _canonical_hash(result.document.to_parsed_data())


def _normalize_dialect(value: str) -> str:
    return value.strip().lower().replace("-", "_")


def _expression_for_target(expression: object, target_dialect: str) -> tuple[str, str] | None:
    expression_object = _mapping(expression)
    variants = _array(expression_object.get("dialects")) if expression_object else None
    if not variants:
        return None

    by_dialect: dict[str, str] = {}
    for value in variants:
        variant = _mapping(value)
        if variant is None:
            continue
        dialect = variant.get("dialect")
        text = variant.get("expression")
        if isinstance(dialect, str) and dialect and isinstance(text, str) and text.strip():
            by_dialect.setdefault(dialect.upper(), text)

    normalized = _normalize_dialect(target_dialect)
    target_label = _DIALECT_LABELS.get(normalized)
    if target_label and target_label in by_dialect:
        return by_dialect[target_label], target_label
    if "ANSI_SQL" in by_dialect:
        return by_dialect["ANSI_SQL"], "ANSI_SQL"
    return None


def _sql_expression_error(expression: str, target_dialect: str) -> str | None:
    dialect = _SQLGLOT_DIALECTS.get(_normalize_dialect(target_dialect))
    if dialect is None:
        return f"target dialect {target_dialect!r} has no configured SQL parser"
    return scalar_sql_expression_error(expression, sqlglot_dialect=dialect)


def _classify_source(source: str, source_dialect: str | None) -> tuple[str, str] | None:
    dialect = _SQLGLOT_DIALECTS.get(_normalize_dialect(source_dialect)) if source_dialect else None
    try:
        parsed = sqlglot.parse_one(source, read=dialect)
    except sqlglot.errors.ParseError:
        parsed = None
    if isinstance(parsed, (exp.Query, exp.Subquery)):
        return "query", source

    try:
        sqlglot.parse_one(source, read=dialect, into=exp.Table)
    except sqlglot.errors.ParseError:
        return None
    return "table", source


def _runtime_dimension_type(logical_type: str | None, is_time: bool) -> str:
    if is_time:
        return "time"
    if logical_type == "Boolean":
        return "boolean"
    if logical_type in _NUMERIC_TYPES:
        return "numeric"
    return "categorical"


def _key_value(columns: Sequence[object] | None) -> str | list[str] | None:
    if not columns or not all(isinstance(column, str) and column for column in columns):
        return None
    values = list(columns)
    return values[0] if len(values) == 1 else values


def _canonical_name_lookup(names: Sequence[str]) -> dict[str, str]:
    """Map comparison keys to exact declarations, excluding ambiguous names."""

    counts = Counter(normalize_identifier(name) for name in names if identifier_within_limit(name))
    return {
        normalize_identifier(name): name
        for name in names
        if identifier_within_limit(name) and counts[normalize_identifier(name)] == 1
    }


def _canonical_columns(columns: Sequence[object] | None, declarations: Mapping[str, str]) -> list[str] | None:
    if not columns or not all(isinstance(column, str) and column for column in columns):
        return None
    resolved: list[str] = []
    seen: set[str] = set()
    for column in columns:
        if not identifier_within_limit(column):
            return None
        declaration = declarations.get(normalize_identifier(column))
        if declaration is None or declaration in seen:
            return None
        resolved.append(declaration)
        seen.add(declaration)
    return resolved


def _unique_named_items(values: Sequence[object] | None) -> list[tuple[int, JSONObject, str]]:
    entries: list[tuple[int, JSONObject, str]] = []
    names: list[str] = []
    for index, value in enumerate(values or ()):
        item = _mapping(value)
        item_name = _name(item.get("name")) if item else None
        if item is not None and item_name is not None:
            entries.append((index, item, item_name))
            names.append(item_name)
    counts = Counter(normalize_identifier(name) for name in names if identifier_within_limit(name))
    return [
        entry for entry in entries if identifier_within_limit(entry[2]) and counts[normalize_identifier(entry[2])] == 1
    ]


def _lower_scope(
    result: OssieParseResult,
    semantic_model: JSONObject,
    *,
    scope_id: str,
    scope_index: int,
    document_id: str,
    target_dialect: str,
    diagnostics: list[OssieDiagnostic],
    document_diagnostics: tuple[OssieDiagnostic, ...],
) -> CompiledSemanticScope:
    graph = SemanticGraph()
    source_dialect = result.options.source_dialect if result.options else None
    dataset_values = _array(semantic_model.get("datasets"))
    lowered_models: dict[str, Model] = {}

    # Model/Metric construction has a legacy auto-registration hook. Lowering
    # must be isolated from any ambient user layer.
    registration_token = set_current_layer(None)
    try:
        for dataset_index, dataset, dataset_name in _unique_named_items(dataset_values):
            pointer = f"/semantic_model/{scope_index}/datasets/{dataset_index}"
            source = dataset.get("source")
            if not isinstance(source, str) or not source.strip():
                diagnostics.append(
                    _diagnostic(
                        result,
                        code="ossie.lowering.source_unusable",
                        message=f"Dataset {dataset_name!r} has no executable source.",
                        pointer=f"{pointer}/source",
                        scope=scope_id,
                    )
                )
                continue
            classified_source = _classify_source(source, source_dialect)
            if classified_source is None:
                diagnostics.append(
                    _diagnostic(
                        result,
                        code="ossie.lowering.source_ambiguous",
                        message=f"Dataset source for {dataset_name!r} is neither a table reference nor a SQL query.",
                        pointer=f"{pointer}/source",
                        scope=scope_id,
                    )
                )
                continue

            dimensions: list[Dimension] = []
            fields = _array(dataset.get("fields"))
            for field_index, field, field_name in _unique_named_items(fields):
                field_pointer = f"{pointer}/fields/{field_index}"
                selected = _expression_for_target(field.get("expression"), target_dialect)
                if selected is None:
                    diagnostics.append(
                        _diagnostic(
                            result,
                            code="ossie.lowering.expression_unavailable",
                            message=(
                                f"Field {dataset_name}.{field_name} has no expression for "
                                f"target dialect {target_dialect!r} or ANSI_SQL."
                            ),
                            pointer=f"{field_pointer}/expression",
                            scope=scope_id,
                        )
                    )
                    continue
                expression, _ = selected
                expression_error = _sql_expression_error(expression, target_dialect)
                if expression_error is not None:
                    diagnostics.append(
                        _diagnostic(
                            result,
                            code="ossie.lowering.expression_invalid",
                            message=(
                                f"Field {dataset_name}.{field_name} is not one executable "
                                f"{target_dialect} SQL expression: {expression_error}"
                            ),
                            pointer=f"{field_pointer}/expression",
                            scope=scope_id,
                        )
                    )
                    continue
                logical_type = field.get("datatype") if isinstance(field.get("datatype"), str) else None
                dimension = _mapping(field.get("dimension"))
                declared_is_time = dimension.get("is_time") if dimension and "is_time" in dimension else None
                declared_is_time = declared_is_time if isinstance(declared_is_time, bool) else None
                effective_is_time = (
                    declared_is_time if declared_is_time is not None else logical_type in _TEMPORAL_TYPES
                )
                try:
                    runtime_dimension = Dimension(
                        name=field_name,
                        type=_runtime_dimension_type(logical_type, effective_is_time),
                        logical_data_type=logical_type,
                        declared_is_time=declared_is_time,
                        sql=expression,
                        description=field.get("description") if isinstance(field.get("description"), str) else None,
                        label=field.get("label") if isinstance(field.get("label"), str) else None,
                    )
                except (TypeError, ValueError) as exc:
                    diagnostics.append(
                        _diagnostic(
                            result,
                            code="ossie.lowering.field_unexecutable",
                            message=f"Field {dataset_name}.{field_name} cannot be represented safely: {exc}",
                            pointer=field_pointer,
                            scope=scope_id,
                        )
                    )
                    continue
                dimensions.append(runtime_dimension)

            field_declarations = _canonical_name_lookup([dimension.name for dimension in dimensions])
            primary_columns = _canonical_columns(_array(dataset.get("primary_key")), field_declarations)
            primary_key = _key_value(primary_columns)
            unique_keys: list[list[str]] = []
            for value in _array(dataset.get("unique_keys")) or ():
                resolved_key = _canonical_columns(_array(value), field_declarations)
                if resolved_key is not None:
                    unique_keys.append(resolved_key)
            source_kind, source_text = classified_source
            try:
                model = Model(
                    name=dataset_name,
                    table=source_text if source_kind == "table" else None,
                    sql=source_text if source_kind == "query" else None,
                    description=dataset.get("description") if isinstance(dataset.get("description"), str) else None,
                    primary_key=primary_key,
                    unique_keys=unique_keys or None,
                    dimensions=dimensions,
                    default_time_dimension=None,
                    metadata={"ossie_source_kind": source_kind, "ossie_pointer": pointer},
                )
            except (TypeError, ValueError) as exc:
                diagnostics.append(
                    _diagnostic(
                        result,
                        code="ossie.lowering.dataset_unexecutable",
                        message=f"Dataset {dataset_name!r} cannot be represented safely: {exc}",
                        pointer=pointer,
                        scope=scope_id,
                    )
                )
                continue
            graph.add_model(model)
            lowered_models[normalize_identifier(dataset_name)] = model

        relationships = _array(semantic_model.get("relationships"))
        for relationship_index, relationship, edge_id in _unique_named_items(relationships):
            pointer = f"/semantic_model/{scope_index}/relationships/{relationship_index}"
            from_name = _name(relationship.get("from"))
            to_name = _name(relationship.get("to"))
            from_columns = _array(relationship.get("from_columns"))
            to_columns = _array(relationship.get("to_columns"))
            from_model = (
                lowered_models.get(normalize_identifier(from_name))
                if from_name and identifier_within_limit(from_name)
                else None
            )
            to_model = (
                lowered_models.get(normalize_identifier(to_name))
                if to_name and identifier_within_limit(to_name)
                else None
            )
            from_declarations = (
                _canonical_name_lookup([dimension.name for dimension in from_model.dimensions]) if from_model else {}
            )
            to_declarations = (
                _canonical_name_lookup([dimension.name for dimension in to_model.dimensions]) if to_model else {}
            )
            canonical_from_columns = _canonical_columns(from_columns, from_declarations)
            canonical_to_columns = _canonical_columns(to_columns, to_declarations)
            from_key = _key_value(canonical_from_columns)
            to_key = _key_value(canonical_to_columns)
            target_keys = {tuple(to_model.primary_key_columns)} if to_model and to_model.primary_key_columns else set()
            if to_model:
                target_keys.update(tuple(key) for key in to_model.unique_keys or ())
            safe = (
                from_model is not None
                and to_model is not None
                and from_key is not None
                and to_key is not None
                and len(canonical_from_columns or ()) == len(canonical_to_columns or ())
                and tuple(canonical_to_columns or ()) in target_keys
                and all(from_model.get_dimension(column) is not None for column in canonical_from_columns or ())
                and all(to_model.get_dimension(column) is not None for column in canonical_to_columns or ())
            )
            if not safe:
                diagnostics.append(
                    _diagnostic(
                        result,
                        code="ossie.lowering.relationship_unsafe",
                        message=f"Relationship {edge_id!r} is preserved but excluded from executable topology.",
                        pointer=pointer,
                        scope=scope_id,
                    )
                )
                continue
            from_model.relationships.append(
                Relationship(
                    name=to_model.name,
                    edge_id=edge_id,
                    type="many_to_one",
                    foreign_key=from_key,
                    primary_key=to_key,
                )
            )

        metrics = _array(semantic_model.get("metrics"))
        for metric_index, metric, metric_name in _unique_named_items(metrics):
            pointer = f"/semantic_model/{scope_index}/metrics/{metric_index}"
            selected = _expression_for_target(metric.get("expression"), target_dialect)
            if selected is None:
                diagnostics.append(
                    _diagnostic(
                        result,
                        code="ossie.lowering.expression_unavailable",
                        message=(
                            f"Metric {metric_name!r} has no expression for target dialect "
                            f"{target_dialect!r} or ANSI_SQL."
                        ),
                        pointer=f"{pointer}/expression",
                        scope=scope_id,
                    )
                )
                continue
            expression, _ = selected
            expression_error = _sql_expression_error(expression, target_dialect)
            if expression_error is not None:
                diagnostics.append(
                    _diagnostic(
                        result,
                        code="ossie.lowering.expression_invalid",
                        message=(
                            f"Metric {metric_name!r} is not one executable {target_dialect} "
                            f"SQL expression: {expression_error}"
                        ),
                        pointer=f"{pointer}/expression",
                        scope=scope_id,
                    )
                )
                continue
            try:
                metric_object = Metric(
                    name=metric_name,
                    sql=expression,
                    logical_data_type=(metric.get("datatype") if isinstance(metric.get("datatype"), str) else None),
                    description=metric.get("description") if isinstance(metric.get("description"), str) else None,
                )
            except (TypeError, ValueError) as exc:
                diagnostics.append(
                    _diagnostic(
                        result,
                        code="ossie.lowering.metric_unexecutable",
                        message=f"Metric {metric_name!r} cannot be represented safely: {exc}",
                        pointer=pointer,
                        scope=scope_id,
                    )
                )
                continue
            graph.add_metric(metric_object)
    finally:
        reset_current_layer(registration_token)

    graph.build_adjacency()
    scope_data = semantic_model
    content_id = _canonical_hash(scope_data)
    compilation_id = _canonical_hash(
        {
            "implementation": _LOWERING_IMPLEMENTATION,
            "document_version": result.document.version,
            "consumer_profile": result.profile.identifier if result.profile else None,
            "schema_commit": result.schema_validation.schema_commit if result.schema_validation else None,
            "source_dialect": _normalize_dialect(source_dialect) if source_dialect else None,
            "target_dialect": _normalize_dialect(target_dialect),
            "lowering_policy": result.options.import_policy.value if result.options else "unknown",
        }
    )
    scope_diagnostics = sort_diagnostics(
        (
            *(diagnostic for diagnostic in document_diagnostics if diagnostic.scope in (None, scope_id)),
            *(diagnostic for diagnostic in diagnostics if diagnostic.scope == scope_id),
        )
    )
    scope_valid = not any(diagnostic.severity is OssieDiagnosticSeverity.ERROR for diagnostic in scope_diagnostics)
    return CompiledSemanticScope(
        scope_id=scope_id,
        document_id=document_id,
        content_id=content_id,
        compilation_id=compilation_id,
        runtime=graph,
        target_dialect=target_dialect,
        lowering_policy=result.options.import_policy.value if result.options else "unknown",
        valid=scope_valid,
        diagnostics=scope_diagnostics,
        provenance={
            "document_version": result.document.version,
            "semantic_model_index": scope_index,
            "source_identifier": result.document.source.identifier if result.document.source else None,
        },
    )


def lower_ossie_document(
    parse_result: OssieParseResult,
    *,
    target_dialect: str | None = None,
) -> OssieLoweringResult:
    """Validate and project each logical semantic model into an isolated scope."""

    if not isinstance(parse_result, OssieParseResult):
        raise TypeError("parse_result must be an OssieParseResult")

    document = parse_result.document
    schema_validation = parse_result.schema_validation
    if schema_validation is None and isinstance(document, (OssieLogicalDocument, OssieOntologyDocument)):
        schema_validation = validate_ossie_schema(
            document.to_parsed_data(),
            profile=parse_result.profile,
            consumer_profile=parse_result.parse_options.consumer_profile,
        )

    semantic_validation = None
    if isinstance(document, (OssieLogicalDocument, OssieOntologyDocument)):
        semantic_validation = validate_ossie_semantics(document, profile=parse_result.profile)

    diagnostics: list[OssieDiagnostic] = []
    selected_target = target_dialect or (parse_result.options.target_dialect if parse_result.options else None)
    if selected_target is None:
        diagnostics.append(
            _diagnostic(
                parse_result,
                code="ossie.lowering.target_dialect_required",
                message="Executable lowering requires an explicit target dialect.",
                pointer="",
            )
        )

    all_pre_lowering = [*parse_result.diagnostics]
    if schema_validation and schema_validation is not parse_result.schema_validation:
        all_pre_lowering.extend(schema_validation.diagnostics)
    if semantic_validation:
        all_pre_lowering.extend(semantic_validation.diagnostics)
    strict = parse_result.options is None or parse_result.options.import_policy is OssieImportPolicy.STRICT
    has_errors = any(diagnostic.severity is OssieDiagnosticSeverity.ERROR for diagnostic in all_pre_lowering)
    if (
        selected_target is None
        or not isinstance(document, OssieLogicalDocument)
        or parse_result.options is None
        or (strict and has_errors)
    ):
        return OssieLoweringResult(
            parse_result=parse_result,
            catalog=SemanticCatalog(),
            schema_validation=schema_validation,
            semantic_validation=semantic_validation,
            lowering_diagnostics=tuple(diagnostics),
        )

    parsed = document.to_parsed_data()
    root = _mapping(parsed)
    semantic_models = _array(root.get("semantic_model")) if root else None
    named_models = [
        (index, model, model_name)
        for index, value in enumerate(semantic_models or ())
        if (model := _mapping(value)) is not None
        if (model_name := _name(model.get("name"))) is not None
    ]
    name_counts = Counter(normalize_identifier(name) for _, _, name in named_models if identifier_within_limit(name))
    document_id = _document_id(parse_result)
    scopes = []
    for index, semantic_model, name in named_models:
        if not identifier_within_limit(name):
            continue
        scope_id = name if name_counts[normalize_identifier(name)] == 1 else f"{name}@{index}"
        scopes.append(
            _lower_scope(
                parse_result,
                semantic_model,
                scope_id=scope_id,
                scope_index=index,
                document_id=document_id,
                target_dialect=selected_target,
                diagnostics=diagnostics,
                document_diagnostics=tuple(all_pre_lowering),
            )
        )

    if strict and any(diagnostic.severity is OssieDiagnosticSeverity.ERROR for diagnostic in diagnostics):
        scopes = []

    return OssieLoweringResult(
        parse_result=parse_result,
        catalog=SemanticCatalog(scopes),
        schema_validation=schema_validation,
        semantic_validation=semantic_validation,
        lowering_diagnostics=sort_diagnostics(diagnostics),
    )
