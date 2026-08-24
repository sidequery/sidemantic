"""Bounded Malloy query/view mapping into native consumption contracts.

Integration contract
--------------------
``MalloyAdapter`` already retains each top-level query as a
``MalloyNamedStatement`` value containing ``(name, raw_definition)``.  An
adapter integration may pass those two values to :func:`map_malloy_query` and,
when ``mapping.supported`` is true, add ``mapping.explore`` and
``mapping.saved_query`` to the graph.  A source-local Malloy view can use
:func:`map_malloy_view` with its owning source name.

This module deliberately implements only one query stage.  It does not invent
new query algebra: pipelines, refinements, nesting, calculations, query-local
joins/extensions, sampling, indexes, timezones, and output aliases which differ
from their source field are rejected with structured diagnostics.
"""

from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass
from typing import Any, Literal

from antlr4 import CommonTokenStream, InputStream
from antlr4.error.ErrorListener import ErrorListener
from sqlglot import exp, parse_one

from sidemantic.adapters.malloy_grammar import MalloyLexer, MalloyParser
from sidemantic.core.consumption import Explore, SavedQuery


@dataclass(frozen=True)
class MalloyQueryDiagnostic:
    """One stable, machine-readable query mapping diagnostic."""

    code: str
    message: str
    status: Literal["rejected"] = "rejected"


@dataclass(frozen=True)
class MalloyQueryMapping:
    """Result of mapping one Malloy query or view."""

    explore: Explore | None
    saved_query: SavedQuery | None
    diagnostics: tuple[MalloyQueryDiagnostic, ...] = ()

    @property
    def supported(self) -> bool:
        return self.explore is not None and self.saved_query is not None and not self.diagnostics


class _SyntaxErrors(ErrorListener):
    def __init__(self) -> None:
        self.messages: list[str] = []

    def syntaxError(self, _recognizer, _symbol, line, column, msg, _error):  # noqa: N802
        self.messages.append(f"{line}:{column}: {msg}")


def _source_text(ctx, stream: CommonTokenStream) -> str:
    if ctx is None or ctx.start is None or ctx.stop is None:
        return ""
    return stream.tokenSource.inputStream.getText(ctx.start.start, ctx.stop.stop)


def _walk(ctx):
    yield ctx
    for child in ctx.getChildren():
        if hasattr(child, "getChildren"):
            yield from _walk(child)


def _reject(code: str, message: str) -> MalloyQueryMapping:
    return MalloyQueryMapping(None, None, (MalloyQueryDiagnostic(code, message),))


def _field_path(entry, stream: CommonTokenStream) -> tuple[str | None, MalloyQueryDiagnostic | None]:
    tagged = entry.taggedRef()
    if tagged is not None:
        if tagged.refExpr() is not None:
            return None, MalloyQueryDiagnostic(
                "malloy_query_field_expression_unsupported",
                f"query field expression '{_source_text(tagged, stream)}' requires Malloy field semantics",
            )
        return _source_text(tagged.fieldPath(), stream), None

    definition = entry.fieldDef()
    if definition is None:
        return None, MalloyQueryDiagnostic(
            "malloy_query_field_expression_unsupported", "query field is not a direct semantic field reference"
        )
    alias = _source_text(definition.fieldNameDef(), stream)
    expression = definition.fieldExpr()
    if not isinstance(expression, MalloyParser.ExprFieldPathContext):
        return None, MalloyQueryDiagnostic(
            "malloy_query_field_expression_unsupported",
            f"query field '{alias}' is defined by an expression which has no native consumption representation",
        )
    source = _source_text(expression.fieldPath(), stream)
    if alias != source.rsplit(".", 1)[-1]:
        return None, MalloyQueryDiagnostic(
            "malloy_query_rename_unsupported",
            f"query output rename '{alias} is {source}' cannot be preserved by SavedQuery",
        )
    return source, None


def _query_fields(field_list, stream: CommonTokenStream) -> tuple[list[str], list[MalloyQueryDiagnostic]]:
    fields: list[str] = []
    diagnostics: list[MalloyQueryDiagnostic] = []
    for entry in field_list.queryFieldEntry():
        field, diagnostic = _field_path(entry, stream)
        if diagnostic is not None:
            diagnostics.append(diagnostic)
        elif field is not None:
            fields.append(field)
    return fields, diagnostics


def _field_name(field: str) -> str:
    """Return the semantic member name from an optionally-qualified path."""
    return field.rsplit(".", 1)[-1]


def _known_paths(fields: Collection[str]) -> set[str]:
    return set(fields)


def _known_matches(field: str, known: set[str]) -> set[str]:
    """Resolve a reference without collapsing distinct qualified paths."""
    if field in known:
        return {field}
    leaf = _field_name(field)
    if "." in field:
        # A qualified query reference carries semantic path identity and therefore
        # must match an equally-qualified known field exactly.
        return set()
    return {candidate for candidate in known if _field_name(candidate) == leaf}


def _column_path(column: exp.Column) -> str:
    return ".".join(part.name for part in column.parts)


def _typed_known_matches(
    field: str, known_dimensions: set[str], known_metrics: set[str]
) -> set[tuple[Literal["dimension", "metric"], str]]:
    """Resolve one reference to typed semantic identities across both namespaces."""
    matches: set[tuple[Literal["dimension", "metric"], str]] = {
        ("dimension", path) for path in _known_matches(field, known_dimensions)
    }
    matches.update(("metric", path) for path in _known_matches(field, known_metrics))
    return matches


def _validate_query_fields(
    fields: list[str],
    *,
    role: Literal["group_by", "aggregate"],
    known_dimensions: set[str],
    known_metrics: set[str],
) -> list[MalloyQueryDiagnostic]:
    diagnostics: list[MalloyQueryDiagnostic] = []
    expected = known_dimensions if role == "group_by" else known_metrics
    swapped = known_metrics if role == "group_by" else known_dimensions
    for field in fields:
        expected_matches = _known_matches(field, expected)
        swapped_matches = _known_matches(field, swapped)
        if len(expected_matches) == 1:
            continue
        if len(expected_matches) > 1:
            diagnostics.append(
                MalloyQueryDiagnostic(
                    f"malloy_query_{role}_field_ambiguous",
                    f"{role} field '{field}' matches multiple qualified semantic fields",
                )
            )
        elif swapped_matches:
            diagnostics.append(
                MalloyQueryDiagnostic(
                    f"malloy_query_{role}_wrong_role",
                    f"{role} field '{field}' is a {'metric' if role == 'group_by' else 'dimension'}",
                )
            )
        else:
            diagnostics.append(
                MalloyQueryDiagnostic(
                    f"malloy_query_{role}_field_unknown",
                    f"{role} field '{field}' is not present in the source semantic schema",
                )
            )
    return diagnostics


def _duplicate_field_diagnostics(
    fields: list[str],
    role: Literal["group_by", "aggregate"],
    known: set[str],
) -> list[MalloyQueryDiagnostic]:
    diagnostics: list[MalloyQueryDiagnostic] = []
    seen: set[str] = set()
    for field in fields:
        matches = _known_matches(field, known)
        identity = next(iter(matches)) if len(matches) == 1 else field
        if identity in seen:
            diagnostics.append(
                MalloyQueryDiagnostic(
                    f"malloy_query_duplicate_{role}",
                    f"{role} field '{field}' is selected more than once",
                )
            )
        seen.add(identity)
    return diagnostics


def _filters(statement, stream: CommonTokenStream) -> tuple[list[str], list[MalloyQueryDiagnostic]]:
    result: list[str] = []
    diagnostics: list[MalloyQueryDiagnostic] = []
    clauses = statement.filterClauseList().fieldExpr()
    for clause in clauses:
        text = _source_text(clause, stream)
        unsupported = next(
            (
                node
                for node in _walk(clause)
                if isinstance(
                    node,
                    (
                        MalloyParser.ExprGivenRefContext,
                        MalloyParser.ExprTimeTruncContext,
                        MalloyParser.ExprDurationContext,
                        MalloyParser.ExprRangeContext,
                        MalloyParser.ExprForRangeContext,
                        MalloyParser.ExprApplyContext,
                        MalloyParser.ExprUngroupContext,
                    ),
                )
            ),
            None,
        )
        if unsupported is not None:
            diagnostics.append(
                MalloyQueryDiagnostic(
                    "malloy_query_filter_expression_unsupported",
                    f"filter expression '{text}' requires Malloy-only expression semantics",
                )
            )
            continue
        try:
            parse_one(text)
        except Exception:
            diagnostics.append(
                MalloyQueryDiagnostic(
                    "malloy_query_filter_expression_unsupported",
                    f"filter expression '{text}' is not accepted by the native SQL expression parser",
                )
            )
        else:
            result.append(text)
    return result, diagnostics


def map_malloy_query(
    name: str,
    raw_definition: str,
    *,
    declared_name: str | None = None,
    source_model_override: str | None = None,
    dimensions: Collection[str] = (),
    metrics: Collection[str] = (),
    parameters: dict[str, Any] | None = None,
) -> MalloyQueryMapping:
    """Map one retained top-level Malloy query into ``Explore`` + ``SavedQuery``.

    ``raw_definition`` is the exact value retained by ``MalloyNamedStatement``
    (for example ``q is orders -> { group_by: state }``); accepting an entire
    ``query:`` statement as a convenience makes the function independently
    testable. ``dimensions`` and ``metrics`` are required only to classify a
    ``select:`` projection. Caller-provided ``parameters`` are preserved in the
    native SavedQuery contract; Malloy source arguments are rejected because
    they do not have the same runtime semantics.
    """

    document = raw_definition.strip()
    if not document.lower().startswith("query:"):
        document = f"query: {document}"
    error_listener = _SyntaxErrors()
    lexer = MalloyLexer(InputStream(document))
    lexer.removeErrorListeners()
    lexer.addErrorListener(error_listener)
    stream = CommonTokenStream(lexer)
    parser = MalloyParser(stream)
    parser.removeErrorListeners()
    parser.addErrorListener(error_listener)
    tree = parser.malloyDocument()
    if error_listener.messages:
        return _reject("malloy_query_syntax_error", "; ".join(error_listener.messages))

    definitions = [node for node in _walk(tree) if isinstance(node, MalloyParser.TopLevelQueryDefContext)]
    if len(definitions) != 1:
        return _reject("malloy_query_definition_invalid", "expected exactly one top-level Malloy query definition")
    definition = definitions[0]
    parsed_name = _source_text(definition.queryName(), stream)
    expected_declared_name = declared_name or name
    if parsed_name != expected_declared_name:
        return _reject(
            "malloy_query_name_mismatch",
            f"retained query name '{expected_declared_name}' does not match definition name '{parsed_name}'",
        )

    expression = definition.sqExpr()
    if not isinstance(expression, MalloyParser.SQArrowContext):
        if isinstance(expression, MalloyParser.SQRefinedQueryContext):
            return _reject("malloy_query_refinement_unsupported", "Malloy query refinements require new query algebra")
        return _reject("malloy_query_stage_required", "query must be a source followed by one query-properties stage")
    if isinstance(expression.sqExpr(), MalloyParser.SQArrowContext):
        return _reject("malloy_query_pipeline_unsupported", "multi-stage Malloy query pipelines are not representable")
    source = expression.sqExpr()
    if not isinstance(source, MalloyParser.SQIDContext):
        return _reject("malloy_query_source_unsupported", "query source must be a direct named source")
    if source.sourceArguments() is not None:
        return _reject(
            "malloy_query_source_arguments_unsupported",
            "Malloy source arguments do not match SavedQuery runtime parameter semantics",
        )
    source_name = _source_text(source.id_(), stream)
    native_source_name = source_model_override or source_name
    segment = expression.segExpr()
    if not isinstance(segment, MalloyParser.SegOpsContext):
        return _reject("malloy_query_named_view_unsupported", "named-view references require module resolution")

    selected_dimensions: list[str] = []
    selected_metrics: list[str] = []
    where_filters: list[str] = []
    having_filters: list[str] = []
    order_by: list[str] = []
    limit: int | None = None
    diagnostics: list[MalloyQueryDiagnostic] = []
    known_dimensions = _known_paths(dimensions)
    known_metrics = _known_paths(metrics)

    for statement in segment.queryProperties().queryStatement():
        if statement.groupByStatement() is not None:
            fields, errors = _query_fields(statement.groupByStatement().queryFieldList(), stream)
            selected_dimensions.extend(fields)
            diagnostics.extend(errors)
            diagnostics.extend(
                _validate_query_fields(
                    fields,
                    role="group_by",
                    known_dimensions=known_dimensions,
                    known_metrics=known_metrics,
                )
            )
        elif statement.aggregateStatement() is not None:
            fields, errors = _query_fields(statement.aggregateStatement().queryFieldList(), stream)
            selected_metrics.extend(fields)
            diagnostics.extend(errors)
            diagnostics.extend(
                _validate_query_fields(
                    fields,
                    role="aggregate",
                    known_dimensions=known_dimensions,
                    known_metrics=known_metrics,
                )
            )
        elif statement.projectStatement() is not None:
            collection = statement.projectStatement().fieldCollection()
            for member in collection.collectionMember():
                if member.collectionWildCard() is not None:
                    diagnostics.append(
                        MalloyQueryDiagnostic(
                            "malloy_query_select_wildcard_unsupported",
                            "select wildcards require a resolved source schema",
                        )
                    )
                    continue
                field, error = _field_path(member, stream)
                if error is not None:
                    diagnostics.append(error)
                else:
                    matches = _typed_known_matches(field, known_dimensions, known_metrics)
                    if len(matches) == 1:
                        role, _ = next(iter(matches))
                        if role == "dimension":
                            selected_dimensions.append(field)
                        else:
                            selected_metrics.append(field)
                    else:
                        diagnostics.append(
                            MalloyQueryDiagnostic(
                                "malloy_query_select_field_ambiguous",
                                f"select field '{field}' does not resolve to exactly one dimension or metric",
                            )
                        )
        elif statement.whereStatement() is not None:
            values, errors = _filters(statement.whereStatement(), stream)
            where_filters.extend(values)
            diagnostics.extend(errors)
        elif statement.havingStatement() is not None:
            values, errors = _filters(statement.havingStatement(), stream)
            having_filters.extend(values)
            diagnostics.extend(errors)
        elif statement.orderByStatement() is not None:
            for spec in statement.orderByStatement().ordering().orderBySpec():
                if spec.INTEGER_LITERAL() is not None:
                    diagnostics.append(
                        MalloyQueryDiagnostic(
                            "malloy_query_ordinal_order_unsupported",
                            "ordinal order_by depends on Malloy projection order",
                        )
                    )
                else:
                    order_by.append(_source_text(spec, stream))
        elif statement.limitStatement() is not None:
            value = int(statement.limitStatement().INTEGER_LITERAL().getText())
            if limit is not None:
                diagnostics.append(
                    MalloyQueryDiagnostic("malloy_query_duplicate_limit", "multiple limit statements are ambiguous")
                )
            limit = value
        elif statement.queryAnnotation() is not None or statement.ignoredModelAnnotations() is not None:
            continue
        else:
            feature = _source_text(statement, stream).split(":", 1)[0].strip().lower()
            diagnostics.append(
                MalloyQueryDiagnostic(
                    f"malloy_query_{feature or 'operation'}_unsupported",
                    f"Malloy query operation '{feature}' requires unsupported query algebra",
                )
            )

    diagnostics.extend(_duplicate_field_diagnostics(selected_dimensions, "group_by", known_dimensions))
    diagnostics.extend(_duplicate_field_diagnostics(selected_metrics, "aggregate", known_metrics))
    selected_dimension_paths = {
        path for field in selected_dimensions for path in _known_matches(field, known_dimensions)
    }
    selected_metric_paths = {path for field in selected_metrics for path in _known_matches(field, known_metrics)}
    for expression in where_filters:
        references = {_column_path(column) for column in parse_one(expression).find_all(exp.Column)}
        resolved = {
            reference: _typed_known_matches(reference, known_dimensions, known_metrics) for reference in references
        }
        unknown = {reference for reference, matches in resolved.items() if not matches}
        ambiguous = {reference for reference, matches in resolved.items() if len(matches) > 1}
        if unknown:
            diagnostics.append(
                MalloyQueryDiagnostic(
                    "malloy_query_filter_field_unknown",
                    f"where expression '{expression}' references unknown field(s): {', '.join(sorted(unknown))}",
                )
            )
        if ambiguous:
            diagnostics.append(
                MalloyQueryDiagnostic(
                    "malloy_query_filter_field_ambiguous",
                    f"where expression '{expression}' has ambiguous field(s): {', '.join(sorted(ambiguous))}",
                )
            )
        has_unique_metric = any(
            len(matches) == 1 and next(iter(matches))[0] == "metric" for matches in resolved.values()
        )
        if has_unique_metric:
            # Exactly one resolved metric identity means the native compiler would
            # classify this expression as HAVING. Ambiguous references were already
            # rejected above and must not influence clause classification.
            diagnostics.append(
                MalloyQueryDiagnostic(
                    "malloy_query_metric_where_unsupported",
                    f"where expression '{expression}' references an aggregate and would become HAVING natively",
                )
            )
    for expression in having_filters:
        references = {_column_path(column) for column in parse_one(expression).find_all(exp.Column)}
        resolved = {
            reference: _typed_known_matches(reference, known_dimensions, known_metrics) for reference in references
        }
        unknown = {reference for reference, matches in resolved.items() if not matches}
        ambiguous = {reference for reference, matches in resolved.items() if len(matches) > 1}
        if unknown:
            diagnostics.append(
                MalloyQueryDiagnostic(
                    "malloy_query_filter_field_unknown",
                    f"having expression '{expression}' references unknown field(s): {', '.join(sorted(unknown))}",
                )
            )
        if ambiguous:
            diagnostics.append(
                MalloyQueryDiagnostic(
                    "malloy_query_filter_field_ambiguous",
                    f"having expression '{expression}' has ambiguous field(s): {', '.join(sorted(ambiguous))}",
                )
            )
        has_unique_metric = any(
            len(matches) == 1 and next(iter(matches))[0] == "metric" for matches in resolved.values()
        )
        if not has_unique_metric and not unknown and not ambiguous:
            diagnostics.append(
                MalloyQueryDiagnostic(
                    "malloy_query_dimension_having_unsupported",
                    f"having expression '{expression}' has no selected metric and cannot be forced into HAVING natively",
                )
            )
    for ordering in order_by:
        ordered_field = ordering.split()[0]
        matches = _typed_known_matches(ordered_field, selected_dimension_paths, selected_metric_paths)
        if not matches:
            diagnostics.append(
                MalloyQueryDiagnostic(
                    "malloy_query_order_field_unselected",
                    f"order_by field '{ordered_field}' is not selected by the query",
                )
            )
        elif len(matches) > 1:
            diagnostics.append(
                MalloyQueryDiagnostic(
                    "malloy_query_order_field_ambiguous",
                    f"order_by field '{ordered_field}' matches multiple selected semantic fields",
                )
            )
    if not selected_dimensions and not selected_metrics:
        diagnostics.append(
            MalloyQueryDiagnostic(
                "malloy_query_selection_required",
                "native SavedQuery requires at least one selected dimension or metric",
            )
        )

    if diagnostics:
        return MalloyQueryMapping(None, None, tuple(diagnostics))

    explore_name = f"__malloy_{name}"
    metadata = {"malloy": {"kind": "single_stage_query", "source": source_name}}
    explore = Explore(name=explore_name, model=native_source_name, metadata=metadata)
    saved_query = SavedQuery(
        name=name,
        explore=explore_name,
        dimensions=selected_dimensions,
        metrics=selected_metrics,
        filters=where_filters + having_filters,
        order_by=order_by,
        limit=limit,
        parameters=parameters,
        metadata=metadata,
    )
    return MalloyQueryMapping(explore, saved_query)


def map_malloy_view(
    name: str,
    source_model: str,
    raw_view: str,
    *,
    dimensions: Collection[str] = (),
    metrics: Collection[str] = (),
    parameters: dict[str, Any] | None = None,
) -> MalloyQueryMapping:
    """Map one source-local, single-stage Malloy view body.

    ``raw_view`` must be a query-properties block (``{ ... }``). Pipelines and
    named/refined view expressions must go through module resolution first.
    """

    body = raw_view.strip()
    if not (body.startswith("{") and body.endswith("}")):
        return _reject("malloy_view_shape_unsupported", "view must contain exactly one query-properties block")
    return map_malloy_query(
        name,
        f"{name} is {source_model} -> {body}",
        dimensions=dimensions,
        metrics=metrics,
        parameters=parameters,
    )
