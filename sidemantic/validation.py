"""Validation and error handling for semantic layer."""

from functools import lru_cache
from typing import TYPE_CHECKING, Literal, get_args, get_origin

from sidemantic.sql.aggregation_detection import sql_has_aggregate

if TYPE_CHECKING:
    from sidemantic.core.consumption import Explore, SavedQuery
    from sidemantic.core.metric import Metric
    from sidemantic.core.model import Model
    from sidemantic.core.semantic_graph import SemanticGraph


class ValidationError(Exception):
    """Raised when semantic layer validation fails."""

    pass


class QueryValidationError(ValidationError, ValueError):
    """Raised when query validation fails."""

    pass


class MetricValidationError(ValidationError):
    """Raised when metric validation fails."""

    pass


class ModelValidationError(ValidationError):
    """Raised when model validation fails."""

    pass


def validate_governance(value, label: str) -> tuple[list[str], list[str]]:
    """Validate cross-field governance lifecycle metadata."""
    errors: list[str] = []
    warnings: list[str] = []
    deprecation = getattr(value, "deprecation", None)
    status = getattr(value, "status", None)
    if status == "deprecated" and deprecation is None:
        errors.append(f"{label} is deprecated but has no deprecation lifecycle/message")
    if deprecation is not None:
        if not deprecation.message:
            warnings.append(f"{label} deprecation has no migration message")
        if deprecation.deprecated_at and deprecation.sunset_at and deprecation.sunset_at < deprecation.deprecated_at:
            errors.append(f"{label} deprecation sunset_at is before deprecated_at")
    tags = getattr(value, "tags", []) or []
    if len(tags) != len(set(tags)):
        errors.append(f"{label} has duplicate governance tags")
    return errors, warnings


def _qualified_consumption_refs(references: list[str], base_model: str) -> list[str]:
    return [ref if "." in ref or "(" in ref or " " in ref else f"{base_model}.{ref}" for ref in references]


def _qualified_consumption_metrics(references: list[str], base_model: str, graph: "SemanticGraph") -> list[str]:
    return [ref if ref in graph.metrics else _qualified_consumption_refs([ref], base_model)[0] for ref in references]


def _is_metric_or_dimension(reference: str, graph: "SemanticGraph") -> bool:
    return not validate_query([reference], [], graph) or not validate_query([], [reference], graph)


def _validate_consumption_expressions(
    label: str,
    field_kind: str,
    expressions: list[str],
    base_model: str,
    graph: "SemanticGraph",
    *,
    query_metrics: list[str] | None = None,
    query_dimensions: list[str] | None = None,
) -> list[str]:
    from sidemantic.core.consumption import expression_field_references

    try:
        references = expression_field_references(expressions, base_model, graph_metrics=graph.metrics.keys())
    except Exception as error:
        return [f"{label} contains an invalid {field_kind} expression: {error}"]
    errors = [
        f"{label} {field_kind} field '{reference}' is not a metric or dimension"
        for reference in sorted(references)
        if not _is_metric_or_dimension(reference, graph)
    ]
    if errors:
        return errors

    expression_metrics: list[str] = []
    expression_dimensions: list[str] = []
    for reference in sorted(references):
        if not validate_query([reference], [], graph):
            expression_metrics.append(reference)
        else:
            expression_dimensions.append(reference)
    compatibility_errors = validate_query(
        [*(query_metrics or []), *expression_metrics],
        [*(query_dimensions or []), *expression_dimensions],
        graph,
    )
    return [
        f"{label} {field_kind} expression is incompatible with its selected query: {error}"
        for error in compatibility_errors
    ]


def _validate_order_fields_selected(
    label: str,
    expressions: list[str],
    selected_metrics: list[str],
    selected_dimensions: list[str],
    base_model: str,
    graph: "SemanticGraph",
) -> list[str]:
    from sidemantic.core.consumption import expression_field_references

    try:
        references = expression_field_references(expressions, base_model, graph_metrics=graph.metrics.keys())
    except Exception:
        return []
    selected = {*selected_metrics, *selected_dimensions}
    outside = sorted(references - selected)
    if not outside:
        return []
    return [f"{label} ordering field(s) must be selected by the query: {', '.join(outside)}"]


def validate_explore(explore: "Explore", graph: "SemanticGraph") -> tuple[list[str], list[str]]:
    """Validate a curated Explore/View contract against the physical graph."""
    errors, warnings = validate_governance(explore, f"Explore '{explore.name}'")
    if explore.model not in graph.models:
        errors.append(f"Explore '{explore.name}' references model '{explore.model}' which doesn't exist")
        return errors, warnings
    dimensions = [*(explore.allowed_dimensions or []), *explore.default_dimensions]
    metrics = [*(explore.allowed_metrics or []), *explore.default_metrics]
    qualified_metrics = _qualified_consumption_metrics(metrics, explore.model, graph)
    qualified_dimensions = _qualified_consumption_refs(dimensions, explore.model)
    errors.extend(
        validate_query(
            qualified_metrics,
            qualified_dimensions,
            graph,
        )
    )
    for field_kind, references in (
        ("filter", explore.allowed_filter_fields or []),
        ("ordering", explore.allowed_order_by or []),
    ):
        for reference in _qualified_consumption_metrics(references, explore.model, graph):
            if not _is_metric_or_dimension(reference, graph):
                errors.append(f"Explore '{explore.name}' {field_kind} field '{reference}' is not a metric or dimension")
    errors.extend(
        _validate_consumption_expressions(
            f"Explore '{explore.name}'",
            "filter",
            [*explore.filters, *explore.default_filters],
            explore.model,
            graph,
            query_metrics=qualified_metrics,
            query_dimensions=qualified_dimensions,
        )
    )
    errors.extend(
        _validate_consumption_expressions(
            f"Explore '{explore.name}'",
            "ordering",
            explore.default_order_by,
            explore.model,
            graph,
            query_metrics=qualified_metrics,
            query_dimensions=qualified_dimensions,
        )
    )
    errors.extend(
        _validate_order_fields_selected(
            f"Explore '{explore.name}' default",
            explore.default_order_by,
            _qualified_consumption_metrics(explore.default_metrics, explore.model, graph),
            _qualified_consumption_refs(explore.default_dimensions, explore.model),
            explore.model,
            graph,
        )
    )
    if not metrics and not dimensions:
        warnings.append(f"Explore '{explore.name}' defines no allowed or default fields")
    if any(not value.strip() for value in [*explore.filters, *explore.default_filters, *explore.default_order_by]):
        errors.append(f"Explore '{explore.name}' contains an empty filter or ordering expression")
    return errors, warnings


def validate_saved_query(saved_query: "SavedQuery", graph: "SemanticGraph") -> tuple[list[str], list[str]]:
    """Validate a SavedQuery against its Explore and physical graph."""
    errors, warnings = validate_governance(saved_query, f"Saved query '{saved_query.name}'")
    metricflow_metadata = (saved_query.metadata or {}).get("metricflow", {})
    preserved_external_syntax = metricflow_metadata.get("executable") is False
    if preserved_external_syntax:
        warnings.append(
            f"Saved query '{saved_query.name}' is preserved from MetricFlow but is not executable: "
            f"{metricflow_metadata.get('compatibility_message', 'convert source expressions to Sidemantic references')}"
        )
    base_model = ""
    explore = None
    if saved_query.explore:
        explore = graph.explores.get(saved_query.explore)
        if explore is None:
            errors.append(
                f"Saved query '{saved_query.name}' references explore '{saved_query.explore}' which doesn't exist"
            )
        else:
            base_model = explore.model
    metrics = (
        _qualified_consumption_metrics(saved_query.metrics, base_model, graph) if base_model else saved_query.metrics
    )
    dimensions = (
        _qualified_consumption_refs(saved_query.dimensions, base_model) if base_model else saved_query.dimensions
    )
    if not preserved_external_syntax:
        errors.extend(validate_query(metrics, dimensions, graph))
        errors.extend(
            _validate_consumption_expressions(
                f"Saved query '{saved_query.name}'",
                "filter",
                saved_query.filters,
                base_model,
                graph,
                query_metrics=metrics,
                query_dimensions=dimensions,
            )
        )
        errors.extend(
            _validate_consumption_expressions(
                f"Saved query '{saved_query.name}'",
                "ordering",
                saved_query.order_by,
                base_model,
                graph,
                query_metrics=metrics,
                query_dimensions=dimensions,
            )
        )
        errors.extend(
            _validate_order_fields_selected(
                f"Saved query '{saved_query.name}'",
                saved_query.order_by,
                metrics,
                dimensions,
                base_model,
                graph,
            )
        )
        for raw_segment in saved_query.segments:
            segment_ref = raw_segment
            if "." not in segment_ref and base_model:
                segment_ref = f"{base_model}.{segment_ref}"
            if "." not in segment_ref:
                errors.append(
                    f"Saved query '{saved_query.name}' segment reference must be in format 'model.segment': "
                    f"{raw_segment}"
                )
                continue
            model_name, segment_name = segment_ref.split(".", 1)
            model = graph.models.get(model_name)
            if model is None:
                errors.append(
                    f"Saved query '{saved_query.name}' segment '{raw_segment}' references model "
                    f"'{model_name}' which doesn't exist"
                )
            elif model.get_segment(segment_name) is None:
                errors.append(
                    f"Saved query '{saved_query.name}' references segment '{segment_name}' which doesn't exist "
                    f"on model '{model_name}'"
                )
        if explore is not None:
            errors.extend(
                _validate_consumption_expressions(
                    f"Saved query '{saved_query.name}' inherited Explore '{explore.name}'",
                    "filter",
                    explore.filters,
                    base_model,
                    graph,
                    query_metrics=metrics,
                    query_dimensions=dimensions,
                )
            )
            if explore.allowed_metrics is not None:
                allowed_metrics = set(_qualified_consumption_metrics(explore.allowed_metrics, base_model, graph))
                denied_metrics = sorted(set(metrics) - allowed_metrics)
                if denied_metrics:
                    errors.append(
                        f"Saved query '{saved_query.name}' selects metric(s) not allowed by Explore "
                        f"'{explore.name}': {', '.join(denied_metrics)}"
                    )
            if explore.allowed_dimensions is not None:
                allowed_dimensions = set(_qualified_consumption_refs(explore.allowed_dimensions, base_model))
                denied_dimensions = sorted(set(dimensions) - allowed_dimensions)
                if denied_dimensions:
                    errors.append(
                        f"Saved query '{saved_query.name}' selects dimension(s) not allowed by Explore "
                        f"'{explore.name}': {', '.join(denied_dimensions)}"
                    )

            from sidemantic.core.consumption import expression_field_references

            graph_metrics = graph.metrics.keys()
            if explore.allowed_filter_fields is not None:
                allowed_filters = set(_qualified_consumption_metrics(explore.allowed_filter_fields, base_model, graph))
                denied_filters = sorted(
                    expression_field_references(saved_query.filters, base_model, graph_metrics=graph_metrics)
                    - allowed_filters
                )
                if denied_filters:
                    errors.append(
                        f"Saved query '{saved_query.name}' filters on field(s) not allowed by Explore "
                        f"'{explore.name}': {', '.join(denied_filters)}"
                    )
            if explore.allowed_order_by is not None:
                allowed_ordering = set(_qualified_consumption_metrics(explore.allowed_order_by, base_model, graph))
                denied_ordering = sorted(
                    expression_field_references(saved_query.order_by, base_model, graph_metrics=graph_metrics)
                    - allowed_ordering
                )
                if denied_ordering:
                    errors.append(
                        f"Saved query '{saved_query.name}' orders by field(s) not allowed by Explore "
                        f"'{explore.name}': {', '.join(denied_ordering)}"
                    )
            if (
                explore.max_limit is not None
                and saved_query.limit is not None
                and saved_query.limit > explore.max_limit
            ):
                errors.append(
                    f"Saved query '{saved_query.name}' limit {saved_query.limit} exceeds Explore "
                    f"'{explore.name}' max_limit {explore.max_limit}"
                )
    if not saved_query.metrics and not saved_query.dimensions:
        errors.append(f"Saved query '{saved_query.name}' must select at least one metric or dimension")
    if any(not value.strip() for value in [*saved_query.filters, *saved_query.order_by]):
        errors.append(f"Saved query '{saved_query.name}' contains an empty filter or ordering expression")
    return errors, warnings


def _extract_literal_strings(annotation) -> set[str]:
    if get_origin(annotation) is Literal:
        return {value for value in get_args(annotation) if isinstance(value, str)}

    values = set()
    for arg in get_args(annotation):
        values.update(_extract_literal_strings(arg))
    return values


@lru_cache(maxsize=1)
def _valid_measure_aggs() -> set[str]:
    from sidemantic.core.metric import Metric

    annotation = Metric.model_fields["agg"].annotation
    return _extract_literal_strings(annotation)


def validate_model(model: "Model") -> list[str]:
    """Validate a model definition.

    Args:
        model: Model to validate

    Returns:
        List of validation errors (empty if valid)
    """
    errors = []

    # A model may legitimately have no primary key (None) -- e.g. a fact or disconnected table
    # imported from a format that declares none; it simply cannot serve as a join target. An
    # explicitly empty primary key (empty string or list) is still treated as a misconfiguration.
    if model.primary_key is not None and not model.primary_key:
        errors.append(f"Model '{model.name}' must have a primary_key defined")

    # Check for a physical, SQL, DAX, or externally sourced model definition.
    # Hex ``view`` resources are presentation layers over a base model and are
    # intentionally table-less, so they are exempt from this requirement.
    is_hex_view = bool((getattr(model, "meta", None) or {}).get("hex_resource_type") == "view")
    if (
        not is_hex_view
        and not model.table
        and not model.sql
        and not getattr(model, "source_uri", None)
        and not getattr(model, "dax", None)
    ):
        errors.append(f"Model '{model.name}' must have one of 'table', 'sql', 'dax', or 'source_uri' defined")

    for label, items in [
        ("dimension", model.dimensions),
        ("metric", model.metrics),
        ("segment", model.segments),
        ("pre-aggregation", model.pre_aggregations),
    ]:
        seen_names = set()
        for item in items:
            if item.name in seen_names:
                errors.append(f"Model '{model.name}' has duplicate {label} '{item.name}'")
            seen_names.add(item.name)

    if model.default_time_dimension:
        default_time_dimension = model.get_dimension(model.default_time_dimension)
        if default_time_dimension is None:
            errors.append(
                f"Model '{model.name}' default_time_dimension "
                f"'{model.default_time_dimension}' does not reference a dimension"
            )
        elif default_time_dimension.type != "time":
            errors.append(
                f"Model '{model.name}' default_time_dimension "
                f"'{model.default_time_dimension}' must reference a time dimension"
            )

    # Check that dimensions have valid types
    for dim in model.dimensions:
        if dim.type not in ["categorical", "time", "boolean", "numeric"]:
            errors.append(
                f"Model '{model.name}': dimension '{dim.name}' has invalid type '{dim.type}'. "
                f"Must be one of: categorical, time, boolean, numeric"
            )

        # Time dimensions should have granularity
        if dim.type == "time" and not dim.granularity:
            errors.append(f"Model '{model.name}': time dimension '{dim.name}' should have a granularity defined")

    # Check that measures have valid aggregation types
    # Derived, ratio, cumulative, time_comparison, and conversion metrics don't need agg
    for measure in model.metrics:
        # Skip validation for complex metric types that don't use agg
        if measure.type in ["derived", "ratio", "cumulative", "time_comparison", "conversion", "retention", "cohort"]:
            continue

        valid_aggs = _valid_measure_aggs()

        if measure.agg in valid_aggs:
            continue
        if measure.agg is None and measure.sql and sql_has_aggregate(measure.sql):
            continue
        # Opaque complete expressions (e.g. imported Cube/Tesseract
        # number_agg/time/string/boolean measures) preserve their sql verbatim
        # with agg=None and are valid even when the sql is a plain column.
        if measure.agg is None and getattr(measure, "sql_is_complete", False) and measure.sql:
            continue
        if measure.agg not in valid_aggs:
            valid_aggs_str = ", ".join(sorted(valid_aggs))
            errors.append(
                f"Model '{model.name}': measure '{measure.name}' has invalid aggregation '{measure.agg}'. "
                f"Must be one of: {valid_aggs_str}"
            )

    for preagg in model.pre_aggregations:
        for measure_name in preagg.measures or []:
            if model.get_metric(measure_name) is None:
                errors.append(
                    f"Pre-aggregation '{model.name}.{preagg.name}' references unknown measure '{measure_name}'"
                )
        for dimension_name in preagg.dimensions or []:
            if model.get_dimension(dimension_name) is None:
                errors.append(
                    f"Pre-aggregation '{model.name}.{preagg.name}' references unknown dimension '{dimension_name}'"
                )
        if preagg.time_dimension:
            time_dimension = model.get_dimension(preagg.time_dimension)
            if time_dimension is None:
                errors.append(
                    f"Pre-aggregation '{model.name}.{preagg.name}' references unknown time_dimension "
                    f"'{preagg.time_dimension}'"
                )
            elif time_dimension.type != "time":
                errors.append(
                    f"Pre-aggregation '{model.name}.{preagg.name}' time_dimension "
                    f"'{preagg.time_dimension}' must reference a time dimension"
                )

    return errors


# Pre-aggregation configuration that Sidemantic parses and stores (e.g. so Cube
# models round-trip cleanly) but does not act on during query routing or refresh.
# Surfacing these as non-fatal warnings keeps an imported model from silently
# behaving differently than it did in its source tool. See docs/compatibility/cube.md.
_INERT_PREAGG_TYPE_NOTES = {
    "rollup_join": (
        "type 'rollup_join' is parsed but not executed; Sidemantic matches and materializes it as a "
        "plain rollup and does not perform cross-data-source rollup joins"
    ),
    "lambda": (
        "type 'lambda' is parsed but not executed; Sidemantic treats it as a plain rollup and does not "
        "union a batch rollup with real-time source data"
    ),
}


def _preagg_unions_source(preagg) -> bool:
    """A lambda pre-aggregation that actually unions fresh source data at query time.

    When True the lambda is executed (UNION of the batch rollup with a fresh source
    aggregation split at build_range_end), so its 'inert' and 'build_range no runtime
    effect' notes no longer apply.
    """
    return (
        preagg.type == "lambda"
        and getattr(preagg, "union_with_source_data", False)
        and preagg.build_range_end is not None
    )


def validate_model_warnings(model: "Model") -> list[str]:
    """Collect non-fatal warnings for a model definition.

    Unlike :func:`validate_model`, these never fail validation. They flag
    pre-aggregation configuration that Sidemantic accepts and stores but does not
    act on at query or refresh time, so an imported model (for example from Cube)
    is not silently degraded without notice.

    Args:
        model: Model to inspect

    Returns:
        List of warning messages (empty if none)
    """
    warnings: list[str] = []

    for preagg in model.pre_aggregations:
        prefix = f"Pre-aggregation '{model.name}.{preagg.name}'"

        # A lambda with union_with_source_data + build_range_end is executed (it
        # unions the batch rollup with fresh source data), so its inert-type note and
        # build_range note no longer apply.
        unions_source = _preagg_unions_source(preagg)

        inert_type_note = _INERT_PREAGG_TYPE_NOTES.get(preagg.type)
        if inert_type_note and not unions_source:
            warnings.append(f"{prefix}: {inert_type_note}")

        if (preagg.build_range_start is not None or preagg.build_range_end is not None) and not unions_source:
            warnings.append(
                f"{prefix}: build_range_start/build_range_end have no runtime effect; Sidemantic does not "
                "bound materialization by a build range"
            )

        if preagg.refresh_key and preagg.refresh_key.sql:
            warnings.append(
                f"{prefix}: refresh_key.sql is parsed but not executed; Sidemantic has no scheduler to run a "
                "change-detection query, so refresh timing must come from your external orchestrator"
            )

    return warnings


def validate_metric(measure: "Metric", graph: "SemanticGraph") -> list[str]:
    """Validate a measure definition.

    Args:
        measure: Metric to validate
        graph: Semantic graph containing models

    Returns:
        List of validation errors (empty if valid)
    """
    errors = []

    # Check measure type
    if measure.type and measure.type not in [
        "ratio",
        "derived",
        "cumulative",
        "time_comparison",
        "conversion",
        "retention",
        "cohort",
    ]:
        errors.append(
            f"Metric '{measure.name}' has invalid type '{measure.type}'. "
            f"Must be one of: ratio, derived, cumulative, time_comparison, conversion, retention, cohort"
        )
        return errors  # Can't continue validation with invalid type

    # Validate untyped metrics with sql (measure references)
    if not measure.type and not measure.agg and measure.sql:
        # Only validate direct model.measure references here.
        # Complex SQL expressions (e.g., COUNT(model.col) / COUNT(other.col))
        # are valid untyped metrics and should not be split as plain refs.
        sql_ref = measure.sql.strip()
        is_direct_ref = (
            "." in sql_ref and " " not in sql_ref and not any(op in sql_ref for op in ["+", "-", "*", "/", "(", ")"])
        )
        if is_direct_ref:
            model_name, measure_name = sql_ref.split(".", 1)
            model = graph.models.get(model_name)
            if not model:
                errors.append(f"Metric '{measure.name}': model '{model_name}' not found")
            elif not model.get_metric(measure_name):
                errors.append(f"Metric '{measure.name}': measure '{measure_name}' not found in model '{model_name}'")

    # Validate based on type
    if measure.type == "ratio":
        if not measure.numerator:
            errors.append(f"Ratio measure '{measure.name}' must have 'numerator' defined")
        if not measure.denominator:
            errors.append(f"Ratio measure '{measure.name}' must have 'denominator' defined")

        # Validate references
        for ref_type, ref in [
            ("numerator", measure.numerator),
            ("denominator", measure.denominator),
        ]:
            if ref and "." in ref and ref not in graph.metrics:
                model_name, measure_name = ref.split(".", 1)
                model = graph.models.get(model_name)
                if not model:
                    errors.append(f"Ratio measure '{measure.name}': {ref_type} model '{model_name}' not found")
                elif not model.get_metric(measure_name):
                    errors.append(
                        f"Ratio measure '{measure.name}': {ref_type} measure '{measure_name}' not found in model '{model_name}'"
                    )

    elif measure.type == "derived":
        if not measure.sql and not getattr(measure, "has_untranslated_dax", False):
            errors.append(f"Derived measure '{measure.name}' must have 'expr' defined")
        if getattr(measure, "has_untranslated_dax", False):
            return errors

        # Auto-detect dependencies and check for circular references
        dependencies = measure.get_dependencies(graph)

        # Check for self-reference first
        if measure.name in dependencies:
            errors.append(f"Derived measure '{measure.name}' cannot reference itself")
        else:
            circular_deps = _check_circular_dependencies(measure, graph, set())
            if circular_deps:
                errors.append(f"Derived measure '{measure.name}' has circular dependency: {' -> '.join(circular_deps)}")

    elif measure.type == "cumulative":
        if not measure.sql and not measure.window_expression:
            errors.append(f"Cumulative measure '{measure.name}' must have 'sql' or 'window_expression' defined")

    elif measure.type == "retention":
        if not measure.entity:
            errors.append(f"Retention measure '{measure.name}' must have 'entity' defined")
        if not measure.cohort_event:
            errors.append(f"Retention measure '{measure.name}' must have 'cohort_event' defined")

    return errors


def _check_circular_dependencies(
    measure: "Metric", graph: "SemanticGraph", visited: set[str], path: list[str] | None = None
) -> list[str] | None:
    """Check for circular dependencies in derived measures.

    Args:
        measure: Metric to check
        graph: Semantic graph
        visited: Set of visited measure names
        path: Current dependency path

    Returns:
        List of measure names in circular path, or None if no cycle
    """
    if path is None:
        path = []

    if measure.name in visited:
        # Found a cycle
        cycle_start = path.index(measure.name)
        return path[cycle_start:] + [measure.name]

    if measure.type != "derived":
        return None

    visited.add(measure.name)
    path.append(measure.name)

    dependencies = measure.get_dependencies(graph)
    for dep_name in dependencies:
        try:
            dep_measure = graph.get_metric(dep_name)
            if dep_measure:
                cycle = _check_circular_dependencies(dep_measure, graph, visited.copy(), path.copy())
                if cycle:
                    return cycle
        except KeyError:
            # Dependency doesn't exist yet, skip circular check
            pass

    return None


def validate_query(metrics: list[str], dimensions: list[str], graph: "SemanticGraph") -> list[str]:
    """Validate a query before execution.

    Args:
        metrics: List of metric references
        dimensions: List of dimension references
        graph: Semantic graph

    Returns:
        List of validation errors (empty if valid)
    """
    errors = []

    def _add_untranslated_dax_error(metric_ref: str, measure: "Metric") -> None:
        if getattr(measure, "has_untranslated_dax", False):
            errors.append(
                f"Metric '{metric_ref}' contains DAX expression but has no SQL translation. "
                "DAX lowering is not available in this build."
            )

    def _add_untranslated_dax_dimension_error(dim_ref: str, dimension) -> None:
        if getattr(dimension, "has_untranslated_dax", False):
            errors.append(
                f"Dimension '{dim_ref}' contains DAX expression but has no SQL translation. "
                "DAX lowering is not available in this build."
            )

    def _add_untranslated_dax_model_error(model_ref: str, model) -> None:
        if getattr(model, "has_untranslated_dax", False):
            errors.append(
                f"Model '{model_ref}' contains DAX table expression but has no SQL/table translation. "
                "DAX table lowering is not available in this build."
            )

    # Validate metric references
    for metric_ref in metrics:
        try:
            graph.resolve_metric_reference(metric_ref)
            continue
        except KeyError:
            pass

        if "." in metric_ref:
            # Direct measure reference
            model_name, measure_name = metric_ref.split(".", 1)
            model = graph.models.get(model_name)
            if not model:
                errors.append(f"Model '{model_name}' not found (referenced in '{metric_ref}')")
            else:
                _add_untranslated_dax_model_error(model_name, model)
                measure = model.get_metric(measure_name)
                if not measure:
                    errors.append(
                        f"Metric '{measure_name}' not found in model '{model_name}' (referenced in '{metric_ref}')"
                    )
                else:
                    _add_untranslated_dax_error(metric_ref, measure)
        else:
            # Metric reference
            try:
                measure = graph.get_metric(metric_ref)
                _add_untranslated_dax_error(metric_ref, measure)
            except KeyError:
                errors.append(f"Metric '{metric_ref}' not found")

    # Validate dimension references
    for dim_ref in dimensions:
        # Handle granularity suffix
        if "__" in dim_ref:
            dim_ref_base, granularity = dim_ref.rsplit("__", 1)
            # Validate granularity
            if granularity not in ["second", "minute", "hour", "day", "week", "month", "quarter", "year"]:
                errors.append(
                    f"Invalid time granularity '{granularity}' in '{dim_ref}'. "
                    f"Must be one of: second, minute, hour, day, week, month, quarter, year"
                )
            dim_ref = dim_ref_base

        if "." in dim_ref:
            model_name, dim_name = dim_ref.split(".", 1)
            model = graph.models.get(model_name)
            if not model:
                errors.append(f"Model '{model_name}' not found (referenced in '{dim_ref}')")
            else:
                _add_untranslated_dax_model_error(model_name, model)
                dimension = model.get_dimension(dim_name)
                if not dimension:
                    errors.append(
                        f"Dimension '{dim_name}' not found in model '{model_name}' (referenced in '{dim_ref}')"
                    )
                else:
                    _add_untranslated_dax_dimension_error(dim_ref, dimension)
        else:
            errors.append(f"Dimension reference '{dim_ref}' must be in 'model.dimension' format")

    # Check for join paths
    model_names = set()
    for metric_ref in metrics:
        try:
            metric_model_name, measure = graph.resolve_metric_reference(metric_ref)
        except KeyError:
            continue  # Already reported as error above
        if metric_model_name:
            model_names.add(metric_model_name)
        elif measure and measure.sql and "." in measure.sql:
            model_names.add(measure.sql.split(".", 1)[0])

    for dim_ref in dimensions:
        if "__" in dim_ref:
            dim_ref = dim_ref.rsplit("__", 1)[0]
        if "." in dim_ref:
            model_names.add(dim_ref.split(".", 1)[0])

    # Check that all model pairs can be joined
    # Only check models that exist in the graph (errors for missing models already reported above)
    valid_model_names = [m for m in model_names if m in graph.models]
    model_list = list(valid_model_names)
    for i, model_a in enumerate(model_list):
        for model_b in model_list[i + 1 :]:
            try:
                graph.find_relationship_path(model_a, model_b)
            except (ValueError, KeyError):
                # Catch both ValueError (no path) and KeyError (model doesn't exist)
                errors.append(
                    f"No join path found between models '{model_a}' and '{model_b}'. "
                    f"Add relationships to enable joining these models."
                )

    return errors
