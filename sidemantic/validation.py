"""Validation and error handling for semantic layer."""

from functools import lru_cache
from typing import TYPE_CHECKING, Literal, get_args, get_origin

from sidemantic.sql.aggregation_detection import sql_has_aggregate

if TYPE_CHECKING:
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

    key_definitions = [(f"unique_keys[{index}]", columns) for index, columns in enumerate(model.unique_keys or [])]
    if model.primary_key is not None:
        key_definitions.insert(0, ("primary_key", model.primary_key_columns))
    for key_name, key_columns in key_definitions:
        if not key_columns:
            errors.append(f"Model '{model.name}' {key_name} must contain at least one column")
        elif len(key_columns) != len(set(key_columns)):
            errors.append(f"Model '{model.name}' {key_name} contains duplicate columns")

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

    if model.primary_key is None:
        warnings.append(
            f"Model '{model.name}' has no primary_key declaration. This is valid for keyless models, "
            "but the model cannot be used where a unique row identity or fan-out-safe aggregation is required."
        )

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


def _declared_unique_key(model: "Model", columns: list[str]) -> bool:
    """Return whether columns are declared as a primary or alternate unique key."""
    if not columns:
        return False
    candidate = tuple(columns)
    declared = {tuple(model.primary_key_columns)} if model.primary_key_columns else set()
    declared.update(tuple(key) for key in model.unique_keys or [] if key)
    return candidate in declared


def validate_relationships(graph: "SemanticGraph") -> list[str]:
    """Validate relationship keys and the uniqueness implied by each cardinality.

    This runs after all models have loaded because related-model keys cannot be resolved safely
    while models are registered one at a time. Custom SQL relationships are exempt from column-key
    requirements, but still require their target model and explicit cardinality.
    """
    errors: list[str] = []

    def add_error(source_name: str, relationship, message: str) -> None:
        errors.append(f"Relationship '{source_name}.{relationship.name}' ({relationship.type}): {message}")

    def check_lengths(source_name: str, relationship, left: list[str], right: list[str]) -> None:
        if left and right and len(left) != len(right):
            add_error(
                source_name,
                relationship,
                f"join key arity differs ({len(left)} source column(s), {len(right)} target column(s))",
            )

    for source_name, source in graph.models.items():
        for relationship in source.relationships:
            if not relationship.active:
                continue
            target = graph.models.get(relationship.name)
            if target is None:
                add_error(source_name, relationship, "target model does not exist")
                continue

            if relationship.type == "cross":
                if any(
                    (
                        relationship.foreign_key,
                        relationship.primary_key,
                        relationship.through,
                        relationship.through_foreign_key,
                        relationship.related_foreign_key,
                    )
                ):
                    add_error(source_name, relationship, "cross relationships must not declare join keys")
                continue

            if relationship.sql:
                continue

            if relationship.type == "many_to_one":
                source_keys = relationship.foreign_key_columns
                target_keys = relationship.primary_key_columns or target.primary_key_columns
                if not source_keys:
                    add_error(source_name, relationship, "foreign_key is required; no column name is inferred")
                if not target_keys:
                    add_error(
                        source_name,
                        relationship,
                        f"target model '{target.name}' has no primary_key; declare relationship.primary_key "
                        "for an alternate unique key or declare the model key",
                    )
                elif relationship.primary_key is None and not _declared_unique_key(target, target_keys):
                    add_error(
                        source_name,
                        relationship,
                        f"target columns {target_keys!r} are not declared as the primary_key or in unique_keys "
                        f"on model '{target.name}'",
                    )
                check_lengths(source_name, relationship, source_keys, target_keys)
                continue

            if relationship.type in {"one_to_many", "one_to_one"}:
                source_keys = relationship.primary_key_columns or source.primary_key_columns
                target_keys = relationship.foreign_key_columns
                if not source_keys:
                    add_error(
                        source_name,
                        relationship,
                        "the source model has no primary_key; declare relationship.primary_key for an alternate "
                        "unique key or declare the model key",
                    )
                elif relationship.primary_key is None and not _declared_unique_key(source, source_keys):
                    add_error(
                        source_name,
                        relationship,
                        f"source columns {source_keys!r} are not declared as the primary_key or in unique_keys",
                    )
                if not target_keys:
                    add_error(source_name, relationship, "foreign_key is required; no column name is inferred")
                check_lengths(source_name, relationship, source_keys, target_keys)
                continue

            if relationship.type == "many_to_many":
                if not relationship.through:
                    # Some source formats represent a direct many-to-many predicate without a bridge.
                    # Preserve it only when both sides are explicit; no conventional keys are invented.
                    source_keys = relationship.foreign_key_columns
                    target_keys = relationship.primary_key_columns
                    if not source_keys or not target_keys:
                        add_error(
                            source_name,
                            relationship,
                            "through is required unless explicit foreign_key and primary_key columns describe "
                            "a direct relationship",
                        )
                    check_lengths(source_name, relationship, source_keys, target_keys)
                    continue

                junction = graph.models.get(relationship.through)
                if junction is None:
                    add_error(source_name, relationship, f"junction model '{relationship.through}' does not exist")
                    continue

                source_keys = source.primary_key_columns
                target_keys = relationship.primary_key_columns or target.primary_key_columns
                junction_source_keys, junction_target_keys = relationship.junction_key_columns()
                if not source_keys:
                    add_error(source_name, relationship, "source model has no primary_key")
                if not target_keys:
                    add_error(source_name, relationship, f"target model '{target.name}' has no primary_key")
                elif not _declared_unique_key(target, target_keys):
                    add_error(
                        source_name,
                        relationship,
                        f"target columns {target_keys!r} are not declared unique on model '{target.name}'",
                    )
                if not junction_source_keys:
                    add_error(source_name, relationship, "through_foreign_key is required")
                if not junction_target_keys:
                    add_error(source_name, relationship, "related_foreign_key is required")
                check_lengths(source_name, relationship, source_keys, junction_source_keys)
                check_lengths(source_name, relationship, junction_target_keys, target_keys)

    return errors


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
    relationship_errors = []
    if len(model_list) > 1:
        selected = set(model_list)
        for relationship_error in validate_relationships(graph):
            relationship_ref = relationship_error.split("'", 2)[1]
            source_name, _, target_name = relationship_ref.partition(".")
            if source_name in selected and target_name in selected:
                relationship_errors.append(relationship_error)
        errors.extend(relationship_errors)

    for i, model_a in enumerate(model_list):
        for model_b in model_list[i + 1 :]:
            try:
                graph.find_relationship_path(model_a, model_b)
            except (ValueError, KeyError):
                # Catch both ValueError (no path) and KeyError (model doesn't exist)
                if not relationship_errors:
                    errors.append(
                        f"No join path found between models '{model_a}' and '{model_b}'. "
                        f"Add relationships to enable joining these models."
                    )

    return errors
