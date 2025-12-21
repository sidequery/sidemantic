"""Validation and error handling for semantic layer."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sidemantic.core.metric import Metric
    from sidemantic.core.model import Model
    from sidemantic.core.semantic_graph import SemanticGraph


class ValidationError(Exception):
    """Raised when semantic layer validation fails."""

    pass


class QueryValidationError(ValidationError):
    """Raised when query validation fails."""

    pass


class MetricValidationError(ValidationError):
    """Raised when metric validation fails."""

    pass


class ModelValidationError(ValidationError):
    """Raised when model validation fails."""

    pass


def validate_model(model: "Model") -> list[str]:
    """Validate a model definition.

    Args:
        model: Model to validate

    Returns:
        List of validation errors (empty if valid)
    """
    errors = []

    # Check for primary_key
    if not model.primary_key:
        errors.append(f"Model '{model.name}' must have a primary_key defined")

    # Check for table or SQL
    if not model.table and not model.sql:
        errors.append(f"Model '{model.name}' must have either 'table' or 'sql' defined")

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
        if measure.type in ["derived", "ratio", "cumulative", "time_comparison", "conversion"]:
            continue

        if measure.agg not in ["sum", "count", "count_distinct", "avg", "min", "max", "median"]:
            errors.append(
                f"Model '{model.name}': measure '{measure.name}' has invalid aggregation '{measure.agg}'. "
                f"Must be one of: sum, count, count_distinct, avg, min, max, median"
            )

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
    ]:
        errors.append(
            f"Metric '{measure.name}' has invalid type '{measure.type}'. "
            f"Must be one of: ratio, derived, cumulative, time_comparison, conversion"
        )
        return errors  # Can't continue validation with invalid type

    # Validate untyped metrics with sql (measure references)
    if not measure.type and not measure.agg and measure.sql:
        # This is an untyped metric referencing a measure
        if "." in measure.sql:
            model_name, measure_name = measure.sql.split(".")
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
            if ref and "." in ref:
                model_name, measure_name = ref.split(".")
                model = graph.models.get(model_name)
                if not model:
                    errors.append(f"Ratio measure '{measure.name}': {ref_type} model '{model_name}' not found")
                elif not model.get_metric(measure_name):
                    errors.append(
                        f"Ratio measure '{measure.name}': {ref_type} measure '{measure_name}' not found in model '{model_name}'"
                    )

    elif measure.type == "derived":
        if not measure.sql:
            errors.append(f"Derived measure '{measure.name}' must have 'expr' defined")

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

    # Validate metric references
    for metric_ref in metrics:
        if "." in metric_ref:
            # Direct measure reference
            model_name, measure_name = metric_ref.split(".")
            model = graph.models.get(model_name)
            if not model:
                errors.append(f"Model '{model_name}' not found (referenced in '{metric_ref}')")
            elif not model.get_metric(measure_name):
                errors.append(
                    f"Metric '{measure_name}' not found in model '{model_name}' (referenced in '{metric_ref}')"
                )
        else:
            # Metric reference
            try:
                graph.get_metric(metric_ref)
            except KeyError:
                errors.append(f"Metric '{metric_ref}' not found")

    # Validate dimension references
    for dim_ref in dimensions:
        # Handle granularity suffix
        if "__" in dim_ref:
            dim_ref_base, granularity = dim_ref.rsplit("__", 1)
            # Validate granularity
            if granularity not in ["hour", "day", "week", "month", "quarter", "year"]:
                errors.append(
                    f"Invalid time granularity '{granularity}' in '{dim_ref}'. "
                    f"Must be one of: hour, day, week, month, quarter, year"
                )
            dim_ref = dim_ref_base

        if "." in dim_ref:
            model_name, dim_name = dim_ref.split(".")
            model = graph.models.get(model_name)
            if not model:
                errors.append(f"Model '{model_name}' not found (referenced in '{dim_ref}')")
            elif not model.get_dimension(dim_name):
                errors.append(f"Dimension '{dim_name}' not found in model '{model_name}' (referenced in '{dim_ref}')")
        else:
            errors.append(f"Dimension reference '{dim_ref}' must be in 'model.dimension' format")

    # Check for join paths
    model_names = set()
    for metric_ref in metrics:
        if "." in metric_ref:
            model_names.add(metric_ref.split(".")[0])
        else:
            try:
                measure = graph.get_metric(metric_ref)
                if measure and measure.sql and "." in measure.sql:
                    model_names.add(measure.sql.split(".")[0])
            except KeyError:
                pass  # Already reported as error above

    for dim_ref in dimensions:
        if "__" in dim_ref:
            dim_ref = dim_ref.rsplit("__", 1)[0]
        if "." in dim_ref:
            model_names.add(dim_ref.split(".")[0])

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
