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

    # Check for primary entity or primary_key (Rails-like)
    if not model.primary_entity and not model.primary_key:
        errors.append(
            f"Model '{model.name}' must have either a primary entity or primary_key defined"
        )

    # Check for table or SQL
    if not model.table and not model.sql:
        errors.append(f"Model '{model.name}' must have either 'table' or 'sql' defined")

    # Check that entities have valid types
    for entity in model.entities:
        if entity.type not in ["primary", "foreign", "unique"]:
            errors.append(
                f"Model '{model.name}': entity '{entity.name}' has invalid type '{entity.type}'. "
                f"Must be one of: primary, foreign, unique"
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
            errors.append(
                f"Model '{model.name}': time dimension '{dim.name}' should have a granularity defined"
            )

    # Check that measures have valid aggregation types
    for measure in model.measures:
        if measure.agg not in ["sum", "count", "count_distinct", "avg", "min", "max", "median"]:
            errors.append(
                f"Model '{model.name}': measure '{measure.name}' has invalid aggregation '{measure.agg}'. "
                f"Must be one of: sum, count, count_distinct, avg, min, max, median"
            )

    return errors


def validate_metric(metric: "Metric", graph: "SemanticGraph") -> list[str]:
    """Validate a metric definition.

    Args:
        metric: Metric to validate
        graph: Semantic graph containing models

    Returns:
        List of validation errors (empty if valid)
    """
    errors = []

    # Check metric type
    if metric.type not in ["simple", "ratio", "derived", "cumulative"]:
        errors.append(
            f"Metric '{metric.name}' has invalid type '{metric.type}'. "
            f"Must be one of: simple, ratio, derived, cumulative"
        )
        return errors  # Can't continue validation with invalid type

    # Validate based on type
    if metric.type == "simple":
        if not metric.measure:
            errors.append(f"Simple metric '{metric.name}' must have 'measure' defined")
        else:
            # Validate measure reference
            if "." in metric.measure:
                model_name, measure_name = metric.measure.split(".")
                model = graph.models.get(model_name)
                if not model:
                    errors.append(
                        f"Simple metric '{metric.name}': model '{model_name}' not found"
                    )
                elif not model.get_measure(measure_name):
                    errors.append(
                        f"Simple metric '{metric.name}': measure '{measure_name}' not found in model '{model_name}'"
                    )
            else:
                errors.append(
                    f"Simple metric '{metric.name}': measure must be in 'model.measure' format"
                )

    elif metric.type == "ratio":
        if not metric.numerator:
            errors.append(f"Ratio metric '{metric.name}' must have 'numerator' defined")
        if not metric.denominator:
            errors.append(f"Ratio metric '{metric.name}' must have 'denominator' defined")

        # Validate references
        for ref_type, ref in [("numerator", metric.numerator), ("denominator", metric.denominator)]:
            if ref and "." in ref:
                model_name, measure_name = ref.split(".")
                model = graph.models.get(model_name)
                if not model:
                    errors.append(
                        f"Ratio metric '{metric.name}': {ref_type} model '{model_name}' not found"
                    )
                elif not model.get_measure(measure_name):
                    errors.append(
                        f"Ratio metric '{metric.name}': {ref_type} measure '{measure_name}' not found in model '{model_name}'"
                    )

    elif metric.type == "derived":
        if not metric.expr:
            errors.append(f"Derived metric '{metric.name}' must have 'expr' defined")

        # Auto-detect dependencies and check for circular references
        dependencies = metric.get_dependencies(graph)

        # Check for self-reference first
        if metric.name in dependencies:
            errors.append(f"Derived metric '{metric.name}' cannot reference itself")
        else:
            circular_deps = _check_circular_dependencies(metric, graph, set())
            if circular_deps:
                errors.append(
                    f"Derived metric '{metric.name}' has circular dependency: {' -> '.join(circular_deps)}"
                )

    elif metric.type == "cumulative":
        if not metric.measure:
            errors.append(f"Cumulative metric '{metric.name}' must have 'measure' defined")

    return errors


def _check_circular_dependencies(
    metric: "Metric", graph: "SemanticGraph", visited: set[str], path: list[str] | None = None
) -> list[str] | None:
    """Check for circular dependencies in derived metrics.

    Args:
        metric: Metric to check
        graph: Semantic graph
        visited: Set of visited metric names
        path: Current dependency path

    Returns:
        List of metric names in circular path, or None if no cycle
    """
    if path is None:
        path = []

    if metric.name in visited:
        # Found a cycle
        cycle_start = path.index(metric.name)
        return path[cycle_start:] + [metric.name]

    if metric.type != "derived":
        return None

    visited.add(metric.name)
    path.append(metric.name)

    dependencies = metric.get_dependencies(graph)
    for dep_name in dependencies:
        try:
            dep_metric = graph.get_metric(dep_name)
            if dep_metric:
                cycle = _check_circular_dependencies(dep_metric, graph, visited.copy(), path.copy())
                if cycle:
                    return cycle
        except KeyError:
            # Dependency doesn't exist yet, skip circular check
            pass

    return None


def validate_query(
    metrics: list[str], dimensions: list[str], graph: "SemanticGraph"
) -> list[str]:
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
            elif not model.get_measure(measure_name):
                errors.append(
                    f"Measure '{measure_name}' not found in model '{model_name}' (referenced in '{metric_ref}')"
                )
        else:
            # Metric reference
            try:
                metric = graph.get_metric(metric_ref)
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
                errors.append(
                    f"Dimension '{dim_name}' not found in model '{model_name}' (referenced in '{dim_ref}')"
                )
        else:
            errors.append(f"Dimension reference '{dim_ref}' must be in 'model.dimension' format")

    # Check for join paths
    model_names = set()
    for metric_ref in metrics:
        if "." in metric_ref:
            model_names.add(metric_ref.split(".")[0])
        else:
            try:
                metric = graph.get_metric(metric_ref)
                if metric and metric.measure and "." in metric.measure:
                    model_names.add(metric.measure.split(".")[0])
            except KeyError:
                pass  # Already reported as error above

    for dim_ref in dimensions:
        if "__" in dim_ref:
            dim_ref = dim_ref.rsplit("__", 1)[0]
        if "." in dim_ref:
            model_names.add(dim_ref.split(".")[0])

    # Check that all model pairs can be joined
    model_list = list(model_names)
    for i, model_a in enumerate(model_list):
        for model_b in model_list[i + 1 :]:
            try:
                graph.find_join_path(model_a, model_b)
            except ValueError:
                errors.append(
                    f"No join path found between models '{model_a}' and '{model_b}'. "
                    f"Add entities to enable joining these models."
                )

    return errors
