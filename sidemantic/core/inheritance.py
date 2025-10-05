"""Inheritance utilities for models and metrics."""

from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.segment import Segment


def merge_model(child: Model, parent: Model) -> Model:
    """Merge child model with parent model using inheritance.

    Child model inherits all fields from parent, with child values taking precedence.

    Args:
        child: Child model with extends reference
        parent: Parent model to inherit from

    Returns:
        New model with merged properties

    Examples:
        >>> parent = Model(name="base", table="base_table", dimensions=[...])
        >>> child = Model(name="child", extends="base", dimensions=[...])
        >>> merged = merge_model(child, parent)
    """
    # Start with parent's data
    merged_data = parent.model_dump(exclude={"name"})

    # Override with child's data (excluding None values and extends)
    child_data = child.model_dump(exclude_none=True, exclude={"extends"})

    # Merge lists (dimensions, metrics, relationships, segments)
    # Child's items are added to parent's items
    for field in ["dimensions", "metrics", "relationships", "segments"]:
        parent_items = merged_data.get(field, [])
        child_items = child_data.get(field, [])

        # Combine, with child items overriding parent items with same name
        parent_by_name = {item["name"]: item for item in parent_items}
        child_by_name = {item["name"]: item for item in child_items}

        # Merge
        parent_by_name.update(child_by_name)
        merged_data[field] = list(parent_by_name.values())

    # Override scalar fields with child values
    for field in ["table", "sql", "description", "primary_key"]:
        if field in child_data:
            merged_data[field] = child_data[field]

    # Keep child's name
    merged_data["name"] = child.name

    # Reconstruct model objects from dicts
    if merged_data.get("dimensions"):
        merged_data["dimensions"] = [Dimension(**d) for d in merged_data["dimensions"]]
    if merged_data.get("metrics"):
        merged_data["metrics"] = [Metric(**m) for m in merged_data["metrics"]]
    if merged_data.get("relationships"):
        merged_data["relationships"] = [Relationship(**r) for r in merged_data["relationships"]]
    if merged_data.get("segments"):
        merged_data["segments"] = [Segment(**s) for s in merged_data["segments"]]

    return Model(**merged_data)


def merge_metric(child: Metric, parent: Metric) -> Metric:
    """Merge child metric with parent metric using inheritance.

    Child metric inherits all fields from parent, with child values taking precedence.

    Args:
        child: Child metric with extends reference
        parent: Parent metric to inherit from

    Returns:
        New metric with merged properties

    Examples:
        >>> parent = Metric(name="base_revenue", agg="sum", sql="amount")
        >>> child = Metric(name="filtered_revenue", extends="base_revenue", filters=["status = 'completed'"])
        >>> merged = merge_metric(child, parent)
    """
    # Start with parent's data
    merged_data = parent.model_dump(exclude={"name"})

    # Override with child's data (excluding None values and extends)
    child_data = child.model_dump(exclude_none=True, exclude={"extends"})

    # Handle list fields - merge arrays
    for field in ["filters", "drill_fields"]:
        parent_items = merged_data.get(field) or []
        child_items = child_data.get(field)

        if child_items:
            # Child can add to parent's list
            merged_data[field] = parent_items + child_items
        else:
            merged_data[field] = parent_items if parent_items else None

    # Override all other fields with child values
    for field, value in child_data.items():
        if field not in ["filters", "drill_fields", "extends", "name"]:
            merged_data[field] = value

    # Keep child's name
    merged_data["name"] = child.name

    return Metric(**merged_data)


def resolve_model_inheritance(models: dict[str, Model]) -> dict[str, Model]:
    """Resolve inheritance for all models in a graph.

    Args:
        models: Dictionary of model name to model

    Returns:
        Dictionary of model name to resolved model (with inheritance applied)

    Raises:
        ValueError: If circular inheritance detected or parent not found
    """
    resolved = {}
    in_progress = set()

    def resolve(name: str) -> Model:
        if name in resolved:
            return resolved[name]

        if name in in_progress:
            raise ValueError(f"Circular inheritance detected for model '{name}'")

        model = models.get(name)
        if not model:
            raise ValueError(f"Model '{name}' not found")

        # If no inheritance, just return as-is
        if not model.extends:
            resolved[name] = model
            return model

        # Resolve parent first
        in_progress.add(name)
        parent = resolve(model.extends)
        in_progress.remove(name)

        # Merge child with parent
        merged = merge_model(model, parent)
        resolved[name] = merged
        return merged

    # Resolve all models
    for name in models:
        resolve(name)

    return resolved


def resolve_metric_inheritance(metrics: dict[str, Metric]) -> dict[str, Metric]:
    """Resolve inheritance for all metrics in a graph.

    Args:
        metrics: Dictionary of metric name to metric

    Returns:
        Dictionary of metric name to resolved metric (with inheritance applied)

    Raises:
        ValueError: If circular inheritance detected or parent not found
    """
    resolved = {}
    in_progress = set()

    def resolve(name: str) -> Metric:
        if name in resolved:
            return resolved[name]

        if name in in_progress:
            raise ValueError(f"Circular inheritance detected for metric '{name}'")

        metric = metrics.get(name)
        if not metric:
            raise ValueError(f"Metric '{name}' not found")

        # If no inheritance, just return as-is
        if not metric.extends:
            resolved[name] = metric
            return metric

        # Resolve parent first
        in_progress.add(name)
        parent = resolve(metric.extends)
        in_progress.remove(name)

        # Merge child with parent
        merged = merge_metric(metric, parent)
        resolved[name] = merged
        return merged

    # Resolve all metrics
    for name in metrics:
        resolve(name)

    return resolved
