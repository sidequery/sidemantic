"""Parse SQL-based metric and segment definitions."""

from pathlib import Path

import yaml
from sqlglot import exp

from sidemantic.core.dialect import (
    PROPERTY_ALIASES,
    DimensionDef,
    MetricDef,
    ModelDef,
    PropertyEQ,
    RelationshipDef,
    SegmentDef,
    parse,
)
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.relationship import Relationship
from sidemantic.core.segment import Segment


def parse_sql_definitions(sql: str) -> tuple[list[Metric], list[Segment]]:
    """Parse SQL string containing METRIC() and SEGMENT() definitions.

    Args:
        sql: SQL string with definitions

    Returns:
        Tuple of (metrics, segments)
    """
    metrics = []
    segments = []

    try:
        statements = parse(sql)
    except Exception:
        # If parsing fails, return empty lists
        return metrics, segments

    for stmt in statements:
        if isinstance(stmt, MetricDef):
            metric = _parse_metric_def(stmt)
            if metric:
                metrics.append(metric)
        elif isinstance(stmt, SegmentDef):
            segment = _parse_segment_def(stmt)
            if segment:
                segments.append(segment)

    return metrics, segments


def parse_sql_model(sql: str) -> Model | None:
    """Parse SQL string containing a complete model definition.

    Expects MODEL(), DIMENSION(), RELATIONSHIP(), METRIC(), and SEGMENT() statements.

    Args:
        sql: SQL string with model definition

    Returns:
        Model instance or None
    """
    model_def = None
    dimensions = []
    relationships = []
    metrics = []
    segments = []

    try:
        statements = parse(sql)
    except Exception:
        return None

    for stmt in statements:
        if isinstance(stmt, ModelDef):
            model_def = _parse_model_def(stmt)
        elif isinstance(stmt, DimensionDef):
            dimension = _parse_dimension_def(stmt)
            if dimension:
                dimensions.append(dimension)
        elif isinstance(stmt, RelationshipDef):
            relationship = _parse_relationship_def(stmt)
            if relationship:
                relationships.append(relationship)
        elif isinstance(stmt, MetricDef):
            metric = _parse_metric_def(stmt)
            if metric:
                metrics.append(metric)
        elif isinstance(stmt, SegmentDef):
            segment = _parse_segment_def(stmt)
            if segment:
                segments.append(segment)

    if not model_def:
        return None

    # Merge parsed definitions with model
    if dimensions:
        model_def.dimensions.extend(dimensions)
    if relationships:
        model_def.relationships.extend(relationships)
    if metrics:
        model_def.metrics.extend(metrics)
    if segments:
        model_def.segments.extend(segments)

    return model_def


def parse_sql_file_with_frontmatter(path: Path) -> tuple[dict, list[Metric], list[Segment]]:
    """Parse .sql file with YAML frontmatter and SQL definitions.

    Format:
        ---
        name: orders
        table: orders
        primary_key: order_id
        ---

        METRIC (
            name revenue,
            expression SUM(amount)
        );

    Args:
        path: Path to .sql file

    Returns:
        Tuple of (frontmatter_dict, metrics, segments)
    """
    with open(path) as f:
        content = f.read()

    frontmatter = {}
    sql_body = content

    # Check for YAML frontmatter (between --- markers)
    if content.strip().startswith("---"):
        parts = content.split("---", 2)
        if len(parts) >= 3:
            frontmatter_text = parts[1].strip()
            sql_body = parts[2].strip()

            if frontmatter_text:
                frontmatter = yaml.safe_load(frontmatter_text) or {}

    metrics, segments = parse_sql_definitions(sql_body)
    return frontmatter, metrics, segments


def _parse_model_def(model_def: ModelDef) -> Model | None:
    """Convert ModelDef expression to Model instance.

    Args:
        model_def: Parsed MODEL() expression

    Returns:
        Model instance or None
    """
    props = _extract_properties(model_def)

    # Get field names from Model
    model_fields = set(Model.model_fields.keys())

    model_data = {}

    for prop_name, value in props.items():
        # Resolve aliases
        field_name = PROPERTY_ALIASES.get(prop_name, prop_name)

        # Skip if not a valid field
        if field_name not in model_fields:
            continue

        # Default: use value as-is
        model_data[field_name] = value

    # Validate required fields
    if "name" not in model_data:
        return None

    # Initialize empty lists for child objects (will be populated separately)
    model_data.setdefault("dimensions", [])
    model_data.setdefault("relationships", [])
    model_data.setdefault("metrics", [])
    model_data.setdefault("segments", [])

    return Model(**model_data)


def _parse_dimension_def(dimension_def: DimensionDef) -> Dimension | None:
    """Convert DimensionDef expression to Dimension instance.

    Args:
        dimension_def: Parsed DIMENSION() expression

    Returns:
        Dimension instance or None
    """
    props = _extract_properties(dimension_def)

    # Get field names from Dimension model
    dimension_fields = set(Dimension.model_fields.keys())

    dimension_data = {}

    for prop_name, value in props.items():
        # Resolve aliases
        field_name = PROPERTY_ALIASES.get(prop_name, prop_name)

        # Skip if not a valid field
        if field_name not in dimension_fields:
            continue

        # Default: use value as-is
        dimension_data[field_name] = value

    # Validate required fields
    if "name" not in dimension_data or "type" not in dimension_data:
        return None

    return Dimension(**dimension_data)


def _parse_relationship_def(relationship_def: RelationshipDef) -> Relationship | None:
    """Convert RelationshipDef expression to Relationship instance.

    Args:
        relationship_def: Parsed RELATIONSHIP() expression

    Returns:
        Relationship instance or None
    """
    props = _extract_properties(relationship_def)

    # Get field names from Relationship model
    relationship_fields = set(Relationship.model_fields.keys())

    relationship_data = {}

    for prop_name, value in props.items():
        # Resolve aliases
        field_name = PROPERTY_ALIASES.get(prop_name, prop_name)

        # Skip if not a valid field
        if field_name not in relationship_fields:
            continue

        # Default: use value as-is
        relationship_data[field_name] = value

    # Validate required fields
    if "name" not in relationship_data or "type" not in relationship_data:
        return None

    return Relationship(**relationship_data)


def _parse_metric_def(metric_def: MetricDef) -> Metric | None:
    """Convert MetricDef expression to Metric instance.

    Args:
        metric_def: Parsed METRIC() expression

    Returns:
        Metric instance or None
    """
    props = _extract_properties(metric_def)

    name = props.get("name")
    if not name:
        return None

    # Get field names from Metric model
    metric_fields = set(Metric.model_fields.keys())

    metric_data = {}

    for prop_name, value in props.items():
        # Resolve aliases
        field_name = PROPERTY_ALIASES.get(prop_name, prop_name)

        # Skip if not a valid field
        if field_name not in metric_fields:
            continue

        # Type conversions for specific fields
        if field_name in ("filters", "drill_fields"):
            # Convert to list if string
            if isinstance(value, str):
                if value.strip().startswith("["):
                    try:
                        import ast

                        metric_data[field_name] = ast.literal_eval(value)
                    except (ValueError, SyntaxError):
                        metric_data[field_name] = [value]
                else:
                    metric_data[field_name] = [value]
            else:
                metric_data[field_name] = value
        elif field_name == "fill_nulls_with":
            # Try to convert to number
            try:
                metric_data[field_name] = float(value) if "." in str(value) else int(value)
            except (ValueError, TypeError):
                metric_data[field_name] = value
        else:
            # Default: use value as-is
            metric_data[field_name] = value

    return Metric(**metric_data)


def _parse_segment_def(segment_def: SegmentDef) -> Segment | None:
    """Convert SegmentDef expression to Segment instance.

    Args:
        segment_def: Parsed SEGMENT() expression

    Returns:
        Segment instance or None
    """
    props = _extract_properties(segment_def)

    # Get field names from Segment model
    segment_fields = set(Segment.model_fields.keys())

    segment_data = {}

    for prop_name, value in props.items():
        # Resolve aliases
        field_name = PROPERTY_ALIASES.get(prop_name, prop_name)

        # Skip if not a valid field
        if field_name not in segment_fields:
            continue

        # Type conversions for specific fields
        if field_name == "public":
            # Convert string to boolean
            if isinstance(value, str):
                segment_data[field_name] = value.lower() in ("true", "t", "yes", "1")
            else:
                segment_data[field_name] = bool(value)
        else:
            # Default: use value as-is
            segment_data[field_name] = value

    # Validate required fields
    if "name" not in segment_data or "sql" not in segment_data:
        return None

    return Segment(**segment_data)


def _extract_properties(definition: MetricDef | SegmentDef) -> dict[str, str]:
    """Extract property assignments from METRIC/SEGMENT definition.

    Args:
        definition: MetricDef or SegmentDef expression

    Returns:
        Dictionary of property names to values
    """
    props = {}

    for expr in definition.expressions:
        if isinstance(expr, PropertyEQ):
            key = expr.this.name.lower()
            value_expr = expr.expression

            # Extract value from expression
            if isinstance(value_expr, exp.Literal):
                value = value_expr.this
            else:
                # For complex expressions, use SQL representation
                value = value_expr.sql(dialect="duckdb")

            # Strip surrounding quotes for simple string values
            # but preserve them within SQL expressions
            if isinstance(value, str):
                # Only strip outer quotes if the entire value is quoted
                if value.startswith("'") and value.endswith("'") and value.count("'") == 2:
                    value = value[1:-1]

            props[key] = value

    return props
