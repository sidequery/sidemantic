"""Parse SQL-based metric and segment definitions."""

from pathlib import Path

import yaml
from sqlglot import exp

from sidemantic.core.dialect import MetricDef, SegmentDef, PropertyEQ, parse, PROPERTY_ALIASES
from sidemantic.core.metric import Metric
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
                    except:
                        metric_data[field_name] = [value]
                else:
                    metric_data[field_name] = [value]
            else:
                metric_data[field_name] = value
        elif field_name == "fill_nulls_with":
            # Try to convert to number
            try:
                metric_data[field_name] = float(value) if "." in str(value) else int(value)
            except:
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
