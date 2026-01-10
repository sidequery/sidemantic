"""Parse SQL-based metric and segment definitions."""

import re
from pathlib import Path

import yaml
from sqlglot import exp

from sidemantic.core.dialect import (
    PROPERTY_ALIASES,
    DimensionDef,
    MetricDef,
    ModelDef,
    ParameterDef,
    PreAggregationDef,
    PropertyEQ,
    RelationshipDef,
    SegmentDef,
    parse,
)
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.parameter import Parameter
from sidemantic.core.pre_aggregation import Index, PreAggregation, RefreshKey
from sidemantic.core.relationship import Relationship
from sidemantic.core.segment import Segment


def _split_top_level(text: str, delimiter: str = ",") -> list[str]:
    items = []
    depth = 0
    in_quote = None
    escape = False
    buf = []

    for char in text:
        if in_quote:
            buf.append(char)
            if escape:
                escape = False
                continue
            if char == "\\":
                escape = True
                continue
            if char == in_quote:
                in_quote = None
            continue

        if char in ("'", '"'):
            in_quote = char
            buf.append(char)
            continue

        if char in ("[", "{"):
            depth += 1
        elif char in ("]", "}"):
            depth = max(depth - 1, 0)

        if char == delimiter and depth == 0:
            item = "".join(buf).strip()
            if item:
                items.append(item)
            buf = []
            continue

        buf.append(char)

    trailing = "".join(buf).strip()
    if trailing:
        items.append(trailing)

    return items


def _split_key_value(text: str) -> tuple[str, str]:
    depth = 0
    in_quote = None

    for idx, char in enumerate(text):
        if in_quote:
            if char == in_quote:
                in_quote = None
            continue

        if char in ("'", '"'):
            in_quote = char
            continue

        if depth == 0 and char in ("[", "{") and idx > 0:
            return text[:idx].strip(), text[idx:].strip()

        if char in ("[", "{"):
            depth += 1
            continue

        if char in ("]", "}"):
            depth = max(depth - 1, 0)
            continue

        if depth == 0 and char in (":", "="):
            return text[:idx].strip(), text[idx + 1 :].strip()

        if depth == 0 and char.isspace():
            return text[:idx].strip(), text[idx:].strip()

    return text.strip(), ""


def _parse_scalar_literal(value: str) -> object:
    if not value:
        return ""

    if value[0] in ("'", '"') and value[-1] == value[0]:
        inner = value[1:-1]
        if value[0] == "'":
            inner = inner.replace("''", "'")
        return inner

    lowered = value.lower()
    if lowered in ("true", "false"):
        return lowered == "true"
    if lowered in ("null", "none"):
        return None

    if re.match(r"^[+-]?\d+$", value):
        return int(value)
    if re.match(r"^[+-]?\d+\.\d+$", value):
        return float(value)

    return value


def _parse_list_literal(value: str) -> list[object]:
    items = _split_top_level(value)
    return [_parse_literal(item) for item in items if item]


def _parse_object_literal(value: str) -> dict[str, object]:
    pairs = _split_top_level(value)
    obj: dict[str, object] = {}

    for pair in pairs:
        if not pair:
            continue
        key, raw_value = _split_key_value(pair)
        if not key:
            continue
        parsed_key = _parse_scalar_literal(key)
        key_str = str(parsed_key)
        if raw_value:
            obj[key_str] = _parse_literal(raw_value)
        else:
            obj[key_str] = True

    return obj


def _parse_literal(value: str) -> object:
    raw = value.strip()
    if not raw:
        return ""

    if raw.startswith("[") and raw.endswith("]"):
        return _parse_list_literal(raw[1:-1].strip())
    if raw.startswith("{") and raw.endswith("}"):
        return _parse_object_literal(raw[1:-1].strip())

    return _parse_scalar_literal(raw)


def _normalize_list(value: object | None) -> list[object] | None:
    if value is None:
        return None
    if isinstance(value, list):
        return value
    return [value]


def _parse_sql_statements(
    sql: str,
) -> tuple[
    Model | None,
    list[Dimension],
    list[Relationship],
    list[Metric],
    list[Segment],
    list[Parameter],
    list[PreAggregation],
]:
    model_def = None
    dimensions: list[Dimension] = []
    relationships: list[Relationship] = []
    metrics: list[Metric] = []
    segments: list[Segment] = []
    parameters: list[Parameter] = []
    pre_aggregations: list[PreAggregation] = []

    statements = parse(sql)

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
        elif isinstance(stmt, ParameterDef):
            parameter = _parse_parameter_def(stmt)
            if parameter:
                parameters.append(parameter)
        elif isinstance(stmt, PreAggregationDef):
            preagg = _parse_pre_aggregation_def(stmt)
            if preagg:
                pre_aggregations.append(preagg)

    return model_def, dimensions, relationships, metrics, segments, parameters, pre_aggregations


def parse_sql_definitions(sql: str) -> tuple[list[Metric], list[Segment]]:
    """Parse SQL string containing METRIC() and SEGMENT() definitions.

    Args:
        sql: SQL string with definitions

    Returns:
        Tuple of (metrics, segments)
    """
    try:
        metrics, segments, _ = parse_sql_graph_definitions(sql)
    except Exception:
        return [], []

    return metrics, segments


def parse_sql_graph_definitions(sql: str) -> tuple[list[Metric], list[Segment], list[Parameter]]:
    """Parse SQL string containing graph-level definitions.

    Args:
        sql: SQL string with definitions

    Returns:
        Tuple of (metrics, segments, parameters)
    """
    try:
        _, _, _, metrics, segments, parameters, _ = _parse_sql_statements(sql)
    except Exception:
        return [], [], []

    return metrics, segments, parameters


def parse_sql_model(sql: str) -> Model | None:
    """Parse SQL string containing a complete model definition.

    Expects MODEL(), DIMENSION(), RELATIONSHIP(), METRIC(), and SEGMENT() statements.

    Args:
        sql: SQL string with model definition

    Returns:
        Model instance or None
    """
    try:
        model_def, dimensions, relationships, metrics, segments, _, pre_aggregations = _parse_sql_statements(sql)
    except Exception:
        return None

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
    if pre_aggregations:
        model_def.pre_aggregations.extend(pre_aggregations)

    return model_def


def parse_sql_file_with_frontmatter_extended(
    path: Path,
) -> tuple[dict, list[Metric], list[Segment], list[Parameter], list[PreAggregation]]:
    """Parse .sql file with YAML frontmatter and SQL definitions.

    Args:
        path: Path to .sql file

    Returns:
        Tuple of (frontmatter_dict, metrics, segments, parameters, pre_aggregations)
    """
    with open(path) as f:
        content = f.read()

    frontmatter = {}
    sql_body = content

    if content.strip().startswith("---"):
        parts = content.split("---", 2)
        if len(parts) >= 3:
            frontmatter_text = parts[1].strip()
            sql_body = parts[2].strip()

            if frontmatter_text:
                frontmatter = yaml.safe_load(frontmatter_text) or {}

    metrics, segments, parameters = parse_sql_graph_definitions(sql_body)

    pre_aggregations: list[PreAggregation] = []
    try:
        _, _, _, _, _, _, pre_aggregations = _parse_sql_statements(sql_body)
    except Exception:
        pre_aggregations = []

    return frontmatter, metrics, segments, parameters, pre_aggregations


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
    frontmatter, metrics, segments, _, _ = parse_sql_file_with_frontmatter_extended(path)
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

        if field_name in ("filters", "drill_fields"):
            metric_data[field_name] = _normalize_list(value)
        else:
            metric_data[field_name] = value

    return Metric(**metric_data)


def _parse_parameter_def(parameter_def: ParameterDef) -> Parameter | None:
    """Convert ParameterDef expression to Parameter instance."""
    props = _extract_properties(parameter_def)

    name = props.get("name")
    if not name:
        return None

    parameter_fields = set(Parameter.model_fields.keys())
    parameter_data = {}

    for prop_name, value in props.items():
        field_name = PROPERTY_ALIASES.get(prop_name, prop_name)
        if field_name not in parameter_fields:
            continue
        if field_name == "allowed_values":
            parameter_data[field_name] = _normalize_list(value)
        else:
            parameter_data[field_name] = value

    if "name" not in parameter_data or "type" not in parameter_data:
        return None

    return Parameter(**parameter_data)


def _parse_pre_aggregation_def(preagg_def: PreAggregationDef) -> PreAggregation | None:
    """Convert PreAggregationDef expression to PreAggregation instance."""
    props = _extract_properties(preagg_def)

    name = props.get("name")
    if not name:
        return None

    preagg_fields = set(PreAggregation.model_fields.keys())
    preagg_data: dict[str, object] = {}

    for prop_name, value in props.items():
        field_name = PROPERTY_ALIASES.get(prop_name, prop_name)
        if field_name not in preagg_fields:
            continue

        if field_name in ("measures", "dimensions"):
            preagg_data[field_name] = _normalize_list(value)
        elif field_name == "indexes":
            indexes = _normalize_list(value) or []
            parsed_indexes = []
            for idx in indexes:
                if isinstance(idx, Index):
                    parsed_indexes.append(idx)
                elif isinstance(idx, dict):
                    parsed_indexes.append(Index(**idx))
                else:
                    parsed_indexes.append(Index(name=str(idx), columns=[str(idx)]))
            preagg_data[field_name] = parsed_indexes
        elif field_name == "refresh_key":
            if isinstance(value, RefreshKey):
                preagg_data[field_name] = value
            elif isinstance(value, dict):
                preagg_data[field_name] = RefreshKey(**value)
            else:
                preagg_data[field_name] = None
        else:
            preagg_data[field_name] = value

    if "name" not in preagg_data:
        return None

    return PreAggregation(**preagg_data)


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


def _extract_properties(definition: exp.Expression) -> dict[str, object]:
    """Extract property assignments from SQL definitions.

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

            if isinstance(value, str):
                props[key] = _parse_literal(value)
            else:
                props[key] = value

    return props
