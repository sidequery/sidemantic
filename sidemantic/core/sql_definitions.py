"""Parse SQL-based metric and segment definitions."""

import re
from dataclasses import dataclass
from pathlib import Path

import sqlglot
import yaml
from sqlglot import exp

from sidemantic.core.dialect import (
    PROPERTY_ALIASES,
    TableBlockParseError,
    is_dimension_def,
    is_metric_def,
    is_model_def,
    is_parameter_def,
    is_pre_aggregation_def,
    is_property_eq,
    is_relationship_def,
    is_segment_def,
    is_table_block_default_time_def,
    is_table_block_field_def,
    is_table_block_join_def,
    is_table_block_model_def,
    is_table_block_primary_key_def,
    is_table_block_segment_def,
    parse,
)
from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.parameter import Parameter
from sidemantic.core.pre_aggregation import Index, PreAggregation, RefreshKey
from sidemantic.core.relationship import Relationship
from sidemantic.core.segment import Segment
from sidemantic.sql.aggregation_detection import sql_has_aggregate


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


def _normalize_definition_placeholders(value: str) -> str:
    return re.sub(r"\{\s*model\s*\}\s*\.", "{model}.", value)


def _normalize_list(value: object | None) -> list[object] | None:
    if value is None:
        return None
    if isinstance(value, list):
        return value
    return [value]


_TIME_GRAINS = {"second", "minute", "hour", "day", "week", "month", "quarter", "year"}


@dataclass(frozen=True)
class TableBlockFieldAnnotation:
    dimension_type: str | None = None
    granularity: str | None = None


def parse_sql_models(sql: str) -> list[Model]:
    """Parse SQL string containing one or more complete model definitions."""
    try:
        statements = parse(sql)
    except TableBlockParseError:
        raise
    except Exception:
        return []

    if any(is_table_block_model_def(stmt) or is_model_def(stmt) for stmt in statements):
        return _parse_mixed_sql_models(statements)
    return []


def _parse_mixed_sql_models(statements: list[exp.Expression | None]) -> list[Model]:
    models = []
    legacy_statements: list[exp.Expression | None] = []

    def flush_legacy_model() -> None:
        if not legacy_statements:
            return

        model_def, dimensions, relationships, metrics, segments, _, pre_aggregations = _parse_statement_defs(
            legacy_statements
        )
        legacy_statements.clear()
        if not model_def:
            return

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

        models.append(model_def)

    for statement in statements:
        if is_table_block_model_def(statement):
            flush_legacy_model()
            models.append(_parse_table_block_model_def(statement))
            continue

        if is_model_def(statement):
            flush_legacy_model()
            legacy_statements.append(statement)
            continue

        if legacy_statements and (
            is_dimension_def(statement)
            or is_relationship_def(statement)
            or is_metric_def(statement)
            or is_segment_def(statement)
            or is_pre_aggregation_def(statement)
        ):
            legacy_statements.append(statement)

    flush_legacy_model()
    return models


def _parse_table_block_model_def(model_def: exp.Expression) -> Model:
    model_name = model_def.args["model_name"].name
    table = model_def.args.get("table")
    source_sql = model_def.args.get("source_sql")
    primary_key: str | list[str] = "id"
    default_time_dimension: str | None = None
    default_grain: str | None = None
    relationships: list[Relationship] = []
    segments: list[Segment] = []
    field_declarations: list[tuple[int, str, str, TableBlockFieldAnnotation | None]] = []
    parsed_fields: list[tuple[int, str, Dimension | Metric]] = []
    seen_fields: set[str] = set()
    seen_segments: set[str] = set()
    seen_primary_key = False
    seen_default_time = False

    for statement_idx, statement in enumerate(model_def.expressions):
        if is_table_block_primary_key_def(statement):
            if seen_primary_key:
                raise TableBlockParseError(f"Model '{model_name}' defines primary key more than once")
            seen_primary_key = True
            primary_key = _collapse_table_block_key_columns(statement.args["columns"])
            continue

        if is_table_block_default_time_def(statement):
            if seen_default_time:
                raise TableBlockParseError(f"Model '{model_name}' defines default time more than once")
            seen_default_time = True
            default_time_dimension = statement.args["field"].name
            default_grain = statement.args.get("grain")
            continue

        if is_table_block_segment_def(statement):
            segment = Segment(name=statement.args["name"].name, sql=statement.args["sql"])
            if segment.name in seen_segments:
                raise TableBlockParseError(f"Model '{model_name}' defines segment '{segment.name}' more than once")
            seen_segments.add(segment.name)
            segments.append(segment)
            continue

        if is_table_block_join_def(statement):
            relationship = _parse_table_block_join_def(statement)
            relationships.append(relationship)
            continue

        if is_table_block_field_def(statement):
            name = statement.args["name"].name
            if name in seen_fields:
                raise TableBlockParseError(f"Model '{model_name}' defines field '{name}' more than once")
            seen_fields.add(name)
            annotation = _table_block_field_annotation(statement)
            field_declarations.append((statement_idx, name, statement.args["sql"], annotation))
            continue

        raise TableBlockParseError(f"Unrecognized statement in model '{model_name}': {statement.sql()}")

    parsed_fields.extend(_classify_table_block_fields(model_name, field_declarations))
    parsed_fields.sort(key=lambda item: item[0])

    dimensions = [field for _, kind, field in parsed_fields if kind == "dimension"]
    metrics = [field for _, kind, field in parsed_fields if kind == "metric"]
    _validate_table_block_default_time(model_name, default_time_dimension, dimensions)

    return Model(
        name=model_name,
        table=table,
        sql=source_sql,
        primary_key=primary_key,
        dimensions=dimensions,
        relationships=relationships,
        metrics=metrics,
        segments=segments,
        default_time_dimension=default_time_dimension,
        default_grain=default_grain,
    )


def _table_block_field_annotation(field_def: exp.Expression) -> TableBlockFieldAnnotation | None:
    dimension_type = field_def.args.get("dimension_type")
    granularity = field_def.args.get("granularity")
    if not dimension_type and not granularity:
        return None
    return TableBlockFieldAnnotation(dimension_type=dimension_type, granularity=granularity)


def _parse_table_block_join_def(join_def: exp.Expression) -> Relationship:
    relationship_type = join_def.args["relationship_type"]
    local_keys = join_def.args["local_keys"]
    target_keys = join_def.args["target_keys"]
    if relationship_type in ("many_to_one", "one_to_one"):
        return Relationship(
            name=join_def.args["target"].name,
            type=relationship_type,
            foreign_key=_collapse_table_block_key_columns(local_keys),
            primary_key=_collapse_table_block_key_columns(target_keys),
        )

    return Relationship(
        name=join_def.args["target"].name,
        type=relationship_type,
        foreign_key=_collapse_table_block_key_columns(target_keys),
        primary_key=_collapse_table_block_key_columns(local_keys),
    )


def _validate_table_block_default_time(
    model_name: str,
    default_time_dimension: str | None,
    dimensions: list[Dimension],
) -> None:
    if not default_time_dimension:
        return

    dimension_by_name = {dimension.name: dimension for dimension in dimensions}
    dimension = dimension_by_name.get(default_time_dimension)
    if not dimension:
        raise TableBlockParseError(
            f"Default time dimension '{default_time_dimension}' in model '{model_name}' is not defined"
        )
    if dimension.type != "time":
        raise TableBlockParseError(
            f"Default time dimension '{default_time_dimension}' in model '{model_name}' must be a time dimension"
        )


def _classify_table_block_fields(
    model_name: str, field_declarations: list[tuple[int, str, str, TableBlockFieldAnnotation | None]]
) -> list[tuple[int, str, Dimension | Metric]]:
    parsed_fields: list[tuple[int, str, Dimension | Metric]] = []
    pending_fields: list[tuple[int, str, str, TableBlockFieldAnnotation | None]] = []
    metric_names: set[str] = set()
    field_names = {field_name for _, field_name, _, _ in field_declarations}

    for statement_idx, name, expression, annotation in field_declarations:
        if sql_has_aggregate(expression, dialect="duckdb"):
            _validate_no_dimension_annotation_on_metric(model_name, name, annotation)
            metric = Metric(name=name, sql=expression)
            parsed_fields.append((statement_idx, "metric", metric))
            metric_names.add(name)
        else:
            pending_fields.append((statement_idx, name, expression, annotation))

    changed = True
    while changed and pending_fields:
        changed = False
        next_pending: list[tuple[int, str, str, TableBlockFieldAnnotation | None]] = []

        for statement_idx, name, expression, annotation in pending_fields:
            if _expression_references_metric(expression, metric_names):
                _validate_no_dimension_annotation_on_metric(model_name, name, annotation)
                metric = Metric(name=name, type="derived", sql=expression)
                parsed_fields.append((statement_idx, "metric", metric))
                metric_names.add(name)
                changed = True
            else:
                next_pending.append((statement_idx, name, expression, annotation))

        pending_fields = next_pending

    for statement_idx, name, expression, annotation in pending_fields:
        if _expression_references_declared_field(expression, field_names):
            _validate_table_block_dimension_expression(model_name, name, expression, metric_names)
        parsed_fields.append((statement_idx, "dimension", _parse_table_block_dimension(name, expression, annotation)))

    return parsed_fields


def _validate_no_dimension_annotation_on_metric(
    model_name: str,
    field_name: str,
    annotation: TableBlockFieldAnnotation | None,
) -> None:
    if annotation and annotation.dimension_type:
        raise TableBlockParseError(
            f"Field '{field_name}' in model '{model_name}' is a metric and cannot use dimension annotation"
        )


def _parse_table_block_dimension(
    name: str,
    expression: str,
    annotation: TableBlockFieldAnnotation | None = None,
) -> Dimension:
    dimension_type = (
        annotation.dimension_type
        if annotation and annotation.dimension_type
        else _infer_dimension_type(name, expression)
    )
    granularity = None
    if dimension_type == "time":
        granularity = (
            annotation.granularity if annotation and annotation.granularity else _infer_time_granularity(expression)
        )

    return Dimension(
        name=name,
        type=dimension_type,
        sql=None if expression == name else expression,
        granularity=granularity,
    )


def _collapse_table_block_key_columns(columns: list[str]) -> str | list[str]:
    if len(columns) == 1:
        return columns[0]
    return columns


def _expression_references_metric(expression: str, metric_names: set[str]) -> bool:
    if not metric_names:
        return False

    try:
        parsed = sqlglot.parse_one(expression, read="duckdb")
    except Exception:
        tokens = set(re.findall(r"\b[A-Za-z_][A-Za-z0-9_]*\b", expression))
        return bool(tokens & metric_names)

    return any(column.name in metric_names for column in parsed.find_all(exp.Column))


def _expression_references_declared_field(expression: str, field_names: set[str]) -> bool:
    if not field_names:
        return False

    try:
        parsed = sqlglot.parse_one(expression, read="duckdb")
    except Exception:
        tokens = set(re.findall(r"\b[A-Za-z_][A-Za-z0-9_]*\b", expression))
        return bool(tokens & field_names)

    return any(column.name in field_names for column in parsed.find_all(exp.Column))


def _validate_table_block_dimension_expression(
    model_name: str,
    field_name: str,
    expression: str,
    metric_names: set[str],
) -> None:
    referenced_metric_names = _referenced_metric_names(expression, metric_names)
    if referenced_metric_names:
        refs = ", ".join(sorted(referenced_metric_names))
        raise TableBlockParseError(
            f"Field '{field_name}' in model '{model_name}' references metric(s) {refs} but could not be classified"
        )


def _referenced_metric_names(expression: str, metric_names: set[str]) -> set[str]:
    if not metric_names:
        return set()

    try:
        parsed = sqlglot.parse_one(expression, read="duckdb")
    except Exception:
        tokens = set(re.findall(r"\b[A-Za-z_][A-Za-z0-9_]*\b", expression))
        return tokens & metric_names

    return {column.name for column in parsed.find_all(exp.Column) if column.name in metric_names}


def _infer_dimension_type(name: str, expression: str) -> str:
    lowered_name = name.lower()
    lowered_expression = expression.lower()

    if _looks_like_time_expression(lowered_name, lowered_expression):
        return "time"

    try:
        parsed = sqlglot.parse_one(expression, read="duckdb")
    except Exception:
        return _infer_dimension_type_from_text(lowered_name, lowered_expression)

    if isinstance(parsed, (exp.Predicate, exp.Boolean)):
        return "boolean"

    if any(isinstance(node, (exp.Add, exp.Sub, exp.Mul, exp.Div, exp.Mod)) for node in parsed.walk()):
        return "numeric"

    return _infer_dimension_type_from_text(lowered_name, lowered_expression)


def _infer_dimension_type_from_text(name: str, expression: str) -> str:
    if _looks_like_time_expression(name, expression):
        return "time"
    if re.search(r"\s(=|<>|!=|>|<|>=|<=)\s|\bis\s+not\b|\bis\b|\bin\s*\(", expression):
        return "boolean"
    if any(operator in expression for operator in (" + ", " - ", " * ", " / ")):
        return "numeric"
    return "categorical"


def _looks_like_time_expression(name: str, expression: str) -> bool:
    time_markers = ("date", "time", "timestamp", "_at", "created", "updated")
    return (
        "date_trunc" in expression
        or "::date" in expression
        or "::timestamp" in expression
        or any(marker in name for marker in time_markers)
    )


def _infer_time_granularity(expression: str) -> str | None:
    try:
        parsed = sqlglot.parse_one(expression, read="duckdb")
    except Exception:
        match = re.search(r"date_trunc\s*\(\s*['\"]([A-Za-z_]+)['\"]", expression, flags=re.IGNORECASE)
        if match and match.group(1).lower() in _TIME_GRAINS:
            return match.group(1).lower()
        return None

    for node in parsed.walk():
        if not node.__class__.__name__.lower().endswith("trunc"):
            continue
        unit = node.args.get("unit")
        if not unit:
            continue

        if isinstance(unit, (exp.Literal, exp.Var)):
            grain = str(unit.this).lower()
        else:
            grain = unit.sql(dialect="duckdb").strip("'\"").lower()

        if grain in _TIME_GRAINS:
            return grain

    return None


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
    return _parse_statement_defs(parse(sql))


def _parse_statement_defs(
    statements: list[exp.Expression | None],
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

    for stmt in statements:
        if stmt is None:
            continue
        if is_table_block_model_def(stmt):
            model_def = _parse_table_block_model_def(stmt)
        elif is_model_def(stmt):
            model_def = _parse_model_def(stmt)
        elif is_dimension_def(stmt):
            dimension = _parse_dimension_def(stmt)
            if dimension:
                dimensions.append(dimension)
        elif is_relationship_def(stmt):
            relationship = _parse_relationship_def(stmt)
            if relationship:
                relationships.append(relationship)
        elif is_metric_def(stmt):
            metric = _parse_metric_def(stmt)
            if metric:
                metrics.append(metric)
        elif is_segment_def(stmt):
            segment = _parse_segment_def(stmt)
            if segment:
                segments.append(segment)
        elif is_parameter_def(stmt):
            parameter = _parse_parameter_def(stmt)
            if parameter:
                parameters.append(parameter)
        elif is_pre_aggregation_def(stmt):
            preagg = _parse_pre_aggregation_def(stmt)
            if preagg:
                pre_aggregations.append(preagg)
        else:
            raise ValueError(f"Unsupported SQL definition statement: {stmt.__class__.__name__}")

    return model_def, dimensions, relationships, metrics, segments, parameters, pre_aggregations


def parse_sql_definitions(sql: str) -> tuple[list[Metric], list[Segment]]:
    """Parse SQL string containing METRIC() and SEGMENT() definitions.

    Args:
        sql: SQL string with definitions

    Returns:
        Tuple of (metrics, segments)
    """
    metrics, segments, _ = parse_sql_graph_definitions(sql)
    return metrics, segments


def parse_sql_graph_definitions(sql: str) -> tuple[list[Metric], list[Segment], list[Parameter]]:
    """Parse SQL string containing graph-level definitions.

    Args:
        sql: SQL string with definitions

    Returns:
        Tuple of (metrics, segments, parameters)
    """
    _, _, _, metrics, segments, parameters, _ = _parse_sql_statements(sql)
    return metrics, segments, parameters


def parse_sql_model(sql: str) -> Model | None:
    """Parse SQL string containing a complete model definition.

    Supports both compact ``model name from table (...)`` blocks and the legacy
    MODEL(), DIMENSION(), RELATIONSHIP(), METRIC(), and SEGMENT() statements.

    Args:
        sql: SQL string with model definition

    Returns:
        Model instance or None
    """
    models = parse_sql_models(sql)
    if not models:
        return None
    return models[0]


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

    _, _, _, metrics, segments, parameters, pre_aggregations = _parse_sql_statements(sql_body)

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


def _parse_model_def(model_def: exp.Expression) -> Model | None:
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


def _parse_dimension_def(dimension_def: exp.Expression) -> Dimension | None:
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


def _parse_relationship_def(relationship_def: exp.Expression) -> Relationship | None:
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


def _parse_metric_def(metric_def: exp.Expression) -> Metric | None:
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


def _parse_parameter_def(parameter_def: exp.Expression) -> Parameter | None:
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


def _parse_pre_aggregation_def(preagg_def: exp.Expression) -> PreAggregation | None:
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


def _parse_segment_def(segment_def: exp.Expression) -> Segment | None:
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
        if is_property_eq(expr):
            key = expr.this.name.lower()
            value_expr = expr.expression

            # Extract value from expression
            if isinstance(value_expr, exp.Literal):
                value = value_expr.this
            else:
                # For complex expressions, use SQL representation
                value = value_expr.sql(dialect="duckdb")

            if isinstance(value, str):
                parsed_value = _parse_literal(value)
                if isinstance(parsed_value, str):
                    parsed_value = _normalize_definition_placeholders(parsed_value)
                props[key] = parsed_value
            else:
                props[key] = value

    return props
