"""MCP server for Sidemantic semantic layer."""

import json
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal

from mcp.server.fastmcp import FastMCP

from sidemantic.core.semantic_layer import SemanticLayer
from sidemantic.loaders import load_from_directory

# Global semantic layer instance
_layer: SemanticLayer | None = None
_apps_enabled: bool = False


def initialize_layer(
    directory: str,
    db_path: str | None = None,
    init_sql: list[str] | None = None,
) -> SemanticLayer:
    """Initialize the semantic layer with models from directory."""
    global _layer

    # Create connection string
    connection = None
    if db_path:
        if db_path == ":memory:":
            connection = "duckdb:///:memory:"
        else:
            connection = f"duckdb:///{Path(db_path).absolute()}"

    _layer = SemanticLayer(connection=connection, init_sql=init_sql)
    load_from_directory(_layer, directory)
    return _layer


def get_layer() -> SemanticLayer:
    """Get the initialized semantic layer."""
    if _layer is None:
        raise RuntimeError(
            "Semantic layer not initialized. The MCP server must be started via "
            "'sidemantic mcp-serve <directory>' which loads models before serving."
        )
    return _layer


def _convert_to_json_compatible(value: Any) -> Any:
    """Convert value to JSON-compatible type.

    Handles Decimal, date, datetime, and other non-JSON-serializable types
    from database results.
    """
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, time):
        return value.isoformat()
    return value


def _validate_filter(filter_str: str, dialect: str | None = None) -> None:
    """Validate a filter string to prevent SQL injection.

    Parses the filter as a SQL expression using the active dialect and rejects
    DDL/DML statements and multi-statement input.
    """
    import sqlglot

    # Detect multi-statement input via sqlglot parsing (not raw string check,
    # which would false-positive on semicolons inside string literals).
    try:
        statements = sqlglot.parse(f"SELECT 1 WHERE {filter_str}", dialect=dialect)
    except Exception:
        raise ValueError(f"Invalid filter expression: {filter_str}")

    if len(statements) > 1:
        raise ValueError("Filter contains disallowed SQL: multi-statement input")

    parsed = statements[0]
    if parsed is None:
        raise ValueError(f"Invalid filter expression: {filter_str}")

    # Build the disallowed SQL node types defensively because sqlglot class
    # names can vary by version (e.g., AlterTable vs Alter).
    disallowed_type_names = (
        "Drop",
        "Insert",
        "Delete",
        "Update",
        "Create",
        "Command",
        "AlterTable",
        "Alter",
    )
    disallowed_types = tuple(
        expr_type
        for type_name in disallowed_type_names
        if (expr_type := getattr(sqlglot.exp, type_name, None)) is not None
    )

    # Walk the parse tree and reject DDL/DML nodes
    for node in parsed.walk():
        if disallowed_types and isinstance(node, disallowed_types):
            raise ValueError(f"Filter contains disallowed SQL: {type(node).__name__}")


def _format_join_condition(model_name: str, rel, models: dict[str, Any]) -> str | None:
    related_name = rel.name
    related_model = models.get(related_name)
    if not related_model:
        return None

    if rel.type == "many_to_one":
        fk = rel.foreign_key or f"{related_name}_id"
        pk = rel.primary_key or related_model.primary_key
        return f"{model_name}.{fk} = {related_name}.{pk}"

    if rel.type in ("one_to_many", "one_to_one"):
        if not rel.foreign_key:
            return None
        pk = models[model_name].primary_key
        return f"{related_name}.{rel.foreign_key} = {model_name}.{pk}"

    if rel.type == "many_to_many":
        if rel.through:
            junction_model = models.get(rel.through)
            if not junction_model:
                return None
            junction_self_fk, junction_related_fk = rel.junction_keys()
            if not junction_self_fk or not junction_related_fk:
                return None
            base_pk = models[model_name].primary_key
            related_pk = rel.primary_key or related_model.primary_key
            return (
                f"{model_name}.{base_pk} = {rel.through}.{junction_self_fk} "
                f"AND {rel.through}.{junction_related_fk} = {related_name}.{related_pk}"
            )
        if rel.foreign_key:
            base_pk = models[model_name].primary_key
            return f"{model_name}.{base_pk} = {related_name}.{rel.foreign_key}"

    return None


# Create MCP server
mcp = FastMCP("sidemantic")


@mcp.tool()
def get_models(model_names: list[str]) -> list[dict[str, Any]]:
    """Get detailed information about one or more models.

    Returns full definitions including all dimensions (with types, SQL, granularity),
    metrics (with aggregation, SQL, filters), segments, relationships with join
    conditions, and model metadata. Use get_semantic_graph first to discover model
    names, then this tool to get the details you need for query construction.

    Args:
        model_names: List of model names to retrieve details for

    Returns:
        Detailed information for each requested model, including dimensions,
        metrics, segments, relationships, and metadata.
    """
    layer = get_layer()

    details = []
    for model_name in model_names:
        if model_name not in layer.graph.models:
            continue

        model = layer.graph.models[model_name]

        # Get dimension details (enriched)
        dims = []
        for dim in model.dimensions:
            dim_info: dict[str, Any] = {
                "name": dim.name,
                "type": dim.type,
                "sql": dim.sql,
            }
            if dim.description:
                dim_info["description"] = dim.description
            if dim.label:
                dim_info["label"] = dim.label
            if dim.granularity:
                dim_info["granularity"] = dim.granularity
            if dim.supported_granularities:
                dim_info["supported_granularities"] = dim.supported_granularities
            if dim.parent:
                dim_info["parent"] = dim.parent
                dim_info["hierarchy_path"] = model.get_hierarchy_path(dim.name)
            if dim.format:
                dim_info["format"] = dim.format
            if dim.value_format_name:
                dim_info["value_format_name"] = dim.value_format_name
            if dim.meta:
                dim_info["meta"] = dim.meta
            dims.append(dim_info)

        # Get metric details (enriched)
        metrics = []
        for metric in model.metrics:
            metric_info: dict[str, Any] = {
                "name": metric.name,
                "sql": metric.sql,
            }
            if metric.agg:
                metric_info["agg"] = metric.agg
            if metric.type:
                metric_info["type"] = metric.type
            if metric.description:
                metric_info["description"] = metric.description
            if metric.label:
                metric_info["label"] = metric.label
            if metric.filters:
                metric_info["filters"] = metric.filters
            if metric.format:
                metric_info["format"] = metric.format
            if metric.value_format_name:
                metric_info["value_format_name"] = metric.value_format_name
            # Ratio fields
            if metric.numerator:
                metric_info["numerator"] = metric.numerator
            if metric.denominator:
                metric_info["denominator"] = metric.denominator
            # Cumulative fields
            if metric.window:
                metric_info["window"] = metric.window
            if metric.grain_to_date:
                metric_info["grain_to_date"] = metric.grain_to_date
            # Time comparison fields
            if metric.base_metric:
                metric_info["base_metric"] = metric.base_metric
            if metric.comparison_type:
                metric_info["comparison_type"] = metric.comparison_type
            # Conversion fields
            if metric.entity:
                metric_info["entity"] = metric.entity
            if metric.base_event:
                metric_info["base_event"] = metric.base_event
            if metric.conversion_event:
                metric_info["conversion_event"] = metric.conversion_event
            if metric.drill_fields:
                metric_info["drill_fields"] = metric.drill_fields
            if metric.non_additive_dimension:
                metric_info["non_additive_dimension"] = metric.non_additive_dimension
            if metric.meta:
                metric_info["meta"] = metric.meta
            metrics.append(metric_info)

        # Get segment details
        segments = []
        for seg in model.segments:
            seg_info: dict[str, Any] = {
                "name": seg.name,
                "sql": seg.sql,
            }
            if seg.description:
                seg_info["description"] = seg.description
            if not seg.public:
                seg_info["public"] = False
            segments.append(seg_info)

        # Get relationship details
        rels = []
        for rel in model.relationships:
            rel_info: dict[str, Any] = {
                "name": rel.name,
                "type": rel.type,
            }
            if rel.foreign_key:
                rel_info["foreign_key"] = rel.foreign_key
            if rel.primary_key:
                rel_info["primary_key"] = rel.primary_key
            if rel.through:
                rel_info["through"] = rel.through
            if rel.through_foreign_key:
                rel_info["through_foreign_key"] = rel.through_foreign_key
            if rel.related_foreign_key:
                rel_info["related_foreign_key"] = rel.related_foreign_key
            join_condition = _format_join_condition(model_name, rel, layer.graph.models)
            if join_condition:
                rel_info["join_condition"] = join_condition
            rels.append(rel_info)

        detail: dict[str, Any] = {
            "name": model_name,
            "table": model.table,
            "primary_key": model.primary_key,
            "dimensions": dims,
            "metrics": metrics,
            "relationships": rels,
        }
        if segments:
            detail["segments"] = segments
        if model.description:
            detail["description"] = model.description
        if model.sql:
            detail["sql"] = model.sql
        if model.default_time_dimension:
            detail["default_time_dimension"] = model.default_time_dimension
        if model.default_grain:
            detail["default_grain"] = model.default_grain
        if model.meta:
            detail["meta"] = model.meta
        if source_format := getattr(model, "_source_format", None):
            detail["source_format"] = source_format
        if source_file := getattr(model, "_source_file", None):
            detail["source_file"] = source_file

        details.append(detail)

    return details


@mcp.tool()
def run_query(
    dimensions: list[str] | None = None,
    metrics: list[str] | None = None,
    where: str | None = None,
    segments: list[str] | None = None,
    order_by: list[str] | None = None,
    limit: int | None = None,
    offset: int | None = None,
    ungrouped: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Run a structured query against the semantic layer.

    Reference fields as model.field_name (e.g., "orders.total_revenue"). Time
    dimensions support granularity suffixes: "orders.created_at__month" (year,
    quarter, month, week, day, hour). Referencing fields from multiple models
    triggers automatic joins via model relationships.

    Args:
        dimensions: Dimension references to group by (e.g., ["orders.status", "orders.created_at__month"])
        metrics: Metric references to aggregate (e.g., ["orders.total_revenue"])
        where: SQL WHERE clause using model.field_name (e.g., "orders.status = 'completed' AND orders.amount > 100")
        segments: Named reusable filters defined on models (e.g., ["orders.completed_orders"])
        order_by: Fields to sort by with optional direction (e.g., ["orders.total_revenue desc"])
        limit: Maximum rows to return
        offset: Rows to skip (for pagination, use with limit)
        ungrouped: If True, return raw rows without GROUP BY aggregation
        dry_run: If True, return only the generated SQL without executing

    Returns:
        sql: Generated SQL query.
        rows: Result rows as list of dicts (omitted when dry_run=True).
        row_count: Number of rows returned (omitted when dry_run=True).
    """
    layer = get_layer()

    # Validate filter to prevent SQL injection
    if where:
        _validate_filter(where, dialect=layer.dialect)

    # Compile SQL
    sql = layer.compile(
        dimensions=dimensions or [],
        metrics=metrics or [],
        filters=[where] if where else None,
        segments=segments,
        order_by=order_by,
        limit=limit,
        offset=offset,
        ungrouped=ungrouped,
    )

    if dry_run:
        return {"sql": sql}

    # Execute query via adapter (works with all database backends)
    result = layer.adapter.execute(sql)

    # Convert to list of dicts with JSON-compatible values
    rows = result.fetchall()
    columns = [desc[0] for desc in result.description]
    row_dicts = [{col: _convert_to_json_compatible(val) for col, val in zip(columns, row)} for row in rows]

    return {
        "sql": sql,
        "rows": row_dicts,
        "row_count": len(row_dicts),
    }


@mcp.tool(meta={"ui": {"resourceUri": "ui://sidemantic/chart"}})
def create_chart(
    dimensions: list[str] | None = None,
    metrics: list[str] | None = None,
    where: str | None = None,
    segments: list[str] | None = None,
    order_by: list[str] | None = None,
    limit: int | None = None,
    offset: int | None = None,
    chart_type: Literal["auto", "bar", "line", "area", "scatter", "point"] = "auto",
    title: str | None = None,
    width: int = 600,
    height: int = 400,
) -> dict[str, Any]:
    """Generate a chart from a semantic layer query, producing a Vega-Lite spec and PNG.

    Query parameters work the same as run_query (model.field_name references,
    time granularity suffixes, automatic joins, segments).

    When chart_type is "auto", the chart type is inferred: time dimensions produce
    line/area charts, categorical dimensions produce bar charts, two numeric
    dimensions produce scatter plots.

    Args:
        dimensions: List of dimension references (e.g., ["orders.created_at__month"])
        metrics: List of metric references (e.g., ["orders.total_revenue"])
        where: Optional WHERE clause filter using model.field_name syntax
        segments: Optional list of segment references to apply
        order_by: List of fields to order by with optional "asc" or "desc"
        limit: Optional row limit
        offset: Optional number of rows to skip
        chart_type: "auto", "bar", "line", "area", "scatter", or "point"
        title: Chart title (auto-generated from field names if not provided)
        width: Chart width in pixels (default: 600)
        height: Chart height in pixels (default: 400)

    Returns:
        sql: Generated SQL query
        vega_spec: Vega-Lite JSON specification (renderable client-side)
        png_base64: Base64-encoded PNG image
        row_count: Number of data points
    """
    from sidemantic.charts import chart_to_base64_png, chart_to_vega
    from sidemantic.charts import create_chart as make_chart

    layer = get_layer()

    # Validate filter to prevent SQL injection
    if where:
        _validate_filter(where, dialect=layer.dialect)

    # Compile and execute query (same as run_query)
    sql = layer.compile(
        dimensions=dimensions or [],
        metrics=metrics or [],
        filters=[where] if where else None,
        segments=segments,
        order_by=order_by,
        limit=limit,
        offset=offset,
    )

    result = layer.adapter.execute(sql)
    rows = result.fetchall()
    columns = [desc[0] for desc in result.description]
    row_dicts = [{col: _convert_to_json_compatible(val) for col, val in zip(columns, row)} for row in rows]

    if not row_dicts:
        raise ValueError(
            "Query returned no data. Check your filter conditions and ensure "
            "the referenced dimensions/metrics contain data. Use validate_query "
            "to verify field references, or run_query to inspect results first."
        )

    # Auto-generate title if not provided
    if title is None:
        title = _generate_chart_title(dimensions or [], metrics or [])

    # Create chart with beautiful defaults
    chart = make_chart(
        data=row_dicts,
        chart_type=chart_type,
        title=title,
        width=width,
        height=height,
    )

    # Export to both formats
    vega_spec = chart_to_vega(chart)
    png_base64 = chart_to_base64_png(chart)

    result = {
        "sql": sql,
        "vega_spec": vega_spec,
        "png_base64": png_base64,
        "row_count": len(row_dicts),
    }

    # When apps mode is enabled, include an interactive UI widget
    if _apps_enabled:
        from sidemantic.apps import create_chart_resource

        return [result, create_chart_resource(vega_spec)]

    return result


def _generate_chart_title(dimensions: list[str], metrics: list[str]) -> str:
    """Generate a descriptive title from query parameters."""
    if not metrics:
        return "Data Visualization"

    # Format metric names
    metric_names = [_format_field_name(m) for m in metrics]

    if len(metric_names) == 1:
        title = metric_names[0]
    elif len(metric_names) == 2:
        title = f"{metric_names[0]} & {metric_names[1]}"
    else:
        title = f"{metric_names[0]} & {len(metric_names) - 1} more"

    # Add dimension context if present
    if dimensions:
        dim_name = _format_field_name(dimensions[0])
        title = f"{title} by {dim_name}"

    return title


def _format_field_name(field: str) -> str:
    """Format field name for display (remove model prefix, format as title)."""
    # Remove model prefix
    if "." in field:
        _, field = field.rsplit(".", 1)

    # Handle granularity suffix
    if "__" in field:
        base, granularity = field.rsplit("__", 1)
        field = f"{base} ({granularity})"

    # Convert to title case
    return field.replace("_", " ").title()


@mcp.tool()
def run_sql(query: str) -> dict[str, Any]:
    """Execute a SQL query rewritten through the semantic layer.

    Write SQL referencing model fields (e.g., SELECT revenue, status FROM orders)
    and sidemantic rewrites it using semantic layer definitions with proper
    aggregations and joins. Supports SELECT, WHERE, ORDER BY, LIMIT, CTEs,
    subqueries, time granularity suffixes, and "FROM metrics" for cross-model queries.
    Joins are automatic; explicit JOIN syntax is not supported.

    Args:
        query: SQL query referencing semantic layer models/fields.
            Examples:
            - "SELECT revenue, status FROM orders"
            - "SELECT orders.revenue, customers.region FROM metrics"
            - "SELECT order_date__month, revenue FROM orders ORDER BY order_date__month"

    Returns:
        sql: The rewritten SQL that was executed
        original_sql: The original SQL you provided
        rows: Result rows as list of dicts
        row_count: Number of rows returned
    """
    from sidemantic.sql.query_rewriter import QueryRewriter

    layer = get_layer()
    rewriter = QueryRewriter(layer.graph, dialect=layer.dialect)
    rewritten_sql = rewriter.rewrite(query)

    result = layer.adapter.execute(rewritten_sql)
    rows = result.fetchall()
    columns = [desc[0] for desc in result.description]
    row_dicts = [{col: _convert_to_json_compatible(val) for col, val in zip(columns, row)} for row in rows]

    return {
        "sql": rewritten_sql,
        "original_sql": query,
        "rows": row_dicts,
        "row_count": len(row_dicts),
    }


@mcp.tool()
def validate_query(
    dimensions: list[str] | None = None,
    metrics: list[str] | None = None,
) -> dict[str, Any]:
    """Validate dimension and metric references before running a query.

    Checks that all referenced models, dimensions, and metrics exist, and that
    join paths exist between all referenced models. Use this to catch errors before
    executing a query.

    Args:
        dimensions: List of dimension references to validate (e.g., ["orders.status", "customers.region"])
        metrics: List of metric references to validate (e.g., ["orders.total_revenue"])

    Returns:
        Dictionary with:
        - valid: True if all references are valid, False otherwise
        - errors: List of validation error messages (empty if valid)
    """
    from sidemantic.validation import validate_query as _validate_query

    layer = get_layer()

    errors = _validate_query(
        metrics=metrics or [],
        dimensions=dimensions or [],
        graph=layer.graph,
    )

    return {
        "valid": len(errors) == 0,
        "errors": errors,
    }


@mcp.tool()
def get_semantic_graph() -> dict[str, Any]:
    """Discover the semantic layer: all models, relationships, and available fields.

    Start here to understand what data is available. Returns every model with its
    dimension/metric/segment names, inter-model relationships, graph-level metrics,
    and which model pairs can be joined. Use get_models for full field definitions.

    Returns:
        models: List of model summaries (name, table, dimensions, metrics, segments, relationships).
        graph_metrics: Graph-level metrics not attached to a single model (if any).
        joinable_pairs: Model pairs that can be joined, with hop count.
    """
    layer = get_layer()
    graph = layer.graph

    # Model summaries
    models = []
    for model_name, model in graph.models.items():
        model_info: dict[str, Any] = {
            "name": model_name,
            "table": model.table,
            "dimensions": [d.name for d in model.dimensions],
            "metrics": [m.name for m in model.metrics],
            "relationships": [{"name": r.name, "type": r.type} for r in model.relationships],
        }
        if model.description:
            model_info["description"] = model.description
        if model.segments:
            model_info["segments"] = [s.name for s in model.segments]
        if model.primary_key:
            model_info["primary_key"] = model.primary_key
        if model.default_time_dimension:
            model_info["default_time_dimension"] = model.default_time_dimension
        models.append(model_info)

    # Graph-level metrics
    graph_metrics = []
    for metric_name, metric in graph.metrics.items():
        metric_info: dict[str, Any] = {
            "name": metric_name,
        }
        if metric.type:
            metric_info["type"] = metric.type
        if metric.description:
            metric_info["description"] = metric.description
        if metric.sql:
            metric_info["sql"] = metric.sql
        if metric.base_metric:
            metric_info["base_metric"] = metric.base_metric
        if metric.comparison_type:
            metric_info["comparison_type"] = metric.comparison_type
        if metric.numerator:
            metric_info["numerator"] = metric.numerator
        if metric.denominator:
            metric_info["denominator"] = metric.denominator
        graph_metrics.append(metric_info)

    # Discover joinable model pairs
    model_names = list(graph.models.keys())
    joinable_pairs = []
    for i, model_a in enumerate(model_names):
        for model_b in model_names[i + 1 :]:
            try:
                path = graph.find_relationship_path(model_a, model_b)
                joinable_pairs.append(
                    {
                        "from": model_a,
                        "to": model_b,
                        "hops": len(path),
                    }
                )
            except (ValueError, KeyError):
                pass

    result: dict[str, Any] = {
        "models": models,
        "joinable_pairs": joinable_pairs,
    }
    if graph_metrics:
        result["graph_metrics"] = graph_metrics

    return result


# --- MCP Resource: Catalog Metadata ---


@mcp.resource("semantic://catalog")
def catalog_resource() -> str:
    """Postgres-compatible catalog metadata for the semantic layer.

    Exposes the semantic layer schema as information_schema-compatible metadata:
    - Models as tables
    - Dimensions and metrics as columns with data types
    - Relationships as foreign key constraints

    Useful for schema discovery, IDE integration, and Postgres protocol compatibility.
    """
    layer = get_layer()
    catalog = layer.get_catalog_metadata()
    return json.dumps(catalog, indent=2)
