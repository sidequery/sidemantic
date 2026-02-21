"""MCP server for Sidemantic semantic layer."""

import json
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal

from mcp.server.fastmcp import FastMCP

from sidemantic.core.semantic_layer import SemanticLayer
from sidemantic.loaders import load_from_directory

# Global semantic layer instance
_layer: SemanticLayer | None = None


def initialize_layer(directory: str, db_path: str | None = None) -> SemanticLayer:
    """Initialize the semantic layer with models from directory."""
    global _layer

    # Create connection string
    connection = None
    if db_path:
        if db_path == ":memory:":
            connection = "duckdb:///:memory:"
        else:
            connection = f"duckdb:///{Path(db_path).absolute()}"

    _layer = SemanticLayer(connection=connection)
    load_from_directory(_layer, directory)
    return _layer


def get_layer() -> SemanticLayer:
    """Get the initialized semantic layer."""
    if _layer is None:
        raise RuntimeError("Semantic layer not initialized. Call initialize_layer first.")
    return _layer


def _convert_to_json_compatible(value: Any) -> Any:
    """Convert value to JSON-compatible type.

    Handles Decimal and other non-JSON-serializable types from database results.
    """
    if isinstance(value, Decimal):
        return float(value)
    return value


def _validate_filter(filter_str: str, dialect: str | None = None) -> None:
    """Validate a filter string to prevent SQL injection.

    Parses the filter as a SQL expression using the active dialect and rejects
    DDL/DML statements. Also rejects multi-statement input (semicolons).
    """
    import sqlglot

    if ";" in filter_str:
        raise ValueError("Filter contains disallowed SQL: multi-statement input")

    try:
        parsed = sqlglot.parse_one(f"SELECT 1 WHERE {filter_str}", dialect=dialect)
    except Exception:
        raise ValueError(f"Invalid filter expression: {filter_str}")

    # Walk the parse tree and reject DDL/DML nodes
    for node in parsed.walk():
        if isinstance(
            node,
            (
                sqlglot.exp.Drop,
                sqlglot.exp.Insert,
                sqlglot.exp.Delete,
                sqlglot.exp.Update,
                sqlglot.exp.Create,
                sqlglot.exp.Command,
                sqlglot.exp.AlterTable,
            ),
        ):
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
def list_models() -> list[dict[str, Any]]:
    """List all available models in the semantic layer.

    Models are the core building blocks of the semantic layer. Each model represents
    a business entity (e.g., orders, customers, products) and contains:
    - Dimensions: attributes you can group by or filter on
    - Metrics: measures you can aggregate (sum, count, average, etc.)
    - Relationships: connections to other models for automatic joins

    Use this to discover what data is available before constructing queries.

    Returns:
        List of models with basic information including name, table, dimensions,
        metrics, and relationship count.
    """
    layer = get_layer()

    models = []
    for model_name, model in layer.graph.models.items():
        models.append(
            {
                "name": model_name,
                "table": model.table,
                "dimensions": [d.name for d in model.dimensions],
                "metrics": [m.name for m in model.metrics],
                "relationships": len(model.relationships),
            }
        )

    return models


@mcp.tool()
def get_models(model_names: list[str]) -> list[dict[str, Any]]:
    """Get detailed information about one or more models.

    Returns comprehensive details about models including:
    - All dimensions with their types, SQL definitions, descriptions, labels, hierarchy, and formatting
    - All metrics with their aggregation types, SQL formulas, filters, descriptions, and complex metric config
    - All segments (named reusable filters) defined on the model
    - All relationships showing how models connect for joins
    - Model metadata: description, primary key, default time dimension, source info

    Dimension types:
    - categorical: text/enum values for grouping (e.g., status, region)
    - time: timestamps supporting granularity rollups (e.g., created_at)
    - numeric: numbers that can be used in calculations
    - boolean: true/false flags

    Metric aggregations:
    - sum, avg, min, max: numeric aggregations
    - count, count_distinct: counting aggregations
    - median: statistical median

    Special metric types:
    - ratio: division of two metrics (has numerator/denominator)
    - derived: formula combining other metrics
    - cumulative: running totals or rolling windows
    - time_comparison: period-over-period calculations (YoY, MoM, etc.)
    - conversion: funnel conversion rates

    Args:
        model_names: List of model names to retrieve details for

    Returns:
        Detailed information for each requested model.
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
) -> dict[str, Any]:
    """Run a query against the semantic layer.

    Sidemantic automatically generates SQL from semantic references and handles joins between models.

    Field References:
    - Use model.field_name format (e.g., "orders.customer_name", "orders.total_revenue")
    - Dimensions and metrics are namespaced by their model

    Time Dimensions:
    - Time dimensions support granularity suffixes using double underscore
    - Available granularities: __year, __quarter, __month, __week, __day, __hour
    - Example: "orders.created_at__month" groups by month
    - Use the base dimension name without suffix for raw timestamp

    Automatic Joins:
    - Reference fields from multiple models to trigger automatic joins
    - Joins are inferred from model relationships
    - Example: ["orders.revenue", "customers.region"] automatically joins orders to customers

    Filters:
    - Use model.field_name in WHERE conditions
    - Standard SQL operators: =, !=, <, >, <=, >=, IN, LIKE, BETWEEN
    - Combine with AND/OR
    - Example: "orders.status = 'completed' AND orders.amount > 100"

    Segments:
    - Named reusable filters defined on models (e.g., "orders.active_users")
    - Use list_segments to discover available segments

    Args:
        dimensions: List of dimension references (e.g., ["orders.customer_name", "orders.created_at__month"])
        metrics: List of metric references (e.g., ["orders.total_revenue", "orders.order_count"])
        where: Optional WHERE clause using model.field_name syntax
        segments: Optional list of segment references to apply (e.g., ["orders.active_users"])
        order_by: List of fields to order by with optional "asc" or "desc" (e.g., ["orders.total_revenue desc"])
        limit: Optional row limit
        offset: Optional number of rows to skip (for pagination, use with limit)
        ungrouped: If True, return raw rows without aggregation (no GROUP BY)

    Returns:
        Query result containing generated SQL, result rows, and row count.

    Examples:
        Simple aggregation:
        - dimensions: ["orders.status"]
        - metrics: ["orders.total_revenue"]

        Time series with granularity:
        - dimensions: ["orders.created_at__month"]
        - metrics: ["orders.total_revenue", "orders.order_count"]

        Cross-model query (automatic join):
        - dimensions: ["customers.region", "products.category"]
        - metrics: ["orders.total_revenue"]

        With filters, segments, and sorting:
        - dimensions: ["orders.status"]
        - metrics: ["orders.total_revenue"]
        - where: "orders.created_at >= '2024-01-01'"
        - segments: ["orders.completed_orders"]
        - order_by: ["orders.total_revenue desc"]
        - limit: 10

        Paginated results:
        - dimensions: ["orders.customer_name"]
        - metrics: ["orders.total_revenue"]
        - limit: 20
        - offset: 40

        Raw ungrouped rows:
        - dimensions: ["orders.customer_name", "orders.created_at"]
        - ungrouped: True
        - limit: 100
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


@mcp.tool()
def create_chart(
    dimensions: list[str] | None = None,
    metrics: list[str] | None = None,
    where: str | None = None,
    segments: list[str] | None = None,
    order_by: list[str] | None = None,
    limit: int | None = None,
    chart_type: Literal["auto", "bar", "line", "area", "scatter", "point"] = "auto",
    title: str | None = None,
    width: int = 600,
    height: int = 400,
) -> dict[str, Any]:
    """Generate a chart from a semantic layer query.

    Combines query execution with chart generation, producing Vega-Lite specs and PNG images.

    Chart Type Selection (when chart_type="auto"):
    - Time dimension + metrics -> Line chart (multiple metrics) or Area chart (single metric)
    - Categorical dimension + metrics -> Bar chart
    - Two numeric dimensions -> Scatter plot
    - Multiple metrics over time -> Multi-line chart

    Visual Design:
    - Modern, accessible color palette (not rainbow defaults)
    - Clean typography with Inter font family
    - Minimal gridlines and chartjunk
    - Responsive tooltips showing all relevant data
    - Smart axis formatting (currency, percentages, thousands separators)
    - Professional spacing and proportions

    Query Semantics (same as run_query):
    - Use model.field_name format for all references
    - Time dimensions support granularity: dimension__month, dimension__year, etc.
    - Automatic joins when referencing multiple models
    - Standard SQL operators in WHERE clause
    - Segments: named reusable filters (e.g., ["orders.active_users"])

    Args:
        dimensions: List of dimension references (e.g., ["orders.created_at__month", "customers.region"])
        metrics: List of metric references (e.g., ["orders.total_revenue", "orders.order_count"])
        where: Optional WHERE clause filter using model.field_name syntax
        segments: Optional list of segment references to apply (e.g., ["orders.active_users"])
        order_by: List of fields to order by with optional "asc" or "desc"
        limit: Optional row limit
        chart_type: Chart type ("auto" for smart selection, or "bar", "line", "area", "scatter", "point")
        title: Chart title (auto-generated if not provided)
        width: Chart width in pixels (default: 600)
        height: Chart height in pixels (default: 400)

    Returns:
        Chart result containing:
        - sql: Generated SQL query
        - vega_spec: Vega-Lite JSON specification (can be rendered client-side)
        - png_base64: Base64-encoded PNG image (ready for display)
        - row_count: Number of data points in the chart

    Examples:
        Revenue trend over time:
        - dimensions: ["orders.created_at__month"]
        - metrics: ["orders.total_revenue"]
        - title: "Monthly Revenue Trend"

        Top products by revenue:
        - dimensions: ["products.name"]
        - metrics: ["orders.total_revenue"]
        - order_by: ["orders.total_revenue desc"]
        - limit: 10
        - chart_type: "bar"

        Revenue by region and status:
        - dimensions: ["customers.region"]
        - metrics: ["orders.total_revenue"]
        - where: "orders.status = 'completed'"
        - chart_type: "bar"

        Multiple metrics over time:
        - dimensions: ["orders.created_at__month"]
        - metrics: ["orders.total_revenue", "orders.order_count"]
        - chart_type: "line"
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
    )

    result = layer.adapter.execute(sql)
    rows = result.fetchall()
    columns = [desc[0] for desc in result.description]
    row_dicts = [{col: _convert_to_json_compatible(val) for col, val in zip(columns, row)} for row in rows]

    if not row_dicts:
        raise ValueError("Query returned no data - cannot create chart")

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

    return {
        "sql": sql,
        "vega_spec": vega_spec,
        "png_base64": png_base64,
        "row_count": len(row_dicts),
    }


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
def compile_query(
    dimensions: list[str] | None = None,
    metrics: list[str] | None = None,
    where: str | None = None,
    segments: list[str] | None = None,
    order_by: list[str] | None = None,
    limit: int | None = None,
    offset: int | None = None,
    ungrouped: bool = False,
) -> dict[str, str]:
    """Compile a semantic layer query to SQL without executing it (dry run).

    Use this to inspect the generated SQL before running it, or to get SQL for use
    in other tools. Accepts the same parameters as run_query.

    Args:
        dimensions: List of dimension references (e.g., ["orders.customer_name", "orders.created_at__month"])
        metrics: List of metric references (e.g., ["orders.total_revenue", "orders.order_count"])
        where: Optional WHERE clause using model.field_name syntax
        segments: Optional list of segment references to apply (e.g., ["orders.active_users"])
        order_by: List of fields to order by with optional "asc" or "desc"
        limit: Optional row limit
        offset: Optional number of rows to skip (for pagination)
        ungrouped: If True, return raw rows without aggregation (no GROUP BY)

    Returns:
        Dictionary with "sql" key containing the generated SQL string.
    """
    layer = get_layer()

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

    return {"sql": sql}


@mcp.tool()
def run_sql(query: str) -> dict[str, Any]:
    """Execute a SQL query rewritten through the semantic layer.

    Write natural SQL referencing model fields, and sidemantic rewrites it to use the
    semantic layer's metric/dimension definitions with proper aggregations and joins.

    The query is parsed and rewritten:
    - Column references are resolved to semantic layer dimensions and metrics
    - Aggregations are applied based on metric definitions
    - Joins between models are automatic based on relationships
    - CTEs and subqueries that reference semantic models are rewritten

    Supported SQL features:
    - Simple SELECT: SELECT revenue, status FROM orders
    - Qualified references: SELECT orders.revenue, customers.region FROM orders
    - Time granularity: SELECT order_date__month, revenue FROM orders
    - WHERE filters: SELECT revenue FROM orders WHERE status = 'completed'
    - ORDER BY and LIMIT: SELECT revenue FROM orders ORDER BY revenue DESC LIMIT 10
    - CTEs: WITH agg AS (SELECT revenue, status FROM orders) SELECT * FROM agg
    - Subqueries: SELECT * FROM (SELECT revenue, status FROM orders) WHERE revenue > 100
    - FROM metrics: SELECT orders.revenue, customers.region FROM metrics (virtual table for cross-model queries)

    Not supported:
    - Explicit JOIN syntax (joins are automatic)
    - Inline aggregation functions (must be defined as metrics)
    - Multiple statements

    Args:
        query: SQL query referencing semantic layer models/fields

    Returns:
        Query result containing:
        - sql: The rewritten SQL that was actually executed
        - original_sql: The original SQL you provided
        - rows: Result rows as list of dicts
        - row_count: Number of rows returned

    Examples:
        Simple query:
            "SELECT revenue, status FROM orders"

        Cross-model with virtual table:
            "SELECT orders.revenue, customers.region FROM metrics"

        With time granularity:
            "SELECT order_date__month, revenue FROM orders ORDER BY order_date__month"

        CTE:
            "WITH monthly AS (SELECT order_date__month, revenue FROM orders) SELECT * FROM monthly WHERE revenue > 1000"
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
def list_segments() -> list[dict[str, Any]]:
    """List all segments (named reusable filters) across all models.

    Segments are predefined WHERE clause filters that can be applied to queries
    for consistent data filtering. For example, "active_users" might filter to
    users with status='active' who logged in within the last 30 days.

    Returns:
        List of segments with model name, segment name, SQL expression, and description.

    Example response:
        [
            {
                "model": "users",
                "name": "active_users",
                "qualified_name": "users.active_users",
                "sql": "{model}.status = 'active' AND {model}.last_login > CURRENT_DATE - 30",
                "description": "Users who are active and logged in recently"
            }
        ]
    """
    layer = get_layer()

    all_segments = []
    for model_name, model in layer.graph.models.items():
        for seg in model.segments:
            seg_info: dict[str, Any] = {
                "model": model_name,
                "name": seg.name,
                "qualified_name": f"{model_name}.{seg.name}",
                "sql": seg.sql,
            }
            if seg.description:
                seg_info["description"] = seg.description
            if not seg.public:
                seg_info["public"] = False
            all_segments.append(seg_info)

    return all_segments


@mcp.tool()
def get_semantic_graph() -> dict[str, Any]:
    """Get the full semantic graph structure showing all models, their relationships, and graph-level metrics.

    Provides a high-level overview of the entire semantic layer:
    - All models with their dimensions, metrics, and segments (names only for brevity)
    - All relationships between models with join types
    - Graph-level metrics (metrics not attached to a specific model)
    - Join paths: which models can be joined together

    Use this for understanding the overall data model before constructing queries.
    For detailed information about specific models, use get_models.

    Returns:
        Dictionary containing:
        - models: List of model summaries (name, table, dimension/metric/segment names, relationship info)
        - graph_metrics: List of graph-level metrics (time_comparison, conversion, derived, ratio)
        - joinable_pairs: List of model pairs that can be joined together
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
