"""MCP server for Sidemantic semantic layer."""

from decimal import Decimal
from pathlib import Path
from typing import Any, Literal

from mcp.server.fastmcp import FastMCP
from pydantic import BaseModel, Field

from sidemantic.core.semantic_layer import SemanticLayer
from sidemantic.loaders import load_from_directory

# Global semantic layer instance
_layer: SemanticLayer | None = None


class ModelInfo(BaseModel):
    """Basic model information."""

    name: str
    table: str | None = None
    dimensions: list[str]
    metrics: list[str]
    relationships: int


class ModelDetail(BaseModel):
    """Detailed model information."""

    name: str
    table: str | None = None
    dimensions: list[dict[str, Any]]
    metrics: list[dict[str, Any]]
    relationships: list[dict[str, Any]]
    source_format: str | None = None
    source_file: str | None = None


class QueryRequest(BaseModel):
    """Query request parameters."""

    dimensions: list[str] = Field(default_factory=list, description="List of dimensions to include")
    metrics: list[str] = Field(default_factory=list, description="List of metrics to include")
    where: str | None = Field(default=None, description="Optional WHERE clause filter")
    order_by: list[str] = Field(default_factory=list, description="List of columns to order by")
    limit: int | None = Field(default=None, description="Optional row limit")


class QueryResult(BaseModel):
    """Query execution result."""

    sql: str
    rows: list[dict[str, Any]]
    row_count: int


class ChartResult(BaseModel):
    """Chart generation result."""

    sql: str
    vega_spec: dict[str, Any]
    png_base64: str
    row_count: int


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
    - All dimensions with their types, SQL definitions, and descriptions
    - All metrics with their aggregation types, SQL formulas, filters, and descriptions
    - All relationships showing how models connect for joins
    - Source metadata (original format and file)

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
    - ratio: division of two metrics
    - derived: formula combining other metrics
    - cumulative: running totals or rolling windows
    - time_comparison: period-over-period calculations (YoY, MoM, etc.)
    - conversion: funnel conversion rates

    Args:
        model_names: List of model names to retrieve details for

    Returns:
        Detailed information for each requested model including all dimensions,
        metrics, relationships, and source metadata.
    """
    layer = get_layer()

    details = []
    for model_name in model_names:
        if model_name not in layer.graph.models:
            continue

        model = layer.graph.models[model_name]

        # Get dimension details
        dims = []
        for dim in model.dimensions:
            dim_info = {
                "name": dim.name,
                "type": dim.type,
                "sql": dim.sql,
            }
            if dim.description:
                dim_info["description"] = dim.description
            dims.append(dim_info)

        # Get metric details
        metrics = []
        for metric in model.metrics:
            metric_info = {
                "name": metric.name,
                "sql": metric.sql,
            }
            # Include aggregation or type
            if metric.agg:
                metric_info["agg"] = metric.agg
            if metric.type:
                metric_info["type"] = metric.type
            if metric.description:
                metric_info["description"] = metric.description
            if metric.filters:
                metric_info["filters"] = metric.filters
            metrics.append(metric_info)

        # Get relationship details
        rels = []
        for rel in model.relationships:
            rel_info = {
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

        detail = {
            "name": model_name,
            "table": model.table,
            "dimensions": dims,
            "metrics": metrics,
            "relationships": rels,
        }
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
    order_by: list[str] | None = None,
    limit: int | None = None,
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

    Args:
        dimensions: List of dimension references (e.g., ["orders.customer_name", "orders.created_at__month"])
        metrics: List of metric references (e.g., ["orders.total_revenue", "orders.order_count"])
        where: Optional WHERE clause using model.field_name syntax
        order_by: List of fields to order by with optional "asc" or "desc" (e.g., ["orders.total_revenue desc"])
        limit: Optional row limit

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

        With filters and sorting:
        - dimensions: ["orders.status"]
        - metrics: ["orders.total_revenue"]
        - where: "orders.created_at >= '2024-01-01'"
        - order_by: ["orders.total_revenue desc"]
        - limit: 10
    """
    layer = get_layer()

    # Compile SQL
    sql = layer.compile(
        dimensions=dimensions or [],
        metrics=metrics or [],
        filters=[where] if where else None,
        order_by=order_by,
        limit=limit,
    )

    # Execute query
    result = layer.conn.execute(sql)

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
    order_by: list[str] | None = None,
    limit: int | None = None,
    chart_type: Literal["auto", "bar", "line", "area", "scatter", "point"] = "auto",
    title: str | None = None,
    width: int = 600,
    height: int = 400,
) -> dict[str, Any]:
    """Generate a beautiful chart from a semantic layer query.

    This tool combines query execution with intelligent chart generation, producing
    professional, publication-quality visualizations with carefully designed aesthetic defaults.

    Chart Type Selection (when chart_type="auto"):
    - Time dimension + metrics → Line chart (multiple metrics) or Area chart (single metric)
    - Categorical dimension + metrics → Bar chart
    - Two numeric dimensions → Scatter plot
    - Multiple metrics over time → Multi-line chart

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

    Args:
        dimensions: List of dimension references (e.g., ["orders.created_at__month", "customers.region"])
        metrics: List of metric references (e.g., ["orders.total_revenue", "orders.order_count"])
        where: Optional WHERE clause filter using model.field_name syntax
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

    # Compile and execute query (same as run_query)
    sql = layer.compile(
        dimensions=dimensions or [],
        metrics=metrics or [],
        filters=[where] if where else None,
        order_by=order_by,
        limit=limit,
    )

    result = layer.conn.execute(sql)
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
