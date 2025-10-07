"""MCP server for Sidemantic semantic layer."""

from pathlib import Path
from typing import Any

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


# Create MCP server
mcp = FastMCP("sidemantic")


@mcp.tool()
def list_models() -> list[ModelInfo]:
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
            ModelInfo(
                name=model_name,
                table=model.table,
                dimensions=[d.name for d in model.dimensions],
                metrics=[m.name for m in model.metrics],
                relationships=len(model.relationships),
            )
        )

    return models


@mcp.tool()
def get_models(model_names: list[str]) -> list[ModelDetail]:
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
            rels.append(
                {
                    "name": rel.name,
                    "to_model": rel.to_model,
                    "type": rel.type,
                    "sql_on": rel.sql_on,
                }
            )

        details.append(
            ModelDetail(
                name=model_name,
                table=model.table,
                dimensions=dims,
                metrics=metrics,
                relationships=rels,
                source_format=getattr(model, "_source_format", None),
                source_file=getattr(model, "_source_file", None),
            )
        )

    return details


@mcp.tool()
def run_query(
    dimensions: list[str] | None = None,
    metrics: list[str] | None = None,
    where: str | None = None,
    order_by: list[str] | None = None,
    limit: int | None = None,
) -> QueryResult:
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

    # Convert to list of dicts
    rows = result.fetchall()
    columns = [desc[0] for desc in result.description]
    row_dicts = [dict(zip(columns, row)) for row in rows]

    return QueryResult(
        sql=sql,
        rows=row_dicts,
        row_count=len(row_dicts),
    )
