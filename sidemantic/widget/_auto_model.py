"""Auto-generate sidemantic model from Arrow schema."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa

if TYPE_CHECKING:
    from sidemantic.core.semantic_graph import SemanticGraph


def is_numeric(arrow_type: pa.DataType) -> bool:
    """Check if Arrow type is numeric."""
    return pa.types.is_integer(arrow_type) or pa.types.is_floating(arrow_type) or pa.types.is_decimal(arrow_type)


def is_temporal(arrow_type: pa.DataType) -> bool:
    """Check if Arrow type is temporal (date/time)."""
    return pa.types.is_date(arrow_type) or pa.types.is_timestamp(arrow_type) or pa.types.is_time(arrow_type)


def build_auto_model(
    schema: pa.Schema,
    table_name: str = "data",
    max_dimension_cardinality: int | None = None,
    cardinality_map: dict[str, int] | None = None,
) -> tuple[SemanticGraph, str | None]:
    """Build a sidemantic SemanticGraph from an Arrow schema.

    Creates an auto-generated model with:
    - All columns as dimensions
    - row_count metric (always)
    - sum_{col} and avg_{col} for numeric columns

    Args:
        schema: PyArrow schema
        table_name: Name for the model/table
        max_dimension_cardinality: If set, skip dimensions with cardinality above this threshold
        cardinality_map: Optional dict mapping column names to their cardinality counts

    Returns:
        Tuple of (SemanticGraph with auto-generated model, detected time dimension name or None)
    """
    from sidemantic import Dimension, Metric, Model
    from sidemantic.core.registry import set_current_layer
    from sidemantic.core.semantic_graph import SemanticGraph

    # Clear auto-registration context to prevent Model from registering with existing layer
    set_current_layer(None)

    dimensions = []
    metrics = [Metric(name="row_count", agg="count")]
    time_dimension_name = None

    for field in schema:
        col_name = field.name

        # Determine dimension type first (must be: categorical, time, boolean, numeric)
        if is_temporal(field.type):
            dim_type = "time"
            if time_dimension_name is None:
                time_dimension_name = col_name
        elif is_numeric(field.type):
            dim_type = "numeric"
        elif pa.types.is_boolean(field.type):
            dim_type = "boolean"
        else:
            dim_type = "categorical"

        # Skip if cardinality threshold is set and exceeded (but never skip time dimensions)
        if dim_type != "time" and max_dimension_cardinality is not None and cardinality_map:
            cardinality = cardinality_map.get(col_name, 0)
            if cardinality > max_dimension_cardinality:
                continue

        # Add dimension (time dimensions need granularity)
        if dim_type == "time":
            dimensions.append(
                Dimension(
                    name=col_name,
                    sql=col_name,
                    type=dim_type,
                    granularity="day",
                )
            )
        else:
            dimensions.append(
                Dimension(
                    name=col_name,
                    sql=col_name,
                    type=dim_type,
                )
            )

        # Add implied metrics for numeric columns
        if is_numeric(field.type):
            metrics.append(
                Metric(
                    name=f"sum_{col_name}",
                    agg="sum",
                    sql=col_name,
                )
            )
            metrics.append(
                Metric(
                    name=f"avg_{col_name}",
                    agg="avg",
                    sql=col_name,
                )
            )

    # Create model using rowid as synthetic primary key for DuckDB
    # Don't set default_time_dimension - the widget handles time dims explicitly
    model = Model(
        name=table_name,
        table=table_name,
        primary_key="rowid",
        dimensions=dimensions,
        metrics=metrics,
    )

    # Build graph
    graph = SemanticGraph()
    graph.add_model(model)

    return graph, time_dimension_name


def compute_cardinality(conn, table_name: str, columns: list[str]) -> dict[str, int]:
    """Compute cardinality (distinct count) for columns.

    Args:
        conn: DuckDB connection
        table_name: Table name
        columns: List of column names

    Returns:
        Dict mapping column name to cardinality
    """
    result = {}
    for col in columns:
        query = f'SELECT COUNT(DISTINCT "{col}") as cnt FROM "{table_name}"'
        try:
            row = conn.execute(query).fetchone()
            result[col] = row[0] if row else 0
        except Exception:
            result[col] = 0
    return result
