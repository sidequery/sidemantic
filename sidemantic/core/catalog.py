"""Postgres-compatible catalog metadata for semantic layer.

Exports semantic layer schema in a format compatible with information_schema
and pg_catalog, similar to how Cube.dev exposes metrics via SQL API.

Key design:
- Each model becomes a table in information_schema.tables
- Dimensions AND metrics both appear as columns in information_schema.columns
- This matches Cube.dev's approach where metrics are queryable as columns
"""

from sidemantic.core.semantic_graph import SemanticGraph


def get_postgres_type_for_dimension(dim_type: str, granularity: str | None = None) -> str:
    """Map semantic dimension type to Postgres type.

    Args:
        dim_type: Dimension type (categorical, numeric, time, boolean)
        granularity: Optional time granularity (day, week, month, etc.)

    Returns:
        Postgres type name
    """
    if dim_type == "categorical":
        return "VARCHAR"
    elif dim_type == "numeric":
        return "NUMERIC"
    elif dim_type == "time":
        # More granular types for time dimensions
        if granularity in ("day", "week", "month", "quarter", "year"):
            return "DATE"
        elif granularity == "hour":
            return "TIMESTAMP"
        else:
            return "TIMESTAMP"  # Default for time
    elif dim_type == "boolean":
        return "BOOLEAN"
    else:
        return "VARCHAR"  # Default fallback


def get_postgres_type_for_metric(agg: str) -> str:
    """Map metric aggregation type to Postgres result type.

    Args:
        agg: Aggregation function (sum, count, avg, min, max, count_distinct, etc.)

    Returns:
        Postgres type name for the metric result
    """
    if agg in ("count", "count_distinct"):
        return "BIGINT"
    elif agg in ("sum", "avg"):
        return "NUMERIC"
    elif agg in ("min", "max"):
        # Min/max could be any type, but we default to numeric
        # In a real implementation, you'd need to know the source column type
        return "NUMERIC"
    elif agg == "median":
        return "NUMERIC"
    elif agg == "percentile":
        return "NUMERIC"
    else:
        return "NUMERIC"  # Safe default


def get_catalog_metadata(graph: SemanticGraph, schema: str = "public") -> dict:
    """Export semantic layer as Postgres-compatible catalog metadata.

    This generates metadata that can be used to populate information_schema
    and pg_catalog tables, making the semantic layer queryable via standard
    Postgres introspection queries.

    Similar to Cube.dev's SQL API:
    - Each model becomes a table
    - Dimensions and metrics become columns
    - Relationships become foreign keys

    Args:
        graph: Semantic graph to export
        schema: Schema name to use (default: 'public')

    Returns:
        Dictionary containing:
        - tables: List of table metadata (for information_schema.tables)
        - columns: List of column metadata (for information_schema.columns)
        - constraints: List of constraints (for information_schema.table_constraints)
        - key_column_usage: Foreign key mappings

    Example:
        >>> catalog = get_catalog_metadata(layer.graph)
        >>> for table in catalog['tables']:
        ...     print(f"{table['table_schema']}.{table['table_name']}")
        >>> for col in catalog['columns']:
        ...     print(f"{col['table_name']}.{col['column_name']}: {col['data_type']}")
    """
    tables = []
    columns = []
    constraints = []
    key_column_usage = []

    for model_name, model in graph.models.items():
        # Add table entry
        tables.append(
            {
                "table_catalog": "sidemantic",
                "table_schema": schema,
                "table_name": model.name,
                "table_type": "BASE TABLE",
                "is_insertable_into": "NO",  # Read-only semantic layer
                "is_typed": "NO",
            }
        )

        ordinal = 1

        # Add primary key column first
        if model.primary_key:
            columns.append(
                {
                    "table_catalog": "sidemantic",
                    "table_schema": schema,
                    "table_name": model.name,
                    "column_name": model.primary_key,
                    "ordinal_position": ordinal,
                    "column_default": None,
                    "is_nullable": "NO",
                    "data_type": "BIGINT",  # Assume PKs are BIGINT
                    "character_maximum_length": None,
                    "numeric_precision": 64,
                    "numeric_scale": 0,
                    "is_primary_key": True,
                    "is_foreign_key": False,
                    "is_metric": False,
                }
            )
            ordinal += 1

            # Add primary key constraint
            constraints.append(
                {
                    "constraint_catalog": "sidemantic",
                    "constraint_schema": schema,
                    "constraint_name": f"{model.name}_pkey",
                    "table_catalog": "sidemantic",
                    "table_schema": schema,
                    "table_name": model.name,
                    "constraint_type": "PRIMARY KEY",
                    "is_deferrable": "NO",
                    "initially_deferred": "NO",
                }
            )

            key_column_usage.append(
                {
                    "constraint_catalog": "sidemantic",
                    "constraint_schema": schema,
                    "constraint_name": f"{model.name}_pkey",
                    "table_catalog": "sidemantic",
                    "table_schema": schema,
                    "table_name": model.name,
                    "column_name": model.primary_key,
                    "ordinal_position": 1,
                }
            )

        # Add dimension columns
        for dim in model.dimensions:
            # Skip primary key if already added
            if dim.name == model.primary_key:
                continue

            data_type = get_postgres_type_for_dimension(dim.type, dim.granularity)

            col_meta = {
                "table_catalog": "sidemantic",
                "table_schema": schema,
                "table_name": model.name,
                "column_name": dim.name,
                "ordinal_position": ordinal,
                "column_default": None,
                "is_nullable": "YES",
                "data_type": data_type,
                "character_maximum_length": 255 if data_type == "VARCHAR" else None,
                "numeric_precision": 38 if data_type == "NUMERIC" else None,
                "numeric_scale": 10 if data_type == "NUMERIC" else None,
                "is_primary_key": False,
                "is_foreign_key": False,
                "is_metric": False,
            }

            # Add semantic metadata
            if dim.description:
                col_meta["description"] = dim.description
            if dim.label:
                col_meta["label"] = dim.label

            columns.append(col_meta)
            ordinal += 1

        # Add metric columns
        # Key design: metrics appear as regular columns that can be SELECT'd
        # The semantic layer handles the aggregation behind the scenes
        for metric in model.metrics:
            data_type = get_postgres_type_for_metric(metric.agg)

            col_meta = {
                "table_catalog": "sidemantic",
                "table_schema": schema,
                "table_name": model.name,
                "column_name": metric.name,
                "ordinal_position": ordinal,
                "column_default": None,
                "is_nullable": "YES",
                "data_type": data_type,
                "character_maximum_length": None,
                "numeric_precision": 38 if data_type == "NUMERIC" else 64,
                "numeric_scale": 10 if data_type == "NUMERIC" else 0,
                "is_primary_key": False,
                "is_foreign_key": False,
                "is_metric": True,  # Custom field to distinguish metrics
                "aggregation": metric.agg,  # Custom field for metric aggregation type
            }

            # Add semantic metadata
            if metric.description:
                col_meta["description"] = metric.description
            if metric.label:
                col_meta["label"] = metric.label

            columns.append(col_meta)
            ordinal += 1

        # Add foreign key constraints from relationships
        for rel in model.relationships:
            if rel.type in ("many_to_one", "one_to_one"):
                # This model has a foreign key to another model
                fk_column = rel.foreign_key
                referenced_table = rel.name
                referenced_column = graph.get_model(referenced_table).primary_key

                constraint_name = f"{model.name}_{fk_column}_fkey"

                constraints.append(
                    {
                        "constraint_catalog": "sidemantic",
                        "constraint_schema": schema,
                        "constraint_name": constraint_name,
                        "table_catalog": "sidemantic",
                        "table_schema": schema,
                        "table_name": model.name,
                        "constraint_type": "FOREIGN KEY",
                        "is_deferrable": "NO",
                        "initially_deferred": "NO",
                    }
                )

                key_column_usage.append(
                    {
                        "constraint_catalog": "sidemantic",
                        "constraint_schema": schema,
                        "constraint_name": constraint_name,
                        "table_catalog": "sidemantic",
                        "table_schema": schema,
                        "table_name": model.name,
                        "column_name": fk_column,
                        "ordinal_position": 1,
                        "position_in_unique_constraint": 1,
                        "referenced_table_schema": schema,
                        "referenced_table_name": referenced_table,
                        "referenced_column_name": referenced_column,
                    }
                )

                # Mark foreign key column
                for col in columns:
                    if col["table_name"] == model.name and col["column_name"] == fk_column:
                        col["is_foreign_key"] = True
                        break

    return {
        "tables": tables,
        "columns": columns,
        "constraints": constraints,
        "key_column_usage": key_column_usage,
    }
