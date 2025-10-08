"""PostgreSQL wire protocol server for semantic layer."""

import riffq
import typer

from sidemantic.core.semantic_layer import SemanticLayer
from sidemantic.server.connection import SemanticLayerConnection


def map_type(duckdb_type: str) -> str:
    """Map DuckDB types to PostgreSQL types."""
    type_lower = duckdb_type.lower()
    if "int" in type_lower or "integer" in type_lower:
        if "big" in type_lower:
            return "bigint"
        elif "small" in type_lower:
            return "smallint"
        return "integer"
    elif "varchar" in type_lower or "text" in type_lower:
        return "text"
    elif "decimal" in type_lower or "numeric" in type_lower:
        return "numeric"
    elif "double" in type_lower or "float" in type_lower:
        return "double precision"
    elif "date" in type_lower:
        return "date"
    elif "timestamp" in type_lower:
        return "timestamp"
    elif "bool" in type_lower:
        return "boolean"
    return "text"  # Default fallback


def start_server(
    layer: SemanticLayer,
    port: int = 5433,
    username: str | None = None,
    password: str | None = None,
):
    """Start PostgreSQL-compatible server for the semantic layer.

    Args:
        layer: Semantic layer instance
        port: Port to listen on
        username: Username for authentication (optional)
        password: Password for authentication (optional)
    """

    # Create connection class with layer injected
    class BoundConnection(SemanticLayerConnection):
        def __init__(self, connection_id, executor):
            super().__init__(connection_id, executor, layer, username, password)

    # Start server
    server = riffq.RiffqServer(f"127.0.0.1:{port}", connection_cls=BoundConnection)

    # Register catalog
    typer.echo("Registering semantic layer catalog...", err=True)

    # Register database
    server._server.register_database("sidemantic")

    # First, register all actual DuckDB tables
    tbls = layer.conn.execute(
        "SELECT table_schema, table_name FROM information_schema.tables "
        "WHERE table_schema NOT IN ('pg_catalog', 'information_schema')"
    ).fetchall()

    for schema_name, table_name in tbls:
        server._server.register_schema("sidemantic", schema_name)
        cols_info = layer.conn.execute(
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
            "WHERE table_schema=? AND table_name=?",
            (schema_name, table_name),
        ).fetchall()
        columns = []
        for col_name, data_type, is_nullable in cols_info:
            columns.append({col_name: {"type": map_type(data_type), "nullable": is_nullable.upper() == "YES"}})
        server._server.register_table("sidemantic", schema_name, table_name, columns)
        typer.echo(f"  Registered source table: {schema_name}.{table_name}", err=True)

    # Register models as tables in the 'semantic_layer' schema
    server._server.register_schema("sidemantic", "semantic_layer")

    for model_name, model in layer.graph.models.items():
        columns = []

        # Add dimensions
        for dim in model.dimensions:
            # Map dimension type to SQL type
            if dim.type == "time":
                sql_type = "timestamp"
            elif dim.type == "number":
                sql_type = "numeric"
            elif dim.type == "boolean":
                sql_type = "boolean"
            else:
                sql_type = "text"

            columns.append({dim.name: {"type": sql_type, "nullable": True}})

        # Add metrics
        for metric in model.metrics:
            # Metrics are typically numeric
            columns.append({metric.name: {"type": "numeric", "nullable": True}})

        server._server.register_table("sidemantic", "semantic_layer", model_name, columns)
        typer.echo(f"  Registered table: semantic_layer.{model_name}", err=True)

    # Also register the magic 'metrics' table if there are graph-level metrics
    if layer.graph.metrics:
        metric_columns = []
        for metric in layer.graph.metrics:
            metric_columns.append({metric.name: {"type": "numeric", "nullable": True}})

        # Add all dimension columns from all models
        all_dims = set()
        for model in layer.graph.models.values():
            for dim in model.dimensions:
                all_dims.add((dim.name, dim.type))

        for dim_name, dim_type in all_dims:
            if dim_type == "time":
                sql_type = "timestamp"
            elif dim_type == "number":
                sql_type = "numeric"
            elif dim_type == "boolean":
                sql_type = "boolean"
            else:
                sql_type = "text"
            metric_columns.append({dim_name: {"type": sql_type, "nullable": True}})

        server._server.register_table("sidemantic", "semantic_layer", "metrics", metric_columns)
        typer.echo("  Registered table: semantic_layer.metrics", err=True)

    typer.echo(f"\nStarting PostgreSQL-compatible server on 127.0.0.1:{port}", err=True)
    if username:
        typer.echo(f"Authentication: username={username}", err=True)
    else:
        typer.echo("Authentication: disabled (any username/password accepted)", err=True)
    typer.echo(f"\nConnect with: psql -h 127.0.0.1 -p {port} -U {username or 'any'} -d sidemantic\n", err=True)

    # Disable catalog_emulation and handle catalog queries manually in our Python handler
    # This prevents riffq from intercepting queries with its DataFusion parser (which fails on multi-statement queries)
    # We manually expose semantic layer tables through information_schema query handling
    server.start(catalog_emulation=False)
