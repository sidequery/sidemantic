"""PostgreSQL wire protocol server for semantic layer."""

import riffq
import typer

from sidemantic.core.semantic_layer import SemanticLayer
from sidemantic.server.connection import SemanticLayerConnection
from sidemantic.server.pg_catalog import PgCatalog


def _sql_string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


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
    host: str = "127.0.0.1",
    port: int = 5433,
    username: str | None = None,
    password: str | None = None,
    user_attrs_map: dict[str, dict] | None = None,
):
    """Start PostgreSQL-compatible server for the semantic layer.

    Args:
        layer: Semantic layer instance
        host: Host/IP to bind to (use 0.0.0.0 for Docker)
        port: Port to listen on
        username: Username for authentication (optional)
        password: Password for authentication (optional)
        user_attrs_map: Optional mapping of Postgres startup username -> security
            ``user_attributes`` dict. Loaded from --user-attrs-file at startup and
            used to enforce model access gates per connecting user.
    """
    if (username is None) != (password is None):
        raise ValueError("Both username and password must be provided together")

    # A user-attrs map selects each session's security attributes from the CLIENT-SUPPLIED
    # startup username. Without password authentication, a client could connect as any key in
    # the map (e.g. an admin) and satisfy access gates, so refuse to start in that configuration.
    if user_attrs_map and username is None:
        raise ValueError(
            "A user-attrs map requires authentication: pass --username with --password-file so the "
            "connecting username cannot be spoofed to select another user's security attributes."
        )

    # Physical tables are snapshotted below and shared with the catalog that
    # answers information_schema/pg_catalog queries for every connection.
    source_tables: list[tuple[str, str, list[tuple[str, str]]]] = []
    catalog = PgCatalog(layer, source_tables)

    # Create connection class with layer injected
    class BoundConnection(SemanticLayerConnection):
        def __init__(self, connection_id, executor):
            super().__init__(connection_id, executor, layer, username, password, user_attrs_map=user_attrs_map)
            self.catalog = catalog

    # Start server
    server = riffq.RiffqServer(f"{host}:{port}", connection_cls=BoundConnection)

    # Register catalog
    typer.echo("Registering semantic layer catalog...", err=True)

    # Register database
    server._server.register_database("sidemantic")

    # First, register all actual database tables
    tbls = layer.adapter.execute(
        "SELECT table_schema, table_name FROM information_schema.tables "
        "WHERE table_schema NOT IN ('pg_catalog', 'information_schema')"
    ).fetchall()

    # riffq >= 0.4 raises on a duplicate schema or table, so register each once.
    # Attached databases can repeat a schema.table pair; the first one wins.
    registered_schemas: set[str] = {"semantic_layer"}
    registered_tables: set[tuple[str, str]] = set()
    for schema_name, table_name in tbls:
        if schema_name == "semantic_layer" or (schema_name, table_name) in registered_tables:
            continue
        registered_tables.add((schema_name, table_name))
        if schema_name not in registered_schemas:
            server._server.register_schema("sidemantic", schema_name)
            registered_schemas.add(schema_name)
        cols_info = layer.adapter.execute(
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
            f"WHERE table_schema = {_sql_string_literal(schema_name)} "
            f"AND table_name = {_sql_string_literal(table_name)} "
            "ORDER BY ordinal_position"
        ).fetchall()
        columns = []
        source_columns: list[tuple[str, str]] = []
        for col_name, data_type, is_nullable in cols_info:
            if any(col_name == seen_name for seen_name, _ in source_columns):
                continue
            columns.append({col_name: {"type": map_type(data_type), "nullable": is_nullable.upper() == "YES"}})
            source_columns.append((col_name, data_type))
        server._server.register_table("sidemantic", schema_name, table_name, columns)
        source_tables.append((schema_name, table_name, source_columns))
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
            elif dim.type in ("numeric", "number"):
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
    if layer.graph.metrics and "metrics" not in layer.graph.models:
        metric_columns = []
        for metric in layer.graph.metrics.values():
            metric_columns.append({metric.name: {"type": "numeric", "nullable": True}})

        # Add all dimension columns from all models
        all_dims = set()
        for model in layer.graph.models.values():
            for dim in model.dimensions:
                all_dims.add((dim.name, dim.type))

        for dim_name, dim_type in all_dims:
            if dim_type == "time":
                sql_type = "timestamp"
            elif dim_type in ("numeric", "number"):
                sql_type = "numeric"
            elif dim_type == "boolean":
                sql_type = "boolean"
            else:
                sql_type = "text"
            metric_columns.append({dim_name: {"type": sql_type, "nullable": True}})

        server._server.register_table("sidemantic", "semantic_layer", "metrics", metric_columns)
        typer.echo("  Registered table: semantic_layer.metrics", err=True)

    typer.echo(f"\nStarting PostgreSQL-compatible server on {host}:{port}", err=True)
    if username:
        typer.echo(f"Authentication: username={username}", err=True)
    else:
        # riffq always asks for a password; libpq refuses to connect without one.
        typer.echo("Authentication: disabled (any username and any non-empty password are accepted)", err=True)
    connect_host = "localhost" if host == "0.0.0.0" else host
    password_hint = "" if username else "PGPASSWORD=any "
    typer.echo(
        f"\nConnect with: {password_hint}psql -h {connect_host} -p {port} -U {username or 'any'} -d sidemantic\n",
        err=True,
    )

    # Disable catalog_emulation and answer catalog queries in our Python handler (see
    # sidemantic.server.pg_catalog). riffq's emulation rejects any statement its
    # PostgreSQL parser cannot read and has one catalog for all sessions, so it cannot
    # hide models a session may not access.
    server.start(catalog_emulation=False)
