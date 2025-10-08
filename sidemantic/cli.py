"""CLI for sidemantic semantic layer operations."""

from pathlib import Path

import typer

from sidemantic import SemanticLayer, __version__, load_from_directory
from sidemantic.config import SidemanticConfig, build_connection_string, find_config, load_config


def version_callback(value: bool):
    """Print version and exit."""
    if value:
        typer.echo(f"sidemantic {__version__}")
        raise typer.Exit()


app = typer.Typer(help="Sidemantic: SQL-first semantic layer")

# Global state for config (set in callback, used in commands)
_loaded_config: SidemanticConfig | None = None


@app.callback()
def main(
    version: bool = typer.Option(
        None, "--version", "-v", callback=version_callback, is_eager=True, help="Show version"
    ),
    config: Path = typer.Option(None, "--config", "-c", help="Path to config file (sidemantic.yaml)"),
):
    """Sidemantic CLI.

    You can use a config file (sidemantic.yaml or sidemantic.json) to set default values.
    CLI arguments override config file values.
    """
    global _loaded_config

    # Try to load config
    config_path = None
    if config:
        # Explicit config path provided
        config_path = config
    else:
        # Try to auto-discover config
        config_path = find_config()

    if config_path:
        try:
            _loaded_config = load_config(config_path)
            typer.echo(f"Loaded config from: {config_path}", err=True)
        except Exception as e:
            typer.echo(f"Warning: Failed to load config: {e}", err=True)
            _loaded_config = None


@app.command()
def workbench(
    directory: Path = typer.Argument(".", help="Directory containing semantic layer files (defaults to current dir)"),
    demo: bool = typer.Option(False, "--demo", help="Launch with demo data (multi-format example)"),
    connection: str = typer.Option(
        None, "--connection", help="Database connection string (e.g., postgres://host/db, bigquery://project/dataset)"
    ),
    db: Path = typer.Option(None, "--db", help="Path to DuckDB database file (shorthand for duckdb:/// connection)"),
):
    """
    Interactive semantic layer workbench with SQL editor and charting.

    Explore models, write SQL queries, and visualize results with interactive charts.

    Examples:
      sidemantic workbench semantic_models/    # Your own models
      sidemantic workbench --demo              # Try the demo
      sidemantic workbench ./models --db data/warehouse.db  # With DuckDB
      sidemantic workbench ./models --connection "postgres://localhost:5432/db"
      uvx sidemantic workbench --demo          # Run demo without installing
    """
    from sidemantic.workbench import run_workbench

    if demo:
        import sidemantic

        # Try packaged location first
        package_dir = Path(sidemantic.__file__).parent
        demo_dir = package_dir / "examples" / "multi_format_demo"

        # Fall back to dev environment location
        if not demo_dir.exists():
            dev_demo_dir = package_dir.parent / "examples" / "multi_format_demo"
            if dev_demo_dir.exists():
                demo_dir = dev_demo_dir
            else:
                typer.echo("Error: Demo models not found", err=True)
                typer.echo(f"Tried: {demo_dir}", err=True)
                typer.echo(f"Tried: {dev_demo_dir}", err=True)
                raise typer.Exit(1)

        directory = demo_dir
        run_workbench(directory, demo_mode=True, connection=None)
    elif not directory.exists():
        typer.echo(f"Error: Directory {directory} does not exist", err=True)
        raise typer.Exit(1)
    else:
        # Build connection string from args or config
        connection_str = None
        if connection:
            # Explicit --connection arg provided
            connection_str = connection
        elif db:
            # Explicit --db arg provided
            connection_str = f"duckdb:///{db.absolute()}"
        elif _loaded_config and _loaded_config.connection:
            # Use connection from config
            connection_str = build_connection_string(_loaded_config)

        # Only pass connection if it's not None
        if connection_str:
            run_workbench(directory, connection=connection_str)
        else:
            run_workbench(directory)


@app.command()
def tree(
    directory: Path = typer.Argument(..., help="Directory containing semantic layer files"),
):
    """
    Alias for 'workbench' command (deprecated).
    """
    from sidemantic.workbench import run_workbench

    if not directory.exists():
        typer.echo(f"Error: Directory {directory} does not exist", err=True)
        raise typer.Exit(1)

    run_workbench(directory)


@app.command()
def validate(
    directory: Path = typer.Argument(..., help="Directory containing semantic layer files"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed validation results"),
):
    """
    Validate semantic layer definitions.

    Shows errors, warnings, and optionally detailed info in an interactive view.
    """
    from sidemantic.workbench import run_validation

    if not directory.exists():
        typer.echo(f"Error: Directory {directory} does not exist", err=True)
        raise typer.Exit(1)

    run_validation(directory, verbose=verbose)


@app.command()
def query(
    directory: Path = typer.Argument(..., help="Directory containing semantic layer files"),
    sql: str = typer.Option(..., "--sql", "-q", help="SQL query to execute"),
    output: Path = typer.Option(None, "--output", "-o", help="Output file (default: stdout)"),
    connection: str = typer.Option(
        None, "--connection", help="Database connection string (e.g., postgres://host/db, bigquery://project/dataset)"
    ),
    db: Path = typer.Option(None, "--db", help="Path to DuckDB database file (shorthand for duckdb:/// connection)"),
):
    """
    Execute a SQL query and output results as CSV.

    Examples:
      sidemantic query models/ --sql "SELECT revenue FROM orders"
      sidemantic query models/ --sql "SELECT * FROM orders" --output results.csv
      sidemantic query models/ --connection "postgres://localhost:5432/db" --sql "SELECT revenue FROM orders"
      sidemantic query models/ --db data.duckdb --sql "SELECT revenue FROM orders"
    """
    if not directory.exists():
        typer.echo(f"Error: Directory {directory} does not exist", err=True)
        raise typer.Exit(1)

    try:
        # Build connection string from args or config
        connection_str = None
        if connection:
            # Explicit --connection arg provided
            connection_str = connection
        elif db:
            # Explicit --db arg provided
            connection_str = f"duckdb:///{db.absolute()}"
        elif _loaded_config and _loaded_config.connection:
            # Use connection from config
            connection_str = build_connection_string(_loaded_config)
        else:
            # Try to find database file in data/
            data_dir = directory / "data"
            if data_dir.exists():
                db_files = list(data_dir.glob("*.db"))
                if db_files:
                    connection_str = f"duckdb:///{db_files[0].absolute()}"

        # Load semantic layer (only pass connection if not None)
        if connection_str:
            layer = SemanticLayer(connection=connection_str)
        else:
            layer = SemanticLayer()
        load_from_directory(layer, str(directory))

        if not layer.graph.models:
            typer.echo("Error: No models found", err=True)
            raise typer.Exit(1)

        # Execute query
        result = layer.sql(sql)

        # Get results
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()

        # Output as CSV
        import csv
        import sys

        if output:
            with open(output, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                writer.writerows(rows)
            typer.echo(f"Results written to {output}", err=True)
        else:
            writer = csv.writer(sys.stdout)
            writer.writerow(columns)
            writer.writerows(rows)

    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@app.command()
def info(
    directory: Path = typer.Argument(..., help="Directory containing semantic layer files"),
):
    """
    Show quick info about the semantic layer.
    """
    if not directory.exists():
        typer.echo(f"Error: Directory {directory} does not exist", err=True)
        raise typer.Exit(1)

    try:
        layer = SemanticLayer()
        load_from_directory(layer, str(directory))

        if not layer.graph.models:
            typer.echo("No models found")
            raise typer.Exit(0)

        typer.echo(f"\nSemantic Layer: {directory}\n")

        for model_name, model in sorted(layer.graph.models.items()):
            typer.echo(f"● {model_name}")
            typer.echo(f"  Table: {model.table or 'N/A'}")
            typer.echo(f"  Dimensions: {len(model.dimensions)}")
            typer.echo(f"  Metrics: {len(model.metrics)}")
            typer.echo(f"  Relationships: {len(model.relationships)}")
            if model.relationships:
                rel_names = [r.name for r in model.relationships]
                typer.echo(f"  Connected to: {', '.join(rel_names)}")
            typer.echo()

    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@app.command()
def mcp_serve(
    directory: Path = typer.Argument(".", help="Directory containing semantic layer files (defaults to current dir)"),
    db: Path = typer.Option(None, "--db", help="Path to DuckDB database file (optional)"),
    demo: bool = typer.Option(False, "--demo", help="Use demo data instead of a directory"),
):
    """
    Start an MCP server for the semantic layer.

    Provides tools for listing models, getting model details, and running queries
    through the Model Context Protocol.

    Examples:
      sidemantic mcp-serve ./models
      sidemantic mcp-serve ./models --db data/warehouse.db
      sidemantic mcp-serve --demo
    """
    from sidemantic.mcp_server import initialize_layer, mcp

    if demo:
        # Use packaged demo models
        import sidemantic

        package_dir = Path(sidemantic.__file__).parent
        demo_dir = package_dir / "examples" / "multi_format_demo"

        # Fall back to dev environment location
        if not demo_dir.exists():
            dev_demo_dir = package_dir.parent / "examples" / "multi_format_demo"
            if dev_demo_dir.exists():
                demo_dir = dev_demo_dir
            else:
                typer.echo("Error: Demo models not found", err=True)
                typer.echo(f"Tried: {demo_dir}", err=True)
                typer.echo(f"Tried: {dev_demo_dir}", err=True)
                raise typer.Exit(1)

        directory = demo_dir
        # For demo mode, use in-memory database
        db_path = ":memory:"
    elif not directory.exists():
        typer.echo(f"Error: Directory {directory} does not exist", err=True)
        raise typer.Exit(1)
    else:
        db_path = str(db) if db else None

    try:
        # Initialize the semantic layer
        initialize_layer(str(directory), db_path)

        # If demo mode, populate with demo data
        if demo:
            try:
                # Try packaged import first
                from sidemantic.examples.multi_format_demo.demo_data import create_demo_database
            except ModuleNotFoundError:
                # Fall back to dev environment import
                import importlib.util
                import sys

                demo_data_path = directory / "demo_data.py"
                if demo_data_path.exists():
                    spec = importlib.util.spec_from_file_location("demo_data", demo_data_path)
                    demo_data_module = importlib.util.module_from_spec(spec)
                    sys.modules["demo_data"] = demo_data_module
                    spec.loader.exec_module(demo_data_module)
                    create_demo_database = demo_data_module.create_demo_database
                else:
                    raise ImportError(f"Could not find demo_data.py at {demo_data_path}")

            from sidemantic.mcp_server import get_layer

            layer = get_layer()
            demo_conn = create_demo_database()
            # Copy data from demo connection to layer's connection
            for table in ["customers", "products", "orders"]:
                # Get table data as regular Python objects (no pandas)
                rows = demo_conn.execute(f"SELECT * FROM {table}").fetchall()
                columns = [desc[0] for desc in demo_conn.execute(f"SELECT * FROM {table} LIMIT 0").description]

                # Create table in target connection
                create_sql = demo_conn.execute(
                    f"SELECT sql FROM duckdb_tables() WHERE table_name = '{table}'"
                ).fetchone()[0]
                layer.conn.execute(create_sql)

                # Insert data if there are rows
                if rows:
                    placeholders = ", ".join(["?" for _ in columns])
                    layer.conn.executemany(f"INSERT INTO {table} VALUES ({placeholders})", rows)

        typer.echo(f"Starting MCP server for: {directory}", err=True)
        if db_path and db_path != ":memory:":
            typer.echo(f"Using database: {db_path}", err=True)
        typer.echo("Server running on stdio...", err=True)

        # Run the MCP server
        mcp.run(transport="stdio")

    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@app.command()
def serve(
    directory: Path = typer.Argument(".", help="Directory containing semantic layer files (defaults to current dir)"),
    demo: bool = typer.Option(False, "--demo", help="Use demo data"),
    connection: str = typer.Option(
        None, "--connection", help="Database connection string (e.g., postgres://host/db, bigquery://project/dataset)"
    ),
    db: Path = typer.Option(None, "--db", help="Path to DuckDB database file (shorthand for duckdb:/// connection)"),
    port: int = typer.Option(None, "--port", "-p", help="Port to listen on (overrides config)"),
    username: str = typer.Option(None, "--username", "-u", help="Username for authentication (overrides config)"),
    password: str = typer.Option(None, "--password", help="Password for authentication (overrides config)"),
):
    """
    Start a PostgreSQL-compatible server for the semantic layer.

    Exposes your semantic layer over the PostgreSQL wire protocol, allowing
    you to connect with any PostgreSQL client (psql, DBeaver, Tableau, etc.).

    Examples:
      sidemantic serve ./models --port 5433
      sidemantic serve ./models --db data/warehouse.db
      sidemantic serve ./models --connection "postgres://localhost:5432/analytics"
      sidemantic serve ./models --connection "bigquery://project/dataset" --port 5433
      sidemantic serve --demo
      sidemantic serve ./models --username user --password secret
    """
    import logging

    from sidemantic.server.server import start_server

    logging.basicConfig(level=logging.INFO)

    # Resolve directory from args or config
    if demo:
        import sidemantic

        package_dir = Path(sidemantic.__file__).parent
        demo_dir = package_dir / "examples" / "multi_format_demo"

        if not demo_dir.exists():
            dev_demo_dir = package_dir.parent / "examples" / "multi_format_demo"
            if dev_demo_dir.exists():
                demo_dir = dev_demo_dir
            else:
                typer.echo("Error: Demo models not found", err=True)
                raise typer.Exit(1)

        directory = demo_dir
    elif directory == Path(".") and _loaded_config:
        # Use config file models_dir if using default directory
        directory = Path(_loaded_config.models_dir)

    if not directory.exists():
        typer.echo(f"Error: Directory {directory} does not exist", err=True)
        raise typer.Exit(1)

    # Build connection string from args or config
    connection_str = None
    if connection:
        # Explicit --connection arg provided
        connection_str = connection
    elif db:
        # Explicit --db arg provided
        connection_str = f"duckdb:///{db.absolute()}"
    elif _loaded_config and _loaded_config.connection:
        # Use connection from config
        connection_str = build_connection_string(_loaded_config)

    # Resolve port, username, password from args or config
    port_resolved = port if port is not None else (_loaded_config.pg_server.port if _loaded_config else 5433)
    username_resolved = username or (_loaded_config.pg_server.username if _loaded_config else None)
    password_resolved = password or (_loaded_config.pg_server.password if _loaded_config else None)

    # Create semantic layer (only pass connection if not None, otherwise use default)
    if connection_str:
        layer = SemanticLayer(connection=connection_str)
    else:
        layer = SemanticLayer()

    # Load models
    load_from_directory(layer, str(directory))

    if not layer.graph.models:
        typer.echo("Error: No models found", err=True)
        raise typer.Exit(1)

    # Populate demo data if needed
    if demo:
        try:
            from sidemantic.examples.multi_format_demo.demo_data import create_demo_database
        except ModuleNotFoundError:
            import importlib.util
            import sys

            demo_data_path = directory / "demo_data.py"
            if demo_data_path.exists():
                spec = importlib.util.spec_from_file_location("demo_data", demo_data_path)
                demo_data_module = importlib.util.module_from_spec(spec)
                sys.modules["demo_data"] = demo_data_module
                spec.loader.exec_module(demo_data_module)
                create_demo_database = demo_data_module.create_demo_database
            else:
                raise ImportError(f"Could not find demo_data.py at {demo_data_path}")

        demo_conn = create_demo_database()
        for table in ["customers", "products", "orders"]:
            rows = demo_conn.execute(f"SELECT * FROM {table}").fetchall()
            columns = [desc[0] for desc in demo_conn.execute(f"SELECT * FROM {table} LIMIT 0").description]

            create_sql = demo_conn.execute(f"SELECT sql FROM duckdb_tables() WHERE table_name = '{table}'").fetchone()[
                0
            ]
            layer.conn.execute(create_sql)

            if rows:
                placeholders = ", ".join(["?" for _ in columns])
                layer.conn.executemany(f"INSERT INTO {table} VALUES ({placeholders})", rows)

    # Start the server
    start_server(layer, port=port_resolved, username=username_resolved, password=password_resolved)


if __name__ == "__main__":
    app()
