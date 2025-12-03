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


app = typer.Typer(
    help="Sidemantic: SQL-first semantic layer",
    no_args_is_help=True,
)

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
def migrator(
    directory: Path = typer.Argument(".", help="Directory containing semantic layer files (defaults to current dir)"),
    queries: Path = typer.Option(
        None, "--queries", "-q", help="Path to file or folder containing SQL queries to analyze"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed analysis for each query"),
    generate_models: Path = typer.Option(
        None,
        "--generate-models",
        "-g",
        help="Generate model definitions from queries and write to this directory",
    ),
):
    """
    Migrate SQL queries to semantic layer by generating model definitions.

    Analyzes existing SQL queries to generate model definitions and rewrite
    queries to use semantic layer syntax.

    Examples:
      sidemantic migrator --queries queries/ --generate-models output/
      sidemantic migrator models/ --queries queries/ --verbose
    """
    from sidemantic.core.migrator import Migrator

    if not queries:
        typer.echo("Error: --queries is required", err=True)
        typer.echo("Usage: sidemantic migrator [models_dir] --queries <path>", err=True)
        raise typer.Exit(1)

    if not queries.exists():
        typer.echo(f"Error: {queries} does not exist", err=True)
        raise typer.Exit(1)

    # Bootstrap mode - generate models from queries
    if generate_models:
        try:
            # Create empty semantic layer for analysis
            layer = SemanticLayer(auto_register=False)
            analyzer = Migrator(layer)

            # Analyze queries
            if queries.is_file():
                query_list = queries.read_text().split(";")
                query_list = [q.strip() for q in query_list if q.strip()]
                report = analyzer.analyze_queries(query_list)
            else:
                report = analyzer.analyze_folder(str(queries))

            # Generate model definitions
            typer.echo("\nGenerating model definitions...", err=True)
            models = analyzer.generate_models(report)

            models_dir = generate_models / "models"
            analyzer.write_model_files(models, str(models_dir))

            # Generate rewritten queries
            typer.echo("\nGenerating rewritten queries...", err=True)
            rewritten = analyzer.generate_rewritten_queries(report)

            queries_dir = generate_models / "rewritten_queries"
            analyzer.write_rewritten_queries(rewritten, str(queries_dir))

            typer.echo(
                f"\n✓ Generated {len(models)} models and {len(rewritten)} rewritten queries in {generate_models}",
                err=True,
            )

        except Exception as e:
            typer.echo(f"Error: {e}", err=True)
            import traceback

            traceback.print_exc()
            raise typer.Exit(1)

    # Coverage analysis mode - compare queries against existing models
    else:
        if not directory.exists():
            typer.echo(f"Error: Directory {directory} does not exist", err=True)
            raise typer.Exit(1)

        try:
            # Load semantic layer
            layer = SemanticLayer()
            load_from_directory(layer, str(directory))

            if not layer.graph.models:
                typer.echo("Error: No models found in semantic layer", err=True)
                raise typer.Exit(1)

            # Create analyzer
            analyzer = Migrator(layer)

            # Analyze queries
            if queries.is_file():
                # Single file - load queries from it
                query_list = queries.read_text().split(";")
                query_list = [q.strip() for q in query_list if q.strip()]
                report = analyzer.analyze_queries(query_list)
            else:
                # Directory - load all .sql files
                report = analyzer.analyze_folder(str(queries))

            # Print report
            analyzer.print_report(report, verbose=verbose)

        except Exception as e:
            typer.echo(f"Error: {e}", err=True)
            import traceback

            traceback.print_exc()
            raise typer.Exit(1)


@app.command()
def info(
    directory: Path = typer.Argument(".", help="Directory containing semantic layer files (defaults to current dir)"),
):
    """
    Show quick info about the semantic layer.

    Examples:
      sidemantic info
      sidemantic info ./models
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
      sidemantic mcp-serve
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
def query(
    sql: str = typer.Argument(..., help="SQL query to execute"),
    models: Path = typer.Option(".", "--models", "-m", help="Directory containing semantic layer files"),
    output: Path = typer.Option(None, "--output", "-o", help="Output file (default: stdout)"),
    connection: str = typer.Option(
        None, "--connection", help="Database connection string (e.g., postgres://host/db, bigquery://project/dataset)"
    ),
    db: Path = typer.Option(None, "--db", help="Path to DuckDB database file (shorthand for duckdb:/// connection)"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show generated SQL without executing"),
):
    """
    Execute a SQL query and output results as CSV.

    Examples:
      sidemantic query "SELECT revenue FROM orders"
      sidemantic query "SELECT * FROM orders" --output results.csv
      sidemantic query "SELECT * FROM orders" --models ./models
      sidemantic query "SELECT revenue FROM orders" --connection "postgres://localhost:5432/db"
      sidemantic query "SELECT revenue FROM orders" --db data.duckdb
      sidemantic query "SELECT revenue FROM orders" --dry-run
    """
    if not models.exists():
        typer.echo(f"Error: Directory {models} does not exist", err=True)
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
            data_dir = models / "data"
            if data_dir.exists():
                db_files = list(data_dir.glob("*.db"))
                if db_files:
                    connection_str = f"duckdb:///{db_files[0].absolute()}"

        # Load semantic layer (only pass connection if not None)
        preagg_db = _loaded_config.preagg_database if _loaded_config else None
        preagg_sch = _loaded_config.preagg_schema if _loaded_config else None
        if connection_str:
            layer = SemanticLayer(connection=connection_str, preagg_database=preagg_db, preagg_schema=preagg_sch)
        else:
            layer = SemanticLayer(preagg_database=preagg_db, preagg_schema=preagg_sch)
        load_from_directory(layer, str(models))

        if not layer.graph.models:
            typer.echo("Error: No models found", err=True)
            raise typer.Exit(1)

        # Dry run: show generated SQL without executing
        if dry_run:
            from sidemantic.sql.query_rewriter import QueryRewriter

            rewriter = QueryRewriter(layer.graph, dialect=layer.adapter.dialect)
            rewritten_sql = rewriter.rewrite(sql)
            typer.echo(rewritten_sql)
            return

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
      sidemantic serve --port 5433
      sidemantic serve ./models --db data/warehouse.db
      sidemantic serve --connection "postgres://localhost:5432/analytics"
      sidemantic serve --connection "bigquery://project/dataset" --port 5433
      sidemantic serve --demo
      sidemantic serve --username user --password secret
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
    preagg_db = _loaded_config.preagg_database if _loaded_config else None
    preagg_sch = _loaded_config.preagg_schema if _loaded_config else None
    if connection_str:
        layer = SemanticLayer(connection=connection_str, preagg_database=preagg_db, preagg_schema=preagg_sch)
    else:
        layer = SemanticLayer(preagg_database=preagg_db, preagg_schema=preagg_sch)

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


@app.command(hidden=True)
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
    directory: Path = typer.Argument(".", help="Directory containing semantic layer files (defaults to current dir)"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed validation results"),
):
    """
    Validate semantic layer definitions.

    Shows errors, warnings, and optionally detailed info in an interactive view.

    Examples:
      sidemantic validate
      sidemantic validate ./models --verbose
    """
    from sidemantic.workbench import run_validation

    if not directory.exists():
        typer.echo(f"Error: Directory {directory} does not exist", err=True)
        raise typer.Exit(1)

    run_validation(directory, verbose=verbose)


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
      sidemantic workbench
      sidemantic workbench --demo
      sidemantic workbench ./models --db data/warehouse.db
      sidemantic workbench ./models --connection "postgres://localhost:5432/db"
      uvx sidemantic workbench --demo
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


# Pre-aggregation recommendation commands
preagg_app = typer.Typer(
    help="Pre-aggregation recommendation and management",
    no_args_is_help=True,
)
app.add_typer(preagg_app, name="preagg")


@preagg_app.command("recommend")
def preagg_recommend(
    queries: Path = typer.Option(None, "--queries", "-q", help="Path to file/folder with SQL queries"),
    connection: str = typer.Option(None, "--connection", help="Database connection string to fetch query history"),
    db: Path = typer.Option(None, "--db", help="Path to DuckDB database file (shorthand for duckdb:/// connection)"),
    days_back: int = typer.Option(7, "--days", "-d", help="Days of query history to fetch (default: 7)"),
    limit: int = typer.Option(1000, "--limit", "-l", help="Max queries to analyze (default: 1000)"),
    min_count: int = typer.Option(10, "--min-count", help="Minimum query count for recommendation (default: 10)"),
    min_score: float = typer.Option(0.3, "--min-score", help="Minimum benefit score (0-1, default: 0.3)"),
    top_n: int = typer.Option(None, "--top", "-n", help="Show only top N recommendations"),
):
    """
    Show pre-aggregation recommendations based on query patterns.

    Examples:
      sidemantic preagg recommend --connection "bigquery://project/dataset"
      sidemantic preagg recommend --db data.db --min-count 50 --top 10
      sidemantic preagg recommend --queries queries.sql --min-score 0.5
    """
    from sidemantic.core.preagg_recommender import PreAggregationRecommender

    if not queries and not connection and not db:
        typer.echo("Error: Must specify --queries, --connection, or --db", err=True)
        raise typer.Exit(1)

    try:
        recommender = PreAggregationRecommender(min_query_count=min_count, min_benefit_score=min_score)

        # Fetch/parse queries (same as analyze command)
        if connection or db:
            connection_str = connection if connection else f"duckdb:///{db.absolute()}"
            from sidemantic import SemanticLayer

            preagg_db = _loaded_config.preagg_database if _loaded_config else None
            preagg_sch = _loaded_config.preagg_schema if _loaded_config else None
            layer = SemanticLayer(connection=connection_str, preagg_database=preagg_db, preagg_schema=preagg_sch)
            adapter = layer._adapter
            typer.echo(f"Fetching query history from {adapter.dialect}...", err=True)
            recommender.fetch_and_parse_query_history(adapter, days_back=days_back, limit=limit)
        elif queries:
            if not queries.exists():
                typer.echo(f"Error: {queries} does not exist", err=True)
                raise typer.Exit(1)
            if queries.is_file():
                recommender.parse_query_log_file(str(queries))
            else:
                for sql_file in queries.glob("**/*.sql"):
                    recommender.parse_query_log_file(str(sql_file))

        # Print summary
        summary = recommender.get_summary()
        typer.echo(f"\n✓ Analyzed {summary['total_queries']} queries", err=True)
        typer.echo(f"  Found {summary['unique_patterns']} unique patterns", err=True)
        typer.echo(f"  {summary['patterns_above_threshold']} patterns above threshold", err=True)

        if summary["models"]:
            typer.echo("\n  Models:", err=True)
            for model_name, count in summary["models"].items():
                typer.echo(f"    {model_name}: {count} queries", err=True)

        # Get recommendations
        recommendations = recommender.get_recommendations(top_n=top_n)

        if not recommendations:
            typer.echo("\nNo recommendations found above thresholds", err=True)
            typer.echo(
                f"Try lowering --min-count (currently {min_count}) or --min-score (currently {min_score})", err=True
            )
            raise typer.Exit(0)

        # Print recommendations
        typer.echo(f"\n{'=' * 80}")
        typer.echo(f"Pre-Aggregation Recommendations (found {len(recommendations)})")
        typer.echo(f"{'=' * 80}\n")

        for i, rec in enumerate(recommendations, 1):
            typer.echo(f"{i}. {rec.suggested_name}")
            typer.echo(f"   Model: {rec.pattern.model}")
            typer.echo(f"   Query Count: {rec.query_count}")
            typer.echo(f"   Benefit Score: {rec.estimated_benefit_score:.2f}")
            typer.echo(f"   Metrics: {', '.join(sorted(rec.pattern.metrics))}")
            typer.echo(
                f"   Dimensions: {', '.join(sorted(rec.pattern.dimensions)) if rec.pattern.dimensions else '(none)'}"
            )
            if rec.pattern.granularities:
                typer.echo(f"   Granularities: {', '.join(sorted(rec.pattern.granularities))}")
            typer.echo()

        typer.echo("Run 'sidemantic preagg apply' to add these to your models", err=True)

    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        import traceback

        traceback.print_exc()
        raise typer.Exit(1)


@preagg_app.command("apply")
def preagg_apply(
    directory: Path = typer.Argument(".", help="Directory containing semantic layer YAML files"),
    queries: Path = typer.Option(None, "--queries", "-q", help="Path to file/folder with SQL queries"),
    connection: str = typer.Option(None, "--connection", help="Database connection string to fetch query history"),
    db: Path = typer.Option(None, "--db", help="Path to DuckDB database file (shorthand for duckdb:/// connection)"),
    days_back: int = typer.Option(7, "--days", "-d", help="Days of query history to fetch (default: 7)"),
    limit: int = typer.Option(1000, "--limit", "-l", help="Max queries to analyze (default: 1000)"),
    min_count: int = typer.Option(10, "--min-count", help="Minimum query count for recommendation (default: 10)"),
    min_score: float = typer.Option(0.3, "--min-score", help="Minimum benefit score (0-1, default: 0.3)"),
    top_n: int = typer.Option(None, "--top", "-n", help="Apply only top N recommendations"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be added without writing files"),
):
    """
    Apply pre-aggregation recommendations to model YAML files.

    Analyzes query patterns and automatically adds pre-aggregation definitions to model YAML files.

    Examples:
      sidemantic preagg apply models/ --connection "bigquery://project/dataset"
      sidemantic preagg apply models/ --db data.db --top 5
      sidemantic preagg apply models/ --queries queries.sql --dry-run
    """
    import yaml

    from sidemantic.core.preagg_recommender import PreAggregationRecommender

    if not directory.exists():
        typer.echo(f"Error: Directory {directory} does not exist", err=True)
        raise typer.Exit(1)

    if not queries and not connection and not db:
        typer.echo("Error: Must specify --queries, --connection, or --db", err=True)
        raise typer.Exit(1)

    try:
        recommender = PreAggregationRecommender(min_query_count=min_count, min_benefit_score=min_score)

        # Fetch/parse queries
        if connection or db:
            connection_str = connection if connection else f"duckdb:///{db.absolute()}"
            from sidemantic import SemanticLayer

            preagg_db = _loaded_config.preagg_database if _loaded_config else None
            preagg_sch = _loaded_config.preagg_schema if _loaded_config else None
            layer = SemanticLayer(connection=connection_str, preagg_database=preagg_db, preagg_schema=preagg_sch)
            adapter = layer._adapter
            typer.echo(f"Fetching query history from {adapter.dialect}...", err=True)
            recommender.fetch_and_parse_query_history(adapter, days_back=days_back, limit=limit)
        elif queries:
            if not queries.exists():
                typer.echo(f"Error: {queries} does not exist", err=True)
                raise typer.Exit(1)
            if queries.is_file():
                recommender.parse_query_log_file(str(queries))
            else:
                for sql_file in queries.glob("**/*.sql"):
                    recommender.parse_query_log_file(str(sql_file))

        # Get recommendations
        recommendations = recommender.get_recommendations(top_n=top_n)

        if not recommendations:
            typer.echo("No recommendations found above thresholds", err=True)
            raise typer.Exit(0)

        typer.echo(f"\nFound {len(recommendations)} recommendations to apply\n", err=True)

        # Group recommendations by model
        by_model = {}
        for rec in recommendations:
            model_name = rec.pattern.model
            if model_name not in by_model:
                by_model[model_name] = []
            by_model[model_name].append(rec)

        # Find and update model YAML files
        yaml_files = list(directory.glob("**/*.yml")) + list(directory.glob("**/*.yaml"))

        updated_count = 0
        for model_name, recs in by_model.items():
            # Find YAML file for this model
            model_file = None
            for yaml_file in yaml_files:
                with open(yaml_file) as f:
                    data = yaml.safe_load(f)
                    if not data:
                        continue

                    # Check if this file contains the model
                    models = data.get("models", [])
                    for model_def in models:
                        if model_def.get("name") == model_name:
                            model_file = yaml_file
                            break
                    if model_file:
                        break

            if not model_file:
                typer.echo(f"Warning: Could not find YAML file for model '{model_name}'", err=True)
                continue

            # Load and update the YAML file
            with open(model_file) as f:
                data = yaml.safe_load(f)

            # Find the model in the file
            for model_def in data.get("models", []):
                if model_def.get("name") == model_name:
                    # Get existing pre_aggregations or create new list
                    if "pre_aggregations" not in model_def:
                        model_def["pre_aggregations"] = []

                    # Add new pre-aggregations
                    for rec in recs:
                        preagg_def = recommender.generate_preagg_definition(rec)

                        # Convert to dict for YAML
                        preagg_dict = {"name": preagg_def.name, "measures": preagg_def.measures}

                        if preagg_def.dimensions:
                            preagg_dict["dimensions"] = preagg_def.dimensions
                        if preagg_def.time_dimension:
                            preagg_dict["time_dimension"] = preagg_def.time_dimension
                        if preagg_def.granularity:
                            preagg_dict["granularity"] = preagg_def.granularity

                        model_def["pre_aggregations"].append(preagg_dict)

                        typer.echo(f"  + {model_name}.{preagg_def.name} ({rec.query_count} queries)", err=True)
                        updated_count += 1

            # Write back to file (unless dry run)
            if not dry_run:
                with open(model_file, "w") as f:
                    yaml.dump(data, f, default_flow_style=False, sort_keys=False)

        if dry_run:
            typer.echo(f"\nDry run: Would add {updated_count} pre-aggregations", err=True)
            typer.echo("Remove --dry-run to apply changes", err=True)
        else:
            typer.echo(f"\n✓ Added {updated_count} pre-aggregations to model files", err=True)

    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        import traceback

        traceback.print_exc()
        raise typer.Exit(1)


@preagg_app.command("refresh")
def refresh(
    directory: Path = typer.Argument(".", help="Directory containing semantic layer files (defaults to current dir)"),
    model: str = typer.Option(None, "--model", "-m", help="Only refresh pre-aggregations for this model"),
    preagg: str = typer.Option(None, "--preagg", "-p", help="Only refresh this specific pre-aggregation"),
    mode: str = typer.Option("incremental", "--mode", help="Refresh mode: full, incremental, or merge"),
    connection: str = typer.Option(
        None, "--connection", help="Database connection string (e.g., postgres://host/db, bigquery://project/dataset)"
    ),
    db: Path = typer.Option(None, "--db", help="Path to DuckDB database file (shorthand for duckdb:/// connection)"),
):
    """
    Refresh pre-aggregation tables.

    Generates materialization SQL for pre-aggregations and executes refresh.
    Stateless: watermarks derived from existing tables. Use cron/Airflow for scheduling.

    Examples:
      sidemantic preagg refresh models/ --db data.db
      sidemantic preagg refresh models/ --model orders --mode full
      sidemantic preagg refresh models/ --connection "postgres://localhost:5432/db"
    """
    if not directory.exists():
        typer.echo(f"Error: Directory {directory} does not exist", err=True)
        raise typer.Exit(1)

    try:
        # Load semantic layer
        preagg_db = _loaded_config.preagg_database if _loaded_config else None
        preagg_sch = _loaded_config.preagg_schema if _loaded_config else None
        layer = SemanticLayer(preagg_database=preagg_db, preagg_schema=preagg_sch)
        load_from_directory(layer, str(directory))

        if not layer.graph.models:
            typer.echo("Error: No models found", err=True)
            raise typer.Exit(1)

        # Build connection string
        connection_str = None
        if connection:
            connection_str = connection
        elif db:
            connection_str = f"duckdb:///{db.absolute()}"
        elif _loaded_config and _loaded_config.connection:
            connection_str = build_connection_string(_loaded_config)
        else:
            typer.echo("Error: No database connection specified. Use --db or --connection", err=True)
            raise typer.Exit(1)

        # Connect to database
        if connection_str.startswith("duckdb://"):
            import duckdb

            db_path = connection_str.replace("duckdb:///", "")
            conn = duckdb.connect(db_path)

            # Create schema if it doesn't exist (DuckDB only)
            if preagg_sch:
                conn.execute(f"CREATE SCHEMA IF NOT EXISTS {preagg_sch}")
        elif mode == "engine":
            # For engine mode, use the database adapter
            temp_layer = SemanticLayer(connection=connection_str)
            conn = temp_layer._adapter.raw_connection
        else:
            typer.echo(f"Error: Unsupported connection type: {connection_str}", err=True)
            typer.echo(
                "Currently only DuckDB is supported for manual refresh modes (full, incremental, merge)", err=True
            )
            typer.echo("Use --mode engine for Snowflake, ClickHouse, BigQuery materialized views", err=True)
            raise typer.Exit(1)

        # Find pre-aggregations to refresh
        preaggs_to_refresh = []

        for model_name, model_obj in layer.graph.models.items():
            # Filter by model if specified
            if model and model_name != model:
                continue

            for preagg_obj in model_obj.pre_aggregations:
                # Filter by preagg name if specified
                if preagg and preagg_obj.name != preagg:
                    continue

                preaggs_to_refresh.append((model_name, model_obj, preagg_obj))

        if not preaggs_to_refresh:
            typer.echo("No pre-aggregations found to refresh", err=True)
            raise typer.Exit(1)

        typer.echo(f"\nRefreshing {len(preaggs_to_refresh)} pre-aggregation(s)...\n", err=True)

        # Get dialect from connection string for engine mode
        dialect = None
        if mode == "engine":
            if "snowflake" in connection_str:
                dialect = "snowflake"
            elif "clickhouse" in connection_str:
                dialect = "clickhouse"
            elif "bigquery" in connection_str:
                dialect = "bigquery"
            else:
                typer.echo(f"Error: Unsupported dialect for engine mode: {connection_str}", err=True)
                typer.echo("Engine mode supports: snowflake, clickhouse, bigquery", err=True)
                raise typer.Exit(1)

        # Refresh each pre-aggregation
        for model_name, model_obj, preagg_obj in preaggs_to_refresh:
            # Get database/schema from config if available
            database = _loaded_config.preagg_database if _loaded_config else None
            schema = _loaded_config.preagg_schema if _loaded_config else None
            table_name = preagg_obj.get_table_name(model_name, database=database, schema=schema)

            # Generate materialization SQL
            source_sql = preagg_obj.generate_materialization_sql(model_obj)

            # Determine watermark column
            watermark_column = None
            if mode in ["incremental", "merge"] and preagg_obj.time_dimension and preagg_obj.granularity:
                watermark_column = f"{preagg_obj.time_dimension}_{preagg_obj.granularity}"

            # Refresh
            typer.echo(f"Refreshing {model_name}.{preagg_obj.name} ({mode})...", err=True)
            result = preagg_obj.refresh(
                connection=conn,
                source_sql=source_sql,
                table_name=table_name,
                mode=mode,
                watermark_column=watermark_column,
                dialect=dialect,
            )

            # Print result
            if result.rows_inserted >= 0:
                typer.echo(f"  ✓ {table_name}: {result.rows_inserted} rows in {result.duration_seconds:.2f}s", err=True)
            else:
                typer.echo(f"  ✓ {table_name}: completed in {result.duration_seconds:.2f}s", err=True)

        typer.echo("\nDone!", err=True)

    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        import traceback

        traceback.print_exc()
        raise typer.Exit(1)


@app.command()
def lsp():
    """
    Start the LSP server for Sidemantic SQL dialect (.sql definition files).

    This provides editor support for the Sidemantic SQL syntax (MODEL, DIMENSION,
    METRIC, RELATIONSHIP, SEGMENT statements). It does NOT provide general SQL
    language support.

    Features:
    - Autocompletion for MODEL, DIMENSION, METRIC, RELATIONSHIP, SEGMENT
    - Context-aware property suggestions (name, type, sql, agg, etc.)
    - Validation errors from pydantic models
    - Hover documentation for keywords and properties

    Editor setup:

    VS Code: Use a generic LSP client extension pointing to 'sidemantic lsp'
    Neovim: Add custom server config to nvim-lspconfig

    Examples:
      sidemantic lsp  # Starts server on stdio
    """
    from sidemantic.lsp import main as lsp_main

    lsp_main()


if __name__ == "__main__":
    app()
