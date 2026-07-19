"""CLI for sidemantic semantic layer operations."""

from __future__ import annotations

import os
from pathlib import Path

import typer

from sidemantic import __version__
from sidemantic.config import SidemanticConfig
from sidemantic.project import ProjectContext, ProjectResolutionError


def SemanticLayer(*args, **kwargs):  # noqa: N802 - deliberately shadows the class so call sites stay unchanged while deferring the import
    """Lazy SemanticLayer constructor for fast help/version paths."""
    from sidemantic import SemanticLayer as _SemanticLayer

    return _SemanticLayer(*args, **kwargs)


def load_from_directory(*args, **kwargs):
    """Lazy loader import for fast help/version paths."""
    from sidemantic import load_from_directory as _load_from_directory

    return _load_from_directory(*args, **kwargs)


def version_callback(value: bool):
    """Print version and exit."""
    if value:
        typer.echo(f"sidemantic {__version__}")
        raise typer.Exit()


app = typer.Typer(
    help="Sidemantic: SQL-first semantic layer",
    no_args_is_help=True,
)
dashboard_app = typer.Typer(
    help="Serve, validate, and type semantic dashboard specs",
    no_args_is_help=True,
)
app.add_typer(dashboard_app, name="dashboard")
gen_app = typer.Typer(
    help="Generate typed TypeScript clients from the semantic layer",
    no_args_is_help=True,
)
app.add_typer(gen_app, name="gen", hidden=True)
app.add_typer(gen_app, name="generate")
migrate_app = typer.Typer(
    help="Generate semantic models from SQL or check migration coverage",
    no_args_is_help=True,
)
app.add_typer(migrate_app, name="migrate")
server_app = typer.Typer(
    help="Run Sidemantic API, PostgreSQL, or MCP servers",
    no_args_is_help=True,
)
app.add_typer(server_app, name="server")

# Global state for config (set in callback, used in commands)
_loaded_config: SidemanticConfig | None = None
_project_context: ProjectContext | None = None


def _project() -> ProjectContext:
    """Return the project selected by the root command callback."""

    return _project_context or ProjectContext.discover()


def _models_path(explicit: Path | None = None) -> Path:
    """Resolve the shared model source for a project-aware command."""

    try:
        return _project().resolve_models(explicit)
    except ProjectResolutionError as exc:
        raise typer.BadParameter(str(exc), param_hint="--models") from exc


def _dashboard_path(explicit: Path | None = None) -> Path:
    try:
        resolved = _project().resolve_dashboard(explicit)
        assert resolved is not None
        return resolved
    except ProjectResolutionError as exc:
        raise typer.BadParameter(str(exc), param_hint="dashboard spec") from exc


def _resolve_connection(**kwargs):
    try:
        return _project().resolve_connection(**kwargs)
    except ProjectResolutionError as exc:
        raise typer.BadParameter(str(exc)) from exc


def _normalize_engine(engine: str | None) -> str | None:
    if engine is None:
        return None
    normalized = engine.lower()
    if normalized not in {"python", "rust", "auto"}:
        raise typer.BadParameter("engine must be one of: python, rust, auto")
    return normalized


def _configure_engine_environment(engine: str | None, fallback: bool) -> None:
    if engine is None:
        return

    if engine == "python":
        os.environ["SIDEMANTIC_RS_SQL_GENERATOR"] = "0"
        os.environ["SIDEMANTIC_RS_QUERY_VALIDATION"] = "0"
        os.environ["SIDEMANTIC_RS_REWRITER"] = "0"
        os.environ["SIDEMANTIC_RS_NO_FALLBACK"] = "0"
        return

    os.environ["SIDEMANTIC_RS_SQL_GENERATOR"] = "1"
    os.environ["SIDEMANTIC_RS_QUERY_VALIDATION"] = "1"
    os.environ["SIDEMANTIC_RS_REWRITER"] = "1"
    os.environ["SIDEMANTIC_RS_SQL_GENERATOR_VERIFY"] = "0"
    os.environ["SIDEMANTIC_RS_NO_FALLBACK"] = "0" if fallback else "1"


def _resolve_engine_options(engine: str | None, fallback: bool | None) -> tuple[str | None, bool]:
    resolved_engine = _normalize_engine(engine)
    resolved_fallback = fallback

    if resolved_engine is None and _loaded_config and _loaded_config.runtime:
        resolved_engine = _loaded_config.runtime.engine
        if resolved_fallback is None:
            resolved_fallback = _loaded_config.runtime.fallback

    if resolved_fallback is None:
        resolved_fallback = resolved_engine == "auto"

    if resolved_engine is None and fallback is not None:
        raise typer.BadParameter("--fallback/--no-fallback requires --engine or runtime.engine in config")
    if resolved_engine == "python" and resolved_fallback:
        raise typer.BadParameter("--fallback is only meaningful with the rust or auto engine")

    return resolved_engine, resolved_fallback


def _load_query_layer(
    models: Path | None = None,
    connection: str | None = None,
    db: Path | None = None,
    use_preaggregations: bool = False,
    engine: str | None = None,
    fallback: bool | None = None,
) -> SemanticLayer:
    """Load a semantic layer for CLI query/explain commands."""
    engine, resolved_fallback = _resolve_engine_options(engine, fallback)
    _configure_engine_environment(engine, resolved_fallback)

    models = _models_path(models)
    resolved_connection = _resolve_connection(connection=connection, database=db, models=models)
    connection_str = resolved_connection.connection if resolved_connection else None
    init_sql = resolved_connection.init_sql if resolved_connection else None

    preagg_db = _loaded_config.preagg_database if _loaded_config else None
    preagg_sch = _loaded_config.preagg_schema if _loaded_config else None
    layer_kwargs = {
        "preagg_database": preagg_db,
        "preagg_schema": preagg_sch,
        "use_preaggregations": use_preaggregations,
        "engine": engine,
        "fallback": resolved_fallback,
    }
    if connection_str:
        layer = SemanticLayer(connection=connection_str, init_sql=init_sql, **layer_kwargs)
    else:
        layer = SemanticLayer(**layer_kwargs)

    if models.is_file():
        # Load exactly the requested file, not its whole parent directory: a
        # sibling model or an unrelated broken draft must not pollute or fail the load.
        from sidemantic.loaders import load_from_file

        load_from_file(layer, models)
    else:
        load_from_directory(layer, str(models))
    if not layer.graph.models:
        raise ValueError("No models found")
    return layer


def _load_graph_layer(
    models: Path | None = None,
    *,
    engine: str | None = None,
    fallback: bool | None = None,
) -> SemanticLayer:
    """Load project models without opening the configured database."""

    models = _models_path(models)
    engine, resolved_fallback = _resolve_engine_options(engine, fallback)
    _configure_engine_environment(engine, resolved_fallback)
    layer = SemanticLayer(engine=engine, fallback=resolved_fallback)
    if models.is_file():
        from sidemantic.loaders import load_from_file

        load_from_file(layer, models)
    else:
        load_from_directory(layer, str(models))
    if not layer.graph.models:
        raise ValueError("No models found")
    return layer


@app.callback()
def main(
    version: bool = typer.Option(
        None, "--version", "-v", callback=version_callback, is_eager=True, help="Show version"
    ),
    project: Path = typer.Option(None, "--project", "-p", help="Project root (defaults to discovery from cwd)"),
    config: Path = typer.Option(None, "--config", "-c", help="Path to config file (sidemantic.yaml)"),
):
    """Sidemantic CLI.

    You can use a config file (sidemantic.yaml or sidemantic.json) to set default values.
    CLI arguments override config file values.
    """
    global _loaded_config, _project_context

    try:
        _project_context = ProjectContext.discover(start_dir=project, config_path=config)
        _loaded_config = _project_context.config
    except ProjectResolutionError as exc:
        raise typer.BadParameter(str(exc), param_hint="--config") from exc


@app.command(hidden=True)
def migrator(
    directory: Path = typer.Argument(
        None, help="Directory containing semantic layer files (defaults to project models)"
    ),
    queries: Path = typer.Option(
        None, "--queries", "-q", help="Path to file or folder containing SQL queries to analyze"
    ),
    connection: str = typer.Option(
        None,
        "--connection",
        help="Database connection string used to import warehouse query history",
    ),
    days_back: int = typer.Option(7, "--days", "-d", help="Days of query history to import (default: 7)"),
    limit: int = typer.Option(1000, "--limit", "-l", help="Maximum history queries to import (default: 1000)"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed analysis for each query"),
    generate_models: Path = typer.Option(
        None,
        "--generate-models",
        "-g",
        help="Generate model definitions from queries and write to this directory",
    ),
    _warn_deprecated: bool = typer.Option(True, hidden=True),
):
    """
    Migrate SQL queries to semantic layer by generating model definitions.

    Analyzes existing SQL queries to generate model definitions and rewrite
    queries to use semantic layer syntax.

    Examples:
      sidemantic migrator --queries queries/ --generate-models output/
      sidemantic migrator --connection "snowflake://..." --days 7 --generate-models output/
      sidemantic migrator models/ --queries queries/ --verbose
    """
    from sidemantic.core.migrator import Migrator

    if _warn_deprecated:
        typer.echo("Warning: `migrator` is deprecated; use `migrate generate` or `migrate check`", err=True)

    source_dialect = "duckdb"

    if not queries and not connection:
        typer.echo("Error: Must specify --queries or --connection", err=True)
        typer.echo("Usage: sidemantic migrator [models_dir] (--queries <path> | --connection <url>)", err=True)
        raise typer.Exit(1)

    if queries and connection:
        typer.echo("Error: --queries and --connection are mutually exclusive", err=True)
        raise typer.Exit(1)

    if queries and not queries.exists():
        typer.echo(f"Error: {queries} does not exist", err=True)
        raise typer.Exit(1)

    def load_queries_from_source() -> list[str] | None:
        nonlocal source_dialect
        if connection:
            history_layer = SemanticLayer(connection=connection, auto_register=False)
            adapter = history_layer.adapter
            source_dialect = adapter.dialect
            get_query_history = getattr(adapter, "get_query_history", None)
            if get_query_history is None:
                raise ValueError(f"{adapter.dialect} does not support query-history import")
            typer.echo(
                f"Importing up to {limit} queries from the last {days_back} day(s) of {adapter.dialect} history...",
                err=True,
            )
            try:
                try:
                    imported = get_query_history(days_back=days_back, limit=limit, instrumented_only=False)
                except TypeError as exc:
                    if "instrumented_only" not in str(exc):
                        raise
                    raise ValueError(
                        f"{adapter.dialect} adapter does not support unfiltered query-history import"
                    ) from exc
            finally:
                close_adapter = getattr(adapter, "close", None)
                if close_adapter:
                    close_adapter()
            queries_from_history = [query.strip() for query in imported if query and query.strip()]
            typer.echo(f"Imported {len(queries_from_history)} queries from warehouse history", err=True)
            if not queries_from_history:
                raise ValueError("No queries found in warehouse history for the requested time range")
            return queries_from_history

        if queries and queries.is_file():
            return [query.strip() for query in queries.read_text().split(";") if query.strip()]
        return None

    # Bootstrap mode - generate models from queries
    if generate_models:
        try:
            # Create empty semantic layer for analysis
            layer = SemanticLayer(auto_register=False)
            # Analyze queries
            query_list = load_queries_from_source()
            analyzer = Migrator(layer, dialect=source_dialect)
            if query_list is not None:
                report = analyzer.analyze_queries(query_list)
            else:
                assert queries is not None
                report = analyzer.analyze_folder(str(queries))

            # Generate model definitions
            typer.echo("\nGenerating model definitions...", err=True)
            models = analyzer.generate_models(report)

            models_dir = generate_models / "models"
            analyzer.write_model_files(models, str(models_dir))
            graph_metrics = analyzer.generate_graph_metrics(report, models)
            analyzer.write_graph_metrics_file(graph_metrics, str(models_dir))

            # Generate rewritten queries
            typer.echo("\nGenerating rewritten queries...", err=True)
            rewritten = analyzer.generate_rewritten_queries(report)

            queries_dir = generate_models / "rewritten_queries"
            analyzer.write_rewritten_queries(rewritten, str(queries_dir))

            typer.echo(
                f"\n✓ Generated {len(models)} models, {len(graph_metrics)} graph metrics, "
                f"and {len(rewritten)} rewritten queries in {generate_models}",
                err=True,
            )

        except typer.Exit:
            raise
        except Exception as e:
            typer.echo(f"Error: {e}", err=True)
            raise typer.Exit(1)

    # Coverage analysis mode - compare queries against existing models
    else:
        try:
            directory = _models_path(directory)
            # Load semantic layer
            layer = SemanticLayer()
            load_from_directory(layer, str(directory))

            if not layer.graph.models:
                typer.echo("Error: No models found in semantic layer", err=True)
                raise typer.Exit(1)

            # Analyze queries
            query_list = load_queries_from_source()
            analyzer = Migrator(layer, dialect=source_dialect)
            if query_list is not None:
                report = analyzer.analyze_queries(query_list)
            else:
                # Directory - load all .sql files
                assert queries is not None
                report = analyzer.analyze_folder(str(queries))

            # Print report
            analyzer.print_report(report, verbose=verbose)

        except typer.Exit:
            raise
        except Exception as e:
            typer.echo(f"Error: {e}", err=True)
            raise typer.Exit(1)


def _migration_queries(queries: Path | None) -> Path:
    project = _project()
    if queries is None:
        resolved = project.root / "queries"
    else:
        resolved = queries.expanduser()
        if not resolved.is_absolute():
            resolved = project.root / resolved
    resolved = resolved.resolve()
    if not resolved.exists():
        raise typer.BadParameter(
            f"Query source not found: {resolved}",
            param_hint="QUERIES",
        )
    return resolved


@migrate_app.command("generate")
def migrate_generate(
    queries: Path = typer.Argument(None, help="SQL file or directory (defaults to project queries/)"),
    output: Path = typer.Option(None, "--output", "-o", help="Output project root (defaults to current project)"),
    history: bool = typer.Option(False, "--history", help="Generate from configured warehouse query history"),
    connection: str = typer.Option(None, "--connection", help="Connection override for --history"),
    days_back: int = typer.Option(7, "--days", "-d"),
    limit: int = typer.Option(1000, "--limit", "-l"),
):
    """Generate models and rewritten queries from SQL or warehouse history."""

    if history and queries is not None:
        raise typer.BadParameter("QUERIES and --history are mutually exclusive")
    if connection and not history:
        raise typer.BadParameter("--connection requires --history")
    resolved_connection = None
    if history:
        selected = _resolve_connection(connection=connection, required=True)
        assert selected is not None
        resolved_connection = selected.connection
    migrator(
        directory=None,
        queries=None if history else _migration_queries(queries),
        connection=resolved_connection,
        days_back=days_back,
        limit=limit,
        verbose=False,
        generate_models=output or _project().root,
        _warn_deprecated=False,
    )


@migrate_app.command("check")
def migrate_check(
    queries: Path = typer.Argument(None, help="SQL file or directory (defaults to project queries/)"),
    models: Path = typer.Option(None, "--models", "-m", help="Models override"),
    history: bool = typer.Option(False, "--history", help="Check configured warehouse query history"),
    connection: str = typer.Option(None, "--connection", help="Connection override for --history"),
    days_back: int = typer.Option(7, "--days", "-d"),
    limit: int = typer.Option(1000, "--limit", "-l"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
):
    """Check how well the project models cover existing SQL."""

    if history and queries is not None:
        raise typer.BadParameter("QUERIES and --history are mutually exclusive")
    if connection and not history:
        raise typer.BadParameter("--connection requires --history")
    resolved_connection = None
    if history:
        selected = _resolve_connection(connection=connection, required=True)
        assert selected is not None
        resolved_connection = selected.connection
    migrator(
        directory=_models_path(models),
        queries=None if history else _migration_queries(queries),
        connection=resolved_connection,
        days_back=days_back,
        limit=limit,
        verbose=verbose,
        generate_models=None,
        _warn_deprecated=False,
    )


@app.command()
def info(
    directory: Path = typer.Argument(
        None, help="Directory containing semantic layer files (defaults to project models)"
    ),
):
    """
    Show quick info about the semantic layer.

    Examples:
      sidemantic info
      sidemantic info ./models
    """
    try:
        directory = _models_path(directory)
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

    except typer.Exit:
        raise
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@app.command(hidden=True)
def mcp_serve(
    directory: Path = typer.Argument(
        None, help="Directory containing semantic layer files (defaults to project models)"
    ),
    connection: str = typer.Option(None, "--connection", help="Database connection string"),
    db: Path = typer.Option(None, "--db", help="Path to DuckDB database file (optional)"),
    init_sql: list[str] = typer.Option(
        [], "--init-sql", help="SQL statements to run after connecting (e.g., LOAD httpfs)"
    ),
    demo: bool = typer.Option(False, "--demo", help="Use demo data instead of a directory"),
    apps: bool = typer.Option(False, "--apps", help="Enable interactive UI widgets (requires mcp-ui-server)"),
    http: bool = typer.Option(False, "--http", help="Use HTTP transport instead of stdio"),
    port: int = typer.Option(4100, "--port", "-p", help="Port for HTTP server"),
):
    """
    Start an MCP server for the semantic layer.

    Provides tools for listing models, getting model details, and running queries
    through the Model Context Protocol.

    Examples:
      sidemantic mcp-serve
      sidemantic mcp-serve ./models --db data/warehouse.db
      sidemantic mcp-serve --demo
      sidemantic mcp-serve --apps --http --port 4100
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
    else:
        directory = _models_path(directory)
        db_path = str(db) if db else None

    # Resolve connection from CLI args or config
    connection_str = None
    effective_init_sql: list[str] | None = init_sql if init_sql else None

    if db_path == ":memory:":
        connection_str = "duckdb:///:memory:"
    elif not demo:
        resolved_connection = _resolve_connection(connection=connection, database=db, models=directory)
        connection_str = resolved_connection.connection if resolved_connection else None
        if resolved_connection and not effective_init_sql:
            effective_init_sql = resolved_connection.init_sql

    try:
        # Initialize the semantic layer
        initialize_layer(str(directory), connection=connection_str, init_sql=effective_init_sql)

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
                layer.adapter.execute(create_sql)

                # Insert data if there are rows
                if rows:
                    placeholders = ", ".join(["?" for _ in columns])
                    layer.adapter.executemany(f"INSERT INTO {table} VALUES ({placeholders})", rows)

        # Enable apps mode if requested
        if apps:
            import sidemantic.mcp_server as _mcp_mod

            _mcp_mod._apps_enabled = True
            typer.echo("Interactive UI widgets enabled", err=True)

        # Determine transport
        if http or apps:
            if apps and not http:
                typer.echo("Note: --apps implies HTTP transport, enabling automatically", err=True)
            mcp.settings.port = port
            transport = "streamable-http"
        else:
            transport = "stdio"

        typer.echo(f"Starting MCP server for: {directory}", err=True)
        if db_path and db_path != ":memory:":
            typer.echo(f"Using database: {db_path}", err=True)
        if transport == "streamable-http":
            typer.echo(f"Server running on HTTP at port {port}...", err=True)
        else:
            typer.echo("Server running on stdio...", err=True)

        # Run the MCP server
        mcp.run(transport=transport)

    except typer.Exit:
        raise
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


server_app.command("mcp")(mcp_serve)


@app.command()
def rewrite(
    sql: str = typer.Argument(..., help="Semantic SQL query to rewrite"),
    models: Path = typer.Option(None, "--models", "-m", help="Directory containing semantic layer files"),
    connection: str = typer.Option(None, "--connection", help="Database connection string (sets SQL dialect)"),
    db: Path = typer.Option(None, "--db", help="Path to DuckDB database file"),
    engine: str = typer.Option(None, "--engine", help="Runtime engine: python, rust, or auto"),
    fallback: bool | None = typer.Option(None, "--fallback/--no-fallback", help="Allow Rust engine fallback to Python"),
    use_preaggregations: bool = typer.Option(
        False, "--use-preaggregations", help="Enable automatic pre-aggregation routing"
    ),
):
    """
    Rewrite semantic SQL to ordinary SQL without executing it.

    Examples:
      sidemantic rewrite "SELECT orders.revenue FROM orders" --models ./models
      sidemantic rewrite "SELECT orders.revenue FROM orders" --models ./models --engine rust
    """
    try:
        layer = _load_query_layer(
            models,
            connection=connection,
            db=db,
            engine=engine,
            fallback=fallback,
            use_preaggregations=use_preaggregations,
        )

        from sidemantic.sql.query_rewriter import QueryRewriter

        typer.echo(
            QueryRewriter(
                layer.graph,
                dialect=layer.adapter.dialect,
                use_preaggregations=layer.use_preaggregations,
            ).rewrite(sql)
        )
    except typer.Exit:
        raise
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@app.command()
def convert(
    source: Path = typer.Argument(None, help="Semantic project, directory, or exact file to convert"),
    output: Path = typer.Option(None, "--output", "-o", help="Destination file or directory"),
    source_format: str = typer.Option("auto", "--from", help="Source format (default: auto-detect)"),
    target_format: str = typer.Option("sidemantic", "--to", help="Destination format"),
    force: bool = typer.Option(False, "--force", help="Allow writing to an existing destination"),
):
    """Convert semantic definitions through the shared format registry."""

    try:
        from sidemantic.formats import convert_semantic_source

        source = _models_path(source)
        output = output or (_project().root / f"converted.{target_format}.yml")
        if output.exists() and not force:
            raise ValueError(f"Destination already exists: {output}; pass --force to replace it")
        graph = convert_semantic_source(
            source,
            output,
            source_format=source_format,
            target_format=target_format,
        )
        typer.echo(f"Converted {len(graph.models)} model(s) to {target_format}: {output}", err=True)
    except typer.Exit:
        raise
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@app.command("export-native", hidden=True)
def export_native(
    source: Path = typer.Argument(None, help="File or directory containing semantic layer definitions"),
    output: Path = typer.Option(..., "--output", "-o", help="Native Sidemantic YAML file to write"),
    validate_rust: bool = typer.Option(
        False,
        "--validate-rust",
        help="Validate the exported native YAML with sidemantic-rs when installed",
    ),
):
    """
    Convert supported Python adapter inputs to canonical native Sidemantic YAML.

    Examples:
      sidemantic export-native ./lookml --output native.yml
      sidemantic export-native ./models --output native.yml --validate-rust
    """
    try:
        typer.echo("Warning: `export-native` is deprecated; use `convert --to sidemantic`", err=True)
        source = _models_path(source)
        from sidemantic.formats import convert_semantic_source

        graph = convert_semantic_source(source, output, target_format="sidemantic")
        if not graph.models:
            typer.echo("Error: No models found", err=True)
            raise typer.Exit(1)

        if validate_rust:
            from sidemantic.rust_bridge import load_graph_from_yaml_with_rust

            load_graph_from_yaml_with_rust(output.read_text())
            typer.echo(f"Exported native YAML to {output} and validated it with Rust", err=True)
        else:
            typer.echo(f"Exported native YAML to {output}", err=True)
    except typer.Exit:
        raise
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@app.command()
def query(
    sql: str = typer.Argument(..., help="SQL query to execute"),
    models: Path = typer.Option(None, "--models", "-m", help="Directory containing semantic layer files"),
    output: Path = typer.Option(None, "--output", "-o", help="Output file (default: stdout)"),
    connection: str = typer.Option(
        None, "--connection", help="Database connection string (e.g., postgres://host/db, bigquery://project/dataset)"
    ),
    db: Path = typer.Option(None, "--db", help="Path to DuckDB database file (shorthand for duckdb:/// connection)"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show generated SQL without executing"),
    engine: str = typer.Option(None, "--engine", help="Runtime engine: python, rust, or auto"),
    fallback: bool | None = typer.Option(None, "--fallback/--no-fallback", help="Allow Rust engine fallback to Python"),
    use_preaggregations: bool = typer.Option(
        False, "--use-preaggregations", help="Enable automatic pre-aggregation routing"
    ),
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
      sidemantic query "SELECT revenue FROM orders" --use-preaggregations --dry-run
    """
    try:
        layer = _load_query_layer(
            models,
            connection=connection,
            db=db,
            use_preaggregations=use_preaggregations,
            engine=engine,
            fallback=fallback,
        )

        # Dry run: show generated SQL without executing
        if dry_run:
            from sidemantic.sql.query_rewriter import QueryRewriter

            rewriter = QueryRewriter(
                layer.graph,
                dialect=layer.adapter.dialect,
                use_preaggregations=layer.use_preaggregations,
            )
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

    except typer.Exit:
        raise
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@dashboard_app.command("validate")
def dashboard_validate(
    spec: Path = typer.Argument(None, help="Dashboard YAML or JSON spec (defaults to the project dashboard)"),
    models: Path = typer.Option(None, "--models", "-m", help="Directory containing semantic layer files"),
    connection: str = typer.Option(
        None, "--connection", help="Database connection string (e.g., postgres://host/db, bigquery://project/dataset)"
    ),
    db: Path = typer.Option(None, "--db", help="Path to DuckDB database file (shorthand for duckdb:/// connection)"),
    use_preaggregations: bool = typer.Option(
        False, "--use-preaggregations", help="Enable automatic pre-aggregation routing while validating SQL"
    ),
):
    """Validate a semantic dashboard spec against loaded models."""
    try:
        from sidemantic.dashboard import DashboardDocument

        spec = _dashboard_path(spec)
        layer = _load_query_layer(
            models,
            connection=connection,
            db=db,
            use_preaggregations=use_preaggregations,
        )
        document = DashboardDocument.from_file(spec)
        errors = document.validate(
            layer,
            execute_sql=_resolve_connection(connection=connection, database=db, models=models) is not None,
        )
        if errors:
            for error in errors:
                typer.echo(f"Error: {error}", err=True)
            raise typer.Exit(1)
        chart_count = sum(len(tab.get("charts") or []) for tab in document.tabs)
        typer.echo(f"Dashboard spec is valid: {len(document.tabs)} tab(s), {chart_count} chart(s)")
    except typer.Exit:
        raise
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@dashboard_app.command("serve")
def dashboard_serve(
    spec: Path = typer.Argument(None, help="Dashboard YAML or JSON spec (defaults to the project dashboard)"),
    models: Path = typer.Option(None, "--models", "-m", help="Directory containing semantic layer files"),
    connection: str = typer.Option(
        None, "--connection", help="Database connection string (e.g., postgres://host/db, bigquery://project/dataset)"
    ),
    db: Path = typer.Option(None, "--db", help="Path to DuckDB database file (shorthand for duckdb:/// connection)"),
    host: str = typer.Option(None, "--host", "-H", help="Host for dashboard server"),
    port: int = typer.Option(None, "--port", "-p", help="Port for dashboard server"),
    use_preaggregations: bool = typer.Option(
        False, "--use-preaggregations", help="Enable automatic pre-aggregation routing"
    ),
    output_dir: Path = typer.Option(None, "--output-dir", hidden=True),
    warm_interaction_preaggregations: bool = typer.Option(False, "--warm-interaction-preaggregations", hidden=True),
):
    """Serve a dashboard spec in the official Sidemantic React UI."""
    try:
        from sidemantic.api_server import start_api_server, ui_static_dir
        from sidemantic.dashboard import DashboardDocument

        spec = _dashboard_path(spec)
        if output_dir is not None or warm_interaction_preaggregations:
            typer.echo(
                "Warning: the legacy crossfilter serve options are deprecated and ignored by the official UI",
                err=True,
            )
        layer = _load_query_layer(
            models,
            connection=connection,
            db=db,
            use_preaggregations=use_preaggregations,
        )
        document = DashboardDocument.from_file(spec)
        errors = document.validate(layer, execute_sql=True)
        if errors:
            for error in errors:
                typer.echo(f"Error: {error}", err=True)
            raise typer.Exit(1)

        serve_ui = ui_static_dir().joinpath("index.html").exists()
        if not serve_ui:
            typer.echo("Error: Official web UI is not built (run scripts/build_webapp.py)", err=True)
            raise typer.Exit(1)

        api_config = _loaded_config.api_server if _loaded_config else None
        resolved_host = host or (api_config.host if api_config else "127.0.0.1")
        resolved_port = port or (api_config.port if api_config else 4400)
        typer.echo(f"Dashboard: {document.title}", err=True)
        typer.echo(f"Web UI: http://{resolved_host}:{resolved_port}/", err=True)
        start_api_server(
            layer,
            host=resolved_host,
            port=resolved_port,
            serve_ui=True,
            dashboard=document,
            auth_token=api_config.auth_token if api_config else None,
            cors_origins=api_config.cors_origins if api_config else None,
            max_request_body_bytes=api_config.max_request_body_bytes if api_config else 1024 * 1024,
            result_cache_mb=api_config.result_cache_mb if api_config else 0,
            result_cache_ttl=api_config.result_cache_ttl if api_config else 60.0,
        )
    except typer.Exit:
        raise
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@dashboard_app.command("types")
def dashboard_types(
    models: Path = typer.Option(None, "--models", "-m", help="Directory containing semantic layer files"),
    output: Path = typer.Option(None, "--out", "--output", "-o", help="TypeScript output file; defaults to stdout"),
    schema_name: str = typer.Option(
        "sidemanticSchema", "--schema-name", help="Generated TypeScript schema export name"
    ),
):
    """Generate TypeScript dashboard config types from the semantic layer."""
    try:
        from sidemantic.dashboard import generate_dashboard_typescript

        layer = _load_graph_layer(models)
        rendered = generate_dashboard_typescript(layer, schema_name=schema_name)
        if output:
            output.write_text(rendered)
            typer.echo(f"Dashboard TypeScript definitions written to {output}", err=True)
        else:
            typer.echo(rendered)
    except typer.Exit:
        raise
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@gen_app.command("types", hidden=True)
def gen_types(
    models: Path = typer.Option(None, "--models", "-m", help="Directory or file with semantic layer definitions"),
    output: Path = typer.Option(None, "--out", "--output", "-o", help="TypeScript output file; defaults to stdout"),
    no_yaml: bool = typer.Option(False, "--no-yaml", help="Omit the embedded SCHEMA_YAML constant"),
):
    """Generate a typed query-client schema from the semantic layer.

    Emits an `as const` schema (field types per model) for use with `createClient`
    from `sidemantic-wasm/client`.
    """
    try:
        from sidemantic.codegen import generate_client_schema_ts

        layer = _load_graph_layer(models)
        rendered = generate_client_schema_ts(layer, include_yaml=not no_yaml)
        if output:
            output.write_text(rendered)
            typer.echo(f"Client schema written to {output}", err=True)
        else:
            typer.echo(rendered)
    except typer.Exit:
        raise
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


gen_app.command("client")(gen_types)


@gen_app.command("sql")
def gen_sql(
    sources: list[str] = typer.Argument(None, help="TypeScript files, directories, or globs to scan"),
    models: Path = typer.Option(None, "--models", "-m", help="Directory or file with semantic layer definitions"),
    output: Path = typer.Option(None, "--out", "--output", "-o", help="TypeScript output file; defaults to stdout"),
    call: str = typer.Option("query", "--call", help="Call name whose first string-literal argument is semantic SQL"),
):
    """Generate typed bindings for semantic SQL literals in TypeScript sources (sqlx-style).

    Emits a `GeneratedQueries` interface for use with `createSqlClient`. Each query is
    validated against the semantic layer (a bad reference fails the build). v1 limits:
    static string/template literals only (no `${}` interpolation); arbitrary SELECT
    expressions and min/max/derived metric value types are approximate.
    """
    try:
        from sidemantic.codegen import expand_sources, extract_sql_literals, generate_sql_types_ts

        if not sources:
            typer.echo("Error: provide at least one TypeScript source file, directory, or glob", err=True)
            raise typer.Exit(1)
        layer = _load_graph_layer(models)
        literals = extract_sql_literals(expand_sources(sources), call=call)
        if not literals:
            typer.echo(f"Error: no `{call}(...)` semantic SQL literals found in the given sources", err=True)
            raise typer.Exit(1)
        rendered = generate_sql_types_ts(layer, literals)
        if output:
            output.write_text(rendered)
            typer.echo(f"Typed query bindings ({len(literals)}) written to {output}", err=True)
        else:
            typer.echo(rendered)
    except typer.Exit:
        raise
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@app.command("explain-sql", hidden=True)
def explain_sql_command(
    sql: str = typer.Argument(..., help="SQL query to explain"),
    models: Path = typer.Option(None, "--models", "-m", help="Directory containing semantic layer files"),
    connection: str = typer.Option(
        None, "--connection", help="Database connection string (e.g., postgres://host/db, bigquery://project/dataset)"
    ),
    db: Path = typer.Option(None, "--db", help="Path to DuckDB database file (shorthand for duckdb:/// connection)"),
    use_preaggregations: bool = typer.Option(
        False, "--use-preaggregations", help="Enable automatic pre-aggregation routing"
    ),
    engine: str = typer.Option(None, "--engine", help="Runtime engine: python, rust, or auto"),
    fallback: bool | None = typer.Option(None, "--fallback/--no-fallback", help="Allow Rust engine fallback to Python"),
    strict: bool = typer.Option(True, "--strict/--no-strict", help="Fail on unsupported semantic SQL"),
):
    """
    Explain semantic SQL rewrite planning as JSON without executing the query.

    Examples:
      sidemantic explain-sql "SELECT revenue FROM orders"
      sidemantic explain-sql "SELECT * FROM (SELECT revenue, status FROM orders) sq WHERE status = 'completed'"
      sidemantic explain-sql "SELECT revenue, status FROM orders" --use-preaggregations
    """
    try:
        import json

        layer = _load_query_layer(
            models,
            connection=connection,
            db=db,
            use_preaggregations=use_preaggregations,
            engine=engine,
            fallback=fallback,
        )
        explanation = layer.explain_sql(sql, strict=strict)
        typer.echo(json.dumps(explanation.to_dict(), indent=2, default=str))

    except typer.Exit:
        raise
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


app.command("explain")(explain_sql_command)


@app.command(hidden=True)
def serve(
    directory: Path = typer.Argument(
        None, help="Directory containing semantic layer files (defaults to project models)"
    ),
    demo: bool = typer.Option(False, "--demo", help="Use demo data"),
    connection: str = typer.Option(
        None, "--connection", help="Database connection string (e.g., postgres://host/db, bigquery://project/dataset)"
    ),
    db: Path = typer.Option(None, "--db", help="Path to DuckDB database file (shorthand for duckdb:/// connection)"),
    host: str = typer.Option(None, "--host", "-H", help="Host/IP to bind to (overrides config, default 127.0.0.1)"),
    port: int = typer.Option(None, "--port", "-p", help="Port to listen on (overrides config)"),
    username: str = typer.Option(None, "--username", "-u", help="Username for authentication (overrides config)"),
    password: str = typer.Option(None, "--password", help="Password for authentication (overrides config)"),
    user_attrs_file: Path = typer.Option(
        None,
        "--user-attrs-file",
        help="Path to a JSON file mapping usernames -> user-attribute dicts for row/access security",
    ),
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

    try:
        from sidemantic.server.server import start_server
    except ImportError as exc:
        typer.echo(
            "Error: `sidemantic serve` requires the optional serve dependencies. "
            "Install with `pip install 'sidemantic[serve]'` or run with "
            "`uvx --from 'sidemantic[serve]' sidemantic serve ...`.",
            err=True,
        )
        raise typer.Exit(1) from exc

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
    else:
        directory = _models_path(directory)

    # Build connection string from args or config
    resolved_connection = _resolve_connection(connection=connection, database=db, models=directory)
    connection_str = resolved_connection.connection if resolved_connection else None

    # Resolve host, port, username, password from args or config
    host_resolved = host or (_loaded_config.pg_server.host if _loaded_config else "127.0.0.1")
    port_resolved = port if port is not None else (_loaded_config.pg_server.port if _loaded_config else 5433)
    username_resolved = username or (_loaded_config.pg_server.username if _loaded_config else None)
    password_resolved = password or (_loaded_config.pg_server.password if _loaded_config else None)

    if (username_resolved is None) != (password_resolved is None):
        typer.echo("Error: Must provide both --username and --password for PG server auth", err=True)
        raise typer.Exit(1)

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

    # Load the optional username -> user-attributes map for security enforcement.
    user_attrs_map = None
    if user_attrs_file is not None:
        import json

        if not user_attrs_file.exists():
            typer.echo(f"Error: user-attrs file {user_attrs_file} does not exist", err=True)
            raise typer.Exit(1)
        try:
            loaded = json.loads(user_attrs_file.read_text())
        except json.JSONDecodeError as exc:
            typer.echo(f"Error: failed to parse user-attrs file {user_attrs_file}: {exc}", err=True)
            raise typer.Exit(1)
        if not isinstance(loaded, dict) or not all(isinstance(v, dict) for v in loaded.values()):
            typer.echo(f"Error: user-attrs file {user_attrs_file} must map usernames to attribute objects", err=True)
            raise typer.Exit(1)
        user_attrs_map = loaded

    # Start the server
    start_server(
        layer,
        host=host_resolved,
        port=port_resolved,
        username=username_resolved,
        password=password_resolved,
        user_attrs_map=user_attrs_map,
    )


server_app.command("postgres")(serve)


@app.command(hidden=True)
def api_serve(
    directory: Path = typer.Argument(
        None, help="Directory containing semantic layer files (defaults to project models)"
    ),
    demo: bool = typer.Option(False, "--demo", help="Use demo data"),
    connection: str = typer.Option(
        None, "--connection", help="Database connection string (e.g., postgres://host/db, bigquery://project/dataset)"
    ),
    db: Path = typer.Option(None, "--db", help="Path to DuckDB database file (shorthand for duckdb:/// connection)"),
    host: str = typer.Option(None, "--host", "-H", help="Host/IP to bind to (overrides config, default 127.0.0.1)"),
    port: int = typer.Option(None, "--port", "-p", help="Port to listen on (overrides config)"),
    auth_token: str = typer.Option(None, "--auth-token", help="Bearer token required for API requests"),
    cors_origin: list[str] | None = typer.Option(None, "--cors-origin", help="Allowed CORS origin (repeatable)"),
    max_request_body_bytes: int = typer.Option(
        None, "--max-request-body-bytes", help="Maximum request body size in bytes"
    ),
    result_cache_mb: int = typer.Option(
        None, "--result-cache-mb", help="Result cache size in MB (0 disables; default 0)"
    ),
    result_cache_ttl: float = typer.Option(
        None, "--result-cache-ttl", help="Result cache entry TTL in seconds (default 60)"
    ),
    require_user_attrs: bool = typer.Option(
        False,
        "--require-user-attrs",
        help="Require the user-attributes header on data endpoints (reject with 400 if missing)",
    ),
    enforce_visibility: bool = typer.Option(
        False,
        "--enforce-visibility",
        help="Reject requests for non-public dimensions/metrics",
    ),
    user_header: str = typer.Option(
        "X-Sidemantic-User",
        "--user-header",
        help="Trusted request header carrying JSON user attributes",
    ),
    ui: bool = typer.Option(True, "--ui/--no-ui", help="Serve the embedded web UI at the root path"),
):
    """
    Start an HTTP API server for the semantic layer.

    Exposes semantic queries over JSON or Arrow IPC for remote clients.

    Examples:
      sidemantic api-serve
      sidemantic api-serve ./models --db data/warehouse.db
      sidemantic api-serve --connection "postgres://localhost:5432/analytics"
      sidemantic api-serve --auth-token secret --cors-origin https://app.example.com
      sidemantic api-serve --demo
    """
    try:
        from sidemantic.api_server import start_api_server, ui_static_dir
    except ImportError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)

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
    else:
        directory = _models_path(directory)

    resolved_connection = _resolve_connection(connection=connection, database=db, models=directory)
    connection_str = resolved_connection.connection if resolved_connection else None
    init_sql = resolved_connection.init_sql if resolved_connection else None

    host_resolved = host or (_loaded_config.api_server.host if _loaded_config else "127.0.0.1")
    port_resolved = port if port is not None else (_loaded_config.api_server.port if _loaded_config else 4400)
    auth_token_resolved = auth_token or (_loaded_config.api_server.auth_token if _loaded_config else None)
    cors_origins_resolved = (
        list(cors_origin)
        if cors_origin is not None
        else (_loaded_config.api_server.cors_origins if _loaded_config else [])
    )
    max_body_bytes_resolved = (
        max_request_body_bytes
        if max_request_body_bytes is not None
        else (_loaded_config.api_server.max_request_body_bytes if _loaded_config else 1024 * 1024)
    )
    result_cache_mb_resolved = (
        result_cache_mb
        if result_cache_mb is not None
        else (_loaded_config.api_server.result_cache_mb if _loaded_config else 0)
    )
    result_cache_ttl_resolved = (
        result_cache_ttl
        if result_cache_ttl is not None
        else (_loaded_config.api_server.result_cache_ttl if _loaded_config else 60.0)
    )

    preagg_db = _loaded_config.preagg_database if _loaded_config else None
    preagg_sch = _loaded_config.preagg_schema if _loaded_config else None
    if connection_str:
        layer = SemanticLayer(
            connection=connection_str, preagg_database=preagg_db, preagg_schema=preagg_sch, init_sql=init_sql
        )
    else:
        layer = SemanticLayer(preagg_database=preagg_db, preagg_schema=preagg_sch)

    load_from_directory(layer, str(directory))

    if not layer.graph.models:
        typer.echo("Error: No models found", err=True)
        raise typer.Exit(1)

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
            layer.adapter.execute(create_sql)

            if rows:
                placeholders = ", ".join(["?" for _ in columns])
                layer.adapter.executemany(f"INSERT INTO {table} VALUES ({placeholders})", rows)

    serve_ui = ui and ui_static_dir().joinpath("index.html").exists()

    typer.echo(f"Starting HTTP API server for: {directory}", err=True)
    typer.echo(f"Listening on http://{host_resolved}:{port_resolved}", err=True)
    if serve_ui:
        typer.echo(f"Web UI: http://{host_resolved}:{port_resolved}/", err=True)
    elif ui:
        typer.echo("Web UI: not built (run scripts/build_webapp.py)", err=True)
    if auth_token_resolved:
        typer.echo("Authentication: bearer token required", err=True)
    else:
        typer.echo("Authentication: disabled", err=True)

    start_api_server(
        layer,
        host=host_resolved,
        port=port_resolved,
        auth_token=auth_token_resolved,
        cors_origins=cors_origins_resolved,
        max_request_body_bytes=max_body_bytes_resolved,
        serve_ui=serve_ui,
        result_cache_mb=result_cache_mb_resolved,
        result_cache_ttl=result_cache_ttl_resolved,
        require_user_attrs=require_user_attrs,
        enforce_visibility=enforce_visibility,
        user_header=user_header,
    )


server_app.command("api")(api_serve)


@app.command(hidden=True)
def tree(
    directory: Path = typer.Argument(..., help="Directory containing semantic layer files"),
):
    """
    Alias for 'workbench' command (deprecated).
    """
    from sidemantic.workbench import WorkbenchDependencyError, run_workbench

    if not directory.exists():
        typer.echo(f"Error: Directory {directory} does not exist", err=True)
        raise typer.Exit(1)

    try:
        run_workbench(directory)
    except WorkbenchDependencyError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@app.command()
def validate(
    directory: Path = typer.Argument(
        None, help="Directory containing semantic layer files (defaults to project models)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed validation results"),
    engine: str = typer.Option(None, "--engine", help="Runtime engine: python, rust, or auto"),
    fallback: bool | None = typer.Option(None, "--fallback/--no-fallback", help="Allow Rust engine fallback to Python"),
):
    """
    Validate semantic layer definitions.

    Shows errors, warnings, and optionally detailed info.

    Examples:
      sidemantic validate
      sidemantic validate ./models --verbose
    """
    directory = _models_path(directory)

    engine, fallback = _resolve_engine_options(engine, fallback)
    _configure_engine_environment(engine, fallback)

    if engine in {"rust", "auto"}:
        try:
            from sidemantic.rust_bridge import load_graph_from_directory_with_rust

            graph = load_graph_from_directory_with_rust(directory)
            typer.echo(f"Validated {len(graph.models)} models with Rust")
            if verbose:
                for model_name in sorted(graph.models):
                    typer.echo(f"  - {model_name}")
        except Exception as e:
            if engine == "rust" or not fallback:
                typer.echo(f"Error: Rust validation failed: {e}", err=True)
                raise typer.Exit(1)

    # Canonical semantic checks are always Python-backed; Rust validation above
    # is an additional compatibility check, not a different definition of valid.
    _configure_engine_environment("python", False)
    try:
        from sidemantic.validation_runner import validate_directory

        report = validate_directory(directory)
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)

    typer.echo(f"Validation Results: {directory}")

    if report.errors:
        typer.echo("Errors:")
        for error in report.errors:
            typer.echo(f"  - {error}")

    if report.warnings:
        typer.echo("Warnings:")
        for warning in report.warnings:
            typer.echo(f"  - {warning}")

    if verbose or not (report.errors or report.warnings):
        typer.echo("Info:")
        for item in report.info:
            typer.echo(f"  - {item}")

    if report.errors:
        typer.echo("Validation Failed", err=True)
        raise typer.Exit(1)

    typer.echo("Validation Passed")


@app.command()
def workbench(
    directory: Path = typer.Argument(
        None, help="Directory containing semantic layer files (defaults to project models)"
    ),
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
      uvx --from 'sidemantic[workbench]' sidemantic workbench --demo
    """
    from sidemantic.workbench import WorkbenchDependencyError, run_workbench

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
        try:
            run_workbench(directory, demo_mode=True, connection=None)
        except WorkbenchDependencyError as e:
            typer.echo(f"Error: {e}", err=True)
            raise typer.Exit(1)
    else:
        directory = _models_path(directory)
        resolved_connection = _resolve_connection(connection=connection, database=db, models=directory)
        connection_str = resolved_connection.connection if resolved_connection else None

        # Only pass connection if it's not None
        try:
            if connection_str:
                run_workbench(directory, connection=connection_str)
            else:
                run_workbench(directory)
        except WorkbenchDependencyError as e:
            typer.echo(f"Error: {e}", err=True)
            raise typer.Exit(1)


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

    try:
        recommender = PreAggregationRecommender(min_query_count=min_count, min_benefit_score=min_score)
        resolved_connection = None
        if not queries:
            resolved_connection = _resolve_connection(
                connection=connection,
                database=db,
                required=True,
            )

        # Fetch/parse queries (same as analyze command)
        if resolved_connection:
            connection_str = resolved_connection.connection
            from sidemantic import SemanticLayer

            preagg_db = _loaded_config.preagg_database if _loaded_config else None
            preagg_sch = _loaded_config.preagg_schema if _loaded_config else None
            layer = SemanticLayer(connection=connection_str, preagg_database=preagg_db, preagg_schema=preagg_sch)
            adapter = layer.adapter
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

    except typer.Exit:
        raise
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@preagg_app.command("apply")
def preagg_apply(
    directory: Path = typer.Argument(None, help="Directory containing semantic layer YAML files"),
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
    from sidemantic.core.preagg_management import apply_recommendations_to_yaml
    from sidemantic.core.preagg_recommender import PreAggregationRecommender

    try:
        directory = _models_path(directory)
        recommender = PreAggregationRecommender(min_query_count=min_count, min_benefit_score=min_score)
        resolved_connection = None
        if not queries:
            resolved_connection = _resolve_connection(
                connection=connection,
                database=db,
                models=directory,
                required=True,
            )

        # Fetch/parse queries
        if resolved_connection:
            connection_str = resolved_connection.connection
            from sidemantic import SemanticLayer

            preagg_db = _loaded_config.preagg_database if _loaded_config else None
            preagg_sch = _loaded_config.preagg_schema if _loaded_config else None
            layer = SemanticLayer(connection=connection_str, preagg_database=preagg_db, preagg_schema=preagg_sch)
            adapter = layer.adapter
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

        result = apply_recommendations_to_yaml(directory, recommendations, recommender, dry_run=dry_run)

        if dry_run:
            typer.echo(f"\nDry run: Would add {result.added} pre-aggregations", err=True)
            typer.echo("Remove --dry-run to apply changes", err=True)
        else:
            typer.echo(f"\n✓ Added {result.added} pre-aggregations to model files", err=True)
        if result.skipped:
            typer.echo(f"Skipped {result.skipped} already-present definitions", err=True)

    except typer.Exit:
        raise
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@preagg_app.command("refresh")
def refresh(
    directory: Path = typer.Argument(
        None, help="Directory containing semantic layer files (defaults to project models)"
    ),
    model: str = typer.Option(None, "--model", help="Only refresh pre-aggregations for this model"),
    preagg: str = typer.Option(None, "--preagg", "-p", help="Only refresh this specific pre-aggregation"),
    mode: str = typer.Option("auto", "--mode", help="Refresh mode: auto, full, incremental, merge, or engine"),
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
    try:
        from sidemantic.core.preagg_management import resolve_preaggregation_targets, resolve_refresh_mode

        directory = _models_path(directory)
        # Load semantic layer
        preagg_db = _loaded_config.preagg_database if _loaded_config else None
        preagg_sch = _loaded_config.preagg_schema if _loaded_config else None
        layer = SemanticLayer(preagg_database=preagg_db, preagg_schema=preagg_sch)
        load_from_directory(layer, str(directory))

        if not layer.graph.models:
            typer.echo("Error: No models found", err=True)
            raise typer.Exit(1)

        resolved_connection = _resolve_connection(
            connection=connection,
            database=db,
            models=directory,
            required=True,
        )
        assert resolved_connection is not None
        connection_str = resolved_connection.connection

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
            conn = temp_layer.adapter.raw_connection
        else:
            typer.echo(f"Error: Unsupported connection type: {connection_str}", err=True)
            typer.echo(
                "Currently only DuckDB is supported for manual refresh modes (full, incremental, merge)", err=True
            )
            typer.echo("Use --mode engine for Snowflake, ClickHouse, BigQuery materialized views", err=True)
            raise typer.Exit(1)

        preaggs_to_refresh = resolve_preaggregation_targets(
            layer.graph.models,
            model_name=model,
            preagg_name=preagg,
        )

        if not preaggs_to_refresh:
            typer.echo("No pre-aggregations found to refresh", err=True)
            raise typer.Exit(1)

        typer.echo(f"\nRefreshing {len(preaggs_to_refresh)} pre-aggregation(s)...\n", err=True)

        # Determine the dialect: drives index DDL (DuckDB/Postgres) and engine MVs.
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
        elif connection_str.startswith("duckdb://"):
            dialect = "duckdb"

        # Refresh each pre-aggregation
        for model_name, model_obj, preagg_obj in preaggs_to_refresh:
            resolved_mode = resolve_refresh_mode(preagg_obj, mode)
            # Get database/schema from config if available
            database = _loaded_config.preagg_database if _loaded_config else None
            schema = _loaded_config.preagg_schema if _loaded_config else None
            table_name = preagg_obj.get_table_name(model_name, database=database, schema=schema)

            # Generate materialization SQL
            source_sql = preagg_obj.generate_materialization_sql(model_obj)

            # Determine watermark column
            watermark_column = None
            if resolved_mode in ["incremental", "merge"] and preagg_obj.time_dimension and preagg_obj.granularity:
                watermark_column = f"{preagg_obj.time_dimension}_{preagg_obj.granularity}"

            # Refresh
            typer.echo(f"Refreshing {model_name}.{preagg_obj.name} ({resolved_mode})...", err=True)
            result = preagg_obj.refresh(
                connection=conn,
                source_sql=source_sql,
                table_name=table_name,
                mode=resolved_mode,
                watermark_column=watermark_column,
                dialect=dialect,
                model=model_obj,
                database=database,
                schema=schema,
            )

            # Print result
            if result.rows_inserted >= 0:
                typer.echo(f"  ✓ {table_name}: {result.rows_inserted} rows in {result.duration_seconds:.2f}s", err=True)
            else:
                typer.echo(f"  ✓ {table_name}: completed in {result.duration_seconds:.2f}s", err=True)

        typer.echo("\nDone!", err=True)

    except typer.Exit:
        raise
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
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
