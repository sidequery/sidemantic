"""CLI for sidemantic semantic layer operations."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import click
import typer

from sidemantic import __version__
from sidemantic.cli_contract import (
    HELP_REQUESTED_META_KEY,
    OUTPUT_FORMATS,
    ContractGroup,
    InvocationError,
    cli_state,
    color_enabled,
    configure_click_color,
    credential_file_from_env,
    emit_diagnostic,
    emit_error,
    emit_guidance,
    emit_json,
    emit_pending_deprecation,
    emit_records,
    emit_result,
    emit_warning,
    fail,
    page_json_output,
    progress,
    read_sql_input,
    read_text_input,
    render_rows,
    resolve_output_format,
    resolve_secret,
    write_text_output,
)
from sidemantic.cli_polish import (
    closest_command,
    complete_dashboard_spec,
    complete_model,
    complete_path,
    complete_source_format,
    complete_target_format,
    emit_deprecation,
    emit_long_output,
    emit_next_step,
)
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


CLI_CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"]}
DOCS_URL = "https://sidemantic.com/sidemantic/cli"
SUPPORT_URL = "https://github.com/sidequery/sidemantic/issues"
ROOT_EPILOG = f"""Examples:
  sidemantic info --format table
  sidemantic query "SELECT orders.revenue FROM orders" --format json
  sidemantic --project ./analytics validate

Documentation: {DOCS_URL}
Support: {SUPPORT_URL}"""
GROUP_EPILOG = f"Documentation: {DOCS_URL}\nSupport: {SUPPORT_URL}"
FORMATTED_COMMANDS = frozenset(
    {"dashboard", "explain", "explain-sql", "info", "migrate", "migrator", "preagg", "query", "validate"}
)


app = typer.Typer(
    cls=ContractGroup,
    context_settings=CLI_CONTEXT_SETTINGS,
    help="Sidemantic: SQL-first semantic layer",
    epilog=ROOT_EPILOG,
    no_args_is_help=True,
)
dashboard_app = typer.Typer(
    cls=ContractGroup,
    context_settings=CLI_CONTEXT_SETTINGS,
    help="Serve, validate, and type semantic dashboard specs",
    epilog=f"Examples: sidemantic dashboard validate dashboard.yml\n\n{GROUP_EPILOG}",
    no_args_is_help=True,
)
app.add_typer(dashboard_app, name="dashboard")
gen_app = typer.Typer(
    cls=ContractGroup,
    context_settings=CLI_CONTEXT_SETTINGS,
    help="Generate typed TypeScript clients from the semantic layer",
    epilog=f"Examples: sidemantic generate client --output client.ts\n\n{GROUP_EPILOG}",
    no_args_is_help=True,
)
app.add_typer(gen_app, name="gen", hidden=True)
app.add_typer(gen_app, name="generate")
migrate_app = typer.Typer(
    cls=ContractGroup,
    context_settings=CLI_CONTEXT_SETTINGS,
    help="Generate semantic models from SQL or check migration coverage",
    epilog=f"Examples: sidemantic migrate check queries/ --format json\n\n{GROUP_EPILOG}",
    no_args_is_help=True,
)
app.add_typer(migrate_app, name="migrate")
server_app = typer.Typer(
    cls=ContractGroup,
    context_settings=CLI_CONTEXT_SETTINGS,
    help="Run Sidemantic API, PostgreSQL, or MCP servers",
    epilog=f"Examples: sidemantic server api --port 4400\n\n{GROUP_EPILOG}",
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
        runtime = _loaded_config.runtime
        resolved_engine = runtime.engine
        if resolved_fallback is None:
            resolved_fallback = runtime.fallback if "fallback" in runtime.model_fields_set else None

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
    discover_connection: bool = True,
) -> SemanticLayer:
    """Load a semantic layer for CLI query/explain commands."""
    engine, resolved_fallback = _resolve_engine_options(engine, fallback)
    _configure_engine_environment(engine, resolved_fallback)

    models = _models_path(models)
    resolved_connection = _resolve_connection(
        connection=connection,
        database=db,
        models=models,
        discover=discover_connection,
    )
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
    ctx: typer.Context,
    version: bool = typer.Option(None, "--version", callback=version_callback, is_eager=True, help="Show version"),
    project: Path = typer.Option(
        None,
        "--project",
        "-p",
        envvar="SIDEMANTIC_PROJECT",
        help="Project root (defaults to discovery from cwd)",
    ),
    config: Path = typer.Option(
        None,
        "--config",
        "-c",
        envvar="SIDEMANTIC_CONFIG",
        help="Path to config file (sidemantic.yaml)",
    ),
    output_format: str = typer.Option(
        None,
        "--format",
        envvar="SIDEMANTIC_FORMAT",
        metavar="table|csv|json|jsonl",
        help="Output format for structured results",
    ),
    plain: bool | None = typer.Option(
        None, "--plain", envvar="SIDEMANTIC_PLAIN", help="Stable undecorated tabular output"
    ),
    quiet: bool | None = typer.Option(
        None, "--quiet", "-q", envvar="SIDEMANTIC_QUIET", help="Suppress non-essential status"
    ),
    verbose: bool | None = typer.Option(
        None, "--verbose", "-v", envvar="SIDEMANTIC_VERBOSE", help="Show detailed diagnostics"
    ),
    debug: bool | None = typer.Option(
        None, "--debug", "-d", envvar="SIDEMANTIC_DEBUG", help="Show tracebacks for unexpected errors"
    ),
    no_color: bool | None = typer.Option(
        None, "--no-color", envvar="SIDEMANTIC_NO_COLOR", help="Disable terminal color"
    ),
    pager: bool | None = typer.Option(
        None, "--pager/--no-pager", help="Page long human-readable output when writing to a terminal"
    ),
):
    """Sidemantic CLI.

    You can use a config file (sidemantic.yaml or sidemantic.json) to set default values.
    CLI arguments override config file values.
    """
    global _loaded_config, _project_context

    if output_format is not None:
        output_format = output_format.lower()
        if output_format not in OUTPUT_FORMATS:
            raise InvocationError(f"--format must be one of: {', '.join(OUTPUT_FORMATS)}")

    format_explicit = ctx.get_parameter_source("output_format") is click.core.ParameterSource.COMMANDLINE
    plain_explicit = ctx.get_parameter_source("plain") is click.core.ParameterSource.COMMANDLINE

    cli_state().reset(
        debug=bool(debug),
        quiet=bool(quiet),
        verbose=bool(verbose),
        requested_format=output_format,
        format_explicit=format_explicit,
        plain=bool(plain),
        plain_explicit=plain_explicit,
        color=color_enabled(no_color=bool(no_color)),
        pager=pager,
    )

    # Help must remain available even when project configuration is malformed,
    # since it is a recovery path. Click has already resolved the subcommand by
    # the time the root callback runs, so skip project discovery for this path.
    if ctx.meta.get(HELP_REQUESTED_META_KEY) or ctx.invoked_subcommand == "help":
        return
    if format_explicit and ctx.invoked_subcommand not in FORMATTED_COMMANDS:
        raise InvocationError(f"{ctx.invoked_subcommand} does not support --format")

    try:
        _project_context = ProjectContext.discover(start_dir=project, config_path=config)
        _loaded_config = _project_context.config
    except ProjectResolutionError as exc:
        raise typer.BadParameter(str(exc), param_hint="--config") from exc

    configured = _loaded_config.cli if _loaded_config else None
    configured_format = configured.format if configured else None
    configured_plain = bool(configured and configured.plain)
    selected_format = output_format if output_format is not None else configured_format
    selected_plain = bool(plain) if plain is not None else configured_plain

    def source_priority(name: str, configured_value: object) -> int:
        source = ctx.get_parameter_source(name)
        if source is click.core.ParameterSource.COMMANDLINE:
            return 3
        if source is click.core.ParameterSource.ENVIRONMENT:
            return 2
        return 1 if configured_value else 0

    format_priority = source_priority("output_format", configured_format)
    plain_priority = source_priority("plain", configured_plain)
    if selected_plain and selected_format is not None:
        if format_priority > plain_priority:
            selected_plain = False
        elif selected_format != "table" and plain_priority > format_priority:
            selected_format = None

    selected_no_color = bool(no_color) if no_color is not None else bool(configured and configured.no_color)
    selected_color = color_enabled(no_color=selected_no_color)
    cli_state().reset(
        debug=bool(debug),
        quiet=bool(quiet) if quiet is not None else bool(configured and configured.quiet),
        verbose=bool(verbose) if verbose is not None else bool(configured and configured.verbose),
        requested_format=selected_format,
        format_explicit=format_explicit,
        plain=selected_plain,
        plain_explicit=plain_explicit,
        color=selected_color,
        pager=pager,
    )
    configure_click_color(ctx, enabled=selected_color)


@dashboard_app.callback()
def dashboard_main(ctx: typer.Context) -> None:
    """Apply dashboard-group presentation contracts."""

    if ctx.find_root().meta.get(HELP_REQUESTED_META_KEY):
        return
    if cli_state().format_explicit and ctx.invoked_subcommand != "validate":
        raise InvocationError(f"dashboard {ctx.invoked_subcommand} does not support --format")


def _emit_long_report(text: str) -> None:
    """Emit a potentially long report under the shared terminal contract."""

    state = cli_state()
    emit_long_output(
        text,
        stream=sys.stdout,
        human_output=state.human_extras,
        pager_enabled=state.pager,
        color=bool(state.color),
    )


@app.command("help", context_settings=CLI_CONTEXT_SETTINGS)
def help_command(
    command: list[str] = typer.Argument(None, metavar="[COMMAND]...", help="Command or nested subcommand path"),
):
    """Show top-level help or help for a command path."""

    from typer.main import get_command

    current = get_command(app)
    context = click.Context(current, info_name="sidemantic")
    for index, part in enumerate(command or []):
        if not isinstance(current, click.Group):
            raise InvocationError(f"{' '.join(command)} does not name a command")
        child = current.get_command(context, part)
        if child is None:
            requested_path = " ".join(command)
            suggestion = closest_command(part, current.list_commands(context))
            if suggestion is None:
                raise InvocationError(f"No such command path: {requested_path}")
            corrected = [*(command or [])]
            corrected[index] = suggestion
            corrected_path = " ".join(corrected)
            raise InvocationError(
                f"No such command path: {requested_path}. Did you mean '{corrected_path}'? "
                f"Run 'sidemantic help {corrected_path}' to review it."
            )
        current = child
        context = click.Context(current, info_name=part, parent=context)
    emit_result(current.get_help(context))


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
    json_output: bool = typer.Option(False, "--json", help="Emit the migration report as JSON"),
):
    """
    Migrate SQL queries to semantic layer by generating model definitions.

    Analyzes existing SQL queries to generate model definitions and rewrite
    queries to use semantic layer syntax.

    Examples:
      sidemantic migrate generate queries/ --output output/
      sidemantic migrate generate --history --connection "snowflake://..." --output output/
      sidemantic migrate check queries/ --models models/ --verbose
    """
    from sidemantic.core.migrator import Migrator

    output_format = resolve_output_format(json_output=json_output)
    structured = json_output or cli_state().requested_format is not None or cli_state().plain
    verbose = verbose or cli_state().verbose

    source_dialect = "duckdb"

    if not queries and not connection:
        raise InvocationError("Must specify --queries or --connection")

    if queries and connection:
        raise InvocationError("--queries and --connection are mutually exclusive")

    if queries and str(queries) != "-" and not queries.exists():
        raise InvocationError(f"Query source does not exist: {queries}")

    def load_queries_from_source() -> list[str] | None:
        nonlocal source_dialect
        if connection:
            history_layer = SemanticLayer(connection=connection, auto_register=False)
            adapter = history_layer.adapter
            source_dialect = adapter.dialect
            get_query_history = getattr(adapter, "get_query_history", None)
            if get_query_history is None:
                raise ValueError(f"{adapter.dialect} does not support query-history import")
            emit_diagnostic(
                f"Importing up to {limit} queries from the last {days_back} day(s) of {adapter.dialect} history...",
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
            emit_diagnostic(f"Imported {len(queries_from_history)} queries from warehouse history")
            if not queries_from_history:
                raise ValueError("No queries found in warehouse history for the requested time range")
            return queries_from_history

        if queries and (str(queries) == "-" or queries.is_file()):
            content = read_text_input(queries, label="migration SQL")
            return [query.strip() for query in content.split(";") if query.strip()]
        return None

    # Bootstrap mode - generate models from queries
    if generate_models:
        try:
            import contextlib
            import io

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

            capture = contextlib.redirect_stdout(io.StringIO()) if structured else contextlib.nullcontext()
            with capture, progress("Generating semantic models and rewritten queries"):
                # Generate model definitions
                emit_diagnostic("Generating model definitions...")
                models = analyzer.generate_models(report)

                models_dir = generate_models / "models"
                analyzer.write_model_files(models, str(models_dir))
                graph_metrics = analyzer.generate_graph_metrics(report, models)
                analyzer.write_graph_metrics_file(graph_metrics, str(models_dir))

                # Generate rewritten queries
                emit_diagnostic("Generating rewritten queries...")
                rewritten = analyzer.generate_rewritten_queries(report)

                queries_dir = generate_models / "rewritten_queries"
                analyzer.write_rewritten_queries(rewritten, str(queries_dir))

            emit_diagnostic(
                f"Generated {len(models)} models, {len(graph_metrics)} graph metrics, "
                f"and {len(rewritten)} rewritten queries in {generate_models}"
            )
            if structured:
                generated = {
                    "models": len(models),
                    "graph_metrics": len(graph_metrics),
                    "rewritten_queries": len(rewritten),
                    "output": str(generate_models),
                }
                payload = {
                    "mode": "generate",
                    "report": _migration_report_json(report),
                    "generated": generated,
                }
                emit_records(
                    [{"mode": "generate", **generated}],
                    output_format=output_format,
                    json_value=payload,
                )

        except typer.Exit:
            raise
        except Exception as e:
            fail(e)

    # Coverage analysis mode - compare queries against existing models
    else:
        try:
            directory = _models_path(directory)
            # Load semantic layer
            layer = SemanticLayer()
            load_from_directory(layer, str(directory))

            if not layer.graph.models:
                fail("No models found in semantic layer")

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
            if structured:
                report_payload = _migration_report_json(report, verbose=verbose)
                emit_records(
                    [{"mode": "check", **{key: value for key, value in report_payload.items() if key != "queries"}}],
                    output_format=output_format,
                    json_value={"mode": "check", "report": report_payload},
                )
            else:
                analyzer.print_report(report, verbose=verbose)

        except typer.Exit:
            raise
        except Exception as e:
            fail(e)


def _migration_report_json(report, *, verbose: bool = True) -> dict[str, object]:
    """Return the stable JSON representation of a migration report."""

    payload: dict[str, object] = {
        "total_queries": report.total_queries,
        "parseable_queries": report.parseable_queries,
        "rewritable_queries": report.rewritable_queries,
        "coverage_percentage": report.coverage_percentage,
        "missing_models": sorted(report.missing_models),
        "missing_dimensions": {
            model: sorted(dimensions) for model, dimensions in sorted(report.missing_dimensions.items())
        },
        "missing_metrics": {
            model: [{"aggregation": aggregation, "column": column} for aggregation, column in sorted(metrics)]
            for model, metrics in sorted(report.missing_metrics.items())
        },
    }
    if verbose:
        payload["queries"] = [
            {
                "query": analysis.query,
                "parse_error": analysis.parse_error,
                "can_rewrite": analysis.can_rewrite,
                "tables": sorted(analysis.tables),
                "missing_models": sorted(analysis.missing_models),
                "missing_dimensions": [
                    {"model": model, "dimension": dimension} for model, dimension in sorted(analysis.missing_dimensions)
                ],
                "missing_metrics": [
                    {"model": model, "aggregation": aggregation, "column": column}
                    for model, aggregation, column in sorted(analysis.missing_metrics)
                ],
                "suggested_rewrite": analysis.suggested_rewrite,
            }
            for analysis in report.query_analyses
        ]
    return payload


def _migration_queries(queries: Path | None) -> Path:
    project = _project()
    if queries is None:
        resolved = project.root / "queries"
    elif str(queries) == "-":
        return Path("-")
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


@migrate_app.command("generate", epilog=GROUP_EPILOG)
def migrate_generate(
    queries: Path = typer.Argument(None, help="SQL file or directory (defaults to project queries/)"),
    output: Path = typer.Option(None, "--output", "-o", help="Output project root (defaults to current project)"),
    history: bool = typer.Option(False, "--history", help="Generate from configured warehouse query history"),
    connection: str = typer.Option(None, "--connection", help="Connection override for --history"),
    days_back: int = typer.Option(7, "--days", "-d"),
    limit: int = typer.Option(1000, "--limit", "-l"),
    json_output: bool = typer.Option(False, "--json", help="Emit the generation report as JSON"),
):
    """Generate models and rewritten queries from SQL or warehouse history."""

    if history and queries is not None:
        raise typer.BadParameter("QUERIES and --history are mutually exclusive")
    if connection and not history:
        raise typer.BadParameter("--connection requires --history")
    if output is not None and str(output) == "-":
        raise typer.BadParameter("Migration generation creates multiple files; --output - is not supported")
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
        json_output=json_output,
    )
    emit_next_step(
        "migrate-generate",
        human_output=cli_state().human_extras,
        emit_diagnostic=emit_guidance,
    )


@migrate_app.command("check", epilog=GROUP_EPILOG)
def migrate_check(
    queries: Path = typer.Argument(None, help="SQL file or directory (defaults to project queries/)"),
    models: Path = typer.Option(None, "--models", "-m", help="Models override"),
    history: bool = typer.Option(False, "--history", help="Check configured warehouse query history"),
    connection: str = typer.Option(None, "--connection", help="Connection override for --history"),
    days_back: int = typer.Option(7, "--days", "-d"),
    limit: int = typer.Option(1000, "--limit", "-l"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
    json_output: bool = typer.Option(False, "--json", help="Emit the coverage report as JSON"),
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
        json_output=json_output,
    )
    emit_next_step(
        "migrate-check",
        human_output=cli_state().human_extras,
        emit_diagnostic=emit_guidance,
    )


@app.command(epilog=GROUP_EPILOG)
def info(
    directory: Path = typer.Argument(
        None, help="Directory containing semantic layer files (defaults to project models)"
    ),
    json_output: bool = typer.Option(False, "--json", help="Emit semantic-layer metadata as JSON"),
):
    """
    Show quick info about the semantic layer.

    Examples:
      sidemantic info
      sidemantic info ./models
    """
    try:
        output_format = resolve_output_format(json_output=json_output)
        structured = json_output or cli_state().requested_format is not None or cli_state().plain
        directory = _models_path(directory)
        layer = SemanticLayer()
        load_from_directory(layer, str(directory))

        models_payload = [
            {
                "name": model_name,
                "table": model.table,
                "dimensions": len(model.dimensions),
                "metrics": len(model.metrics),
                "relationships": len(model.relationships),
                "connected_to": [relationship.name for relationship in model.relationships],
            }
            for model_name, model in sorted(layer.graph.models.items())
        ]

        if structured:
            emit_records(
                models_payload,
                columns=("name", "table", "dimensions", "metrics", "relationships", "connected_to"),
                output_format=output_format,
                json_value={"path": str(directory), "models": models_payload},
            )
            return

        if not models_payload:
            emit_result("No models found")
            return

        lines = [f"Semantic Layer: {directory}", ""]
        for model in models_payload:
            lines.extend(
                [
                    f"● {model['name']}",
                    f"  Table: {model['table'] or 'N/A'}",
                    f"  Dimensions: {model['dimensions']}",
                    f"  Metrics: {model['metrics']}",
                    f"  Relationships: {model['relationships']}",
                ]
            )
            if model["connected_to"]:
                lines.append(f"  Connected to: {', '.join(model['connected_to'])}")
            lines.append("")
        _emit_long_report("\n".join(lines).rstrip())

    except typer.Exit:
        raise
    except Exception as e:
        fail(e)


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
      sidemantic server mcp
      sidemantic server mcp ./models --db data/warehouse.db
      sidemantic server mcp --demo
      sidemantic server mcp --apps --http --port 4100
    """
    emit_pending_deprecation("mcp-serve")
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
                fail(f"Demo models not found (tried {demo_dir} and {dev_demo_dir})")

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
        with progress("Initializing MCP server"):
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
            emit_diagnostic("Interactive UI widgets enabled")

        # Determine transport
        if http or apps:
            if apps and not http:
                emit_diagnostic("Note: --apps implies HTTP transport, enabling automatically")
            mcp.settings.port = port
            transport = "streamable-http"
        else:
            transport = "stdio"

        emit_diagnostic(f"Starting MCP server for: {directory}")
        if db_path and db_path != ":memory:":
            emit_diagnostic(f"Using database: {db_path}")
        if transport == "streamable-http":
            emit_diagnostic(f"Server running on HTTP at port {port}...")
        else:
            emit_diagnostic("Server running on stdio...")

        # Run the MCP server
        mcp.run(transport=transport)

    except typer.Exit:
        raise
    except Exception as e:
        fail(e)


server_app.command("mcp")(mcp_serve)


@app.command()
def rewrite(
    sql: str = typer.Argument(..., help="Semantic SQL query to rewrite, or - to read from stdin"),
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
        sql = read_sql_input(sql)
        layer = _load_query_layer(
            models,
            connection=connection,
            db=db,
            engine=engine,
            fallback=fallback,
            use_preaggregations=use_preaggregations,
            discover_connection=False,
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
        fail(e)


@app.command(epilog=GROUP_EPILOG)
def convert(
    source: Path = typer.Argument(
        None,
        help="Semantic project, directory, or exact file to convert",
        autocompletion=complete_path,
    ),
    output: Path = typer.Option(None, "--output", "-o", help="Destination file or directory"),
    source_format: str = typer.Option(
        "auto",
        "--from",
        help="Source format (default: auto-detect)",
        autocompletion=complete_source_format,
    ),
    source_extension: str = typer.Option(
        None,
        "--source-extension",
        help="File extension for stdin input when a format supports multiple syntaxes (for example, .sql)",
    ),
    target_format: str = typer.Option(
        "sidemantic", "--to", help="Destination format", autocompletion=complete_target_format
    ),
    force: bool = typer.Option(False, "--force", help="Allow writing to an existing destination"),
):
    """Convert semantic definitions through the shared format registry."""

    try:
        import tempfile

        from sidemantic.formats import OutputKind, convert_semantic_source, get_semantic_format

        source_from_stdin = str(source) == "-"
        output_to_stdout = str(output) == "-"
        if source_from_stdin and source_format == "auto":
            raise InvocationError("--from is required when the conversion source is standard input")
        if source_extension and not source_from_stdin:
            raise InvocationError("--source-extension only applies when the conversion source is standard input")

        source = source if source_from_stdin else _models_path(source)
        output = output or (_project().root / f"converted.{target_format}.yml")
        if output_to_stdout:
            target = get_semantic_format(target_format, operation="export")
            if target.output_kind != OutputKind.FILE:
                raise InvocationError(
                    f"Format '{target.name}' produces multiple or shape-dependent files and cannot use --output -"
                )
        if str(output) != "-" and output.exists() and not force:
            raise ValueError(f"Destination already exists: {output}; pass --force to replace it")

        with tempfile.TemporaryDirectory(prefix="sidemantic-convert-") as temp_dir:
            temp_root = Path(temp_dir)
            if source_from_stdin:
                source_spec = get_semantic_format(source_format, operation="import")
                if source_spec.source_kind.value == "directory":
                    raise InvocationError(f"Format '{source_spec.name}' requires a directory source")
                content = read_text_input("-", label="semantic source")
                suffix = _stdin_source_extension(source_spec, content, requested=source_extension)
                source = temp_root / f"stdin{suffix}"
                source.write_text(content)
            converted_output = output
            if output_to_stdout:
                target_spec = get_semantic_format(target_format, operation="export")
                suffix = target_spec.extensions[0] if target_spec.extensions else ".txt"
                converted_output = temp_root / f"stdout{suffix}"
            with progress(f"Converting semantic definitions to {target_format}"):
                graph = convert_semantic_source(
                    source,
                    converted_output,
                    source_format=source_format,
                    target_format=target_format,
                )
            if output_to_stdout:
                write_text_output("-", converted_output.read_text())
            else:
                emit_diagnostic(f"Converted {len(graph.models)} model(s) to {target_format}: {output}")
        emit_next_step(
            "convert",
            human_output=cli_state().human_extras and not output_to_stdout,
            emit_diagnostic=emit_guidance,
        )
    except typer.Exit:
        raise
    except Exception as e:
        fail(e)


def _stdin_source_extension(source_spec, content: str, *, requested: str | None) -> str:
    """Select a meaningful temporary suffix for streamed semantic definitions."""

    extensions = tuple(extension.lower() for extension in source_spec.extensions)
    if requested:
        selected = requested.lower()
        if not selected.startswith("."):
            selected = f".{selected}"
        if extensions and selected not in extensions:
            supported = ", ".join(extensions)
            raise InvocationError(
                f"Format '{source_spec.name}' does not support stdin extension '{selected}'; choose from {supported}"
            )
        return selected
    if len(extensions) == 1:
        return extensions[0]

    stripped = content.lstrip()
    if ".json" in extensions and stripped.startswith(("{", "[")):
        return ".json"
    if ".sql" in extensions:
        import re

        sql_definition = re.compile(
            r"(?is)^(?:(?:--[^\n]*\n)|(?:/\*.*?\*/)|\s)*"
            r"(?:model|metric|dimension|segment|parameter|pre_aggregation|relationship)\b"
        )
        if sql_definition.match(content):
            return ".sql"
    return extensions[0] if extensions else ".txt"


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
        source = _models_path(source)
        from sidemantic.formats import convert_semantic_source

        graph = convert_semantic_source(source, output, target_format="sidemantic")
        if not graph.models:
            fail("No models found")

        if validate_rust:
            from sidemantic.rust_bridge import load_graph_from_yaml_with_rust

            load_graph_from_yaml_with_rust(output.read_text())
            typer.echo(f"Exported native YAML to {output} and validated it with Rust", err=True)
        else:
            typer.echo(f"Exported native YAML to {output}", err=True)
    except typer.Exit:
        raise
    except Exception as e:
        fail(e)


@app.command(epilog=GROUP_EPILOG)
def query(
    sql: str = typer.Argument(..., help="SQL query to execute, or - to read from stdin"),
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
        output_format = resolve_output_format(default="csv")
        sql = read_sql_input(sql)
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
            if cli_state().format_explicit and output_format in {"csv", "json", "jsonl"}:
                emit_records(
                    [{"sql": rewritten_sql}],
                    columns=("sql",),
                    output_format=output_format,
                    json_value={"sql": rewritten_sql},
                )
            else:
                emit_result(rewritten_sql)
            return

        # Execute query
        result = layer.sql(sql)

        # Get results
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()

        rendered = render_rows(rows, columns=columns, output_format=output_format)
        write_text_output(output, rendered)
        if output and str(output) != "-":
            emit_diagnostic(f"Results written to {output}")

    except typer.Exit:
        raise
    except Exception as e:
        fail(e)


@dashboard_app.command("validate", epilog=GROUP_EPILOG)
def dashboard_validate(
    spec: Path = typer.Argument(
        None,
        help="Dashboard YAML or JSON spec (defaults to the project dashboard)",
        autocompletion=complete_dashboard_spec,
    ),
    models: Path = typer.Option(None, "--models", "-m", help="Directory containing semantic layer files"),
    connection: str = typer.Option(
        None, "--connection", help="Database connection string (e.g., postgres://host/db, bigquery://project/dataset)"
    ),
    db: Path = typer.Option(None, "--db", help="Path to DuckDB database file (shorthand for duckdb:/// connection)"),
    use_preaggregations: bool = typer.Option(
        False, "--use-preaggregations", help="Enable automatic pre-aggregation routing while validating SQL"
    ),
    json_output: bool = typer.Option(False, "--json", help="Emit the validation report as JSON"),
):
    """Validate a semantic dashboard spec against loaded models."""
    try:
        output_format = resolve_output_format(json_output=json_output)
        structured = json_output or cli_state().requested_format is not None or cli_state().plain
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
        chart_count = sum(len(tab.get("charts") or []) for tab in document.tabs)
        if structured:
            payload = {
                "valid": not errors,
                "spec": str(spec),
                "tabs": len(document.tabs),
                "charts": chart_count,
                "errors": list(errors),
            }
            emit_records(
                [
                    {
                        "valid": not errors,
                        "spec": str(spec),
                        "tabs": len(document.tabs),
                        "charts": chart_count,
                        "errors": list(errors),
                    }
                ],
                columns=("valid", "spec", "tabs", "charts", "errors"),
                output_format=output_format,
                json_value=payload,
            )
            if errors:
                raise typer.Exit(1)
            return
        if errors:
            fail("; ".join(str(error) for error in errors))
        typer.echo(f"Dashboard spec is valid: {len(document.tabs)} tab(s), {chart_count} chart(s)")
    except typer.Exit:
        raise
    except Exception as e:
        fail(e)


@dashboard_app.command("serve")
def dashboard_serve(
    spec: Path = typer.Argument(
        None,
        help="Dashboard YAML or JSON spec (defaults to the project dashboard)",
        autocompletion=complete_dashboard_spec,
    ),
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
        if output_dir is not None:
            emit_deprecation(
                "dashboard serve --output-dir",
                human_output=cli_state().human_extras,
                emit_diagnostic=emit_warning,
            )
        if warm_interaction_preaggregations:
            emit_deprecation(
                "dashboard serve --warm-interaction-preaggregations",
                human_output=cli_state().human_extras,
                emit_diagnostic=emit_warning,
            )
        with progress("Preparing dashboard server"):
            layer = _load_query_layer(
                models,
                connection=connection,
                db=db,
                use_preaggregations=use_preaggregations,
            )
            document = DashboardDocument.from_file(spec)
            errors = document.validate(layer, execute_sql=True)
        if errors:
            fail("; ".join(str(error) for error in errors))

        serve_ui = ui_static_dir().joinpath("index.html").exists()
        if not serve_ui:
            fail("Official web UI is not built (run scripts/build_webapp.py)")

        api_config = _loaded_config.api_server if _loaded_config else None
        resolved_host = host or (api_config.host if api_config else "127.0.0.1")
        resolved_port = port or (api_config.port if api_config else 4400)
        auth_token = resolve_secret(
            direct=None,
            secret_file=credential_file_from_env("SIDEMANTIC_API_AUTH_TOKEN_FILE"),
            configured_direct=api_config.auth_token if api_config else None,
            configured_file=api_config.auth_token_file if api_config else None,
            direct_option="--auth-token",
            file_option="--auth-token-file",
            label="API auth token",
        )
        emit_diagnostic(f"Dashboard: {document.title}")
        emit_diagnostic(f"Web UI: http://{resolved_host}:{resolved_port}/")
        start_api_server(
            layer,
            host=resolved_host,
            port=resolved_port,
            serve_ui=True,
            dashboard=document,
            auth_token=auth_token,
            cors_origins=api_config.cors_origins if api_config else None,
            max_request_body_bytes=api_config.max_request_body_bytes if api_config else 1024 * 1024,
            result_cache_mb=api_config.result_cache_mb if api_config else 0,
            result_cache_ttl=api_config.result_cache_ttl if api_config else 60.0,
        )
    except typer.Exit:
        raise
    except Exception as e:
        fail(e)


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
        write_text_output(output, rendered)
        if output and str(output) != "-":
            emit_diagnostic(f"Dashboard TypeScript definitions written to {output}")
    except typer.Exit:
        raise
    except Exception as e:
        fail(e)


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
        write_text_output(output, rendered)
        if output and str(output) != "-":
            emit_diagnostic(f"Client schema written to {output}")
    except typer.Exit:
        raise
    except Exception as e:
        fail(e)


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
            raise InvocationError("provide at least one TypeScript source file, directory, or glob")
        layer = _load_graph_layer(models)
        literals = extract_sql_literals(expand_sources(sources), call=call)
        if not literals:
            fail(f"no `{call}(...)` semantic SQL literals found in the given sources")
        rendered = generate_sql_types_ts(layer, literals)
        write_text_output(output, rendered)
        if output and str(output) != "-":
            emit_diagnostic(f"Typed query bindings ({len(literals)}) written to {output}")
    except typer.Exit:
        raise
    except Exception as e:
        fail(e)


@app.command("explain-sql", hidden=True)
def explain_sql_command(
    sql: str = typer.Argument(..., help="SQL query to explain, or - to read from stdin"),
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
    Explain semantic SQL rewrite planning without executing the query.

    Examples:
      sidemantic explain "SELECT revenue FROM orders"
      sidemantic explain "SELECT * FROM (SELECT revenue, status FROM orders) sq WHERE status = 'completed'"
      sidemantic explain "SELECT revenue, status FROM orders" --use-preaggregations
      sidemantic explain "SELECT revenue FROM orders" --format json
    """
    try:
        state = cli_state()
        if state.plain and state.plain_explicit:
            raise InvocationError("explain does not support --plain; use --format json")
        if state.format_explicit and state.requested_format != "json":
            raise InvocationError("explain supports only --format json")
        if state.format_explicit:
            resolve_output_format(default="json")
        else:
            state.requested_format = None
            state.plain = False

        sql = read_sql_input(sql)
        layer = _load_query_layer(
            models,
            connection=connection,
            db=db,
            use_preaggregations=use_preaggregations,
            engine=engine,
            fallback=fallback,
            discover_connection=False,
        )
        explanation = layer.explain_sql(sql, strict=strict)
        payload = explanation.to_dict()
        if cli_state().machine_output:
            emit_json(payload)
        else:
            with page_json_output():
                emit_json(payload)

    except typer.Exit:
        raise
    except Exception as e:
        fail(e)


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
    password_file: Path = typer.Option(
        None, "--password-file", help="Read the authentication password from a file, or - for stdin"
    ),
    password: str = typer.Option(None, "--password", hidden=True),
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
      sidemantic server postgres --port 5433
      sidemantic server postgres ./models --db data/warehouse.db
      sidemantic server postgres --connection "postgres://localhost:5432/analytics"
      sidemantic server postgres --connection "bigquery://project/dataset" --port 5433
      sidemantic server postgres --demo
      sidemantic server postgres --username user --password-file .secrets/pg-password
    """
    emit_pending_deprecation("serve")
    import logging

    try:
        from sidemantic.server.server import start_server
    except ImportError:
        fail(
            "`sidemantic server postgres` requires the optional serve dependencies. "
            "Install with `pip install 'sidemantic[serve]'` or run with "
            "`uvx --from 'sidemantic[serve]' sidemantic server postgres ...`."
        )

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
                fail("Demo models not found")

        directory = demo_dir
    else:
        directory = _models_path(directory)

    # Demo data is isolated in memory unless the user explicitly selects a target database.
    resolved_connection = (
        None
        if demo and connection is None and db is None
        else _resolve_connection(connection=connection, database=db, models=directory)
    )
    connection_str = resolved_connection.connection if resolved_connection else None

    # Resolve host, port, username, password from args or config
    host_resolved = host or (_loaded_config.pg_server.host if _loaded_config else "127.0.0.1")
    port_resolved = port if port is not None else (_loaded_config.pg_server.port if _loaded_config else 5433)
    username_resolved = username or (_loaded_config.pg_server.username if _loaded_config else None)
    resolved_password_file = password_file
    if password is None and password_file is None:
        resolved_password_file = credential_file_from_env("SIDEMANTIC_PG_PASSWORD_FILE")
    password_resolved = resolve_secret(
        direct=password,
        secret_file=resolved_password_file,
        configured_direct=_loaded_config.pg_server.password if _loaded_config else None,
        configured_file=_loaded_config.pg_server.password_file if _loaded_config else None,
        direct_option="--password",
        file_option="--password-file",
        label="PostgreSQL server password",
    )

    if (username_resolved is None) != (password_resolved is None):
        raise InvocationError("Must provide both --username and --password-file/--password for PG server auth")

    # Create semantic layer (only pass connection if not None, otherwise use default)
    preagg_db = _loaded_config.preagg_database if _loaded_config else None
    preagg_sch = _loaded_config.preagg_schema if _loaded_config else None
    if connection_str:
        layer = SemanticLayer(connection=connection_str, preagg_database=preagg_db, preagg_schema=preagg_sch)
    else:
        layer = SemanticLayer(preagg_database=preagg_db, preagg_schema=preagg_sch)

    # Load models
    with progress("Preparing PostgreSQL server"):
        load_from_directory(layer, str(directory))

    if not layer.graph.models:
        fail("No models found")

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
            raise InvocationError(f"user-attrs file {user_attrs_file} does not exist")
        try:
            loaded = json.loads(user_attrs_file.read_text())
        except json.JSONDecodeError as exc:
            raise InvocationError(f"failed to parse user-attrs file {user_attrs_file}: {exc}") from exc
        if not isinstance(loaded, dict) or not all(isinstance(v, dict) for v in loaded.values()):
            raise InvocationError(f"user-attrs file {user_attrs_file} must map usernames to attribute objects")
        user_attrs_map = loaded

    # Start the server
    emit_diagnostic(f"PostgreSQL server listening on {host_resolved}:{port_resolved}")
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
    auth_token_file: Path = typer.Option(
        None, "--auth-token-file", help="Read the API bearer token from a file, or - for stdin"
    ),
    auth_token: str = typer.Option(None, "--auth-token", hidden=True),
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
      sidemantic server api
      sidemantic server api ./models --db data/warehouse.db
      sidemantic server api --connection "postgres://localhost:5432/analytics"
      sidemantic server api --auth-token-file .secrets/api-token --cors-origin https://app.example.com
      sidemantic server api --demo
    """
    emit_pending_deprecation("api-serve")
    try:
        from sidemantic.api_server import start_api_server, ui_static_dir
    except ImportError as e:
        fail(e)

    if demo:
        import sidemantic

        package_dir = Path(sidemantic.__file__).parent
        demo_dir = package_dir / "examples" / "multi_format_demo"

        if not demo_dir.exists():
            dev_demo_dir = package_dir.parent / "examples" / "multi_format_demo"
            if dev_demo_dir.exists():
                demo_dir = dev_demo_dir
            else:
                fail("Demo models not found")

        directory = demo_dir
    else:
        directory = _models_path(directory)

    resolved_connection = (
        None
        if demo and connection is None and db is None
        else _resolve_connection(connection=connection, database=db, models=directory)
    )
    connection_str = resolved_connection.connection if resolved_connection else None
    init_sql = resolved_connection.init_sql if resolved_connection else None

    host_resolved = host or (_loaded_config.api_server.host if _loaded_config else "127.0.0.1")
    port_resolved = port if port is not None else (_loaded_config.api_server.port if _loaded_config else 4400)
    resolved_auth_token_file = auth_token_file
    if auth_token is None and auth_token_file is None:
        resolved_auth_token_file = credential_file_from_env("SIDEMANTIC_API_AUTH_TOKEN_FILE")
    auth_token_resolved = resolve_secret(
        direct=auth_token,
        secret_file=resolved_auth_token_file,
        configured_direct=_loaded_config.api_server.auth_token if _loaded_config else None,
        configured_file=_loaded_config.api_server.auth_token_file if _loaded_config else None,
        direct_option="--auth-token",
        file_option="--auth-token-file",
        label="API auth token",
    )
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

    with progress("Preparing HTTP API server"):
        load_from_directory(layer, str(directory))

    if not layer.graph.models:
        fail("No models found")

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

    emit_diagnostic(f"Starting HTTP API server for: {directory}")
    emit_diagnostic(f"Listening on http://{host_resolved}:{port_resolved}")
    if serve_ui:
        emit_diagnostic(f"Web UI: http://{host_resolved}:{port_resolved}/")
    elif ui:
        emit_diagnostic("Web UI: not built (run scripts/build_webapp.py)")
    if auth_token_resolved:
        emit_diagnostic("Authentication: bearer token required")
    else:
        emit_diagnostic("Authentication: disabled")

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
    emit_pending_deprecation("tree")
    from sidemantic.workbench import WorkbenchDependencyError, run_workbench

    if not directory.exists():
        raise InvocationError(f"Directory {directory} does not exist")

    try:
        run_workbench(directory)
    except WorkbenchDependencyError as e:
        fail(e)


@app.command(epilog=GROUP_EPILOG)
def validate(
    directory: Path = typer.Argument(
        None, help="Directory containing semantic layer files (defaults to project models)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed validation results"),
    engine: str = typer.Option(None, "--engine", help="Runtime engine: python, rust, or auto"),
    fallback: bool | None = typer.Option(None, "--fallback/--no-fallback", help="Allow Rust engine fallback to Python"),
    json_output: bool = typer.Option(False, "--json", help="Emit the validation report as JSON"),
):
    """
    Validate semantic layer definitions.

    Shows errors, warnings, and optionally detailed info.

    Examples:
      sidemantic validate
      sidemantic validate ./models --verbose
    """
    output_format = resolve_output_format(json_output=json_output)
    structured = json_output or cli_state().requested_format is not None or cli_state().plain
    verbose = verbose or cli_state().verbose
    directory = _models_path(directory)

    engine, fallback = _resolve_engine_options(engine, fallback)
    _configure_engine_environment(engine, fallback)

    rust_models: list[str] | None = None
    if engine in {"rust", "auto"}:
        try:
            from sidemantic.rust_bridge import load_graph_from_directory_with_rust

            graph = load_graph_from_directory_with_rust(directory)
            rust_models = sorted(graph.models)
            if not structured:
                typer.echo(f"Validated {len(graph.models)} models with Rust")
            if verbose and not structured:
                for model_name in sorted(graph.models):
                    typer.echo(f"  - {model_name}")
        except Exception as e:
            if engine == "rust" or not fallback:
                if cli_state().debug:
                    raise
                message = f"Rust validation failed: {e}"
                if structured:
                    emit_records(
                        [{"level": "error", "message": message}],
                        columns=("level", "message"),
                        output_format=output_format,
                        json_value={
                            "valid": False,
                            "path": str(directory),
                            "engine": engine,
                            "errors": [message],
                            "warnings": [],
                            "info": [],
                        },
                    )
                else:
                    emit_error(message)
                raise typer.Exit(1)

    # Canonical semantic checks are always Python-backed; Rust validation above
    # is an additional compatibility check, not a different definition of valid.
    _configure_engine_environment("python", False)
    try:
        from sidemantic.validation_runner import validate_directory

        report = validate_directory(directory)
    except Exception as e:
        if cli_state().debug:
            raise
        payload = {
            "valid": False,
            "path": str(directory),
            "engine": engine or "python",
            "errors": [str(e)],
            "warnings": [],
            "info": [],
        }
        if structured:
            emit_records(
                [{"level": "error", "message": str(e)}],
                columns=("level", "message"),
                output_format=output_format,
                json_value=payload,
            )
        else:
            emit_error(e)
        raise typer.Exit(1)

    if structured:
        payload = {
            "valid": not report.errors,
            "path": str(directory),
            "engine": engine or "python",
            "rust_models": rust_models,
            "errors": list(report.errors),
            "warnings": list(report.warnings),
            "info": list(report.info),
        }
        records = [
            *({"level": "error", "message": str(message)} for message in report.errors),
            *({"level": "warning", "message": str(message)} for message in report.warnings),
            *({"level": "info", "message": str(message)} for message in report.info),
        ]
        emit_records(records, columns=("level", "message"), output_format=output_format, json_value=payload)
        if report.errors:
            raise typer.Exit(1)
        return

    lines = [f"Validation Results: {directory}"]

    if report.errors:
        lines.append("Errors:")
        for error in report.errors:
            lines.append(f"  - {error}")

    if report.warnings:
        lines.append("Warnings:")
        for warning in report.warnings:
            lines.append(f"  - {warning}")

    if verbose or not (report.errors or report.warnings):
        lines.append("Info:")
        for item in report.info:
            lines.append(f"  - {item}")

    _emit_long_report("\n".join(lines))

    if report.errors:
        typer.echo("Validation Failed", err=True)
        raise typer.Exit(1)

    typer.echo("Validation Passed")
    emit_next_step(
        "validate",
        human_output=cli_state().human_extras,
        emit_diagnostic=emit_guidance,
    )


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
                fail(f"Demo models not found (tried {demo_dir} and {dev_demo_dir})")

        directory = demo_dir
        try:
            run_workbench(directory, demo_mode=True, connection=None)
        except WorkbenchDependencyError as e:
            fail(e)
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
            fail(e)


# Pre-aggregation recommendation commands
preagg_app = typer.Typer(
    cls=ContractGroup,
    context_settings=CLI_CONTEXT_SETTINGS,
    help="Pre-aggregation recommendation and management",
    epilog=f"Examples: sidemantic preagg recommend --queries queries.sql\n\n{GROUP_EPILOG}",
    no_args_is_help=True,
)
app.add_typer(preagg_app, name="preagg")


@preagg_app.command("recommend", epilog=GROUP_EPILOG)
def preagg_recommend(
    queries: Path = typer.Option(None, "--queries", "-q", help="Path to file/folder with SQL queries"),
    connection: str = typer.Option(None, "--connection", help="Database connection string to fetch query history"),
    db: Path = typer.Option(None, "--db", help="Path to DuckDB database file (shorthand for duckdb:/// connection)"),
    days_back: int = typer.Option(7, "--days", "-d", help="Days of query history to fetch (default: 7)"),
    limit: int = typer.Option(1000, "--limit", "-l", help="Max queries to analyze (default: 1000)"),
    min_count: int = typer.Option(10, "--min-count", help="Minimum query count for recommendation (default: 10)"),
    min_score: float = typer.Option(0.3, "--min-score", help="Minimum benefit score (0-1, default: 0.3)"),
    top_n: int = typer.Option(None, "--top", "-n", help="Show only top N recommendations"),
    json_output: bool = typer.Option(False, "--json", help="Emit recommendations as JSON"),
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
        output_format = resolve_output_format(json_output=json_output)
        structured = json_output or cli_state().requested_format is not None or cli_state().plain
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
            emit_diagnostic(f"Fetching query history from {adapter.dialect}...")
            with progress("Fetching and analyzing query history"):
                recommender.fetch_and_parse_query_history(adapter, days_back=days_back, limit=limit)
        elif queries:
            if str(queries) == "-":
                sql = read_text_input(queries, label="query log")
                recommender.parse_query_log([query.strip() for query in sql.split(";") if query.strip()])
            elif not queries.exists():
                raise InvocationError(f"Query source does not exist: {queries}")
            elif queries.is_file():
                recommender.parse_query_log_file(str(queries))
            else:
                for sql_file in queries.glob("**/*.sql"):
                    recommender.parse_query_log_file(str(sql_file))

        # Print summary
        summary = recommender.get_summary()
        emit_diagnostic(f"✓ Analyzed {summary['total_queries']} queries")
        emit_diagnostic(f"Found {summary['unique_patterns']} unique patterns")
        emit_diagnostic(f"{summary['patterns_above_threshold']} patterns above threshold")

        if summary["models"]:
            emit_diagnostic("Models:")
            for model_name, count in summary["models"].items():
                emit_diagnostic(f"  {model_name}: {count} queries")

        # Get recommendations
        recommendations = recommender.get_recommendations(top_n=top_n)

        recommendation_payload = [
            {
                "name": rec.suggested_name,
                "model": rec.pattern.model,
                "query_count": rec.query_count,
                "benefit_score": rec.estimated_benefit_score,
                "metrics": sorted(rec.pattern.metrics),
                "dimensions": sorted(rec.pattern.dimensions),
                "granularities": sorted(rec.pattern.granularities),
            }
            for rec in recommendations
        ]

        if structured:
            emit_records(
                recommendation_payload,
                columns=("name", "model", "query_count", "benefit_score", "metrics", "dimensions", "granularities"),
                output_format=output_format,
                json_value={"summary": summary, "recommendations": recommendation_payload},
            )
            return

        if not recommendations:
            typer.echo("\nNo recommendations found above thresholds", err=True)
            typer.echo(
                f"Try lowering --min-count (currently {min_count}) or --min-score (currently {min_score})", err=True
            )
            raise typer.Exit(0)

        # Print recommendations as one report so terminal paging remains atomic.
        lines = ["=" * 80, f"Pre-Aggregation Recommendations (found {len(recommendations)})", "=" * 80, ""]
        for i, rec in enumerate(recommendations, 1):
            lines.extend(
                [
                    f"{i}. {rec.suggested_name}",
                    f"   Model: {rec.pattern.model}",
                    f"   Query Count: {rec.query_count}",
                    f"   Benefit Score: {rec.estimated_benefit_score:.2f}",
                    f"   Metrics: {', '.join(sorted(rec.pattern.metrics))}",
                    "   Dimensions: "
                    f"{', '.join(sorted(rec.pattern.dimensions)) if rec.pattern.dimensions else '(none)'}",
                ]
            )
            if rec.pattern.granularities:
                lines.append(f"   Granularities: {', '.join(sorted(rec.pattern.granularities))}")
            lines.append("")

        _emit_long_report("\n".join(lines).rstrip())

        typer.echo("Run 'sidemantic preagg apply' to add these to your models", err=True)
        emit_next_step(
            "preagg-recommend",
            human_output=cli_state().human_extras,
            emit_diagnostic=emit_guidance,
        )

    except typer.Exit:
        raise
    except Exception as e:
        fail(e)


@preagg_app.command("apply", epilog=GROUP_EPILOG)
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
    json_output: bool = typer.Option(False, "--json", help="Emit the apply report as JSON"),
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
        output_format = resolve_output_format(json_output=json_output)
        structured = json_output or cli_state().requested_format is not None or cli_state().plain
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
            emit_diagnostic(f"Fetching query history from {adapter.dialect}...")
            with progress("Fetching and analyzing query history"):
                recommender.fetch_and_parse_query_history(adapter, days_back=days_back, limit=limit)
        elif queries:
            if str(queries) == "-":
                sql = read_text_input(queries, label="query log")
                recommender.parse_query_log([query.strip() for query in sql.split(";") if query.strip()])
            elif not queries.exists():
                raise InvocationError(f"Query source does not exist: {queries}")
            elif queries.is_file():
                recommender.parse_query_log_file(str(queries))
            else:
                for sql_file in queries.glob("**/*.sql"):
                    recommender.parse_query_log_file(str(sql_file))

        # Get recommendations
        recommendations = recommender.get_recommendations(top_n=top_n)

        if not recommendations:
            if structured:
                payload = {"dry_run": dry_run, "recommendations": 0, "added": 0, "skipped": 0}
                emit_records([payload], output_format=output_format, json_value=payload)
                return
            emit_diagnostic("No recommendations found above thresholds")
            raise typer.Exit(0)

        emit_diagnostic(f"Found {len(recommendations)} recommendations to apply")

        with progress("Applying pre-aggregation recommendations"):
            result = apply_recommendations_to_yaml(directory, recommendations, recommender, dry_run=dry_run)

        if structured:
            payload = {
                "dry_run": dry_run,
                "recommendations": len(recommendations),
                "added": result.added,
                "skipped": result.skipped,
            }
            emit_records([payload], output_format=output_format, json_value=payload)
            return

        if dry_run:
            emit_diagnostic(f"Dry run: Would add {result.added} pre-aggregations")
            emit_diagnostic("Remove --dry-run to apply changes")
        else:
            emit_diagnostic(f"✓ Added {result.added} pre-aggregations to model files")
        emit_next_step(
            "preagg-apply-dry-run" if dry_run else "preagg-apply",
            human_output=cli_state().human_extras,
            emit_diagnostic=emit_guidance,
        )
        if result.skipped:
            emit_diagnostic(f"Skipped {result.skipped} already-present definitions")

    except typer.Exit:
        raise
    except Exception as e:
        fail(e)


@preagg_app.command("refresh", epilog=GROUP_EPILOG)
def refresh(
    directory: Path = typer.Argument(
        None, help="Directory containing semantic layer files (defaults to project models)"
    ),
    model: str = typer.Option(
        None, "--model", help="Only refresh pre-aggregations for this model", autocompletion=complete_model
    ),
    preagg: str = typer.Option(None, "--preagg", "-p", help="Only refresh this specific pre-aggregation"),
    mode: str = typer.Option("auto", "--mode", help="Refresh mode: auto, full, incremental, merge, or engine"),
    connection: str = typer.Option(
        None, "--connection", help="Database connection string (e.g., postgres://host/db, bigquery://project/dataset)"
    ),
    db: Path = typer.Option(None, "--db", help="Path to DuckDB database file (shorthand for duckdb:/// connection)"),
    json_output: bool = typer.Option(False, "--json", help="Emit the refresh report as JSON"),
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
        output_format = resolve_output_format(json_output=json_output)
        structured = json_output or cli_state().requested_format is not None or cli_state().plain
        from sidemantic.core.preagg_management import resolve_preaggregation_targets, resolve_refresh_mode

        directory = _models_path(directory)
        # Load semantic layer
        preagg_db = _loaded_config.preagg_database if _loaded_config else None
        preagg_sch = _loaded_config.preagg_schema if _loaded_config else None
        layer = SemanticLayer(preagg_database=preagg_db, preagg_schema=preagg_sch)
        load_from_directory(layer, str(directory))

        if not layer.graph.models:
            fail("No models found")

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
            fail(
                f"Unsupported connection type: {connection_str}. Currently only DuckDB is supported for manual "
                "refresh modes (full, incremental, merge); use --mode engine for Snowflake, ClickHouse, or BigQuery"
            )

        preaggs_to_refresh = resolve_preaggregation_targets(
            layer.graph.models,
            model_name=model,
            preagg_name=preagg,
        )

        if not preaggs_to_refresh:
            fail("No pre-aggregations found to refresh")

        emit_diagnostic(f"Refreshing {len(preaggs_to_refresh)} pre-aggregation(s)...")

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
                fail(
                    f"Unsupported dialect for engine mode: {connection_str}. "
                    "Engine mode supports snowflake, clickhouse, and bigquery"
                )
        elif connection_str.startswith("duckdb://"):
            dialect = "duckdb"

        # Refresh each pre-aggregation
        refreshed: list[dict[str, object]] = []
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
            emit_diagnostic(f"Refreshing {model_name}.{preagg_obj.name} ({resolved_mode})...")
            with progress(f"Refreshing {model_name}.{preagg_obj.name}"):
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
            refreshed.append(
                {
                    "model": model_name,
                    "preaggregation": preagg_obj.name,
                    "table": table_name,
                    "mode": resolved_mode,
                    "rows_inserted": result.rows_inserted,
                    "duration_seconds": result.duration_seconds,
                }
            )

            # Print result
            if not structured:
                if result.rows_inserted >= 0:
                    emit_diagnostic(f"  ✓ {table_name}: {result.rows_inserted} rows in {result.duration_seconds:.2f}s")
                else:
                    emit_diagnostic(f"  ✓ {table_name}: completed in {result.duration_seconds:.2f}s")

        if structured:
            emit_records(refreshed, output_format=output_format, json_value={"refreshed": refreshed})
        else:
            emit_diagnostic("Done!")

    except typer.Exit:
        raise
    except Exception as e:
        fail(e)


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
