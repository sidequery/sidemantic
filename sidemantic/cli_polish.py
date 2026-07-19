"""Human-facing polish for the Sidemantic command line.

The helpers in this module deliberately do not own output-format or terminal
state.  Callers resolve that state once in the shared CLI context, then pass a
single ``human_output`` decision here.  This keeps hints, warnings, paging, and
completion metadata out of machine-readable output.
"""

from __future__ import annotations

import os
import shutil
from collections.abc import Callable, Iterable
from contextlib import contextmanager
from dataclasses import dataclass
from difflib import get_close_matches
from pathlib import Path
from typing import IO, Literal

import click
from click.shell_completion import CompletionItem
from typer.core import TyperGroup

MACHINE_FORMATS = frozenset({"csv", "json", "jsonl"})


def closest_command(requested: str, candidates: Iterable[str]) -> str | None:
    """Return one plausible command correction without executing it."""

    matches = get_close_matches(requested, list(candidates), n=1, cutoff=0.55)
    return matches[0] if matches else None


def invocation_requests_machine_output(args: Iterable[str]) -> bool:
    """Return whether raw CLI arguments explicitly request machine output."""

    values = list(args)
    for index, value in enumerate(values):
        if value in {"--quiet", "-q", "--plain", "--json"}:
            return True
        if value.startswith("--format=") and value.partition("=")[2].lower() in MACHINE_FORMATS:
            return True
        if value == "--format" and index + 1 < len(values) and values[index + 1].lower() in MACHINE_FORMATS:
            return True
    return False


def context_requests_machine_output(ctx: click.Context) -> bool:
    """Return whether already-parsed root options suppress human extras."""

    params = ctx.find_root().params
    requested_format = str(params.get("output_format") or params.get("format") or "").lower()
    return bool(params.get("quiet") or params.get("plain") or requested_format in MACHINE_FORMATS)


class SuggestionGroup(TyperGroup):
    """Typer group that adds safe command-name recovery guidance."""

    def resolve_command(
        self,
        ctx: click.Context,
        args: list[str],
    ) -> tuple[str | None, click.Command | None, list[str]]:
        try:
            resolved = super().resolve_command(ctx, args)
        except click.UsageError as exc:
            if not args or invocation_requests_machine_output(args) or context_requests_machine_output(ctx):
                raise
            requested = args[0]
            suggestion = closest_command(requested, self.list_commands(ctx))
            if suggestion is None or "No such command" not in exc.message:
                raise
            command_path = f"{ctx.command_path} {suggestion} --help"
            raise click.UsageError(
                f"{exc.message}\nDid you mean '{suggestion}'?\n"
                f"Run '{command_path}' to review the corrected command before executing it.",
                ctx=ctx,
            ) from exc
        command_name = resolved[0]
        if command_name and find_deprecation(command_name) is not None:
            ctx.meta.setdefault("deprecated_commands", []).append(command_name)
        return resolved


@dataclass(frozen=True)
class Deprecation:
    """A public compatibility promise for one legacy CLI surface."""

    legacy: str
    replacement: str
    deprecated_since: str
    remove_in: str
    kind: Literal["command", "flag"] = "command"
    note: str | None = None

    def warning(self) -> str:
        message = (
            f"Deprecated: '{self.legacy}' is retained for compatibility; "
            f"use '{self.replacement}'. It will be removed in Sidemantic {self.remove_in}."
        )
        if self.note:
            message += f" {self.note}"
        return message


DEPRECATIONS: tuple[Deprecation, ...] = (
    Deprecation("gen", "generate", "0.10.0", "0.12.0"),
    Deprecation("migrator", "migrate generate/check", "0.10.0", "0.12.0"),
    Deprecation("export-native", "convert --to sidemantic", "0.10.0", "0.12.0"),
    Deprecation("explain-sql", "explain", "0.10.0", "0.12.0"),
    Deprecation("serve", "server postgres", "0.10.0", "0.12.0"),
    Deprecation("api-serve", "server api", "0.10.0", "0.12.0"),
    Deprecation("mcp-serve", "server mcp", "0.10.0", "0.12.0"),
    Deprecation("tree", "workbench", "0.10.0", "0.12.0"),
    Deprecation(
        "server postgres --password",
        "server postgres --password-file",
        "0.10.2",
        "1.0.0",
        kind="flag",
        note="The file option also accepts '-' for standard input.",
    ),
    Deprecation(
        "server api --auth-token",
        "server api --auth-token-file",
        "0.10.2",
        "1.0.0",
        kind="flag",
        note="The file option also accepts '-' for standard input.",
    ),
    Deprecation(
        "dashboard serve --output-dir",
        "dashboard serve without --output-dir",
        "0.10.0",
        "0.12.0",
        kind="flag",
        note="The official UI ignores this option.",
    ),
    Deprecation(
        "dashboard serve --warm-interaction-preaggregations",
        "dashboard serve without --warm-interaction-preaggregations",
        "0.10.0",
        "0.12.0",
        kind="flag",
        note="The official UI ignores this option.",
    ),
)


def find_deprecation(legacy: str) -> Deprecation | None:
    """Find lifecycle metadata for a command or flag."""

    return next((item for item in DEPRECATIONS if item.legacy == legacy), None)


def emit_deprecation(
    legacy: str,
    *,
    human_output: bool,
    emit_diagnostic: Callable[[str], None],
) -> None:
    """Emit a registered warning through the shared diagnostic channel."""

    if not human_output:
        return
    deprecation = find_deprecation(legacy)
    if deprecation is not None:
        emit_diagnostic(deprecation.warning())


RECOVERY_HINTS: tuple[tuple[str, str], ...] = (
    ("destination already exists", "Review the destination, then pass --force only if replacing it is intentional."),
    ("no models found", "Run 'sidemantic validate' and confirm the project or --models path."),
    ("query source not found", "Check the query path, or omit it to use the project's queries/ directory."),
    ("must specify --queries or --connection", "Pass --queries PATH, or configure a database and pass --connection."),
    ("requires --history", "Add --history, or remove the warehouse connection option."),
    ("unknown semantic format", "Run 'sidemantic help convert' to list supported source and destination formats."),
)


def recovery_hint(message: str) -> str | None:
    """Return a non-mutating corrective action for a familiar error."""

    normalized = message.casefold()
    return next((hint for marker, hint in RECOVERY_HINTS if marker in normalized), None)


NEXT_STEPS: dict[str, str] = {
    "validate": "Next: inspect the model with 'sidemantic info'.",
    "convert": "Next: validate the converted project with 'sidemantic validate'.",
    "migrate-generate": "Next: review the generated files, then run 'sidemantic migrate check'.",
    "migrate-check": "Next: validate generated models with 'sidemantic validate'.",
    "preagg-recommend": "Next: preview changes with 'sidemantic preagg apply --dry-run'.",
    "preagg-apply-dry-run": "Next: remove --dry-run when the proposed changes look correct.",
    "preagg-apply": "Next: refresh materializations with 'sidemantic preagg refresh'.",
}


def next_step(workflow: str) -> str | None:
    """Return concise guidance for a successful workflow."""

    return NEXT_STEPS.get(workflow)


def emit_next_step(
    workflow: str,
    *,
    human_output: bool,
    emit_diagnostic: Callable[[str], None],
) -> None:
    """Emit post-success guidance through the shared diagnostic channel."""

    if not human_output:
        return
    hint = next_step(workflow)
    if hint:
        emit_diagnostic(hint)


def should_page(
    text: str,
    *,
    stream: IO[str],
    human_output: bool,
    pager_enabled: bool | None = None,
    terminal_height: int | None = None,
) -> bool:
    """Return whether long human output should be sent through a pager."""

    if not human_output or pager_enabled is False or not stream.isatty():
        return False
    if os.environ.get("TERM") == "dumb":
        return False
    height = terminal_height or shutil.get_terminal_size(fallback=(80, 24)).lines
    return pager_enabled is True or text.count("\n") + 1 > max(height - 2, 1)


def emit_long_output(
    text: str,
    *,
    stream: IO[str],
    human_output: bool,
    pager_enabled: bool | None = None,
    color: bool = False,
) -> None:
    """Page long TTY output, otherwise write it directly without decoration."""

    if should_page(
        text,
        stream=stream,
        human_output=human_output,
        pager_enabled=pager_enabled,
    ):
        with _configured_pager():
            click.echo_via_pager(text, color=color)
        return
    click.echo(text, file=stream, color=color)


@contextmanager
def _configured_pager():
    """Temporarily apply the Sidemantic-specific pager preference."""

    configured = os.environ.get("SIDEMANTIC_PAGER")
    if not configured:
        yield
        return
    previous = os.environ.get("PAGER")
    os.environ["PAGER"] = configured
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("PAGER", None)
        else:
            os.environ["PAGER"] = previous


def complete_semantic_format(
    _ctx: click.Context,
    param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete import/export formats without importing adapter dependencies."""

    operation = "export" if param.name == "target_format" else "import"
    return _semantic_format_choices(operation, incomplete)


def complete_source_format(_ctx: click.Context, incomplete: str) -> list[tuple[str, str]]:
    """Complete import formats for Typer's completion callback contract."""

    return [(item.value, item.help or "") for item in _semantic_format_choices("import", incomplete)]


def complete_target_format(_ctx: click.Context, incomplete: str) -> list[tuple[str, str]]:
    """Complete export formats for Typer's completion callback contract."""

    return [(item.value, item.help or "") for item in _semantic_format_choices("export", incomplete)]


def _semantic_format_choices(operation: Literal["import", "export"], incomplete: str) -> list[CompletionItem]:
    from sidemantic.formats import semantic_formats

    choices: list[CompletionItem] = []
    if operation == "import":
        choices.append(CompletionItem("auto", help="detect the source format"))
    for spec in semantic_formats():
        if operation == "export" and not spec.supports_export:
            continue
        if spec.name.startswith(incomplete):
            choices.append(CompletionItem(spec.name, help=f"{operation} semantic definitions"))
    return choices


def complete_dashboard_spec(
    _ctx: click.Context,
    incomplete: str,
) -> list[str]:
    """Complete dashboard directories plus YAML and JSON specification files."""

    raw = Path(incomplete)
    parent = raw.parent
    prefix = raw.name
    try:
        candidates = sorted(parent.expanduser().iterdir(), key=lambda item: item.name)
    except OSError:
        return []
    results: list[str] = []
    for candidate in candidates:
        if not candidate.name.startswith(prefix):
            continue
        value = str(parent / candidate.name) if str(parent) != "." else candidate.name
        if candidate.is_dir():
            results.append(f"{value}/")
        elif candidate.suffix.casefold() in {".json", ".yaml", ".yml"}:
            results.append(value)
    return results


def complete_path(
    _ctx: click.Context,
    incomplete: str,
) -> list[str]:
    """Complete readable files and directories for project input paths."""

    raw = Path(incomplete)
    parent = raw.parent
    prefix = raw.name
    try:
        candidates = sorted(parent.expanduser().iterdir(), key=lambda item: item.name)
    except OSError:
        return []
    results: list[str] = []
    for candidate in candidates:
        if not candidate.name.startswith(prefix):
            continue
        value = str(parent / candidate.name) if str(parent) != "." else candidate.name
        results.append(f"{value}/" if candidate.is_dir() else value)
    return results


def complete_model(
    ctx: click.Context,
    incomplete: str,
) -> list[tuple[str, str]]:
    """Complete model names from the selected project without opening a database."""

    try:
        names = _model_names(ctx)
        return [(name, "semantic model") for name in names if name.startswith(incomplete)]
    except Exception:
        # Completion must remain fast and silent for incomplete or invalid projects.
        return []


def _model_names(ctx: click.Context) -> list[str]:
    from sidemantic.project import ProjectContext

    project_value = ctx.find_root().params.get("project")
    config_value = ctx.find_root().params.get("config")
    project = ProjectContext.discover(start_dir=project_value, config_path=config_value)
    models = project.resolve_models(None)

    from sidemantic import SemanticLayer, load_from_directory
    from sidemantic.loaders import load_from_file

    layer = SemanticLayer()
    if Path(models).is_file():
        load_from_file(layer, Path(models))
    else:
        load_from_directory(layer, str(models))
    return sorted(layer.graph.models)
