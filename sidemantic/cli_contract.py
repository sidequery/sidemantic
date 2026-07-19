"""Shared behavioral contract for the Sidemantic command-line interface."""

from __future__ import annotations

import csv
import dataclasses
import io
import json
import os
import sys
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, time
from decimal import Decimal
from enum import IntEnum
from pathlib import Path
from typing import Any, NoReturn, TextIO

import click
import typer
from typer.core import TyperGroup


class ExitCode(IntEnum):
    """Stable process exit codes exposed by the CLI."""

    SUCCESS = 0
    OPERATIONAL_ERROR = 1
    INVALID_INVOCATION = 2


@dataclass
class CLIState:
    """Invocation-wide presentation state.

    T1 owns ``debug`` and ``machine_output``.  The remaining presentation fields
    are intentionally shared seams for the T2/T3 formatter, color, quiet, pager,
    warning, and guidance work.
    """

    debug: bool = False
    quiet: bool = False
    verbose: bool = False
    machine_output: bool = False
    requested_format: str | None = None
    format_explicit: bool = False
    plain: bool = False
    plain_explicit: bool = False
    color: bool | None = None
    redactions: set[str] = field(default_factory=set, repr=False)

    def reset(
        self,
        *,
        debug: bool = False,
        quiet: bool = False,
        verbose: bool = False,
        requested_format: str | None = None,
        format_explicit: bool = False,
        plain: bool = False,
        plain_explicit: bool = False,
        color: bool | None = None,
    ) -> None:
        """Reset state at the start of each root invocation."""

        self.debug = debug
        self.quiet = quiet
        self.verbose = verbose or debug
        self.machine_output = False
        self.requested_format = requested_format
        self.format_explicit = format_explicit
        self.plain = plain
        self.plain_explicit = plain_explicit
        self.color = color
        self.redactions.clear()

    @property
    def human_extras(self) -> bool:
        """Whether hints, decorations, paging, and similar extras are safe."""

        return not (self.quiet or self.machine_output or self.plain)


_state = CLIState()


def cli_state() -> CLIState:
    """Return the mutable state for the current CLI invocation."""

    return _state


class CLIError(Exception):
    """Base class for concise, expected CLI failures."""

    exit_code = ExitCode.OPERATIONAL_ERROR


class OperationalError(CLIError):
    """An operation or validation failed after a valid invocation."""


class InvocationError(CLIError):
    """Arguments or configuration are invalid."""

    exit_code = ExitCode.INVALID_INVOCATION


HELP_REQUESTED_META_KEY = "sidemantic_help_requested"


class ContractGroup(TyperGroup):
    """Root Click group that enforces concise failures and the debug contract."""

    _global_value_options = {"--project", "--config", "--format"}
    _global_flag_options = {"--plain", "--quiet", "--verbose", "--debug", "--no-color"}

    def parse_args(self, ctx: click.Context, args: list[str]) -> list[str]:
        """Accept unambiguous long global options after a subcommand.

        Click normally stops parsing root options after selecting a command.
        Long Sidemantic presentation/configuration options have no conflicting
        command-local meaning, so move only those options to the root parser.
        Short options keep Click's normal position-sensitive behavior because
        ``-q`` is already the pre-aggregation query-source option.
        """

        if ctx.parent is None:
            args = self._hoist_global_options(args)
            explicit_no_color = "--no-color" in args
            ctx.color = color_enabled(no_color=explicit_no_color, is_tty=is_terminal(sys.stdout))
        return super().parse_args(ctx, args)

    @classmethod
    def _hoist_global_options(cls, args: list[str]) -> list[str]:
        global_args: list[str] = []
        remaining: list[str] = []
        index = 0
        after_separator = False
        while index < len(args):
            token = args[index]
            if token == "--":
                after_separator = True
                remaining.append(token)
                index += 1
                continue
            if not after_separator and token in cls._global_flag_options:
                global_args.append(token)
                index += 1
                continue
            matching_value_option = next(
                (option for option in cls._global_value_options if token == option or token.startswith(f"{option}=")),
                None,
            )
            if not after_separator and matching_value_option is not None:
                global_args.append(token)
                if token == matching_value_option and index + 1 < len(args):
                    global_args.append(args[index + 1])
                    index += 2
                else:
                    index += 1
                continue
            remaining.append(token)
            index += 1
        return [*global_args, *remaining]

    def invoke(self, ctx: click.Context) -> Any:
        if self._is_help_request(ctx):
            ctx.meta[HELP_REQUESTED_META_KEY] = True
        try:
            return super().invoke(ctx)
        except (click.ClickException, click.exceptions.Exit, click.Abort):
            raise
        except CLIError as exc:
            emit_error(str(exc))
            raise click.exceptions.Exit(int(exc.exit_code)) from exc
        except Exception as exc:
            if cli_state().debug:
                raise
            emit_error(str(exc) or exc.__class__.__name__)
            raise click.exceptions.Exit(int(ExitCode.OPERATIONAL_ERROR)) from exc

    def _is_help_request(self, ctx: click.Context) -> bool:
        """Recognize help flags and no-argument subgroup help before callbacks run."""

        protected = list(getattr(ctx, "_protected_args", ()))
        tokens = [*protected, *ctx.args]
        option_tokens = tokens[: tokens.index("--")] if "--" in tokens else tokens
        if any(token in {"-h", "--help"} for token in option_tokens):
            return True
        if tokens and tokens[0] == "help":
            return True

        command: click.Command = self
        index = 0
        while isinstance(command, click.Group) and index < len(tokens):
            token = tokens[index]
            if token.startswith("-"):
                return False
            child = command.get_command(ctx, token)
            if child is None:
                return False
            command = child
            index += 1
        return index == len(tokens) and isinstance(command, click.Group) and bool(command.no_args_is_help)


def sanitize(text: object) -> str:
    """Remove registered secret values from user-visible text."""

    rendered = str(text)
    for secret in sorted(cli_state().redactions, key=len, reverse=True):
        if secret:
            rendered = rendered.replace(secret, "[REDACTED]")
    return rendered


def emit_result(value: object = "", *, nl: bool = True) -> None:
    """Write primary human or machine output to stdout."""

    typer.echo(value, nl=nl)


def emit_diagnostic(value: object, *, force: bool = False) -> None:
    """Write status or diagnostic text to stderr.

    ``force`` is for errors.  Ordinary status output is suppressible by the
    shared quiet policy and machine output mode.
    """

    if not force and (cli_state().quiet or cli_state().machine_output):
        return
    typer.echo(sanitize(value), err=True)


def emit_warning(value: object) -> None:
    """Emit a suppressible warning through the shared diagnostic seam."""

    emit_diagnostic(f"Warning: {value}")


def emit_guidance(value: object) -> None:
    """Emit suppressible next-step guidance through the shared diagnostic seam."""

    emit_diagnostic(value)


def emit_error(value: object) -> None:
    """Emit a concise error that is never hidden by quiet/machine modes."""

    emit_diagnostic(f"Error: {sanitize(value)}", force=True)


def _json_default(value: object) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    if isinstance(value, (date, datetime, time)):
        return value.isoformat()
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return dataclasses.asdict(value)
    if isinstance(value, (set, frozenset)):
        return sorted(value)
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json", exclude_none=True)
    if hasattr(value, "to_dict"):
        return value.to_dict()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


OUTPUT_FORMATS = ("table", "csv", "json", "jsonl")


def resolve_output_format(*, default: str = "table", json_output: bool = False) -> str:
    """Resolve the current output format and enforce compatible aliases."""

    state = cli_state()
    requested = state.requested_format
    if requested is not None:
        requested = requested.lower()
        if requested not in OUTPUT_FORMATS:
            raise InvocationError(f"--format must be one of: {', '.join(OUTPUT_FORMATS)}")
    if json_output:
        if requested not in {None, "json"} and state.format_explicit:
            raise InvocationError("--json cannot be combined with a non-JSON --format")
        if state.plain and state.plain_explicit:
            raise InvocationError("--plain cannot be combined with --json")
        state.plain = False
        selected = "json"
    else:
        selected = requested or default
    if state.plain:
        if requested not in {None, "table"}:
            raise InvocationError("--plain cannot be combined with --format csv, json, or jsonl")
        selected = "table"
    state.machine_output = selected in {"csv", "json", "jsonl"}
    return selected


def _record_value(value: object) -> object:
    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (Decimal, date, datetime, time, Path)):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    return json.dumps(value, sort_keys=True, default=_json_default)


def render_records(
    records: Sequence[dict[str, object]],
    *,
    columns: Sequence[str] | None = None,
    output_format: str,
    json_value: object | None = None,
) -> str:
    """Render a record collection under the standard table/CSV/JSON/JSONL contract."""

    selected_columns = list(columns or (records[0].keys() if records else []))
    if output_format == "json":
        return json.dumps(
            json_value if json_value is not None else list(records), indent=2, sort_keys=True, default=_json_default
        )
    if output_format == "jsonl":
        return "\n".join(json.dumps(record, sort_keys=True, default=_json_default) for record in records)

    buffer = io.StringIO()
    if output_format == "csv":
        writer = csv.DictWriter(buffer, fieldnames=selected_columns, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        for record in records:
            writer.writerow({column: _record_value(record.get(column)) for column in selected_columns})
        return buffer.getvalue().rstrip("\n")

    rows = [[str(_record_value(record.get(column))) for column in selected_columns] for record in records]
    if cli_state().plain:
        writer = csv.writer(buffer, dialect="excel-tab", lineterminator="\n")
        if selected_columns:
            writer.writerow(selected_columns)
        writer.writerows(rows)
        return buffer.getvalue().rstrip("\n")
    if not selected_columns:
        return ""
    widths = [
        max(len(column), *(len(row[index]) for row in rows)) if rows else len(column)
        for index, column in enumerate(selected_columns)
    ]
    header = "  ".join(column.ljust(widths[index]) for index, column in enumerate(selected_columns)).rstrip()
    separator = "  ".join("-" * width for width in widths).rstrip()
    body = ["  ".join(value.ljust(widths[index]) for index, value in enumerate(row)).rstrip() for row in rows]
    return "\n".join([header, separator, *body])


def _unique_column_names(columns: Sequence[str]) -> list[str]:
    """Return deterministic object keys while preserving every positional column."""

    unique: list[str] = []
    used: set[str] = set()
    for column in columns:
        candidate = column
        suffix = 2
        while candidate in used:
            candidate = f"{column}_{suffix}"
            suffix += 1
        used.add(candidate)
        unique.append(candidate)
    return unique


def render_rows(
    rows: Sequence[Sequence[object]],
    *,
    columns: Sequence[str],
    output_format: str,
) -> str:
    """Render positional query rows without collapsing duplicate column names."""

    selected_columns = list(columns)
    if output_format in {"json", "jsonl"}:
        object_columns = _unique_column_names(selected_columns)
        records = [dict(zip(object_columns, row, strict=True)) for row in rows]
        if output_format == "json":
            return json.dumps(records, indent=2, sort_keys=True, default=_json_default)
        return "\n".join(json.dumps(record, sort_keys=True, default=_json_default) for record in records)

    rendered_rows = [[str(_record_value(value)) for value in row] for row in rows]
    buffer = io.StringIO()
    if output_format == "csv" or cli_state().plain:
        dialect = "excel" if output_format == "csv" else "excel-tab"
        writer = csv.writer(buffer, dialect=dialect, lineterminator="\n")
        writer.writerow(selected_columns)
        writer.writerows(rendered_rows)
        return buffer.getvalue().rstrip("\n")

    widths = [
        max(len(column), *(len(row[index]) for row in rendered_rows)) if rendered_rows else len(column)
        for index, column in enumerate(selected_columns)
    ]
    header = "  ".join(column.ljust(widths[index]) for index, column in enumerate(selected_columns)).rstrip()
    separator = "  ".join("-" * width for width in widths).rstrip()
    body = ["  ".join(value.ljust(widths[index]) for index, value in enumerate(row)).rstrip() for row in rendered_rows]
    return "\n".join([header, separator, *body])


def emit_records(
    records: Sequence[dict[str, object]],
    *,
    columns: Sequence[str] | None = None,
    output_format: str | None = None,
    json_value: object | None = None,
) -> None:
    """Write standard structured records to stdout."""

    selected = output_format or resolve_output_format()
    rendered = render_records(records, columns=columns, output_format=selected, json_value=json_value)
    emit_result(rendered, nl=not (selected == "jsonl" and not rendered))


def color_enabled(*, no_color: bool = False, is_tty: bool | None = None) -> bool:
    """Resolve standard terminal color controls with explicit disable winning."""

    if no_color or _env_truthy("SIDEMANTIC_NO_COLOR") or os.environ.get("NO_COLOR", ""):
        return False
    if os.environ.get("FORCE_COLOR", ""):
        return True
    if os.environ.get("TERM", "").lower() == "dumb":
        return False
    return is_terminal(sys.stdout) if is_tty is None else is_tty


def _env_truthy(name: str) -> bool:
    value = os.environ.get(name)
    return value is not None and value.strip().lower() not in {"", "0", "false", "no", "off"}


def is_terminal(stream: TextIO) -> bool:
    """Return whether a text stream is attached to an interactive terminal."""

    isatty = getattr(stream, "isatty", None)
    return bool(isatty and isatty())


def progress_enabled(*, is_tty: bool | None = None) -> bool:
    """Return whether animated progress is safe for the current invocation."""

    state = cli_state()
    terminal = is_terminal(sys.stderr) if is_tty is None else is_tty
    return terminal and state.human_extras and not _env_truthy("CI")


@contextmanager
def progress(message: str) -> Iterator[None]:
    """Show a transient TTY-only spinner around a potentially slow operation."""

    if not progress_enabled():
        yield
        return
    from rich.console import Console

    console = Console(stderr=True, force_terminal=cli_state().color)
    with console.status(message):
        yield


def credential_file_from_env(name: str) -> Path | None:
    """Resolve a public credential-file environment variable from cwd."""

    value = os.environ.get(name)
    if not value:
        return None
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    return path.resolve()


def emit_json(value: object) -> None:
    """Write stable, valid JSON and nothing else to stdout."""

    cli_state().machine_output = True
    emit_result(json.dumps(value, indent=2, sort_keys=True, default=_json_default))


def fail(exc: Exception | str, *, usage: bool = False) -> NoReturn:
    """Convert an expected failure into the shared exit/error contract.

    In debug mode an original exception is re-raised with its traceback.  Plain
    strings remain concise expected errors even under debug.
    """

    if isinstance(exc, CLIError):
        raise exc
    if isinstance(exc, Exception) and cli_state().debug:
        raise exc
    error_type = InvocationError if usage else OperationalError
    raise error_type(sanitize(exc)) from exc if isinstance(exc, Exception) else None


def read_text_input(value: str | Path, *, label: str = "input", stdin: TextIO | None = None) -> str:
    """Read text from a literal path or standard input when ``value`` is ``-``."""

    if str(value) == "-":
        stream = stdin or sys.stdin
        content = stream.read()
        if not content:
            raise InvocationError(f"No {label} was provided on standard input")
        return content
    try:
        return Path(value).read_text()
    except OSError as exc:
        raise InvocationError(f"Could not read {label} from {value}: {exc.strerror or exc}") from exc


def read_sql_input(value: str, *, stdin: TextIO | None = None) -> str:
    """Return a SQL argument, reading standard input for the conventional ``-``."""

    if value != "-":
        return value
    content = (stdin or sys.stdin).read()
    if not content.strip():
        raise InvocationError("No SQL was provided on standard input")
    return content


def write_text_output(destination: str | Path | None, content: str, *, stdout: TextIO | None = None) -> None:
    """Write generated text to a path or stdout for ``None``/``-``."""

    if destination is None or str(destination) == "-":
        stream = stdout or sys.stdout
        stream.write(content)
        if content and not content.endswith("\n"):
            stream.write("\n")
        return
    try:
        Path(destination).write_text(content)
    except OSError as exc:
        raise OperationalError(f"Could not write {destination}: {exc.strerror or exc}") from exc


def read_secret(path: str | Path, *, label: str, stdin: TextIO | None = None) -> str:
    """Read a secret from a credential file or stdin without echoing it."""

    secret = read_text_input(path, label=label, stdin=stdin).rstrip("\r\n")
    if not secret:
        raise InvocationError(f"The {label} is empty")
    cli_state().redactions.add(secret)
    return secret


def resolve_secret(
    *,
    direct: str | None,
    secret_file: str | Path | None,
    configured_direct: str | None = None,
    configured_file: str | Path | None = None,
    direct_option: str,
    file_option: str,
    label: str,
) -> str | None:
    """Resolve a secret with file/stdin input preferred over command-line text."""

    if direct is not None and secret_file is not None:
        raise InvocationError(f"{direct_option} and {file_option} cannot be used together")
    if direct is None and secret_file is None and configured_direct is not None and configured_file is not None:
        raise InvocationError(f"inline {label} and its credential-file setting cannot both be configured")
    if direct is not None:
        cli_state().redactions.add(direct)
        emit_warning(f"{direct_option} is deprecated because command-line secrets can leak; use {file_option}")
        return direct
    if secret_file is not None:
        return read_secret(secret_file, label=label)
    if configured_file is not None:
        return read_secret(configured_file, label=label)
    if configured_direct is not None:
        cli_state().redactions.add(configured_direct)
        return configured_direct
    return None
