"""Shared behavioral contract for the Sidemantic command-line interface."""

from __future__ import annotations

import dataclasses
import json
import sys
from dataclasses import dataclass, field
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
    plain: bool = False
    color: bool | None = None
    redactions: set[str] = field(default_factory=set, repr=False)

    def reset(self, *, debug: bool = False) -> None:
        """Reset state at the start of each root invocation."""

        self.debug = debug
        self.quiet = False
        self.verbose = False
        self.machine_output = False
        self.requested_format = None
        self.plain = False
        self.color = None
        self.redactions.clear()


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


class ContractGroup(TyperGroup):
    """Root Click group that enforces concise failures and the debug contract."""

    def invoke(self, ctx: click.Context) -> Any:
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
