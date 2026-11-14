"""Deterministic roff man-page generation from the live Click command tree.

The tree may be built on standalone ``click`` or on the Click copy vendored by
newer Typer releases, which has no ``Group``, ``Option``, or ``Argument``
classes. Commands and parameters are therefore classified by Click's stable
attributes rather than by class, and contexts use the command's own
``context_class``.
"""

from __future__ import annotations

import inspect
from collections.abc import Iterator

from sidemantic.click_compat import Command, Context, Parameter


def _is_group(command: Command) -> bool:
    return callable(getattr(command, "list_commands", None))


def _is_option(param: Parameter) -> bool:
    return param.param_type_name == "option"


def _is_argument(param: Parameter) -> bool:
    return param.param_type_name == "argument"


def _roff(text: str) -> str:
    """Escape user-facing text so it is safe in a roff paragraph."""

    escaped = text.replace("\\", r"\e").replace("-", r"\-")
    if escaped.startswith((".", "'")):
        escaped = r"\&" + escaped
    return escaped


def _paragraphs(text: str | None) -> list[str]:
    if not text:
        return []
    return [" ".join(part.split()) for part in inspect.cleandoc(text).split("\n\n") if part.strip()]


def _make_metavar(param: Parameter, ctx: Context) -> str:
    """Call Click's version-dependent metavar API."""

    make_metavar = getattr(param, "make_metavar", None)
    if make_metavar is None:
        # Typer 0.26+ parameters have no make_metavar; mirror the spelling of
        # their own help output instead.
        if _is_argument(param):
            name = param.metavar or param.name.upper()
            return name if param.nargs == 1 else f"{name}..."
        return param.metavar or param.display_type(ctx)
    if "ctx" in inspect.signature(make_metavar).parameters:
        return make_metavar(ctx)
    return make_metavar()


def _walk_commands(
    command: Command,
    *,
    path: tuple[str, ...],
    parent: Context | None = None,
) -> Iterator[tuple[tuple[str, ...], Command, Context]]:
    ctx = command.context_class(command, info_name=path[-1], parent=parent, color=False)
    yield path, command, ctx
    if not _is_group(command):
        return
    for name in command.list_commands(ctx):
        child = command.get_command(ctx, name)
        if child is None or child.hidden:
            continue
        yield from _walk_commands(child, path=(*path, name), parent=ctx)


def _synopsis(path: tuple[str, ...], command: Command, ctx: Context) -> str:
    pieces = [*path]
    if any(_is_option(param) and not param.hidden for param in command.params):
        pieces.append("[OPTIONS]")
    for param in command.params:
        if not _is_argument(param):
            continue
        metavar = _make_metavar(param, ctx)
        pieces.append(metavar if param.required else f"[{metavar}]")
    if _is_group(command):
        pieces.extend(("COMMAND", "[ARGS]..."))
    return " ".join(pieces)


def _render_parameters(command: Command, ctx: Context) -> list[str]:
    lines: list[str] = []
    for param in command.params:
        if not _is_option(param) or param.hidden:
            continue
        declarations = [*param.opts, *param.secondary_opts]
        label = ", ".join(declarations)
        if not param.is_flag:
            label += f" {_make_metavar(param, ctx)}"
        lines.extend((".TP", f"\\fB{_roff(label)}\\fR", _roff(param.help or "")))
    return lines


def render_manpage(command: Command, *, version: str) -> str:
    """Render a complete, deterministic ``sidemantic(1)`` man page."""

    lines = [
        f'.TH "SIDEMANTIC" "1" "" "Sidemantic {version}" "User Commands"',
        ".SH NAME",
        "sidemantic \\- SQL\\-first semantic layer",
    ]
    for index, (path, item, ctx) in enumerate(_walk_commands(command, path=("sidemantic",))):
        title = " ".join(path).upper()
        if index == 0:
            lines.append(".SH SYNOPSIS")
        else:
            lines.extend((f'.SH "{_roff(title)}"', ".SS SYNOPSIS"))
        lines.extend((".B", _roff(_synopsis(path, item, ctx))))
        paragraphs = _paragraphs(item.help)
        if paragraphs:
            lines.append(".SS DESCRIPTION" if index else ".SH DESCRIPTION")
            for paragraph in paragraphs:
                lines.extend((".PP", _roff(paragraph)))
        parameters = _render_parameters(item, ctx)
        if parameters:
            lines.append(".SS OPTIONS" if index else ".SH OPTIONS")
            lines.extend(parameters)
    lines.extend(
        (
            ".SH DOCUMENTATION",
            "https://sidemantic.com",
            ".SH REPORTING BUGS",
            "https://github.com/sidequery/sidemantic/issues",
        )
    )
    return "\n".join(lines) + "\n"
