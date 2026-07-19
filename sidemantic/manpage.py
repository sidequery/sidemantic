"""Deterministic roff man-page generation from the live Click command tree."""

from __future__ import annotations

import inspect
from collections.abc import Iterator

import click


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


def _walk_commands(
    command: click.Command,
    *,
    path: tuple[str, ...],
    parent: click.Context | None = None,
) -> Iterator[tuple[tuple[str, ...], click.Command, click.Context]]:
    ctx = click.Context(command, info_name=path[-1], parent=parent, color=False)
    yield path, command, ctx
    if not isinstance(command, click.Group):
        return
    for name in command.list_commands(ctx):
        child = command.get_command(ctx, name)
        if child is None or child.hidden:
            continue
        yield from _walk_commands(child, path=(*path, name), parent=ctx)


def _synopsis(path: tuple[str, ...], command: click.Command, ctx: click.Context) -> str:
    pieces = [*path]
    if any(isinstance(param, click.Option) and not param.hidden for param in command.params):
        pieces.append("[OPTIONS]")
    for param in command.params:
        if not isinstance(param, click.Argument):
            continue
        metavar = param.make_metavar(ctx)
        pieces.append(metavar if param.required else f"[{metavar}]")
    if isinstance(command, click.Group):
        pieces.extend(("COMMAND", "[ARGS]..."))
    return " ".join(pieces)


def _render_parameters(command: click.Command, ctx: click.Context) -> list[str]:
    lines: list[str] = []
    for param in command.params:
        if not isinstance(param, click.Option) or param.hidden:
            continue
        declarations = [*param.opts, *param.secondary_opts]
        label = ", ".join(declarations)
        if not param.is_flag:
            label += f" {param.make_metavar(ctx)}"
        lines.extend((".TP", f"\\fB{_roff(label)}\\fR", _roff(param.help or "")))
    return lines


def render_manpage(command: click.Command, *, version: str) -> str:
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
