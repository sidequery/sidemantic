"""Packaging guards for the bundled ``--demo`` project.

Every public command with a ``--demo`` flag loads ``examples/multi_format_demo``
(force-included into the wheel). The extra a user installs to run that command
must therefore carry the parsers the demo's files need.
"""

import re
import tomllib
from pathlib import Path

import click
from typer.main import get_command

from sidemantic.cli import app

ROOT = Path(__file__).resolve().parents[1]
DEMO_DIR = ROOT / "examples" / "multi_format_demo"

# The extras the README and CLI errors tell users to install for each --demo command.
DEMO_COMMAND_EXTRAS = {
    "serve": {"api"},
    "server api": {"api"},
    "server mcp": {"mcp", "apps"},
    "server postgres": {"serve"},
    "workbench": {"workbench"},
}

# Demo file types whose parser lives behind an optional extra.
PARSER_EXTRA_BY_SUFFIX = {".lkml": "lookml"}


def _public_commands_with_demo_flag(command: click.Command, prefix: tuple[str, ...] = ()):
    for name, subcommand in getattr(command, "commands", {}).items():
        if subcommand.hidden:
            continue
        path = (*prefix, name)
        if any("--demo" in param.opts for param in subcommand.params):
            yield " ".join(path)
        yield from _public_commands_with_demo_flag(subcommand, path)


def _requirement_names(requirements: list[str]) -> set[str]:
    return {re.match(r"[A-Za-z0-9._-]+", requirement).group(0).lower() for requirement in requirements}


def test_demo_command_extras_cover_every_public_demo_flag():
    assert set(_public_commands_with_demo_flag(get_command(app))) == set(DEMO_COMMAND_EXTRAS)


def test_every_demo_extra_can_parse_the_bundled_demo():
    extras = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["optional-dependencies"]
    parser_extras = {
        PARSER_EXTRA_BY_SUFFIX[path.suffix] for path in DEMO_DIR.rglob("*") if path.suffix in PARSER_EXTRA_BY_SUFFIX
    }
    assert parser_extras, "no demo file matched PARSER_EXTRA_BY_SUFFIX; update it if the demo's formats changed"

    missing = {}
    for extra in sorted(set().union(*DEMO_COMMAND_EXTRAS.values())):
        provided = _requirement_names(extras[extra])
        needed = set().union(*(_requirement_names(extras[parser_extra]) for parser_extra in parser_extras))
        if needed - provided:
            missing[extra] = sorted(needed - provided)

    assert missing == {}
