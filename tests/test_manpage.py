"""Tests for generated terminal documentation."""

import tomllib
from pathlib import Path

import click
from typer.main import get_command

from sidemantic import __version__
from sidemantic.cli import app
from sidemantic.manpage import render_manpage


def _sample_cli() -> click.Group:
    @click.group(help="A useful command suite.")
    @click.option("--debug", is_flag=True, help="Show diagnostic details.")
    def cli(debug: bool) -> None:
        del debug

    @cli.command(help="Validate semantic models.")
    @click.argument("models", required=False)
    @click.option("--verbose", is_flag=True, help="Show validation details.")
    def validate(models: str | None, verbose: bool) -> None:
        del models, verbose

    @cli.command(hidden=True)
    def legacy() -> None:
        pass

    return cli


def test_manpage_is_deterministic_and_covers_visible_commands() -> None:
    first = render_manpage(_sample_cli(), version="1.2.3")
    second = render_manpage(_sample_cli(), version="1.2.3")

    assert first == second
    assert '.TH "SIDEMANTIC" "1"' in first
    assert "SIDEMANTIC VALIDATE" in first
    assert "Validate semantic models" in first
    assert "--verbose" in first.replace(r"\-", "-")
    assert "LEGACY" not in first


def test_manpage_supports_legacy_click_metavar_signature(monkeypatch) -> None:
    monkeypatch.setattr(click.Argument, "make_metavar", lambda self: self.metavar or self.name.upper())
    monkeypatch.setattr(click.Option, "make_metavar", lambda self: self.metavar or "TEXT")

    rendered = render_manpage(_sample_cli(), version="1.2.3")

    assert "[MODELS]" in rendered
    assert r"\fB\-\-debug\fR" in rendered


def test_checked_in_manpage_exists() -> None:
    path = Path(__file__).parents[1] / "sidemantic" / "man" / "sidemantic.1"
    assert path.exists()


def test_checked_in_manpage_matches_live_cli() -> None:
    path = Path(__file__).parents[1] / "sidemantic" / "man" / "sidemantic.1"
    assert path.read_text() == render_manpage(get_command(app), version=__version__)


def test_wheel_installs_generated_manpage_in_standard_location() -> None:
    root = Path(__file__).parents[1]
    config = tomllib.loads((root / "pyproject.toml").read_text())

    assert config["tool"]["hatch"]["build"]["targets"]["wheel"]["shared-data"] == {
        "sidemantic/man/sidemantic.1": "share/man/man1/sidemantic.1"
    }
