"""Generate the checked-in sidemantic(1) man page from the live CLI."""

from pathlib import Path

from typer.main import get_command

from sidemantic import __version__
from sidemantic.cli import app
from sidemantic.manpage import render_manpage

OUTPUT = Path(__file__).parents[1] / "sidemantic" / "man" / "sidemantic.1"


def main() -> None:
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(render_manpage(get_command(app), version=__version__))


if __name__ == "__main__":
    main()
