"""Validation application."""

from pathlib import Path

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import VerticalScroll
from textual.widgets import Footer, Header, Static

from sidemantic.validation_runner import validate_directory


class ValidationApp(App):
    """Interactive validation results viewer."""

    CSS = """
    Screen {
        background: $surface;
    }

    .section {
        margin: 1 2;
        padding: 1;
        border: solid $primary;
    }

    .section-title {
        text-style: bold;
        margin-bottom: 1;
    }

    .error {
        color: $error;
    }

    .warning {
        color: $warning;
    }

    .success {
        color: $success;
    }

    .info {
        color: $accent;
    }
    """

    BINDINGS = [
        Binding("ctrl+c", "quit", "Quit"),
    ]

    def __init__(self, directory: Path, verbose: bool = False):
        super().__init__()
        self.directory = directory
        self.verbose = verbose
        self.errors = []
        self.warnings = []
        self.info = []

    def compose(self) -> ComposeResult:
        """Create child widgets."""
        yield Header()
        with VerticalScroll():
            yield Static("", id="validation-results")
        yield Footer()

    def on_mount(self) -> None:
        """Run validation."""
        try:
            report = validate_directory(self.directory)
            self.errors = report.errors
            self.warnings = report.warnings
            self.info = report.info
            self._update_display()
        except Exception as e:
            self.exit(message=f"Error during validation: {e}")

    def _update_display(self) -> None:
        """Update the validation results display."""
        results = self.query_one("#validation-results", Static)
        content = []

        content.append(f"[bold]Validation Results: {self.directory}[/bold]\n")

        if self.errors:
            content.append("[bold error]✗ Errors[/bold error]")
            for error in self.errors:
                content.append(f"  [error]✗[/error] {error}")
            content.append("")

        if self.warnings:
            content.append("[bold warning]⚠ Warnings[/bold warning]")
            for warning in self.warnings:
                content.append(f"  [warning]⚠[/warning] {warning}")
            content.append("")

        if self.verbose or not (self.errors or self.warnings):
            content.append("[bold info]ℹ Info[/bold info]")
            for i in self.info:
                content.append(f"  [info]ℹ[/info] {i}")
            content.append("")

        if not self.errors:
            content.append("\n[bold success]✓ Validation Passed[/bold success]")
        else:
            content.append("\n[bold error]✗ Validation Failed[/bold error]")

        results.update("\n".join(content))
