"""Validation application."""

from pathlib import Path

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import VerticalScroll
from textual.widgets import Footer, Header, Static

from sidemantic import SemanticLayer, load_from_directory


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
            layer = SemanticLayer()
            load_from_directory(layer, str(self.directory))

            if not layer.graph.models:
                self.exit(message="No models found in directory")
                return

            self.info.append(f"Loaded {len(layer.graph.models)} models")

            # Validate each model
            for model_name, model in layer.graph.models.items():
                # Check primary key
                if not model.primary_key:
                    self.warnings.append(f"Model '{model_name}' has no primary key defined")

                # Check for dimensions
                if not model.dimensions:
                    self.warnings.append(f"Model '{model_name}' has no dimensions")

                # Check for metrics
                if not model.metrics:
                    self.warnings.append(f"Model '{model_name}' has no metrics")

                # Validate relationships
                for rel in model.relationships:
                    if rel.name not in layer.graph.models:
                        self.errors.append(f"Model '{model_name}' has relationship to '{rel.name}' which doesn't exist")

                # Check for duplicate dimension names
                dim_names = [d.name for d in model.dimensions]
                duplicates = [name for name in set(dim_names) if dim_names.count(name) > 1]
                if duplicates:
                    self.errors.append(f"Model '{model_name}' has duplicate dimensions: {', '.join(duplicates)}")

                # Check for duplicate metric names
                metric_names = [m.name for m in model.metrics]
                duplicates = [name for name in set(metric_names) if metric_names.count(name) > 1]
                if duplicates:
                    self.errors.append(f"Model '{model_name}' has duplicate metrics: {', '.join(duplicates)}")

            # Check for orphaned models
            if len(layer.graph.models) > 1:
                orphaned = []
                for model_name, model in layer.graph.models.items():
                    has_outgoing = len(model.relationships) > 0
                    has_incoming = any(
                        any(r.name == model_name for r in m.relationships)
                        for name, m in layer.graph.models.items()
                        if name != model_name
                    )
                    if not has_outgoing and not has_incoming:
                        orphaned.append(model_name)

                if orphaned:
                    self.warnings.append(f"Orphaned models (no relationships): {', '.join(orphaned)}")

            # Add summary stats
            total_dims = sum(len(m.dimensions) for m in layer.graph.models.values())
            total_metrics = sum(len(m.metrics) for m in layer.graph.models.values())
            total_rels = sum(len(m.relationships) for m in layer.graph.models.values())

            self.info.append(f"Total dimensions: {total_dims}")
            self.info.append(f"Total metrics: {total_metrics}")
            self.info.append(f"Total relationships: {total_rels}")

            # Display results
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
