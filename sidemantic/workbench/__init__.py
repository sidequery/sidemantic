"""Interactive workbench for exploring semantic layers."""

from pathlib import Path

from sidemantic.workbench.app import SidequeryWorkbench
from sidemantic.workbench.validation_app import ValidationApp


def run_workbench(directory: Path, demo_mode: bool = False, connection: str | None = None):
    """Run the interactive workbench application."""
    workbench_app = SidequeryWorkbench(directory, demo_mode=demo_mode, connection=connection)
    workbench_app.run()


def run_validation(directory: Path, verbose: bool = False):
    """Run the validation UI application."""
    app = ValidationApp(directory, verbose=verbose)
    app.run()
