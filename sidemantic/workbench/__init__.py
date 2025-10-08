"""Interactive workbench for exploring semantic layers."""

from pathlib import Path


def run_workbench(directory: Path, demo_mode: bool = False):
    """Run the interactive workbench application."""
    from sidemantic.workbench.app import SidequeryWorkbench

    workbench_app = SidequeryWorkbench(directory, demo_mode=demo_mode)
    workbench_app.run()


def run_validation(directory: Path, verbose: bool = False):
    """Run the validation UI application."""
    from sidemantic.workbench.validation_app import ValidationApp

    app = ValidationApp(directory, verbose=verbose)
    app.run()
