"""Interactive workbench for exploring semantic layers."""

from pathlib import Path

WORKBENCH_EXTRA_INSTALL = "uvx --from 'sidemantic[workbench]' sidemantic workbench --demo"
WORKBENCH_EXTRA_ADD = "uv add 'sidemantic[workbench]'"


class WorkbenchDependencyError(RuntimeError):
    """Raised when optional workbench dependencies are not installed."""


def _is_optional_workbench_dependency(module_name: str | None) -> bool:
    if not module_name:
        return False
    return module_name == "plotext" or module_name.startswith("textual")


def _missing_dependency_message(module_name: str | None, command: str) -> str:
    missing = module_name or "required package"
    return (
        f"Missing optional dependency for `sidemantic {command}`: {missing}. "
        "Install the workbench extra or run it with uvx, for example: "
        f"`{WORKBENCH_EXTRA_INSTALL}`. In a project, use `{WORKBENCH_EXTRA_ADD}`."
    )


def run_workbench(directory: Path, demo_mode: bool = False, connection: str | None = None):
    """Run the interactive workbench application."""
    try:
        from sidemantic.workbench.app import SidequeryWorkbench
    except ModuleNotFoundError as exc:
        if _is_optional_workbench_dependency(exc.name):
            raise WorkbenchDependencyError(_missing_dependency_message(exc.name, "workbench")) from exc
        raise

    workbench_app = SidequeryWorkbench(directory, demo_mode=demo_mode, connection=connection)
    workbench_app.run()


def run_validation(directory: Path, verbose: bool = False):
    """Run the validation UI application."""
    try:
        from sidemantic.workbench.validation_app import ValidationApp
    except ModuleNotFoundError as exc:
        if _is_optional_workbench_dependency(exc.name):
            raise WorkbenchDependencyError(_missing_dependency_message(exc.name, "validate")) from exc
        raise

    app = ValidationApp(directory, verbose=verbose)
    app.run()
