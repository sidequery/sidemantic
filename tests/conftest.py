"""Pytest configuration and fixtures."""

import pytest
from typer import rich_utils as typer_rich_utils

from sidemantic import SemanticLayer


@pytest.fixture(autouse=True)
def isolate_cli_color_environment(monkeypatch: pytest.MonkeyPatch):
    """Keep host color settings from leaking into captured CLI output."""

    monkeypatch.delenv("FORCE_COLOR", raising=False)

    # Typer snapshots GitHub Actions' color preference when rich_utils is
    # imported. Restore automatic stream detection for tests while still
    # allowing individual FORCE_COLOR tests to opt in dynamically.
    monkeypatch.setattr(typer_rich_utils, "FORCE_TERMINAL", None)


@pytest.fixture(autouse=True)
def reset_registry():
    """Clear the global registry before and after each test.

    This ensures test isolation when using auto-registration.
    """
    from sidemantic.core.registry import set_current_layer

    # Clear before test to ensure no cross-test contamination
    set_current_layer(None)

    yield

    # Clear after test
    set_current_layer(None)


@pytest.fixture
def layer():
    """Create a fresh SemanticLayer for testing with auto-registration disabled.

    This prevents models from being auto-registered during creation,
    allowing tests to explicitly control when models are added.
    """
    return SemanticLayer(auto_register=False)
