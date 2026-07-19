"""Pytest configuration and fixtures."""

import os

import pytest

# Test captured output against Sidemantic's default policy even when the host
# runner requests colored logs. Individual color tests opt back in explicitly.
os.environ.pop("FORCE_COLOR", None)

from sidemantic import SemanticLayer


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
