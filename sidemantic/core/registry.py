"""Global registry for auto-registration of models and measures."""

from contextvars import ContextVar
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .semantic_layer import SemanticLayer

# Thread-local context for current semantic layer
_current_layer: ContextVar["SemanticLayer | None"] = ContextVar("current_layer", default=None)


def get_current_layer() -> "SemanticLayer | None":
    """Get the current semantic layer from context."""
    return _current_layer.get()


def set_current_layer(layer: "SemanticLayer | None"):
    """Set the current semantic layer context."""
    _current_layer.set(layer)


def auto_register_model(model):
    """Auto-register model with current layer if available."""
    layer = get_current_layer()
    if layer is not None:
        layer.add_model(model)


def auto_register_measure(model_name: str, measure):
    """Auto-register measure with current layer if available.

    Note: Measures are added to models, so we need the model name.
    """
    # For now, measures are registered via their model
    # Complex measures (metrics) would need model context
    pass
