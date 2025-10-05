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
        # Check if model already exists before adding
        # This prevents double-registration when user explicitly calls add_model()
        if model.name not in layer.graph.models:
            layer.add_model(model)


def auto_register_metric(metric):
    """Auto-register graph-level metric with current layer if available.

    Only auto-registers standalone metrics (not part of a model).
    For model-level metrics, time_comparison and conversion types are auto-registered
    when the model is added to the layer (handled in SemanticGraph.add_model).

    Note: This is called from Metric.__init__ for all metrics, but we only
    register standalone graph-level metrics like derived or ratio metrics created
    outside of a model.
    """
    layer = get_current_layer()
    if layer is not None:
        # Only register if it's a metric type that makes sense at graph level
        # Don't register simple aggregations (they belong to models)
        if not metric.agg or metric.type in ("derived", "ratio"):
            try:
                layer.add_metric(metric)
            except Exception:
                # Ignore errors (metric might already exist or validation might fail)
                pass
