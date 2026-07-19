"""Global registry for auto-registration of models and measures."""

from contextvars import ContextVar, Token
from typing import TYPE_CHECKING

from sidemantic.validation import MetricValidationError

if TYPE_CHECKING:
    from .semantic_layer import SemanticLayer

# Thread-local context for current semantic layer
_current_layer: ContextVar["SemanticLayer | None"] = ContextVar("current_layer", default=None)


def get_current_layer() -> "SemanticLayer | None":
    """Get the current semantic layer from context."""
    return _current_layer.get()


def set_current_layer(layer: "SemanticLayer | None") -> Token:
    """Set the current semantic layer context."""
    return _current_layer.set(layer)


def reset_current_layer(token: Token) -> None:
    """Restore the layer that was active before ``set_current_layer``."""
    _current_layer.reset(token)


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
            except (ValueError, MetricValidationError):
                # Best-effort: metric might already exist or validation might fail during init
                import logging

                logging.debug("Auto-registration of metric %s failed", metric.name, exc_info=True)
