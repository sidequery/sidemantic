"""Sidemantic: SQLGlot-based semantic layer with multi-format adapter support."""

__version__ = "0.1.2"

from sidemantic.core.dimension import Dimension
from sidemantic.core.entity import Entity
from sidemantic.core.join import Join
from sidemantic.core.measure import Measure
from sidemantic.core.model import Model
from sidemantic.core.parameter import Parameter

__all__ = [
    "Dimension",
    "Entity",
    "Join",
    "Measure",
    "Model",
    "Parameter",
    "SemanticLayer",
]

def __getattr__(name):  # Lazy import to avoid importing duckdb on package import
    if name == "SemanticLayer":
        from sidemantic.core.semantic_layer import SemanticLayer  # type: ignore
        return SemanticLayer
    raise AttributeError(name)
