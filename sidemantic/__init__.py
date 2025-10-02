"""Sidemantic: SQLGlot-based semantic layer with multi-format adapter support."""

__version__ = "0.1.0"

from sidemantic.core.dimension import Dimension
from sidemantic.core.entity import Entity
from sidemantic.core.measure import Measure
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.semantic_layer import SemanticLayer

__all__ = [
    "Dimension",
    "Entity",
    "Measure",
    "Metric",
    "Model",
    "SemanticLayer",
]
