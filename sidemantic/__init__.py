"""Sidemantic: SQLGlot-based semantic layer with multi-format adapter support."""

__version__ = "0.2.6"

from sidemantic.core.dimension import Dimension
from sidemantic.core.metric import Metric
from sidemantic.core.model import Model
from sidemantic.core.parameter import Parameter
from sidemantic.core.pre_aggregation import PreAggregation, RefreshKey, RefreshResult
from sidemantic.core.preagg_recommender import PreAggRecommendation, PreAggregationRecommender, QueryPattern
from sidemantic.core.relationship import Relationship
from sidemantic.core.segment import Segment

# Backwards compatibility alias
Measure = Metric

__all__ = [
    "Dimension",
    "Measure",  # Backwards compatibility
    "Metric",
    "Model",
    "Parameter",
    "PreAggregation",
    "PreAggregationRecommender",
    "PreAggRecommendation",
    "QueryPattern",
    "RefreshKey",
    "RefreshResult",
    "Relationship",
    "Segment",
    "SemanticLayer",
]


def __getattr__(name):  # Lazy import to avoid importing duckdb on package import
    if name == "SemanticLayer":
        from sidemantic.core.semantic_layer import SemanticLayer  # type: ignore

        return SemanticLayer
    raise AttributeError(name)
