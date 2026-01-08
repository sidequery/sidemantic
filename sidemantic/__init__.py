"""Sidemantic: Universal semantic layer - import from Cube, dbt, LookML, Hex, and more."""

__version__ = "0.5.1"

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
    "load_from_directory",
]


def __getattr__(name):  # Lazy import to avoid importing duckdb on package import
    if name == "SemanticLayer":
        from sidemantic.core.semantic_layer import SemanticLayer  # type: ignore

        return SemanticLayer
    if name == "load_from_directory":
        from sidemantic.loaders import load_from_directory  # type: ignore

        return load_from_directory
    raise AttributeError(name)
